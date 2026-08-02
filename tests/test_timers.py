"""Timers: deadline scheduling, overrun counting, and the error policy.

These drive ``Timer`` directly — no node, no session — because what matters
here is arithmetic and control flow: that the period does not absorb the body's
runtime, that a body which outruns its deadline says so, and that a failing
control loop can be made to stop the process instead of running as a zombie.
"""

from __future__ import annotations

import asyncio
import logging
from types import SimpleNamespace
from typing import Any, cast

import pytest

from zenode.errors import ConfigError, ContractError
from zenode.timers import Timer, resolve_interval

log = logging.getLogger("zenode.test.timer")


def make_timer(fn, interval: float = 0.01, **kwargs) -> Timer:
    return Timer(kwargs.pop("name", "tick"), interval, fn, log=log, **kwargs)


async def run_until(timer: Timer, predicate, *, timeout: float = 2.0) -> asyncio.Task[None]:
    """Start ``timer`` and let it tick until ``predicate`` holds (or time out)."""
    task = asyncio.create_task(timer.run())
    timer.task = task
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate() and asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(0.005)
    return task


# ------------------------------------------------------------------ scheduling


async def test_the_period_does_not_absorb_the_body_runtime():
    """The bug this replaces: sleep-then-run makes the real period dt+body."""
    interval, body = 0.02, 0.015
    starts: list[float] = []
    loop = asyncio.get_running_loop()

    async def slow_body() -> None:
        starts.append(loop.time())
        await asyncio.sleep(body)

    timer = make_timer(slow_body, interval)
    task = await run_until(timer, lambda: len(starts) >= 5)
    task.cancel()

    elapsed = starts[4] - starts[0]
    assert elapsed == pytest.approx(4 * interval, abs=0.03)
    assert elapsed < 4 * (interval + body) * 0.8, "period drifted by the body's runtime"


async def test_a_body_that_outruns_its_deadline_is_counted_and_skipped():
    """One slow tick costs a period, not a burst of catch-up ticks."""
    calls = 0

    async def slow_first() -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            await asyncio.sleep(0.05)  # five periods long

    timer = make_timer(slow_first, 0.01)
    task = await run_until(timer, lambda: timer.overruns > 0)
    overruns_then = timer.overruns
    await asyncio.sleep(0.03)
    task.cancel()

    assert overruns_then >= 1
    assert timer.overruns == overruns_then, "fast ticks must not keep counting overruns"


async def test_a_punctual_timer_reports_no_overruns():
    timer = make_timer(lambda: None, 0.01)
    task = await run_until(timer, lambda: timer.ticks >= 5)
    task.cancel()
    assert timer.overruns == 0


async def test_sync_and_async_bodies_both_tick():
    seen: list[str] = []

    async def async_body() -> None:
        seen.append("async")

    sync = make_timer(lambda: seen.append("sync"), 0.01)
    coro = make_timer(async_body, 0.01)
    tasks = [
        await run_until(sync, lambda: seen.count("sync") >= 2),
        await run_until(coro, lambda: seen.count("async") >= 2),
    ]
    for task in tasks:
        task.cancel()

    assert seen.count("sync") >= 2 and seen.count("async") >= 2


async def test_cancel_stops_the_timer():
    timer = make_timer(lambda: None, 0.01)
    await run_until(timer, lambda: timer.ticks >= 1)
    timer.cancel()
    await asyncio.sleep(0.03)
    ticks = timer.ticks
    await asyncio.sleep(0.03)
    assert timer.ticks == ticks


# ---------------------------------------------------------------- error policy


async def test_the_default_policy_logs_and_keeps_ticking(caplog: pytest.LogCaptureFixture):
    def boom() -> None:
        raise RuntimeError("axis 1 is not responding")

    timer = make_timer(boom, 0.01)
    with caplog.at_level(logging.ERROR, logger=log.name):
        task = await run_until(timer, lambda: timer.errors >= 3)
    task.cancel()

    assert timer.errors >= 3, "a telemetry tick keeps going after a failure"
    assert timer.ticks == 0
    assert any("timer raised" in r.message for r in caplog.records)


async def test_on_error_stop_stops_the_node_and_ends_the_timer():
    """A broken control loop must not keep the process alive as a publisher."""
    stopped: list[bool] = []
    timer = make_timer(lambda: 1 / 0, 0.01, on_error="stop", stop=lambda: stopped.append(True))
    task = await run_until(timer, lambda: bool(stopped))

    await asyncio.wait_for(task, timeout=1.0)  # the loop exits by itself
    assert stopped == [True]
    assert timer.errors == 1


async def test_on_error_callback_sees_the_exception_and_the_timer_continues():
    seen: list[Exception] = []
    timer = make_timer(lambda: 1 / 0, 0.01, on_error=seen.append)
    task = await run_until(timer, lambda: len(seen) >= 2)
    task.cancel()

    assert isinstance(seen[0], ZeroDivisionError)
    assert timer.errors >= 2


async def test_an_async_error_callback_is_awaited():
    latched: list[str] = []

    async def latch(exc: Exception) -> None:
        latched.append(type(exc).__name__)

    timer = make_timer(lambda: 1 / 0, 0.01, on_error=latch)
    task = await run_until(timer, lambda: bool(latched))
    task.cancel()
    assert latched[0] == "ZeroDivisionError"


async def test_an_error_callback_that_raises_is_logged_not_fatal(
    caplog: pytest.LogCaptureFixture,
):
    def worse(_exc: Exception) -> None:
        raise ValueError("the fault handler is broken too")

    timer = make_timer(lambda: 1 / 0, 0.01, on_error=worse)
    with caplog.at_level(logging.ERROR, logger=log.name):
        task = await run_until(timer, lambda: timer.errors >= 2)
    task.cancel()

    assert any("timer error handler raised" in r.message for r in caplog.records)


def test_construction_rejects_nonsense():
    with pytest.raises(ContractError, match="interval must be positive"):
        Timer("t", 0.0, lambda: None)
    with pytest.raises(ContractError, match="on_error"):
        Timer("t", 1.0, lambda: None, on_error=cast(Any, "explode"))


# ------------------------------------------------------------ interval specs


@pytest.mark.parametrize(
    "spec,unit,expected",
    [
        pytest.param(0.05, "s", 0.05, id="seconds"),
        pytest.param(50, "hz", 0.02, id="hz"),
        pytest.param("control_rate_hz", "hz", 0.02, id="config-field-hz"),
        pytest.param("period_s", "s", 0.25, id="config-field-seconds"),
        pytest.param(lambda self: 1 / self.config.control_rate_hz, "s", 0.02, id="callable"),
    ],
)
def test_resolve_interval(spec, unit, expected):
    owner = SimpleNamespace(config=SimpleNamespace(control_rate_hz=50.0, period_s=0.25))
    assert resolve_interval(spec, owner, unit=unit, where="@every") == pytest.approx(expected)


@pytest.mark.parametrize(
    "spec,unit,match",
    [
        pytest.param("missing", "hz", "no config field 'missing'", id="unknown-field"),
        pytest.param("name", "s", "must be a number", id="not-a-number"),
        pytest.param("control_rate_hz", "s", None, id="valid"),
        pytest.param(0.0, "s", "must be positive", id="zero"),
        pytest.param(-1.0, "hz", "must be positive", id="negative"),
        pytest.param(lambda self: 0.0, "s", "must be positive", id="callable-zero"),
    ],
)
def test_resolve_interval_failures(spec, unit, match):
    owner = SimpleNamespace(config=SimpleNamespace(control_rate_hz=50.0, name="motor"))
    if match is None:
        resolve_interval(spec, owner, unit=unit, where="@every on Motor.tick")
        return
    with pytest.raises(ConfigError, match=match) as info:
        resolve_interval(spec, owner, unit=unit, where="@every on Motor.tick")
    assert "@every on Motor.tick" in str(info.value)  # says which declaration


def test_resolve_interval_without_any_config_says_so():
    with pytest.raises(ConfigError, match="no config field 'rate_hz'"):
        resolve_interval("rate_hz", SimpleNamespace(), where="@every")
