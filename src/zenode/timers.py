"""Periodic work: pinned to a period grid, with a stated error policy.

- **Scheduling.** Ticks are pinned to a period grid, so the period does not
  drift by the body's runtime. A body that outruns its period *skips* the
  missed periods (counted as overruns) instead of bursting to catch up.
  ("Deadline" means one thing in this codebase, and it is
  :class:`zenode.Subscription`'s silence detector — not this.)
- **Failure.** The caller states what a raising body means:
  ``on_error="log"`` (default), ``"stop"``, or a callback — see
  :data:`OnTimerError`.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import math
from collections.abc import Awaitable, Callable
from typing import Any, Literal

from .errors import ConfigError, ContractError

logger = logging.getLogger(__name__)

OnTimerError = Literal["log", "stop"] | Callable[[Exception], Any]
"""What a timer does when its body raises.

``"log"`` logs the traceback and keeps ticking. ``"stop"`` logs it and stops
the node (running ``on_stop()``, so hardware is released and a supervisor can
restart the process). A callable receives the exception (sync or async) and
the timer continues; call ``node.stop()`` from it to combine both.
"""

IntervalSpec = float | str | Callable[[Any], float]
"""A timer period: a number, the name of a config field, or ``self -> number``."""

IntervalUnit = Literal["s", "hz"]


def resolve_interval(
    spec: IntervalSpec,
    owner: Any,
    *,
    unit: IntervalUnit = "s",
    where: str,
) -> float:
    """Turn an :data:`IntervalSpec` into seconds, against ``owner``'s config.

    Raises :class:`ConfigError` — naming ``where`` the declaration lives — so a
    rate that cannot be resolved fails at ``start()`` rather than at first tick.
    """
    if isinstance(spec, str):
        config = getattr(owner, "config", None)
        if not hasattr(config, spec):
            raise ConfigError(
                f"{where}: no config field {spec!r} on {type(config).__name__} "
                f"(the interval is read from self.config at start)"
            )
        value = getattr(config, spec)
    elif isinstance(spec, (int, float)):
        value = spec
    else:
        value = spec(owner)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigError(f"{where}: interval must be a number, got {value!r}")
    number = float(value)
    if number <= 0 or not math.isfinite(number):
        raise ConfigError(f"{where}: interval must be positive and finite, got {number!r} ({unit})")
    return 1.0 / number if unit == "hz" else number


class Timer:
    """One periodic call and its counters. Created by :meth:`zenode.Node.every`."""

    def __init__(
        self,
        name: str,
        interval: float,
        fn: Callable[[], Any | Awaitable[Any]],
        *,
        on_error: OnTimerError = "log",
        log: logging.Logger = logger,
        stop: Callable[[], None] | None = None,
    ) -> None:
        if interval <= 0:
            raise ContractError(f"timer {name!r}: interval must be positive, got {interval!r}")
        if on_error not in ("log", "stop") and not callable(on_error):
            raise ContractError(
                f"timer {name!r}: on_error must be 'log', 'stop', or a callable, got {on_error!r}"
            )
        self.name = name
        self.interval = interval
        self.ticks = 0
        """Bodies that completed without raising."""
        self.overruns = 0
        """Deadlines missed because a body ran longer than the interval."""
        self.errors = 0
        self.task: asyncio.Task[None] | None = None
        self._fn = fn
        self._on_error = on_error
        self._log = log
        self._stop = stop

    def cancel(self) -> None:
        """Stop this timer. The node cancels all of its timers on teardown."""
        if self.task is not None:
            self.task.cancel()

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        next_at = loop.time()
        while True:
            next_at += self.interval
            behind = loop.time() - next_at
            if behind > 0:
                missed = int(behind // self.interval) + 1
                self.overruns += missed
                next_at += missed * self.interval
            await asyncio.sleep(max(0.0, next_at - loop.time()))
            if not await self._tick():
                return

    async def _tick(self) -> bool:
        """Run the body once; ``False`` means the timer is done."""
        try:
            result = self._fn()
            if inspect.isawaitable(result):
                await result
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.errors += 1
            self._log.exception("timer raised", extra={"timer": self.name})
            return await self._on_failure(exc)
        self.ticks += 1
        return True

    async def _on_failure(self, exc: Exception) -> bool:
        if self._on_error == "stop":
            self._log.error("stopping the node: timer %r failed", self.name)
            if self._stop is not None:
                self._stop()
            return False
        if callable(self._on_error):
            try:
                result = self._on_error(exc)
                if inspect.isawaitable(result):
                    await result
            except Exception:
                self._log.exception("timer error handler raised", extra={"timer": self.name})
        return True


__all__ = ["IntervalSpec", "IntervalUnit", "OnTimerError", "Timer", "resolve_interval"]
