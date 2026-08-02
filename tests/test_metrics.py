"""Latency accounting: the accumulator, and the numbers it puts on NodeHealth."""

import asyncio
import time

import pytest
from pydantic import BaseModel

from zenode import Node, Service, Topic, every, serve, subscribe
from zenode.metrics import Latency, ProcessStats, summarize
from zenode.msgs import NodeHealth
from zenode.testing import harness


class Ping(BaseModel):
    n: int = 0


class Reply(BaseModel):
    ok: bool = True


SLOW = Topic("test/slow", Ping)
SLOW_SVC = Service("test/slow_svc", request=Ping, reply=Reply)


# --------------------------------------------------------------- accumulator


def test_empty_latency_summarizes_to_zero():
    assert summarize([Latency()]) == (0.0, 0.0)
    assert summarize([]) == (0.0, 0.0)


def test_mean_and_max():
    acc = Latency()
    for seconds in (0.001, 0.002, 0.009):
        acc.observe(seconds)
    mean_ms, max_ms = summarize([acc])
    assert mean_ms == 4.0
    assert max_ms == 9.0


def test_reset_clears_the_window():
    acc = Latency()
    acc.observe(0.05)
    acc.reset()
    assert summarize([acc]) == (0.0, 0.0)


def test_summarize_weights_by_count():
    """A 1 Hz topic must not drag a 200 Hz topic's mean around."""
    busy, quiet = Latency(), Latency()
    for _ in range(99):
        busy.observe(0.001)  # 99 samples at 1 ms
    quiet.observe(0.101)  # 1 sample at 101 ms
    mean_ms, max_ms = summarize([busy, quiet])
    assert mean_ms == 2.0  # (99*1 + 101) / 100
    assert max_ms == 101.0


def test_summarize_takes_the_worst_max():
    a, b = Latency(), Latency()
    a.observe(0.001)
    b.observe(0.500)
    assert summarize([a, b])[1] == 500.0


def test_latency_has_no_per_observation_growth():
    """Bounded memory is the whole point on a robot that runs for a week."""
    acc = Latency()
    for _ in range(100_000):
        acc.observe(0.001)
    assert len(Latency.__slots__) == 3
    assert acc.count == 100_000


# ----------------------------------------------------------------- end-to-end


async def _next_health(out, predicate=lambda h: True) -> NodeHealth:
    """The first heartbeat matching ``predicate`` — heartbeats race with traffic."""
    for _ in range(30):
        health = await out.next(timeout=3.0)
        if predicate(health):
            return health
    raise AssertionError("no matching heartbeat")


@pytest.mark.integration
async def test_health_reports_handler_latency():
    class Slow(Node):
        name = "slowpoke"
        health_interval = 0.15

        @subscribe(SLOW)
        async def on_ping(self, msg: Ping) -> None:
            await asyncio.sleep(0.05)

    async with harness() as h:
        out = h.collect(Topic("node/slowpoke/health", NodeHealth))
        await h.start_node(Slow)
        pub = h.publisher(SLOW)
        for n in range(3):
            pub.put(Ping(n=n))
            await asyncio.sleep(0.02)
        health = await _next_health(out, lambda h: h.received > 0)
        assert health.handler_max_ms >= 45.0  # the 50 ms sleep, allowing jitter
        assert health.handler_mean_ms > 0.0


@pytest.mark.integration
async def test_health_reports_transport_age():
    class Quick(Node):
        name = "quick"
        health_interval = 0.15

        @subscribe(SLOW)
        async def on_ping(self, msg: Ping) -> None:
            pass

    async with harness() as h:
        out = h.collect(Topic("node/quick/health", NodeHealth))
        await h.start_node(Quick)
        h.publisher(SLOW).put(Ping(n=1))
        health = await _next_health(out, lambda h: h.received > 0)
        assert health.age_max_ms > 0.0
        assert health.age_max_ms < 1000.0  # in-process: sub-second, sanity only


@pytest.mark.integration
async def test_service_handler_time_reaches_health():
    class Server(Node):
        name = "svc"
        health_interval = 0.15

        @serve(SLOW_SVC)
        async def on_call(self, req: Ping) -> Reply:
            await asyncio.sleep(0.05)
            return Reply()

    async with harness() as h:
        out = h.collect(Topic("node/svc/health", NodeHealth))
        node = await h.start_node(Server)
        await h.call(SLOW_SVC, Ping(n=1))
        assert node._servers[0].handler_time.count == 1
        health = await _next_health(out, lambda h: h.handler_max_ms > 0)
        assert health.handler_max_ms >= 45.0


@pytest.mark.integration
async def test_latency_window_resets_between_heartbeats():
    """A spike must not haunt every later heartbeat."""

    class Spiky(Node):
        name = "spiky"
        health_interval = 0.15

        @subscribe(SLOW)
        async def on_ping(self, msg: Ping) -> None:
            await asyncio.sleep(0.05)

    async with harness() as h:
        out = h.collect(Topic("node/spiky/health", NodeHealth))
        await h.start_node(Spiky)
        h.publisher(SLOW).put(Ping(n=1))
        await _next_health(out, lambda h: h.handler_max_ms > 0)
        # Go quiet; a later heartbeat must report a clean window.
        out.clear()
        await asyncio.sleep(0.4)
        quiet = await out.next(timeout=3.0)
        assert quiet.handler_max_ms == 0.0
        assert quiet.received > 0  # counters stay cumulative


@pytest.mark.integration
async def test_timer_overruns_and_failures_reach_health():
    """A control loop that misses its deadline or throws is visible from outside."""

    class Struggling(Node):
        name = "struggling"
        health_interval = 0.15

        @every(0.01)
        async def slow_tick(self) -> None:
            await asyncio.sleep(0.06)  # six periods per tick

        @every(0.02)
        async def broken_tick(self) -> None:
            raise RuntimeError("axis fault")

    async with harness() as h:
        out = h.collect(Topic("node/struggling/health", NodeHealth))
        node = await h.start_node(Struggling)
        health = await _next_health(out, lambda h: h.timer_overruns > 0)
        assert health.handler_errors > 0, "a timer that raises counts as a handler error"
        assert sum(t.overruns for t in node.timers) >= health.timer_overruns


@pytest.mark.integration
async def test_deadline_misses_reach_health():
    """A producer that never came up is an absence — it has to be pushed, not polled."""

    class Waiting(Node):
        name = "waiting"
        health_interval = 0.15

        async def on_start(self) -> None:
            self.subscribe(SLOW, self.on_ping, deadline=0.05)

        async def on_ping(self, msg: Ping) -> None:
            pass

    async with harness() as h:
        out = h.collect(Topic("node/waiting/health", NodeHealth))
        node = await h.start_node(Waiting)
        health = await _next_health(out, lambda h: h.deadline_misses > 0)
        assert health.received == 0
        assert node.subscriptions[0].silent is True


@pytest.mark.integration
async def test_a_node_can_read_its_own_subscription_counters():
    """C8: 'commands dropped' belongs in a node's own status message."""

    class Counting(Node):
        name = "counting"
        health_interval = None

        @subscribe(SLOW)
        async def on_ping(self, msg: Ping) -> None:
            pass

    async with harness() as h:
        node = await h.start_node(Counting)
        h.publisher(SLOW).put(Ping(n=1))
        for _ in range(40):
            if node.subscriptions[0].received:
                break
            await asyncio.sleep(0.05)
        (sub,) = node.subscriptions
        assert (sub.received, sub.dropped, sub.stale, sub.errors) == (1, 0, 0, 0)
        assert sub.topic is SLOW


@pytest.mark.integration
async def test_handler_that_raises_is_still_timed():
    class Boom(Node):
        name = "boom"
        health_interval = None

        @subscribe(SLOW)
        async def on_ping(self, msg: Ping) -> None:
            await asyncio.sleep(0.03)
            raise ValueError("boom")

    async with harness() as h:
        node = await h.start_node(Boom)
        h.publisher(SLOW).put(Ping(n=1))
        for _ in range(40):
            if node._subscriptions[0].errors:
                break
            await asyncio.sleep(0.05)
        sub = node._subscriptions[0]
        assert sub.errors == 1
        assert sub.handler_time.max_s >= 0.025


# ------------------------------------------------------------- process stats


def test_rss_is_a_plausible_number():
    stats = ProcessStats()
    rss = stats.rss_bytes()
    if rss is None:
        pytest.skip("/proc not available on this platform")
    assert 1_000_000 < rss < 8_000_000_000  # a Python process, not a rounding error


def test_first_cpu_sample_has_no_interval_to_divide_by():
    assert ProcessStats().cpu_percent() is None


def test_cpu_percent_measures_work_between_samples():
    stats = ProcessStats()
    if stats.rss_bytes() is None:
        pytest.skip("/proc not available on this platform")
    stats.cpu_percent()  # prime
    deadline = time.monotonic() + 0.2
    while time.monotonic() < deadline:  # burn a core
        pass
    busy = stats.cpu_percent()
    assert busy is not None
    assert busy > 50.0  # a spin loop is most of one core


def test_unsupported_platform_degrades_to_none(monkeypatch):
    """No /proc means unknown, not zero — the two are different answers."""
    monkeypatch.setattr("zenode.metrics.os.path.exists", lambda _: False)
    stats = ProcessStats()
    assert stats.cpu_percent() is None
    assert stats.rss_bytes() is None


@pytest.mark.integration
async def test_health_carries_resource_signals():
    class Idle(Node):
        name = "resourced"
        health_interval = 0.1

    async with harness() as h:
        health = h.collect(Topic(f"node/{Idle.name}/health", NodeHealth))
        await h.start_node(Idle)
        await health.next()
        second = await health.next()  # the first has no CPU interval yet

    assert second.rss_bytes is not None and second.rss_bytes > 0
    assert second.cpu_percent is not None and second.cpu_percent >= 0.0


@pytest.mark.integration
async def test_queue_depth_is_reported_before_anything_is_dropped():
    """DROP tells you it overflowed; QMAX tells you it was about to."""
    slow = Topic("test/metrics/slow", Ping)

    class Slow(Node):
        name = "backlogged"
        health_interval = 0.3

        async def on_start(self) -> None:
            self.subscribe(slow, self.crawl, queue_size=64)

        async def crawl(self, msg: Ping) -> None:
            await asyncio.sleep(0.02)

    async with harness() as h:
        health = h.collect(Topic(f"node/{Slow.name}/health", NodeHealth))
        await h.start_node(Slow)
        publisher = h.publisher(slow)
        for n in range(20):
            publisher.put(Ping(n=n))
        deepest = 0
        for _ in range(4):
            deepest = max(deepest, (await health.next()).queue_max_depth)

    assert deepest > 1  # a backlog formed and was visible without any drops
