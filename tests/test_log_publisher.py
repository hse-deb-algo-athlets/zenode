"""Log records on the bus: the handler, its loop guard, and `zenode logs`."""

import asyncio
import logging
from typing import cast

import pytest

from zenode import Node, Topic, trace
from zenode.log import LogPublisher, _publishing
from zenode.msgs.log import LogRecordMsg, log_key, log_pattern
from zenode.pubsub import Publisher
from zenode.testing import harness

LOGS = Topic(log_key("noisy"), LogRecordMsg)


def _record(message: str = "m", level: int = logging.WARNING, **extra) -> logging.LogRecord:
    record = logging.LogRecord("some.logger", level, "f.py", 42, message, None, None)
    record.__dict__.update(extra)
    return record


class _FakePublisher:
    """Stands in for the node's log publisher; only ``put`` is ever called."""

    def __init__(self) -> None:
        self.published: list[LogRecordMsg] = []

    def put(self, value: LogRecordMsg) -> None:
        self.published.append(value)


def _handler(publisher: _FakePublisher, **kwargs) -> LogPublisher:
    return LogPublisher(
        cast(Publisher[LogRecordMsg], publisher), "nav", asyncio.get_running_loop(), **kwargs
    )


# --------------------------------------------------------------------- keys


def test_log_pattern_matches_every_node():
    assert log_key("nav") == "node/nav/log"
    assert log_pattern("robodog") == "robodog/node/*/log"
    assert log_pattern("") == "node/*/log"


# ------------------------------------------------------------------ handler


async def test_emit_queues_and_drain_publishes():
    publisher = _FakePublisher()
    handler = _handler(publisher)
    handler.emit(_record("disk almost full", key="state/odometry"))

    task = asyncio.create_task(handler.drain())
    await asyncio.sleep(0.05)
    task.cancel()

    assert len(publisher.published) == 1
    published = publisher.published[0]
    assert published.node == "nav"
    assert published.level == "WARNING"
    assert published.logger == "some.logger"
    assert published.message == "disk almost full"
    assert published.line == 42
    assert published.fields == {"key": "state/odometry"}


async def test_trace_id_rides_along():
    publisher = _FakePublisher()
    handler = _handler(publisher)
    traceparent = trace.new_traceparent()
    with trace.using(traceparent):
        handler.emit(_record())

    task = asyncio.create_task(handler.drain())
    await asyncio.sleep(0.05)
    task.cancel()

    assert publisher.published[0].trace == trace.trace_id_of(traceparent)


async def test_full_queue_drops_oldest_and_counts():
    publisher = _FakePublisher()
    handler = _handler(publisher, capacity=4)
    for n in range(10):
        handler.emit(_record(f"m{n}"))

    assert handler.dropped == 6

    task = asyncio.create_task(handler.drain())
    await asyncio.sleep(0.05)
    task.cancel()

    # Oldest dropped, newest kept: the recent past is what you want at 2am.
    assert [m.message for m in publisher.published] == ["m6", "m7", "m8", "m9"]


async def test_publishing_does_not_publish_its_own_logs():
    """The loop guard: without it, a publish that logs publishes forever."""
    publisher = _FakePublisher()
    handler = _handler(publisher)

    token = _publishing.set(True)
    try:
        handler.emit(_record("emitted from inside the publish path"))
    finally:
        _publishing.reset(token)

    task = asyncio.create_task(handler.drain())
    await asyncio.sleep(0.05)
    task.cancel()

    assert publisher.published == []


async def test_closed_handler_stops_accepting():
    publisher = _FakePublisher()
    handler = _handler(publisher)
    handler.close()
    handler.emit(_record())
    assert not handler._pending


# --------------------------------------------------------------- end-to-end


@pytest.mark.integration
async def test_node_publishes_its_warnings_on_the_bus():
    class Noisy(Node):
        name = "noisy"
        health_interval = None

        async def on_start(self) -> None:
            self.log.warning("wheel slip detected", extra={"wheel": "fl"})

    async with harness() as h:
        out = h.collect(LOGS)
        await h.start_node(Noisy)
        record = await out.next()

    assert record.node == "noisy"
    assert record.level == "WARNING"
    assert record.message == "wheel slip detected"
    assert record.fields["wheel"] == "fl"


@pytest.mark.integration
async def test_level_gate_keeps_info_off_the_bus():
    """The console can be at INFO while the bus stays at WARNING."""

    class Chatty(Node):
        name = "noisy"
        health_interval = None

        async def on_start(self) -> None:
            self.log.setLevel(logging.INFO)
            self.log.info("this stays local")
            self.log.error("this goes on the bus")

    async with harness() as h:
        out = h.collect(LOGS)
        await h.start_node(Chatty)
        record = await out.next()
        await asyncio.sleep(0.1)

    assert record.message == "this goes on the bus"
    assert [r.message for r in out.items] == ["this goes on the bus"]


@pytest.mark.integration
async def test_publish_logs_at_none_disables_it():
    class Quiet(Node):
        name = "noisy"
        health_interval = None
        publish_logs_at = None

        async def on_start(self) -> None:
            self.log.error("nobody hears this")

    async with harness() as h:
        out = h.collect(LOGS)
        await h.start_node(Quiet)
        await asyncio.sleep(0.2)

    assert out.items == []


@pytest.mark.integration
async def test_handler_is_removed_on_shutdown():
    class Brief(Node):
        name = "noisy"
        health_interval = None

    node_log = logging.getLogger("zenode.node.noisy")
    async with harness() as h:
        node = await h.start_node(Brief)
        assert any(isinstance(x, LogPublisher) for x in node_log.handlers)
        await h.stop_node(node)
    assert not any(isinstance(x, LogPublisher) for x in node_log.handlers)


@pytest.mark.integration
async def test_two_nodes_in_one_process_do_not_publish_each_others_records():
    """A root-logger handler would attribute every record to both nodes."""

    class Alpha(Node):
        name = "alpha"
        health_interval = None

        async def on_start(self) -> None:
            self.log.error("from alpha")

    class Beta(Node):
        name = "beta"
        health_interval = None

    alpha_logs = Topic(log_key("alpha"), LogRecordMsg)
    beta_logs = Topic(log_key("beta"), LogRecordMsg)

    async with harness() as h:
        seen_alpha = h.collect(alpha_logs)
        seen_beta = h.collect(beta_logs)
        await h.start_node(Beta)
        await h.start_node(Alpha)
        await seen_alpha.next()
        await asyncio.sleep(0.1)

    assert [r.message for r in seen_alpha.items] == ["from alpha"]
    assert seen_beta.items == []
