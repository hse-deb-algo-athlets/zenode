"""End-to-end tests over a real in-process zenoh session (no router needed)."""

import asyncio
import logging
import time

import pytest
from pydantic import BaseModel

from zenode import (
    DuplicateNodeError,
    Node,
    Service,
    ServiceError,
    ServiceTimeout,
    Topic,
    on_silence,
    publish,
    subscribe,
)
from zenode.envelope import encode_envelope
from zenode.msgs import NodeHealth
from zenode.presence import list_nodes_async
from zenode.testing import harness

pytestmark = pytest.mark.integration  # every test here stands up a real session


class Ping(BaseModel):
    value: int = 0


class Pong(BaseModel):
    doubled: int = 0


PING = Topic("test/ping", Ping)
POUT = Topic("test/pout", Ping)
LATCHED = Topic("test/latched", Ping, latched=True)
STALE = Topic("test/stale", Ping, max_age=0.05)
DOUBLE = Service("test/double", request=Ping, reply=Pong)


class EchoNode(Node):
    """Subscribes PING, republishes value+1 on POUT, serves DOUBLE."""

    name = "echoer"
    health_interval = None

    async def on_start(self) -> None:
        self.out = self.publisher(POUT)
        self.subscribe(PING, self.on_ping)
        self.serve(DOUBLE, self.on_double)

    async def on_ping(self, msg: Ping) -> None:
        self.out.put(Ping(value=msg.value + 1))

    async def on_double(self, req: Ping) -> Pong:
        if req.value < 0:
            raise ValueError("negative input")
        return Pong(doubled=req.value * 2)


async def test_typed_pubsub_roundtrip_through_node():
    async with harness() as h:
        await h.start_node(EchoNode)
        out = h.collect(POUT)
        h.publisher(PING).put(Ping(value=41))
        result = await out.next()
        assert result == Ping(value=42)


async def test_envelope_metadata_attached():
    async with harness() as h:
        await h.start_node(EchoNode)
        out = h.collect(POUT)
        h.publisher(PING).put(Ping(value=1))
        await out.next()
        env = out.envelopes[0]
        assert env.node == "echoer"
        assert env.seq == 1
        age = env.age_s()
        assert age is not None and age < 5.0


async def test_namespace_prefixing():
    async with harness(namespace="robo") as h:
        await h.start_node(EchoNode)
        out = h.collect(POUT)
        pub = h.publisher(PING)
        assert pub.key == "robo/test/ping"
        pub.put(Ping(value=1))
        assert (await out.next()).value == 2


async def test_malformed_payload_is_counted_not_fatal():
    async with harness() as h:
        received: list[Ping] = []
        sub = h.subscribe(PING, received.append)
        await asyncio.sleep(0.1)
        h.session.put(PING.resolve(""), b"this is not json")
        h.publisher(PING).put(Ping(value=5))
        deadline = time.monotonic() + 2.0
        while not received and time.monotonic() < deadline:
            await asyncio.sleep(0.02)
        assert received == [Ping(value=5)]
        assert sub.errors == 1


async def test_max_age_drops_stale_samples():
    async with harness() as h:
        received: list[Ping] = []
        sub = h.subscribe(STALE, received.append)
        await asyncio.sleep(0.1)
        old = encode_envelope("past", 1, time.time_ns() - 10_000_000_000)
        h.session.put(STALE.resolve(""), Ping(value=1).model_dump_json().encode(), attachment=old)
        h.publisher(STALE).put(Ping(value=2))
        deadline = time.monotonic() + 2.0
        while not received and time.monotonic() < deadline:
            await asyncio.sleep(0.02)
        assert received == [Ping(value=2)]
        assert sub.stale == 1


async def test_latest_mode_keeps_newest():
    async with harness() as h:
        seen: list[int] = []

        async def slow_handler(msg: Ping) -> None:
            seen.append(msg.value)
            await asyncio.sleep(0.05)

        h.subscribe(PING, slow_handler, mode="latest")
        await asyncio.sleep(0.1)
        pub = h.publisher(PING)
        for i in range(20):
            pub.put(Ping(value=i))
        deadline = time.monotonic() + 3.0
        while (not seen or seen[-1] != 19) and time.monotonic() < deadline:
            await asyncio.sleep(0.05)
        assert seen[-1] == 19
        assert len(seen) < 20  # intermediate samples were superseded


async def test_latched_topic_delivers_to_late_joiner():
    async with harness() as h:
        pub = h.publisher(LATCHED)
        pub.put(Ping(value=7))
        await asyncio.sleep(0.2)  # ensure the put lands in the cache first
        out = h.collect(LATCHED)  # subscribes AFTER the publish
        result = await out.next()
        assert result == Ping(value=7)


async def test_topic_qos_reaches_the_zenoh_publisher():
    """QoS is fixed at declare time, so a dropped parameter is invisible at runtime."""
    import zenoh

    async with harness() as h:
        pub = h.publisher(Topic("test/qos", Ping, priority="real_time", congestion_control="block"))
        assert pub._inner.priority == zenoh.Priority.REAL_TIME
        assert pub._inner.congestion_control == zenoh.CongestionControl.BLOCK


async def test_latched_topic_carries_qos_too():
    """The advanced-publisher branch takes the same parameters; it must not drift."""
    import zenoh

    async with harness() as h:
        pub = h.publisher(Topic("test/qos_latched", Ping, latched=True, priority="data_low"))
        assert pub._inner.priority == zenoh.Priority.DATA_LOW


async def test_service_call():
    async with harness() as h:
        await h.start_node(EchoNode)
        reply = await h.call(DOUBLE, Ping(value=21))
        assert reply == Pong(doubled=42)


async def test_service_error_propagates():
    async with harness() as h:
        await h.start_node(EchoNode)
        with pytest.raises(ServiceError, match="negative input"):
            await h.call(DOUBLE, Ping(value=-1))


async def test_service_timeout_when_unserved():
    async with harness() as h:
        nobody = Service("test/nobody", request=Ping, reply=Pong)
        with pytest.raises(ServiceTimeout):
            await h.call(nobody, Ping(value=1), timeout=0.3)


async def test_presence_lists_live_nodes():
    async with harness() as h:
        node = await h.start_node(EchoNode)
        names = await list_nodes_async(h.session, "")
        assert "echoer" in names
        await h.stop_node(node)
        await asyncio.sleep(0.2)
        names = await list_nodes_async(h.session, "")
        assert "echoer" not in names


async def test_duplicate_name_warns(caplog: pytest.LogCaptureFixture):
    with caplog.at_level(logging.WARNING, logger="zenode.node.echoer"):
        async with harness() as h:
            await h.start_node(EchoNode)
            await h.start_node(EchoNode)
    assert any("'echoer' is already running" in r.message for r in caplog.records)


async def test_duplicate_name_strict_raises():
    class StrictEcho(EchoNode):
        allow_duplicates = False

    async with harness() as h:
        await h.start_node(EchoNode)
        with pytest.raises(DuplicateNodeError, match="echoer"):
            await h.start_node(StrictEcho)


async def test_unique_name_does_not_warn(caplog: pytest.LogCaptureFixture):
    with caplog.at_level(logging.WARNING, logger="zenode.node.echoer"):
        async with harness() as h:
            await h.start_node(EchoNode)
    assert not any("already running" in r.message for r in caplog.records)


async def test_health_heartbeat():
    class ChattyNode(EchoNode):
        name = "chatty"
        health_interval = 0.1

    async with harness() as h:
        out = h.collect(Topic("node/chatty/health", NodeHealth))
        await h.start_node(ChattyNode)
        health = await out.next()
        assert health.node == "chatty"
        assert health.state == "running"
        assert health.uptime_s >= 0.0


async def test_timer_runs_and_survives_errors():
    class TickNode(Node):
        name = "ticker"
        health_interval = None
        ticks: int = 0

        async def on_start(self) -> None:
            self.every(0.02, self.tick)

        async def tick(self) -> None:
            self.ticks += 1
            if self.ticks == 1:
                raise RuntimeError("first tick explodes")

    async with harness() as h:
        node = await h.start_node(TickNode)
        deadline = time.monotonic() + 2.0
        while node.ticks < 3 and time.monotonic() < deadline:
            await asyncio.sleep(0.02)
        assert node.ticks >= 3  # kept ticking after the error


async def test_a_silent_producer_makes_the_node_safe_itself():
    """The whole point: nav dies, and the motors stop instead of coasting."""

    class Motors(Node):
        name = "motors"
        health_interval = None
        out = publish(POUT)

        @subscribe(PING, mode="latest", deadline=0.05)
        async def on_cmd(self, msg: Ping) -> None:
            self.out.put(msg)

        @on_silence(PING)
        async def cmd_lost(self, silent_for: float) -> None:
            self.out.put(Ping(value=0))  # zero velocity, once

    async with harness() as h:
        out = h.collect(POUT)
        await h.start_node(Motors)
        h.publisher(PING).put(Ping(value=7))

        assert (await out.next()).value == 7
        assert (await out.next()).value == 0  # the producer went quiet


async def test_a_deadline_can_stop_the_node():
    class Fatal(Node):
        name = "fatal"
        health_interval = None

        @subscribe(PING, deadline=0.05, on_deadline="stop")
        async def on_cmd(self, msg: Ping) -> None: ...

    async with harness() as h:
        node = await h.start_node(Fatal)
        await asyncio.wait_for(node.run_until_stopped(), timeout=2.0)


async def test_teardown_cancels_a_pending_deadline(caplog: pytest.LogCaptureFixture):
    """A stopped node must not keep warning about a producer it no longer wants."""

    class Quiet(Node):
        name = "quietly-waiting"
        health_interval = None

        @subscribe(PING, deadline=0.02)
        async def on_cmd(self, msg: Ping) -> None: ...

    async with harness() as h:
        node = await h.start_node(Quiet)
        (sub,) = node.subscriptions  # teardown clears the list; keep the object
        await asyncio.sleep(0.05)
        assert sub.deadline_misses == 1

        await h.stop_node(node)
        caplog.clear()  # drop the pre-stop warning; only what follows matters
        with caplog.at_level(logging.WARNING, logger="zenode.node.quietly-waiting"):
            await asyncio.sleep(0.1)

        assert sub.deadline_misses == 1  # the timer stopped with the node
        assert not any("never received" in r.message for r in caplog.records)
