"""Declarative wiring: @subscribe/@serve/@every decorators and publish() descriptors."""

import asyncio
import time

import pytest
from pydantic import BaseModel

from zenode import (
    ConfigError,
    ContractError,
    Envelope,
    Node,
    NodeConfig,
    Service,
    Topic,
    every,
    on_matching,
    on_resume,
    on_silence,
    publish,
    serve,
    subscribe,
)
from zenode.declarative import collect_bindings, collect_publishers
from zenode.testing import harness, local_transport


class Ping(BaseModel):
    value: int = 0


class Pong(BaseModel):
    doubled: int = 0


IN_A = Topic("decl/in_a", Ping)
IN_B = Topic("decl/in_b", Ping)
OUT = Topic("decl/out", Ping)
DOUBLE = Service("decl/double", request=Ping, reply=Pong)


class DeclNode(Node):
    name = "decl"
    health_interval = None

    out = publish(OUT)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.ticks = 0
        self.envelopes: list[Envelope] = []

    @subscribe(IN_A)
    @subscribe(IN_B)
    async def on_ping(self, msg: Ping, env: Envelope) -> None:
        self.envelopes.append(env)
        self.out.put(Ping(value=msg.value + 1))

    @serve(DOUBLE)
    async def on_double(self, req: Ping) -> Pong:
        return Pong(doubled=req.value * 2)

    @every(0.02)
    async def tick(self) -> None:
        self.ticks += 1


async def _settle(predicate, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate() and time.monotonic() < deadline:
        await asyncio.sleep(0.02)


@pytest.mark.integration
async def test_decorated_subscribe_and_publish_descriptor():
    async with harness() as h:
        await h.start_node(DeclNode)
        out = h.collect(OUT)
        h.publisher(IN_A).put(Ping(value=1))
        assert (await out.next()).value == 2


@pytest.mark.integration
async def test_stacked_subscribe_feeds_one_handler_from_both_topics():
    async with harness() as h:
        node = await h.start_node(DeclNode)
        out = h.collect(OUT)
        h.publisher(IN_A).put(Ping(value=1))
        h.publisher(IN_B).put(Ping(value=10))
        await out.next()
        await out.next()
        assert len(node.envelopes) == 2


@pytest.mark.integration
async def test_decorated_serve():
    async with harness() as h:
        await h.start_node(DeclNode)
        assert (await h.call(DOUBLE, Ping(value=21))).doubled == 42


@pytest.mark.integration
async def test_decorated_timer_runs():
    async with harness() as h:
        node = await h.start_node(DeclNode)
        await _settle(lambda: node.ticks >= 3)
        assert node.ticks >= 3


# -------------------------------------------------------------- @every(config)


class RateConfig(NodeConfig):
    control_rate_hz: float = 100.0
    diagnostics_period_s: float = 0.05


class RateNode(Node):
    """Timer rates from config — the first thing anyone tunes on real hardware."""

    name = "rates"
    health_interval = None
    config: RateConfig

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.control = 0
        self.diagnostics = 0
        self.derived = 0

    @every("control_rate_hz", unit="hz")
    async def control_tick(self) -> None:
        self.control += 1

    @every("diagnostics_period_s")
    async def diagnostics_tick(self) -> None:
        self.diagnostics += 1

    @every(lambda self: 1 / self.config.control_rate_hz)
    async def derived_tick(self) -> None:
        self.derived += 1


@pytest.mark.integration
async def test_every_reads_its_rate_from_config():
    async with harness() as h:
        node = await h.start_node(RateNode, config=RateConfig(control_rate_hz=200.0))
        intervals = {t.name: t.interval for t in node.timers}
        assert intervals == {
            "control_tick": pytest.approx(0.005),
            "diagnostics_tick": pytest.approx(0.05),
            "derived_tick": pytest.approx(0.005),
        }
        await _settle(lambda: node.control >= 3 and node.derived >= 3)
        assert node.control >= 3 and node.derived >= 3


@pytest.mark.integration
async def test_an_unresolvable_rate_fails_at_start_not_at_the_first_tick():
    class Typo(Node):
        name = "typo"
        health_interval = None
        config: RateConfig

        @every("controll_rate_hz", unit="hz")  # typo
        async def tick(self) -> None: ...

    async with harness() as h:
        node = Typo(config=RateConfig(), session=h.session, transport=local_transport())
        with pytest.raises(ConfigError, match="no config field 'controll_rate_hz'"):
            await node.start()
        assert node.state == "stopped"


@pytest.mark.integration
async def test_a_decorated_timer_can_be_fatal():
    class Fatal(Node):
        name = "fatal-tick"
        health_interval = None

        @every(0.01, on_error="stop")
        async def tick(self) -> None:
            raise RuntimeError("control loop broken")

    async with harness() as h:
        node = await h.start_node(Fatal)
        await asyncio.wait_for(node.run_until_stopped(), timeout=2.0)


def test_a_literal_interval_is_still_validated_at_decoration():
    with pytest.raises(ContractError, match="must be positive"):
        every(0.0)


@pytest.mark.integration
async def test_undecorated_override_inherits_binding():
    class Child(DeclNode):
        name = "decl-child"

        async def on_ping(self, msg: Ping, env: Envelope) -> None:  # no re-decoration
            self.out.put(Ping(value=msg.value + 100))

    async with harness() as h:
        await h.start_node(Child)
        out = h.collect(OUT)
        h.publisher(IN_A).put(Ping(value=1))
        assert (await out.next()).value == 101


def test_publisher_descriptor_guards():
    node = DeclNode(transport=local_transport())
    with pytest.raises(RuntimeError, match="before the node has started"):
        _ = node.out
    with pytest.raises(AttributeError, match="zenode-managed publisher"):
        node.out = None  # type: ignore[assignment]
    assert DeclNode.out.topic is OUT  # class access returns the descriptor


@pytest.mark.integration
async def test_publisher_descriptor_usable_in_on_start():
    class Early(Node):
        name = "early"
        health_interval = None
        out = publish(OUT)

        async def on_start(self) -> None:
            self.out.put(Ping(value=7))  # descriptors materialize before on_start

    async with harness() as h:
        out = h.collect(OUT)
        await asyncio.sleep(0.1)
        await h.start_node(Early)
        assert (await out.next()).value == 7


def test_collect_semantics():
    bindings = collect_bindings(DeclNode)
    assert set(bindings) == {"on_ping", "on_double", "tick"}
    assert len(bindings["on_ping"]) == 2  # stacked
    assert set(collect_publishers(DeclNode)) == {"out"}

    class Overridden(DeclNode):
        out = "not a publisher anymore"  # type: ignore[assignment]

    assert collect_publishers(Overridden) == {}


# --------------------------------------------------------- silence / resume


def test_hook_bindings_are_collected_and_stackable():
    class Hooked(Node):
        name = "hooked"
        health_interval = None

        @subscribe(IN_A, deadline=0.3)
        async def on_ping(self, msg: Ping) -> None: ...

        @on_silence(IN_A)
        @on_silence(IN_B)
        async def lost(self, silent_for: float) -> None: ...

    bindings = collect_bindings(Hooked)
    assert set(bindings) == {"on_ping", "lost"}
    assert [b.kind for b in bindings["lost"]] == ["on_silence", "on_silence"]


def test_a_literal_deadline_is_validated_at_decoration():
    """Fail where you typed it, as ``@every`` does."""
    with pytest.raises(ContractError, match="deadline must be positive"):
        subscribe(IN_A, deadline=0.0)


async def test_a_silence_handler_stays_directly_callable():
    """Decorators only stamp metadata, so a test drives the reaction directly."""

    class Reacting(Node):
        name = "reacting"
        health_interval = None

        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.lost: list[float] = []

        @subscribe(IN_A, deadline=0.3)
        async def on_ping(self, msg: Ping) -> None: ...

        @on_silence(IN_A)
        async def a_lost(self, silent_for: float) -> None:
            self.lost.append(silent_for)

    node = Reacting()
    await node.a_lost(1.5)
    assert node.lost == [1.5]


@pytest.mark.integration
async def test_on_silence_binds_to_a_subscription_declared_later():
    """Hook and subscription are separate attributes; declaration order must not matter."""

    class Watcher(Node):
        name = "watcher"
        health_interval = None

        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.lost: list[float] = []
            self.back: list[float] = []

        # deliberately declared *above* the @subscribe it depends on
        @on_silence(IN_A)
        async def a_lost(self, silent_for: float) -> None:
            self.lost.append(silent_for)

        @on_resume(IN_A)
        async def a_back(self, silent_for: float) -> None:
            self.back.append(silent_for)

        @subscribe(IN_A, deadline=0.03)
        async def on_ping(self, msg: Ping) -> None: ...

    async with harness() as h:
        node = await h.start_node(Watcher)
        await asyncio.sleep(0.08)
        assert len(node.lost) == 1

        h.publisher(IN_A).put(Ping(value=1))
        for _ in range(50):
            if node.back:
                break
            await asyncio.sleep(0.02)
        assert len(node.back) == 1


@pytest.mark.integration
async def test_on_silence_attaches_to_an_imperative_subscription():
    """Matching is by resolved key, so the ``on_start`` escape hatch is covered."""

    class Manual(Node):
        name = "manual"
        health_interval = None

        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.lost: list[float] = []

        async def on_start(self) -> None:
            self.subscribe(IN_A, self.on_ping, deadline=0.03)

        async def on_ping(self, msg: Ping) -> None: ...

        @on_silence(IN_A)
        async def a_lost(self, silent_for: float) -> None:
            self.lost.append(silent_for)

    async with harness() as h:
        node = await h.start_node(Manual)
        await asyncio.sleep(0.08)
        assert len(node.lost) == 1


@pytest.mark.integration
async def test_a_slow_on_start_does_not_lose_the_silence_edge():
    """Bindings wire after ``on_start``, so the edge can pass while hardware opens.

    Edge-triggered means a missed edge is missed forever — the flagship failure
    (producer never came up, motors never zeroed) is exactly this case.
    """

    class SlowStart(Node):
        name = "slowstart"
        health_interval = None

        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.lost: list[float] = []

        async def on_start(self) -> None:
            self.subscribe(IN_A, self.on_ping, deadline=0.02)
            await asyncio.sleep(0.1)  # ...opening the third motor

        async def on_ping(self, msg: Ping) -> None: ...

        @on_silence(IN_A)
        async def a_lost(self, silent_for: float) -> None:
            self.lost.append(silent_for)

    async with harness() as h:
        node = await h.start_node(SlowStart)
        await asyncio.sleep(0.02)
        assert len(node.lost) == 1
        assert node.lost[0] >= 0.02


@pytest.mark.integration
async def test_on_silence_without_a_deadline_fails_at_start():
    class NoDeadline(Node):
        name = "nodeadline"
        health_interval = None

        @subscribe(IN_A)
        async def on_ping(self, msg: Ping) -> None: ...

        @on_silence(IN_A)
        async def lost(self, silent_for: float) -> None: ...

    async with harness() as h:
        node = NoDeadline(session=h.session, transport=local_transport())
        with pytest.raises(ContractError, match="without deadline"):
            await node.start()
        assert node.state == "stopped"


@pytest.mark.integration
async def test_on_silence_for_an_unsubscribed_topic_fails_at_start():
    class Orphan(Node):
        name = "orphan"
        health_interval = None

        @on_silence(IN_B)
        async def lost(self, silent_for: float) -> None: ...

    async with harness() as h:
        node = Orphan(session=h.session, transport=local_transport())
        with pytest.raises(ContractError, match="nothing on this node subscribes"):
            await node.start()


@pytest.mark.integration
async def test_on_matching_gates_a_declared_publisher():
    """The camera case: told where it starts, then told about every change."""

    class Camera(Node):
        name = "camera"
        health_interval = None

        frames = publish(OUT)

        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.streaming: list[bool] = []

        @on_matching(OUT)
        async def on_viewers(self, matching: bool) -> None:
            self.streaming.append(matching)

    async with harness() as h:
        node = await h.start_node(Camera)
        for _ in range(50):
            if node.streaming:
                break
            await asyncio.sleep(0.02)
        assert node.streaming == [False]

        sub = h.subscribe(OUT, lambda msg: None)
        for _ in range(50):
            if node.streaming[-1]:
                break
            await asyncio.sleep(0.02)
        assert node.streaming == [False, True]

        await sub.stop()
        for _ in range(50):
            if not node.streaming[-1]:
                break
            await asyncio.sleep(0.02)
        assert node.streaming == [False, True, False]


@pytest.mark.integration
async def test_on_matching_for_an_unpublished_topic_fails_at_start():
    """A gate over a topic this node never publishes can only be a typo."""

    class Orphan(Node):
        name = "orphan-matching"
        health_interval = None

        @on_matching(OUT)
        async def viewers(self, matching: bool) -> None: ...

    async with harness() as h:
        node = Orphan(session=h.session, transport=local_transport())
        with pytest.raises(ContractError, match="nothing on this node publishes"):
            await node.start()


@pytest.mark.integration
async def test_on_matching_binds_an_imperative_publisher():
    """Matched by resolved key, so the ``on_start`` escape hatch works too."""

    class Manual(Node):
        name = "manual-matching"
        health_interval = None

        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.streaming: list[bool] = []

        async def on_start(self) -> None:
            self.publisher(OUT)

        @on_matching(OUT)
        def viewers(self, matching: bool) -> None:
            self.streaming.append(matching)

    async with harness() as h:
        node = await h.start_node(Manual)
        for _ in range(50):
            if node.streaming:
                break
            await asyncio.sleep(0.02)
        assert node.streaming == [False]
