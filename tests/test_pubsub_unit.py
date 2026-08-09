"""Publisher/Subscription internals, driven directly instead of over the wire.

test_integration.py proves the pieces work through a real session; these tests
pin down the decisions that are hard to provoke on purpose from outside — a
queue overflowing, a sample arriving after shutdown, a handler that lies about
its own signature.
"""

from __future__ import annotations

import asyncio
import logging
import time
from types import SimpleNamespace
from typing import Any, cast

import pytest
import zenoh
from pydantic import BaseModel

from zenode import pubsub
from zenode.envelope import Envelope, encode_envelope
from zenode.errors import ContractError
from zenode.pubsub import Publisher, Subscription, _handler_arity
from zenode.topic import Topic


class Ping(BaseModel):
    value: int = 0


PING = Topic("unit/ping", Ping)
FRESH = Topic("unit/fresh", Ping, max_age=0.5)
BRIEF = Topic("unit/brief", Ping, max_age=0.05)
"""Short enough that a sample can go stale while sitting in the queue."""


class FakeInner:
    """Stands in for ``zenoh.Publisher``; records what was handed to zenoh."""

    def __init__(self) -> None:
        self.puts: list[tuple[bytes, bytes | None]] = []
        self.undeclared = False

    def put(self, payload: bytes, attachment: bytes | None = None) -> None:
        self.puts.append((payload, attachment))

    def undeclare(self) -> None:
        self.undeclared = True


def zenoh_sample(payload: bytes, attachment: bytes | None = None, kind=zenoh.SampleKind.PUT) -> Any:
    return SimpleNamespace(
        kind=kind,
        payload=SimpleNamespace(to_bytes=lambda: payload),
        attachment=None if attachment is None else SimpleNamespace(to_bytes=lambda: attachment),
    )


def make_subscription(handler, *, topic: Topic[Ping] = PING, **kwargs) -> Subscription[Ping]:
    return Subscription(topic, topic.key, handler, asyncio.get_running_loop(), **kwargs)


def make_armed(handler, *, topic: Topic[Ping] = PING, **kwargs) -> Subscription[Ping]:
    """A subscription with its deadline timer running, as ``_attach`` would."""
    sub = make_subscription(handler, topic=topic, **kwargs)
    sub._arm_deadline()
    return sub


# ---------------------------------------------------------------------- arity


class _Handlers:
    def two_args(self, msg, envelope) -> None: ...


class _CallableHandler:
    def __call__(self, msg) -> None: ...


class _Unintrospectable:
    def __call__(self, msg) -> None: ...

    @property
    def __signature__(self):
        raise ValueError("this callable refuses to describe itself")


@pytest.mark.parametrize(
    "handler,expected",
    [
        pytest.param(lambda msg: None, 1, id="msg-only"),
        pytest.param(lambda msg, envelope: None, 2, id="msg-and-envelope"),
        pytest.param(lambda msg, envelope, extra: None, 2, id="clamped-to-two"),
        pytest.param(lambda: None, 1, id="clamped-to-one"),
        pytest.param(lambda *args: None, 1, id="varargs"),
        pytest.param(lambda msg, *, flag=False: None, 1, id="keyword-only-ignored"),
        pytest.param(_Handlers().two_args, 2, id="bound-method-skips-self"),
        pytest.param(_CallableHandler(), 1, id="callable-object"),
        pytest.param(_Unintrospectable(), 1, id="no-signature-defaults-to-one"),
    ],
)
def test_handler_arity(handler, expected):
    assert _handler_arity(handler) == expected


async def test_envelope_is_passed_to_two_argument_handlers():
    seen: list[Envelope] = []
    sub = make_subscription(lambda msg, envelope: seen.append(envelope))
    await sub._dispatch(Ping(value=1), Envelope(node="talker", seq=4))
    assert seen == [Envelope(node="talker", seq=4)]


async def test_async_handlers_are_awaited():
    seen: list[int] = []

    async def handler(msg: Ping) -> None:
        await asyncio.sleep(0)
        seen.append(msg.value)

    sub = make_subscription(handler)
    await sub._dispatch(Ping(value=7), Envelope())
    assert seen == [7]


async def test_a_raising_handler_is_counted_and_still_timed():
    """A handler that fails after 5 s still cost the loop 5 s."""

    def handler(msg: Ping) -> None:
        raise ValueError("bad pose")

    sub = make_subscription(handler)
    await sub._dispatch(Ping(), Envelope())
    assert sub.errors == 1
    assert sub.handler_time.count == 1


# ----------------------------------------------------------------- validation


@pytest.mark.parametrize("mode", ["newest", "", "LATEST"])
async def test_unknown_subscription_mode_is_rejected(mode):
    with pytest.raises(ValueError, match="unknown subscription mode"):
        make_subscription(print, mode=mode)


@pytest.mark.parametrize("size", [0, -1])
async def test_queue_size_must_hold_at_least_one_sample(size):
    with pytest.raises(ValueError, match="queue_size must be >= 1"):
        make_subscription(print, queue_size=size)


# ------------------------------------------------------------------ delivery


async def test_queue_mode_drops_the_oldest_when_full():
    """Backpressure is explicit: the freshest samples survive, drops are counted."""
    sub = make_subscription(print, queue_size=2)
    for value in (1, 2, 3):
        sub._push(str(value).encode(), None)

    assert sub.received == 3
    assert sub.dropped == 1
    assert [payload for payload, _ in (await sub._next(), await sub._next())] == [b"2", b"3"]


async def test_latest_mode_keeps_only_the_newest():
    sub = make_subscription(print, mode="latest")
    sub._push(b"old", None)
    sub._push(b"new", None)

    assert sub.received == 2
    assert sub.dropped == 0  # nothing is "dropped": latest mode is not a queue
    assert await sub._next() == (b"new", Envelope())


async def test_samples_arriving_after_stop_are_ignored():
    """Shutdown race: zenoh may still be delivering while we tear down."""
    sub = make_subscription(print)
    await sub.stop()
    sub._push(b"late", None)
    assert sub.received == 0


async def test_non_put_samples_are_ignored():
    """Deletes are a key-space event, not a message on the topic."""
    sub = make_subscription(print)
    sub._zenoh_callback(zenoh_sample(b"{}", kind=zenoh.SampleKind.DELETE))
    await asyncio.sleep(0)
    assert sub.received == 0


async def test_the_zenoh_callback_hands_payload_and_envelope_to_the_loop():
    """The attachment is decoded on arrival, so the queue carries an Envelope."""
    sub = make_subscription(print)
    attachment = encode_envelope("talker", 1, time.time_ns())
    sub._zenoh_callback(zenoh_sample(b'{"value":1}', attachment))
    await asyncio.sleep(0)  # let call_soon_threadsafe run

    payload, envelope = await sub._next()
    assert payload == b'{"value":1}'
    assert (envelope.node, envelope.seq) == ("talker", 1)


async def test_the_envelope_is_decoded_once_per_sample(monkeypatch: pytest.MonkeyPatch):
    """The queue carries the decoded Envelope so _consume never re-parses it."""
    calls = 0
    real = pubsub.decode_envelope

    def counting(data: bytes | None) -> Envelope:
        nonlocal calls
        calls += 1
        return real(data)

    monkeypatch.setattr(pubsub, "decode_envelope", counting)
    received: list[Ping] = []
    sub = make_subscription(received.append)
    task = asyncio.create_task(sub._consume())
    sub._push(
        Ping(value=1).model_dump_json().encode(),
        encode_envelope("talker", 1, time.time_ns()),
    )
    await asyncio.sleep(0.02)
    task.cancel()

    assert received == [Ping(value=1)]
    assert calls == 1


# --------------------------------------------------------------------- stale


@pytest.mark.parametrize(
    "topic,age,expected",
    [
        pytest.param(PING, 99.0, False, id="no-max-age-never-stale"),
        pytest.param(FRESH, None, False, id="unknown-age-is-not-stale"),
        pytest.param(FRESH, 0.1, False, id="within-max-age"),
        pytest.param(FRESH, 0.6, True, id="beyond-max-age"),
    ],
)
async def test_staleness(topic, age, expected):
    sub = make_subscription(print, topic=topic)
    assert sub._is_stale(age) is expected


async def test_stale_samples_are_still_counted_as_latency():
    """Dropping a late message must not hide the delay that made it late."""
    received: list[Ping] = []
    sub = make_subscription(received.append, topic=FRESH)
    task = asyncio.create_task(sub._consume())
    sub._push(
        Ping(value=1).model_dump_json().encode(),
        encode_envelope("past", 1, time.time_ns() - 10_000_000_000),
    )
    await asyncio.sleep(0.02)
    task.cancel()

    assert sub.stale == 1
    assert received == []
    assert sub.age.max_s > 9.0


async def test_a_stale_sample_never_reaches_the_queue():
    """A stale burst must not evict good samples on its way to being dropped."""
    sub = make_subscription(print, topic=FRESH, queue_size=2)
    old = time.time_ns() - 10_000_000_000
    for seq in range(5):
        sub._push(Ping(value=seq).model_dump_json().encode(), encode_envelope("past", seq, old))
    sub._push(Ping(value=99).model_dump_json().encode(), encode_envelope("now", 99, time.time_ns()))

    assert sub.stale == 5
    assert sub.dropped == 0  # the queue never filled, so nothing was evicted
    assert sub._queue.qsize() == 1


async def test_a_stale_burst_does_not_clobber_the_last_good_latest_value():
    sub = make_subscription(print, topic=FRESH, mode="latest")
    good = Ping(value=7).model_dump_json().encode()
    sub._push(good, encode_envelope("now", 1, time.time_ns()))
    old = time.time_ns() - 10_000_000_000
    for seq in range(3):
        sub._push(Ping(value=seq).model_dump_json().encode(), encode_envelope("past", seq, old))

    payload, _ = await sub._next()
    assert payload == good
    assert sub.stale == 3


async def test_a_sample_that_ages_past_max_age_while_queued_is_dropped_at_dequeue():
    """Fresh on arrival, too old by the time the handler reached it."""
    received: list[Ping] = []
    sub = make_subscription(received.append, topic=BRIEF)
    sub._push(
        Ping(value=1).model_dump_json().encode(),
        encode_envelope("talker", 1, time.time_ns()),
    )
    assert sub._queue.qsize() == 1  # it passed the arrival stage
    await asyncio.sleep(0.06)  # ...and went stale sitting there

    task = asyncio.create_task(sub._consume())
    await asyncio.sleep(0.02)
    task.cancel()

    assert received == []
    assert sub.stale == 1
    assert (sub._stale_arrival, sub._stale_queued) == (0, 1)


async def test_the_two_stale_stages_name_different_causes(caplog: pytest.LogCaptureFixture):
    """Arrival-stale is the sender's clock; dequeue-stale is this node lagging.

    One shared message would send you after the wrong bug half the time.
    """
    with caplog.at_level(logging.WARNING, logger="zenode.pubsub"):
        arrival = make_subscription(print, topic=FRESH)
        arrival._push(
            Ping(value=1).model_dump_json().encode(),
            encode_envelope("past", 1, time.time_ns() - 10_000_000_000),
        )

        queued = make_subscription(print, topic=BRIEF)
        queued._push(
            Ping(value=1).model_dump_json().encode(),
            encode_envelope("talker", 1, time.time_ns()),
        )
        await asyncio.sleep(0.06)
        task = asyncio.create_task(queued._consume())
        await asyncio.sleep(0.02)
        task.cancel()

    messages = [r.message for r in caplog.records if "max_age" in r.message]
    assert len(messages) == 2
    assert any("on arrival" in m and "clock sync (NTP)" in m for m in messages)
    assert any("while queued" in m and "this node is behind" in m for m in messages)


async def _push_stale(sub: Subscription[Ping], count: int = 1) -> None:
    task = asyncio.create_task(sub._consume())
    for seq in range(count):
        sub._push(
            Ping(value=seq).model_dump_json().encode(),
            encode_envelope("past", seq, time.time_ns() - 10_000_000_000),
        )
    await asyncio.sleep(0.02)
    task.cancel()


async def test_the_first_stale_drop_is_warned_about(caplog: pytest.LogCaptureFixture):
    """A counter nobody reads is silence: an off-by-400ms clock drops everything."""
    sub = make_subscription(print, topic=FRESH)
    with caplog.at_level(logging.WARNING, logger="zenode.pubsub"):
        await _push_stale(sub)

    warnings = [r for r in caplog.records if "max_age" in r.message]
    assert len(warnings) == 1
    assert "clock sync" in warnings[0].message  # names the actual suspect
    assert getattr(warnings[0], "key", None) == FRESH.key


async def test_further_stale_drops_are_rate_limited(caplog: pytest.LogCaptureFixture):
    """Loud once, then quiet — a mismatched clock must not become the log."""
    sub = make_subscription(print, topic=FRESH)
    with caplog.at_level(logging.WARNING, logger="zenode.pubsub"):
        await _push_stale(sub, count=25)

    assert sub.stale == 25
    assert len([r for r in caplog.records if "max_age" in r.message]) == 1


async def test_a_malformed_payload_is_counted_not_fatal(caplog: pytest.LogCaptureFixture):
    received: list[Ping] = []
    sub = make_subscription(received.append)
    task = asyncio.create_task(sub._consume())
    with caplog.at_level(logging.WARNING, logger="zenode.pubsub"):
        sub._push(b"not json at all", None)
        sub._push(Ping(value=3).model_dump_json().encode(), None)
        await asyncio.sleep(0.02)
    task.cancel()

    assert received == [Ping(value=3)]  # the subscription kept going
    assert sub.errors == 1
    assert any("malformed payload" in r.message for r in caplog.records)


# ------------------------------------------------------------------ deadline
#
# Most of these drive ``_check_deadline()`` directly and cost no wall time:
# ``_last_arrival -= 1.0`` is "pretend a second of silence just passed".


async def test_without_a_deadline_nothing_is_scheduled():
    sub = make_subscription(print)
    sub._arm_deadline()

    assert sub._handle is None
    assert sub.silent is False
    assert sub.silent_for == 0.0


async def test_a_pending_check_reschedules_when_data_arrived():
    """The lazy timer: a wake-up that finds fresh data re-arms for what is left."""
    sub = make_armed(print, deadline=0.3)
    sub._check_deadline()

    assert sub.silent is False
    assert sub._handle is not None


async def test_silence_is_entered_once_not_repeatedly():
    """Edge-triggered: the reaction is latching, so re-firing it is noise."""
    calls: list[float] = []
    sub = make_armed(print, deadline=0.3)
    sub._add_silence_hook(calls.append)

    sub._last_arrival -= 1.0
    for _ in range(3):
        sub._check_deadline()
    await asyncio.sleep(0)

    assert sub.deadline_misses == 1
    assert len(calls) == 1


async def test_arrival_clears_silence_immediately():
    resumed: list[float] = []
    sub = make_armed(print, deadline=0.3)
    sub._add_resume_hook(resumed.append)
    sub._last_arrival -= 1.0
    sub._check_deadline()
    assert sub.silent is True
    assert sub.silent_for > 0.9

    sub._push(Ping(value=1).model_dump_json().encode(), None)
    await asyncio.sleep(0)

    assert sub.silent is False
    assert sub.silent_for == 0.0
    assert len(resumed) == 1 and resumed[0] > 0.9
    assert sub.deadline_misses == 1  # resuming is not a second miss


async def test_a_stale_sample_does_not_satisfy_the_deadline():
    """A skewed producer must not look like a healthy one.

    Its samples arrive at full rate and ``max_age`` discards every one, so a
    deadline they satisfied would leave the consumer believing data flows while
    its handler has not run in minutes.
    """
    sub = make_armed(print, topic=FRESH, deadline=0.3)
    sub._last_arrival -= 1.0
    sub._push(
        Ping(value=1).model_dump_json().encode(),
        encode_envelope("past", 1, time.time_ns() - 10_000_000_000),
    )
    sub._check_deadline()

    assert sub.stale == 1
    assert sub.silent is True


async def test_a_malformed_payload_still_satisfies_the_deadline():
    """The accepted hole: payload decode happens after the deadline is stamped.

    A producer emitting garbage keeps the deadline satisfied; it surfaces as
    ``errors`` instead. That is a deploy-time schema mismatch — loud on the
    first message — rather than a condition that develops on a moving robot the
    way clock drift does.
    """
    sub = make_armed(print, deadline=0.3)
    sub._last_arrival -= 1.0
    sub._push(b"not json at all", None)
    sub._check_deadline()

    assert sub.silent is False


@pytest.mark.parametrize("bad", [0, -1.0, float("inf"), float("nan"), True])
async def test_a_deadline_must_be_positive_and_finite(bad):
    with pytest.raises(ContractError, match="deadline must be positive"):
        make_subscription(print, deadline=bad)


async def test_an_unknown_on_deadline_policy_is_rejected():
    with pytest.raises(ContractError, match="on_deadline must be"):
        make_subscription(print, deadline=0.3, on_deadline="explode")


async def test_a_policy_without_a_deadline_is_rejected():
    """Otherwise the typo sits there doing nothing, forever, silently."""
    with pytest.raises(ContractError, match="no deadline"):
        make_subscription(print, on_deadline="stop")


async def test_on_deadline_stop_calls_the_node_stop_callback():
    stopped: list[bool] = []
    sub = make_armed(print, deadline=0.3, on_deadline="stop", stop=lambda: stopped.append(True))

    sub._last_arrival -= 1.0
    sub._check_deadline()

    assert stopped == [True]


async def test_a_callable_policy_receives_the_silence_duration():
    seen: list[float] = []
    sub = make_armed(print, deadline=0.3, on_deadline=seen.append)

    sub._last_arrival -= 1.0
    sub._check_deadline()
    await asyncio.sleep(0)

    assert len(seen) == 1 and seen[0] > 0.9


async def test_the_first_silence_says_nothing_ever_arrived(caplog: pytest.LogCaptureFixture):
    """A mistyped key and a dead producer read identically until you say so."""
    sub = make_armed(print, deadline=0.3)
    with caplog.at_level(logging.WARNING, logger="zenode.pubsub"):
        sub._last_arrival -= 1.0
        sub._check_deadline()

    assert any("never received any data" in r.message for r in caplog.records)


async def test_silence_after_traffic_says_how_long(caplog: pytest.LogCaptureFixture):
    sub = make_armed(print, deadline=0.3)
    sub._push(Ping(value=1).model_dump_json().encode(), None)
    with caplog.at_level(logging.WARNING, logger="zenode.pubsub"):
        sub._last_arrival -= 1.0
        sub._check_deadline()

    assert any("no data on" in r.message for r in caplog.records)
    assert not any("never received" in r.message for r in caplog.records)


async def test_repeated_silence_warnings_are_throttled(caplog: pytest.LogCaptureFixture):
    """Loud once, then quiet — a dead producer must not become the log."""
    sub = make_armed(print, deadline=0.3)
    with caplog.at_level(logging.WARNING, logger="zenode.pubsub"):
        sub._last_arrival -= 1.0
        for _ in range(5):
            sub._check_deadline()

    assert len([r for r in caplog.records if "deadline" in r.message]) == 1


async def test_a_raising_silence_hook_is_counted_not_fatal(caplog: pytest.LogCaptureFixture):
    def boom(silent_for: float) -> None:
        raise RuntimeError("bad")

    sub = make_armed(print, deadline=0.3)
    sub._add_silence_hook(boom)
    with caplog.at_level(logging.ERROR, logger="zenode.pubsub"):
        sub._last_arrival -= 1.0
        sub._check_deadline()
        await asyncio.sleep(0)

    assert sub.errors == 1
    assert sub.silent is True
    assert any("silence handler raised" in r.message for r in caplog.records)


async def test_a_hook_attached_while_already_silent_fires_immediately():
    """Bindings wire after ``on_start``, so a slow start must not lose the edge."""
    calls: list[float] = []
    sub = make_armed(print, deadline=0.3)
    sub._last_arrival -= 1.0
    sub._check_deadline()
    assert sub.silent is True

    sub._add_silence_hook(calls.append)
    await asyncio.sleep(0)

    assert len(calls) == 1


async def test_stop_cancels_the_deadline_timer():
    sub = make_armed(print, deadline=0.3)
    await sub.stop()
    assert sub._handle is None

    sub._check_deadline()  # a wake-up already queued when we stopped

    assert sub.silent is False
    assert sub.deadline_misses == 0


async def test_the_deadline_timer_fires_and_rearms_on_its_own():
    """On the real clock: trip, resume, trip again.

    The second trip only lands this fast if the resume edge re-armed to
    ``deadline`` rather than leaving the slow while-silent schedule in place.
    """
    sub = make_armed(print, deadline=0.02)
    await asyncio.sleep(0.05)
    assert sub.silent is True
    assert sub.deadline_misses == 1

    sub._push(Ping(value=1).model_dump_json().encode(), None)
    await asyncio.sleep(0)
    assert sub.silent is False

    await asyncio.sleep(0.05)
    assert sub.silent is True
    assert sub.deadline_misses == 2

    await sub.stop()


# ----------------------------------------------------------------- publisher


def make_publisher() -> tuple[Publisher[Ping], FakeInner]:
    inner = FakeInner()
    return Publisher(cast(Any, inner), topic=PING, key=PING.key, node_name="talker"), inner


def test_put_encodes_the_payload_with_the_topic_codec():
    pub, inner = make_publisher()
    pub.put(Ping(value=5))
    assert inner.puts[0][0] == b'{"value":5}'


def test_put_stamps_an_incrementing_sequence_number():
    """Gaps in ``seq`` are how a subscriber notices it missed something."""
    from zenode.envelope import decode_envelope

    pub, inner = make_publisher()
    for _ in range(3):
        pub.put(Ping())

    seqs = [decode_envelope(attachment).seq for _, attachment in inner.puts]
    assert seqs == [1, 2, 3]
    assert pub.sent == 3


def test_put_identifies_the_sending_node():
    from zenode.envelope import decode_envelope

    pub, inner = make_publisher()
    pub.put(Ping())
    assert decode_envelope(inner.puts[0][1]).node == "talker"


def test_undeclare_reaches_zenoh():
    pub, inner = make_publisher()
    pub.undeclare()
    assert inner.undeclared


def test_undeclare_survives_a_transport_that_is_already_gone():
    """Teardown ordering is not guaranteed; undeclare must never raise."""

    class Broken(FakeInner):
        def undeclare(self) -> None:
            raise RuntimeError("session closed")

    Publisher(cast(Any, Broken()), topic=PING, key=PING.key, node_name="talker").undeclare()


def test_matching_is_optimistic_for_non_plain_publishers():
    """Latched publishers keep their cache warm, so they always report matching."""
    pub, _inner = make_publisher()
    assert pub.matching is True


@pytest.mark.integration
async def test_matching_reports_whether_anyone_is_listening():
    """The whole value of this property is saying *no*.

    zenoh returns a ``MatchingStatus`` object, which is truthy whatever it
    says — so unwrapping the wrong thing turns "skip the expensive payload"
    into a constant yes and the optimization silently never fires.
    """
    from zenode.testing import harness

    lonely = Topic("unit/lonely", Ping)
    async with harness() as h:
        pub = h.publisher(lonely)
        for _ in range(20):
            if not pub.matching:
                break
            await asyncio.sleep(0.02)
        assert pub.matching is False

        h.collect(lonely)
        for _ in range(50):
            if pub.matching:
                break
            await asyncio.sleep(0.02)
        assert pub.matching is True


def test_on_matching_refuses_non_plain_publishers():
    """Latched means always matching, so the falling edge would never arrive.

    A gate whose closing edge cannot happen is worse than no gate: it looks
    wired up while the camera runs forever.
    """
    pub, _inner = make_publisher()
    with pytest.raises(ContractError, match="latched"):
        pub.on_matching(lambda matching: None)


async def _wait_for(predicate: Any, what: str, timeout: float = 3.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"timed out waiting for {what}")


@pytest.mark.integration
async def test_on_matching_reports_the_current_state_then_each_edge():
    """The seed matters as much as the edges.

    zenoh only reports a *change*, so a publisher declared with nobody
    listening never hears anything — a hook that only saw edges would leave the
    node guessing where it started.
    """
    from zenode.testing import harness

    watched = Topic("unit/watched", Ping)
    seen: list[bool] = []
    async with harness() as h:
        pub = h.publisher(watched)
        pub.on_matching(seen.append)
        await _wait_for(lambda: seen == [False], "the initial state")

        sub = h.subscribe(watched, lambda msg: None)
        await _wait_for(lambda: seen == [False, True], "the rising edge")

        await sub.stop()
        await _wait_for(lambda: seen == [False, True, False], "the falling edge")


@pytest.mark.integration
async def test_a_second_matching_hook_learns_the_state_it_missed():
    """Registered late — after ``on_start`` opened hardware — it still gets told."""
    from zenode.testing import harness

    watched = Topic("unit/watched_late", Ping)
    async with harness() as h:
        pub = h.publisher(watched)
        first: list[bool] = []
        pub.on_matching(first.append)
        h.subscribe(watched, lambda msg: None)
        await _wait_for(lambda: first == [False, True], "the first hook to see a subscriber")

        late: list[bool] = []
        pub.on_matching(late.append)
        await _wait_for(lambda: late == [True], "the late hook to be seeded")

        # A second subscriber is not a second edge: the hook gates work that is
        # already running, and re-firing would restart it.
        h.subscribe(watched, lambda msg: None)
        await asyncio.sleep(0.2)
        assert late == [True]


@pytest.mark.integration
async def test_a_raising_matching_hook_is_counted_not_fatal():
    from zenode.testing import harness

    watched = Topic("unit/watched_raises", Ping)

    def boom(matching: bool) -> None:
        raise RuntimeError("camera is on fire")

    async with harness() as h:
        pub = h.publisher(watched)
        pub.on_matching(boom)
        await _wait_for(lambda: pub.errors == 1, "the raising hook to be counted")
        h.subscribe(watched, lambda msg: None)
        await _wait_for(lambda: pub.errors == 2, "the rising edge to reach it anyway")
