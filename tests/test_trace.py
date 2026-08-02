"""W3C trace context: format, propagation across nodes, and log injection."""

import asyncio
import logging

import pytest
from pydantic import BaseModel

from zenode import Envelope, Node, Service, Topic, every, serve, subscribe, trace
from zenode.testing import harness


class Frame(BaseModel):
    n: int = 0


class Detection(BaseModel):
    n: int = 0


class SumRequest(BaseModel):
    value: int = 0


class SumReply(BaseModel):
    doubled: int = 0


CAMERA = Topic("test/camera", Frame, trace=True)  # trace root
DETECTIONS = Topic("test/detections", Detection)  # continues the trace
UNTRACED = Topic("test/untraced", Frame)
DOUBLE = Service("test/double", request=SumRequest, reply=SumReply)


# ------------------------------------------------------------------- format


def test_new_traceparent_is_w3c():
    version, trace_id, span_id, flags = trace.new_traceparent().split("-")
    assert version == "00"
    assert len(trace_id) == 32
    assert len(span_id) == 16
    assert flags == "01"
    assert int(trace_id, 16) >= 0  # hex


def test_new_traceparent_is_unique():
    assert trace.new_traceparent() != trace.new_traceparent()


def test_trace_id_of_tolerates_garbage():
    assert trace.trace_id_of(None) is None
    assert trace.trace_id_of("") is None
    assert trace.trace_id_of("not-a-traceparent") is None
    assert trace.trace_id_of("00-tooshort-abc-01") is None


def test_trace_id_of_rejects_what_w3c_calls_invalid():
    """An all-zero id is well-formed and unfindable; non-hex is neither."""
    assert trace.trace_id_of(f"00-{'0' * 32}-{'a' * 16}-01") is None
    assert trace.trace_id_of(f"00-{'a' * 32}-{'0' * 16}-01") is None
    assert trace.trace_id_of(f"00-{'z' * 32}-{'a' * 16}-01") is None
    assert trace.trace_id_of(f"00-{'a' * 32}-{'a' * 16}-0") is None
    assert trace.trace_id_of(f"00-{'a' * 32}-{'a' * 16}-01") == "a" * 32


def test_outgoing_passes_the_span_id_through_without_a_recorder():
    """No spans exist, so zenode must not invent an id for one."""
    parent = trace.new_traceparent()
    with trace.using(parent):
        assert trace.outgoing() == parent


# ----------------------------------------------------------------- sampling


def test_sampled_flag_round_trips():
    assert trace.sampled_of(trace.new_traceparent(sampled=True))
    assert not trace.sampled_of(trace.new_traceparent(sampled=False))
    assert not trace.sampled_of(None)
    assert not trace.sampled_of("garbage")


def test_ratio_of_one_always_samples():
    assert all(trace.sampled_of(trace.root_traceparent(1.0)) for _ in range(50))


def test_ratio_of_zero_never_samples():
    assert not any(trace.sampled_of(trace.root_traceparent(0.0)) for _ in range(50))


def test_ratio_samples_roughly_that_fraction():
    sampled = sum(trace.sampled_of(trace.root_traceparent(0.25)) for _ in range(4000))
    assert 800 < sampled < 1200  # 25% of 4000, generous bounds against flakiness


def test_unsampled_traces_still_have_an_id_to_correlate_by():
    """The whole point of a ratio: correlation stays at full rate, spans do not."""
    unsampled = trace.root_traceparent(0.0)
    assert trace.trace_id_of(unsampled) is not None
    record = logging.LogRecord("t", logging.INFO, "f.py", 1, "m", None, None)
    with trace.using(unsampled):
        trace.TraceContextFilter().filter(record)
    assert record.__dict__["trace"] == trace.trace_id_of(unsampled)


# -------------------------------------------------------------- contextvar


def test_outgoing_is_none_when_untraced():
    assert trace.outgoing() is None


def test_outgoing_continues_an_active_trace():
    parent = trace.new_traceparent()
    with trace.using(parent):
        assert trace.trace_id_of(trace.outgoing()) == trace.trace_id_of(parent)


def test_using_restores_previous():
    assert trace.current() is None
    with trace.using("outer"):
        with trace.using("inner"):
            assert trace.current() == "inner"
        assert trace.current() == "outer"
    assert trace.current() is None


async def test_context_is_per_task():
    """Concurrent handlers must not see each other's trace."""
    seen: dict[str, str | None] = {}

    async def worker(name: str) -> None:
        with trace.using(name):
            await asyncio.sleep(0.01)
            seen[name] = trace.current()

    await asyncio.gather(worker("a"), worker("b"))
    assert seen == {"a": "a", "b": "b"}


# ------------------------------------------------------------------ logging


def test_filter_injects_trace_id():
    record = logging.LogRecord("t", logging.INFO, "f.py", 1, "m", None, None)
    parent = trace.new_traceparent()
    with trace.using(parent):
        trace.TraceContextFilter().filter(record)
    assert record.__dict__["trace"] == trace.trace_id_of(parent)


def test_filter_leaves_untraced_records_clean():
    record = logging.LogRecord("t", logging.INFO, "f.py", 1, "m", None, None)
    trace.TraceContextFilter().filter(record)
    assert "trace" not in record.__dict__


# -------------------------------------------------------------- end-to-end


@pytest.mark.integration
async def test_root_topic_stamps_traceparent():
    async with harness() as h:
        out = h.collect(CAMERA)
        h.publisher(CAMERA).put(Frame(n=1))
        await out.next()
        assert trace.trace_id_of(out.envelopes[0].traceparent) is not None


@pytest.mark.integration
async def test_untraced_topic_carries_nothing():
    async with harness() as h:
        out = h.collect(UNTRACED)
        h.publisher(UNTRACED).put(Frame(n=1))
        await out.next()
        assert out.envelopes[0].traceparent is None


@pytest.mark.integration
async def test_trace_survives_a_node_hop():
    """camera -> detector -> detections keeps one trace id across processes."""

    class Detector(Node):
        name = "detector"
        health_interval = None

        async def on_start(self) -> None:
            self.out = self.publisher(DETECTIONS)

        @subscribe(CAMERA)
        async def on_frame(self, msg: Frame) -> None:
            self.out.put(Detection(n=msg.n))

    async with harness() as h:
        await h.start_node(Detector)
        out = h.collect(DETECTIONS)
        h.publisher(CAMERA).put(Frame(n=7))
        await out.next()
        # The detector never mentions tracing; the context rode along.
        assert trace.trace_id_of(out.envelopes[0].traceparent) is not None


@pytest.mark.integration
async def test_service_call_joins_the_callers_trace():
    seen: list[str | None] = []

    class Server(Node):
        name = "doubler"
        health_interval = None

        @serve(DOUBLE)
        async def on_double(self, req: SumRequest) -> SumReply:
            seen.append(trace.current())
            return SumReply(doubled=req.value * 2)

    async with harness() as h:
        await h.start_node(Server)
        parent = trace.new_traceparent()
        with trace.using(parent):
            reply = await h.call(DOUBLE, SumRequest(value=21))
        assert reply.doubled == 42
        assert trace.trace_id_of(seen[0]) == trace.trace_id_of(parent)


@pytest.mark.integration
async def test_handler_runs_inside_the_incoming_trace():
    seen: list[str | None] = []

    async with harness() as h:

        def handler(msg: Frame, env: Envelope) -> None:
            seen.append(trace.current())

        h.subscribe(CAMERA, handler)
        h.publisher(CAMERA).put(Frame(n=1))
        for _ in range(40):
            if seen:
                break
            await asyncio.sleep(0.05)
        assert trace.trace_id_of(seen[0]) is not None


@pytest.mark.integration
async def test_timer_publish_starts_a_new_trace_each_tick():
    class Camera(Node):
        name = "camera"
        health_interval = None

        async def on_start(self) -> None:
            self.frames = self.publisher(CAMERA)
            self.n = 0

        @every(0.05)
        async def tick(self) -> None:
            self.n += 1
            self.frames.put(Frame(n=self.n))

    async with harness() as h:
        out = h.collect(CAMERA)
        await h.start_node(Camera)
        await out.next()
        await out.next()
        ids = {trace.trace_id_of(e.traceparent) for e in out.envelopes[:2]}
        assert None not in ids
        assert len(ids) == 2  # each tick is its own trace
