"""The per-node flight recorder behind `zenode trace`."""

import asyncio
import time

import pytest
from pydantic import BaseModel

from zenode import Node, Topic, subscribe, trace
from zenode.msgs.trace import Hop, TraceQuery, trace_key, trace_pattern
from zenode.testing import harness
from zenode.trace import TraceRing


class Frame(BaseModel):
    n: int = 0


class Detection(BaseModel):
    n: int = 0


CAMERA = Topic("test/ring/camera", Frame, trace=True)
DETECTIONS = Topic("test/ring/detections", Detection)
UNSAMPLED = Topic("test/ring/rare", Frame, trace=True, trace_ratio=0.0)


def _record(ring: TraceRing, traceparent: str | None, *, node: str = "nav", key: str = "k") -> None:
    ring.record(
        node=node,
        key=key,
        traceparent=traceparent,
        envelope_node="camera",
        seq=1,
        ts_ns=123,
        age_ms=1.5,
        handler_ms=2.5,
    )


async def _wait_for_hops(node: Node, trace_id: str, *, timeout: float = 2.0) -> list[Hop]:
    """This node's hops for ``trace_id``, once it has actually handled one."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        hops = node._answer_trace(TraceQuery(trace_id=trace_id)).hops
        if hops:
            return hops
        await asyncio.sleep(0.02)
    return []


# ---------------------------------------------------------------------- keys


def test_trace_pattern_matches_every_node():
    assert trace_key("nav") == "node/nav/trace"
    assert trace_pattern("robodog") == "robodog/node/*/trace"


# ---------------------------------------------------------------------- ring


def test_records_and_returns_by_trace_id():
    ring = TraceRing(8)
    traceparent = trace.new_traceparent()
    _record(ring, traceparent)

    trace_id = trace.trace_id_of(traceparent)
    assert trace_id is not None
    hops = ring.hops(trace_id)
    assert len(hops) == 1
    assert hops[0].node == "nav"
    assert hops[0].source == "camera"
    assert hops[0].age_ms == 1.5
    assert hops[0].handler_ms == 2.5
    assert hops[0].span_id == traceparent.split("-")[2]


def test_other_traces_are_not_returned():
    ring = TraceRing(8)
    _record(ring, trace.new_traceparent())
    other = trace.trace_id_of(trace.new_traceparent())
    assert other is not None
    assert ring.hops(other) == []


def test_untraced_messages_are_not_recorded():
    ring = TraceRing(8)
    _record(ring, None)
    _record(ring, "garbage")
    assert len(ring) == 0


def test_unsampled_traces_are_not_recorded():
    """Otherwise trace_ratio would only be a saving on paper."""
    ring = TraceRing(8)
    _record(ring, trace.new_traceparent(sampled=False))
    assert len(ring) == 0


def test_ring_is_bounded():
    ring = TraceRing(4)
    for _ in range(50):
        _record(ring, trace.new_traceparent())
    assert len(ring) == 4


def test_capacity_zero_means_the_node_builds_no_ring():
    class Ringless(Node):
        name = "ringless"
        trace_ring = 0

    assert Ringless()._ring is None


# --------------------------------------------------------------- end-to-end


@pytest.mark.integration
async def test_a_trace_is_assembled_from_every_node_that_saw_it():
    class Detector(Node):
        name = "ring-detector"
        health_interval = None

        async def on_start(self) -> None:
            self.out = self.publisher(DETECTIONS)

        @subscribe(CAMERA)
        async def on_frame(self, msg: Frame) -> None:
            self.out.put(Detection(n=msg.n))

    class Planner(Node):
        name = "ring-planner"
        health_interval = None

        @subscribe(DETECTIONS)
        async def on_detection(self, msg: Detection) -> None:
            pass

    async with harness() as h:
        detector = await h.start_node(Detector)
        planner = await h.start_node(Planner)
        out = h.collect(DETECTIONS)
        h.publisher(CAMERA).put(Frame(n=3))
        await out.next()

        trace_id = trace.trace_id_of(out.envelopes[0].traceparent)
        assert trace_id is not None

        # What `zenode trace` does, without the CLI's session. Polled, not read
        # once: `out.next()` only says the *collector* saw the detection, and
        # the planner is a separate subscriber that may not have run yet.
        detector_hops = await _wait_for_hops(detector, trace_id)
        planner_hops = await _wait_for_hops(planner, trace_id)

    assert [h.key for h in detector_hops] == ["test/ring/camera"]
    assert detector_hops[0].source == "zenode-test-probe"
    assert [h.key for h in planner_hops] == ["test/ring/detections"]
    assert planner_hops[0].source == "ring-detector"


@pytest.mark.integration
async def test_unsampled_pipeline_leaves_the_rings_empty():
    class Watcher(Node):
        name = "ring-watcher"
        health_interval = None

        @subscribe(UNSAMPLED)
        async def on_frame(self, msg: Frame) -> None:
            pass

    async with harness() as h:
        watcher = await h.start_node(Watcher)
        out = h.collect(UNSAMPLED)
        h.publisher(UNSAMPLED).put(Frame(n=1))
        await out.next()
        trace_id = trace.trace_id_of(out.envelopes[0].traceparent)

    assert trace_id is not None  # still correlatable in logs
    assert watcher._answer_trace(TraceQuery(trace_id=trace_id)).hops == []
