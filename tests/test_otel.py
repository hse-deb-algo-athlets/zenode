"""The optional OpenTelemetry bridge: spans, and the ids that connect them.

The property under test throughout is F9's: the span id zenode puts on the wire
must name a span that a backend can resolve, so a multi-hop pipeline assembles
into a chain rather than a star of orphans.
"""

import asyncio

import pytest
from opentelemetry import trace as otel_api
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from pydantic import BaseModel

from zenode import Node, Service, Topic, otel, serve, subscribe, trace
from zenode.testing import harness


class Frame(BaseModel):
    n: int = 0


class Detection(BaseModel):
    n: int = 0


class Ping(BaseModel):
    value: int = 0


CAMERA = Topic("test/otel/camera", Frame, trace=True)
DETECTIONS = Topic("test/otel/detections", Detection)
UNTRACED = Topic("test/otel/untraced", Frame)
NEVER_SAMPLED = Topic("test/otel/rare", Frame, trace=True, trace_ratio=0.0)
ECHO = Service("test/otel/echo", request=Ping, reply=Ping)


@pytest.fixture(scope="session")
def _provider() -> InMemorySpanExporter:
    """One provider per process — OpenTelemetry ignores a second registration."""
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    otel_api.set_tracer_provider(provider)
    return exporter


@pytest.fixture
def spans(_provider: InMemorySpanExporter) -> InMemorySpanExporter:
    _provider.clear()
    return _provider


def _named(exporter: InMemorySpanExporter, name: str) -> list:
    return [s for s in exporter.get_finished_spans() if s.name == name]


def _hex(span_id: int) -> str:
    return format(span_id, "016x")


# --------------------------------------------------------------------- module


def test_extra_is_installed():
    assert otel.available()


def test_active_traceparent_is_none_outside_a_span():
    assert otel.active_traceparent() is None


def test_active_traceparent_names_the_open_span(spans):
    tracer = otel_api.get_tracer("test")
    with tracer.start_as_current_span("s") as span:
        context = span.get_span_context()
        assert otel.active_traceparent() == (
            f"00-{format(context.trace_id, '032x')}-{_hex(context.span_id)}-01"
        )


# ------------------------------------------------------------------ the fix


@pytest.mark.integration
async def test_wire_traceparent_names_the_producer_span(spans):
    """F9, directly: the id on the wire must be a span that exists."""
    async with harness() as h:
        out = h.collect(CAMERA)
        h.publisher(CAMERA).put(Frame(n=1))
        await out.next()

    published = _named(spans, "publish test/otel/camera")
    assert len(published) == 1
    wire = out.envelopes[0].traceparent
    assert wire is not None
    wire_span_id = wire.split("-")[2]
    assert wire_span_id == _hex(published[0].context.span_id)


@pytest.mark.integration
async def test_multi_hop_assembles_into_a_chain(spans):
    """camera -> detector -> detections: each hop parents on the one before it,
    which is the property a star of orphans fails."""

    class Detector(Node):
        name = "otel-detector"
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

    camera_publish = _named(spans, "publish test/otel/camera")[0]
    detector_process = _named(spans, "process test/otel/camera")[0]
    detector_publish = _named(spans, "publish test/otel/detections")[0]

    # One trace throughout.
    trace_ids = {s.context.trace_id for s in (camera_publish, detector_process, detector_publish)}
    assert len(trace_ids) == 1

    # And a chain, not a star: every parent is the span before it.
    assert detector_process.parent.span_id == camera_publish.context.span_id
    assert detector_publish.parent.span_id == detector_process.context.span_id


@pytest.mark.integration
async def test_a_root_publish_is_a_real_root_span(spans):
    """A root parented on an id zenode invented makes every trace arrive at the
    backend reporting a missing root — Tempo renders it as
    `<root span not yet received>`. The publish that starts a trace must be a
    true root, and must own the trace id the wire then carries."""
    async with harness() as h:
        out = h.collect(CAMERA)
        h.publisher(CAMERA).put(Frame(n=1))
        await out.next()

    published = _named(spans, "publish test/otel/camera")[0]
    assert published.parent is None

    wire = out.envelopes[0].traceparent
    assert wire is not None
    assert wire.split("-")[1] == format(published.context.trace_id, "032x")


@pytest.mark.integration
async def test_untraced_topic_records_nothing(spans):
    async with harness() as h:
        out = h.collect(UNTRACED)
        h.publisher(UNTRACED).put(Frame(n=1))
        await out.next()

    assert out.envelopes[0].traceparent is None
    assert _named(spans, "publish test/otel/untraced") == []
    assert _named(spans, "process test/otel/untraced") == []


@pytest.mark.integration
async def test_unsampled_root_propagates_an_id_but_records_no_spans(spans):
    """F13: correlation stays at full rate, recording does not."""
    async with harness() as h:
        out = h.collect(NEVER_SAMPLED)
        h.publisher(NEVER_SAMPLED).put(Frame(n=1))
        await out.next()

    traceparent = out.envelopes[0].traceparent
    assert trace.trace_id_of(traceparent) is not None  # still correlatable
    assert not trace.sampled_of(traceparent)
    assert _named(spans, "publish test/otel/rare") == []
    assert _named(spans, "process test/otel/rare") == []


@pytest.mark.integration
async def test_service_call_is_a_child_of_the_callers_span(spans):
    class Server(Node):
        name = "otel-echo"
        health_interval = None

        @serve(ECHO)
        async def on_echo(self, req: Ping) -> Ping:
            return Ping(value=req.value)

    async with harness() as h:
        await h.start_node(Server)
        tracer = otel_api.get_tracer("test")
        with tracer.start_as_current_span("caller") as caller, trace.using(trace.new_traceparent()):
            await h.call(ECHO, Ping(value=1))
        caller_span_id = caller.get_span_context().span_id

    served = _named(spans, "serve test/otel/echo")
    assert len(served) == 1
    assert served[0].parent.span_id == caller_span_id


@pytest.mark.integration
async def test_handler_error_marks_the_span(spans):
    """zenode catches and keeps going, so the span would otherwise close clean."""

    async with harness() as h:

        def boom(msg: Frame) -> None:
            raise RuntimeError("handler exploded")

        h.subscribe(CAMERA, boom)
        h.publisher(CAMERA).put(Frame(n=1))
        for _ in range(40):
            if _named(spans, "process test/otel/camera"):
                break
            await asyncio.sleep(0.05)

    processed = _named(spans, "process test/otel/camera")[0]
    assert processed.status.status_code is otel_api.StatusCode.ERROR
    assert "handler exploded" in (processed.status.description or "")
    assert any(e.name == "exception" for e in processed.events)


@pytest.mark.integration
async def test_semantic_conventions_are_set(spans):
    async with harness() as h:
        out = h.collect(CAMERA)
        h.publisher(CAMERA).put(Frame(n=3))
        await out.next()

    attributes = _named(spans, "publish test/otel/camera")[0].attributes
    assert attributes["messaging.system"] == "zenoh"
    assert attributes["messaging.destination.name"] == "test/otel/camera"
    assert attributes["messaging.operation.type"] == "publish"
