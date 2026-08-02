"""A traced three-node pipeline, exporting to an OpenTelemetry backend.

Run each node in its own process, the way they would run on a robot::

    python examples/otel_pipeline.py camera
    python examples/otel_pipeline.py detector
    python examples/otel_pipeline.py motors

Needs the extra and an SDK with an exporter::

    pip install 'zenode[otel]' opentelemetry-sdk opentelemetry-exporter-otlp-proto-http

and somewhere to send them — the Grafana LGTM all-in-one is one container::

    docker run --rm -p 3000:3000 -p 4318:4318 grafana/otel-lgtm

Then open http://localhost:3000, pick the Tempo datasource, and search by
service name. ``$OTLP_ENDPOINT`` overrides the default.

Everything OpenTelemetry-specific in this file is in ``setup_telemetry`` below.
zenode records the spans; it does not decide where they go.
"""

from __future__ import annotations

import asyncio
import os
import sys

from opentelemetry import trace as otel_trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from pydantic import BaseModel

from zenode import Node, Service, Topic, every, run, serve, subscribe

OTLP_ENDPOINT = os.environ.get("OTLP_ENDPOINT", "http://localhost:4318")

tracer = otel_trace.get_tracer("examples.otel_pipeline")
"""A ProxyTracer until setup_telemetry runs, so module scope is fine."""


def setup_telemetry(service_name: str) -> None:
    """The whole integration. zenode never does this for you, on purpose.

    ``service.name`` is what a backend groups spans by, so it belongs to the
    deployment rather than to the framework — one process per node here means
    one service per node in Tempo.
    """
    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
    provider.add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{OTLP_ENDPOINT}/v1/traces"))
    )
    otel_trace.set_tracer_provider(provider)


class Frame(BaseModel):
    n: int = 0


class Detection(BaseModel):
    n: int = 0
    obstacle: bool = False


class DescribeRequest(BaseModel):
    n: int = 0


class Description(BaseModel):
    label: str = ""


# The trace starts here and follows the data. trace_ratio would thin it out on a
# real 30 Hz camera; at 2 Hz there is nothing to thin.
CAMERA = Topic("camera/rgb", Frame, trace=True)
DETECTIONS = Topic("perception/boxes", Detection)

# A service call joins the trace of whatever triggered it, so the reply shows up
# under the same trace as the frame that caused it — even though the request
# travels back towards the node the data came from.
DESCRIBE = Service("perception/describe", request=DescribeRequest, reply=Description)


class Camera(Node):
    name = "camera"

    async def on_start(self) -> None:
        self.frames = self.publisher(CAMERA)
        self.n = 0

    @every(0.5)
    async def tick(self) -> None:
        self.n += 1
        await asyncio.sleep(0.008)  # stand in for a grab
        self.frames.put(Frame(n=self.n))


class Detector(Node):
    name = "detector"

    async def on_start(self) -> None:
        self.out = self.publisher(DETECTIONS)

    @subscribe(CAMERA)
    async def on_frame(self, msg: Frame) -> None:
        # An application span. It nests inside zenode's `process camera/rgb`,
        # and because the publish below happens inside it, the trace context on
        # the wire names *this* span — so `motors` chains onto it, not onto the
        # handler. Nothing has to be wired up for that.
        with tracer.start_as_current_span("inference") as span:
            await asyncio.sleep(0.02)
            obstacle = msg.n % 3 == 0
            span.set_attribute("detector.obstacle", obstacle)
            if obstacle:
                self.log.warning("obstacle detected", extra={"frame": msg.n})
            self.out.put(Detection(n=msg.n, obstacle=obstacle))

    @serve(DESCRIBE)
    async def on_describe(self, req: DescribeRequest) -> Description:
        await asyncio.sleep(0.003)
        return Description(label=f"obstacle-{req.n % 4}")


class Motors(Node):
    name = "motors"

    @subscribe(DETECTIONS)
    async def on_detection(self, msg: Detection) -> None:
        await asyncio.sleep(0.004)
        if not msg.obstacle:
            return
        # Calling back into `detector` from inside this handler: the request
        # carries the active trace, so the `serve` span lands in the same trace
        # as the frame that started it.
        described = await self.call(DESCRIBE, DescribeRequest(n=msg.n))
        self.log.warning("emergency stop", extra={"frame": msg.n, "why": described.label})


NODES = {node.name: node for node in (Camera, Detector, Motors)}


def main() -> None:
    if len(sys.argv) != 2 or sys.argv[1] not in NODES:
        sys.exit(f"usage: {sys.argv[0]} {{{'|'.join(NODES)}}}")
    name = sys.argv[1]
    setup_telemetry(name)
    run(NODES[name])


if __name__ == "__main__":
    main()
