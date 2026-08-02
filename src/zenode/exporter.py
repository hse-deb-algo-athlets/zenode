"""``NodeHealth`` on the bus, re-served as Prometheus metrics.

Every node already publishes the four golden signals and its resource usage on
``<ns>/node/<name>/health``. This turns that into something standard
infrastructure can scrape, without any node knowing about it.

A **sidecar**, deliberately, rather than instruments inside each node:

- Nodes stay dependency-free. Where metrics go is a deployment decision, the
  same boundary :mod:`zenode.otel` draws for spans.
- One process to configure and one connection to a collector, instead of one
  per node — which is the connectivity argument that kept the OpenTelemetry SDK
  out of the nodes in the first place.
- The exposition format is a string join, so this costs no dependency at all.
  No protobuf, no gRPC, nothing compiled, on hardware where that matters.

Cardinality is bounded by construction: one series set per live node, labelled
only by node and namespace, both of which come from the contract.

Only pull is implemented. A robot behind NAT cannot be scraped and wants OTLP
push instead; that needs an SDK and an exporter, so it belongs behind the
``otel`` extra rather than here.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .msgs.health import NodeHealth

CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"

DEFAULT_STALE_AFTER = 60.0
"""Seconds without a heartbeat before a node's series are dropped entirely.

Long enough that ``zenode_node_last_seen_seconds`` can carry an alert about a
node that went quiet, short enough that a decommissioned node stops being
reported as if it were merely slow."""


@dataclass(frozen=True, slots=True)
class Sample:
    """One node's most recent heartbeat, and when it arrived."""

    health: NodeHealth
    at: float
    """``time.monotonic()`` on arrival — wall clocks are the sender's problem."""


@dataclass(frozen=True, slots=True)
class Metric:
    """One exported field of ``NodeHealth``, in both wire formats.

    Shared by the Prometheus and OTLP paths on purpose: two tables would drift,
    and a field exported by one and not the other is the kind of gap nobody
    notices until a dashboard is silently missing a series.
    """

    name: str
    """Prometheus name, without the ``zenode_node_`` prefix."""
    kind: str
    help: str
    value: Callable[[NodeHealth], float | None]
    """``None`` omits the series rather than reporting zero: a node with no
    ``/proc`` has unknown CPU, and unknown is not idle."""
    otlp: str
    """OTLP name, dotted per OpenTelemetry convention. Collectors normalise it
    back to the Prometheus form, so both paths land on one series."""
    unit: str
    """UCUM, as OTLP expects: ``s``, ``By``, ``%``, or ``{thing}`` for a count."""
    integral: bool = False
    """Whether the value is a whole number, which OTLP encodes differently."""


# Counters are cumulative since node start. Prometheus detects the reset when a
# node restarts, which is exactly the semantics these have.
COUNTERS: tuple[Metric, ...] = (
    Metric(
        "sent_total",
        "counter",
        "Messages published.",
        lambda h: h.sent,
        "zenode.node.sent",
        "{message}",
        integral=True,
    ),
    Metric(
        "received_total",
        "counter",
        "Messages received.",
        lambda h: h.received,
        "zenode.node.received",
        "{message}",
        integral=True,
    ),
    Metric(
        "dropped_total",
        "counter",
        "Messages dropped by a full queue.",
        lambda h: h.dropped,
        "zenode.node.dropped",
        "{message}",
        integral=True,
    ),
    Metric(
        "stale_total",
        "counter",
        "Messages dropped past max_age.",
        lambda h: h.stale,
        "zenode.node.stale",
        "{message}",
        integral=True,
    ),
    Metric(
        "handler_errors_total",
        "counter",
        "Exceptions raised inside subscription, service and timer handlers.",
        lambda h: h.handler_errors,
        "zenode.node.handler_errors",
        "{error}",
        integral=True,
    ),
    Metric(
        "timer_overruns_total",
        "counter",
        "Timer deadlines missed because a body outran its interval.",
        lambda h: h.timer_overruns,
        "zenode.node.timer_overruns",
        "{overrun}",
        integral=True,
    ),
    Metric(
        "shm_fallbacks_total",
        "counter",
        "Messages on a shm=True topic that published through the normal path.",
        lambda h: h.shm_fallbacks,
        "zenode.node.shm_fallbacks",
        "{message}",
        integral=True,
    ),
    Metric(
        "logs_dropped_total",
        "counter",
        "Log records dropped before publishing, leaving `zenode logs` incomplete.",
        lambda h: h.logs_dropped,
        "zenode.node.logs_dropped",
        "{record}",
        integral=True,
    ),
)

# Base units, per Prometheus convention: seconds and bytes, never milliseconds.
GAUGES: tuple[Metric, ...] = (
    Metric(
        "uptime_seconds",
        "gauge",
        "Time since this node started.",
        lambda h: h.uptime_s,
        "zenode.node.uptime",
        "s",
    ),
    Metric(
        "cpu_percent",
        "gauge",
        "Process CPU since the last heartbeat, as a percentage of one core.",
        lambda h: h.cpu_percent,
        "zenode.node.cpu",
        "%",
    ),
    Metric(
        "rss_bytes",
        "gauge",
        "Process resident set size.",
        lambda h: h.rss_bytes,
        "zenode.node.rss",
        "By",
        integral=True,
    ),
    Metric(
        "queue_max_depth",
        "gauge",
        "Deepest any subscription queue got since the last heartbeat.",
        lambda h: h.queue_max_depth,
        "zenode.node.queue_max_depth",
        "{message}",
        integral=True,
    ),
    Metric(
        "message_age_mean_seconds",
        "gauge",
        "Publish-to-dequeue delay, mean over the last heartbeat interval.",
        lambda h: h.age_mean_ms / 1000.0,
        "zenode.node.message_age_mean",
        "s",
    ),
    Metric(
        "message_age_max_seconds",
        "gauge",
        "Publish-to-dequeue delay, worst case over the last heartbeat interval.",
        lambda h: h.age_max_ms / 1000.0,
        "zenode.node.message_age_max",
        "s",
    ),
    Metric(
        "handler_duration_mean_seconds",
        "gauge",
        "Time spent inside handlers, mean over the last heartbeat interval.",
        lambda h: h.handler_mean_ms / 1000.0,
        "zenode.node.handler_duration_mean",
        "s",
    ),
    Metric(
        "handler_duration_max_seconds",
        "gauge",
        "Time spent inside handlers, worst case over the last heartbeat interval.",
        lambda h: h.handler_max_ms / 1000.0,
        "zenode.node.handler_duration_max",
        "s",
    ),
)


_SELF_HELP = {
    "log_records": "Log records handled by this exporter, by outcome.",
    "metric_pushes": "Metric pushes attempted by this exporter, by outcome.",
}
"""The exporter's own counters.

Nothing else watches the sidecar. When it cannot reach a collector it logs to
its own stderr and every signal stops, which from a dashboard is
indistinguishable from a quiet robot — so the counters it already keeps are
published alongside the nodes' own.
"""


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _labels(namespace: str, node: str, **extra: str) -> str:
    pairs = {"namespace": namespace, "node": node, **extra}
    return ",".join(f'{key}="{_escape(value)}"' for key, value in sorted(pairs.items()))


def _format(value: float) -> str:
    # Integers print without a trailing .0 so counters read as counts; floats
    # get six significant digits, because `6.0023076990000845` seconds of
    # uptime is bytes on every scrape in exchange for nothing.
    return str(int(value)) if float(value).is_integer() else f"{value:.6g}"


def render_self(stats: dict[str, dict[str, int]]) -> list[str]:
    """The exporter's own counters, as exposition lines."""
    lines: list[str] = []
    for family, outcomes in sorted(stats.items()):
        name = f"zenode_exporter_{family}_total"
        lines.append(f"# HELP {name} {_SELF_HELP.get(family, family)}")
        lines.append(f"# TYPE {name} counter")
        lines.extend(
            f'{name}{{outcome="{_escape(outcome)}"}} {value}'
            for outcome, value in sorted(outcomes.items())
        )
    return lines


def render(
    samples: dict[str, Sample],
    namespace: str,
    *,
    now: float,
    stale_after: float = DEFAULT_STALE_AFTER,
    self_stats: dict[str, dict[str, int]] | None = None,
) -> str:
    """The full exposition, as one string. Pure — no clock, no socket.

    Nodes not heard from in ``stale_after`` seconds are omitted entirely, so
    their series go absent rather than freezing at their last value and reading
    as a healthy node that stopped doing anything.
    """
    live = sorted(
        (name, sample) for name, sample in samples.items() if now - sample.at <= stale_after
    )
    lines: list[str] = []

    lines.append("# HELP zenode_node_info Node identity and lifecycle state.")
    lines.append("# TYPE zenode_node_info gauge")
    for name, sample in live:
        lines.append(f"zenode_node_info{{{_labels(namespace, name, state=sample.health.state)}}} 1")

    lines.append("# HELP zenode_node_last_seen_seconds Age of this node's newest heartbeat.")
    lines.append("# TYPE zenode_node_last_seen_seconds gauge")
    for name, sample in live:
        lines.append(
            f"zenode_node_last_seen_seconds{{{_labels(namespace, name)}}} "
            f"{_format(round(now - sample.at, 3))}"
        )

    for metric in (*COUNTERS, *GAUGES):
        rendered = [
            (name, value)
            for name, sample in live
            if (value := metric.value(sample.health)) is not None
        ]
        if not rendered:
            continue
        lines.append(f"# HELP zenode_node_{metric.name} {metric.help}")
        lines.append(f"# TYPE zenode_node_{metric.name} {metric.kind}")
        lines.extend(
            f"zenode_node_{metric.name}{{{_labels(namespace, name)}}} {_format(value)}"
            for name, value in rendered
        )

    if self_stats:
        lines.extend(render_self(self_stats))

    return "\n".join(lines) + "\n"


class Registry:
    """Latest heartbeat per node. Written from a zenoh thread, read by HTTP."""

    def __init__(self, namespace: str, *, stale_after: float = DEFAULT_STALE_AFTER) -> None:
        self.namespace = namespace
        self.stale_after = stale_after
        self.self_stats: Callable[[], dict[str, dict[str, int]]] | None = None
        """Set by the caller to also publish the exporter's own counters."""
        self._samples: dict[str, Sample] = {}
        self._lock = threading.Lock()

    def offer(self, payload: bytes) -> None:
        """Accept a heartbeat; ignore anything that is not one.

        A key expression is not a promise about what is published on it, so a
        foreign payload on a matching key is skipped rather than fatal — the
        same tolerance ``zenode health`` applies.
        """
        try:
            health = NodeHealth.model_validate_json(payload)
        except ValueError:
            return
        with self._lock:
            self._samples[health.node] = Sample(health, time.monotonic())

    def snapshot(self) -> dict[str, Sample]:
        """Every node heard from recently enough to still count.

        Shared by both export paths, so a scrape and a push made a moment apart
        report the same nodes.
        """
        now = time.monotonic()
        with self._lock:
            return {
                name: sample
                for name, sample in self._samples.items()
                if now - sample.at <= self.stale_after
            }

    def render(self) -> str:
        with self._lock:
            samples = dict(self._samples)
        return render(
            samples,
            self.namespace,
            now=time.monotonic(),
            stale_after=self.stale_after,
            self_stats=self.self_stats() if self.self_stats else None,
        )

    def __len__(self) -> int:
        with self._lock:
            return len(self._samples)


class _Handler(BaseHTTPRequestHandler):
    registry: Registry

    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:
        if self.path.split("?")[0] not in ("/metrics", "/"):
            self.send_error(404)
            return
        body = self.registry.render().encode()
        self.send_response(200)
        self.send_header("Content-Type", CONTENT_TYPE)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        """Silence per-request logging: a scrape every 15s is not news."""


def make_server(registry: Registry, host: str, port: int) -> ThreadingHTTPServer:
    """An HTTP server exposing ``registry`` at ``/metrics``.

    The handler class is built per call so the registry is bound to it as a
    class attribute — :class:`~http.server.BaseHTTPRequestHandler` is
    instantiated per request and takes no arguments of its own.
    """
    handler = type("_BoundHandler", (_Handler,), {"registry": registry})
    return ThreadingHTTPServer((host, port), handler)
