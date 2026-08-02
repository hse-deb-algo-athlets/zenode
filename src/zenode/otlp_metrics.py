"""``NodeHealth`` pushed to an OpenTelemetry endpoint.

:mod:`zenode.exporter` serves the same numbers for Prometheus to *pull*. That
works on a bench and fails in two common cases: a robot behind NAT cannot be
scraped, and an OTLP-first stack (``grafana/otel-lgtm``, most vendors) runs a
Prometheus that receives rather than scrapes, so there is nothing to point at
your sidecar.

Both paths read the same :class:`~zenode.exporter.Registry` and the same metric
table, so a scrape and a push a moment apart report identical numbers, and no
field can be exported by one and quietly missed by the other.

Hand-rolled OTLP/JSON, so this needs **no dependency** — the same reasoning as
:mod:`zenode.otlp_logs`. A transport, not an SDK.
"""

from __future__ import annotations

import json
import logging
import threading
import time
import urllib.error
import urllib.request
from typing import Any

from .exporter import COUNTERS, GAUGES, Metric, Registry, Sample
from .otlp_logs import TIMEOUT

logger = logging.getLogger(__name__)

DEFAULT_INTERVAL = 15.0
"""Matches Prometheus's usual scrape interval: these are heartbeat-derived
gauges, and pushing faster than they change is bytes for nothing."""

_CUMULATIVE = 2
"""``AGGREGATION_TEMPORALITY_CUMULATIVE``. Counters are cumulative since node
start, which is what lets a backend detect the reset when a node restarts."""


def _attribute(key: str, value: str) -> dict[str, Any]:
    return {"key": key, "value": {"stringValue": value}}


def _point(
    metric: Metric,
    value: float,
    start_ns: int,
    now_ns: int,
    *,
    node: str,
    namespace: str,
) -> dict[str, Any]:
    # `node` and `namespace` repeat what the resource already says, because a
    # collector turns resource attributes into `service_name`/`job` while the
    # Prometheus path labels the same series `node`/`namespace`. Without this a
    # dashboard written against one path silently matches nothing on the other.
    point: dict[str, Any] = {
        "timeUnixNano": str(now_ns),
        "attributes": [_attribute("node", node), _attribute("namespace", namespace)],
    }
    # OTLP/JSON carries int64 as a string and double as a number; sending a
    # float where the backend expects an integer silently changes the type.
    if metric.integral:
        point["asInt"] = str(int(value))
    else:
        point["asDouble"] = float(value)
    if metric.kind == "counter":
        point["startTimeUnixNano"] = str(start_ns)
    return point


def _start_ns(sample: Sample) -> int:
    """When the node started, in wall time — the counters' origin.

    Derived from the heartbeat rather than observed by the sidecar, so a
    restarted node reports a new origin and the backend sees a reset instead of
    a counter that appears to fall.
    """
    return int(sample.health.ts_ns - sample.health.uptime_s * 1_000_000_000)


def encode(samples: dict[str, Sample], namespace: str, *, now_ns: int) -> dict[str, Any]:
    """One OTLP ``resourceMetrics`` payload, one resource per node."""
    resources = []
    for node, sample in sorted(samples.items()):
        attributes = [_attribute("service.name", node)]
        if namespace:
            attributes.append(_attribute("service.namespace", namespace))
        start_ns = _start_ns(sample)

        metrics: list[dict[str, Any]] = []
        for metric in (*COUNTERS, *GAUGES):
            value = metric.value(sample.health)
            if value is None:
                continue  # unknown is not zero — see Metric.value
            point = _point(metric, value, start_ns, now_ns, node=node, namespace=namespace)
            body: dict[str, Any] = {
                "name": metric.otlp,
                "unit": metric.unit,
                "description": metric.help,
            }
            if metric.kind == "counter":
                body["sum"] = {
                    "dataPoints": [point],
                    "aggregationTemporality": _CUMULATIVE,
                    "isMonotonic": True,
                }
            else:
                body["gauge"] = {"dataPoints": [point]}
            metrics.append(body)

        if metrics:
            resources.append(
                {
                    "resource": {"attributes": attributes},
                    "scopeMetrics": [{"scope": {"name": "zenode"}, "metrics": metrics}],
                }
            )
    return {"resourceMetrics": resources}


class OtlpMetricShipper:
    """Pushes the registry's newest heartbeat per node to an OTLP endpoint."""

    def __init__(
        self,
        endpoint: str,
        registry: Registry,
        *,
        interval: float = DEFAULT_INTERVAL,
    ) -> None:
        self.url = endpoint.rstrip("/") + "/v1/metrics"
        self.registry = registry
        self.interval = interval
        self.pushed = 0
        self.failed = 0

    def flush(self, now_ns: int) -> int:
        """Push the current snapshot. Returns the number of nodes reported."""
        samples = self.registry.snapshot()
        if not samples:
            return 0
        payload = encode(samples, self.registry.namespace, now_ns=now_ns)
        if not payload["resourceMetrics"]:
            return 0
        request = urllib.request.Request(
            self.url,
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=TIMEOUT) as response:
                if response.status >= 300:
                    raise urllib.error.HTTPError(
                        self.url, response.status, "rejected", response.headers, None
                    )
        except (urllib.error.URLError, OSError, TimeoutError) as e:
            # Nothing is queued: the next tick carries the current value anyway,
            # so a dropped push costs one interval of resolution rather than
            # unbounded memory. Gauges have no backlog worth keeping.
            self.failed += 1
            logger.warning("dropping a metrics push: %s", e, extra={"url": self.url})
            return 0
        self.pushed += 1
        return len(payload["resourceMetrics"])

    def run(self, stop: threading.Event) -> None:
        """Push on ``interval`` until ``stop`` is set. Runs on its own thread."""
        while not stop.wait(self.interval):
            self.flush(time.time_ns())
