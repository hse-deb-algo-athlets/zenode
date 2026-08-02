"""The log topic, pushed to an OpenTelemetry endpoint.

:mod:`zenode.exporter` re-serves health for Prometheus to pull. Logs cannot work
that way — Loki and every OTLP endpoint are push-only — so this ships them.

OTLP rather than Loki's native API, for one reason that matters here: an OTLP
log record carries ``traceId`` as a first-class field. Grafana uses it to jump
straight from a log line to the trace that produced it, which is the payoff the
whole trace-context chain exists for. Loki's own push API would need the id
smuggled into a label (bad cardinality) or re-extracted from the line by a
regex. It also means one endpoint for traces and logs, and it works against a
collector, against Loki 3.x directly, and against a vendor.

Hand-rolled OTLP/JSON, so this needs **no dependency** — the same reasoning that
keeps protobuf and gRPC off the robot in :mod:`zenode.exporter`. It is a
transport, not an SDK: no provider, no batching processor, no ``OTEL_*``.

Bounded like everything else on this path: a full queue drops the oldest and
counts it, and an unreachable endpoint drops a batch rather than growing.
"""

from __future__ import annotations

import json
import logging
import threading
import urllib.error
import urllib.request
from collections import deque
from typing import Any

from .msgs.log import LogRecordMsg

logger = logging.getLogger(__name__)

DEFAULT_CAPACITY = 1024
DEFAULT_INTERVAL = 2.0
TIMEOUT = 5.0

_SEVERITY = {
    "TRACE": (1, "TRACE"),
    "DEBUG": (5, "DEBUG"),
    "INFO": (9, "INFO"),
    "WARNING": (13, "WARN"),
    "ERROR": (17, "ERROR"),
    "CRITICAL": (21, "FATAL"),
}
"""Python level names to OpenTelemetry severity numbers. Unknown levels land on
INFO rather than being dropped: a record with an odd level is still a record."""


def _attribute(key: str, value: str | int) -> dict[str, Any]:
    if isinstance(value, int):
        return {"key": key, "value": {"intValue": str(value)}}
    return {"key": key, "value": {"stringValue": value}}


def _record(entry: LogRecordMsg) -> dict[str, Any]:
    number, text = _SEVERITY.get(entry.level.upper(), _SEVERITY["INFO"])
    attributes = [
        _attribute("logger", entry.logger),
        _attribute("code.lineno", entry.line),
        *(_attribute(key, value) for key, value in sorted(entry.fields.items())),
    ]
    record: dict[str, Any] = {
        "timeUnixNano": str(entry.ts_ns),
        "severityNumber": number,
        "severityText": text,
        "body": {"stringValue": entry.message},
        "attributes": attributes,
    }
    if entry.trace:
        # The field Grafana reads to offer "view trace" on a log line.
        record["traceId"] = entry.trace
    return record


def encode(entries: list[LogRecordMsg], namespace: str) -> dict[str, Any]:
    """One OTLP ``resourceLogs`` payload, one resource per node.

    ``service.name`` is the node, matching what a node's own span exporter would
    report, so logs and traces land under the same service.
    """
    by_node: dict[str, list[LogRecordMsg]] = {}
    for entry in entries:
        by_node.setdefault(entry.node, []).append(entry)

    resources = []
    for node, records in sorted(by_node.items()):
        attributes = [_attribute("service.name", node)]
        if namespace:
            attributes.append(_attribute("service.namespace", namespace))
        resources.append(
            {
                "resource": {"attributes": attributes},
                "scopeLogs": [
                    {
                        "scope": {"name": "zenode"},
                        "logRecords": [_record(entry) for entry in records],
                    }
                ],
            }
        )
    return {"resourceLogs": resources}


class OtlpLogShipper:
    """Batches log records off the bus and pushes them to an OTLP endpoint."""

    def __init__(
        self,
        endpoint: str,
        namespace: str,
        *,
        capacity: int = DEFAULT_CAPACITY,
        interval: float = DEFAULT_INTERVAL,
    ) -> None:
        self.url = endpoint.rstrip("/") + "/v1/logs"
        self.namespace = namespace
        self.interval = interval
        self._pending: deque[LogRecordMsg] = deque(maxlen=capacity)
        self._lock = threading.Lock()
        self.dropped = 0
        """Records discarded because the queue was full."""
        self.failed = 0
        """Records discarded because the endpoint would not take them."""
        self.shipped = 0

    def offer(self, payload: bytes) -> None:
        """Accept a record off the bus; ignore anything that is not one."""
        try:
            entry = LogRecordMsg.model_validate_json(payload)
        except ValueError:
            return
        with self._lock:
            if len(self._pending) == self._pending.maxlen:
                self.dropped += 1
            self._pending.append(entry)

    def flush(self) -> int:
        """Push everything queued. Returns the number of records sent."""
        with self._lock:
            batch = list(self._pending)
            self._pending.clear()
        if not batch:
            return 0
        body = json.dumps(encode(batch, self.namespace)).encode()
        request = urllib.request.Request(
            self.url, data=body, headers={"Content-Type": "application/json"}
        )
        try:
            with urllib.request.urlopen(request, timeout=TIMEOUT) as response:
                if response.status >= 300:
                    raise urllib.error.HTTPError(
                        self.url, response.status, "rejected", response.headers, None
                    )
        except (urllib.error.URLError, OSError, TimeoutError) as e:
            # Dropped, not retried: a robot in the field is disconnected as the
            # normal case, and a queue that grows until the link comes back is
            # how a sidecar takes down the thing it was meant to observe.
            self.failed += len(batch)
            logger.warning("dropping %d log records: %s", len(batch), e, extra={"url": self.url})
            return 0
        self.shipped += len(batch)
        return len(batch)

    def run(self, stop: threading.Event) -> None:
        """Flush on ``interval`` until ``stop`` is set. Runs on its own thread."""
        while not stop.wait(self.interval):
            self.flush()
        self.flush()  # last batch on the way out
