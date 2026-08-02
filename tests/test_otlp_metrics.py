"""NodeHealth pushed as OTLP metrics."""

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest

from zenode.exporter import COUNTERS, GAUGES, Registry, Sample
from zenode.msgs.health import NodeHealth
from zenode.otlp_metrics import OtlpMetricShipper, encode

NOW_NS = 1_700_000_000_000_000_000


def _health(node: str = "camera", **kwargs: Any) -> NodeHealth:
    return NodeHealth.model_validate(
        {"node": node, "state": "running", "uptime_s": 6.0, "ts_ns": NOW_NS, **kwargs}
    )


def _metrics(payload: dict[str, Any], index: int = 0) -> dict[str, Any]:
    return {m["name"]: m for m in payload["resourceMetrics"][index]["scopeMetrics"][0]["metrics"]}


# -------------------------------------------------------------------- encode


def test_service_name_is_the_node():
    payload = encode({"camera": Sample(_health(), 1.0)}, "", now_ns=NOW_NS)
    attrs = payload["resourceMetrics"][0]["resource"]["attributes"]
    assert {"key": "service.name", "value": {"stringValue": "camera"}} in attrs


def test_namespace_becomes_service_namespace():
    payload = encode({"camera": Sample(_health(), 1.0)}, "robodog", now_ns=NOW_NS)
    attrs = payload["resourceMetrics"][0]["resource"]["attributes"]
    assert {"key": "service.namespace", "value": {"stringValue": "robodog"}} in attrs


def test_one_resource_per_node():
    samples = {"camera": Sample(_health("camera"), 1.0), "motors": Sample(_health("motors"), 1.0)}
    payload = encode(samples, "", now_ns=NOW_NS)
    names = [
        next(a["value"]["stringValue"] for a in r["resource"]["attributes"])
        for r in payload["resourceMetrics"]
    ]
    assert names == ["camera", "motors"]


def test_counters_are_cumulative_monotonic_sums():
    payload = encode({"camera": Sample(_health(sent=163), 1.0)}, "", now_ns=NOW_NS)
    sent = _metrics(payload)["zenode.node.sent"]
    assert sent["sum"]["isMonotonic"] is True
    assert sent["sum"]["aggregationTemporality"] == 2  # CUMULATIVE
    assert sent["sum"]["dataPoints"][0]["asInt"] == "163"


def test_gauges_are_gauges():
    payload = encode({"camera": Sample(_health(cpu_percent=61.8), 1.0)}, "", now_ns=NOW_NS)
    cpu = _metrics(payload)["zenode.node.cpu"]
    assert "gauge" in cpu
    assert (
        cpu["dataPoints" if "dataPoints" in cpu else "gauge"]["dataPoints"][0]["asDouble"] == 61.8
    )


def test_counter_start_time_is_when_the_node_started():
    """A restart must read as a reset, not as a counter that fell."""
    payload = encode({"camera": Sample(_health(sent=1, uptime_s=6.0), 1.0)}, "", now_ns=NOW_NS)
    point = _metrics(payload)["zenode.node.sent"]["sum"]["dataPoints"][0]
    assert int(point["startTimeUnixNano"]) == NOW_NS - 6_000_000_000


def test_gauges_carry_no_start_time():
    payload = encode({"camera": Sample(_health(), 1.0)}, "", now_ns=NOW_NS)
    assert (
        "startTimeUnixNano" not in _metrics(payload)["zenode.node.uptime"]["gauge"]["dataPoints"][0]
    )


def test_integers_are_strings_and_floats_are_numbers():
    """OTLP/JSON carries int64 as a string; getting this wrong changes the type."""
    payload = encode(
        {"camera": Sample(_health(sent=5, rss_bytes=4096, cpu_percent=0.5), 1.0)},
        "",
        now_ns=NOW_NS,
    )
    metrics = _metrics(payload)
    assert metrics["zenode.node.sent"]["sum"]["dataPoints"][0]["asInt"] == "5"
    assert metrics["zenode.node.rss"]["gauge"]["dataPoints"][0]["asInt"] == "4096"
    assert metrics["zenode.node.cpu"]["gauge"]["dataPoints"][0]["asDouble"] == 0.5


def test_unknown_resources_are_omitted_not_zeroed():
    payload = encode(
        {"camera": Sample(_health(cpu_percent=None, rss_bytes=None), 1.0)}, "", now_ns=NOW_NS
    )
    metrics = _metrics(payload)
    assert "zenode.node.cpu" not in metrics
    assert "zenode.node.rss" not in metrics


def test_units_are_set_on_every_metric():
    payload = encode(
        {"camera": Sample(_health(cpu_percent=1.0, rss_bytes=1), 1.0)}, "", now_ns=NOW_NS
    )
    metrics = _metrics(payload)
    assert metrics["zenode.node.uptime"]["unit"] == "s"
    assert metrics["zenode.node.rss"]["unit"] == "By"
    assert metrics["zenode.node.cpu"]["unit"] == "%"


def test_both_paths_export_the_same_fields():
    """The point of one shared table: no field exported by one path only."""
    payload = encode(
        {"camera": Sample(_health(cpu_percent=1.0, rss_bytes=1), 1.0)}, "", now_ns=NOW_NS
    )
    assert set(_metrics(payload)) == {m.otlp for m in (*COUNTERS, *GAUGES)}


def test_empty_registry_encodes_to_nothing():
    assert encode({}, "", now_ns=NOW_NS)["resourceMetrics"] == []


def test_payload_is_json_serialisable():
    json.dumps(encode({"camera": Sample(_health(), 1.0)}, "ns", now_ns=NOW_NS))


# ------------------------------------------------------------------- shipper


class _Collector(BaseHTTPRequestHandler):
    received: list[Any]
    status = 200

    def do_POST(self) -> None:
        body = self.rfile.read(int(self.headers["Content-Length"]))
        type(self).received.append(json.loads(body))
        self.send_response(type(self).status)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, format: str, *args: object) -> None:
        pass


@pytest.fixture
def collector():
    received: list[Any] = []
    handler = type("_Bound", (_Collector,), {"received": received, "status": 200})
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address[:2]
    try:
        yield f"http://{host}:{port}", received, handler
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_flush_pushes_the_registry(collector):
    url, received, _ = collector
    registry = Registry("robodog")
    registry.offer(_health(sent=42).model_dump_json().encode())

    shipper = OtlpMetricShipper(url, registry)
    assert shipper.flush(NOW_NS) == 1
    assert shipper.pushed == 1
    assert len(received) == 1
    assert _metrics(received[0])["zenode.node.sent"]["sum"]["dataPoints"][0]["asInt"] == "42"


def test_endpoint_path_is_appended(collector):
    url, _, _ = collector
    assert OtlpMetricShipper(url + "/", Registry("")).url == url + "/v1/metrics"


def test_empty_registry_pushes_nothing(collector):
    url, received, _ = collector
    assert OtlpMetricShipper(url, Registry("")).flush(NOW_NS) == 0
    assert received == []


def test_stale_nodes_are_not_pushed(collector):
    """A node gone quiet drops out of the push, so its series goes absent —
    which is how staleness is expressed without a `last_seen` gauge."""
    url, _, _ = collector
    registry = Registry("", stale_after=0.0)
    registry.offer(_health().model_dump_json().encode())
    time.sleep(0.01)

    assert OtlpMetricShipper(url, registry).flush(NOW_NS) == 0


def test_unreachable_endpoint_drops_rather_than_queueing():
    """Gauges have no backlog worth keeping: the next tick carries the value."""
    registry = Registry("")
    registry.offer(_health().model_dump_json().encode())
    shipper = OtlpMetricShipper("http://127.0.0.1:1", registry)

    assert shipper.flush(NOW_NS) == 0
    assert shipper.failed == 1
    assert shipper.pushed == 0


def test_rejected_push_is_counted_failed(collector):
    url, _, handler = collector
    handler.status = 503
    registry = Registry("")
    registry.offer(_health().model_dump_json().encode())

    shipper = OtlpMetricShipper(url, registry)
    assert shipper.flush(NOW_NS) == 0
    assert shipper.failed == 1


def test_points_carry_node_and_namespace_like_the_scrape_path():
    """A collector maps resource attributes to `service_name`/`job`, but the
    Prometheus path labels the same series `node`/`namespace`. Both must be
    present or a dashboard only works against one of them."""
    payload = encode({"camera": Sample(_health(), 1.0)}, "robodog", now_ns=NOW_NS)
    point = _metrics(payload)["zenode.node.sent"]["sum"]["dataPoints"][0]
    attrs = {a["key"]: a["value"]["stringValue"] for a in point["attributes"]}
    assert attrs == {"node": "camera", "namespace": "robodog"}
