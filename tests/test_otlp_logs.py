"""Log records off the bus, pushed as OTLP."""

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest

from zenode.msgs.log import LogRecordMsg
from zenode.otlp_logs import OtlpLogShipper, encode


def _entry(**kwargs: Any) -> LogRecordMsg:
    return LogRecordMsg.model_validate(
        {
            "node": "detector",
            "level": "WARNING",
            "logger": "zenode.node.detector",
            "message": "obstacle detected",
            "ts_ns": 1_700_000_000_000_000_000,
            "line": 42,
            **kwargs,
        }
    )


def _records(payload: dict[str, Any], index: int = 0) -> list[Any]:
    return payload["resourceLogs"][index]["scopeLogs"][0]["logRecords"]


def _attrs(record: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for a in record["attributes"]:
        value = a["value"]
        out[a["key"]] = value.get("stringValue", value.get("intValue"))
    return out


# -------------------------------------------------------------------- encode


def test_service_name_is_the_node():
    payload = encode([_entry()], "")
    attrs = payload["resourceLogs"][0]["resource"]["attributes"]
    assert {"key": "service.name", "value": {"stringValue": "detector"}} in attrs


def test_namespace_becomes_service_namespace():
    payload = encode([_entry()], "robodog")
    attrs = payload["resourceLogs"][0]["resource"]["attributes"]
    assert {"key": "service.namespace", "value": {"stringValue": "robodog"}} in attrs

    payload = encode([_entry()], "")
    keys = [a["key"] for a in payload["resourceLogs"][0]["resource"]["attributes"]]
    assert "service.namespace" not in keys


def test_one_resource_per_node():
    payload = encode([_entry(node="camera"), _entry(node="motors"), _entry(node="camera")], "")
    names = [
        next(a["value"]["stringValue"] for a in r["resource"]["attributes"])
        for r in payload["resourceLogs"]
    ]
    assert names == ["camera", "motors"]
    assert len(_records(payload, 0)) == 2


def test_trace_id_is_a_first_class_field():
    """What Grafana reads to offer `view trace` on a log line."""
    record = _records(encode([_entry(trace="a" * 32)], ""))[0]
    assert record["traceId"] == "a" * 32


def test_untraced_records_carry_no_trace_id():
    assert "traceId" not in _records(encode([_entry()], ""))[0]


def test_levels_map_to_otel_severity():
    for level, (number, text) in (
        ("DEBUG", (5, "DEBUG")),
        ("INFO", (9, "INFO")),
        ("WARNING", (13, "WARN")),
        ("ERROR", (17, "ERROR")),
        ("CRITICAL", (21, "FATAL")),
    ):
        record = _records(encode([_entry(level=level)], ""))[0]
        assert (record["severityNumber"], record["severityText"]) == (number, text)


def test_unknown_level_lands_on_info_rather_than_vanishing():
    record = _records(encode([_entry(level="NOTICE")], ""))[0]
    assert record["severityNumber"] == 9


def test_extra_fields_become_attributes():
    record = _records(encode([_entry(fields={"wheel": "fl", "frame": "9"})], ""))[0]
    attrs = _attrs(record)
    assert attrs["wheel"] == "fl"
    assert attrs["logger"] == "zenode.node.detector"
    assert attrs["code.lineno"] == "42"


def test_body_and_timestamp():
    record = _records(encode([_entry()], ""))[0]
    assert record["body"] == {"stringValue": "obstacle detected"}
    assert record["timeUnixNano"] == "1700000000000000000"


def test_payload_is_json_serialisable():
    json.dumps(encode([_entry(trace="b" * 32, fields={"k": "v"})], "ns"))


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


def test_flush_posts_what_was_offered(collector):
    url, received, _ = collector
    shipper = OtlpLogShipper(url, "")
    shipper.offer(_entry().model_dump_json().encode())
    shipper.offer(_entry(node="motors").model_dump_json().encode())

    assert shipper.flush() == 2
    assert shipper.shipped == 2
    assert len(received) == 1
    assert len(received[0]["resourceLogs"]) == 2


def test_endpoint_path_is_appended(collector):
    url, _, _ = collector
    assert OtlpLogShipper(url + "/", "").url == url + "/v1/logs"


def test_flush_with_nothing_queued_posts_nothing(collector):
    url, received, _ = collector
    assert OtlpLogShipper(url, "").flush() == 0
    assert received == []


def test_foreign_payloads_are_ignored(collector):
    url, _, _ = collector
    shipper = OtlpLogShipper(url, "")
    shipper.offer(b"not json")
    shipper.offer(b'{"unrelated": true}')
    assert shipper.flush() == 0


def test_full_queue_drops_oldest_and_counts(collector):
    url, received, _ = collector
    shipper = OtlpLogShipper(url, "", capacity=3)
    for n in range(10):
        shipper.offer(_entry(message=f"m{n}").model_dump_json().encode())

    assert shipper.dropped == 7
    shipper.flush()
    bodies = [r["body"]["stringValue"] for r in _records(received[0])]
    assert bodies == ["m7", "m8", "m9"]


def test_unreachable_endpoint_drops_rather_than_growing():
    """A field robot is disconnected as the normal case; a queue that grows
    until the link returns is how a sidecar kills what it was watching."""
    shipper = OtlpLogShipper("http://127.0.0.1:1", "")  # nothing listens
    shipper.offer(_entry().model_dump_json().encode())

    assert shipper.flush() == 0
    assert shipper.failed == 1
    assert shipper.shipped == 0
    assert len(shipper._pending) == 0  # not retained for a retry that never comes


def test_rejected_batch_is_counted_failed(collector):
    url, _, handler = collector
    handler.status = 503
    shipper = OtlpLogShipper(url, "")
    shipper.offer(_entry().model_dump_json().encode())

    assert shipper.flush() == 0
    assert shipper.failed == 1


def test_run_flushes_on_its_interval_and_on_stop(collector):
    url, received, _ = collector
    shipper = OtlpLogShipper(url, "", interval=0.05)
    stop = threading.Event()
    thread = threading.Thread(target=shipper.run, args=(stop,), daemon=True)
    thread.start()

    shipper.offer(_entry().model_dump_json().encode())
    for _ in range(40):
        if received:
            break
        threading.Event().wait(0.05)
    stop.set()
    thread.join(timeout=2)

    assert shipper.shipped >= 1
