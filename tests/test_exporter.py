"""NodeHealth re-served as Prometheus metrics."""

import threading
import urllib.error
import urllib.request
from collections.abc import Iterator
from typing import Any

import pytest

from zenode.exporter import Registry, Sample, make_server, render
from zenode.msgs.health import NodeHealth


def _health(node: str = "camera", **kwargs: Any) -> NodeHealth:
    # Built through model_validate rather than the constructor: a **kwargs
    # spread widens every field to one union, which a checker cannot reconcile
    # with `state`'s Literal. Validation still runs, so a typo still fails.
    return NodeHealth.model_validate(
        {"node": node, "state": "running", "uptime_s": 6.0, "ts_ns": 1, **kwargs}
    )


def _lines(text: str, prefix: str) -> list[str]:
    return [line for line in text.splitlines() if line.startswith(prefix)]


def _value(text: str, prefix: str) -> str:
    matches = _lines(text, prefix)
    assert len(matches) == 1, f"expected one {prefix!r}, got {matches}"
    return matches[0].rsplit(" ", 1)[1]


# -------------------------------------------------------------------- format


def test_counters_and_gauges_are_typed():
    text = render({"camera": Sample(_health(sent=177), 100.0)}, "", now=100.0)
    assert "# TYPE zenode_node_sent_total counter" in text
    assert "# TYPE zenode_node_uptime_seconds gauge" in text
    assert "# HELP zenode_node_sent_total Messages published." in text


def test_labels_carry_node_and_namespace():
    text = render({"camera": Sample(_health(sent=3), 100.0)}, "robodog", now=100.0)
    assert 'zenode_node_sent_total{namespace="robodog",node="camera"} 3' in text


def test_milliseconds_become_seconds():
    """Prometheus convention is base units; NodeHealth reports milliseconds."""
    text = render({"camera": Sample(_health(age_max_ms=121.5), 100.0)}, "", now=100.0)
    assert _value(text, "zenode_node_message_age_max_seconds") == "0.1215"


def test_unknown_resources_are_omitted_not_zeroed():
    """A node with no /proc has unknown CPU, and unknown is not idle."""
    text = render(
        {"camera": Sample(_health(cpu_percent=None, rss_bytes=None), 100.0)}, "", now=100.0
    )
    assert _lines(text, "zenode_node_cpu_percent") == []
    assert _lines(text, "zenode_node_rss_bytes") == []

    text = render(
        {"camera": Sample(_health(cpu_percent=61.8, rss_bytes=4096), 100.0)}, "", now=100.0
    )
    assert _value(text, "zenode_node_cpu_percent") == "61.8"
    assert _value(text, "zenode_node_rss_bytes") == "4096"


def test_state_rides_on_the_info_metric():
    text = render({"camera": Sample(_health(state="stopping"), 100.0)}, "", now=100.0)
    assert 'zenode_node_info{namespace="",node="camera",state="stopping"} 1' in text


def test_last_seen_grows_with_silence():
    text = render({"camera": Sample(_health(), 100.0)}, "", now=104.5)
    assert _value(text, "zenode_node_last_seen_seconds") == "4.5"


def test_stale_nodes_are_dropped_entirely():
    """Frozen counters read as a healthy node doing nothing; absence does not."""
    samples = {"camera": Sample(_health("camera"), 100.0), "gone": Sample(_health("gone"), 10.0)}
    text = render(samples, "", now=100.0, stale_after=60.0)
    assert 'node="camera"' in text
    assert 'node="gone"' not in text


def test_every_node_appears_under_every_metric():
    samples = {
        "camera": Sample(_health("camera", sent=1), 100.0),
        "motors": Sample(_health("motors", sent=2), 100.0),
    }
    text = render(samples, "", now=100.0)
    assert len(_lines(text, "zenode_node_sent_total{")) == 2


def test_label_values_are_escaped():
    text = render({'we"ird': Sample(_health('we"ird'), 100.0)}, "", now=100.0)
    assert r'node="we\"ird"' in text


def test_empty_registry_still_renders():
    assert render({}, "", now=100.0).endswith("\n")


# ------------------------------------------------------------------ registry


def test_registry_keeps_the_newest_per_node():
    registry = Registry("")
    registry.offer(_health(sent=1).model_dump_json().encode())
    registry.offer(_health(sent=9).model_dump_json().encode())
    assert len(registry) == 1
    assert _value(registry.render(), "zenode_node_sent_total") == "9"


def test_registry_ignores_foreign_payloads():
    """A key expression is not a promise about what is published on it."""
    registry = Registry("")
    registry.offer(b"not json")
    registry.offer(b'{"unrelated": true}')
    assert len(registry) == 0


# ---------------------------------------------------------------------- http


@pytest.fixture
def served() -> Iterator[tuple[str, Registry]]:
    registry = Registry("robodog")
    server = make_server(registry, "127.0.0.1", 0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address[:2]
    try:
        yield f"http://{host}:{port}", registry
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_metrics_endpoint_serves_the_registry(served):
    base, registry = served
    registry.offer(_health(sent=42).model_dump_json().encode())

    with urllib.request.urlopen(f"{base}/metrics", timeout=5) as response:
        assert response.status == 200
        assert response.headers["Content-Type"].startswith("text/plain")
        body = response.read().decode()

    assert 'zenode_node_sent_total{namespace="robodog",node="camera"} 42' in body


def test_unknown_paths_are_404(served):
    base, _ = served
    with pytest.raises(urllib.error.HTTPError) as excinfo:
        urllib.request.urlopen(f"{base}/nope", timeout=5)
    assert excinfo.value.code == 404


def test_floats_are_not_full_precision():
    """Uptime as `6.0023076990000845` is bytes on every scrape for nothing."""
    text = render({"camera": Sample(_health(uptime_s=6.0023076990000845), 100.0)}, "", now=100.0)
    assert _value(text, "zenode_node_uptime_seconds") == "6.00231"


# ------------------------------------------------------------- self counters


def test_self_counters_are_absent_by_default():
    assert "zenode_exporter" not in render({}, "", now=100.0)


def test_self_counters_are_typed_and_labelled_by_outcome():
    text = render(
        {},
        "",
        now=100.0,
        self_stats={"log_records": {"shipped": 12, "queue_full": 3, "push_failed": 1}},
    )
    assert "# TYPE zenode_exporter_log_records_total counter" in text
    assert 'zenode_exporter_log_records_total{outcome="shipped"} 12' in text
    assert 'zenode_exporter_log_records_total{outcome="queue_full"} 3' in text
    assert 'zenode_exporter_log_records_total{outcome="push_failed"} 1' in text


def test_self_counters_accompany_node_series():
    """A broken sidecar must be visible on the same scrape as the nodes."""
    text = render(
        {"camera": Sample(_health(sent=1), 100.0)},
        "",
        now=100.0,
        self_stats={"metric_pushes": {"ok": 4, "failed": 2}},
    )
    assert 'zenode_node_sent_total{namespace="",node="camera"} 1' in text
    assert 'zenode_exporter_metric_pushes_total{outcome="failed"} 2' in text


def test_registry_publishes_the_configured_self_stats():
    registry = Registry("")
    registry.self_stats = lambda: {"metric_pushes": {"ok": 7, "failed": 0}}
    assert 'zenode_exporter_metric_pushes_total{outcome="ok"} 7' in registry.render()
