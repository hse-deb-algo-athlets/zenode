"""The `zenode health` view: key derivation, table formatting, and decoding."""

import queue
import sys

from zenode.cli import _drain_health, _format_uptime, _health_row, _render_health
from zenode.msgs import NodeHealth, health_key, health_pattern

_BASE = NodeHealth(node="nav", state="running", uptime_s=1.0, ts_ns=0)


def make_health(**kwargs) -> NodeHealth:
    return _BASE.model_copy(update=kwargs)


# ------------------------------------------------------------------ key derivation


def test_health_key_is_relative():
    assert health_key("nav") == "node/nav/health"


def test_pattern_matches_the_key_it_derives_from():
    """The publisher and the CLI must not be able to drift apart."""
    assert health_pattern("") == "node/*/health"
    assert health_pattern("robodog") == "robodog/node/*/health"
    assert health_pattern("robodog").replace("*", "nav") == f"robodog/{health_key('nav')}"


# ---------------------------------------------------------------------- uptime


def test_uptime_seconds():
    assert _format_uptime(3) == "3s"
    assert _format_uptime(59.4) == "59s"


def test_uptime_minutes():
    assert _format_uptime(62) == "1m02s"


def test_uptime_hours():
    assert _format_uptime(3 * 3600 + 7 * 60) == "3h07m"


# ------------------------------------------------------------------------- row


def test_row_shows_mean_and_max_latency():
    row = _health_row(make_health(age_mean_ms=11.5, age_max_ms=87.4), seen_s=0.3)
    assert "11.5/87.4" in row
    assert "nav" in row
    assert "running" in row


def test_row_shows_timer_overruns():
    """A control loop missing its deadline belongs where you already look."""
    row = _health_row(make_health(timer_overruns=17), seen_s=0.3)
    assert "17" in row.split()


def test_row_shows_deadline_misses():
    """A producer that went silent belongs where you already look."""
    row = _health_row(make_health(deadline_misses=4), seen_s=0.3)
    assert "4" in row.split()


def test_header_names_the_miss_column():
    from zenode.cli import _HEALTH_HEADER

    assert "MISS" in _HEALTH_HEADER.split()


def test_row_columns_line_up_with_header():
    from zenode.cli import _HEALTH_HEADER

    row = _health_row(make_health(sent=12345, received=999), seen_s=1.0)
    assert len(row) == len(_HEALTH_HEADER)


# ----------------------------------------------------------------------- drain


def test_drain_keeps_the_latest_per_node():
    inbox: queue.Queue[bytes] = queue.Queue()
    inbox.put(make_health(node="nav", sent=1).model_dump_json().encode())
    inbox.put(make_health(node="nav", sent=2).model_dump_json().encode())
    inbox.put(make_health(node="cam", sent=9).model_dump_json().encode())
    latest: dict = {}
    _drain_health(inbox, latest)
    assert set(latest) == {"nav", "cam"}
    assert latest["nav"][0].sent == 2


def test_drain_ignores_foreign_payloads():
    """A matching key expression is not a promise about the payload."""
    inbox: queue.Queue[bytes] = queue.Queue()
    inbox.put(b'{"not":"health"}')
    inbox.put(b"not even json")
    inbox.put(make_health(node="nav").model_dump_json().encode())
    latest: dict = {}
    _drain_health(inbox, latest)
    assert set(latest) == {"nav"}


# ---------------------------------------------------------------------- render


def test_render_reports_emptiness_rather_than_a_bare_header(capsys):
    _render_health({}, clear=False)
    out = capsys.readouterr().out
    assert "no node health seen" in out
    assert "NODE" not in out


def test_render_clears_the_screen_only_on_a_tty(monkeypatch, capsys):
    """``--watch`` redraws in place on a terminal, but must not spray escapes
    into a log file or a pipe."""
    latest = {"nav": (make_health(), 0.0)}
    monkeypatch.setattr(sys.stdout, "isatty", lambda: True, raising=False)
    _render_health(latest, clear=True)
    assert "\033[H\033[2J" in capsys.readouterr().out

    monkeypatch.setattr(sys.stdout, "isatty", lambda: False, raising=False)
    _render_health(latest, clear=True)
    assert "\033[" not in capsys.readouterr().out


def test_render_sorts_nodes(capsys):
    latest = {
        "nav": (make_health(node="nav"), 0.0),
        "cam": (make_health(node="cam"), 0.0),
    }
    _render_health(latest, clear=False)
    lines = capsys.readouterr().out.splitlines()
    assert lines[1].startswith("cam")
    assert lines[2].startswith("nav")
