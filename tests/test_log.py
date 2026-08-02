"""Log formatting and setup: the console/JSON split and `extra` handling."""

import io
import json
import logging

import pytest

from zenode.log import HumanFormatter, JsonFormatter, _resolve_format, setup_logging


@pytest.fixture
def restore_root_logging():
    """setup_logging() replaces the root handlers; put them back afterwards."""
    root = logging.getLogger()
    saved_handlers, saved_level = root.handlers[:], root.level
    yield
    root.handlers[:] = saved_handlers
    root.setLevel(saved_level)


def make_record(msg: str = "hello", args: tuple = (), **extra) -> logging.LogRecord:
    record = logging.LogRecord(
        name="zenode.node.nav",
        level=logging.INFO,
        pathname="nav.py",
        lineno=42,
        msg=msg,
        args=args,
        exc_info=None,
    )
    record.__dict__.update(extra)
    return record


# ------------------------------------------------------------------- human


def test_human_appends_extra_fields():
    line = HumanFormatter().format(make_record(key="state/odometry"))
    assert "hello" in line
    assert "key=state/odometry" in line


def test_human_omits_separator_without_extras():
    line = HumanFormatter().format(make_record())
    assert line.endswith("hello")


def test_human_interpolates_lazily():
    line = HumanFormatter().format(make_record("dropped %s on %s", ("bad json", "cmd")))
    assert "dropped bad json on cmd" in line


def test_human_puts_extras_before_traceback():
    try:
        raise ValueError("boom")
    except ValueError:
        import sys

        record = make_record(key="cmd/vel")
        record.exc_info = sys.exc_info()
    line = HumanFormatter().format(record)
    assert line.index("key=cmd/vel") < line.index("Traceback")


# -------------------------------------------------------------------- json


def test_json_is_one_object_per_line():
    line = JsonFormatter().format(make_record(key="state/odometry"))
    assert "\n" not in line
    payload = json.loads(line)
    assert payload["message"] == "hello"
    assert payload["level"] == "INFO"
    assert payload["logger"] == "zenode.node.nav"
    assert payload["line"] == 42
    assert payload["key"] == "state/odometry"


def test_json_standard_fields_win_over_clashing_extra():
    payload = json.loads(JsonFormatter().format(make_record(level="TOTALLY-WRONG")))
    assert payload["level"] == "INFO"


def test_json_survives_unserializable_extra():
    payload = json.loads(JsonFormatter().format(make_record(blob=b"\x00\xff")))
    assert isinstance(payload["blob"], str)


def test_json_includes_exception():
    try:
        raise ValueError("boom")
    except ValueError:
        import sys

        record = make_record()
        record.exc_info = sys.exc_info()
    payload = json.loads(JsonFormatter().format(record))
    assert "ValueError: boom" in payload["exception"]


# ---------------------------------------------------------------- selection


@pytest.mark.parametrize(
    ("fmt", "tty", "expected"),
    [
        (None, True, "human"),
        (None, False, "json"),
        ("human", False, "human"),
        ("json", True, "json"),
        ("JSON", True, "json"),
    ],
)
def test_resolve_format(fmt, tty, expected, monkeypatch):
    monkeypatch.delenv("ZENODE_LOG_FORMAT", raising=False)
    assert _resolve_format(fmt, tty=tty) == expected


def test_resolve_format_reads_env(monkeypatch):
    monkeypatch.setenv("ZENODE_LOG_FORMAT", "json")
    assert _resolve_format(None, tty=True) == "json"


def test_resolve_format_rejects_typos(monkeypatch):
    monkeypatch.delenv("ZENODE_LOG_FORMAT", raising=False)
    with pytest.raises(ValueError, match="unknown log format"):
        _resolve_format("jsonn", tty=True)


# -------------------------------------------------------------------- setup


@pytest.mark.usefixtures("restore_root_logging")
def test_setup_logging_writes_json_to_stream():
    stream = io.StringIO()
    setup_logging("INFO", stream=stream, fmt="json")
    logging.getLogger("zenode.node.nav").info("started", extra={"namespace": "robodog"})
    payload = json.loads(stream.getvalue().strip())
    assert payload["message"] == "started"
    assert payload["namespace"] == "robodog"


@pytest.mark.usefixtures("restore_root_logging")
def test_setup_logging_honors_level():
    stream = io.StringIO()
    setup_logging("WARNING", stream=stream, fmt="json")
    log = logging.getLogger("zenode.node.nav")
    log.info("filtered out")
    log.warning("kept")
    assert "filtered out" not in stream.getvalue()
    assert "kept" in stream.getvalue()
