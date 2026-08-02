"""The `zenode` CLI: key resolution, payload formatting, commands, argv wiring.

The commands that own a session are driven against a mocked ``_open_session``
— what is worth testing here is the decision logic (which key, which format,
which exit code), not zenoh's ability to open a socket.
"""

from __future__ import annotations

import sys
import time
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest
from pydantic import BaseModel

from zenode import Service, Topic, TopicSet
from zenode.cli import (
    _format_raw,
    _format_typed,
    _load_contracts,
    _resolve_cli_key,
    _transport_from_args,
    cmd_doctor,
    cmd_echo,
    cmd_health,
    cmd_hz,
    cmd_nodes,
    cmd_topics,
    main,
)
from zenode.envelope import encode_envelope
from zenode.msgs import NodeHealth


class Ping(BaseModel):
    value: int = 0


PING = Topic("test/ping", Ping)


def fake_sample(payload: bytes) -> SimpleNamespace:
    """The slice of ``zenoh.Sample`` the CLI subscribers actually touch."""
    return SimpleNamespace(payload=SimpleNamespace(to_bytes=lambda: payload))


# ------------------------------------------------------------- key resolution


@pytest.mark.parametrize(
    "key,namespace,absolute,expected",
    [
        pytest.param("state/odom", "", False, "state/odom", id="no-namespace"),
        pytest.param("state/odom", "robo", False, "robo/state/odom", id="prefixed"),
        pytest.param("state/odom", "robo", True, "state/odom", id="absolute-flag"),
        pytest.param("robo/state/odom", "robo", False, "robo/state/odom", id="already-prefixed"),
        pytest.param("robo", "robo", False, "robo", id="key-is-the-namespace"),
        pytest.param("**", "robo", False, "robo/**", id="wildcard-prefixed"),
    ],
)
def test_cli_key_resolution(key, namespace, absolute, expected):
    assert _resolve_cli_key(key, namespace, absolute) == expected


def test_prefixing_is_not_applied_twice():
    """Typing the full key and typing the relative key must reach the same place."""
    assert _resolve_cli_key("robo/a/b", "robo", False) == _resolve_cli_key("a/b", "robo", False)


# ----------------------------------------------------------------- formatting


def test_typed_format_is_compact_by_default():
    assert _format_typed(PING, b'{"value": 3}', pretty=False) == '{"value":3}'


def test_typed_format_indents_when_pretty():
    out = _format_typed(PING, b'{"value": 3}', pretty=True)
    assert out.splitlines()[0] == "{"
    assert '"value": 3' in out


def test_typed_format_falls_back_to_repr_for_non_models():
    """A ``bytes`` topic has no ``model_dump_json``; show something anyway."""
    raw_topic = Topic("test/blob", bytes)
    assert _format_typed(raw_topic, b"\x01\x02", pretty=False) == repr(b"\x01\x02")


def test_raw_format_pretty_prints_json():
    out = _format_raw(b'{"a":1}', pretty=True)
    assert out == '{\n  "a": 1\n}'


def test_raw_format_compacts_json_by_default():
    assert _format_raw(b'{"a": 1, "b": 2}', pretty=False) == '{"a":1,"b":2}'


def test_raw_format_describes_binary_payloads():
    """Point-cloud bytes must not scroll the terminal to death."""
    out = _format_raw(b"\x00\x01\x02", pretty=False)
    assert out == "<3 bytes> 00 01 02"


def test_raw_format_truncates_long_binary():
    out = _format_raw(bytes(range(64)), pretty=False)
    assert out.startswith("<64 bytes> 00 01 02")
    assert out.endswith("…")


def test_raw_format_handles_undecodable_bytes():
    assert "<2 bytes>" in _format_raw(b"\xff\xfe", pretty=False)


# ------------------------------------------------------------------ transport


@pytest.mark.usefixtures("no_ambient_config")
def test_transport_defaults_when_nothing_is_given(cli_args):
    transport = _transport_from_args(cli_args())
    assert transport.mode == "peer"
    assert transport.namespace == ""


def test_transport_reads_the_config_file(cli_args, config_file):
    transport = _transport_from_args(cli_args(config=str(config_file)))
    assert transport.mode == "client"
    assert transport.namespace == "robodog"


def test_connect_flag_adds_to_file_endpoints(cli_args, config_file):
    """``--connect`` is additive: a router in the file plus one on the command line."""
    transport = _transport_from_args(
        cli_args(config=str(config_file), connect=["tcp/192.168.4.1:7447"])
    )
    assert transport.connect == ["tcp/10.0.0.1:7447", "tcp/192.168.4.1:7447"]


def test_mode_flag_overrides_the_file(cli_args, config_file):
    assert _transport_from_args(cli_args(config=str(config_file), mode="peer")).mode == "peer"


def test_namespace_flag_overrides_the_file(cli_args, config_file):
    transport = _transport_from_args(cli_args(config=str(config_file), namespace="other"))
    assert transport.namespace == "other"


def test_empty_namespace_flag_is_honored(cli_args, config_file):
    """``-n ''`` means the root namespace, not "unset" — hence the ``is not None`` check."""
    transport = _transport_from_args(cli_args(config=str(config_file), namespace=""))
    assert transport.namespace == ""


# ------------------------------------------------------------------ contracts


def test_loading_no_contracts_leaves_sys_path_alone():
    """No ``--contract`` means no import machinery and no cwd on the path."""
    before = sys.path[:]
    _load_contracts([])
    assert sys.path == before


@pytest.mark.usefixtures("restore_sys_path", "isolated_registry")
def test_contract_module_is_imported_from_the_working_directory(tmp_path, monkeypatch):
    """``--contract my_project.topics`` has to work from a plain repo checkout."""
    (tmp_path / "cli_contract_fixture.py").write_text(
        "from pydantic import BaseModel\n"
        "from zenode import Topic, TopicSet\n"
        "class M(BaseModel):\n"
        "    v: int = 0\n"
        "class T(TopicSet):\n"
        "    thing = Topic('loaded/thing', M)\n"
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.delitem(sys.modules, "cli_contract_fixture", raising=False)

    _load_contracts(["cli_contract_fixture"])

    from zenode.topic import registered_topics

    assert [t.key for _, t in registered_topics()] == ["loaded/thing"]


# --------------------------------------------------------------------- topics


@pytest.mark.usefixtures("isolated_registry")
def test_topics_reports_an_empty_registry_as_a_problem(cli_args, capsys):
    """Exit 1 with a hint: an empty table looks like a broken deployment."""
    assert cmd_topics(cli_args()) == 1
    assert "--contract" in capsys.readouterr().out


@pytest.mark.usefixtures("isolated_registry")
def test_topics_lists_topics_and_services(cli_args, capsys):
    class _Demo(TopicSet):
        moves = Topic("motion/move", Ping)
        double = Service("svc/double", request=Ping, reply=Ping)

    assert cmd_topics(cli_args()) == 0
    out = capsys.readouterr().out
    assert "motion/move" in out
    assert "svc/double" in out
    assert "_Demo.moves" in out  # the declaration site, so you can go fix it


@pytest.mark.usefixtures("isolated_registry")
def test_topics_resolves_keys_against_the_namespace(cli_args, capsys):
    class _Demo(TopicSet):
        moves = Topic("motion/move", Ping)

    cmd_topics(cli_args(namespace="robodog"))
    assert "robodog/motion/move" in capsys.readouterr().out


@pytest.mark.usefixtures("isolated_registry")
def test_topics_shows_delivery_flags(cli_args, capsys):
    """``latched``/``max_age`` change what a subscriber sees; surface them."""

    class _Demo(TopicSet):
        state = Topic("state/x", Ping, latched=True, history=5)
        fresh = Topic("state/y", Ping, max_age=0.5)
        source = Topic("sensors/z", Ping, trace=True)
        thinned = Topic("sensors/w", Ping, trace=True, trace_ratio=0.01)
        shared = Topic("camera/rgb", Ping, shm=True)

    cmd_topics(cli_args())
    out = capsys.readouterr().out
    assert "latched(5)" in out
    assert "max_age=0.5" in out
    # Which topic starts a trace is the first question a multi-process
    # pipeline raises, and the contract is the only place that answers it.
    assert "trace" in out
    # A thinned root is not the same promise as a full one, so it reads
    # differently rather than looking like every other traced topic.
    assert "trace@0.01" in out
    assert "shm" in out


# --------------------------------------------------- driving the watch loops


@pytest.fixture
def mock_session(monkeypatch):
    """Replace ``_open_session`` so command tests never touch the network."""
    session = MagicMock(name="session")
    monkeypatch.setattr("zenode.cli._open_session", MagicMock(return_value=session))
    return session


@pytest.fixture
def scripted_queue(monkeypatch):
    """Feed the commands' inbox a fixed script, then Ctrl-C.

    ``echo``/``hz``/``--watch`` block forever by design. Swapping the ``queue``
    module *in the CLI's namespace only* (not in the stdlib) lets the loop run
    for a known number of iterations and then take the exit it was written for.
    """

    def install(items: list[object]):
        pending = list(items)

        class Empty(Exception):
            pass

        class Queue:
            def __init__(self, *args, **kwargs) -> None:
                pass

            def put(self, item: object) -> None:
                pending.append(item)

            def get(self, timeout: float | None = None) -> object:
                if pending:
                    return pending.pop(0)
                raise KeyboardInterrupt  # the operator hits Ctrl-C

            def get_nowait(self) -> object:
                if not pending:
                    raise Empty
                return pending.pop(0)

        monkeypatch.setattr("zenode.cli.queue", SimpleNamespace(Queue=Queue, Empty=Empty))

    return install


def deliver_samples(session: MagicMock, count: int) -> None:
    """Make the mocked session hand ``count`` samples to the subscriber callback."""
    subscriber = MagicMock(name="subscriber")

    def declare(_key, callback):
        for _ in range(count):
            callback(None)
        return subscriber

    session.declare_subscriber.side_effect = declare


@pytest.fixture
def scripted_clock(monkeypatch):
    """A monotonic clock that reads from a script, and a sleep that Ctrl-Cs.

    ``hz`` sleeps a full second per report; the tests would otherwise be
    dominated by real waiting.
    """

    def install(monotonic_values: list[float], *, sleeps_before_interrupt: int = 1):
        remaining = list(monotonic_values)
        last = [0.0]
        slept = [0]

        def monotonic() -> float:
            if remaining:
                last[0] = remaining.pop(0)
            return last[0]

        def sleep(_seconds: float) -> None:
            slept[0] += 1
            if slept[0] > sleeps_before_interrupt:
                raise KeyboardInterrupt

        monkeypatch.setattr("zenode.cli.time", SimpleNamespace(monotonic=monotonic, sleep=sleep))

    return install


# ---------------------------------------------------------------------- nodes


@pytest.mark.usefixtures("no_ambient_config")
def test_nodes_lists_live_names_sorted(cli_args, mock_session, monkeypatch, capsys):
    monkeypatch.setattr("zenode.cli.list_nodes", MagicMock(return_value={"nav", "cam"}))
    assert cmd_nodes(cli_args(timeout=1.0, watch=False)) == 0
    lines = capsys.readouterr().out.splitlines()
    assert lines[0] == "live nodes (2):"
    assert [line.strip() for line in lines[1:]] == ["cam", "nav"]


@pytest.mark.usefixtures("no_ambient_config")
def test_nodes_says_so_when_nothing_is_up(cli_args, mock_session, monkeypatch, capsys):
    monkeypatch.setattr("zenode.cli.list_nodes", MagicMock(return_value=set()))
    assert cmd_nodes(cli_args(timeout=1.0, watch=False)) == 0
    assert "no live nodes found" in capsys.readouterr().out


@pytest.mark.usefixtures("no_ambient_config")
def test_nodes_closes_the_session(cli_args, mock_session, monkeypatch):
    """A CLI that leaks sessions leaves phantom peers in the network."""
    monkeypatch.setattr("zenode.cli.list_nodes", MagicMock(return_value=set()))
    cmd_nodes(cli_args(timeout=1.0, watch=False))
    mock_session.close.assert_called_once()


@pytest.mark.usefixtures("no_ambient_config")
def test_nodes_watch_narrates_joins_and_leaves(
    cli_args, mock_session, scripted_queue, monkeypatch, capsys
):
    monkeypatch.setattr("zenode.cli.list_nodes", MagicMock(return_value=set()))
    scripted_queue([("nav", True), ("cam", False)])

    assert cmd_nodes(cli_args(timeout=1.0, watch=True)) == 0
    out = capsys.readouterr().out
    assert "+ nav joined" in out
    assert "- cam left" in out


# --------------------------------------------------------------------- health


def health_json(node: str = "nav", **kwargs) -> bytes:
    base = NodeHealth(node=node, state="running", uptime_s=1.0, ts_ns=0)
    return base.model_copy(update=kwargs).model_dump_json().encode()


@pytest.mark.usefixtures("no_ambient_config")
def test_health_one_shot_prints_what_it_heard(cli_args, mock_session, capsys):
    subscriber = MagicMock(name="subscriber")

    def deliver_on_declare(_key, callback):
        callback(fake_sample(health_json()))  # stand in for a heartbeat arriving
        return subscriber

    mock_session.declare_subscriber.side_effect = deliver_on_declare

    assert cmd_health(cli_args(watch=False, wait=0.05)) == 0
    out = capsys.readouterr().out
    assert "NODE" in out
    assert "nav" in out


@pytest.mark.usefixtures("no_ambient_config")
def test_health_exits_nonzero_when_nothing_answers(cli_args, mock_session, capsys):
    """Scriptable: `zenode health` failing means "nobody is publishing health"."""
    assert cmd_health(cli_args(watch=False, wait=0.05)) == 1
    assert "no node health seen" in capsys.readouterr().out


@pytest.mark.usefixtures("no_ambient_config")
def test_health_subscribes_to_the_namespaced_pattern(cli_args, mock_session):
    cmd_health(cli_args(namespace="robodog", watch=False, wait=0.0))
    key = mock_session.declare_subscriber.call_args.args[0]
    assert key == "robodog/node/*/health"


@pytest.mark.usefixtures("no_ambient_config")
def test_health_undeclares_and_closes(cli_args, mock_session):
    cmd_health(cli_args(watch=False, wait=0.0))
    mock_session.declare_subscriber.return_value.undeclare.assert_called_once()
    mock_session.close.assert_called_once()


@pytest.mark.usefixtures("no_ambient_config")
def test_health_watch_redraws_until_interrupted(cli_args, mock_session, scripted_clock, capsys):
    scripted_clock([])
    assert cmd_health(cli_args(watch=True, wait=3.0)) == 0
    out = capsys.readouterr().out
    assert "watching 'node/*/health'" in out
    assert out.count("no node health seen") == 2  # one render per refresh


# ----------------------------------------------------------------------- echo


def echo_sample(payload: bytes, attachment: bytes | None = None, key: str = "test/ping") -> Any:
    return SimpleNamespace(
        key_expr=key,
        payload=SimpleNamespace(to_bytes=lambda: payload),
        attachment=None if attachment is None else SimpleNamespace(to_bytes=lambda: attachment),
    )


@pytest.fixture
def echo_args(cli_args):
    def make(key: str = "test/ping", **overrides):
        defaults = {"raw": False, "meta": False, "pretty": False, "absolute": False}
        return cli_args(key=key, **{**defaults, **overrides})

    return make


@pytest.mark.usefixtures("no_ambient_config", "isolated_registry")
def test_echo_decodes_typed_payloads(echo_args, mock_session, scripted_queue, capsys):
    """The payoff of an introspectable contract: `echo` knows the schema."""

    class _Contract(TopicSet):
        ping = Topic("test/ping", Ping)

    scripted_queue([echo_sample(b'{"value": 42}')])

    assert cmd_echo(echo_args()) == 0
    out = capsys.readouterr().out
    assert "typed: Ping" in out
    assert '{"value":42}' in out


@pytest.mark.usefixtures("no_ambient_config", "isolated_registry")
def test_echo_falls_back_to_raw_json_for_unknown_keys(
    echo_args, mock_session, scripted_queue, capsys
):
    scripted_queue([echo_sample(b'{"anything": 1}', key="some/other/key")])

    assert cmd_echo(echo_args("some/other/key")) == 0
    out = capsys.readouterr().out
    assert "(raw)" in out
    assert '{"anything":1}' in out


@pytest.mark.usefixtures("no_ambient_config", "isolated_registry")
def test_echo_raw_flag_skips_typed_decoding(echo_args, mock_session, scripted_queue, capsys):
    class _Contract(TopicSet):
        ping = Topic("test/ping", Ping)

    scripted_queue([echo_sample(b'{"value": 42}')])

    cmd_echo(echo_args(raw=True))
    assert "typed" not in capsys.readouterr().out


@pytest.mark.usefixtures("no_ambient_config", "isolated_registry")
def test_echo_reports_a_payload_the_schema_rejects(echo_args, mock_session, scripted_queue, capsys):
    """A contract mismatch has to be visible, and must not kill the stream."""

    class _Contract(TopicSet):
        ping = Topic("test/ping", Ping)

    scripted_queue([echo_sample(b'{"value": "not a number"}')])

    assert cmd_echo(echo_args()) == 0
    out = capsys.readouterr().out
    assert "!! decode failed" in out
    assert '{"value":"not a number"}' in out  # raw fallback, so you can still see it


@pytest.mark.usefixtures("no_ambient_config", "isolated_registry")
def test_echo_meta_shows_sender_and_sequence(echo_args, mock_session, scripted_queue, capsys):
    class _Contract(TopicSet):
        ping = Topic("test/ping", Ping)

    attachment = encode_envelope("talker", 7, time.time_ns())
    scripted_queue([echo_sample(b'{"value": 1}', attachment)])

    cmd_echo(echo_args(meta=True))
    out = capsys.readouterr().out
    assert "from=talker" in out
    assert "seq=7" in out
    assert "key=test/ping" in out


@pytest.mark.usefixtures("no_ambient_config", "isolated_registry")
def test_echo_meta_copes_with_a_foreign_publisher(echo_args, mock_session, scripted_queue, capsys):
    """Plain zenoh publishers attach nothing; show '?' rather than crashing."""
    scripted_queue([echo_sample(b"{}", key="foreign/key")])

    assert cmd_echo(echo_args("foreign/key", meta=True)) == 0
    assert "from=? seq=? age=?" in capsys.readouterr().out


@pytest.mark.usefixtures("no_ambient_config", "isolated_registry")
def test_echo_resolves_the_key_against_the_namespace(echo_args, mock_session, scripted_queue):
    scripted_queue([])
    cmd_echo(echo_args("state/odom", namespace="robodog"))
    assert mock_session.declare_subscriber.call_args.args[0] == "robodog/state/odom"


@pytest.mark.usefixtures("no_ambient_config", "isolated_registry")
def test_echo_undeclares_and_closes(echo_args, mock_session, scripted_queue):
    scripted_queue([])
    cmd_echo(echo_args())
    mock_session.declare_subscriber.return_value.undeclare.assert_called_once()
    mock_session.close.assert_called_once()


# ------------------------------------------------------------------------- hz


@pytest.mark.usefixtures("no_ambient_config")
def test_hz_computes_the_rate_over_the_window(cli_args, mock_session, scripted_clock, capsys):
    """Three samples 0.5 s apart is 2 Hz — (n-1) intervals, not n."""
    scripted_clock([0.0, 0.5, 1.0, 1.0])
    deliver_samples(mock_session, 3)

    assert cmd_hz(cli_args(key="test/ping", window=5.0, absolute=False)) == 0
    out = capsys.readouterr().out
    assert "2.00 Hz" in out
    assert "(3 samples in window)" in out


@pytest.mark.usefixtures("no_ambient_config")
def test_hz_forgets_samples_older_than_the_window(cli_args, mock_session, scripted_clock, capsys):
    """The window is what makes `hz` react to a publisher that just stopped."""
    scripted_clock([0.0, 0.5, 10.0, 10.0])
    deliver_samples(mock_session, 3)

    cmd_hz(cli_args(key="test/ping", window=5.0, absolute=False))
    assert "waiting for more samples" in capsys.readouterr().out


@pytest.mark.usefixtures("no_ambient_config")
def test_hz_reports_silence(cli_args, mock_session, scripted_clock, capsys):
    scripted_clock([0.0])
    assert cmd_hz(cli_args(key="test/ping", window=5.0, absolute=False)) == 0
    assert "rate: no samples" in capsys.readouterr().out


# --------------------------------------------------------------------- doctor


def test_doctor_stops_at_a_bad_config_without_opening_a_session(cli_args, capsys):
    assert cmd_doctor(cli_args(config="/nonexistent/zenode.toml", timeout=1.0)) == 1
    out = capsys.readouterr().out
    assert "✗ configuration" in out
    assert "session open" not in out


@pytest.mark.usefixtures("no_ambient_config")
def test_doctor_reports_a_session_that_will_not_open(cli_args, monkeypatch, capsys):
    def explode(_transport):
        raise RuntimeError("no route to host")

    monkeypatch.setattr("zenode.cli._open_session", explode)
    assert cmd_doctor(cli_args(timeout=1.0)) == 1
    assert "✗ session open: no route to host" in capsys.readouterr().out


@pytest.fixture
def quiet_config(tmp_path):
    """A config whose scouting is off, so ``doctor`` skips its 1.5 s scout."""
    path = tmp_path / "zenode.toml"
    path.write_text("[transport]\nmulticast-scouting = false\n")
    return str(path)


def test_doctor_reports_peers_and_live_nodes(
    cli_args, quiet_config, mock_session, monkeypatch, capsys
):
    monkeypatch.setattr("zenode.cli.list_nodes", MagicMock(return_value={"nav"}))
    mock_session.info.routers_zid.return_value = []
    mock_session.info.peers_zid.return_value = ["abc"]

    cmd_doctor(cli_args(config=quiet_config, timeout=1.0))

    out = capsys.readouterr().out
    assert "0 router(s), 1 peer(s)" in out
    assert "live nodes: nav" in out
    mock_session.close.assert_called_once()


def test_doctor_fails_a_client_with_no_router(
    cli_args, tmp_path, mock_session, monkeypatch, capsys
):
    """``mode = client`` without a router is the classic misconfiguration."""
    config = tmp_path / "zenode.toml"
    config.write_text("[transport]\nmode = 'client'\nmulticast-scouting = false\n")
    monkeypatch.setattr("zenode.cli.list_nodes", MagicMock(return_value=set()))
    mock_session.info.routers_zid.return_value = []
    mock_session.info.peers_zid.return_value = []

    assert cmd_doctor(cli_args(config=str(config), timeout=1.0)) == 1
    assert "✗ connectivity" in capsys.readouterr().out


def test_doctor_flags_a_contract_that_does_not_import(
    cli_args, quiet_config, mock_session, monkeypatch, capsys
):
    monkeypatch.setattr("zenode.cli.list_nodes", MagicMock(return_value=set()))

    assert cmd_doctor(cli_args(config=quiet_config, timeout=1.0, contract=["no.such.module"])) == 1
    assert "✗ contract: import failed" in capsys.readouterr().out


@pytest.mark.usefixtures("restore_sys_path", "isolated_registry")
def test_doctor_counts_what_the_contract_declares(
    cli_args, quiet_config, mock_session, monkeypatch, tmp_path, capsys
):
    (tmp_path / "doctor_contract_fixture.py").write_text(
        "from pydantic import BaseModel\n"
        "from zenode import Service, Topic, TopicSet\n"
        "class M(BaseModel):\n"
        "    v: int = 0\n"
        "class T(TopicSet):\n"
        "    a = Topic('doctor/a', M)\n"
        "    b = Topic('doctor/b', M)\n"
        "    s = Service('doctor/s', request=M, reply=M)\n"
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.delitem(sys.modules, "doctor_contract_fixture", raising=False)
    monkeypatch.setattr("zenode.cli.list_nodes", MagicMock(return_value=set()))

    cmd_doctor(cli_args(config=quiet_config, timeout=1.0, contract=["doctor_contract_fixture"]))
    assert "✓ contract: 2 topics, 1 services" in capsys.readouterr().out


# ----------------------------------------------------------------------- main


def test_version_flag_exits_zero(capsys):
    with pytest.raises(SystemExit) as exit_info:
        main(["--version"])
    assert exit_info.value.code == 0
    assert "zenode" in capsys.readouterr().out


def test_a_subcommand_is_required():
    with pytest.raises(SystemExit) as exit_info:
        main([])
    assert exit_info.value.code == 2


def test_exit_code_comes_from_the_command(monkeypatch):
    monkeypatch.setattr("zenode.cli.cmd_topics", lambda _args: 7)
    with pytest.raises(SystemExit) as exit_info:
        main(["topics"])
    assert exit_info.value.code == 7


@pytest.fixture
def captured_args(monkeypatch):
    """Run ``main`` up to dispatch and hand back the parsed namespace."""
    seen: dict[str, object] = {}

    def capture(name):
        def fn(args):
            seen["name"] = name
            seen["args"] = args
            return 0

        return fn

    for command in ("topics", "echo", "hz", "health", "nodes", "doctor"):
        monkeypatch.setattr(f"zenode.cli.cmd_{command}", capture(command))

    def run(argv: list[str]):
        with pytest.raises(SystemExit):
            main(argv)
        return seen["args"]

    return run


def test_echo_defaults(captured_args):
    args = captured_args(["echo", "state/odom"])
    assert args.key == "state/odom"
    assert (args.raw, args.meta, args.pretty, args.absolute) == (False, False, False, False)


def test_echo_flags(captured_args):
    args = captured_args(["echo", "k", "--raw", "--meta", "--pretty", "--absolute"])
    assert (args.raw, args.meta, args.pretty, args.absolute) == (True, True, True, True)


def test_repeatable_flags_accumulate(captured_args):
    args = captured_args(
        ["topics", "--contract", "a.topics", "--contract", "b.topics", "--connect", "tcp/h:7447"]
    )
    assert args.contract == ["a.topics", "b.topics"]
    assert args.connect == ["tcp/h:7447"]


def test_hz_window_default(captured_args):
    assert captured_args(["hz", "k"]).window == 5.0


def test_health_wait_default(captured_args):
    assert captured_args(["health"]).wait == 3.0


def test_nodes_timeout_default(captured_args):
    assert captured_args(["nodes"]).timeout == 1.0


def test_namespace_short_flag(captured_args):
    assert captured_args(["nodes", "-n", "robodog"]).namespace == "robodog"


def test_hz_takes_no_contract_flag():
    """``hz`` counts samples; it never decodes, so a contract would be noise."""
    with pytest.raises(SystemExit):
        main(["hz", "k", "--contract", "a.topics"])


def test_main_makes_stdout_line_buffered(monkeypatch, capsys):
    """`echo`/`hz`/`--watch` stream until interrupted, and people pipe them.

    Piped stdout is block-buffered, so without this their output sits in a
    buffer and is *lost* when the process is killed — the command looks like
    it printed nothing at all, which is exactly how it fails in practice.
    """
    calls: list[bool] = []

    class _Stream:
        def reconfigure(self, *, line_buffering: bool) -> None:
            calls.append(line_buffering)

    monkeypatch.setattr("zenode.cli.sys.stdout", _Stream())
    with pytest.raises(SystemExit):
        main(["--version"])
    assert calls == [True]
