"""The ``zenode`` CLI: topics · echo · hz · health · logs · trace · export · nodes · doctor.

Because the contract is introspectable (TopicSet registry), ``echo`` can
decode payloads *typed* when pointed at a contract module — unlike a raw
zenoh subscriber. Point it at your contract with ``--contract``::

    zenode topics --contract robodog_contract.topics
    zenode echo state/odometry --contract robodog_contract.topics -n robodog
    zenode health -n robodog --watch
    zenode nodes -n robodog --watch
    zenode doctor --connect tcp/192.168.4.100:7447

``nodes`` answers *is it up?* from liveliness; ``health`` answers *how well is
it doing?* from the heartbeat every node publishes.
"""

from __future__ import annotations

import argparse
import importlib
import json
import queue
import resource
import sys
import threading
import time
from collections import deque
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import zenoh
from pydantic import ValidationError

from . import __version__
from .config import TransportConfig, find_config_file, load_transport_config
from .envelope import decode_envelope
from .errors import ConfigError
from .exporter import DEFAULT_STALE_AFTER, Registry, make_server
from .msgs.health import NodeHealth, health_pattern
from .msgs.log import LogRecordMsg, log_key, log_pattern
from .msgs.trace import Hop, TraceHops, TraceQuery, trace_pattern
from .otlp_logs import TIMEOUT, OtlpLogShipper
from .otlp_metrics import OtlpMetricShipper
from .presence import list_nodes, node_name_from_key, presence_pattern
from .topic import Topic, find_topic, registered_services, registered_topics, resolve_key


def _load_contracts(modules: list[str]) -> None:
    if not modules:
        return
    cwd = str(Path.cwd())
    if cwd not in sys.path:  # let `--contract my_project.topics` work from the repo root
        sys.path.insert(0, cwd)
    for module in modules:
        importlib.import_module(module)


def _transport_from_args(args: argparse.Namespace) -> TransportConfig:
    transport = load_transport_config(args.config)
    update: dict[str, Any] = {}
    if args.connect:
        update["connect"] = [*transport.connect, *args.connect]
    if args.mode:
        update["mode"] = args.mode
    if args.namespace is not None:
        update["namespace"] = args.namespace
    return transport.model_copy(update=update) if update else transport


def _open_session(transport: TransportConfig) -> zenoh.Session:
    return zenoh.open(transport.to_zenoh_config())


def _resolve_cli_key(key: str, namespace: str, absolute: bool) -> str:
    if absolute or not namespace or key == namespace or key.startswith(f"{namespace}/"):
        return key
    return resolve_key(key, namespace)


def _format_typed(topic: Topic[Any], payload: bytes, pretty: bool) -> str:
    value = topic.codec.decode(payload)
    dump = getattr(value, "model_dump_json", None)
    if dump is not None:
        return dump(indent=2) if pretty else dump()
    return repr(value)


def _format_raw(payload: bytes, pretty: bool) -> str:
    try:
        parsed = json.loads(payload)
    except (ValueError, UnicodeDecodeError):
        preview = payload[:48].hex(" ")
        return f"<{len(payload)} bytes> {preview}{'…' if len(payload) > 48 else ''}"
    return json.dumps(
        parsed, indent=2 if pretty else None, separators=None if pretty else (",", ":")
    )


# --------------------------------------------------------------------- topics


def cmd_topics(args: argparse.Namespace) -> int:
    _load_contracts(args.contract)
    namespace = args.namespace or ""
    topics = list(registered_topics())
    services = list(registered_services())
    if not topics and not services:
        print("no registered topics — pass --contract <module> that defines TopicSets")
        return 1
    if topics:
        print(f"{'KEY':<44} {'SCHEMA':<24} {'FLAGS':<26} OWNER")
        for entry, topic in topics:
            flags = []
            if topic.latched:
                flags.append(f"latched({topic.history})")
            if topic.max_age is not None:
                flags.append(f"max_age={topic.max_age}")
            if topic.trace:
                # Where traces begin is the first thing you want from a
                # contract listing when a pipeline spans five processes.
                flags.append("trace" if topic.trace_ratio >= 1.0 else f"trace@{topic.trace_ratio}")
            if topic.shm:
                flags.append("shm")
            print(
                f"{topic.resolve(namespace):<44} {topic.schema.__name__:<24} "
                f"{','.join(flags) or '-':<26} {entry.owner}.{entry.attr}"
            )
    if services:
        print()
        print(f"{'SERVICE':<44} {'REQUEST':<24} {'REPLY':<24} OWNER")
        for entry, service in services:
            print(
                f"{service.resolve(namespace):<44} {service.request.__name__:<24} "
                f"{service.reply.__name__:<24} {entry.owner}.{entry.attr}"
            )
    return 0


# ----------------------------------------------------------------------- echo


def cmd_echo(args: argparse.Namespace) -> int:
    _load_contracts(args.contract)
    transport = _transport_from_args(args)
    key = _resolve_cli_key(args.key, transport.namespace, args.absolute)
    topic = None if args.raw else find_topic(key, transport.namespace) or find_topic(key)
    samples: queue.Queue[zenoh.Sample] = queue.Queue()
    session = _open_session(transport)
    if topic is not None and topic.latched:
        import zenoh.ext as zext

        sub: Any = zext.declare_advanced_subscriber(
            session,
            key,
            samples.put,
            history=zext.HistoryConfig(detect_late_publishers=True, max_samples=topic.history),
        )
    else:
        sub = session.declare_subscriber(key, samples.put)
    print(f"listening on {key!r}" + (f" (typed: {topic.schema.__name__})" if topic else " (raw)"))
    try:
        while True:
            try:
                sample = samples.get(timeout=0.2)
            except queue.Empty:
                continue
            payload = sample.payload.to_bytes()
            if args.meta:
                attachment = sample.attachment
                env = decode_envelope(attachment.to_bytes() if attachment else None)
                age = env.age_s()
                print(
                    f"-- from={env.node or '?'} seq={env.seq if env.seq is not None else '?'} "
                    f"age={f'{age * 1000:.1f}ms' if age is not None else '?'} key={sample.key_expr}"
                )
            if topic is not None:
                try:
                    print(_format_typed(topic, payload, args.pretty))
                    continue
                except Exception as e:
                    print(f"!! decode failed ({e}); raw:")
            print(_format_raw(payload, args.pretty))
    except KeyboardInterrupt:
        return 0
    finally:
        sub.undeclare()
        session.close()


# ------------------------------------------------------------------------- hz


def cmd_hz(args: argparse.Namespace) -> int:
    transport = _transport_from_args(args)
    key = _resolve_cli_key(args.key, transport.namespace, args.absolute)
    stamps: deque[float] = deque(maxlen=100_000)

    session = _open_session(transport)
    sub = session.declare_subscriber(key, lambda _s: stamps.append(time.monotonic()))
    print(f"measuring rate on {key!r} (window {args.window}s, Ctrl-C to stop)")
    try:
        while True:
            time.sleep(1.0)
            now = time.monotonic()
            while stamps and stamps[0] < now - args.window:
                stamps.popleft()
            n = len(stamps)
            if n < 2:
                print("rate: no samples" if n == 0 else "rate: waiting for more samples")
                continue
            span = stamps[-1] - stamps[0]
            rate = (n - 1) / span if span > 0 else 0.0
            print(f"rate: {rate:6.2f} Hz  ({n} samples in window)")
    except KeyboardInterrupt:
        return 0
    finally:
        sub.undeclare()
        session.close()


# --------------------------------------------------------------------- health


def _format_uptime(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.0f}s"
    if seconds < 3600:
        return f"{int(seconds // 60)}m{int(seconds % 60):02d}s"
    return f"{int(seconds // 3600)}h{int(seconds % 3600 // 60):02d}m"


_HEALTH_HEADER = (
    f"{'NODE':<16} {'STATE':<8} {'UP':>6} {'SEEN':>5} {'CPU%':>6} {'RSS':>7} "
    f"{'SENT':>6} {'RECV':>6} {'QMAX':>5} {'DROP':>5} {'STALE':>5} {'ERR':>4} {'OVER':>4} "
    f"{'MISS':>5} {'MSG AGE ms':>13} {'HANDLER ms':>13}"
)


def _format_bytes(value: int | None) -> str:
    if value is None:
        return "-"
    scaled = float(value)
    for unit in ("B", "K", "M", "G"):
        if scaled < 1024 or unit == "G":
            return f"{scaled:.0f}{unit}" if unit == "B" else f"{scaled:.1f}{unit}"
        scaled /= 1024
    return "-"  # pragma: no cover - unreachable, the loop returns at G


def _health_row(health: NodeHealth, seen_s: float) -> str:
    age = f"{health.age_mean_ms:.1f}/{health.age_max_ms:.1f}"
    handler = f"{health.handler_mean_ms:.1f}/{health.handler_max_ms:.1f}"
    cpu = "-" if health.cpu_percent is None else f"{health.cpu_percent:.1f}"
    return (
        f"{health.node:<16} {health.state:<8} {_format_uptime(health.uptime_s):>6} "
        f"{seen_s:>4.1f}s {cpu:>6} {_format_bytes(health.rss_bytes):>7} "
        f"{health.sent:>6} {health.received:>6} {health.queue_max_depth:>5} "
        f"{health.dropped:>5} {health.stale:>5} {health.handler_errors:>4} "
        f"{health.timer_overruns:>4} {health.deadline_misses:>5} {age:>13} {handler:>13}"
    )


def _drain_health(inbox: queue.Queue[bytes], latest: dict[str, tuple[NodeHealth, float]]) -> None:
    while True:
        try:
            payload = inbox.get_nowait()
        except queue.Empty:
            return
        try:
            health = NodeHealth.model_validate_json(payload)
        except ValidationError:
            continue  # someone else's message on a matching key
        latest[health.node] = (health, time.monotonic())


def _render_health(latest: dict[str, tuple[NodeHealth, float]], *, clear: bool) -> None:
    if clear and sys.stdout.isatty():
        print("\033[H\033[2J", end="")
    if not latest:
        print("no node health seen — is anything running, and is the namespace right?")
        return
    now = time.monotonic()
    print(_HEALTH_HEADER)
    for name in sorted(latest):
        health, at = latest[name]
        print(_health_row(health, now - at))
    # Not a column: normally zero, and a permanent column of zeros teaches you
    # to stop reading it. When it is not zero, `zenode logs` is lying by omission.
    starved = {name: h.logs_dropped for name, (h, _) in latest.items() if h.logs_dropped}
    if starved:
        detail = ", ".join(f"{name}={count}" for name, count in sorted(starved.items()))
        print(f"\n! log records dropped before publishing ({detail}) — `zenode logs` is incomplete")


def cmd_health(args: argparse.Namespace) -> int:
    transport = _transport_from_args(args)
    pattern = health_pattern(transport.namespace)
    inbox: queue.Queue[bytes] = queue.Queue()
    session = _open_session(transport)
    sub = session.declare_subscriber(pattern, lambda s: inbox.put(s.payload.to_bytes()))
    latest: dict[str, tuple[NodeHealth, float]] = {}
    try:
        if args.watch:
            print(f"watching {pattern!r} (Ctrl-C to stop)…")
            while True:
                _drain_health(inbox, latest)
                _render_health(latest, clear=True)
                time.sleep(1.0)
        # One shot: heartbeats are periodic, so wait long enough to hear one.
        deadline = time.monotonic() + args.wait
        while time.monotonic() < deadline:
            _drain_health(inbox, latest)
            time.sleep(0.1)
        _drain_health(inbox, latest)
        _render_health(latest, clear=False)
        return 0 if latest else 1
    except KeyboardInterrupt:
        return 0
    finally:
        sub.undeclare()
        session.close()


# ----------------------------------------------------------------------- logs

_LEVEL_ORDER = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40, "CRITICAL": 50}


def _log_line(record: LogRecordMsg) -> str:
    stamp = datetime.fromtimestamp(record.ts_ns / 1e9, tz=UTC).strftime("%H:%M:%S.%f")[:-3]
    line = f"{stamp} | {record.level:<8} | {record.node:<16} | {record.logger} - {record.message}"
    extras = " ".join(f"{k}={v}" for k, v in record.fields.items())
    if record.trace:
        extras = f"{extras} trace={record.trace}".strip()
    return f"{line} | {extras}" if extras else line


def cmd_logs(args: argparse.Namespace) -> int:
    transport = _transport_from_args(args)
    key = (
        resolve_key(log_key(args.node), transport.namespace)
        if args.node
        else log_pattern(transport.namespace)
    )
    floor = _LEVEL_ORDER.get(args.level.upper(), 0)
    inbox: queue.Queue[bytes] = queue.Queue()
    session = _open_session(transport)
    sub = session.declare_subscriber(key, lambda s: inbox.put(s.payload.to_bytes()))
    print(f"following {key!r} at {args.level.upper()}+ (Ctrl-C to stop)…")
    try:
        while True:
            try:
                payload = inbox.get(timeout=0.2)
            except queue.Empty:
                continue
            try:
                record = LogRecordMsg.model_validate_json(payload)
            except ValidationError:
                continue  # a key expression is not a promise about what is on it
            if _LEVEL_ORDER.get(record.level, 0) < floor:
                continue
            if args.trace and record.trace != args.trace:
                continue
            if args.grep and args.grep not in record.message:
                continue
            print(_log_line(record))
    except KeyboardInterrupt:
        return 0
    finally:
        sub.undeclare()
        session.close()


# --------------------------------------------------------------------- export


def _parse_listen(value: str) -> tuple[str, int]:
    """``:9100``, ``9100`` or ``host:9100``. Defaults to all interfaces."""
    host, _, port = value.rpartition(":")
    try:
        # All interfaces by default: a sidecar exists to be scraped from
        # elsewhere. Pass 127.0.0.1:9100 to keep it on the box.
        return host or "0.0.0.0", int(port)
    except ValueError:
        raise ConfigError(f"--prometheus: expected [host:]port, got {value!r}") from None


def cmd_export(args: argparse.Namespace) -> int:
    """Re-serve health for Prometheus to pull, and push logs to OTLP.

    Both read the same bus, so one sidecar holds one connection to your
    telemetry stack rather than one per node.
    """
    transport = _transport_from_args(args)
    host, port = _parse_listen(args.prometheus)
    registry = Registry(transport.namespace, stale_after=args.stale_after)
    health = health_pattern(transport.namespace)

    session = _open_session(transport)
    subscriptions = [
        session.declare_subscriber(health, lambda s: registry.offer(s.payload.to_bytes()))
    ]
    server = make_server(registry, host, port)
    print(f"scraping {health!r} → http://{host}:{port}/metrics")

    stop = threading.Event()
    threads: list[threading.Thread] = []
    log_shipper: OtlpLogShipper | None = None
    metric_shipper: OtlpMetricShipper | None = None

    if args.otlp_logs:
        log_shipper = OtlpLogShipper(args.otlp_logs, transport.namespace)
        shipper = log_shipper
        logs = log_pattern(transport.namespace)
        subscriptions.append(
            session.declare_subscriber(logs, lambda s: shipper.offer(s.payload.to_bytes()))
        )
        threads.append(threading.Thread(target=log_shipper.run, args=(stop,), daemon=True))
        print(f"shipping {logs!r} → {log_shipper.url}")

    if args.otlp_metrics:
        # Reads the same registry the scrape endpoint serves, so both paths
        # report one set of numbers from one subscription.
        metric_shipper = OtlpMetricShipper(args.otlp_metrics, registry)
        threads.append(threading.Thread(target=metric_shipper.run, args=(stop,), daemon=True))
        print(f"pushing {health!r} → {metric_shipper.url}")

    def _self_stats() -> dict[str, dict[str, int]]:
        """The sidecar's own counters, so a broken pipeline is visible.

        Without these, an unreachable collector stops every signal while the
        dashboard just goes quiet — indistinguishable from an idle robot.
        """
        stats: dict[str, dict[str, int]] = {}
        if log_shipper is not None:
            stats["log_records"] = {
                "shipped": log_shipper.shipped,
                "queue_full": log_shipper.dropped,
                "push_failed": log_shipper.failed,
            }
        if metric_shipper is not None:
            stats["metric_pushes"] = {
                "ok": metric_shipper.pushed,
                "failed": metric_shipper.failed,
            }
        return stats

    registry.self_stats = _self_stats

    for thread in threads:
        thread.start()
    print("Ctrl-C to stop…")
    try:
        server.serve_forever()
        return 0
    except KeyboardInterrupt:
        return 0
    finally:
        server.server_close()
        stop.set()
        for thread in threads:
            thread.join(timeout=TIMEOUT + 1.0)
        for sub in subscriptions:
            sub.undeclare()
        session.close()


# ---------------------------------------------------------------------- trace


def cmd_trace(args: argparse.Namespace) -> int:
    """Assemble one trace from every node's ring — no collector required."""
    transport = _transport_from_args(args)
    pattern = trace_pattern(transport.namespace)
    session = _open_session(transport)
    try:
        replies = session.get(
            pattern,
            payload=TraceQuery(trace_id=args.trace_id).model_dump_json().encode(),
            timeout=args.timeout,
        )
        hops: list[Hop] = []
        for reply in replies:
            sample = reply.ok
            if sample is None:
                continue
            try:
                hops.extend(TraceHops.model_validate_json(sample.payload.to_bytes()).hops)
            except ValidationError:
                continue  # a key expression is not a promise about what answers on it
    finally:
        session.close()

    if not hops:
        print(
            f"no hops for trace {args.trace_id}\n"
            "  — the trace may have aged out of the rings, been unsampled "
            "(see trace_ratio), or\n    the nodes that handled it may no longer be running"
        )
        return 1

    hops.sort(key=lambda hop: hop.ts_ns)
    print(f"TRACE {args.trace_id}")
    for hop in hops:
        source = f"← {hop.source}" if hop.source else ""
        # span is the id that was on the wire. With zenode[otel] recording it
        # names a real span; without it, it is whatever was passed through — so
        # it is shown as data, not as somewhere to go look it up.
        print(
            f"  {hop.node:<14} {hop.key:<26} seq {hop.seq:<7} {source:<14} "
            f"age {hop.age_ms:>7.1f}ms  handler {hop.handler_ms:>7.1f}ms  span {hop.span_id}"
        )
    return 0


# ---------------------------------------------------------------------- nodes


def cmd_nodes(args: argparse.Namespace) -> int:
    transport = _transport_from_args(args)
    session = _open_session(transport)
    try:
        names = sorted(list_nodes(session, transport.namespace, timeout=args.timeout))
        if names:
            print(f"live nodes ({len(names)}):")
            for name in names:
                print(f"  {name}")
        else:
            print("no live nodes found")
        if not args.watch:
            return 0
        events: queue.Queue[tuple[str, bool]] = queue.Queue()
        sub = session.liveliness().declare_subscriber(
            presence_pattern(transport.namespace),
            lambda s: events.put(
                (node_name_from_key(str(s.key_expr)), s.kind == zenoh.SampleKind.PUT)
            ),
            history=False,
        )
        print("watching (Ctrl-C to stop)…")
        try:
            while True:
                try:
                    name, alive = events.get(timeout=0.2)
                except queue.Empty:
                    continue
                print(f"  {'+' if alive else '-'} {name} {'joined' if alive else 'left'}")
        except KeyboardInterrupt:
            return 0
        finally:
            sub.undeclare()
    finally:
        session.close()


# --------------------------------------------------------------------- doctor


def cmd_doctor(args: argparse.Namespace) -> int:
    ok = True

    def check(label: str, good: bool, detail: str = "") -> None:
        nonlocal ok
        ok = ok and good
        mark = "✓" if good else "✗"
        print(f"  {mark} {label}" + (f": {detail}" if detail else ""))

    print(f"zenode {__version__} doctor")
    try:
        config_file = find_config_file(args.config)
        check("config file", True, str(config_file) if config_file else "none (defaults)")
        transport = _transport_from_args(args)
        check(
            "transport",
            True,
            f"mode={transport.mode} connect={transport.connect or '-'} "
            f"namespace={transport.namespace or '-'}",
        )
    except ConfigError as e:
        check("configuration", False, str(e))
        return 1

    try:
        started = time.monotonic()
        session = _open_session(transport)
        check(
            "session open",
            True,
            f"{(time.monotonic() - started) * 1000:.0f}ms, zid={session.zid()}",
        )
    except Exception as e:
        check("session open", False, str(e))
        return 1

    try:
        info = session.info
        routers = list(info.routers_zid())
        peers = list(info.peers_zid())
        check(
            "connectivity",
            transport.mode == "peer" or bool(routers),
            f"{len(routers)} router(s), {len(peers)} peer(s)",
        )

        if transport.multicast_scouting:
            # Our own open session answers scout queries, so on a healthy host
            # this sees at least one hello — zero means multicast is blocked
            # (firewalls like ufw/firewalld commonly drop UDP 7446).
            hellos: list[Any] = []
            scout = zenoh.scout(handler=hellos.append)
            time.sleep(1.5)
            scout.stop()
            scouting_needed = transport.mode == "peer" and not transport.connect
            check(
                "multicast scouting",
                bool(hellos) or not scouting_needed,
                f"{len(hellos)} hello(s) received"
                + (
                    ""
                    if hellos
                    else " — UDP multicast appears blocked (e.g. by ufw/firewalld)."
                    " Allow UDP port 7446, or skip discovery with explicit"
                    " [transport] connect/listen endpoints or a zenoh router"
                ),
            )

        names = sorted(list_nodes(session, transport.namespace, timeout=args.timeout))
        check("live nodes", True, ", ".join(names) if names else "none")
    finally:
        session.close()

    try:
        importlib.import_module("zenoh.shm")
        check("shared memory module", True, "available")
        # The real gate is RLIMIT_MEMLOCK, not the build: a pool larger than it
        # fails, and zenoh reports that as a Rust panic rather than an error.
        soft, _hard = resource.getrlimit(resource.RLIMIT_MEMLOCK)
        unlimited = soft == resource.RLIM_INFINITY
        check(
            "memlock limit",
            unlimited or soft >= 64 * 1024 * 1024,
            "unlimited"
            if unlimited
            else f"{soft // (1024 * 1024)} MiB — raise it (LimitMEMLOCK=infinity) "
            "before using Topic(shm=True) for frames",
        )
    except ImportError:
        check("shared memory module", False, "not available in this build")

    if args.contract:
        try:
            _load_contracts(args.contract)
            n_topics = sum(1 for _ in registered_topics())
            n_services = sum(1 for _ in registered_services())
            check(
                "contract", n_topics + n_services > 0, f"{n_topics} topics, {n_services} services"
            )
        except Exception as e:
            check("contract", False, f"import failed: {e}")

    print("all good" if ok else "problems found")
    return 0 if ok else 1


# ----------------------------------------------------------------------- main


def _add_common(parser: argparse.ArgumentParser, *, contract: bool = True) -> None:
    parser.add_argument("--config", help="config file (default: $ZENODE_CONFIG or ./zenode.toml)")
    parser.add_argument(
        "--connect",
        action="append",
        default=[],
        metavar="ENDPOINT",
        help="zenoh endpoint to connect to, e.g. tcp/host:7447 (repeatable)",
    )
    parser.add_argument("--mode", choices=("peer", "client"), help="override session mode")
    parser.add_argument("-n", "--namespace", help="override deployment namespace")
    if contract:
        parser.add_argument(
            "--contract",
            action="append",
            default=[],
            metavar="MODULE",
            help="python module defining TopicSets, for typed output (repeatable)",
        )


def main(argv: list[str] | None = None) -> None:
    # `echo`, `hz` and the `--watch` loops stream until you interrupt them, and
    # people pipe them into head/grep/tee. Piped stdout is block-buffered by
    # default, so that output sits in a 8 KiB buffer and is *lost* when the
    # process is killed — the command looks like it printed nothing at all.
    reconfigure = getattr(sys.stdout, "reconfigure", None)  # absent if redirected
    if reconfigure is not None:
        reconfigure(line_buffering=True)

    parser = argparse.ArgumentParser(prog="zenode", description=__doc__)
    parser.add_argument("--version", action="version", version=f"zenode {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("topics", help="list the registered contract")
    _add_common(p)
    p.set_defaults(fn=cmd_topics)

    p = sub.add_parser("echo", help="print messages on a key (typed if the contract knows it)")
    p.add_argument("key")
    p.add_argument("--raw", action="store_true", help="skip typed decoding")
    p.add_argument("--meta", action="store_true", help="show envelope metadata per message")
    p.add_argument("--pretty", action="store_true", help="pretty-print JSON")
    p.add_argument("--absolute", action="store_true", help="do not prefix the namespace")
    _add_common(p)
    p.set_defaults(fn=cmd_echo)

    p = sub.add_parser("hz", help="measure the publish rate on a key")
    p.add_argument("key")
    p.add_argument("--window", type=float, default=5.0, help="window in seconds (default 5)")
    p.add_argument("--absolute", action="store_true", help="do not prefix the namespace")
    _add_common(p, contract=False)
    p.set_defaults(fn=cmd_hz)

    p = sub.add_parser("health", help="show the health heartbeat of every live node")
    p.add_argument("--watch", action="store_true", help="keep refreshing the table")
    p.add_argument(
        "--wait", type=float, default=3.0, help="seconds to collect before printing (default 3)"
    )
    _add_common(p, contract=False)
    p.set_defaults(fn=cmd_health)

    p = sub.add_parser("logs", help="follow log records from every node")
    p.add_argument("--node", help="only this node")
    p.add_argument("--level", default="INFO", help="minimum level (default INFO)")
    p.add_argument("--trace", metavar="ID", help="only records carrying this trace id")
    p.add_argument("--grep", metavar="TEXT", help="only records whose message contains TEXT")
    _add_common(p, contract=False)
    p.set_defaults(fn=cmd_logs)

    p = sub.add_parser("export", help="re-serve node health as Prometheus metrics")
    p.add_argument(
        "--prometheus",
        default=":9100",
        metavar="[HOST:]PORT",
        help="address to serve /metrics on (default :9100)",
    )
    p.add_argument(
        "--stale-after",
        type=float,
        default=DEFAULT_STALE_AFTER,
        metavar="SECONDS",
        help=f"drop a node's series after this long without a heartbeat "
        f"(default {DEFAULT_STALE_AFTER:.0f})",
    )
    p.add_argument(
        "--otlp-logs",
        metavar="URL",
        help="also push log records to this OTLP endpoint, e.g. http://localhost:4318",
    )
    p.add_argument(
        "--otlp-metrics",
        metavar="URL",
        help="also push health metrics to this OTLP endpoint. Needed when the "
        "backend receives rather than scrapes, or when the robot cannot be reached",
    )
    _add_common(p, contract=False)
    p.set_defaults(fn=cmd_export)

    p = sub.add_parser("trace", help="show every hop of one trace, across the fleet")
    p.add_argument("trace_id", help="the 32-hex trace id, as it appears in logs")
    p.add_argument("--timeout", type=float, default=2.0, help="query timeout (default 2)")
    _add_common(p, contract=False)
    p.set_defaults(fn=cmd_trace)

    p = sub.add_parser("nodes", help="list live nodes (liveliness tokens)")
    p.add_argument("--watch", action="store_true", help="keep watching join/leave events")
    p.add_argument("--timeout", type=float, default=1.0, help="liveliness query timeout")
    _add_common(p, contract=False)
    p.set_defaults(fn=cmd_nodes)

    p = sub.add_parser("doctor", help="check config, connectivity, and contract health")
    p.add_argument("--timeout", type=float, default=1.0, help="liveliness query timeout")
    _add_common(p)
    p.set_defaults(fn=cmd_doctor)

    args = parser.parse_args(argv)
    sys.exit(args.fn(args))


if __name__ == "__main__":
    main()
