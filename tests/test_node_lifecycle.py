"""Node lifecycle: construction guards, teardown, and `run()`'s exit codes.

The wiring itself is covered end-to-end in test_integration.py; what is tested
here is everything around it — the errors a node raises when it is used out of
order, that a failed start leaves nothing behind, and that the process entry
point reports the right thing to systemd.
"""

from __future__ import annotations

import asyncio
import logging
import threading

import pytest
from conftest import internals
from pydantic import BaseModel

from zenode import Node, NodeConfig, Service, Topic, run
from zenode.declarative import BINDINGS_ATTR, Binding
from zenode.errors import ConfigError, ContractError, StartTimeout
from zenode.testing import harness, local_transport


class Ping(BaseModel):
    value: int = 0


PING = Topic("lifecycle/ping", Ping)
SVC = Service("lifecycle/svc", request=Ping, reply=Ping)


class RequiredConfig(NodeConfig):
    """A config with no usable default — the node cannot start without a file."""

    speed_limit: float


class Quiet(Node):
    name = "quiet"
    health_interval = None


class NeedsConfig(Node):
    name = "needs-config"
    health_interval = None
    config: RequiredConfig


@pytest.fixture
def no_logging_setup(monkeypatch):
    """``run()`` reconfigures root logging; don't let that leak into the suite."""
    monkeypatch.setattr("zenode.node.setup_logging", lambda *a, **kw: None)


# --------------------------------------------------------------- construction


def test_a_node_must_be_named():
    class Anonymous(Node):
        pass

    with pytest.raises(ContractError, match="class-level `name`"):
        Anonymous()


def test_missing_required_config_is_reported_at_construction():
    """Fail before the transport is touched, and name the model that needs filling."""
    with pytest.raises(ConfigError, match="RequiredConfig"):
        NeedsConfig()


def test_explicit_config_satisfies_a_required_model():
    node = NeedsConfig(config=RequiredConfig(speed_limit=2.0))
    assert node.config.speed_limit == 2.0


def test_namespace_defaults_to_the_transport_namespace():
    node = Quiet(transport=local_transport("robodog"))
    assert node.namespace == "robodog"


def test_explicit_namespace_wins_over_the_transport():
    node = Quiet(transport=local_transport("robodog"), namespace="other")
    assert node.namespace == "other"


@pytest.mark.parametrize("budget", [0, -1.0])
def test_a_nonpositive_start_timeout_is_refused(budget):
    class Impatient(Node):
        name = "impatient"
        start_timeout = budget

    with pytest.raises(ContractError, match="start_timeout"):
        Impatient()


@pytest.mark.parametrize("budget", [0, -1.0])
def test_a_nonpositive_shutdown_timeout_is_refused(budget):
    """Unlike ``start_timeout``, this one has no "wait forever" setting."""

    class Lingering(Node):
        name = "lingering"
        shutdown_timeout = budget

    with pytest.raises(ContractError, match="shutdown_timeout"):
        Lingering()


@pytest.mark.parametrize(
    "relative,absolute,namespace,expected",
    [
        pytest.param("state/x", False, "robo", "robo/state/x", id="prefixed"),
        pytest.param("livox/lidar", True, "robo", "livox/lidar", id="absolute"),
        pytest.param("state/x", False, "", "state/x", id="no-namespace"),
    ],
)
def test_key_resolution(relative, absolute, namespace, expected):
    node = Quiet(namespace=namespace)
    assert node.key(relative, absolute=absolute) == expected


# --------------------------------------------------------------- use-too-soon


def test_session_before_start_explains_itself():
    node = Quiet()
    with pytest.raises(RuntimeError, match="not started"):
        _ = node.session


def _spawn_something(node: Node) -> None:
    coro = asyncio.sleep(0)
    try:
        node.spawn(coro)
    finally:
        coro.close()  # never scheduled: close it so it does not warn


@pytest.mark.parametrize(
    "call",
    [
        pytest.param(lambda n: n.subscribe(PING, print), id="subscribe"),
        pytest.param(lambda n: n.serve(SVC, print), id="serve"),
        pytest.param(_spawn_something, id="spawn"),
    ],
)
def test_wiring_before_start_is_refused(call):
    """These need the event loop; the message points at ``on_start``."""
    with pytest.raises(RuntimeError, match="after start"):
        call(Quiet())


def test_a_binding_on_something_uncallable_is_a_contract_error():
    """Bindings are stamped attributes; say what is wrong instead of a TypeError."""

    class NotAHandler:
        pass

    setattr(NotAHandler, BINDINGS_ATTR, (Binding(kind="subscribe", target=PING),))

    class Broken(Node):
        name = "broken"
        health_interval = None
        handler = NotAHandler()

    assert getattr(Broken.handler, BINDINGS_ATTR, None) is not None
    with pytest.raises(ContractError, match="not callable"):
        Broken()._wire_bindings()


# -------------------------------------------------------------------- helpers


async def test_blocking_runs_off_the_event_loop():
    """Serial ports and cv2 must not stall the loop."""
    node = Quiet()
    worker = await node.blocking(threading.get_ident)
    assert worker != threading.get_ident()


async def test_blocking_forwards_arguments():
    node = Quiet()
    assert await node.blocking(pow, 2, 10) == 1024


async def test_stop_before_start_still_releases_run_until_stopped():
    node = Quiet()
    node.stop()
    await asyncio.wait_for(node.run_until_stopped(), timeout=1.0)


async def test_shutdown_is_idempotent():
    node = Quiet()
    await node.shutdown()
    await node.shutdown()
    assert node.state == "stopped"


def test_health_publish_without_a_heartbeat_is_a_noop():
    Quiet()._publish_health()  # health_interval is None: no publisher exists


# ----------------------------------------------------------------- start/stop


@pytest.mark.integration
async def test_a_failed_start_leaves_nothing_declared():
    """If ``on_start`` raises, the node must not keep a token or publishers."""

    class Exploding(Node):
        name = "exploding"
        health_interval = 0.5

        async def on_start(self) -> None:
            self.publisher(PING)
            raise RuntimeError("bad hardware")

    async with harness() as h:
        node = Exploding(session=h.session, transport=local_transport())
        with pytest.raises(RuntimeError, match="bad hardware"):
            await node.start()

    assert node.state == "stopped"
    assert internals(node).publishers == []
    assert internals(node).token is None


class HalfOpen(Node):
    """A node that acquires hardware in ``on_start`` and dies half-way through."""

    name = "half-open"
    health_interval = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.acquired: list[str] = []
        self.released: list[str] = []

    async def on_start(self) -> None:
        self.acquired.append("axis-0")
        raise TimeoutError("axis 1 did not answer")

    async def on_stop(self) -> None:
        self.released.extend(self.acquired)


@pytest.mark.integration
async def test_on_stop_runs_when_on_start_raises():
    """Whatever ``on_start`` armed before it failed must still be safed."""
    async with harness() as h:
        node = HalfOpen(session=h.session, transport=local_transport())
        with pytest.raises(TimeoutError, match="axis 1"):
            await node.start()

    assert node.released == ["axis-0"], "on_stop must run on the failure path"
    assert node.state == "stopped"


@pytest.mark.integration
async def test_on_stop_runs_once_even_if_shutdown_follows_a_failed_start():
    async with harness() as h:
        node = HalfOpen(session=h.session, transport=local_transport())
        with pytest.raises(TimeoutError):
            await node.start()
        await node.shutdown()

    assert node.released == ["axis-0"]  # not twice


@pytest.mark.integration
async def test_on_stop_is_skipped_when_the_failure_precedes_on_start(
    monkeypatch: pytest.MonkeyPatch,
):
    """``on_stop`` releases what ``on_start`` took; if it never ran, there is nothing."""

    def explode() -> None:
        raise RuntimeError("no session plumbing")

    async with harness() as h:
        node = HalfOpen(session=h.session, transport=local_transport())
        monkeypatch.setattr(node, "_materialize_publishers", explode)
        with pytest.raises(RuntimeError, match="no session plumbing"):
            await node.start()

    assert node.released == []


@pytest.mark.integration
async def test_on_stop_errors_are_logged_not_raised(caplog: pytest.LogCaptureFixture):
    """A broken cleanup must not stop the rest of the teardown."""

    class Messy(Node):
        name = "messy"
        health_interval = None

        async def on_stop(self) -> None:
            raise RuntimeError("could not park the arm")

    async with harness() as h:
        node = await h.start_node(Messy)
        with caplog.at_level(logging.ERROR, logger="zenode.node.messy"):
            await node.shutdown()

    assert node.state == "stopped"
    assert any("on_stop raised" in r.message for r in caplog.records)


@pytest.mark.integration
async def test_a_broken_on_stop_does_not_mask_the_startup_error(caplog: pytest.LogCaptureFixture):
    class Worse(HalfOpen):
        name = "worse"

        async def on_stop(self) -> None:
            raise RuntimeError("the cleanup is broken too")

    async with harness() as h:
        node = Worse(session=h.session, transport=local_transport())
        with (
            caplog.at_level(logging.ERROR, logger="zenode.node.worse"),
            pytest.raises(TimeoutError, match="axis 1"),  # the real cause survives
        ):
            await node.start()

    assert any("on_stop raised" in r.message for r in caplog.records)


# ------------------------------------------------------------- start_timeout


class Wedged(Node):
    """Hardware that never answers — the case ``start_timeout`` exists for."""

    name = "wedged"
    health_interval = None
    start_timeout = 0.2

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.released = False

    async def on_start(self) -> None:
        await asyncio.sleep(30)

    async def on_stop(self) -> None:
        self.released = True


@pytest.mark.integration
async def test_an_on_start_that_overruns_its_timeout_tears_the_node_down():
    """Otherwise the node sits in ``starting`` forever and no supervisor notices."""
    async with harness() as h:
        node = Wedged(session=h.session, transport=local_transport())
        with pytest.raises(StartTimeout, match=r"wedged.*start_timeout=0\.2s"):
            await node.start()

    assert node.released, "the failure path must still run on_stop"
    assert node.state == "stopped"
    assert internals(node).publishers == []
    assert internals(node).token is None


@pytest.mark.integration
async def test_on_starts_own_timeout_error_is_not_relabelled():
    """``asyncio``'s timeout *is* ``TimeoutError``, and so is "the axis did not answer"."""
    async with harness() as h:
        node = HalfOpen(session=h.session, transport=local_transport())
        with pytest.raises(TimeoutError, match="axis 1") as raised:
            await node.start()

    assert not isinstance(raised.value, StartTimeout)


@pytest.mark.integration
async def test_start_timeout_none_waits():
    class Patient(Wedged):
        name = "patient"
        start_timeout = None

        async def on_start(self) -> None:
            await asyncio.sleep(0.3)  # longer than Wedged's budget, but none applies

    async with harness() as h:
        node = await h.start_node(Patient)
        assert node.state == "running"


# ------------------------------------------------------------------ single-use


@pytest.mark.integration
async def test_a_stopped_node_refuses_to_start_again():
    """The stop request is latched, so a silent second start would exit at once."""
    async with harness() as h:
        node = await h.start_node(Quiet)
        await node.shutdown()
        with pytest.raises(RuntimeError, match="single-use"):
            await node.start()


@pytest.mark.integration
async def test_starting_a_running_node_again_is_refused():
    """Without the guard this declares a second token, publishers and health timer."""
    async with harness() as h:
        node = await h.start_node(Quiet)
        with pytest.raises(RuntimeError, match="already running"):
            await node.start()


@pytest.mark.integration
async def test_a_failed_start_may_be_retried():
    """A start that failed left nothing declared, so transient hardware gets another go."""
    attempts: list[int] = []

    class Flaky(Node):
        name = "flaky"
        health_interval = None

        async def on_start(self) -> None:
            attempts.append(1)
            if len(attempts) == 1:
                raise RuntimeError("usb not ready")

    async with harness() as h:
        node = Flaky(session=h.session, transport=local_transport())
        with pytest.raises(RuntimeError, match="usb not ready"):
            await node.start()
        await node.start()
        assert node.state == "running"
        await node.shutdown()


# --------------------------------------------------------------- teardown bound


@pytest.mark.integration
async def test_teardown_does_not_wait_forever_for_a_stuck_task(caplog: pytest.LogCaptureFixture):
    """A task in ``blocking()`` ignores cancellation; shutdown must not hang on it."""

    class Sticky(Node):
        name = "sticky"
        health_interval = None
        shutdown_timeout = 0.2

    release = asyncio.Event()

    async def stubborn() -> None:
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            await release.wait()  # stands in for an executor thread mid-call

    async with harness() as h:
        node = await h.start_node(Sticky)
        node.spawn(stubborn(), name="stuck")
        await asyncio.sleep(0)  # let it reach the sleep
        with caplog.at_level(logging.WARNING, logger="zenode.node.sticky"):
            await asyncio.wait_for(node.shutdown(), timeout=2.0)
        release.set()
        await asyncio.sleep(0.05)  # let the task retire before the loop goes away

    warnings = [r for r in caplog.records if "still running after shutdown_timeout" in r.message]
    assert warnings, "an overstaying task must be reported, not waited on in silence"
    assert "sticky:stuck" in warnings[0].getMessage()


@pytest.mark.integration
async def test_a_timer_with_on_error_stop_stops_the_node():
    """C2 composes with C1: the node stops, which runs on_stop, which safes the hardware."""

    class BrokenLoop(Node):
        name = "broken-loop"
        health_interval = None

        async def on_start(self) -> None:
            self.every(0.01, self.control_tick, on_error="stop", name="control")

        def control_tick(self) -> None:
            raise RuntimeError("axis 1 is not responding")

    async with harness() as h:
        node = await h.start_node(BrokenLoop)
        await asyncio.wait_for(node.run_until_stopped(), timeout=2.0)
        assert node.timers[0].errors == 1


@pytest.mark.integration
async def test_stop_from_another_thread_wakes_the_loop():
    """Signal handlers and worker threads both call ``stop()``."""
    async with harness() as h:
        node = await h.start_node(Quiet)
        waiter = asyncio.create_task(node.run_until_stopped())
        await asyncio.to_thread(node.stop)
        await asyncio.wait_for(waiter, timeout=2.0)


@pytest.mark.integration
async def test_a_crashing_background_task_is_logged(caplog: pytest.LogCaptureFixture):
    async def boom() -> None:
        raise ValueError("task went bad")

    async with harness() as h:
        node = await h.start_node(Quiet)
        with caplog.at_level(logging.ERROR, logger="zenode.node.quiet"):
            node.spawn(boom(), name="boomer")
            await asyncio.sleep(0.05)

    crashes = [r for r in caplog.records if "background task crashed" in r.message]
    assert crashes, "a crashing spawn() must be reported"
    assert getattr(crashes[0], "task", None) == "quiet:boomer"  # names the task in `extra`


@pytest.mark.integration
async def test_wait_for_nodes_returns_once_they_are_up():
    async with harness() as h:
        watcher = await h.start_node(Quiet)
        await h.start_node(type("Peer", (Node,), {"name": "peer", "health_interval": None}))
        await asyncio.wait_for(watcher.wait_for_nodes({"peer"}, timeout=5.0), timeout=6.0)


@pytest.mark.integration
async def test_wait_for_nodes_names_who_is_missing():
    async with harness() as h:
        node = await h.start_node(Quiet)
        with pytest.raises(TimeoutError, match="ghost"):
            await node.wait_for_nodes({"ghost"}, timeout=0.3, poll=0.05)


# ---------------------------------------------------------------------- run()


@pytest.mark.usefixtures("no_logging_setup")
def test_run_exits_2_on_a_configuration_error():
    """systemd distinguishes "misconfigured" from "crashed"; so must we."""
    instance = Quiet(transport=local_transport())
    with pytest.raises(SystemExit) as exit_info:
        run(instance, transport=local_transport())
    assert exit_info.value.code == 2


@pytest.mark.integration
@pytest.mark.usefixtures("no_logging_setup")
def test_run_exits_1_when_the_node_crashes(caplog: pytest.LogCaptureFixture):
    class Crasher(Node):
        name = "crasher"
        health_interval = None

        async def on_start(self) -> None:
            raise RuntimeError("no camera")

    with (
        caplog.at_level(logging.ERROR, logger="zenode.node.crasher"),
        pytest.raises(SystemExit) as exit_info,
    ):
        run(Crasher, transport=local_transport())

    assert exit_info.value.code == 1  # non-zero: let the supervisor restart us
    assert any("node crashed" in r.message for r in caplog.records)


@pytest.mark.integration
@pytest.mark.usefixtures("no_logging_setup")
def test_run_exits_0_on_a_clean_stop():
    class SelfStopping(Node):
        name = "self-stopping"
        health_interval = None

        async def on_start(self) -> None:
            self.stop()

    with pytest.raises(SystemExit) as exit_info:
        run(SelfStopping, transport=local_transport())
    assert exit_info.value.code == 0


@pytest.mark.integration
@pytest.mark.usefixtures("no_logging_setup")
def test_run_exits_0_on_ctrl_c():
    """Ctrl-C is how a robot operator stops a node; it is not a failure."""

    class Interrupted(Node):
        name = "interrupted"
        health_interval = None

        async def on_start(self) -> None:
            raise KeyboardInterrupt

    with pytest.raises(SystemExit) as exit_info:
        run(Interrupted, transport=local_transport())
    assert exit_info.value.code == 0
