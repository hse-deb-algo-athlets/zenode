"""Node presence: key derivation, the liveliness query, and the watcher.

Presence is what answers *is it up?*. The keys the token is held on and the
pattern used to look for it are derived from the same function, so a rename
cannot make discovery silently stop finding anything.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import zenoh

from zenode.presence import (
    PresenceWatcher,
    list_nodes,
    node_name_from_key,
    presence_key,
    presence_pattern,
)
from zenode.testing import harness


def liveliness_sample(key: str, kind: zenoh.SampleKind) -> SimpleNamespace:
    """The slice of ``zenoh.Sample`` the watcher reads."""
    return SimpleNamespace(key_expr=key, kind=kind)


def session_with_replies(*replies: object) -> MagicMock:
    session = MagicMock(name="session")
    session.liveliness.return_value.get.return_value = list(replies)
    return session


# ------------------------------------------------------------- key derivation


@pytest.mark.parametrize(
    "namespace,name,expected",
    [
        pytest.param("", "nav", "node/nav", id="root"),
        pytest.param("robodog", "nav", "robodog/node/nav", id="namespaced"),
    ],
)
def test_presence_key(namespace, name, expected):
    assert presence_key(namespace, name) == expected


def test_pattern_matches_the_key_it_derives_from():
    """The token holder and the discoverer must not be able to drift apart."""
    assert presence_pattern("robodog").replace("*", "nav") == presence_key("robodog", "nav")
    assert presence_pattern("").replace("*", "nav") == presence_key("", "nav")


@pytest.mark.parametrize(
    "key,expected",
    [
        pytest.param("node/nav", "nav", id="root"),
        pytest.param("robodog/node/nav", "nav", id="namespaced"),
        pytest.param("a/b/c/node/front-left", "front-left", id="deep-namespace"),
    ],
)
def test_node_name_from_key(key, expected):
    assert node_name_from_key(key) == expected


def test_name_survives_the_key_round_trip():
    assert node_name_from_key(presence_key("robodog", "nav")) == "nav"


# ------------------------------------------------------------------ list_nodes


def test_list_nodes_collects_names_from_replies():
    session = session_with_replies(
        SimpleNamespace(ok=SimpleNamespace(key_expr="robodog/node/nav")),
        SimpleNamespace(ok=SimpleNamespace(key_expr="robodog/node/cam")),
    )
    assert list_nodes(session, "robodog") == {"nav", "cam"}


def test_list_nodes_skips_error_replies():
    """A liveliness query can return errors; they are not node names."""
    session = session_with_replies(
        SimpleNamespace(ok=None),
        SimpleNamespace(ok=SimpleNamespace(key_expr="node/nav")),
    )
    assert list_nodes(session) == {"nav"}


def test_list_nodes_queries_the_namespaced_pattern():
    session = session_with_replies()
    list_nodes(session, "robodog", timeout=0.5)
    call = session.liveliness.return_value.get.call_args
    assert call.args[0] == "robodog/node/*"
    assert call.kwargs["timeout"] == 0.5


# -------------------------------------------------------------------- watcher


@pytest.fixture
def watcher_setup():
    """A watcher wired to a mock session, plus the events its callback saw."""
    events: list[tuple[str, bool]] = []
    session = MagicMock(name="session")
    loop = MagicMock(name="loop")
    # The real watcher hops to the loop; run the callback inline so the
    # assertions stay about *what* was dispatched, not *when*.
    loop.call_soon_threadsafe.side_effect = lambda fn, *args: fn(*args)
    watcher = PresenceWatcher(session, "robodog", lambda n, a: events.append((n, a)), loop)
    return watcher, session, loop, events


def test_watcher_replays_current_state_on_start(watcher_setup):
    """``history=True``: the callback sees who is already up, not just changes."""
    watcher, session, _loop, _events = watcher_setup
    watcher.start()
    call = session.liveliness.return_value.declare_subscriber.call_args
    assert call.args[0] == "robodog/node/*"
    assert call.kwargs["history"] is True


def test_watcher_reports_a_put_as_alive(watcher_setup):
    watcher, _session, _loop, events = watcher_setup
    watcher._on_sample(liveliness_sample("robodog/node/nav", zenoh.SampleKind.PUT))
    assert events == [("nav", True)]


def test_watcher_reports_a_delete_as_gone(watcher_setup):
    """The network retracts the token when a node dies, however it dies."""
    watcher, _session, _loop, events = watcher_setup
    watcher._on_sample(liveliness_sample("robodog/node/nav", zenoh.SampleKind.DELETE))
    assert events == [("nav", False)]


def test_watcher_dispatches_onto_the_loop(watcher_setup):
    """Zenoh calls back on a worker thread; user code must not run there."""
    watcher, _session, loop, _events = watcher_setup
    watcher._on_sample(liveliness_sample("robodog/node/nav", zenoh.SampleKind.PUT))
    loop.call_soon_threadsafe.assert_called_once()


def test_watcher_survives_a_closed_loop(watcher_setup):
    """Shutdown races: a late sample must not raise on a zenoh thread."""
    watcher, _session, loop, _events = watcher_setup
    loop.call_soon_threadsafe.side_effect = RuntimeError("Event loop is closed")
    watcher._on_sample(liveliness_sample("robodog/node/nav", zenoh.SampleKind.PUT))


def test_watcher_stop_undeclares_once(watcher_setup):
    watcher, session, _loop, _events = watcher_setup
    watcher.start()
    inner = session.liveliness.return_value.declare_subscriber.return_value
    watcher.stop()
    watcher.stop()  # idempotent
    inner.undeclare.assert_called_once()


def test_watcher_stop_before_start_is_a_noop(watcher_setup):
    watcher, _session, _loop, _events = watcher_setup
    watcher.stop()


def test_watcher_stop_swallows_undeclare_errors(watcher_setup):
    """Teardown must not raise; the transport may already be gone."""
    watcher, session, _loop, _events = watcher_setup
    watcher.start()
    inner = session.liveliness.return_value.declare_subscriber.return_value
    inner.undeclare.side_effect = RuntimeError("session closed")
    watcher.stop()


# ---------------------------------------------------------------- integration


async def _until(predicate, timeout: float = 3.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() >= deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.02)


@pytest.mark.integration
async def test_watcher_sees_a_real_node_join_and_leave():
    """The whole chain: token declared → zenoh → watcher → user callback."""
    from zenode import Node

    class Blinker(Node):
        name = "blinker"
        health_interval = None

    events: list[tuple[str, bool]] = []

    def on_change(name: str, alive: bool) -> None:
        events.append((name, alive))

    async with harness() as h:
        watcher = PresenceWatcher(h.session, "", on_change, asyncio.get_running_loop())
        watcher.start()
        try:
            node = await h.start_node(Blinker)
            await _until(lambda: ("blinker", True) in events)
            await h.stop_node(node)
            await _until(lambda: ("blinker", False) in events)
        finally:
            watcher.stop()
