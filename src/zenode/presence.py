"""Node presence via zenoh liveliness tokens.

Every running node holds a liveliness token at ``<ns>/node/<name>``; the
zenoh network retracts it automatically when the node dies, however it dies.
That gives discovery ("which nodes are up?") and monitoring (join/leave
events) with no heartbeat protocol of our own.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Callable
from typing import Any

import zenoh

from .topic import resolve_key

logger = logging.getLogger(__name__)

PRESENCE_GROUP = "node"


def presence_key(namespace: str, name: str) -> str:
    """The liveliness key held by node ``name`` in ``namespace``."""
    return resolve_key(f"{PRESENCE_GROUP}/{name}", namespace)


def presence_pattern(namespace: str) -> str:
    """Key expression matching every node's presence token in ``namespace``."""
    return resolve_key(f"{PRESENCE_GROUP}/*", namespace)


def node_name_from_key(key: str) -> str:
    """The node name encoded in a presence key."""
    return key.rsplit("/", 1)[-1]


def list_nodes(session: zenoh.Session, namespace: str = "", timeout: float = 1.0) -> set[str]:
    """Blocking: names of currently live nodes. See ``list_nodes_async``."""
    names: set[str] = set()
    for reply in session.liveliness().get(presence_pattern(namespace), timeout=timeout):
        sample = reply.ok
        if sample is not None:
            names.add(node_name_from_key(str(sample.key_expr)))
    return names


async def list_nodes_async(
    session: zenoh.Session, namespace: str = "", timeout: float = 1.0
) -> set[str]:
    """:func:`list_nodes` off the event loop — it blocks for up to ``timeout``."""
    return await asyncio.to_thread(list_nodes, session, namespace, timeout)


class PresenceWatcher:
    """Invoke ``callback(name, alive)`` on the loop when nodes join or leave.

    ``history=True`` replays currently-live tokens on start, so the callback
    sees the full current state, then increments.
    """

    def __init__(
        self,
        session: zenoh.Session,
        namespace: str,
        callback: Callable[[str, bool], None],
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        self._session = session
        self._namespace = namespace
        self._callback = callback
        self._loop = loop
        self._inner: Any = None

    def start(self) -> None:
        self._inner = self._session.liveliness().declare_subscriber(
            presence_pattern(self._namespace), self._on_sample, history=True
        )

    def _on_sample(self, sample: zenoh.Sample) -> None:
        name = node_name_from_key(str(sample.key_expr))
        alive = sample.kind == zenoh.SampleKind.PUT
        with contextlib.suppress(RuntimeError):  # loop already closed
            self._loop.call_soon_threadsafe(self._callback, name, alive)

    def stop(self) -> None:
        if self._inner is not None:
            try:
                self._inner.undeclare()
            except Exception as e:
                logger.debug("undeclare presence watcher failed: %s", e)
            self._inner = None
