"""The standard node-health heartbeat message.

Every node publishes this on ``<ns>/node/<name>/health`` (see
:class:`zenode.node.Node`); liveliness answers *whether* a node is up, health
answers *how well* it is doing.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel

from ..topic import resolve_key

NodeState = Literal["starting", "running", "stopping", "stopped"]


def health_key(name: str) -> str:
    """The key, relative to the namespace, that ``name`` publishes health on."""
    return f"node/{name}/health"


def health_pattern(namespace: str) -> str:
    """Key expression matching every node's health in ``namespace``.

    Derived from :func:`health_key` so the publisher and the CLI cannot drift.
    """
    return resolve_key(health_key("*"), namespace)


class NodeHealth(BaseModel):
    """One node's health heartbeat.

    Counters are cumulative since start; the ``*_ms`` latencies cover the
    interval since the previous heartbeat.
    """

    node: str
    state: NodeState
    uptime_s: float
    sent: int = 0
    received: int = 0
    dropped: int = 0
    stale: int = 0
    handler_errors: int = 0
    """Exceptions raised inside subscription handlers, service handlers, timer
    bodies and ``@on_matching`` hooks — all the places zenode catches and keeps
    going."""
    timer_overruns: int = 0
    """Timer periods missed because a body ran longer than its interval.
    Sustained overruns mean a periodic loop cannot hold its rate."""
    deadline_misses: int = 0
    """Subscriptions that went silent — a producer that stopped, never started,
    or lost its link. Cumulative, one per transition rather than one per second,
    so a rising number is the alert."""
    logs_dropped: int = 0
    """Log records dropped instead of published on this node's log topic,
    because the publish queue was full. A log transport that loses records
    silently is worse than one that says so."""

    shm_fallbacks: int = 0
    """Messages on a ``shm=True`` topic that published through the normal path
    instead — shared memory unavailable, or the pool exhausted. Silently taking
    a seven-times-slower path is how a robot misses its deadline."""

    cpu_percent: float | None = None
    """This process's CPU since the last heartbeat, as a percentage of *one*
    core — a node saturating two cores reports 200. ``None`` where ``/proc`` is
    unavailable, and on the first heartbeat, which has no interval to divide by.
    Unknown and zero are different answers."""
    rss_bytes: int | None = None
    """Resident set size. ``None`` where ``/proc`` is unavailable."""
    queue_max_depth: int = 0
    """Deepest any subscription queue got since the last heartbeat. ``dropped``
    says a queue overflowed; this says one is at 60 of 64 and about to."""

    age_mean_ms: float = 0.0
    """Publish-to-dequeue delay, averaged over subscriptions by message count.
    Measures the network plus this node's queue; relies on synchronized
    clocks between machines, as :meth:`~zenode.Envelope.age_s` does."""
    age_max_ms: float = 0.0
    handler_mean_ms: float = 0.0
    """Time spent inside handlers — subscription and service alike."""
    handler_max_ms: float = 0.0

    ts_ns: int
