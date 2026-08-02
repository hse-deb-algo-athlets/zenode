"""The flight-recorder messages behind ``zenode trace``.

A trace id is greppable across logs, but "where did this go, and how long did
each hop take" needs the hops themselves. Every node keeps a bounded ring of
recent ones and serves them on ``<ns>/node/<name>/trace``, so the question can
be answered on a robot with no collector deployed — which is the realistic
field deployment.

With ``zenode[otel]`` installed the same trace is also in Jaeger or Tempo, in
more detail; ``span_id`` is the cross-reference. This exists for when it is not.
"""

from __future__ import annotations

from pydantic import BaseModel

from ..topic import resolve_key


def trace_key(name: str) -> str:
    """The key, relative to the namespace, that ``name`` serves its ring on."""
    return f"node/{name}/trace"


def trace_pattern(namespace: str) -> str:
    """Key expression matching every node's trace service in ``namespace``.

    Derived from :func:`trace_key` so the server and the CLI cannot drift.
    """
    return resolve_key(trace_key("*"), namespace)


class Hop(BaseModel):
    """One message, received and handled by one node."""

    node: str
    """Who handled it — the node that recorded this hop."""
    key: str
    source: str = ""
    """Who sent it. The edge that makes a list of hops a tree."""
    seq: int = 0
    ts_ns: int = 0
    """When the *sender* stamped it, so hops sort into causal order."""
    age_ms: float = 0.0
    handler_ms: float = 0.0
    span_id: str = ""
    """The sender's span, when spans are being recorded — paste it into
    whichever backend has the rest of the detail."""


class TraceQuery(BaseModel):
    """Ask one node for its hops of a single trace."""

    trace_id: str


class TraceHops(BaseModel):
    """One node's answer — its own view only.

    ``zenode trace`` queries every node and stitches the replies together; no
    single node sees the whole path.
    """

    node: str
    hops: list[Hop] = []
