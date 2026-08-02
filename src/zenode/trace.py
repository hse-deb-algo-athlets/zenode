"""W3C trace context, propagated through zenoh attachments.

A topic declared ``Topic(..., trace=True)`` is a *trace root*: publishing on
it starts a new trace. From there the context follows the data — a subscriber
runs its handler inside the incoming trace, so anything it publishes (or any
service it calls) carries the same trace id. Topics that are not roots never
start a trace but always continue an active one, so the sampling decision is
made once, at the source. Messages outside a trace carry no context and pay
no cost.

The wire format is `W3C traceparent
<https://www.w3.org/TR/trace-context/#traceparent-header>`_
(``00-<32 hex trace id>-<16 hex span id>-<2 hex flags>``). Carrying it costs
nothing and needs no dependency: the trace id lands on every log record via
:class:`TraceContextFilter`, which is enough to answer *which camera frame
caused this* with ``grep``.

The span id is only meaningful if something records spans. Install
``zenode[otel]`` and zenode does (see :mod:`zenode.otel`): the id stamped on an
outgoing message becomes the id of the span the publish happened inside, so a
receiver parents onto a span that exists. Without the extra the field is inert
and the trace id still works.
"""

from __future__ import annotations

import contextlib
import logging
import random
import secrets
import string
from collections import deque
from collections.abc import Generator
from contextvars import ContextVar

from . import otel
from .msgs.trace import Hop

_VERSION = "00"
_SAMPLED = "01"
_NOT_SAMPLED = "00"
_TRACE_ID_HEX = 32
_SPAN_ID_HEX = 16
_FLAGS_HEX = 2
_HEX = frozenset(string.hexdigits[:16])  # W3C requires lowercase

_current: ContextVar[str | None] = ContextVar("zenode_traceparent", default=None)


def _is_hex(value: str) -> bool:
    return all(char in _HEX for char in value)


def _parse(traceparent: str | None) -> tuple[str, str, str] | None:
    """Split a traceparent into ``(trace_id, span_id, flags)``, or ``None``.

    Rejects everything W3C calls invalid, including the all-zero trace and span
    ids — a value that otherwise reads as a perfectly well-formed trace nobody
    can ever find.
    """
    if not traceparent:
        return None
    parts = traceparent.split("-")
    if len(parts) != 4:
        return None
    version, trace_id, span_id, flags = parts
    if len(version) != 2 or len(trace_id) != _TRACE_ID_HEX:
        return None
    if len(span_id) != _SPAN_ID_HEX or len(flags) != _FLAGS_HEX:
        return None
    if not (_is_hex(version) and _is_hex(trace_id) and _is_hex(span_id) and _is_hex(flags)):
        return None
    if trace_id == "0" * _TRACE_ID_HEX or span_id == "0" * _SPAN_ID_HEX:
        return None
    return trace_id, span_id, flags


def new_traceparent(*, sampled: bool = True) -> str:
    """Start a new trace: fresh trace id, fresh span id."""
    return (
        f"{_VERSION}-{secrets.token_hex(_TRACE_ID_HEX // 2)}"
        f"-{secrets.token_hex(_SPAN_ID_HEX // 2)}-{_SAMPLED if sampled else _NOT_SAMPLED}"
    )


def root_traceparent(ratio: float = 1.0) -> str:
    """Start a trace, sampled with probability ``ratio``.

    Head-based: the decision is made once, here, at the source, and every hop
    downstream honors it — so a pipeline is either recorded end to end or not at
    all, never half.

    An unsampled trace still gets a real trace id, and
    :class:`TraceContextFilter` still stamps it on every log record. What it
    does not get is spans. "The id is in the logs but not in the backend" is the
    normal state of every sampled tracing system, and it keeps correlation —
    round one's actual payoff — working at full rate on a topic you cannot
    afford to record.
    """
    return new_traceparent(sampled=ratio >= 1.0 or random.random() < ratio)


def trace_id_of(traceparent: str | None) -> str | None:
    """The 32-hex trace id, or ``None`` if absent or malformed (never raises)."""
    parsed = _parse(traceparent)
    return None if parsed is None else parsed[0]


def sampled_of(traceparent: str | None) -> bool:
    """Whether ``traceparent`` carries the W3C sampled flag."""
    parsed = _parse(traceparent)
    return parsed is not None and bool(int(parsed[2], 16) & 0x01)


def current() -> str | None:
    """The traceparent of the message being handled, if any."""
    return _current.get()


def outgoing(fallback: str | None = None) -> str | None:
    """The traceparent to stamp on an outgoing message.

    ``None`` means there is no trace in flight, so no attachment field is added
    and nothing is paid for.

    When something is recording spans, the value comes from the span this call
    happens inside, so the receiver parents onto a span that exists — and, at a
    root, so the trace id is the one the backend knows. With nothing recording
    there are no spans to name, and ``fallback`` (or the active context) passes
    through unchanged: zenode does not mint ids for spans that do not exist.
    """
    return otel.active_traceparent() or fallback or _current.get()


@contextlib.contextmanager
def using(traceparent: str | None) -> Generator[None]:
    """Run a block inside ``traceparent``; restores the previous one after.

    Use it to root a trace by hand — a timer that reads a sensor has no incoming
    message to inherit from::

        with trace.using(trace.new_traceparent()):
            self.frames.put(self.camera.read())
    """
    token = _current.set(traceparent)
    try:
        yield
    finally:
        _current.reset(token)


class TraceRing:
    """A bounded ring of recently handled hops, per node.

    Constant memory by construction — a ``deque`` with ``maxlen`` drops the
    oldest, the same discipline :class:`~zenode.metrics.Latency` was chosen for.
    Nothing here may grow on a robot that runs for a week.

    Only *sampled* traces are recorded: an unsampled trace is one the deployment
    already said it does not want to pay for, and recording it here would make
    ``trace_ratio`` a lie.
    """

    __slots__ = ("_hops",)

    def __init__(self, capacity: int = 4096) -> None:
        # Keyed by trace id alongside the hop rather than inside it: the id is
        # the same for every hop of a trace, and repeating it on the wire in
        # every reply is bytes for nothing.
        self._hops: deque[tuple[str, Hop]] = deque(maxlen=capacity)

    def record(
        self,
        *,
        node: str,
        key: str,
        traceparent: str | None,
        envelope_node: str | None,
        seq: int | None,
        ts_ns: int | None,
        age_ms: float,
        handler_ms: float,
    ) -> None:
        parsed = _parse(traceparent)
        if parsed is None or not bool(int(parsed[2], 16) & 0x01):
            return
        trace_id, span_id, _ = parsed
        self._hops.append(
            (
                trace_id,
                Hop(
                    node=node,
                    key=key,
                    source=envelope_node or "",
                    seq=seq or 0,
                    ts_ns=ts_ns or 0,
                    age_ms=round(age_ms, 3),
                    handler_ms=round(handler_ms, 3),
                    span_id=span_id,
                ),
            )
        )

    def hops(self, trace_id: str) -> list[Hop]:
        return [hop for recorded, hop in self._hops if recorded == trace_id]

    def __len__(self) -> int:
        return len(self._hops)


class TraceContextFilter(logging.Filter):
    """Adds the active trace id to every record as ``trace``.

    Attached to a *handler* rather than a logger, so it also stamps records from
    third-party libraries logging inside a handler. ``setup_logging`` installs
    it; embedders add it to their own handler.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        trace_id = trace_id_of(_current.get())
        if trace_id is not None:
            record.trace = trace_id
        return True
