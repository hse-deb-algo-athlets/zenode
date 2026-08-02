"""Optional OpenTelemetry bridge: spans around the messages zenode already times.

zenode carries a W3C ``traceparent`` on every message without OpenTelemetry (see
:mod:`zenode.trace`), and that correlation — the trace id on every log record —
never depends on this module. What this adds is the *span*: a recorded interval
with a real id, so the traceparent on the wire names something a backend can
resolve, and a multi-hop pipeline assembles into a chain instead of a star.

Install with ``pip install 'zenode[otel]'``, which pulls ``opentelemetry-api``
only — pure Python, one transitive dependency. zenode never constructs a
``TracerProvider``, never selects an exporter, and never reads ``OTEL_*``; where
spans go, and whether they are recorded at all, is the application's decision::

    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    provider = TracerProvider()
    provider.add_span_processor(BatchSpanProcessor(my_exporter))
    trace.set_tracer_provider(provider)     # the whole integration

With the extra installed and no provider registered, every span here is a no-op
object. With the extra absent, every function is a bool check returning a shared
:func:`contextlib.nullcontext`, and nothing in zenode imports OpenTelemetry.

Spans are only created for messages that are actually part of a trace — an
untraced topic pays one contextvar read and nothing else.
"""

from __future__ import annotations

import contextlib
from contextlib import AbstractContextManager
from typing import TYPE_CHECKING, Any

_NULL: AbstractContextManager[Any] = contextlib.nullcontext()
"""Shared no-op context manager. Stateless and reentrant, so one instance
serves every call site that has nothing to record."""

# The import list is written twice on purpose: the type checker resolves it
# unconditionally (the extra is a dev dependency, so the stubs are always
# there), while at runtime it is allowed to fail. Guarding a single try/except
# instead leaves every name "possibly unbound" for the checker.
if TYPE_CHECKING:
    from opentelemetry import trace as _api
    from opentelemetry.trace import SpanKind, Status, StatusCode
    from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

    _ENABLED = True
else:
    try:
        from opentelemetry import trace as _api
        from opentelemetry.trace import SpanKind, Status, StatusCode
        from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

        _ENABLED = True
    except ImportError:
        _ENABLED = False

_TRACER: Any = None
"""A ProxyTracer until a provider is registered, so importing zenode before the
application configures its SDK still produces recorded spans afterwards."""
_PROPAGATOR: Any = None

if _ENABLED:
    _TRACER = _api.get_tracer("zenode")
    _PROPAGATOR = TraceContextTextMapPropagator()

_SYSTEM = "zenoh"


def available() -> bool:
    """Whether the ``otel`` extra is installed. Says nothing about an SDK."""
    return _ENABLED


def active_traceparent() -> str | None:
    """The W3C traceparent of the span in scope, or ``None`` when there is none.

    ``None`` whenever there is nothing real to name — the extra is absent, no
    SDK is registered, or no span is open — in which case callers keep whatever
    they were going to stamp anyway.

    Both ids come from the span, not just the span id: a recording span already
    belongs to a trace, and minting a separate trace id beside it would put the
    logs and the spans of one message in two different traces.
    """
    if not _ENABLED:
        return None
    context = _api.get_current_span().get_span_context()
    if not context.is_valid:
        return None
    flags = "01" if context.trace_flags.sampled else "00"
    return f"00-{format(context.trace_id, '032x')}-{format(context.span_id, '016x')}-{flags}"


def _recordable(traceparent: str | None) -> bool:
    """Whether this message should produce a span at all.

    An unsampled trace must not reach OpenTelemetry: extracting the context,
    running the sampler, and attaching a non-recording span costs most of what
    recording one costs, which would leave ``trace_ratio`` saving almost
    nothing. The flag byte is read here rather than through
    :func:`zenode.trace.sampled_of` because that module imports this one.
    """
    if not traceparent:
        return False
    flags = traceparent.rpartition("-")[2]
    try:
        return bool(int(flags, 16) & 0x01)
    except ValueError:
        return False


def _attributes(key: str, node: str, operation: str) -> dict[str, Any]:
    # OpenTelemetry messaging semantic conventions, so a generic backend groups
    # zenode traces by destination with no per-deployment configuration.
    return {
        "messaging.system": _SYSTEM,
        "messaging.destination.name": key,
        "messaging.operation.type": operation,
        "zenode.node": node,
    }


def producer_span(
    key: str, node: str, seq: int, traceparent: str | None, *, root: bool = False
) -> AbstractContextManager[Any]:
    """Span around one publish.

    ``root`` means this publish *starts* the trace — nothing upstream produced
    it. The span is then a true root and OpenTelemetry assigns the trace id,
    which the caller reads back off the span. Handing it ``traceparent`` instead
    would make the span a child of an id zenode invented and nobody recorded,
    and every trace would arrive at the backend reporting a missing root.

    Otherwise the parent is the span already in scope — the handler this publish
    happens inside — falling back to the wire context when nothing is recording
    yet, as when a trace was rooted by hand with ``trace.using()``.

    ``traceparent`` still decides *whether* to record: an unsampled trace, or no
    trace at all, records nothing.
    """
    if not _ENABLED or not _recordable(traceparent):
        return _NULL
    attributes = _attributes(key, node, "publish")
    attributes["zenode.seq"] = seq
    context = None
    if not root and traceparent and not _api.get_current_span().get_span_context().is_valid:
        context = _PROPAGATOR.extract({"traceparent": traceparent})
    return _TRACER.start_as_current_span(
        f"publish {key}", context=context, kind=SpanKind.PRODUCER, attributes=attributes
    )


def consumer_span(key: str, node: str, traceparent: str | None) -> AbstractContextManager[Any]:
    """Span around one subscription handler, parented from the sender's span."""
    if not _ENABLED or not _recordable(traceparent):
        return _NULL
    return _TRACER.start_as_current_span(
        f"process {key}",
        context=_PROPAGATOR.extract({"traceparent": traceparent}),
        kind=SpanKind.CONSUMER,
        attributes=_attributes(key, node, "process"),
    )


def server_span(key: str, node: str, traceparent: str | None) -> AbstractContextManager[Any]:
    """Span around one service handler, parented from the caller's span."""
    if not _ENABLED or not _recordable(traceparent):
        return _NULL
    return _TRACER.start_as_current_span(
        f"serve {key}",
        context=_PROPAGATOR.extract({"traceparent": traceparent}),
        kind=SpanKind.SERVER,
        attributes=_attributes(key, node, "process"),
    )


def record_error(exc: BaseException) -> None:
    """Mark the span in scope as failed.

    zenode catches handler exceptions and keeps going, so the span would
    otherwise close as successful — the trace would show the failure taking
    time but not that it failed.
    """
    if not _ENABLED:
        return
    span = _api.get_current_span()
    if not span.is_recording():
        return
    span.record_exception(exc)
    span.set_status(Status(StatusCode.ERROR, str(exc)))
