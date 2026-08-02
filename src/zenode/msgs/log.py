"""The log record every node publishes on ``<ns>/node/<name>/log``.

Reading a fleet's logs should not mean one ``ssh`` per machine and one
``journalctl`` per node. The trace id that :mod:`zenode.trace` puts on every
record is what makes eleven streams one story — but only once they are in the
same place, which is what this topic is for.

The shape mirrors :class:`~zenode.log.JsonFormatter`'s output, so the line on
the bus and the line in the file carry the same fields.
"""

from __future__ import annotations

from pydantic import BaseModel

from ..topic import resolve_key


def log_key(name: str) -> str:
    """The key, relative to the namespace, that ``name`` publishes logs on."""
    return f"node/{name}/log"


def log_pattern(namespace: str) -> str:
    """Key expression matching every node's logs in ``namespace``.

    Derived from :func:`log_key` so the publisher and the CLI cannot drift.
    """
    return resolve_key(log_key("*"), namespace)


class LogRecordMsg(BaseModel):
    """One log record, on the bus."""

    node: str
    level: str
    logger: str
    message: str
    ts_ns: int

    line: int = 0
    trace: str | None = None
    """The active trace id, when the record was emitted inside a trace — the
    join key between this record and the same trace on every other node."""
    fields: dict[str, str] = {}
    """``extra`` fields, stringified. Values are rendered rather than typed
    because a log record's extras are arbitrary and this is a wire format."""
