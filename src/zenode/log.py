"""Application-side logging setup.

zenode's library code logs through the standard :mod:`logging` module and never
configures it: a node embedded in a larger application inherits that
application's handlers, levels, and formatting, and stays silent until the
application asks for output.

:func:`setup_logging` is the *application* side of that split. ``run()`` calls
it because ``run()`` owns the process; importing zenode as a library runs
nothing here.

Loggers are named after the module (``zenode.pubsub``) or the node
(``zenode.node.nav``), so identity is carried by the logger name rather than by
a bound field. That makes it filterable with plain stdlib configuration::

    logging.getLogger("zenode.node.nav").setLevel(logging.DEBUG)  # one noisy node
    logging.getLogger("zenode.pubsub").setLevel(logging.WARNING)  # quiet the transport

Environment: ``ZENODE_LOG`` (level), ``ZENODE_LOG_FORMAT`` (human/json/auto),
``RUST_LOG`` (zenoh's own, Rust-side) — see docs/configuration.md.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import sys
from collections import deque
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import IO, TYPE_CHECKING

import zenoh

from .msgs.log import LogRecordMsg
from .trace import TraceContextFilter, trace_id_of
from .trace import current as current_traceparent

if TYPE_CHECKING:
    from .pubsub import Publisher

_LEVEL_COLORS = {
    "DEBUG": "\033[36m",
    "INFO": "\033[32m",
    "WARNING": "\033[33m",
    "ERROR": "\033[31m",
    "CRITICAL": "\033[1;31m",
}
_DIM = "\033[2m"
_RESET = "\033[0m"

_STANDARD_FIELDS = frozenset(
    logging.LogRecord(
        name="", level=0, pathname="", lineno=0, msg="", args=(), exc_info=None
    ).__dict__
) | {"message", "asctime"}
"""LogRecord's own attributes. Anything else on a record came from ``extra``."""


class HumanFormatter(logging.Formatter):
    """Console format that renders ``extra`` fields as trailing ``key=value``.

    Structured fields stay visible during development without a second format
    string: ``log.warning("dropping payload: %s", e, extra={"key": key})`` shows
    the key on the console *and* keeps it a discrete field for handlers that
    serialize records.
    """

    def __init__(self, *, color: bool = False) -> None:
        super().__init__(datefmt="%H:%M:%S")
        self._color = color

    def format(self, record: logging.LogRecord) -> str:
        level = f"{record.levelname:<8}"
        origin = f"{record.name}:{record.lineno}"
        if self._color:
            level = f"{_LEVEL_COLORS.get(record.levelname, '')}{level}{_RESET}"
            origin = f"{_DIM}{origin}{_RESET}"
        stamp = f"{self.formatTime(record, self.datefmt)}.{int(record.msecs):03d}"
        line = f"{stamp} | {level} | {origin} - {record.getMessage()}"

        extras = " ".join(
            f"{key}={value}"
            for key, value in record.__dict__.items()
            if key not in _STANDARD_FIELDS
        )
        if extras:
            line = f"{line} | {extras}"
        if record.exc_info:
            line = f"{line}\n{self.formatException(record.exc_info)}"
        if record.stack_info:
            line = f"{line}\n{self.formatStack(record.stack_info)}"
        return line


class JsonFormatter(logging.Formatter):
    """One JSON object per line, with ``extra`` fields flattened alongside.

    The standard fields are written *last*, so an ``extra`` key can never
    displace the level or the timestamp. Values that are not JSON-serializable
    (pydantic models, exceptions, ``bytes``) fall back to ``str`` rather than
    raising inside a log call.
    """

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            key: value for key, value in record.__dict__.items() if key not in _STANDARD_FIELDS
        }
        payload.update(
            time=datetime.fromtimestamp(record.created, tz=UTC).isoformat(timespec="milliseconds"),
            level=record.levelname,
            logger=record.name,
            line=record.lineno,
            message=record.getMessage(),
        )
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        if record.stack_info:
            payload["stack"] = self.formatStack(record.stack_info)
        return json.dumps(payload, default=str)


_publishing: ContextVar[bool] = ContextVar("zenode_log_publishing", default=False)
"""Set while a record is being published, so anything the publish path itself
logs is dropped rather than published — the loop guard."""


class LogPublisher(logging.Handler):
    """Publishes log records on the node's own log topic.

    Two properties matter more than throughput:

    - **It never blocks the caller.** ``emit`` appends to a bounded deque and
      wakes a drain task; the zenoh publish happens on the event loop. A
      handler that blocked would block whatever thread logged, including zenoh
      worker threads.
    - **It drops rather than grows.** The deque is capped and drops the oldest,
      counting as it goes, the same discipline ``Subscription`` uses. A log
      transport that silently loses records is worse than one that admits it,
      so ``dropped`` is reported on ``NodeHealth``.

    Installed on the node's own logger (``zenode.node.<name>``) rather than the
    root, so that two nodes sharing a process do not each publish the other's
    records under their own name.
    """

    def __init__(
        self,
        publisher: Publisher[LogRecordMsg],
        node: str,
        loop: asyncio.AbstractEventLoop,
        *,
        capacity: int = 256,
    ) -> None:
        super().__init__()
        self._publisher = publisher
        self._node = node
        self._loop = loop
        self._pending: deque[LogRecordMsg] = deque(maxlen=capacity)
        self._wake = asyncio.Event()
        self._closed = False
        self.dropped = 0

    def emit(self, record: logging.LogRecord) -> None:
        if self._closed or _publishing.get():
            return
        try:
            message = self._to_message(record)
        except Exception:  # pragma: no cover - a record that cannot be rendered
            return
        if len(self._pending) == self._pending.maxlen:
            self.dropped += 1
        self._pending.append(message)
        # emit() runs on whatever thread logged, which is often not the loop.
        with contextlib.suppress(RuntimeError):  # loop already closed (shutdown race)
            self._loop.call_soon_threadsafe(self._wake.set)

    def _to_message(self, record: logging.LogRecord) -> LogRecordMsg:
        fields = {
            key: str(value)
            for key, value in record.__dict__.items()
            if key not in _STANDARD_FIELDS and key != "trace"
        }
        return LogRecordMsg(
            node=self._node,
            level=record.levelname,
            logger=record.name,
            message=record.getMessage(),
            ts_ns=int(record.created * 1e9),
            line=record.lineno,
            trace=trace_id_of(current_traceparent()),
            fields=fields,
        )

    async def drain(self) -> None:
        """Publish pending records as they arrive. Runs for the node's lifetime."""
        while True:
            await self._wake.wait()
            self._wake.clear()
            while self._pending:
                message = self._pending.popleft()
                token = _publishing.set(True)
                try:
                    self._publisher.put(message)
                except Exception:
                    # Nothing to do but keep going: reporting it would log,
                    # and logging here is what the guard exists to stop.
                    pass
                finally:
                    _publishing.reset(token)

    def close(self) -> None:
        self._closed = True
        self._pending.clear()
        super().close()


def _resolve_format(fmt: str | None, *, tty: bool) -> str:
    resolved = (fmt or os.environ.get("ZENODE_LOG_FORMAT", "auto")).lower()
    if resolved == "auto":
        return "human" if tty else "json"
    if resolved not in ("human", "json"):
        raise ValueError(f"unknown log format {resolved!r} (expected 'human', 'json', or 'auto')")
    return resolved


def setup_logging(
    level: str | None = None,
    *,
    stream: IO[str] | None = None,
    fmt: str | None = None,
) -> None:
    """Install zenode's console handler on the root logger.

    Call once per process, from the entry point — ``run()`` does. This replaces
    the root handlers, which is appropriate for code that owns the process and
    wrong for code that does not; library code in zenode never calls it.

    ``level`` defaults to ``$ZENODE_LOG`` (then ``INFO``), ``fmt`` to
    ``$ZENODE_LOG_FORMAT`` (then ``auto``: human on a terminal, JSON otherwise).

    Also initializes zenoh's Rust-side logger, which is separate from Python
    logging and would otherwise stay silent about connectivity problems.
    """
    resolved_level = (level or os.environ.get("ZENODE_LOG", "INFO")).upper()
    target = sys.stderr if stream is None else stream
    resolved_fmt = _resolve_format(fmt, tty=target.isatty())

    handler = logging.StreamHandler(target)
    handler.setFormatter(
        JsonFormatter() if resolved_fmt == "json" else HumanFormatter(color=target.isatty())
    )
    handler.addFilter(TraceContextFilter())
    logging.basicConfig(level=resolved_level, handlers=[handler], force=True)

    # Honors $RUST_LOG; `warn` is where zenoh reports unreachable peers. Idempotent.
    zenoh.init_log_from_env_or("warn")
