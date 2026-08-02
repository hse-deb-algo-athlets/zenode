"""Request/reply services over zenoh queryables.

The server side declares a queryable; the handler runs on the node's event
loop (never on a zenoh thread). Handler exceptions become a structured error
reply — the caller gets a :class:`~zenode.errors.ServiceError` with the
message instead of a silent timeout.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import json
import logging
import time
from collections.abc import Awaitable, Callable
from typing import Any, Generic, TypeVar, cast

import zenoh

from . import otel
from .envelope import decode_envelope, encode_envelope
from .errors import ServiceError, ServiceTimeout
from .metrics import Latency
from .topic import Service
from .trace import TraceRing
from .trace import outgoing as outgoing_traceparent
from .trace import using as using_trace

logger = logging.getLogger(__name__)

Req = TypeVar("Req")
Rep = TypeVar("Rep")

ServiceHandler = Callable[[Req], Rep | Awaitable[Rep]]


def _encode_error(message: str) -> bytes:
    return json.dumps({"error": message}, separators=(",", ":")).encode()


def _decode_error(data: bytes) -> str:
    try:
        parsed = json.loads(data)
        if isinstance(parsed, dict) and isinstance(parsed.get("error"), str):
            return parsed["error"]
    except (ValueError, UnicodeDecodeError):
        pass
    return data.decode(errors="replace") or "unknown service error"


class ServiceServer(Generic[Req, Rep]):
    """Serves one :class:`~zenode.topic.Service` on a queryable."""

    def __init__(
        self,
        service: Service[Req, Rep],
        key: str,
        handler: ServiceHandler[Req, Rep],
        loop: asyncio.AbstractEventLoop,
        *,
        log: logging.Logger = logger,
        node_name: str = "",
        ring: TraceRing | None = None,
    ) -> None:
        self.service = service
        self.key = key
        self._node_name = node_name
        self._ring = ring
        self._handler = handler
        self._loop = loop
        self._log = log
        self._inner: zenoh.Queryable[Any] | None = None
        self.served = 0
        self.errors = 0
        self.handler_time = Latency()

    def _zenoh_callback(self, query: zenoh.Query) -> None:
        # zenoh worker thread: copy out and hop to the loop; the coroutine
        # keeps the query alive until it has replied.
        payload = query.payload
        data = payload.to_bytes() if payload is not None else None
        attachment = query.attachment
        meta = attachment.to_bytes() if attachment is not None else None
        with contextlib.suppress(RuntimeError):  # loop closed during shutdown
            asyncio.run_coroutine_threadsafe(self._handle(query, data, meta), self._loop)

    async def _handle(
        self, query: zenoh.Query, data: bytes | None, meta: bytes | None = None
    ) -> None:
        envelope = decode_envelope(meta)
        traceparent = envelope.traceparent
        started = time.perf_counter()
        with (
            using_trace(traceparent),
            otel.server_span(self.key, self._node_name, traceparent),
        ):
            await self._reply(query, data)
        if self._ring is not None:
            self._ring.record(
                node=self._node_name,
                key=self.key,
                traceparent=traceparent,
                envelope_node=envelope.node,
                seq=envelope.seq,
                ts_ns=envelope.ts_ns,
                age_ms=(envelope.age_s() or 0.0) * 1000.0,
                handler_ms=(time.perf_counter() - started) * 1000.0,
            )

    async def _reply(self, query: zenoh.Query, data: bytes | None) -> None:
        try:
            if data is None:
                query.reply_err(_encode_error("missing request payload"))
                return
            try:
                request = self.service.request_codec.decode(data)
            except Exception as e:
                self.errors += 1
                query.reply_err(_encode_error(f"bad request: {e}"))
                return
            started = time.perf_counter()
            try:
                result = self._handler(request)
                if inspect.isawaitable(result):
                    result = await result
                reply_value = cast(Rep, result)
            except Exception as e:
                self.errors += 1
                otel.record_error(e)
                self._log.exception("service handler raised", extra={"key": self.key})
                query.reply_err(_encode_error(str(e)))
                return
            finally:
                self.handler_time.observe(time.perf_counter() - started)
            query.reply(
                self.key,
                self.service.reply_codec.encode(reply_value),
                encoding=self.service.reply_codec.encoding,
            )
            self.served += 1
        finally:
            with contextlib.suppress(Exception):
                query.drop()

    def _attach(self, inner: zenoh.Queryable[Any]) -> None:
        self._inner = inner

    def undeclare(self) -> None:
        if self._inner is not None:
            try:
                self._inner.undeclare()
            except Exception as e:
                self._log.debug("undeclare queryable failed: %s", e, extra={"key": self.key})
            self._inner = None


async def call_service(
    session: zenoh.Session,
    service: Service[Req, Rep],
    key: str,
    request: Req,
    *,
    timeout: float = 2.0,
    node: str = "",
) -> Rep:
    """Call a service and return the decoded reply.

    Raises :class:`ServiceError` if the server replied with an error and
    :class:`ServiceTimeout` if no reply arrived in ``timeout`` seconds
    (usually: nobody serves this key).

    The caller's identity and active trace context travel in the query
    attachment, so a service call is part of the same trace as the message that
    triggered it.
    """
    loop = asyncio.get_running_loop()
    future: asyncio.Future[bytes] = loop.create_future()

    def _resolve(data: bytes) -> None:
        if not future.done():
            future.set_result(data)

    def _reject(exc: Exception) -> None:
        if not future.done():
            future.set_exception(exc)

    def _on_reply(reply: zenoh.Reply) -> None:
        # zenoh worker thread
        sample = reply.ok
        if sample is not None:
            data = sample.payload.to_bytes()
            loop.call_soon_threadsafe(_resolve, data)
        else:
            err = reply.err
            message = (
                _decode_error(err.payload.to_bytes())
                if err is not None
                else "unknown service error"
            )
            loop.call_soon_threadsafe(_reject, ServiceError(f"{key}: {message}"))

    session.get(
        key,
        handler=_on_reply,
        payload=service.request_codec.encode(request),
        encoding=service.request_codec.encoding,
        attachment=encode_envelope(node, 0, time.time_ns(), outgoing_traceparent()),
        timeout=timeout,
    )
    try:
        data = await asyncio.wait_for(future, timeout)
    except TimeoutError as e:
        raise ServiceTimeout(f"no reply from {key} within {timeout}s") from e
    return service.reply_codec.decode(data)
