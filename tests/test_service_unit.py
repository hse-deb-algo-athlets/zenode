"""Service server internals: error replies, counters, and query cleanup.

A service failure has to reach the caller as a message, not as a timeout —
that is the whole reason ``_reply`` catches everything it does.
"""

from __future__ import annotations

import asyncio
import logging
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock

import pytest
from pydantic import BaseModel

from zenode.service import ServiceServer, _decode_error, _encode_error
from zenode.topic import Service


class Req(BaseModel):
    value: int = 0


class Rep(BaseModel):
    doubled: int = 0


DOUBLE = Service("unit/double", request=Req, reply=Rep)


def fake_query() -> Any:
    """A ``zenoh.Query`` stand-in that records replies instead of sending them."""
    return MagicMock(name="query")


def make_server(handler, *, service: Service = DOUBLE) -> ServiceServer[Any, Any]:
    return ServiceServer(service, service.key, handler, asyncio.get_running_loop())


def double(req: Req) -> Rep:
    return Rep(doubled=req.value * 2)


# ------------------------------------------------------------- error payloads


def test_error_payload_round_trips():
    assert _decode_error(_encode_error("arm is stowed")) == "arm is stowed"


@pytest.mark.parametrize(
    "payload,expected",
    [
        pytest.param(b'{"error":"boom"}', "boom", id="structured"),
        pytest.param(b"plain text failure", "plain text failure", id="not-json"),
        pytest.param(b'{"code":500}', '{"code":500}', id="json-without-error-key"),
        pytest.param(b'{"error":42}', '{"error":42}', id="error-key-not-a-string"),
        pytest.param(b"", "unknown service error", id="empty"),
        pytest.param(b"\xff\xfe", "��", id="undecodable-bytes"),
    ],
)
def test_error_decoding_never_raises(payload, expected):
    """Whatever the far side sent, the caller gets a string to put in an exception."""
    assert _decode_error(payload) == expected


# ---------------------------------------------------------------- reply paths


async def test_a_successful_call_replies_with_the_encoded_reply():
    server = make_server(double)
    query = fake_query()

    await server._reply(query, b'{"value":21}')

    key, payload = query.reply.call_args.args
    assert key == DOUBLE.key
    assert payload == b'{"doubled":42}'
    assert server.served == 1
    assert server.errors == 0


async def test_async_handlers_are_awaited():
    async def slow_double(req: Req) -> Rep:
        await asyncio.sleep(0)
        return Rep(doubled=req.value * 2)

    server = make_server(slow_double)
    query = fake_query()

    await server._reply(query, b'{"value":3}')

    assert query.reply.call_args.args[1] == b'{"doubled":6}'


async def test_a_query_without_a_payload_gets_an_error_reply():
    """A request with no body is the caller's mistake, not a handler failure."""
    server = make_server(double)
    query = fake_query()

    await server._reply(query, None)

    assert _decode_error(query.reply_err.call_args.args[0]) == "missing request payload"
    assert server.errors == 0


async def test_an_undecodable_request_is_reported_as_a_bad_request():
    server = make_server(double)
    query = fake_query()

    await server._reply(query, b"not json")

    assert "bad request" in _decode_error(query.reply_err.call_args.args[0])
    assert server.errors == 1
    query.reply.assert_not_called()


async def test_a_raising_handler_becomes_an_error_reply(caplog: pytest.LogCaptureFixture):
    """Better a ``ServiceError`` with the message than a silent timeout."""

    def refuse(req: Req) -> Rep:
        raise ValueError("negative input")

    server = make_server(refuse)
    query = fake_query()

    with caplog.at_level(logging.ERROR, logger="zenode.service"):
        await server._reply(query, b'{"value":-1}')

    assert _decode_error(query.reply_err.call_args.args[0]) == "negative input"
    assert server.errors == 1
    assert server.served == 0
    assert any("service handler raised" in r.message for r in caplog.records)


async def test_a_failing_handler_is_still_timed():
    def refuse(req: Req) -> Rep:
        raise ValueError("nope")

    server = make_server(refuse)
    await server._reply(fake_query(), b'{"value":1}')
    assert server.handler_time.count == 1


@pytest.mark.parametrize(
    "data",
    [
        pytest.param(b'{"value":1}', id="success"),
        pytest.param(b"not json", id="bad-request"),
        pytest.param(None, id="no-payload"),
    ],
)
async def test_the_query_is_always_dropped(data):
    """Leaking queries keeps the caller waiting for a reply that never comes."""
    server = make_server(double)
    query = fake_query()
    await server._reply(query, data)
    query.drop.assert_called_once()


# ------------------------------------------------------------ zenoh callback


async def test_the_zenoh_callback_hands_the_query_to_the_loop():
    server = make_server(double)
    query = fake_query()
    query.payload = SimpleNamespace(to_bytes=lambda: b'{"value":4}')
    query.attachment = None

    server._zenoh_callback(query)
    await asyncio.sleep(0.01)  # the coroutine runs on the loop, not the zenoh thread

    assert query.reply.call_args.args[1] == b'{"doubled":8}'


async def test_the_zenoh_callback_tolerates_a_missing_payload():
    server = make_server(double)
    query = fake_query()
    query.payload = None
    query.attachment = None

    server._zenoh_callback(query)
    await asyncio.sleep(0.01)

    assert _decode_error(query.reply_err.call_args.args[0]) == "missing request payload"


# ------------------------------------------------------------------ lifecycle


async def test_undeclare_is_idempotent():
    server = make_server(double)
    inner = MagicMock(name="queryable")
    server._attach(cast(Any, inner))

    server.undeclare()
    server.undeclare()

    inner.undeclare.assert_called_once()


async def test_undeclare_survives_a_transport_that_is_already_gone():
    server = make_server(double)
    inner = MagicMock(name="queryable")
    inner.undeclare.side_effect = RuntimeError("session closed")
    server._attach(cast(Any, inner))

    server.undeclare()  # must not raise during teardown


async def test_undeclare_before_attach_is_a_noop():
    make_server(double).undeclare()
