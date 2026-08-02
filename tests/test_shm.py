"""Shared-memory publishing: the pool, and its fallbacks."""

import logging
from typing import Any, cast

import pytest

from zenode import Topic
from zenode.shm import ShmPool


class _Boom(BaseException):
    """Stands in for a PyO3 panic, which derives from BaseException and would
    otherwise escape an ordinary `except Exception`."""


# ----------------------------------------------------------------- allocation


def test_buffer_holds_the_payload():
    pool = ShmPool(1 << 20)
    payload = b"\x01\x02\x03" * 1000
    buffer = pool.buffer(payload)

    assert buffer is not None
    assert bytes(buffer) == payload
    assert pool.fallbacks == 0
    assert pool.available


def test_provider_is_created_lazily():
    """Most nodes never publish an SHM topic; a provider reserves its pool."""
    pool = ShmPool(1 << 20)
    assert not pool.available
    pool.buffer(b"x")
    assert pool.available


def test_payload_larger_than_the_pool_falls_back():
    pool = ShmPool(1 << 20)
    assert pool.buffer(b"x" * (4 << 20)) is None
    assert pool.fallbacks == 1


def test_unavailable_provider_falls_back_and_is_only_tried_once(monkeypatch, caplog):
    from zenoh import shm as zenoh_shm

    calls = 0

    def refuse(*args, **kwargs):
        nonlocal calls
        calls += 1
        raise RuntimeError("no memory to lock")

    monkeypatch.setattr(zenoh_shm.ShmProvider, "default_backend", refuse)
    pool = ShmPool(1 << 20)
    with caplog.at_level(logging.WARNING):
        for _ in range(5):
            assert pool.buffer(b"payload") is None

    assert pool.fallbacks == 5
    assert calls == 1, "a failed provider must not be retried per message"
    assert "shared memory unavailable" in caplog.text


def test_a_rust_panic_does_not_escape(monkeypatch):
    """zenoh's SHM alloc can panic out of Rust as a BaseException; a camera node
    must degrade rather than die."""
    from zenoh import shm as zenoh_shm

    monkeypatch.setattr(
        zenoh_shm.ShmProvider, "default_backend", lambda *a, **k: (_ for _ in ()).throw(_Boom())
    )
    pool = ShmPool(1 << 20)
    assert pool.buffer(b"payload") is None
    assert pool.fallbacks == 1


def test_keyboard_interrupt_is_never_swallowed(monkeypatch):
    from zenoh import shm as zenoh_shm

    monkeypatch.setattr(
        zenoh_shm.ShmProvider,
        "default_backend",
        lambda *a, **k: (_ for _ in ()).throw(KeyboardInterrupt()),
    )
    with pytest.raises(KeyboardInterrupt):
        ShmPool(1 << 20).buffer(b"payload")


def test_warnings_are_rate_limited(monkeypatch, caplog):
    pool = ShmPool(1 << 20)
    with caplog.at_level(logging.WARNING):
        for _ in range(20):
            pool.buffer(b"x" * (4 << 20))
    assert caplog.text.count("allocation failed") == 1, "a per-frame failure must not log storm"
    assert pool.fallbacks == 20


# ------------------------------------------------------------------ the topic


def test_topic_defaults_to_no_shm():
    assert Topic("t/plain", bytes).shm is False


def test_publisher_only_uses_the_pool_for_shm_topics():
    from zenode.pubsub import Publisher

    class _Inner:
        def __init__(self):
            self.payloads = []

        def put(self, payload, attachment=None):
            self.payloads.append(payload)

    pool = ShmPool(1 << 20)
    plain = Publisher(
        cast(Any, _Inner()), topic=Topic("t/a", bytes), key="t/a", node_name="n", pool=pool
    )
    shared = Publisher(
        cast(Any, _Inner()),
        topic=Topic("t/b", bytes, shm=True),
        key="t/b",
        node_name="n",
        pool=pool,
    )

    assert plain._pool is None
    assert shared._pool is pool
