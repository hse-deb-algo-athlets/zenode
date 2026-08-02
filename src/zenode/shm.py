"""Shared-memory publishing for large payloads.

A publish normally copies the payload into the transport. For a camera frame
that copy dominates: measured cross-process at 30 Hz, publishing 1080p RGB
(6.2 MB) costs 1.9 ms through the normal path and 0.28 ms through shared
memory — about 6 % of a core versus 0.8 %, before any perception work.

Opt in per topic, with ``Topic(..., shm=True)``, and enable it on the transport
at both ends (``[transport] shared_memory = true``). It is worth it for frames
and point clouds and pointless below a few tens of kilobytes, where the
allocation costs more than the copy it avoids.

Only the publish side changes. A subscriber reads an SHM-backed sample exactly
as it reads any other, so nothing downstream needs to know.

zenoh marks its SHM API unstable (``@_unstable`` in its own type stubs), which
is another reason every failure here degrades rather than propagates: the
surface may change under us, and a node should survive that.

Two properties matter more than throughput:

- **A node never dies because shared memory is unavailable.** Every failure
  falls back to a normal publish and is counted as ``shm_fallbacks`` on
  ``NodeHealth``. A camera that runs at the slower speed beats one that does
  not run.
- **Failures are counted, not silent.** Falling back to a path seven times
  slower without saying so is how a robot quietly misses its deadline.
"""

from __future__ import annotations

import logging
import time
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_POOL_BYTES = 64 * 1024 * 1024
"""Enough for ten 1080p RGB frames. The pool is reclaimed on allocation, so
this bounds frames in flight rather than total throughput — but a larger pool
reclaims less often, and reclaiming is what produces the latency outliers."""

WARN_INTERVAL = 30.0
"""Seconds between repeat warnings, so a persistent failure does not become a
log storm at frame rate."""


def _zenoh_shm() -> Any:
    """``zenoh.shm``, loaded on demand and deliberately untyped.

    zenoh 1.9.0 ships stubs that disagree with the compiled module —
    ``MemoryLayout`` declares an ``alignment`` parameter the runtime rejects —
    and marks the whole surface unstable. Treating it as ``Any`` keeps a wrong
    stub from becoming a build error, and keeps the guards below meaningful
    rather than decorative.
    """
    global _module
    if _module is None:
        import importlib

        _module = importlib.import_module("zenoh.shm")
    return _module


_module: Any = None

_FATAL = (KeyboardInterrupt, SystemExit)
"""Never swallowed. Everything else from zenoh's SHM path is turned into a
fallback — including Rust panics, which surface as ``pyo3_runtime.
PanicException`` deriving from ``BaseException`` rather than ``Exception``, so
an ordinary ``except Exception`` would let them kill the publishing thread."""


class ShmPool:
    """A node's shared-memory provider, created on first use.

    Lazy because most nodes never publish an SHM topic, and creating a provider
    reserves its whole pool up front.
    """

    def __init__(self, size: int = DEFAULT_POOL_BYTES, *, log: logging.Logger = logger) -> None:
        self.size = size
        self.fallbacks = 0
        self._log = log
        self._provider: Any = None
        self._unavailable = False
        self._warned_at = float("-inf")

    def _warn(self, message: str, error: BaseException) -> None:
        now = time.monotonic()
        if now - self._warned_at < WARN_INTERVAL:
            return
        self._warned_at = now
        self._log.warning(
            "%s: %s — publishing normally instead. Shared memory needs "
            "`ulimit -l` above the pool size (see docs/open-telemetry.md)",
            message,
            error,
            extra={"pool_bytes": self.size},
        )

    def _provider_or_none(self) -> Any:
        if self._provider is not None or self._unavailable:
            return self._provider
        try:
            zenoh_shm = _zenoh_shm()
            self._provider = zenoh_shm.ShmProvider.default_backend(
                zenoh_shm.MemoryLayout(self.size)
            )
        except _FATAL:
            raise
        except BaseException as e:
            # Usually RLIMIT_MEMLOCK. Refuse once and stay quiet: retrying per
            # frame would cost more than the copy shared memory was avoiding.
            self._unavailable = True
            self._warn("shared memory unavailable", e)
        return self._provider

    def buffer(self, payload: bytes) -> Any:
        """An SHM buffer holding ``payload``, or ``None`` to publish normally."""
        provider = self._provider_or_none()
        if provider is None:
            self.fallbacks += 1
            return None
        try:
            zenoh_shm = _zenoh_shm()
            # The default policy (JustAlloc) never reclaims, so a pool fills
            # after a handful of frames and every later allocation fails.
            buffer = provider.alloc(
                len(payload),
                zenoh_shm.GarbageCollect(inner_policy=zenoh_shm.Defragment()),
            )
            buffer[0 : len(payload)] = payload
        except _FATAL:
            raise
        except BaseException as e:
            self.fallbacks += 1
            self._warn("shared-memory allocation failed", e)
            return None
        return buffer

    @property
    def available(self) -> bool:
        """Whether a provider exists. ``False`` before first use."""
        return self._provider is not None
