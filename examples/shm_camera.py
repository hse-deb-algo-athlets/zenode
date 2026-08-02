"""A camera pipeline over shared memory, and the same one without it.

Run each node in its own process — shared memory only helps between processes::

    python examples/shm_camera.py camera
    python examples/shm_camera.py detector

Add ``--plain`` to both for the comparison; everything else is identical, so the
difference in the reported publish time is the copy shared memory avoids::

    python examples/shm_camera.py camera --plain
    python examples/shm_camera.py detector --plain

Needs ``ulimit -l`` above the pool size. The default is 8 MB, which is barely
one 1080p frame — ``zenode doctor`` checks it, and ``LimitMEMLOCK=infinity`` in
a systemd unit is the deployment fix.
"""

from __future__ import annotations

import asyncio
import statistics
import sys
import time

import zenoh

from zenode import Node, Topic, TransportConfig, every, run, subscribe
from zenode.codec import RawCodec

WIDTH, HEIGHT = 1920, 1080
FRAME_BYTES = WIDTH * HEIGHT * 3
PLAIN = "--plain" in sys.argv

# The only line that differs between the two runs. The subscriber is unchanged
# either way: an SHM-backed sample is read exactly like any other, which is why
# `detector` below never mentions shared memory.
FRAME = Topic(
    "camera/frame",
    bytes,
    codec=RawCodec(zenoh.Encoding.APPLICATION_OCTET_STREAM),
    shm=not PLAIN,
)


def transport() -> TransportConfig:
    """Shared memory has to be enabled at *both* ends to engage."""
    return TransportConfig(shared_memory=not PLAIN)


class Camera(Node):
    name = "shm-camera"

    async def on_start(self) -> None:
        self.frames = self.publisher(FRAME)
        self.buffer = bytearray(FRAME_BYTES)
        self.times: list[float] = []
        self.n = 0
        self.log.info(
            "publishing %.1f MB frames (%s)",
            FRAME_BYTES / 1e6,
            "plain" if PLAIN else "shared memory",
        )

    @every(1 / 30)
    async def grab(self) -> None:
        self.n += 1
        self.buffer[0] = self.n % 256  # so it is not the same frame every time

        started = time.perf_counter()
        self.frames.put(bytes(self.buffer))
        self.times.append((time.perf_counter() - started) * 1000)

        if len(self.times) == 90:  # every three seconds
            # shm_fallbacks counts frames that took the normal path anyway —
            # pool exhausted, or shared memory unavailable. Silence here is the
            # difference between "fast" and "quietly slow".
            self.log.info(
                "publish median %.3f ms over %d frames",
                statistics.median(self.times),
                len(self.times),
                extra={"shm_fallbacks": self._shm.fallbacks},
            )
            self.times.clear()


class Detector(Node):
    name = "shm-detector"

    async def on_start(self) -> None:
        self.received = 0
        self.ages: list[float] = []

    @subscribe(FRAME, mode="latest")
    async def on_frame(self, frame: bytes, envelope) -> None:
        # Nothing here knows about shared memory. `frame` is bytes either way.
        self.received += 1
        age = envelope.age_s()
        if age is not None:
            self.ages.append(age * 1000)
        await asyncio.sleep(0.005)  # stand in for inference

        if self.received % 90 == 0:
            self.log.info(
                "%d frames, %.1f MB each, median age %.3f ms",
                self.received,
                len(frame) / 1e6,
                statistics.median(self.ages),
            )
            self.ages.clear()


NODES = {node.name: node for node in (Camera, Detector)}
ALIASES = {"camera": "shm-camera", "detector": "shm-detector"}


def main() -> None:
    roles = [a for a in sys.argv[1:] if not a.startswith("-")]
    name = ALIASES.get(roles[0]) if roles else None
    if name is None:
        sys.exit(f"usage: {sys.argv[0]} {{camera|detector}} [--plain]")
    run(NODES[name], transport=transport())


if __name__ == "__main__":
    main()
