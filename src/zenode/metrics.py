"""Bounded latency and resource accounting for the health heartbeat.

Deliberately mean and max rather than percentiles: both are exact, O(1) per
observation, and constant-size — and the max preserves the tail spike that a
percentile would smooth away.

Process CPU and memory come from ``/proc`` directly rather than from ``psutil``.
``psutil`` is a compiled dependency, and a robotics framework lands on ARM
targets where that is the difference between ``pip install`` and an afternoon.
Two file reads and a subtraction are not worth a wheel.
"""

from __future__ import annotations

import os
import time
from collections.abc import Iterable


class Latency:
    """Mean and max of observed durations since the last :meth:`reset`.

    The health heartbeat resets it after reporting, so each heartbeat covers
    the interval since the previous one.
    """

    __slots__ = ("count", "max_s", "total_s")

    def __init__(self) -> None:
        self.reset()

    def observe(self, seconds: float) -> None:
        self.count += 1
        self.total_s += seconds
        if seconds > self.max_s:
            self.max_s = seconds

    def reset(self) -> None:
        self.count = 0
        self.total_s = 0.0
        self.max_s = 0.0


_STAT = "/proc/self/stat"
_STATM = "/proc/self/statm"
_UTIME_INDEX = 11
"""``utime`` is field 14 of ``/proc/self/stat``; fields 1 and 2 are consumed by
splitting after the comm field's closing paren, which may itself contain
spaces, so the remainder starts at field 3."""
_RESIDENT_INDEX = 1
"""``resident`` is the second field of ``/proc/self/statm``, in pages."""


class ProcessStats:
    """CPU and resident memory of this process, sampled from ``/proc``.

    Degrades to ``None`` wherever ``/proc`` is not available — the way
    :meth:`~zenode.Envelope.age_s` degrades without a timestamp — rather than
    raising or pretending zero. Zero CPU and unknown CPU are different answers.
    """

    __slots__ = ("_clock_ticks", "_last_at", "_last_cpu_s", "_page_size", "_supported")

    def __init__(self) -> None:
        self._last_cpu_s: float | None = None
        self._last_at: float | None = None
        try:
            self._clock_ticks = os.sysconf("SC_CLK_TCK")
            self._page_size = os.sysconf("SC_PAGE_SIZE")
            self._supported = os.path.exists(_STAT)
        except (AttributeError, ValueError, OSError):  # not a POSIX platform
            self._clock_ticks = 0
            self._page_size = 0
            self._supported = False

    def _cpu_seconds(self) -> float | None:
        try:
            with open(_STAT) as handle:
                fields = handle.read().rpartition(")")[2].split()
            ticks = int(fields[_UTIME_INDEX]) + int(fields[_UTIME_INDEX + 1])
        except (OSError, IndexError, ValueError):
            return None
        return ticks / self._clock_ticks

    def rss_bytes(self) -> int | None:
        if not self._supported:
            return None
        try:
            with open(_STATM) as handle:
                pages = int(handle.read().split()[_RESIDENT_INDEX])
        except (OSError, IndexError, ValueError):
            return None
        return pages * self._page_size

    def cpu_percent(self) -> float | None:
        """CPU since the previous call, as a percentage of *one* core.

        A process saturating two cores reports 200. The first call has no
        interval to divide by and returns ``None``.
        """
        if not self._supported:
            return None
        cpu_s = self._cpu_seconds()
        now = time.monotonic()
        previous_cpu_s, previous_at = self._last_cpu_s, self._last_at
        self._last_cpu_s, self._last_at = cpu_s, now
        if cpu_s is None or previous_cpu_s is None or previous_at is None:
            return None
        elapsed = now - previous_at
        if elapsed <= 0:
            return None
        return round((cpu_s - previous_cpu_s) / elapsed * 100.0, 1)


def summarize(samples: Iterable[Latency]) -> tuple[float, float]:
    """Count-weighted mean and overall max across accumulators, in milliseconds."""
    count = 0
    total_s = 0.0
    max_s = 0.0
    for sample in samples:
        count += sample.count
        total_s += sample.total_s
        max_s = max(max_s, sample.max_s)
    mean_ms = total_s / count * 1000.0 if count else 0.0
    return round(mean_ms, 3), round(max_s * 1000.0, 3)
