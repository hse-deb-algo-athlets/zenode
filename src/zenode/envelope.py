"""Per-message delivery metadata, carried in zenoh attachments.

Payloads stay clean (plain JSON / binary, readable by any zenoh tool); the
runtime's metadata — sender node, sequence number, publish timestamp, and an
optional W3C ``traceparent`` — travels alongside as a compact JSON attachment.
Consumers that don't know zenode simply ignore it.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Envelope:
    """Decoded delivery metadata of one received message."""

    node: str | None = None
    seq: int | None = None
    ts_ns: int | None = None
    traceparent: str | None = None

    def age_s(self, now_ns: int | None = None) -> float | None:
        """Seconds since the sender stamped the message.

        Relies on synchronized clocks between machines (NTP/chrony); on a
        single machine it is exact.
        """
        if self.ts_ns is None:
            return None
        now = time.time_ns() if now_ns is None else now_ns
        return (now - self.ts_ns) / 1e9


def encode_envelope(node: str, seq: int, ts_ns: int, traceparent: str | None = None) -> bytes:
    """Pack delivery metadata for a zenoh attachment.

    Keys are single letters and separators are tight: this rides along with
    *every* message, so the bytes are worth counting.
    """
    data: dict[str, str | int] = {"n": node, "s": seq, "t": ts_ns}
    if traceparent is not None:
        data["tp"] = traceparent
    return json.dumps(data, separators=(",", ":")).encode()


def decode_envelope(data: bytes | None) -> Envelope:
    """Tolerant decode: anything malformed or missing yields empty fields."""
    if not data:
        return Envelope()
    try:
        raw = json.loads(data)
    except (ValueError, UnicodeDecodeError):
        return Envelope()
    if not isinstance(raw, dict):
        return Envelope()
    node = raw.get("n")
    seq = raw.get("s")
    ts_ns = raw.get("t")
    traceparent = raw.get("tp")
    return Envelope(
        node=node if isinstance(node, str) else None,
        seq=seq if isinstance(seq, int) else None,
        ts_ns=ts_ns if isinstance(ts_ns, int) else None,
        traceparent=traceparent if isinstance(traceparent, str) else None,
    )
