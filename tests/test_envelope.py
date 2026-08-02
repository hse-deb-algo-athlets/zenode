import time

from zenode.envelope import Envelope, decode_envelope, encode_envelope


def test_roundtrip():
    ts = time.time_ns()
    env = decode_envelope(encode_envelope("nav", 7, ts, "00-abc-def-01"))
    assert env == Envelope(node="nav", seq=7, ts_ns=ts, traceparent="00-abc-def-01")


def test_age():
    env = decode_envelope(encode_envelope("nav", 1, time.time_ns() - 500_000_000))
    age = env.age_s()
    assert age is not None and 0.4 < age < 1.0


def test_tolerant_decode():
    assert decode_envelope(None) == Envelope()
    assert decode_envelope(b"") == Envelope()
    assert decode_envelope(b"not json") == Envelope()
    assert decode_envelope(b"[1,2]") == Envelope()
    assert decode_envelope(b'{"n": 42, "s": "x"}') == Envelope()  # wrong types ignored
