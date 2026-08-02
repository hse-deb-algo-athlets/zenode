from typing import cast

import pytest
import zenoh
from pydantic import BaseModel

from zenode.codec import PydanticJsonCodec, RawCodec, default_codec


class Sample(BaseModel):
    x: float = 1.5
    label: str = "a"


def test_pydantic_json_roundtrip():
    codec = PydanticJsonCodec(Sample)
    value = Sample(x=2.5, label="hello")
    data = codec.encode(value)
    assert codec.decode(data) == value
    assert b'"label"' in data  # plain JSON on the wire


def test_raw_roundtrip():
    codec = RawCodec(zenoh.Encoding.IMAGE_JPEG)
    assert codec.decode(codec.encode(b"\xff\xd8jpeg")) == b"\xff\xd8jpeg"
    assert codec.encoding == zenoh.Encoding.IMAGE_JPEG


def test_raw_rejects_non_bytes():
    with pytest.raises(TypeError):
        RawCodec().encode(cast(bytes, "not bytes"))


def test_default_codec_selection():
    assert isinstance(default_codec(Sample), PydanticJsonCodec)
    assert isinstance(default_codec(bytes), RawCodec)
    with pytest.raises(TypeError):
        default_codec(dict)
