"""Payload codecs: how a message type maps to bytes on the wire.

A codec is part of the topic contract, so publisher and subscriber always
agree on the wire format. The default for Pydantic models is JSON — readable
with any zenoh tool. Binary payloads (camera frames, point clouds) use
:class:`RawCodec` with an explicit wire encoding.
"""

from __future__ import annotations

from typing import Any, Generic, Protocol, TypeVar, runtime_checkable

import zenoh
from pydantic import BaseModel

T = TypeVar("T")
M = TypeVar("M", bound=BaseModel)


@runtime_checkable
class Codec(Protocol[T]):
    """Encodes/decodes one payload type and names the matching wire encoding."""

    @property
    def encoding(self) -> zenoh.Encoding: ...

    def encode(self, value: T) -> bytes: ...

    def decode(self, data: bytes) -> T: ...


class PydanticJsonCodec(Generic[M]):
    """JSON via Pydantic — the default codec for ``BaseModel`` schemas."""

    def __init__(self, model: type[M]) -> None:
        self.model = model

    @property
    def encoding(self) -> zenoh.Encoding:
        return zenoh.Encoding.APPLICATION_JSON

    def encode(self, value: M) -> bytes:
        return value.model_dump_json().encode()

    def decode(self, data: bytes) -> M:
        return self.model.model_validate_json(data)


class RawCodec:
    """``bytes`` passthrough with a declarable wire encoding.

    Use for payloads that are already encoded (JPEG frames, packed point
    clouds, CDR blobs): ``Topic("camera/rgb", bytes, codec=RawCodec(zenoh.Encoding.IMAGE_JPEG))``.
    """

    def __init__(self, encoding: zenoh.Encoding | None = None) -> None:
        self._encoding = (
            encoding if encoding is not None else zenoh.Encoding.APPLICATION_OCTET_STREAM
        )

    @property
    def encoding(self) -> zenoh.Encoding:
        return self._encoding

    def encode(self, value: bytes) -> bytes:
        if not isinstance(value, (bytes, bytearray, memoryview)):
            raise TypeError(f"RawCodec expects bytes, got {type(value).__name__}")
        return bytes(value)

    def decode(self, data: bytes) -> bytes:
        return data


def default_codec(schema: type[Any]) -> Codec[Any]:
    """Pick the default codec for a schema type."""
    if isinstance(schema, type) and issubclass(schema, BaseModel):
        return PydanticJsonCodec(schema)
    if schema is bytes:
        return RawCodec()
    raise TypeError(
        f"No default codec for {schema!r}. Use a pydantic.BaseModel, bytes, "
        f"or pass an explicit codec."
    )
