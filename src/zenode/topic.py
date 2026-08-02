"""The typed contract: topics and services.

A :class:`Topic` binds a key expression, a payload type, a codec, and
delivery semantics in one declaration. Publisher and subscriber sides both
derive their behavior from the same object, so a mismatch is a type error at
the call site instead of a runtime parse failure in another process.

Topics declared inside a :class:`TopicSet` subclass are collected into a
process-wide registry, which powers the CLI (``zenode topics``, typed
``zenode echo``) and any other introspection.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

from .codec import Codec, default_codec
from .errors import ContractError

T = TypeVar("T")
Req = TypeVar("Req")
Rep = TypeVar("Rep")

_CODEC_UNSET: Any = None
"""Sentinel default for codec fields; replaced by ``default_codec`` in __post_init__."""


def _validate_key(key: str, *, what: str) -> None:
    if not key:
        raise ContractError(f"{what}: key must not be empty")
    if key.startswith("/") or key.endswith("/"):
        raise ContractError(f"{what}: key {key!r} must not start or end with '/'")
    if any(not segment for segment in key.split("/")):
        raise ContractError(f"{what}: key {key!r} contains an empty segment")
    if any(ch.isspace() for ch in key):
        raise ContractError(f"{what}: key {key!r} contains whitespace")


def resolve_key(key: str, namespace: str, *, absolute: bool = False) -> str:
    """Prefix a relative key with the deployment namespace."""
    if absolute or not namespace:
        return key
    return f"{namespace}/{key}"


@dataclass(frozen=True)
class Topic(Generic[T]):
    """One pub/sub channel of the contract.

    Args:
        key: Hierarchical key, relative to the deployment namespace. Use
            :meth:`Topic.absolute` for keys outside the namespace.
        schema: Payload type — a Pydantic model, or ``bytes`` for raw payloads.
        codec: Wire format. Defaults to Pydantic-JSON for models and raw
            octet-stream for ``bytes``.
        latched: Late joiners receive the last published value(s), via
            zenoh-ext advanced pub/sub.
        max_age: Subscribers drop samples older than this many seconds. Age
            compares the sender's wall clock against the receiver's, so it
            requires synchronized clocks (NTP/chrony); a sender skewed by more
            than ``max_age`` has every message dropped. Checked twice — on
            arrival (too old when it lands: not enqueued) and on dequeue (aged
            out while queued: not dispatched). Drops are counted (``stale``)
            and warned about, with a different message per stage.
        history: Number of samples kept/recovered for a latched topic.
        trace: Publishing on this topic starts a new trace that follows the
            data downstream (see :mod:`zenode.trace`). Set it on topics that
            begin a pipeline; an already-active trace is continued regardless.
        trace_ratio: Fraction of the traces started here that are *sampled* —
            recorded as spans by :mod:`zenode.otel`. Unsampled traces still
            carry an id, so log correlation keeps working at full rate; they
            just cost nothing to record. Use it on topics you cannot afford to
            record every message of: a 30 Hz camera at ``0.01`` gives ~0.3
            recorded traces a second.
        shm: Publish through shared memory (see :mod:`zenode.shm`). Worth it
            for frames and point clouds, pointless below a few tens of
            kilobytes. Requires ``[transport] shared_memory = true`` at both
            ends; falls back to a normal publish whenever that is not so.
    """

    key: str
    schema: type[T]
    codec: Codec[T] = _CODEC_UNSET
    latched: bool = False
    max_age: float | None = None
    history: int = 1
    trace: bool = False
    trace_ratio: float = 1.0
    shm: bool = False
    description: str = ""
    is_absolute: bool = field(default=False, repr=False)

    def __post_init__(self) -> None:
        _validate_key(self.key, what=f"Topic({self.key!r})")
        if self.max_age is not None and self.max_age <= 0:
            raise ContractError(f"Topic({self.key!r}): max_age must be positive")
        if self.history < 1:
            raise ContractError(f"Topic({self.key!r}): history must be >= 1")
        if not 0.0 <= self.trace_ratio <= 1.0:
            raise ContractError(f"Topic({self.key!r}): trace_ratio must be between 0.0 and 1.0")
        if self.trace_ratio != 1.0 and not self.trace:
            # Silently doing nothing is the worse failure: the topic is not a
            # root, so it never starts a trace to sample in the first place.
            raise ContractError(
                f"Topic({self.key!r}): trace_ratio has no effect without trace=True"
            )
        if self.codec is None:
            object.__setattr__(self, "codec", default_codec(self.schema))

    @classmethod
    def absolute(
        cls,
        key: str,
        schema: type[T],
        codec: Codec[T] | None = None,
        **kwargs: Any,
    ) -> Topic[T]:
        """A topic whose key is used verbatim, ignoring the namespace.

        For contracts owned by external systems (e.g. ``livox/lidar``).
        """
        if codec is None:
            return cls(key, schema, is_absolute=True, **kwargs)
        return cls(key, schema, codec, is_absolute=True, **kwargs)

    def resolve(self, namespace: str) -> str:
        return resolve_key(self.key, namespace, absolute=self.is_absolute)


@dataclass(frozen=True)
class Service(Generic[Req, Rep]):
    """A request/reply endpoint, served over a zenoh queryable."""

    key: str
    request: type[Req]
    reply: type[Rep]
    request_codec: Codec[Req] = _CODEC_UNSET
    reply_codec: Codec[Rep] = _CODEC_UNSET
    description: str = ""
    is_absolute: bool = field(default=False, repr=False)

    def __post_init__(self) -> None:
        _validate_key(self.key, what=f"Service({self.key!r})")
        if self.request_codec is None:
            object.__setattr__(self, "request_codec", default_codec(self.request))
        if self.reply_codec is None:
            object.__setattr__(self, "reply_codec", default_codec(self.reply))

    def resolve(self, namespace: str) -> str:
        return resolve_key(self.key, namespace, absolute=self.is_absolute)


@dataclass(frozen=True)
class RegisteredEntry:
    """A Topic or Service found in a TopicSet, with its declaration site."""

    owner: str
    attr: str
    entry: Topic[Any] | Service[Any, Any]


_REGISTRY: list[RegisteredEntry] = []


class TopicSet:
    """Declare topics/services as class attributes to register them.

    Subclassing is the registration mechanism::

        class StateTopics(TopicSet):
            odometry = Topic("state/odometry", OdometryState)

    The registry is what makes the contract introspectable (CLI, docs, tests).
    """

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        for attr, value in vars(cls).items():
            if isinstance(value, (Topic, Service)):
                _REGISTRY.append(
                    RegisteredEntry(
                        owner=f"{cls.__module__}.{cls.__qualname__}", attr=attr, entry=value
                    )
                )


def registered_entries(owner_prefix: str = "") -> Iterator[RegisteredEntry]:
    """All topics/services registered via TopicSet subclasses, in definition order.

    The registry is process-global, so a process that imports two contracts
    sees both. ``owner_prefix`` filters by declaring module/class — pass your
    own package (``"my_robot.topics"``) to assert over just your contract.
    """
    for item in _REGISTRY:
        if item.owner.startswith(owner_prefix):
            yield item


def registered_topics(owner_prefix: str = "") -> Iterator[tuple[RegisteredEntry, Topic[Any]]]:
    """Just the topics of :func:`registered_entries`, narrowed to :class:`Topic`."""
    for item in registered_entries(owner_prefix):
        if isinstance(item.entry, Topic):
            yield item, item.entry


def registered_services(
    owner_prefix: str = "",
) -> Iterator[tuple[RegisteredEntry, Service[Any, Any]]]:
    """Just the services of :func:`registered_entries`, narrowed to :class:`Service`."""
    for item in registered_entries(owner_prefix):
        if isinstance(item.entry, Service):
            yield item, item.entry


def find_topic(key: str, namespace: str = "") -> Topic[Any] | None:
    """Find a registered topic whose resolved key matches ``key`` exactly."""
    for _, topic in registered_topics():
        if topic.resolve(namespace) == key:
            return topic
    return None
