"""Declarative wiring: bind handlers and publishers at class-definition time.

Instead of wiring everything imperatively in ``on_start``, a node can declare
its I/O where it lives::

    class Talker(Node):
        name = "talker"
        cmd = publish(Topics.cmd_vel)              # typed Publisher once started

        @subscribe(Topics.pose, mode="latest")
        async def on_pose(self, msg: Pose) -> None: ...

        @serve(Services.get_map)
        async def on_get_map(self, req: MapRequest) -> CostMap: ...

        @every(0.1)                                   # or @every("rate_hz", unit="hz")
        async def tick(self) -> None: ...

Semantics:

- The decorators only stamp metadata and return the function unchanged, so
  handlers stay directly callable in tests (``await node.on_pose(msg)``).
- ``publish()`` descriptors are materialized when the node starts, *before*
  ``on_start`` runs (so ``on_start`` may use them). Reading one earlier
  raises; assigning to one always raises.
- Decorated bindings are activated *after* ``on_start`` returns, so handlers
  and timers never observe a half-initialized node. ``@every`` intervals are
  resolved against ``self.config`` at that point, so they can come from the
  deployment's config file.
- Inheritance: a subclass that overrides a decorated method *without*
  re-decorating inherits the binding (the override is called). Re-decorating
  replaces the binding. Overriding a ``publish()`` attribute with anything
  else removes that publisher.
- The imperative API (``self.subscribe(...)`` in ``on_start``) remains the
  escape hatch for wiring that is only known at runtime.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Generic, Literal, TypeVar, overload

from .errors import ContractError
from .pubsub import OnDeadline, Publisher, SubscriptionMode
from .timers import IntervalSpec, IntervalUnit, OnTimerError
from .topic import Service, Topic

T = TypeVar("T")
F = TypeVar("F", bound=Callable[..., Any])

BINDINGS_ATTR = "__zenode_bindings__"


@dataclass(frozen=True)
class Binding:
    """One declarative wiring instruction stamped onto a method."""

    kind: Literal["subscribe", "serve", "every", "on_silence", "on_resume"]
    target: Topic[Any] | Service[Any, Any] | None = None
    interval: IntervalSpec | None = None
    opts: dict[str, Any] = field(default_factory=dict)


def _stamp(fn: F, binding: Binding) -> F:
    existing: tuple[Binding, ...] = getattr(fn, BINDINGS_ATTR, ())
    setattr(fn, BINDINGS_ATTR, (*existing, binding))
    return fn


def subscribe(
    topic: Topic[Any],
    *,
    mode: SubscriptionMode = "queue",
    queue_size: int = 64,
    deadline: float | None = None,
    on_deadline: OnDeadline = "log",
) -> Callable[[F], F]:
    """Bind this method as a subscription handler for ``topic``.

    The method keeps the normal handler signature: ``(self, msg)`` or
    ``(self, msg, envelope)``, sync or async. Stack multiple ``@subscribe``
    decorators to feed several topics into one handler.

    ``deadline`` (seconds) reacts to *silence* — see :meth:`zenode.Node.subscribe`.
    Give it a named reaction with :func:`on_silence` / :func:`on_resume`, or a
    policy with ``on_deadline``.
    """
    if isinstance(deadline, (int, float)) and not isinstance(deadline, bool) and deadline <= 0:
        raise ContractError("@subscribe deadline must be positive")

    def deco(fn: F) -> F:
        return _stamp(
            fn,
            Binding(
                kind="subscribe",
                target=topic,
                opts={
                    "mode": mode,
                    "queue_size": queue_size,
                    "deadline": deadline,
                    "on_deadline": on_deadline,
                },
            ),
        )

    return deco


def serve(service: Service[Any, Any]) -> Callable[[F], F]:
    """Bind this method as the request handler for ``service``:
    ``(self, request) -> reply``, sync or async."""

    def deco(fn: F) -> F:
        return _stamp(fn, Binding(kind="serve", target=service))

    return deco


def on_silence(topic: Topic[Any]) -> Callable[[F], F]:
    """React when ``topic`` stops arriving for longer than its ``deadline``.

    Signature ``(self, silent_for: float)``, sync or async. Fires **once** per
    outage, on the edge — the reaction is latching, so re-firing it while the
    producer stays gone would be noise. Pair with :func:`on_resume` to learn
    when it is safe to run again.

    The topic must be subscribed with ``deadline=`` somewhere on the node,
    decoratively or in ``on_start``; otherwise the node fails at ``start()``.
    Stackable, so one method can cover several topics.
    """

    def deco(fn: F) -> F:
        return _stamp(fn, Binding(kind="on_silence", target=topic))

    return deco


def on_resume(topic: Topic[Any]) -> Callable[[F], F]:
    """React when ``topic`` starts arriving again after a silence.

    Signature ``(self, silent_for: float)`` — how long the outage lasted, which
    is the number worth logging. Without this edge a node that safed itself has
    no way to learn it may run again, and you are back to polling.
    """

    def deco(fn: F) -> F:
        return _stamp(fn, Binding(kind="on_resume", target=topic))

    return deco


def every(
    interval: IntervalSpec,
    *,
    unit: IntervalUnit = "s",
    on_error: OnTimerError = "log",
) -> Callable[[F], F]:
    """Run this method periodically once the node is running.

    The interval may be a literal, a config field name, or a callable —
    resolved against ``self.config`` when the node starts::

        @every(0.1)                             # 10 Hz, fixed
        @every("control_rate_hz", unit="hz")    # self.config.control_rate_hz
        @every(lambda self: 1 / self.config.control_rate_hz)

    A name that is not a field of ``self.config``, or a value that is not a
    positive number, raises :class:`~zenode.ConfigError` at ``start()`` — not
    at the first tick.

    ``on_error`` is the policy for a raising body (see
    :data:`~zenode.timers.OnTimerError`). Scheduling and counters are those of
    :meth:`zenode.Node.every`.
    """
    if isinstance(interval, (int, float)) and not isinstance(interval, bool) and interval <= 0:
        raise ContractError("@every interval must be positive")

    def deco(fn: F) -> F:
        return _stamp(
            fn,
            Binding(kind="every", interval=interval, opts={"unit": unit, "on_error": on_error}),
        )

    return deco


class publish(Generic[T]):
    """Class-level publisher declaration, materialized at node start.

    ``cmd = publish(Topics.cmd_vel)`` makes ``self.cmd`` a typed
    :class:`~zenode.Publisher` once the node has started. Reading it earlier
    raises; assigning to it always raises; class-level access returns the
    descriptor itself.
    """

    def __init__(self, topic: Topic[T]) -> None:
        self.topic = topic
        self._name = ""

    def __set_name__(self, owner: type, name: str) -> None:
        self._name = name

    @property
    def storage_key(self) -> str:
        return f"__zenode_pub_{self._name}"

    @overload
    def __get__(self, obj: None, objtype: type) -> publish[T]: ...

    @overload
    def __get__(self, obj: object, objtype: type | None = None) -> Publisher[T]: ...

    def __get__(self, obj: object | None, objtype: type | None = None) -> Publisher[T] | publish[T]:
        if obj is None:
            return self
        pub = obj.__dict__.get(self.storage_key)
        if pub is None:
            raise RuntimeError(
                f"publisher {self._name!r} is not available before the node has started"
            )
        return pub

    def __set__(self, obj: object, value: Any) -> None:
        raise AttributeError(f"{self._name!r} is a zenode-managed publisher; it cannot be assigned")


def collect_bindings(cls: type) -> dict[str, tuple[Binding, ...]]:
    """All decorated bindings of a class, attribute name → bindings.

    Walks the MRO base-first so the most-derived decoration wins; an
    undecorated override keeps the inherited binding (the wiring resolves the
    handler via ``getattr``, which finds the override).
    """
    out: dict[str, tuple[Binding, ...]] = {}
    for klass in reversed(cls.__mro__):
        for name, member in vars(klass).items():
            bindings = getattr(member, BINDINGS_ATTR, None)
            if bindings:
                out[name] = tuple(bindings)
    return out


def collect_publishers(cls: type) -> dict[str, publish[Any]]:
    """All ``publish()`` descriptors of a class, attribute name → descriptor."""
    out: dict[str, publish[Any]] = {}
    for klass in reversed(cls.__mro__):
        for name, member in vars(klass).items():
            if isinstance(member, publish):
                out[name] = member
            elif name in out:
                del out[name]  # overridden by something that is not a publisher
    return out
