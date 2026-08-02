"""In-process test harness: run nodes against a local session, no router.

All nodes under test share one peer-mode session with multicast scouting
disabled — zenoh routes matching pub/sub locally, so typed round-trips work
entirely in-process with zero network configuration::

    async with harness() as h:
        await h.start_node(NavManager, config=NavConfig(max_speed=1.0))
        out = h.collect(MotionTopics.move)
        h.publisher(StateTopics.odometry).put(OdometryState(...))
        cmd = await out.next()

A node that owns hardware gets its fakes the same way — ``start_node`` passes
extra keyword arguments to the node's ``__init__``, or takes a node instance
the test built itself.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from typing import Any, Generic, TypeVar

import zenoh
from pydantic import BaseModel

from .config import TransportConfig
from .envelope import Envelope
from .node import Node
from .pubsub import Publisher, SubscriptionMode
from .topic import Service, Topic

T = TypeVar("T")
N = TypeVar("N", bound=Node)
Req = TypeVar("Req")
Rep = TypeVar("Rep")


def local_transport(namespace: str = "") -> TransportConfig:
    """Peer mode, no multicast scouting, no endpoints: fully in-process."""
    return TransportConfig(mode="peer", multicast_scouting=False, namespace=namespace)


class Collector(Generic[T]):
    """Collects everything published on one topic; ``await next()`` to consume."""

    def __init__(self) -> None:
        self.items: list[T] = []
        self.envelopes: list[Envelope] = []
        self._queue: asyncio.Queue[T] = asyncio.Queue()

    def _handler(self, msg: T, envelope: Envelope) -> None:
        self.items.append(msg)
        self.envelopes.append(envelope)
        self._queue.put_nowait(msg)

    async def next(self, timeout: float = 2.0) -> T:
        return await asyncio.wait_for(self._queue.get(), timeout)

    def clear(self) -> None:
        self.items.clear()
        self.envelopes.clear()
        while not self._queue.empty():
            self._queue.get_nowait()


class _Probe(Node):
    """Internal node the harness uses to publish/collect/call."""

    name = "zenode-test-probe"
    health_interval = None


class Harness:
    """Nodes under test, plus an external producer/consumer for talking to them.

    Obtained from :func:`harness`. Every method routes through one in-process
    probe node, so a test publishes and collects exactly as a neighbouring node
    would — there is no privileged back door into the node under test.
    """

    def __init__(self, session: zenoh.Session, namespace: str, probe: _Probe) -> None:
        self.session = session
        self.namespace = namespace
        self._probe = probe
        self._nodes: list[Node] = []

    async def start_node(
        self,
        node: type[N] | N,
        *,
        config: BaseModel | None = None,
        namespace: str | None = None,
        **kwargs: Any,
    ) -> N:
        """Start a node on the harness's session, by class or by instance.

        Extra keyword arguments go to the node's ``__init__``, which is how a
        node under test is handed a fake instead of the hardware it would open
        for itself::

            await h.start_node(MotorNode, config=cfg, axes=[FakeAxis()])

        Passing an *instance* covers the rest: a node with a custom
        ``__init__`` signature, or one that needs setting up before it starts.
        The harness points it at its own session — construct it without
        transport arguments::

            await h.start_node(MotorNode(axes=[FakeAxis()]))
        """
        ns = self.namespace if namespace is None else namespace
        if isinstance(node, Node):
            if config is not None or kwargs:
                raise TypeError(
                    "start_node() got an already-constructed node together with "
                    "config/constructor arguments — pass those to the node itself"
                )
            instance = node
            instance.adopt_session(
                self.session, namespace=ns, transport=local_transport(self.namespace)
            )
        else:
            instance = node(
                config=config,
                transport=local_transport(self.namespace),
                session=self.session,
                namespace=ns,
                **kwargs,
            )
        await instance.start()
        self._nodes.append(instance)
        return instance

    def publisher(self, topic: Topic[T]) -> Publisher[T]:
        return self._probe.publisher(topic)

    def collect(
        self, topic: Topic[T], *, mode: SubscriptionMode = "queue", queue_size: int = 256
    ) -> Collector[T]:
        collector: Collector[T] = Collector()
        self._probe.subscribe(topic, collector._handler, mode=mode, queue_size=queue_size)
        return collector

    def subscribe(self, topic: Topic[T], handler: Any, **kwargs: Any) -> Any:
        """Raw subscribe through the probe node; returns the Subscription."""
        return self._probe.subscribe(topic, handler, **kwargs)

    async def call(self, service: Service[Req, Rep], request: Req, *, timeout: float = 2.0) -> Rep:
        return await self._probe.call(service, request, timeout=timeout)

    async def stop_node(self, node: Node) -> None:
        if node in self._nodes:
            self._nodes.remove(node)
        await node.shutdown()


@contextlib.asynccontextmanager
async def harness(namespace: str = "") -> AsyncIterator[Harness]:
    """Open an in-process session for the duration of one test.

    Every node started through the yielded :class:`Harness` is shut down in
    reverse order on exit, then the session is closed.

    Pass a ``namespace`` to scope every key, which isolates the run from
    anything else on the machine — including a real robot on the same LAN,
    since multicast is off here but a router's need not be.
    """
    session = await asyncio.to_thread(zenoh.open, local_transport(namespace).to_zenoh_config())
    probe = _Probe(session=session, namespace=namespace)
    await probe.start()
    h = Harness(session, namespace, probe)
    try:
        yield h
    finally:
        for node in reversed(list(h._nodes)):
            await node.shutdown()
        await probe.shutdown()
        await asyncio.to_thread(session.close)


__all__: list[str] = ["Collector", "Harness", "harness", "local_transport"]
