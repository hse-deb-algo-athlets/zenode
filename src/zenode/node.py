"""The node runtime: lifecycle, wiring, and the process entry point.

A node subclasses :class:`Node`, declares its wiring, and is started with
:func:`run`::

    class Nav(Node):
        name = "nav"
        config: NavConfig                 # loaded from [node.nav] automatically

        cmd = publish(MotionTopics.move)  # typed Publisher once started

        @subscribe(StateTopics.odometry, mode="latest")
        async def on_pose(self, msg: OdometryState) -> None: ...

        @every("control_rate_hz", unit="hz")
        async def tick(self) -> None:
            self.cmd.put(...)

    def cli() -> None:
        run(Nav)

The imperative equivalents (``self.subscribe(...)``, ``self.publisher(...)``,
``self.every(...)`` inside ``on_start``) remain for wiring only known at
runtime. ``run()`` owns the rest: config resolution, logging, session
bootstrap, the liveliness presence token, the health heartbeat, signal
handling, graceful teardown, and exit codes.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
import sys
import time
import typing
from collections.abc import Awaitable, Callable, Coroutine
from typing import Any, ClassVar, TypeVar

import zenoh
import zenoh.ext as zext
from pydantic import BaseModel, ValidationError

from .config import (
    NodeConfig,
    TransportConfig,
    load_node_config,
    load_transport_config,
)
from .declarative import Binding, collect_bindings, collect_publishers
from .errors import ConfigError, ContractError, DuplicateNodeError
from .log import LogPublisher, setup_logging
from .metrics import ProcessStats, summarize
from .msgs.health import NodeHealth, NodeState, health_key
from .msgs.log import LogRecordMsg, log_key
from .msgs.trace import TraceHops, TraceQuery, trace_key
from .presence import list_nodes_async, presence_key
from .pubsub import Handler, OnDeadline, Publisher, Subscription, SubscriptionMode
from .service import ServiceHandler, ServiceServer, call_service
from .shm import DEFAULT_POOL_BYTES, ShmPool
from .timers import OnTimerError, Timer, resolve_interval
from .topic import Service, Topic, resolve_key
from .trace import TraceRing

T = TypeVar("T")
Req = TypeVar("Req")
Rep = TypeVar("Rep")


def node_logger(name: str) -> logging.Logger:
    """The logger for a node: ``zenode.node.<name>``."""
    return logging.getLogger(f"zenode.node.{name}")


def _config_model(node_class: type[Node]) -> type[BaseModel] | None:
    hints = typing.get_type_hints(node_class)
    model = hints.get("config")
    if isinstance(model, type) and issubclass(model, BaseModel):
        return model
    return None


class Node:
    """Base class for all nodes. Subclasses must set a class-level ``name``."""

    name: ClassVar[str] = ""
    health_interval: ClassVar[float | None] = 2.0
    """Seconds between health heartbeats; ``None`` disables them."""

    publish_logs_at: ClassVar[str | None] = "WARNING"
    """Level at or above which this node's log records are also published on
    ``<ns>/node/<name>/log``, for ``zenode logs``. ``None`` disables it.

    Defaults to WARNING rather than the console level: a node at DEBUG
    publishing every record at 30 Hz is a self-inflicted traffic problem."""

    shm_pool_bytes: ClassVar[int] = DEFAULT_POOL_BYTES
    """Shared-memory pool for ``Topic(shm=True)`` publishers, created on first
    use. Sized in frames in flight, not throughput — the pool is reclaimed on
    allocation. Needs `ulimit -l` above this; see :mod:`zenode.shm`."""

    trace_ring: ClassVar[int] = 4096
    """Hops kept for ``zenode trace``, per node. ``0`` disables the ring and its
    service. About 100 bytes each, so the default is ~400 KB — nothing on a
    Jetson, worth turning off on an MCU-class target."""

    allow_duplicates: ClassVar[bool] = True
    """A second live node with this name only logs a warning by default,
    because restarts and handovers legitimately overlap for a moment.
    Set ``False`` to make ``start()`` raise :class:`DuplicateNodeError`."""

    config: Any

    def __init__(
        self,
        *,
        config: BaseModel | None = None,
        transport: TransportConfig | None = None,
        session: zenoh.Session | None = None,
        namespace: str | None = None,
    ) -> None:
        if not self.name:
            raise ContractError(f"{type(self).__name__} must set a class-level `name`")
        self._transport = transport if transport is not None else TransportConfig()
        self.namespace = self._transport.namespace if namespace is None else namespace
        self._session: zenoh.Session | None = session
        self._session_owned = session is None
        self.log = node_logger(self.name)

        if config is not None:
            self.config = config
        else:
            model = _config_model(type(self))
            if model is not None:
                try:
                    self.config = model()
                except ValidationError as e:
                    raise ConfigError(
                        f"node {self.name!r} requires configuration "
                        f"({model.__name__} has required fields): {e}"
                    ) from e

        self._loop: asyncio.AbstractEventLoop | None = None
        self._stop_event = asyncio.Event()
        self._state: NodeState = "stopped"
        self._started_monotonic = 0.0
        self._entered_on_start = False
        self._token: Any = None
        self._publishers: list[Publisher[Any]] = []
        self._subscriptions: list[Subscription[Any]] = []
        self._servers: list[ServiceServer[Any, Any]] = []
        self._timers: list[Timer] = []
        self._tasks: list[asyncio.Task[Any]] = []
        self._health_pub: Publisher[NodeHealth] | None = None
        self._log_handler: LogPublisher | None = None
        self._process = ProcessStats()
        self._ring = TraceRing(self.trace_ring) if self.trace_ring else None
        self._shm = ShmPool(self.shm_pool_bytes, log=self.log)

    # ------------------------------------------------------------------ hooks

    async def on_start(self) -> None:
        """Acquire resources and declare publishers/subscriptions/services/timers here."""

    async def on_stop(self) -> None:
        """Release what ``on_start`` acquired, before the transport is torn down.

        Runs whenever ``on_start`` was *entered*, including when it raised
        half-way — so it must tolerate partially initialized state (guard
        with ``getattr``/``None`` checks). Exceptions raised here are logged;
        teardown continues regardless.
        """

    # -------------------------------------------------------------- lifecycle

    @property
    def session(self) -> zenoh.Session:
        if self._session is None:
            raise RuntimeError(f"node {self.name!r} is not started")
        return self._session

    @property
    def state(self) -> NodeState:
        return self._state

    @property
    def subscriptions(self) -> tuple[Subscription[Any], ...]:
        """Every subscription of this node, decorated ones included.

        Their ``received``/``dropped``/``stale``/``errors`` counters are the
        per-topic detail behind the totals on ``NodeHealth``::

            dropped = sum(s.dropped for s in self.subscriptions if s.topic is Topics.cmd_vel)
        """
        return tuple(self._subscriptions)

    @property
    def timers(self) -> tuple[Timer, ...]:
        """Every timer of this node, with its tick/overrun/error counters."""
        return tuple(self._timers)

    async def start(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._state = "starting"
        self._started_monotonic = time.monotonic()
        if self._session is None:
            self._session = await asyncio.to_thread(zenoh.open, self._transport.to_zenoh_config())
        try:
            await self._check_duplicate()
            self._token = self.session.liveliness().declare_token(
                presence_key(self.namespace, self.name)
            )
            self._materialize_publishers()
            self._start_log_publishing()
            if self.health_interval is not None:
                self._health_pub = self.publisher(Topic(health_key(self.name), NodeHealth))
                self.every(self.health_interval, self._publish_health, name="health")
            self._entered_on_start = True
            await self.on_start()
            self._wire_bindings()
            # Last, so the node's own services keep the leading positions in
            # `_servers` and this one never displaces what the node declared.
            if self._ring is not None:
                self.serve(
                    Service(trace_key(self.name), request=TraceQuery, reply=TraceHops),
                    self._answer_trace,
                )
        except BaseException:
            # on_stop() runs on the failure path too: on_start is where the
            # hardware is acquired, and a node that dies half-way through it
            # would otherwise leave motors armed and sockets open.
            await self._safe_on_stop()
            await self._teardown()
            self._state = "stopped"
            raise
        self._state = "running"
        self.log.info("started", extra={"namespace": self.namespace})

    def adopt_session(
        self,
        session: zenoh.Session,
        *,
        namespace: str | None = None,
        transport: TransportConfig | None = None,
    ) -> None:
        """Point a not-yet-started node at an externally owned zenoh session.

        The node will not close that session. Used by the test harness to run
        a node the test constructed itself (with fakes, or a custom
        ``__init__``) on the harness's in-process session.
        """
        if self._state != "stopped":
            raise RuntimeError(f"node {self.name!r} is already {self._state}")
        self._session = session
        self._session_owned = False
        if transport is not None:
            self._transport = transport
        if namespace is not None:
            self.namespace = namespace

    async def _check_duplicate(self) -> None:
        """Detect another live node holding this name's presence token.

        Duplicates share the presence key, interleave health heartbeats, and
        make latched topics replay one cached sample per instance — so warn
        loudly (or refuse to start, if ``allow_duplicates`` is ``False``).
        """
        key = presence_key(self.namespace, self.name)

        def _taken() -> bool:
            replies = self.session.liveliness().get(key, timeout=1.0)
            return any(reply.ok is not None for reply in replies)

        if not await asyncio.to_thread(_taken):
            return
        message = (
            f"another node named {self.name!r} is already running in namespace {self.namespace!r}"
        )
        if not self.allow_duplicates:
            raise DuplicateNodeError(message)
        self.log.warning(message)

    def _materialize_publishers(self) -> None:
        """Create publishers for class-level ``publish()`` declarations.

        Runs before ``on_start`` so it may already use them.
        """
        for descriptor in collect_publishers(type(self)).values():
            self.__dict__[descriptor.storage_key] = self.publisher(descriptor.topic)

    def _start_log_publishing(self) -> None:
        """Put this node's log records on the bus, for ``zenode logs``.

        The handler goes on the node's own logger, not the root: two nodes in
        one process (which ``harness()`` creates routinely) would otherwise each
        publish the other's records under their own name. Everything zenode logs
        on the node's behalf — subscriptions, services, timers — already goes
        through ``self.log``, so what this misses is records from third-party
        libraries, which no node can claim without lying about who emitted them.

        The drain task is tracked like any other, so teardown cancels it before
        the publisher goes away.
        """
        if self.publish_logs_at is None or self._loop is None:
            return
        publisher = self.publisher(Topic(log_key(self.name), LogRecordMsg))
        handler = LogPublisher(publisher, self.name, self._loop)
        handler.setLevel(self.publish_logs_at.upper())
        self.log.addHandler(handler)
        self._log_handler = handler
        self.spawn(handler.drain(), name="logs")

    def _answer_trace(self, request: TraceQuery) -> TraceHops:
        """This node's view of one trace, for ``zenode trace``."""
        ring = self._ring
        return TraceHops(node=self.name, hops=ring.hops(request.trace_id) if ring else [])

    def _stop_log_publishing(self) -> None:
        if self._log_handler is None:
            return
        self.log.removeHandler(self._log_handler)
        self._log_handler.close()
        self._log_handler = None

    def _wire_bindings(self) -> None:
        """Activate ``@subscribe``/``@serve``/``@every``/``@on_silence`` declarations.

        Runs after ``on_start`` so handlers never observe a half-initialized
        node.

        Two passes, because ``@on_silence(T)`` on one method must reach the
        subscription that ``@subscribe(T)`` created on another — which the
        attribute-ordered walk may not have reached yet. There is no ``await``
        between the passes, so no deadline can fire in the gap.
        """
        hooks: list[tuple[str, Binding, Handler]] = []
        for attr, bindings in collect_bindings(type(self)).items():
            handler = getattr(self, attr)
            if not callable(handler):
                raise ContractError(
                    f"{type(self).__name__}.{attr} carries a zenode binding but is not callable"
                )
            for binding in bindings:
                if binding.kind in ("on_silence", "on_resume"):
                    hooks.append((attr, binding, handler))
                elif binding.kind == "subscribe" and isinstance(binding.target, Topic):
                    self.subscribe(binding.target, handler, **binding.opts)
                elif binding.kind == "serve" and isinstance(binding.target, Service):
                    self.serve(binding.target, handler)
                elif binding.kind == "every" and binding.interval is not None:
                    self.every(
                        resolve_interval(
                            binding.interval,
                            self,
                            unit=binding.opts.get("unit", "s"),
                            where=f"@every on {type(self).__name__}.{attr}",
                        ),
                        handler,
                        name=attr,
                        on_error=binding.opts.get("on_error", "log"),
                    )
                else:  # pragma: no cover - unreachable with the public decorators
                    raise ContractError(f"invalid binding on {type(self).__name__}.{attr}")

        for attr, binding, handler in hooks:
            self._attach_silence_hook(attr, binding, handler)

    def _attach_silence_hook(self, attr: str, binding: Binding, handler: Handler) -> None:
        """Point one ``@on_silence``/``@on_resume`` body at its subscriptions.

        Matched by *resolved key* rather than ``Topic`` identity, so an alias —
        or a subscription created imperatively in ``on_start`` — binds just the
        same. Attaches to every armed subscription of that key and only
        complains when none is armed: a dashboard subscription without a
        deadline sitting alongside a safety one must not be fatal.
        """
        where = f"@{binding.kind} on {type(self).__name__}.{attr}"
        if not isinstance(binding.target, Topic):  # pragma: no cover - decorators enforce this
            raise ContractError(f"{where}: target must be a Topic")
        key = binding.target.resolve(self.namespace)
        matches = [sub for sub in self._subscriptions if sub.key == key]
        if not matches:
            raise ContractError(f"{where}: nothing on this node subscribes {key!r}")
        armed = [sub for sub in matches if sub.deadline is not None]
        if not armed:
            raise ContractError(
                f"{where}: {key!r} is subscribed without deadline=, so it can never go silent"
            )
        for sub in armed:
            if binding.kind == "on_silence":
                sub._add_silence_hook(handler)
            else:
                sub._add_resume_hook(handler)

    async def shutdown(self) -> None:
        if self._state in ("stopping", "stopped"):
            return
        self._state = "stopping"
        await self._safe_on_stop()
        await self._teardown()
        self._state = "stopped"
        self.log.info("stopped")

    async def _safe_on_stop(self) -> None:
        """Run ``on_stop`` once, if ``on_start`` was entered; never raise."""
        if not self._entered_on_start:
            return
        self._entered_on_start = False
        try:
            await self.on_stop()
        except Exception:
            self.log.exception("on_stop raised")

    async def run_until_stopped(self) -> None:
        await self._stop_event.wait()

    def stop(self) -> None:
        """Request shutdown. Safe to call from any thread or signal handler."""
        loop = self._loop
        if loop is not None and loop.is_running():
            loop.call_soon_threadsafe(self._stop_event.set)
        else:
            self._stop_event.set()

    async def _teardown(self) -> None:
        # Before cancelling the drain task, so nothing queues onto a publisher
        # that is about to be undeclared.
        self._stop_log_publishing()
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        self._tasks.clear()
        self._timers.clear()
        for sub in self._subscriptions:
            await sub.stop()
        self._subscriptions.clear()
        for server in self._servers:
            server.undeclare()
        self._servers.clear()
        for pub in self._publishers:
            pub.undeclare()
        self._publishers.clear()
        if self._token is not None:
            try:
                self._token.undeclare()
            except Exception as e:
                self.log.debug("undeclare liveliness token failed: %s", e)
            self._token = None
        if self._session is not None and self._session_owned:
            try:
                await asyncio.to_thread(self._session.close)
            except Exception as e:
                self.log.debug("session close failed: %s", e)
        if self._session_owned:
            self._session = None

    # ----------------------------------------------------------------- wiring

    def key(self, relative: str, *, absolute: bool = False) -> str:
        """Resolve a relative key against this node's namespace."""
        return resolve_key(relative, self.namespace, absolute=absolute)

    def publisher(self, topic: Topic[T]) -> Publisher[T]:
        key = topic.resolve(self.namespace)
        if topic.latched:
            inner: Any = zext.declare_advanced_publisher(
                self.session,
                key,
                encoding=topic.codec.encoding,
                cache=zext.CacheConfig(topic.history),
                publisher_detection=True,
            )
        else:
            inner = self.session.declare_publisher(key, encoding=topic.codec.encoding)
        if topic.shm and not self._transport.shared_memory:
            # Publishing still works, just with the copy shm=True was meant to
            # avoid — and silently, which is the part worth warning about.
            self.log.warning(
                "topic declares shm=True but [transport] shared_memory is off, "
                "so it publishes through the normal path",
                extra={"key": key},
            )
        pub = Publisher(inner, topic=topic, key=key, node_name=self.name, pool=self._shm)
        self._publishers.append(pub)
        return pub

    def subscribe(
        self,
        topic: Topic[T],
        handler: Handler,
        *,
        mode: SubscriptionMode = "queue",
        queue_size: int = 64,
        deadline: float | None = None,
        on_deadline: OnDeadline = "log",
    ) -> Subscription[T]:
        """Subscribe; ``handler(msg)`` or ``handler(msg, envelope)``, sync or async.

        Handlers always run on the node's event loop. ``mode="latest"`` keeps
        only the newest sample (state streams); ``mode="queue"`` buffers up to
        ``queue_size`` and drops the oldest on overflow (counted).

        ``deadline`` (seconds) detects **silence** — a producer that stopped,
        never started, or lost its link. It is measured on the monotonic loop
        clock, so unlike ``Topic.max_age`` it needs no clock synchronization,
        and it is armed at subscription time: a producer that never comes up
        trips it once, which is usually a mistyped key or a namespace mismatch.
        React with ``on_deadline`` (see :data:`~zenode.pubsub.OnDeadline`) or,
        for a named method, with ``@on_silence``/``@on_resume``.
        """
        if self._loop is None:
            raise RuntimeError("subscribe() must be called after start (e.g. in on_start)")
        key = topic.resolve(self.namespace)
        sub = Subscription(
            topic,
            key,
            handler,
            self._loop,
            mode=mode,
            queue_size=queue_size,
            deadline=deadline,
            on_deadline=on_deadline,
            stop=self.stop,
            log=self.log,
            node_name=self.name,
            ring=self._ring,
        )
        if topic.latched:
            inner: Any = zext.declare_advanced_subscriber(
                self.session,
                key,
                sub._zenoh_callback,
                history=zext.HistoryConfig(detect_late_publishers=True, max_samples=topic.history),
            )
        else:
            inner = self.session.declare_subscriber(key, sub._zenoh_callback)
        task = self._loop.create_task(sub._consume(), name=f"{self.name}:sub:{key}")
        sub._attach(inner, task)
        self._subscriptions.append(sub)
        return sub

    def serve(
        self, service: Service[Req, Rep], handler: ServiceHandler[Req, Rep]
    ) -> ServiceServer[Req, Rep]:
        if self._loop is None:
            raise RuntimeError("serve() must be called after start (e.g. in on_start)")
        key = service.resolve(self.namespace)
        # Explicit type arguments: inference would otherwise solve Rep from the
        # handler's `Rep | Awaitable[Rep]` return and clash with Service's invariance.
        server = ServiceServer[Req, Rep](
            service, key, handler, self._loop, log=self.log, node_name=self.name, ring=self._ring
        )
        inner = self.session.declare_queryable(key, server._zenoh_callback)
        server._attach(inner)
        self._servers.append(server)
        return server

    async def call(self, service: Service[Req, Rep], request: Req, *, timeout: float = 2.0) -> Rep:
        return await call_service(
            self.session,
            service,
            service.resolve(self.namespace),
            request,
            timeout=timeout,
            node=self.name,
        )

    def every(
        self,
        interval: float,
        fn: Callable[[], Any | Awaitable[Any]],
        *,
        name: str | None = None,
        on_error: OnTimerError = "log",
    ) -> Timer:
        """Run ``fn`` every ``interval`` seconds, on a period grid.

        Ticks are scheduled against absolute times, so the period does not
        drift by the body's runtime; a body that outruns its period skips
        the missed periods (counted as ``overruns``, reported on
        ``NodeHealth``) instead of bursting to catch up.

        ``on_error`` decides what a raising body means (see
        :data:`~zenode.timers.OnTimerError`) — the default logs and keeps
        ticking, which is wrong for a control loop::

            self.every(dt, self.control_tick, on_error="stop")
            self.every(1.0, self.publish_state)  # log and continue
        """
        timer = Timer(
            name or getattr(fn, "__name__", "timer"),
            interval,
            fn,
            on_error=on_error,
            log=self.log,
            stop=self.stop,
        )
        timer.task = self.spawn(timer.run(), name=f"timer:{timer.name}")
        self._timers.append(timer)
        return timer

    def spawn(
        self, coro: Coroutine[Any, Any, Any], *, name: str | None = None
    ) -> asyncio.Task[Any]:
        """Track a background task for the node's lifetime; crash is logged."""
        if self._loop is None:
            raise RuntimeError("spawn() must be called after start (e.g. in on_start)")
        task = self._loop.create_task(coro, name=f"{self.name}:{name or 'task'}")

        def _done(t: asyncio.Task[Any]) -> None:
            if not t.cancelled() and t.exception() is not None:
                self.log.error(
                    "background task crashed",
                    exc_info=t.exception(),
                    extra={"task": t.get_name()},
                )

        task.add_done_callback(_done)
        self._tasks.append(task)
        return task

    async def blocking(self, fn: Callable[..., T], /, *args: Any, **kwargs: Any) -> T:
        """Run blocking code off the event loop (serial ports, pygame, cv2…)."""
        return await asyncio.to_thread(fn, *args, **kwargs)

    async def wait_for_nodes(
        self, names: set[str] | list[str], *, timeout: float = 10.0, poll: float = 0.25
    ) -> None:
        """Block until all ``names`` hold presence tokens; ``TimeoutError`` otherwise."""
        wanted = set(names)
        deadline = time.monotonic() + timeout
        while True:
            alive = await list_nodes_async(self.session, self.namespace, timeout=poll * 2)
            if wanted <= alive:
                return
            if time.monotonic() >= deadline:
                missing = ", ".join(sorted(wanted - alive))
                raise TimeoutError(f"nodes not present after {timeout}s: {missing}")
            await asyncio.sleep(poll)

    # ----------------------------------------------------------------- health

    def _publish_health(self) -> None:
        if self._health_pub is None:
            return
        ages = [s.age for s in self._subscriptions]
        handlers = [s.handler_time for s in self._subscriptions]
        handlers += [srv.handler_time for srv in self._servers]
        age_mean_ms, age_max_ms = summarize(ages)
        handler_mean_ms, handler_max_ms = summarize(handlers)
        self._health_pub.put(
            NodeHealth(
                node=self.name,
                state=self._state,
                uptime_s=time.monotonic() - self._started_monotonic,
                sent=sum(p.sent for p in self._publishers),
                received=sum(s.received for s in self._subscriptions),
                dropped=sum(s.dropped for s in self._subscriptions),
                stale=sum(s.stale for s in self._subscriptions),
                handler_errors=sum(s.errors for s in self._subscriptions)
                + sum(srv.errors for srv in self._servers)
                + sum(t.errors for t in self._timers),
                timer_overruns=sum(t.overruns for t in self._timers),
                deadline_misses=sum(s.deadline_misses for s in self._subscriptions),
                logs_dropped=self._log_handler.dropped if self._log_handler else 0,
                shm_fallbacks=self._shm.fallbacks,
                cpu_percent=self._process.cpu_percent(),
                rss_bytes=self._process.rss_bytes(),
                queue_max_depth=max((s.queue_peak for s in self._subscriptions), default=0),
                age_mean_ms=age_mean_ms,
                age_max_ms=age_max_ms,
                handler_mean_ms=handler_mean_ms,
                handler_max_ms=handler_max_ms,
                ts_ns=time.time_ns(),
            )
        )
        # Latency is windowed: each heartbeat covers the interval since the
        # last, so one startup spike cannot dominate the number for hours.
        for accumulator in (*ages, *handlers):
            accumulator.reset()
        for subscription in self._subscriptions:
            subscription.queue_peak = 0


async def _amain(node: Node) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):  # platforms without handler support
            loop.add_signal_handler(sig, node.stop)
    await node.start()
    try:
        await node.run_until_stopped()
    finally:
        await node.shutdown()


def _build_node(
    node: Node | type[Node],
    config: BaseModel | None,
    transport: TransportConfig | None,
    config_path: str | None,
) -> Node:
    if isinstance(node, Node):
        if config is not None or transport is not None or config_path is not None:
            raise ConfigError(
                "run() got an already-constructed node together with config/transport/"
                "config_path — pass those to the node's constructor instead"
            )
        return node
    if transport is None:
        transport = load_transport_config(config_path)
    if config is None:
        model = _config_model(node)
        if model is not None and issubclass(model, NodeConfig):
            config = load_node_config(model, node.name, config_path)
    return node(config=config, transport=transport)


def run(
    node: Node | type[Node],
    *,
    config: BaseModel | None = None,
    transport: TransportConfig | None = None,
    config_path: str | None = None,
) -> None:
    """Process entry point: run a node and exit with a meaningful code.

    Given a node *class*, config is resolved for it (``[transport]`` +
    ``[node.<name>]`` + env) and the node is constructed. Given an already
    constructed *instance* — e.g. because your subclass has its own
    ``__init__`` parameters — it is run as-is; combine with the loaders if
    you still want file/env config::

        run(Talker(amplitude=2.0, transport=load_transport_config()))

    Exit codes: 0 clean stop, 1 crash (lets Docker/systemd restart), 2 bad config.
    """
    setup_logging()
    log = node_logger(node.name)
    try:
        instance = _build_node(node, config, transport, config_path)
    except ConfigError as e:
        log.error("configuration error: %s", e)
        sys.exit(2)
    try:
        asyncio.run(_amain(instance))
    except KeyboardInterrupt:
        pass
    except Exception:
        log.exception("node crashed")
        sys.exit(1)
    sys.exit(0)
