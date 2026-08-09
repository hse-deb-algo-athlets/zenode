"""Typed publisher/subscription wrappers around zenoh.

Invariants:

- Zenoh callbacks run on zenoh worker threads; they only copy bytes out and
  hand them to the asyncio loop. User handlers always run on the event loop.
- A malformed payload or a raising handler never kills a node: errors are
  logged, counted, and the subscription keeps going.
- Backpressure is explicit: ``mode="queue"`` drops the oldest sample when
  full, ``mode="latest"`` keeps only the newest. All drops are counted.
- ``max_age`` is checked twice: on **arrival** (a sample already too old when
  it lands is not enqueued, so a stale burst cannot evict good samples on its
  way to being discarded anyway) and on **dequeue** (a sample that aged past
  ``max_age`` while queued is not dispatched). Both count ``stale``, and each
  warns about its own cause — arrival-stale means the *sender's* clock is off,
  dequeue-stale means *this node* is behind.
- ``max_age`` drops are warned about, not just counted, because a sender with
  a skewed clock loses every message it publishes.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import itertools
import logging
import math
import time
from collections.abc import Callable, Coroutine, Sequence
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar

import zenoh

from . import otel
from .envelope import Envelope, decode_envelope, encode_envelope
from .errors import ContractError
from .metrics import Latency
from .shm import ShmPool
from .topic import Topic
from .trace import TraceRing, root_traceparent
from .trace import current as current_traceparent
from .trace import outgoing as outgoing_traceparent
from .trace import using as using_trace

if TYPE_CHECKING:
    import zenoh.ext as zenoh_ext

logger = logging.getLogger(__name__)

T = TypeVar("T")

SubscriptionMode = Literal["queue", "latest"]

STALE_LOG_INTERVAL = 10.0
"""Seconds between ``max_age`` drop warnings — the first one is immediate."""

SILENCE_LOG_INTERVAL = 10.0
"""Seconds between "still silent" warnings — the first one is immediate."""

Handler = Callable[..., Any]
"""``(msg) -> None | Awaitable`` or ``(msg, envelope) -> None | Awaitable``."""

OnDeadline = Literal["log", "stop"] | Callable[[float], Any]
"""What a subscription does when its ``deadline`` elapses with no data.

Mirrors :data:`~zenode.timers.OnTimerError`, but the callable receives
``silent_for`` in seconds rather than an exception — silence is not an error
object.

``"log"`` (default) warns and keeps going. ``"stop"`` logs and stops the node
(running ``on_stop()``, so hardware is released and a supervisor can restart
the process). A callable receives the seconds of silence, sync or async.

``"stop"`` deserves care together with the arm-at-start behaviour: a node whose
producer comes up a moment later will trip once and exit. Pair it with
``wait_for_nodes()``, or start the producer first.
"""

SilenceHook = Callable[[float], Any]
"""``(silent_for) -> None | Awaitable`` — an ``@on_silence``/``@on_resume`` body."""

MatchingHook = Callable[[bool], Any]
"""``(matching) -> None | Awaitable`` — an ``@on_matching`` body."""


def _handler_arity(handler: Handler) -> int:
    try:
        params = [
            p
            for p in inspect.signature(handler).parameters.values()
            if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
        ]
        return min(max(len(params), 1), 2)
    except (TypeError, ValueError):
        return 1


class Publisher(Generic[T]):
    """A typed publisher bound to one topic. ``put()`` is thread-safe."""

    def __init__(
        self,
        inner: zenoh.Publisher | zenoh_ext.AdvancedPublisher,
        topic: Topic[T],
        key: str,
        node_name: str,
        pool: ShmPool | None = None,
        log: logging.Logger = logger,
    ) -> None:
        self._inner = inner
        self.topic = topic
        self.key = key
        self._node_name = node_name
        self._pool = pool if topic.shm else None
        self._log = log
        self._seq = itertools.count(1)
        self.sent = 0
        self.errors = 0

        self._matching_hooks: list[MatchingHook] = []
        self._matching_state = False
        self._matching_listener: Any = None
        self._matching_loop: asyncio.AbstractEventLoop | None = None
        self._matching_tasks: set[asyncio.Task[None]] = set()

    def put(self, value: T) -> None:
        seq = next(self._seq)
        # Resolve the trace before doing any work: a root topic decides here,
        # once, whether this message is sampled, and an untraced topic outside a
        # trace pays one contextvar read and nothing else.
        parent = current_traceparent()
        root = parent is None
        if root:
            if not self.topic.trace:
                self._publish(value, seq, None)
                return
            parent = root_traceparent(self.topic.trace_ratio)
        with otel.producer_span(self.key, self._node_name, seq, parent, root=root):
            # Read back off the span, so the wire, the contextvar and the
            # recorded span all name one trace — at a root the span owns the
            # trace id, and `parent` is only the fallback when none is recording.
            traceparent = outgoing_traceparent(parent)
            with using_trace(traceparent):
                self._publish(value, seq, traceparent)

    def _publish(self, value: T, seq: int, traceparent: str | None) -> None:
        attachment = encode_envelope(self._node_name, seq, time.time_ns(), traceparent)
        payload: Any = self.topic.codec.encode(value)
        if self._pool is not None:
            # None means shared memory was unavailable for this message; the
            # encoded bytes still publish normally, just with the copy.
            payload = self._pool.buffer(payload) or payload
        self._inner.put(payload, attachment=attachment)
        self.sent += 1

    @property
    def matching(self) -> bool:
        """Whether any subscriber currently matches this publisher.

        Use to skip producing expensive payloads (e.g. JPEG encoding) when
        nobody is listening. Latched publishers report ``True`` (their cache
        must stay warm for late joiners anyway). For work that has to be
        started and stopped rather than skipped, take the edge instead:
        :meth:`on_matching`.
        """
        inner = self._inner
        if isinstance(inner, zenoh.Publisher):
            try:
                # zenoh's type stub says ``bool``; the runtime hands back a
                # ``MatchingStatus`` object that is truthy even when it means
                # "nobody is listening". Returning it directly made this
                # property a constant ``True`` and the optimization a no-op, so
                # unwrap it — via getattr, to keep working either way.
                status = inner.matching_status
                return bool(getattr(status, "matching", status))
            except Exception:
                return True
        return True

    # -- matching edges ------------------------------------------------------

    def on_matching(self, hook: MatchingHook) -> None:
        """Call ``hook(matching)`` when the first subscriber arrives or the last leaves.

        The edge form of :attr:`matching`, for work that is too expensive to
        gate per message — a camera that should not run at all while nobody is
        watching. Polling can only skip the encode; an edge can stop the sensor.

        The hook fires **once with the current state** at registration, so a
        node never has to poll to learn where it starts, and thereafter only on
        a change. Registration must happen on the node's event loop (in
        ``on_start`` or later); the hook itself runs there too, sync or async.

        Not available on a latched topic: an advanced publisher keeps its cache
        warm for late joiners, so it always matches and the falling edge would
        never come. Raising beats a gate that silently never closes.
        """
        if not isinstance(self._inner, zenoh.Publisher):
            raise ContractError(
                f"publisher {self.key!r}: on_matching needs a plain publisher, but this topic is "
                "latched — its cache must stay warm for late joiners, so it always matches"
            )
        self._matching_hooks.append(hook)
        if self._matching_listener is None:
            self._matching_loop = asyncio.get_running_loop()
            # Listener first, status second: a subscriber that appears between
            # the two is then an event we can dedupe against the seed, rather
            # than an edge that fell into the gap and was seen by neither.
            self._matching_listener = self._inner.declare_matching_listener(self._matching_event)
            self._matching_state = self.matching
        self._fire_matching((hook,), self._matching_state)

    def _matching_event(self, status: Any) -> None:
        """zenoh worker thread: unwrap and hand the edge to the loop, nothing else."""
        matching = bool(getattr(status, "matching", status))
        loop = self._matching_loop
        if loop is None:  # pragma: no cover - set before the listener exists
            return
        with contextlib.suppress(RuntimeError):  # loop already closed (shutdown race)
            loop.call_soon_threadsafe(self._matching_changed, matching)

    def _matching_changed(self, matching: bool) -> None:
        # Deduped against the seed: zenoh replays the current status when a
        # listener is declared while a subscriber is already there, and a hook
        # that starts hardware must not be told twice to start it.
        if matching == self._matching_state:
            return
        self._matching_state = matching
        self._fire_matching(tuple(self._matching_hooks), matching)

    def _fire_matching(self, hooks: Sequence[MatchingHook], matching: bool) -> None:
        loop = self._matching_loop
        if loop is None or not hooks:  # pragma: no cover - on_matching sets both
            return
        task = loop.create_task(self._run_matching_hooks(hooks, matching))
        self._matching_tasks.add(task)
        task.add_done_callback(self._matching_tasks.discard)

    async def _run_matching_hooks(self, hooks: Sequence[MatchingHook], matching: bool) -> None:
        for hook in hooks:
            try:
                result = hook(matching)
                if inspect.isawaitable(result):
                    await result
            except asyncio.CancelledError:
                raise
            except Exception:
                self.errors += 1
                self._log.exception("matching handler raised", extra={"key": self.key})

    def undeclare(self) -> None:
        # Listener first, so no further edge is queued onto a loop that is
        # tearing down; the hooks it would call are about to be cancelled.
        if self._matching_listener is not None:
            try:
                self._matching_listener.undeclare()
            except Exception as e:
                self._log.debug(
                    "undeclare matching listener failed: %s", e, extra={"key": self.key}
                )
            self._matching_listener = None
        for task in list(self._matching_tasks):
            task.cancel()
        self._matching_tasks.clear()
        try:
            self._inner.undeclare()
        except Exception as e:
            self._log.debug("undeclare publisher failed: %s", e, extra={"key": self.key})


class Subscription(Generic[T]):
    """A typed subscription: zenoh thread → loop → decoded handler call."""

    def __init__(
        self,
        topic: Topic[T],
        key: str,
        handler: Handler,
        loop: asyncio.AbstractEventLoop,
        *,
        mode: SubscriptionMode = "queue",
        queue_size: int = 64,
        deadline: float | None = None,
        on_deadline: OnDeadline = "log",
        stop: Callable[[], None] | None = None,
        log: logging.Logger = logger,
        node_name: str = "",
        ring: TraceRing | None = None,
    ) -> None:
        if mode not in ("queue", "latest"):
            raise ValueError(f"unknown subscription mode {mode!r}")
        if queue_size < 1:
            raise ValueError("queue_size must be >= 1")
        if deadline is not None and (
            isinstance(deadline, bool) or not math.isfinite(deadline) or deadline <= 0
        ):
            raise ContractError(
                f"subscription {key!r}: deadline must be positive and finite, got {deadline!r}"
            )
        if on_deadline not in ("log", "stop") and not callable(on_deadline):
            raise ContractError(
                f"subscription {key!r}: on_deadline must be 'log', 'stop', or a callable, "
                f"got {on_deadline!r}"
            )
        if deadline is None and on_deadline != "log":
            raise ContractError(
                f"subscription {key!r}: on_deadline={on_deadline!r} has no deadline= to act on"
            )
        self.topic = topic
        self.key = key
        self._node_name = node_name
        self._ring = ring
        self._handler = handler
        self._handler_arity = _handler_arity(handler)
        self._handler_is_coro = inspect.iscoroutinefunction(handler)
        self._loop = loop
        self._mode: SubscriptionMode = mode
        self._log = log
        self._queue: asyncio.Queue[tuple[bytes, Envelope]] = asyncio.Queue(maxsize=queue_size)
        self._latest: tuple[bytes, Envelope] | None = None
        self._latest_ready = asyncio.Event()
        self._inner: Any = None
        self._task: asyncio.Task[None] | None = None
        self._closed = False

        self.received = 0
        self.dropped = 0
        self.stale = 0
        self.errors = 0
        self.age = Latency()
        self.handler_time = Latency()
        self.queue_peak = 0
        # ``stale`` is the public total; the two stages keep their own
        # sub-counts so each warning reports only its own drops.
        self._stale_arrival = 0
        self._stale_arrival_logged_at = float("-inf")
        self._stale_arrival_logged = 0
        self._stale_queued = 0
        self._stale_queued_logged_at = float("-inf")
        self._stale_queued_logged = 0

        self.deadline = deadline
        self.deadline_misses = 0
        self._on_deadline = on_deadline
        self._stop = stop
        self._silent = False
        self._silent_since = 0.0
        self._last_arrival = 0.0
        self._handle: asyncio.TimerHandle | None = None
        self._silence_hooks: list[SilenceHook] = []
        self._resume_hooks: list[SilenceHook] = []
        self._hook_tasks: set[asyncio.Task[None]] = set()
        self._silence_logged_at = float("-inf")

    @property
    def silent(self) -> bool:
        """Whether no data has arrived for longer than ``deadline``."""
        return self._silent

    @property
    def silent_for(self) -> float:
        """Seconds since data stopped arriving; ``0.0`` while receiving."""
        return self._loop.time() - self._silent_since if self._silent else 0.0

    # -- zenoh side (worker thread) ------------------------------------------

    def _zenoh_callback(self, sample: zenoh.Sample) -> None:
        if sample.kind != zenoh.SampleKind.PUT:
            return
        payload = sample.payload.to_bytes()
        attachment = sample.attachment
        attachment_bytes = attachment.to_bytes() if attachment is not None else None
        with contextlib.suppress(RuntimeError):  # loop already closed (shutdown race)
            self._loop.call_soon_threadsafe(self._push, payload, attachment_bytes)

    # -- loop side -------------------------------------------------------------

    def _push(self, payload: bytes, attachment: bytes | None) -> None:
        if self._closed:
            return
        self.received += 1
        # Decoded once, here, on the loop side: the queue carries the envelope
        # so ``_consume`` never re-parses the attachment.
        envelope = decode_envelope(attachment)
        age = envelope.age_s()
        if age is not None and self._is_stale(age):
            self._drop_stale_on_arrival(age)
            return
        # Stamped only once the sample is *usable*: a producer whose clock is
        # skewed sends a healthy stream that `max_age` discards entirely, and a
        # deadline satisfied by it would leave the consumer believing data is
        # flowing while its handler has not run in minutes.
        self._mark_arrival()
        if self._mode == "latest":
            self._latest = (payload, envelope)
            self._latest_ready.set()
            return
        if self._queue.full():
            try:
                self._queue.get_nowait()
                self.dropped += 1
            except asyncio.QueueEmpty:  # pragma: no cover - full then empty is impossible
                pass
        self._queue.put_nowait((payload, envelope))
        # High-water mark, not instantaneous depth: a queue that filled and
        # drained between two heartbeats still backed up, and that is the
        # warning `dropped` only gives after it is too late.
        depth = self._queue.qsize()
        if depth > self.queue_peak:
            self.queue_peak = depth

    def _drop_stale_on_arrival(self, age: float) -> None:
        """Discard a sample that was already too old when it landed.

        Dropping here rather than at dequeue keeps a stale burst from evicting
        good samples out of the queue — or clobbering ``_latest`` — on its way
        to being discarded anyway.
        """
        self.stale += 1
        self._stale_arrival += 1
        # Observed even though the sample never reaches a handler: the age is
        # what tells the operator *how far* off the clock is, and it is a lower
        # bound on the dequeue age this sample would have had.
        self.age.observe(age)
        self._report_arrival_stale(age)

    async def _next(self) -> tuple[bytes, Envelope]:
        if self._mode == "latest":
            await self._latest_ready.wait()
            self._latest_ready.clear()
            assert self._latest is not None
            return self._latest
        return await self._queue.get()

    async def _consume(self) -> None:
        while True:
            payload, envelope = await self._next()
            # Recomputed rather than carried from ``_push``: this is the
            # publish-to-dequeue delay ``NodeHealth.age_*`` documents — the
            # network plus this node's queue.
            age = envelope.age_s()
            if age is not None:
                self.age.observe(age)
            if age is not None and self._is_stale(age):
                self.stale += 1
                self._stale_queued += 1
                self._report_queued_stale(age)
                continue
            try:
                value = self.topic.codec.decode(payload)
            except Exception as e:
                self.errors += 1
                self._log.warning("dropping malformed payload: %s", e, extra={"key": self.key})
                continue
            await self._dispatch(value, envelope)

    def _is_stale(self, age: float | None) -> bool:
        # Age is recorded *before* this check at both stages, so dropping a
        # late message never hides the delay that made it late.
        if self.topic.max_age is None or age is None:
            return False
        return age > self.topic.max_age

    def _report_arrival_stale(self, age: float) -> None:
        """Warn on the first arrival-stage drop, then every ``STALE_LOG_INTERVAL``.

        A sample already too old when it lands means the *sender's* clock
        disagrees with ours: age compares their wall clock against ours, so a
        skewed sender has everything it publishes dropped.
        """
        now = time.monotonic()
        if now - self._stale_arrival_logged_at < STALE_LOG_INTERVAL:
            return
        self._log.warning(
            "dropping samples older than max_age=%ss on arrival (%d since the last warning) — "
            "age is the sender's wall clock against ours, so check clock sync (NTP) before "
            "suspecting the sender",
            self.topic.max_age,
            self._stale_arrival - self._stale_arrival_logged,
            extra={"key": self.key, "age_s": age},
        )
        self._stale_arrival_logged_at = now
        self._stale_arrival_logged = self._stale_arrival

    def _report_queued_stale(self, age: float) -> None:
        """Warn about a sample that aged past ``max_age`` while queued.

        Not a clock problem: it was fresh on arrival, so *this node* is behind
        — the handler is too slow, or the queue too deep for the topic's rate.
        Saying so keeps the NTP advice above from sending you after the wrong
        bug.
        """
        now = time.monotonic()
        if now - self._stale_queued_logged_at < STALE_LOG_INTERVAL:
            return
        self._log.warning(
            "dropping samples that aged past max_age=%ss while queued (%d since the last "
            "warning) — they were fresh on arrival, so this node is behind: check handler "
            "time and queue_size",
            self.topic.max_age,
            self._stale_queued - self._stale_queued_logged,
            extra={"key": self.key, "age_s": age},
        )
        self._stale_queued_logged_at = now
        self._stale_queued_logged = self._stale_queued

    # -- deadline ------------------------------------------------------------

    def _arm_deadline(self) -> None:
        """Start the silence timer.

        Called from :meth:`_attach`, not ``__init__``: ``Node.subscribe``
        declares the zenoh subscriber *after* constructing us, and if that
        raises we are never registered for teardown. A timer armed in
        ``__init__`` would then reschedule itself forever — and with
        ``on_deadline="stop"`` would stop the node from beyond the grave.
        """
        if self.deadline is None:
            return
        self._last_arrival = self._loop.time()
        self._schedule(self.deadline)

    def _schedule(self, delay: float) -> None:
        self._handle = self._loop.call_later(delay, self._check_deadline)

    def _mark_arrival(self) -> None:
        """Record that usable data landed, and end any silence immediately."""
        if self.deadline is None:
            return
        self._last_arrival = self._loop.time()
        if self._silent:
            self._leave_silence()

    def _check_deadline(self) -> None:
        """Lazy timer: reschedules itself instead of being re-armed per message.

        A message costs one attribute write in :meth:`_mark_arrival`; this wakes
        at most once per ``deadline`` and reschedules to whatever time is
        actually left, so there is no granularity error to document.
        """
        self._handle = None
        if self._closed or self.deadline is None:
            return
        idle = self._loop.time() - self._last_arrival
        remaining = self.deadline - idle
        if remaining > 0:  # data arrived after we were scheduled
            self._schedule(remaining)
            return
        if self._silent:
            self._warn_silent(idle)
        else:
            self._enter_silence(idle)
        # Resume is detected by _push, so while silent the timer's only
        # remaining job is the throttled warning — no reason to keep waking at
        # the deadline rate for a producer that may be gone for hours.
        self._schedule(max(self.deadline, SILENCE_LOG_INTERVAL))

    def _enter_silence(self, idle: float) -> None:
        self._silent = True
        self._silent_since = self._last_arrival
        self.deadline_misses += 1
        self._warn_silent(idle, force=True)
        # Snapshot: the coroutine iterates when it runs, by which time
        # _add_silence_hook may have appended a hook that already fired itself.
        self._spawn(self._run_hooks(tuple(self._silence_hooks), idle))
        if self._on_deadline == "stop":
            self._log.error("stopping the node: %s went silent", self.key)
            if self._stop is not None:
                self._stop()
        elif callable(self._on_deadline):
            # Through _run_hooks so sync/async and exceptions behave identically
            # whether the reaction came from the kwarg or from @on_silence.
            self._spawn(self._run_hooks([self._on_deadline], idle))

    def _leave_silence(self) -> None:
        silent_for = self._loop.time() - self._silent_since
        self._silent = False
        self._log.info(
            "data resumed on %s after %.1fs",
            self.key,
            silent_for,
            extra={"key": self.key, "silent_for": silent_for},
        )
        # The pending wake-up is on the slow while-silent schedule; re-arm it to
        # the deadline, or the next outage goes undetected for that much longer.
        if self._handle is not None:
            self._handle.cancel()
        assert self.deadline is not None  # _mark_arrival guards
        self._schedule(self.deadline)
        self._spawn(self._run_hooks(tuple(self._resume_hooks), silent_for))

    def _warn_silent(self, idle: float, *, force: bool = False) -> None:
        """Warn about ongoing silence, at most every ``SILENCE_LOG_INTERVAL``.

        "Never received anything" and "stopped receiving" are different bugs —
        the first is usually a mistyped key or a startup race, the second a
        producer that died — so they say different things.
        """
        now = time.monotonic()
        if not force and now - self._silence_logged_at < SILENCE_LOG_INTERVAL:
            return
        if self.received == 0:
            self._log.warning(
                "never received any data on %s (deadline %ss)",
                self.key,
                self.deadline,
                extra={"key": self.key, "silent_for": idle},
            )
        else:
            self._log.warning(
                "no data on %s for %.1fs (deadline %ss)",
                self.key,
                idle,
                self.deadline,
                extra={"key": self.key, "silent_for": idle},
            )
        self._silence_logged_at = now

    def _add_silence_hook(self, hook: SilenceHook) -> None:
        """Register an ``@on_silence`` body.

        Fires immediately when the subscription is *already* silent: bindings
        are wired after ``on_start``, so a node that subscribes imperatively and
        then spends longer than its deadline opening hardware would otherwise
        cross the edge before the hook existed and — being edge-triggered —
        never hear about it.
        """
        self._silence_hooks.append(hook)
        if self._silent:
            self._spawn(self._run_hooks([hook], self.silent_for))

    def _add_resume_hook(self, hook: SilenceHook) -> None:
        self._resume_hooks.append(hook)

    def _spawn(self, coro: Coroutine[Any, Any, None]) -> None:
        task = self._loop.create_task(coro)
        self._hook_tasks.add(task)
        task.add_done_callback(self._hook_tasks.discard)

    async def _run_hooks(self, hooks: Sequence[SilenceHook], silent_for: float) -> None:
        # Not timed into ``handler_time``: that documents per-message handler
        # cost, and a once-per-outage safe-the-robot callback would distort it.
        for hook in hooks:
            try:
                result = hook(silent_for)
                if inspect.isawaitable(result):
                    await result
            except asyncio.CancelledError:
                raise
            except Exception:
                self.errors += 1
                self._log.exception("silence handler raised", extra={"key": self.key})

    async def _dispatch(self, value: T, envelope: Envelope) -> None:
        # Inside the sender's trace: the handler's own logs and publishes
        # continue it without the handler knowing tracing exists.
        started = time.perf_counter()
        with (
            using_trace(envelope.traceparent),
            otel.consumer_span(self.key, self._node_name, envelope.traceparent),
        ):
            try:
                if self._handler_arity >= 2:
                    result = self._handler(value, envelope)
                else:
                    result = self._handler(value)
                if self._handler_is_coro or inspect.isawaitable(result):
                    await result
            except Exception as e:
                self.errors += 1
                otel.record_error(e)
                self._log.exception("handler raised", extra={"key": self.key})
            finally:
                # Timed even when it raised: a handler that fails after 5s
                # still cost 5s of the loop.
                elapsed = time.perf_counter() - started
                self.handler_time.observe(elapsed)
                if self._ring is not None:
                    age = envelope.age_s()
                    self._ring.record(
                        node=self._node_name,
                        key=self.key,
                        traceparent=envelope.traceparent,
                        envelope_node=envelope.node,
                        seq=envelope.seq,
                        ts_ns=envelope.ts_ns,
                        age_ms=(age or 0.0) * 1000.0,
                        handler_ms=elapsed * 1000.0,
                    )

    # -- lifecycle ---------------------------------------------------------------

    def _attach(self, inner: Any, task: asyncio.Task[None]) -> None:
        self._inner = inner
        self._task = task
        # Last: everything that could still fail has succeeded (see _arm_deadline).
        self._arm_deadline()

    async def stop(self) -> None:
        self._closed = True
        if self._handle is not None:
            self._handle.cancel()
            self._handle = None
        for hook_task in list(self._hook_tasks):
            hook_task.cancel()
        for hook_task in list(self._hook_tasks):
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await hook_task
        self._hook_tasks.clear()
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._task
            self._task = None
        if self._inner is not None:
            try:
                self._inner.undeclare()
            except Exception as e:
                self._log.debug("undeclare subscriber failed: %s", e, extra={"key": self.key})
            self._inner = None
