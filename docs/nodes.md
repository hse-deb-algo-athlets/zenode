# Nodes

Reference for the node runtime: lifecycle, wiring, handlers, timers, silence
detection and shutdown.

## Overview

A node is one process. It subclasses `Node`, declares its wiring, and is
started with `run()`, which owns the process: configuration, logging, the zenoh
session, the presence token, the health heartbeat, signal handling, graceful
teardown and exit codes.

```python
class Nav(Node):
    name = "nav"
    config: NavConfig

    cmd = publish(MotionTopics.move)

    @subscribe(StateTopics.odometry, mode="latest")
    async def on_pose(self, msg: OdometryState) -> None: ...

    @serve(NavServices.get_map)
    async def on_get_map(self, req: MapRequest) -> CostMap: ...

    @every("control_rate_hz", unit="hz", on_error="stop")
    async def tick(self) -> None:
        self.cmd.put(...)


run(Nav)
```

## Class attributes

| Attribute | Default | Effect |
|---|---|---|
| `name` | — | Required. Identifies the node on the network and in logs. |
| `health_interval` | `2.0` | Seconds between health heartbeats. `None` disables. |
| `start_timeout` | `30.0` | Seconds `on_start` may take before the node tears down and raises `StartTimeout`. `None` waits indefinitely. |
| `shutdown_timeout` | `5.0` | Seconds teardown waits for cancelled background tasks. Overstayers are named in a warning. |
| `allow_duplicates` | `True` | When `False`, a second node of this name raises `DuplicateNodeError`. |
| `publish_logs_at` | `"WARNING"` | Minimum level published to the log topic. `None` disables. |
| `trace_ring` | `4096` | Hops retained for `zenode trace`. `0` disables. |
| `shm_pool_bytes` | 64 MiB | Shared-memory pool, created on first use. |

Those eight, plus `config`, `on_start` and `on_stop`, are the only names of
zenode's own that a subclass may redefine.

## The shared namespace

A node subclasses `Node`, so every name the runtime holds is unavailable to
your node. Two mechanisms prevent collisions.

**Runtime state is name-mangled.** Everything `__init__` sets lives at
`_Node__loop`, `_Node__session`, `_Node__tasks` and so on, so `_state`,
`_process`, `_tasks`, `_timers` and the rest of the obvious private names are
yours to use. Python resolves an instance attribute *before* a class method, so
an unmangled `self._process` assigned during construction would silently shadow
your `_process` method, surfacing later as a `TypeError`.

**Collisions with the public API are refused at import.** Defining a method or
attribute that lands on one of `Node`'s own raises `ContractError` when the class
is created, naming what clashed:

```python
class Detector(Node):
    name = "detector"

    def state(self) -> str:        # ContractError: Detector redefines names
        return "busy"              # zenode owns: state.
```

`log` and `namespace` are also guarded. Both are set on the instance rather
than the class, so `dir(Node)` does not list them and no IDE warns about them.

Overriding a *private* method (`_teardown`, `_wire_bindings`) is refused on the
same grounds. Nothing in the runtime is designed to be replaced except the
attributes in the table above.

## Wiring

Declarative and imperative wiring do the same thing; the decorators only stamp
metadata, so decorated handlers remain directly callable in tests.

### Declarative

| Decorator | Purpose |
|---|---|
| `publish(topic)` | Class attribute that becomes a typed `Publisher`. |
| `@subscribe(topic, …)` | Handler for incoming messages. |
| `@serve(service)` | Handler for requests. |
| `@every(interval, …)` | Periodic body. |
| `@on_silence(topic)` | Reaction when a subscription goes quiet. |
| `@on_resume(topic)` | Reaction when it recovers. |
| `@on_matching(topic)` | Reaction when a published topic gains its first subscriber or loses its last. |

`publish()` descriptors materialise **before** `on_start`, so `on_start` may
use them. Decorated bindings activate **after** it, so handlers never observe a
half-initialised node. A subclass overriding a decorated method without
re-decorating inherits the binding.

`@every` accepts a number, or the name of a config field:

```python
@every("control_rate_hz", unit="hz", on_error="stop")
async def tick(self) -> None: ...
```

### Imperative

For wiring only known at runtime, inside `on_start`:

```python
async def on_start(self) -> None:
    self.cmd = self.publisher(MotionTopics.move)
    for axis in self.config.axes:
        self.subscribe(Topic(f"axis/{axis}/state", AxisState), self.on_axis)
```

| Method | Signature |
|---|---|
| `publisher(topic)` | → `Publisher[T]` |
| `subscribe(topic, handler, *, mode, queue_size, deadline, on_deadline)` | → `Subscription[T]` |
| `serve(service, handler)` | → `ServiceServer` |
| `every(interval, fn, *, name, on_error)` | → `Timer` |
| `await call(service, request, *, timeout=2.0)` | → reply |
| `spawn(coro, *, name)` | Tracked background task; a crash is logged. |
| `await blocking(fn, *args)` | Run blocking code off the event loop. Required in `on_start` — see [Do not block in `on_start`](#do-not-block-in-on_start). |
| `await wait_for_nodes(names, *, timeout=10.0)` | Gate startup on other nodes. |

## Handlers

A subscription handler takes the message, and optionally the envelope:

```python
async def on_pose(self, msg: OdometryState) -> None: ...
async def on_pose(self, msg: OdometryState, env: Envelope) -> None: ...
```

Sync and async handlers are both accepted. All handlers run on the node's event
loop, never on a zenoh thread.

`Envelope` carries the delivery metadata: `node` (the sender), `seq`, `ts_ns`,
`traceparent`, and `age_s()`.

A handler that raises is logged and counted as `handler_errors`; the
subscription continues. One bad message never takes a node down.

### Backpressure

| Mode | Behaviour | Use for |
|---|---|---|
| `queue` (default) | Buffers up to `queue_size`, drops the **oldest** when full. | Streams where every message matters. |
| `latest` | Keeps only the newest sample. | State where only the freshest value matters. |

Drops are counted as `dropped`; the deepest the queue reached since the last
heartbeat is reported as `queue_max_depth`, which warns before drops begin.

## Silence detection

`max_age` triggers on a message *arriving* and asks whether it is too old. It
never fires for a producer that stopped, because no message arrives to check.
A deadline triggers on a message **not** arriving:

```python
self.subscribe(CMD, self.on_cmd, deadline=0.5, on_deadline="stop")


@subscribe(CMD, deadline=0.5, on_deadline="stop")
async def on_cmd(self, msg: Twist) -> None: ...
```

| Parameter | Default | Effect |
|---|---|---|
| `deadline` | `None` | Seconds of silence before the subscription is considered silent. |
| `on_deadline` | `"log"` | `"log"`, `"stop"`, or a callable taking `silent_for`. |

`"stop"` stops the node the same way a signal does, so `on_stop` runs and
hardware is released. Use it where continuing without data is worse than
stopping — a control loop. `"log"` is right for telemetry. A callable receives
the silence duration in seconds.

`deadline` must be positive and finite, and `on_deadline` without a `deadline`
raises `ContractError` at subscription time rather than silently doing nothing.

The clock is the event loop's monotonic clock, stamped on arrival, so unlike
`max_age` this has no cross-host clock dependency and works where NTP is not
trustworthy.

### What satisfies a deadline

Arrival is stamped once a sample is *usable*, which makes the two failure modes
behave differently:

| Sample | Satisfies the deadline | Why |
|---|---|---|
| Handled normally | Yes | Data is flowing. |
| Dropped by `max_age` | **No** | It never reaches a handler. |
| Malformed payload | Yes | Decoding happens after arrival is stamped. |

The `max_age` case matters: a producer with a skewed clock sends a healthy
stream that `max_age` discards in full. If that satisfied the deadline, the
consumer would believe data was flowing while its handler never ran.

The malformed case is an accepted gap: decoding happens after arrival is
stamped, so a producer emitting garbage keeps the deadline satisfied and
surfaces as `errors` instead. A schema mismatch is loud on the first message at
deploy time; clock drift is not.

### Reacting to transitions

Silence detection is **edge-triggered**: one callback per transition, not one
per second. The deadline is armed when the subscription starts, so a producer
that never starts is caught as well as one that dies.

```python
@on_silence(CMD)
async def cmd_lost(self, silent_for: float) -> None:
    self.motors.stop()

@on_resume(CMD)
async def cmd_back(self, silent_for: float) -> None:
    self.log.info("commands resumed after %.1fs", silent_for)
```

Without `@on_resume` a node that safed itself has no way to learn it may run
again. Transitions are counted as `deadline_misses` on `NodeHealth`.

### Polling instead of reacting

Where a reaction does not fit, the state is readable:

```python
if self.cmd_sub.silent:
    self.motors.stop()
```

`silent` is `True` while the deadline is elapsed; `silent_for` gives the
seconds since data stopped, and `0.0` while receiving. Prefer `@on_silence`; it
fires once, at the transition.

## Timers

```python
self.every(0.1, self.control_tick, on_error="stop")
```

Ticks are scheduled against absolute deadlines, so the period does not drift
with the body's runtime. A body that outruns its deadline skips the missed
periods — counted as `timer_overruns` — rather than bursting to catch up.

| `on_error` | Behaviour |
|---|---|
| `"log"` (default) | Log the traceback and keep ticking. Right for telemetry. |
| `"stop"` | Log and stop the node. Right for a control loop that must not run blind. |

Timer bodies run outside any trace; see
[trace lifetime](open-telemetry.md#trace-lifetime).

## Lifecycle

```
start() → on_start() → bindings activated → running → stop() → on_stop() → teardown
```

`on_start` acquires resources — hardware, files, connections. `on_stop`
releases them, and runs **whenever `on_start` was entered**, including when it
raised part-way or ran out of time. Write it to tolerate partially initialised
state:

```python
async def on_stop(self) -> None:
    if getattr(self, "motors", None) is not None:
        self.motors.disable()
```

`run()` installs SIGINT and SIGTERM handlers, so `Ctrl-C` and `systemctl stop`
both take the graceful path.

### Do not block in `on_start`

`on_start` runs on the event loop under `start_timeout` (30 s by default). On
overrun the node tears down and raises `StartTimeout`, which `run()` turns into
exit 1, so a supervisor can restart a node wedged on hardware.

The deadline is a loop timer and only fires while the loop runs. A
*synchronous* call in `on_start` blocks it along with the signal handlers: the
node can neither be timed out nor answer `Ctrl-C`. Push blocking calls to a
thread:

```python
async def on_start(self) -> None:
    self.pipeline = await self.blocking(open_camera, self.config.device)   # not open_camera(...)
```

Set `start_timeout = None` to wait indefinitely.

The health heartbeat already ticks while `on_start` runs, so `zenode health`
reports `state="starting"` during a slow start. It is the only thing live
before `on_start` returns.

### Nodes are single-use

A node that has run and been stopped will not start again: `stop()` is latched,
and a second `start()` raises instead of coming up and exiting silently.
Construct a new instance. A start that *failed* may be retried, since it left
nothing declared.

### Teardown is bounded

`on_stop` runs first, then the node's background tasks are cancelled and joined
for at most `shutdown_timeout` (5 s). Cancellation does not reach a task sitting
in `blocking()` — an executor future cannot be interrupted once its thread is
running — so an unbounded join would hand the process to SIGKILL. Tasks still
going when the bound expires are named in a warning. Hardware is released either
way, because `on_stop` has already run.

## Presence

Every node holds a liveliness token at `<ns>/node/<name>`. `zenode nodes` lists
them, and a node can gate its own startup:

```python
async def on_start(self) -> None:
    await self.wait_for_nodes({"localization", "lidar"})
```

Starting a second node with a live name logs a warning by default, since
restarts and handovers legitimately overlap for a moment. Set
`allow_duplicates = False` to make it an error — useful during development,
where a stray instance silently doubles every message.

## Publisher

```python
self.cmd.put(Twist(linear=Vector3(x=0.5)))
```

`put()` is thread-safe. `matching` reports whether any subscriber currently
matches, so expensive payloads can be skipped:

```python
if self.frames.matching:
    self.frames.put(self.camera.encode_jpeg())
```

Latched publishers always report `True`, since their cache must stay warm for
late joiners.

### Producing only while someone is listening

Polling `matching` skips the encode but still runs the sensor. For producers
that are expensive to *run* — a camera, a lidar — take the edge instead:

```python
class CameraNode(Node):
    frames = publish(Topics.frames)

    @on_matching(Topics.frames)
    async def on_viewers(self, matching: bool) -> None:
        await (self.camera.start() if matching else self.camera.stop())
        self.streaming = matching

    @every(0.033)
    async def grab(self) -> None:
        if self.streaming:
            self.frames.put(self.camera.encode_jpeg())
```

The hook fires **once with the current state** when the node starts, then only
on a change. zenoh reports changes, not levels; the initial callback is what
saves the node from guessing its starting state.

Edges are first-and-last, not per subscriber: a second viewer joining is not
another rising edge, so a hook that starts hardware is never told to start it
twice.

Same shape imperatively, for a publisher created in `on_start`:

```python
self.publisher(Topics.frames).on_matching(self._on_viewers)
```

Two constraints, both errors at `start()` rather than silent no-ops: the node
must publish the topic, and the topic must not be latched — a latched publisher
always matches, so the falling edge would never arrive. A hook that raises is
logged and counted as `handler_errors`; the node keeps running.

Matching is a view of the routing graph, not a delivery receipt: the right
signal for "don't bother producing this", the wrong one for "the data arrived".
It says nothing about *who* is listening.

## Entry point

```python
run(Nav)                                  # config from file and environment
run(Nav, config=NavConfig(max_speed=1.0)) # explicit
run(nav_instance)                         # already constructed
```

`run()` is what makes a process a node. Importing zenode as a library
configures nothing — an embedded `Node` inherits the host application's logging
and emits only what it is asked to.
