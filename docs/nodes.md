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
| `allow_duplicates` | `True` | When `False`, a second node of this name raises `DuplicateNodeError`. |
| `publish_logs_at` | `"WARNING"` | Minimum level published to the log topic. `None` disables. |
| `trace_ring` | `4096` | Hops retained for `zenode trace`. `0` disables. |
| `shm_pool_bytes` | 64 MiB | Shared-memory pool, created on first use. |

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
| `await blocking(fn, *args)` | Run blocking code off the event loop. |
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

The `max_age` case is the one worth understanding. A producer whose clock is
skewed sends a perfectly healthy stream that `max_age` discards in full. If
that satisfied the deadline, the consumer would believe data was flowing while
its handler had not run in minutes — the exact failure the deadline exists to
catch.

The malformed case is an accepted hole rather than a design goal: decoding
happens after arrival is stamped, so a producer emitting garbage keeps the
deadline satisfied and surfaces as `errors` instead. That is tolerable because
a schema mismatch is loud on the very first message at deploy time, unlike
clock drift, which develops on a moving robot.

### Reacting to transitions

Silence detection is **edge-triggered**: one callback per transition, not one
per second, because a latching reaction re-fired continuously is noise. The
deadline is armed when the subscription starts, so a producer that never starts
is caught as well as one that dies.

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
seconds since data stopped, and `0.0` while receiving. Prefer `@on_silence` —
it fires once, at the moment it matters, instead of requiring somewhere to poll
from.

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
raised part-way. Write it to tolerate partially initialised state:

```python
async def on_stop(self) -> None:
    if getattr(self, "motors", None) is not None:
        self.motors.disable()
```

That guarantee is why a node that dies while arming its third motor still safes
the first two.

`run()` installs SIGINT and SIGTERM handlers, so `Ctrl-C` and `systemctl stop`
both take the graceful path.

## Presence

Every node holds a liveliness token at `<ns>/node/<name>`. `zenode nodes` lists
them, and a node can gate its own startup:

```python
async def on_start(self) -> None:
    await self.wait_for_nodes({"localization", "lidar"})
```

Starting a second node with a live name logs a warning by default, because
restarts and handovers legitimately overlap for a moment. Set
`allow_duplicates = False` to make it an error instead — worth doing during
development, where a stray instance silently doubles every message.

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

## Entry point

```python
run(Nav)                                  # config from file and environment
run(Nav, config=NavConfig(max_speed=1.0)) # explicit
run(nav_instance)                         # already constructed
```

`run()` is what makes a process a node. Importing zenode as a library
configures nothing — an embedded `Node` inherits the host application's logging
and emits only what it is asked to.
