# Contracts

Reference for the typed contract: `Topic`, `Service`, `TopicSet`, codecs and
delivery semantics.

## Overview

A `Topic` binds four things in one declaration: the key, the payload type, the
wire format, and the delivery semantics. Both the publisher and the subscriber
derive their behaviour from the same object, so a mismatch is a type error at
the call site rather than a parse failure in another process.

The contract is the only thing two nodes share. It normally lives in its own
module, importable by every node that speaks it.

## Topic

```python
CMD = Topic("command/cmd_vel", Twist, max_age=0.5)
```

| Parameter | Default | Effect |
|---|---|---|
| `key` | — | Hierarchical key, relative to the deployment namespace. |
| `schema` | — | Payload type: a Pydantic model, or `bytes` for raw payloads. |
| `codec` | derived | Wire format. Defaults to Pydantic-JSON for models, octet-stream for `bytes`. |
| `latched` | `False` | Late joiners receive the last published value. |
| `history` | `1` | Samples retained for a latched topic. |
| `max_age` | `None` | Subscribers drop samples older than this many seconds. |
| `trace` | `False` | Publishing starts a trace — see [Observability](open-telemetry.md). |
| `trace_ratio` | `1.0` | Fraction of traces started here that are recorded. |
| `shm` | `False` | Publish through shared memory — see [Shared memory](shared-memory.md). |
| `priority` | `"data"` | Transmission priority band on a congested link. |
| `congestion_control` | `"drop"` | `"drop"` or `"block"` when the transmit queue is full. |
| `express` | `False` | Send immediately instead of batching. |
| `description` | `""` | Free text, shown by `zenode topics`. |

Invalid combinations raise `ContractError` at import time rather than failing
at runtime: an empty or whitespace-containing key, a non-positive `max_age`, a
`history` below 1, a `trace_ratio` outside 0–1, a `trace_ratio` on a topic
that is not a trace root, or an unknown `priority`/`congestion_control`.

### Keys and namespaces

Keys are relative. The deployment namespace is prefixed at runtime, so one
contract runs on many robots:

```toml
[transport]
namespace = "robodog"
```

`Topic("state/odometry", …)` then resolves to `robodog/state/odometry`. For
keys owned by an external system, opt out:

```python
LIDAR = Topic.absolute("livox/lidar", PointCloud)
```

### Delivery semantics

**`latched`** keeps the last value available to subscribers that join later.
Use it for state that is occasionally updated and always needed — a battery
level, a map version, a mode. It requires HLC timestamping, which zenode
enables by default.

**`max_age`** drops samples older than a threshold. It compares the *sender's*
wall clock against the receiver's, so it requires synchronised clocks
(NTP/chrony): a sender skewed by more than `max_age` has everything it
publishes dropped. Drops are counted as `stale` and warned about.

`max_age` answers *is this message too old to act on?* It does not detect a
producer that stopped, because a message that never arrives is never checked.
For that, see [silence detection](nodes.md#silence-detection), which is
declared on the subscription rather than the topic.

### Quality of service

The three QoS parameters are fixed when the publisher is declared and apply to
every message on the topic. They only matter once a link is actually congested;
on an idle link all three are invisible.

**`priority`** picks a transmission band. Highest to lowest: `real_time`,
`interactive_high`, `interactive_low`, `data_high`, `data` (the default),
`data_low`, `background`. Zenoh drains higher bands first, so this is how a
control topic keeps precedence over bulk telemetry on the same link:

```python
CMD = Topic("command/cmd_vel", Twist, priority="real_time", max_age=0.5)
CAMERA = Topic("camera/rgb", bytes, codec=RawCodec(Encoding.IMAGE_JPEG),
               priority="data_low", shm=True)
```

Only the *relative* order matters. Raising every topic to `real_time` restores
exactly the situation you started from, so spend the bands on the handful of
topics that genuinely outrank the rest.

zenode's own runtime traffic is already placed below application data: the
health heartbeat publishes at `data_low` and the log stream at `background`, so
a node that starts logging hard cannot push control messages off the link.

**`congestion_control`** decides what happens when the transmit queue is full.
The default `"drop"` discards the message, which is what you want for a stream
where the next sample supersedes this one — a pose at 30 Hz, a camera frame.
`"block"` waits for the queue to drain instead, for low-rate topics where a
lost message is a fault rather than a skipped frame.

> `"block"` blocks the calling thread, and `put()` normally runs on the node's
> event loop — so a stalled link stalls every timer, handler and signal handler
> in the process, the same failure mode [`Node.blocking`](nodes.md) exists to
> avoid. Use it on low-rate topics, and not from a handler that has to keep
> running regardless.

**`express`** sends each message on its own instead of batching it with
whatever else is queued, trading throughput for a little latency. It is worth
it for small, infrequent, latency-critical messages, and counterproductive on a
high-rate stream, where batching is what keeps the per-message overhead down.

`zenode topics` shows all three as flags, but only when they differ from the
default — so the listing highlights the topics that claim an exception.

## Service

Request/reply over a zenoh queryable.

```python
GET_MAP = Service("nav/get_map", request=MapRequest, reply=CostMap)
```

| Parameter | Default | Effect |
|---|---|---|
| `key` | — | Hierarchical key, relative to the namespace. |
| `request` / `reply` | — | Payload types. |
| `request_codec` / `reply_codec` | derived | Wire formats. |

A handler that raises produces a structured error reply, so the caller receives
a `ServiceError` carrying the message rather than a silent timeout. No server
produces `ServiceTimeout`.

Services are for questions with answers — fetch a map, query a parameter, run a
calibration. They are not a substitute for a topic: a value that changes
continuously belongs on a topic, where late joiners and backpressure are
handled.

## Codecs

| Codec | Used for |
|---|---|
| `PydanticJsonCodec` | Pydantic models. The default for a model `schema`. |
| `RawCodec(encoding)` | `bytes`. The default for a `bytes` schema, as octet-stream. |

`RawCodec` takes a zenoh `Encoding`, which is carried on the wire and lets
generic tools interpret the payload:

```python
CAMERA = Topic("camera/rgb", bytes, codec=RawCodec(Encoding.IMAGE_JPEG))
```

Payloads stay plain JSON or plain bytes. zenode's own delivery metadata —
sender, sequence number, timestamp, trace context — travels in a zenoh
*attachment* alongside, so any zenoh tool can read the payload without knowing
about zenode.

A custom codec is any object with `encode`, `decode` and `encoding`.

## TopicSet

Subclassing `TopicSet` registers the topics declared in it:

```python
class RobotTopics(TopicSet):
    cmd_vel = Topic("command/cmd_vel", Twist, max_age=0.5)
    odometry = Topic("state/odometry", OdometryState)
```

Registration is what makes the contract introspectable. `zenode topics` lists
it, `zenode echo` decodes payloads with the right type, and tests can assert
over it:

```python
for entry, topic in registered_topics("my_robot.contract"):
    assert topic.description, f"{entry.attr} needs a description"
```

The registry is process-global, so a process importing two contracts sees both.
Pass an owner prefix to filter to your own.

Topics do not have to live in a `TopicSet` — a plain module-level `Topic` works
everywhere except the registry-driven tooling.

## Standard messages

`zenode.msgs` provides a deliberately small set:

| Module | Contents |
|---|---|
| `msgs.geometry` | `Vector3`, `Quaternion`, `Pose`, `Pose2D`, `Twist`, `Transform` |
| `msgs.health` | `NodeHealth`, `NodeState` and the health key helpers |
| `msgs.log` | `LogRecordMsg` and the log key helpers |
| `msgs.trace` | `Hop`, `TraceQuery`, `TraceHops` and the trace key helpers |

Domain messages belong in your own contract package, where they can evolve with
the robot. See [Conventions](conventions.md) for units and frames.
