# zenode

**Typed node framework for distributed robot systems on [Eclipse Zenoh](https://zenoh.io).**

zenode is the thin layer between "raw zenoh" and "a whole robotics framework":
independent processes ("nodes"), coupled only through a **typed topic
contract**, with the runtime plumbing — session bootstrap, thread→asyncio
dispatch, config, presence, health, graceful shutdown — done once, correctly.

It deliberately is **not** a ROS or dora replacement: no launch system, no IDL,
no coordinator. You start processes however you like (shell, process-compose,
docker); they find each other over zenoh.


## Documentation

Rendered docs, including the full API reference:
**[hse-deb-algo-athlets.github.io/zenode](https://hse-deb-algo-athlets.github.io/zenode/)**

| | |
|---|---|
| [Getting started](docs/index.md) | Install, a first node, where to go next |
| [Contracts](docs/contracts.md) | `Topic`, `Service`, codecs, delivery semantics |
| [Nodes](docs/nodes.md) | Lifecycle, wiring, handlers, timers, silence detection |
| [Configuration](docs/configuration.md) | TOML, environment, shared sections |
| [Testing](docs/testing.md) | The in-process harness |
| [CLI](docs/cli.md) | Every command |
| [Conventions](docs/conventions.md) | Units, frames, time, key naming |
| [Observability](docs/open-telemetry.md) | Logging, tracing, metrics, exporting |
| [Shared memory](docs/shared-memory.md) | Publishing large payloads |

## The contract

A `Topic` binds key, payload type, wire codec, and delivery semantics in one
declaration. Both sides derive their behavior from it — a mismatch is a type
error at the call site, not a runtime parse failure in another process.

```python
from pydantic import BaseModel
from zenode import Topic, Service, TopicSet
from zenode.msgs import Twist


class OdometryState(BaseModel): ...


class MapRequest(BaseModel): ...


class CostMap(BaseModel): ...


class RobotTopics(TopicSet):  # TopicSet = registered & introspectable
    cmd_vel = Topic(
        "command/cmd_vel", Twist, max_age=0.5
    )  # older than 0.5s: dropped (synced clocks!)
    odometry = Topic("state/odometry", OdometryState)
    battery = Topic("state/battery", OdometryState, latched=True)  # late joiners get last value


class RobotServices(TopicSet):
    get_map = Service("nav/get_map", request=MapRequest, reply=CostMap)
```

Keys are relative: the deployment namespace (`[transport] namespace = "robodog"`)
is prefixed at runtime, so the same contract runs on N robots. Binary payloads
(JPEG frames, point clouds) use `Topic("camera/rgb", bytes, codec=RawCodec(Encoding.IMAGE_JPEG))`.

## A node

```python
from zenode import Node, NodeConfig, every, publish, run, serve, subscribe


class NavConfig(NodeConfig):
    max_speed: float = 0.5
    control_rate_hz: float = 15.0


class Nav(Node):
    name = "nav"
    config: NavConfig  # loaded from [node.nav] + ZENODE_NAV__* env

    cmd = publish(RobotTopics.cmd_vel)  # typed Publisher once the node starts

    @subscribe(RobotTopics.odometry, mode="latest")
    async def on_pose(self, msg: OdometryState) -> None: ...

    @serve(RobotServices.get_map)
    async def on_get_map(self, req: MapRequest) -> CostMap: ...

    @every("control_rate_hz", unit="hz", on_error="stop")  # rate from config
    async def tick(self) -> None:
        self.cmd.put(...)


def cli() -> None:
    run(Nav)  # config, logging, session, signals, presence, health, exit codes
```

Wiring is declarative: `publish()` descriptors materialize before `on_start`
(so it may use them), decorated bindings activate right after it (so handlers
never see a half-initialized node), and a subclass overriding a decorated
method without re-decorating inherits the binding. The imperative API
(`self.subscribe(...)`, `self.publisher(...)` inside `on_start`) remains the
escape hatch for wiring only known at runtime — and the decorators just stamp
metadata, so handlers stay directly callable in tests.

## Try the example

```bash
uv sync
uv run python examples/talker.py          # terminal 1
uv run python examples/listener.py        # terminal 2
uv run zenode echo demo/cmd_vel --contract examples.contract --meta \
    --connect tcp/127.0.0.1:17447
```

The example uses an explicit localhost endpoint (the talker listens on
`tcp/127.0.0.1:17447`, everyone else connects) so it works on any machine.

### If nodes don't find each other

Zenoh's zero-config discovery uses **UDP multicast** (port 7446), which
firewalls commonly block — `ufw`/`firewalld` on Linux do by default. A node that
cannot find its peers says so itself (`WARN … Scouting delay elapsed before
start conditions are met`); `uv run zenode doctor` confirms it, with a
*multicast scouting* check that tells you whether discovery works on your host.
Your options, any one of which is enough:

1. Allow multicast scouting through the firewall (e.g. `sudo ufw allow 7446/udp`).
2. Skip discovery: set explicit `[transport] listen`/`connect` endpoints
   (what the examples do).
3. Run a `zenohd` router and use `mode = "client"` — the recommended setup
   for real deployments anyway.

## Observability, in one paragraph

Every node emits structured logs with a trace id, a health heartbeat carrying
the four golden signals plus CPU and memory, and a W3C trace context on every
message. `zenode logs --trace <id>` follows one message across the fleet;
`zenode trace <id>` reconstructs its path with no collector deployed. One
sidecar, `zenode export`, forwards all of it to Prometheus, Loki or any OTLP
endpoint. Spans are optional and cost nothing until you install
`zenode[otel]`.

Full detail: **[docs/open-telemetry.md](docs/open-telemetry.md)**.

## Large payloads

`Topic(..., shm=True)` publishes through shared memory. Measured cross-process
at 30 Hz, a 1080p RGB frame costs 1.91 ms to publish normally and 0.28 ms
through shared memory.

Full detail: **[docs/shared-memory.md](docs/shared-memory.md)**.

## Design notes

- **Zenoh is the transport, not an implementation detail to hedge against.**
  The public API hides zenoh types for ergonomics and testability, but zenode
  does not promise middleware portability — that trade buys liveliness,
  queryables, attachments, and zenoh-ext late-joiner recovery as first-class
  features.
- Latched topics use zenoh-ext advanced pub/sub (cache + history query +
  publisher detection). This requires HLC timestamping, which zenode enables
  on every session (`[transport] timestamping`, default on).
- `max_age` staleness checks compare sender timestamps against local time, so
  a host whose clock is off by more than `max_age` has *everything* it sends
  dropped. Synchronize clocks (chrony/NTP); the subscriber warns and counts
  (`stale`) rather than dropping silently. It is checked twice — on arrival and
  again at dequeue — and each stage names its own cause, because a sample that
  is old on arrival means the *sender's* clock is off while one that aged in
  the queue means *this node* is behind.
- `deadline` needs no synchronized clocks: it is measured on the monotonic loop
  clock, so it is the tool for "did my commander stop talking to me?" on a
  deployment where `max_age` is not usable.
- Scope fence: no launch system, no parameter server, no TF tree, no IDL/codegen.

## Status

Early (0.1.x): APIs may still move. Python ≥ 3.11, `eclipse-zenoh` ≥ 1.7.
