# Configuration

Reference for configuring a deployment: the TOML file, environment overrides,
and how nodes share facts without duplicating them.

## Overview

One TOML file describes a deployment. Each node validates **only its own
section**, so a broken `[node.joy]` cannot prevent `nav` from starting.

```toml
[transport]
mode = "client"
connect = ["tcp/192.168.4.100:7447"]
namespace = "robodog"

[node.nav]
max-speed = 0.5

[geometry]
wheel-radius = 0.0485
```

A missing file means defaults: peer mode with multicast scouting, which works
on a LAN with no configuration at all.

## Precedence

Lowest to highest:

1. Model defaults
2. TOML file — `./zenode.toml`, or `$ZENODE_CONFIG`, or `run(config_path=…)`
3. Environment variables

Environment names are derived from the section: `ZENODE_TRANSPORT__CONNECT`,
`ZENODE_NAV__MAX_SPEED`, `ZENODE_GEOMETRY__WHEEL_RADIUS`. The double underscore
separates section from field.

Keys may be written `max-speed` or `max_speed`; both resolve to the same field.

## Node configuration

Declare a model and annotate it on the node. The section is `[node.<name>]`:

```python
class NavConfig(NodeConfig):
    max_speed: float = 0.5
    control_rate_hz: float = 15.0


class Nav(Node):
    name = "nav"
    config: NavConfig      # loaded automatically
```

`run()` loads and validates it before the node starts. A model with required
fields and nothing to fill them raises `ConfigError` with the validation
detail, at startup rather than at first use.

The loaded model is available as `self.config`, and `@every` can read a rate
from it by name:

```python
@every("control_rate_hz", unit="hz")
async def tick(self) -> None: ...
```

## Transport

```toml
[transport]
mode = "peer"                 # or "client"
connect = ["tcp/host:7447"]
listen = []
namespace = "robodog"
multicast_scouting = true
shared_memory = false
timestamping = true
```

| Field | Default | Effect |
|---|---|---|
| `mode` | `peer` | `peer` discovers others directly; `client` connects to a router. |
| `connect` | `[]` | Endpoints to dial. |
| `listen` | `[]` | Endpoints to accept on. |
| `namespace` | `""` | Prefixed to every relative key. |
| `multicast_scouting` | `true` | Automatic discovery on a LAN. |
| `shared_memory` | `false` | See [Shared memory](shared-memory.md). |
| `timestamping` | `true` | HLC timestamps; required for latched topics. |
| `overrides` | `{}` | Raw zenoh config entries, inserted by path. |

`overrides` is the escape hatch for anything zenode does not model:

```toml
[transport.overrides]
"transport/link/tx/queue/congestion_control/wait_before_drop" = 1000
```

### Choosing a mode

`peer` with multicast is right on a robot and on a lab LAN: nodes find each
other with no infrastructure. Use `client` with an explicit `connect` when
multicast is blocked — which is common on corporate networks, in Docker with
default bridge networking, and across subnets. `zenode doctor` reports whether
scouting is working.

## Shared sections

Facts belonging to the robot rather than to any one node — chassis geometry,
frame ids, calibration — get their own section instead of being copied into two
`[node.*]` tables where they can silently diverge:

```toml
[geometry]
wheel-radius = 0.0485
wheel-separation = 0.235
```

```python
class Geometry(BaseModel):
    wheel_radius: float
    wheel_separation: float


geometry = load_section(Geometry, "geometry")
```

`load_section` follows the same precedence and supports `ZENODE_GEOMETRY__*`.
Two nodes computing odometry from the same wheel radius should read it from one
place; see [Conventions](conventions.md) for what belongs there.

## Logging

Logging is configured by `run()`, from the environment rather than the TOML
file, so it can be changed per process without editing a shared deployment
file:

| Variable | Default | Effect |
|---|---|---|
| `ZENODE_LOG` | `INFO` | Console level. |
| `ZENODE_LOG_FORMAT` | `auto` | `human`, `json`, or `auto` (human on a tty, JSON when redirected). |
| `RUST_LOG` | `warn` | zenoh's own Rust-side logging. `off` to silence. |

See [Observability](open-telemetry.md) for what a node emits and how to collect
it.

## Checking a deployment

```bash
zenode doctor
```

Reports the config file in use, the resolved transport, whether a session
opens, connectivity, whether multicast scouting works, live nodes, shared
memory availability and the memlock limit. Run it first when a node cannot see
another.
