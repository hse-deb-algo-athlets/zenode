# CLI

Reference for the `zenode` command: inspecting a running system, and forwarding
its telemetry.

## Overview

```
zenode {topics,echo,hz,health,logs,trace,export,nodes,doctor}
```

Every command reads the same configuration a node does, so it addresses the
same deployment without further arguments. `nodes` answers *is it up?*;
`health` answers *how well is it doing?*

## Common options

Accepted by every command:

| Option | Effect |
|---|---|
| `--config PATH` | Config file. Default `$ZENODE_CONFIG` or `./zenode.toml`. |
| `--connect ENDPOINT` | Endpoint to dial, e.g. `tcp/192.168.4.100:7447`. Repeatable. |
| `--mode {peer,client}` | Override the session mode. |
| `-n, --namespace NAME` | Override the deployment namespace. |
| `--contract MODULE` | Import a module defining `TopicSet`s, for typed output. Accepted by `topics`, `echo` and `doctor`. |

`--contract` is what makes output typed rather than raw. Point it at your
contract package:

```bash
zenode echo state/odometry --contract my_robot.contract -n robodog
```

## Inspecting the contract

### `topics`

Lists the registered contract — every `Topic` and `Service` declared in an
imported `TopicSet`, with its flags and where it was declared.

```bash
zenode topics --contract my_robot.contract
```

```
KEY                        SCHEMA          FLAGS                                  OWNER
state/odometry             OdometryState   -                                      contract.Robot.odometry
command/cmd_vel            Twist           max_age=0.5,prio=real_time,express     contract.Robot.cmd_vel
state/battery              Battery         latched(1),block                       contract.Robot.battery
camera/rgb                 bytes           trace@0.01,shm,prio=data_low           contract.Robot.camera
```

Flags show what the contract promises: `latched(n)`, `max_age=`, `trace` or
`trace@<ratio>` for a trace root, `shm`, and the
[QoS](contracts.md#quality-of-service) settings — `prio=<band>`, `block`,
`express` — each shown only when it differs from the default, so the listing
highlights the topics that claim an exception rather than repeating `prio=data`
on every row.

## Watching data

### `echo`

Prints messages on a key, decoded with the contract's type when known.

```bash
zenode echo state/odometry --contract my_robot.contract --meta
```

| Option | Effect |
|---|---|
| `--raw` | Skip typed decoding. |
| `--meta` | Show sender, sequence number and age per message. |
| `--pretty` | Pretty-print JSON. |
| `--absolute` | Do not prefix the namespace. |

`--meta` is how you tell *nothing is being published* from *it is arriving and
being dropped*: it shows the sender and the age of every sample.

### `hz`

Measures the publish rate on a key over a rolling window.

```bash
zenode hz camera/rgb --window 5
```

Use it to confirm a producer actually holds its rate, rather than inferring it
from a timer's configuration.

## Watching nodes

### `nodes`

Lists nodes holding a liveliness token.

```bash
zenode nodes --watch
```

`--watch` prints join and leave events as they happen, which is the quickest
way to catch a node that is restarting in a loop.

### `health`

One row per node, from the heartbeat every node publishes.

```bash
zenode health --watch
```

```
NODE       STATE     UP  SEEN  CPU%     RSS  SENT  RECV  QMAX  DROP STALE  ERR OVER  MISS   MSG AGE ms  HANDLER ms
camera     running   6s  1.9s   4.2   50.1M   177     0     0     0     0    0    0     0      0.0/0.0     0.0/0.0
perception running   6s  1.9s  61.8  184.3M     2   175    12     0     0    0    3     0   10.1/116.9  10.1/151.0
```

| Column | Meaning |
|---|---|
| `SEEN` | Seconds since that node last reported. A number that stops rising is a node that stopped. |
| `QMAX` | Deepest any queue got since the last heartbeat — warns before `DROP` starts. |
| `OVER` | Timer overruns. |
| `MISS` | Deadline misses: a subscription that went silent. |
| `MSG AGE` / `HANDLER` | mean/max in milliseconds, over the last interval. |

Exits non-zero when nothing answered, so it composes into a health check.
`--wait` sets how long to collect first; `--watch` refreshes in place.

## Debugging a pipeline

### `logs`

Follows log records from every node, off the bus rather than out of eleven
terminals.

```bash
zenode logs --level WARNING
zenode logs --node nav --grep "wheel slip"
zenode logs --trace 45d06757a549f538cf9ada58309e0bef
```

| Option | Effect |
|---|---|
| `--node NAME` | One node only. |
| `--level LEVEL` | Minimum level. Default `INFO`. |
| `--trace ID` | Only records carrying this trace id. |
| `--grep TEXT` | Only records whose message contains TEXT. |

Nodes publish at `publish_logs_at` (default `WARNING`) and above, so that is
the floor regardless of `--level`.

`--trace` is the 2am command: one trace id, every hop of it, across the fleet.

### `trace`

Reconstructs one trace from every node's ring buffer.

```bash
zenode trace 45d06757a549f538cf9ada58309e0bef
```

```
TRACE 45d06757a549f538cf9ada58309e0bef
  detector  camera/rgb        seq 9  ← camera    age 20.6ms  handler 20.3ms  span 1481c709…
  motors    perception/boxes  seq 9  ← detector  age  4.4ms  handler  4.2ms  span 50037f00…
```

No collector required — it queries live nodes directly. Only sampled traces are
recorded; see [Observability](open-telemetry.md).

## Forwarding telemetry

### `export`

A sidecar that re-serves node health and logs to external systems.

```bash
zenode export --prometheus :9100
zenode export --otlp-metrics http://localhost:4318 --otlp-logs http://localhost:4318
```

| Option | Default | Effect |
|---|---|---|
| `--prometheus [HOST:]PORT` | `:9100` | Serve `/metrics` in Prometheus text format. |
| `--otlp-metrics URL` | — | Push health to `URL/v1/metrics`. |
| `--otlp-logs URL` | — | Push log records to `URL/v1/logs`. |
| `--stale-after SECONDS` | `60` | Drop a node's series after this long without a heartbeat. |

Use the push options where the backend receives rather than scrapes, or where
the host cannot be reached. Full detail in [Observability](open-telemetry.md).

## Diagnosing

### `doctor`

Checks a deployment end to end and exits non-zero on a problem.

```bash
zenode doctor --contract my_robot.contract
```

```
zenode 0.1.0 doctor
  ✓ config file: /home/fabian/robot/zenode.toml
  ✓ transport: mode=peer connect=- namespace=robodog
  ✓ session open: 12ms, zid=a3f1…
  ✓ connectivity: 0 router(s), 2 peer(s)
  ✓ multicast scouting: 3 hello(s) received
  ✓ live nodes: camera, detector, motors
  ✓ shared memory module: available
  ✓ memlock limit: unlimited
  ✓ contract: 12 topics, 3 services
```

Run it first whenever nodes cannot see each other. The multicast check is the
one that usually explains it: a firewall dropping UDP 7446 is the most common
cause, and the check says so rather than leaving you to guess.
