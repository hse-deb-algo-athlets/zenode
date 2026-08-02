# Open-Telemetry

Reference for zenode's logging, tracing and metrics: what is enabled by
default, what must be configured, and how the pieces are implemented.

## Overview

A node started with `run()` emits four things without configuration:
structured logs, a W3C trace context on every message, a health heartbeat, and
a bounded record of recent message hops. A separate sidecar process,
`zenode export`, forwards health and logs to external systems. Spans are
optional and require an OpenTelemetry SDK supplied by the application.

## Defaults

| Signal | Destination | Controlled by |
|---|---|---|
| Structured logs | stderr | `ZENODE_LOG`, `ZENODE_LOG_FORMAT` |
| Trace id on log records | every record | automatic when a trace is active |
| Log records | `<ns>/node/<name>/log` | `publish_logs_at` (`"WARNING"`) |
| Health heartbeat | `<ns>/node/<name>/health` | `health_interval` (`2.0`) |
| Hop records | `<ns>/node/<name>/trace` | `trace_ring` (`4096`) |
| Trace context | zenoh attachment | `Topic(trace=…, trace_ratio=…)` |

## Configuration

### Node attributes

| Attribute | Default | Effect |
|---|---|---|
| `health_interval` | `2.0` | Seconds between heartbeats. `None` disables. |
| `publish_logs_at` | `"WARNING"` | Minimum level published to the log topic. `None` disables. |
| `trace_ring` | `4096` | Hops retained for `zenode trace`. `0` disables the ring and its service. |
| `allow_duplicates` | `True` | When `False`, a second node of the same name raises `DuplicateNodeError` instead of warning. |

### Topic attributes

| Attribute | Default | Effect |
|---|---|---|
| `trace` | `False` | Publishing starts a trace when none is active. |
| `trace_ratio` | `1.0` | Fraction of traces started here that are sampled. |

### Environment

| Variable | Effect |
|---|---|
| `ZENODE_LOG` | Console log level. Default `INFO`. |
| `ZENODE_LOG_FORMAT` | `human`, `json`, or `auto` (default: human on a tty, JSON otherwise). |
| `RUST_LOG` | zenoh's own Rust-side logging. `off` to silence. |

## Tracing

### Trace lifetime

A trace begins when a message is published on a topic declared
`Topic(..., trace=True)` and no trace is currently active, or explicitly:

```python
with trace.using(trace.new_traceparent()):
    self.frames.put(self.camera.read())
```

The context propagates from a subscription or service handler into everything
it causes:

| Call from a handler             | Propagates   |
|---------------------------------|--------------|
| `publisher.put(...)`            | yes          |
| `await self.call(service, ...)` | yes          |
| `self.spawn(...)`               | yes          |
| `await self.blocking(...)`      | yes          |
| Timer body (`@every`)           | no           |

A trace ends when no further message is published in the chain.

Timer bodies run outside any trace, because a tick is caused by the clock
rather than by a message. Code that stores a message in a handler and publishes
it from a timer must carry the context explicitly:

```python
@subscribe(CAMERA)
async def on_frame(self, msg):
    self.latest = (msg, trace.current())

@every(0.1)
async def tick(self):
    msg, traceparent = self.latest
    with trace.using(traceparent):
        self.out.put(msg)
```

### Multiple trace roots

Several topics may be declared `trace=True`. A topic starts a trace only when
no trace is active; published inside an existing trace it continues that trace
instead. A pipeline therefore remains a single trace even when more than one of
its topics is marked as a root.

Consequently `trace_ratio` takes effect only on the topic that actually starts
the trace. A downstream root inherits the upstream sampling decision. Apply the
ratio to the topic that begins the pipeline.

### Sampling

`trace_ratio` reduces the proportion of traces that are recorded as spans.
Unsampled traces still carry a trace id and still appear on log records; only
span recording is skipped.

```python
CAMERA = Topic("camera/rgb", Frame, trace=True, trace_ratio=0.01)
```

Cost per `put()`, measured over 200 000 iterations:

| Configuration                | Untraced topic   | Traced topic   |
|------------------------------|------------------|----------------|
| `otel` extra absent          | 1.89 µs          | 3.63 µs        |
| `otel` extra present, no SDK | 1.88 µs          | 3.91 µs        |
| Recording                    | 1.90 µs          | 15.61 µs       |

Untraced topics are unaffected in all configurations. Recording adds ~13.7 µs
per message: 0.05 % of one core at 30 Hz, 16 % at 10 000 msg/s.

### Spans

Install the extra and register a provider to record spans:

```bash
pip install 'zenode[otel]' opentelemetry-sdk opentelemetry-exporter-otlp-proto-http
```

```python
def setup_telemetry(service_name: str) -> None:
    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
    provider.add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter(endpoint="http://localhost:4318/v1/traces"))
    )
    trace.set_tracer_provider(provider)
```

zenode records three spans — `publish <key>`, `process <key>` and
`serve <key>` — carrying OpenTelemetry messaging semantic conventions. It does
not construct a `TracerProvider`, select an exporter, or read `OTEL_*`
variables. See `examples/otel_pipeline.py` for a complete configuration.

Application spans nest normally. A message published inside one is parented on
it, and the trace context on the wire identifies it, so downstream nodes chain
onto the application span:

```python
with tracer.start_as_current_span("inference"):
    self.out.put(result)
```

## Logging

Each node publishes its own records on `<ns>/node/<name>/log` at
`publish_logs_at` and above. The handler is attached to the node's logger
rather than the root logger, so nodes sharing a process do not publish each
other's records; records from third-party libraries are not published.

The publish queue is bounded and discards the oldest record under pressure.
Discards are reported as `NodeHealth.logs_dropped` and flagged by
`zenode health`.

## Health metrics

`NodeHealth` is published every `health_interval` seconds.

| Group      | Fields                                                                  |
|------------|-------------------------------------------------------------------------|
| Traffic    | `sent`, `received`                                                      |
| Errors     | `handler_errors`                                                        |
| Saturation | `dropped`, `stale`, `timer_overruns`, `queue_max_depth`, `logs_dropped`, `shm_fallbacks` |
| Latency    | `age_mean_ms`, `age_max_ms`, `handler_mean_ms`, `handler_max_ms`        |
| Resources  | `cpu_percent`, `rss_bytes`                                              |

`age_*` measures publish-to-dequeue delay and depends on synchronised clocks
between machines. Counters are cumulative since node start; latencies and
`queue_max_depth` cover the interval since the previous heartbeat.
`cpu_percent` is expressed as a percentage of one core. `cpu_percent` and
`rss_bytes` are `None` where `/proc` is unavailable.

### Application metrics

`NodeHealth` is a fixed set of runtime signals and is not extensible: unknown
fields are discarded during validation, and both export paths iterate a fixed
table. Application measurements — battery voltage, temperature, disk headroom —
are recorded through the OpenTelemetry metrics API instead, which reaches the
same backend without involving zenode.

Register a `MeterProvider` alongside the tracer provider, using the same
`service.name` so that metrics, spans and logs group under one service:

```python
from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader

reader = PeriodicExportingMetricReader(
    OTLPMetricExporter(endpoint=f"{OTLP_ENDPOINT}/v1/metrics")
)
metrics.set_meter_provider(MeterProvider(resource=resource, metric_readers=[reader]))
```

Then instrument as usual — `create_counter`, `create_gauge`,
`create_up_down_counter` and `create_histogram` are all available:

```python
class Nav(Node):
    async def on_start(self) -> None:
        meter = metrics.get_meter("nav")
        self.battery = meter.create_gauge("robot.battery.volts")

    @every(1.0)
    async def sample(self) -> None:
        self.battery.set(self.sensors.voltage())
```

zenode is not in this path: it neither aggregates nor forwards these, and
`zenode export` is not involved. Requires `opentelemetry-sdk` and a metric
exporter in addition to `zenode[otel]`.

## Exporting

`zenode export` subscribes to the health and log topics and forwards them.

```bash
zenode export --prometheus :9100                                  # scrape endpoint
zenode export --otlp-metrics http://localhost:4318                # metrics push
zenode export --otlp-logs http://localhost:4318                   # logs push
```

| Option                     | Default   | Effect                                                    |
|----------------------------|-----------|-----------------------------------------------------------|
| `--prometheus [HOST:]PORT` | `:9100`   | Serve `/metrics` in Prometheus text format.               |
| `--otlp-metrics URL`       | —         | Push health to `URL/v1/metrics` every 15 s.               |
| `--otlp-logs URL`          | —         | Push log records to `URL/v1/logs` every 2 s.              |
| `--stale-after SECONDS`    | `60`      | Drop a node's series after this long without a heartbeat. |

Use the push options where the backend receives rather than scrapes
(`grafana/otel-lgtm` and most vendor stacks) or where the host cannot be
reached. Both paths produce identical series names and labels.

`zenode_node_info` and `zenode_node_last_seen_seconds` exist only on the scrape
path; the push path represents staleness by omitting the node.

OTLP log records carry `traceId`, which allows a backend to link a log record
to its trace.

### Monitoring the exporter

The scrape endpoint also publishes the exporter's own counters, so that a
telemetry pipeline which has stopped working is distinguishable from a system
with nothing to report:

```
zenode_exporter_log_records_total{outcome="shipped"|"queue_full"|"push_failed"}
zenode_exporter_metric_pushes_total{outcome="ok"|"failed"}
```

An unreachable collector otherwise only logs to the sidecar's own stderr, while
every signal silently stops. Alert on:

```promql
rate(zenode_exporter_metric_pushes_total{outcome="failed"}[5m]) > 0
```

## Command reference

| Command                                             | Purpose                                       |
|-----------------------------------------------------|-----------------------------------------------|
| `zenode logs [--node] [--level] [--trace] [--grep]` | Follow log records from every node.           |
| `zenode health [--watch]`                           | Table of every node's heartbeat.              |
| `zenode trace <trace-id>`                           | Reconstruct one trace from every node's ring. |
| `zenode export`                                     | Forward health and logs to external systems.  |
| `zenode nodes`, `topics`, `echo`, `hz`, `doctor`    | General inspection.                           |

`zenode trace` queries live nodes directly and requires no collector. Only
sampled traces are recorded.

## Troubleshooting

**Duplicate spans, counters or log records.** Two instances of the same node
are running; both subscribe, so every message is handled twice. zenode logs a
warning at startup. Set `allow_duplicates = False` to make this an error.

**A trace ends unexpectedly.** The chain probably crosses a timer boundary; see
*Trace lifetime*.

**A `trace=True` topic is never sampled.** It is downstream of another root and
inherits that root's sampling decision; see *Multiple trace roots*.

**Messages dropped as stale.** `max_age` compares the sender's clock to the
receiver's. Verify NTP synchronisation before investigating the sender.

**zenoh output is not JSON.** zenoh logs from Rust directly to stderr. Set
`RUST_LOG=off` for a strictly parseable stream.

---

# Implementation notes

## Dependency policy

The core requires `eclipse-zenoh` and `pydantic`. The single optional extra,
`zenode[otel]`, adds `opentelemetry-api` — pure Python, one transitive
dependency, and inert unless the application registers an SDK.

All other telemetry integration is hand-written JSON over `urllib`. This is
deliberate: zenode targets ARM hardware where `grpcio` and `protobuf`
complicate installation, and field deployments where an exporter buffering
against an unreachable collector is the normal condition. A change that
requires a compiled dependency should be treated as out of scope; `psutil` was
declined in favour of `/proc` reads, and protobuf in favour of OTLP/JSON.

## Module map

| Module                            | Responsibility                                             |
|-----------------------------------|------------------------------------------------------------|
| `trace.py`                        | W3C context, contextvar, sampling, `TraceRing`, log filter |
| `otel.py`                         | Span bridge. The only module importing OpenTelemetry       |
| `log.py`                          | Formatters, `setup_logging`, `LogPublisher`                |
| `metrics.py`                      | `Latency`, `ProcessStats`                                  |
| `exporter.py`                     | Prometheus exposition, shared `Metric` table, `Registry`   |
| `otlp_logs.py`, `otlp_metrics.py` | OTLP/JSON push                                             |
| `msgs/{health,log,trace}.py`      | Wire contracts and key helpers                             |

Instrumentation is confined to four call sites: `Publisher.put`,
`Subscription._dispatch`, `ServiceServer._handle` and `Node._publish_health`.

## Invariants

- **Bounded state.** Every buffer on the observability path has a fixed
  capacity and reports discards. Nothing may grow over a long-running process.
- **No global configuration from library code.** `setup_logging` is called by
  `run()`, which owns the process. An embedded `Node` emits only what it is
  asked to.
- **Correlation is dependency-free.** The trace id on log records must continue
  to work with no extras installed.
- **Absent rather than zero.** An unknown value omits its series.
- **Tolerant decoding.** A key expression is not a guarantee about payloads;
  unrecognised messages are skipped, not fatal.

## Metric definitions

`exporter.Metric` holds the Prometheus name, the OTLP name, a UCUM unit and the
accessor. Both export paths iterate the same table;
`test_both_paths_export_the_same_fields` enforces this.

Units are chosen so that a collector's OTLP-to-Prometheus normalisation yields
the names produced by the text exposition — `zenode.node.sent` with unit
`{message}` as a monotonic sum becomes `zenode_node_sent_total`. Data points
also repeat `node` and `namespace` as attributes, because collectors map
resource attributes to `service_name` and `job` while the text path uses
`node` and `namespace`.

