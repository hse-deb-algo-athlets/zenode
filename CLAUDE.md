# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

Dependency management and execution are `uv`-based (`uv_build` backend, `uv.lock` committed).

```bash
uv sync                                          # install, incl. the dev group
uv run pytest -q                                 # full suite (~15 s)
uv run pytest -m "not integration" -q            # fast pass, skips real zenoh sessions
uv run pytest tests/test_pubsub_unit.py -q       # one module
uv run pytest tests/test_pubsub_unit.py::test_name -q   # one test
uv run ruff check .                              # lint
uv run ruff format src tests examples            # format — see note below
uv run pyright                                   # type check (src + tests)
uv run ty check src tests                        # second type checker
```

`asyncio_mode = "auto"`, so async tests need no decorator. `--strict-markers` is on: a harness
test must carry `@pytest.mark.integration` or the marker fails the run.

Two tool caveats worth knowing before "fixing" what looks broken:

- `ruff format .` also reformats Python snippets inside `docs/*.md`, which are hand-aligned.
  Scope it to `src tests examples`.
- `uv run ty check` over the repo root reports one unresolved import in
  `examples/otel_pipeline.py` (an OTLP exporter that is deliberately not a dependency).
  Scope it to `src tests`.

Trying things end to end:

```bash
uv run python examples/talker.py                 # listens on tcp/127.0.0.1:17447
uv run python examples/listener.py
uv run zenode echo demo/cmd_vel --contract examples.contract --meta --connect tcp/127.0.0.1:17447
uv run zenode doctor                             # config, connectivity, multicast scouting
```

## Architecture

Four layers, each of which only depends on the ones above it.

**The contract** (`topic.py`, `codec.py`, `envelope.py`, `msgs/`). A frozen `Topic` binds key +
payload type + codec + delivery semantics (`latched`, `max_age`, `shm`, `trace`) in one
declaration that both sides import. `TopicSet.__init_subclass__` pushes every declared
`Topic`/`Service` into a **process-global registry** (`topic._REGISTRY`) — that registry is what
makes `zenode topics` and typed `zenode echo` possible, and it is why tests that assert on
registry *contents* use the `isolated_registry` fixture. Keys are relative; the deployment
namespace is prefixed at runtime by `resolve_key`. `Topic.absolute()` opts out.

**The runtime** (`node.py`, `pubsub.py`, `service.py`, `timers.py`, `presence.py`, `shm.py`).
`Node.start()` has a load-bearing order: session → duplicate-name check → liveliness token →
`publish()` descriptors materialized → log publishing → health timer → `on_start()` →
`_wire_bindings()` → trace service. Publishers exist *before* `on_start` so it can use them;
decorated bindings activate *after* it so a handler never sees a half-built node. The health
timer is the one deliberate exception, spawned before `on_start` so `state="starting"` is
observable while a node is still coming up. Any failure in that block runs `on_stop()` and tears
down, because `on_start` is where hardware is acquired.

`on_start` runs under `start_timeout` via `asyncio.timeout` — **not** `wait_for` — because since
3.11 asyncio's timeout *is* the builtin `TimeoutError`, which is also what a driver raises when
an axis does not answer; only `expired()` tells the two apart. Neither helps against a
*synchronous* blocking call, which stalls the loop and the signal handlers with it; `Node.blocking`
is the answer and the docs say so. Nodes are single-use: `_stop_event` is latched, so `start()`
refuses a node that has already run rather than coming up and exiting silently. Teardown bounds
its task join by `shutdown_timeout` because cancellation cannot reach a thread inside `blocking()`.

`declarative.py` decorators only stamp `__zenode_bindings__` metadata and return the function
unchanged — handlers stay directly callable in tests. `collect_bindings`/`collect_publishers`
walk the MRO base-first, so an undecorated override inherits its parent's binding.

The **thread boundary** is the invariant to preserve when touching `pubsub.py`/`service.py`:
zenoh worker threads only copy bytes out and `call_soon_threadsafe` them onto the loop. Every
user handler runs on the node's event loop, so there is one concurrency model.

**Observability** (`log.py`, `trace.py`, `otel.py`, `metrics.py`, `msgs/health.py`). Delivery
metadata rides in the zenoh **attachment**, not the payload — payloads stay readable by any
zenoh tool. `trace.py` propagates a W3C `traceparent` through a contextvar: only `Topic(trace=True)`
starts a trace, everything else continues an active one, and messages outside a trace pay one
contextvar read. `otel.py` is a strict no-op unless `zenode[otel]` is installed *and* the
application registered a provider — zenode never builds a `TracerProvider` or reads `OTEL_*`.
Every counter a subscription/timer/server keeps (`received`, `dropped`, `stale`, `errors`,
`overruns`, `deadline_misses`, `shm_fallbacks`) is summed onto the `NodeHealth` heartbeat.

**Out-of-process tooling** (`cli.py`, `exporter.py`, `otlp_logs.py`, `otlp_metrics.py`). Nodes
publish health/logs on the bus; `zenode export` is a sidecar that re-serves them for Prometheus
pull or pushes OTLP. Nodes themselves never link a metrics SDK. `exporter.py`'s `COUNTERS`/`GAUGES`
table is shared with `otlp_metrics.py` so pull and push can't drift apart.

**Testing** (`testing.py`). `harness()` opens one peer-mode session with multicast off; zenoh
routes matching pub/sub in-process, so typed round trips need no router. An internal `_Probe`
node backs `h.publisher()`/`h.collect()`/`h.call()`.

### Reserved keys

`<ns>/node/**` belongs to the runtime; applications must not publish there. The key builders are
`presence.presence_key`, `msgs.health.health_key`, `msgs.log.log_key`, `msgs.trace.trace_key`,
each with a matching `*_pattern` for the CLI side.

### Two clocks

Deliberately distinct, and conflating them is a real bug: `Topic.max_age` / `Envelope.age_s()`
use the **wall** clock and therefore require NTP-synced hosts, while a subscription `deadline`
(silence detection) uses the **monotonic** loop clock and never needs sync. `max_age` is checked
twice — on arrival (sender's clock is off) and on dequeue (this node is behind) — and each stage
logs its own cause on purpose.

## Project constraints

These are decisions the code defends, not preferences. Changing one is a design change.

- **Runtime dependencies are `eclipse-zenoh` and `pydantic`, full stop.** The codebase reads
  `/proc` instead of taking `psutil`, hand-rolls OTLP/JSON instead of taking protobuf/gRPC, and
  puts `opentelemetry-api` (API only) behind the `otel` extra. Reason: ARM robot targets. Do not
  add a compiled dependency without raising it explicitly.
- **Degrade, never crash.** A malformed payload, a raising handler, an unavailable shared-memory
  pool, an unreachable OTLP endpoint — each is logged, counted, and survived.
- **Bounded by construction.** `deque(maxlen=…)`, mean+max instead of percentiles, explicit queue
  drops. Nothing may grow on a robot that runs for a week.
- **Scope fence** (from the README): no launch system, no parameter server, no TF tree, no
  IDL/codegen, and no standard topic *keys* — `docs/conventions.md` §7 explains why the last one
  will not change.
- **Zenoh is the transport, not an abstraction to hedge against.** The public API hides zenoh
  types for ergonomics and testability; middleware portability is explicitly not promised.

## Conventions

`docs/conventions.md` is **normative for `zenode.msgs` and zenode-owned keys**: SI units on the
wire (radians, never `*_deg`; SoC as 0.0–1.0, never percent), right-handed FLU body / ENU world
frames, quaternions in xyzw order, and no ROS-style `Header` — time and provenance live in the
`Envelope`, spatial `frame_id` lives in the payload. It ends with a checklist; use it before
adding anything to `msgs/`.

Docstrings in this codebase carry the *why* (trade-offs, failure modes, what breaks otherwise),
not restatements of the signature. Match that when editing; the module docstrings are the fastest
route into any file. `docs/` mirrors the same material for users — a behavior change usually
means editing the matching page.
