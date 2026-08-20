# Shared memory

Reference for publishing large payloads through shared memory: when it helps,
what must be configured, how failures behave, and how it is implemented.

## Overview

A normal publish copies the payload into the transport. For a camera frame that
copy dominates the cost of publishing. Declaring `Topic(..., shm=True)` places
the payload in a shared-memory segment instead and sends a handle, removing the
transport copy between processes on one host.

Only the publish side changes. A subscriber reads a shared-memory sample
exactly as it reads any other, so no subscribing code is aware of it.

Shared memory is opt-in per topic and requires the transport to enable it at
both ends. Every failure falls back to a normal publish.

## When it helps

Measured cross-process at 30 Hz, two processes over TCP loopback, 150 frames.
Both the copy into the segment and the copy out of it are included.

| Payload | Publish (plain) | Publish (SHM) | Latency (plain) | Latency (SHM) |
|---|---|---|---|---|
| VGA RGB, 0.92 MB | 0.299 ms | 0.059 ms | 0.296 ms | 0.097 ms |
| 720p RGB, 2.76 MB | 0.816 ms | 0.126 ms | 0.768 ms | 0.163 ms |
| 1080p RGB, 6.22 MB | 1.906 ms | 0.275 ms | 1.765 ms | 0.314 ms |

The gain scales with payload size, from about 5× at VGA to 7× at 1080p. In
absolute terms, publishing 1080p at 30 Hz costs roughly 6 % of one core through
the normal path and 0.8 % through shared memory.

It is not worth enabling below a few tens of kilobytes, where the allocation
costs more than the copy it avoids.

**Tail latency is worse.** At 1080p the maximum observed latency was 4.89 ms
under shared memory against 2.96 ms plain, caused by pool reclamation. Medians
and p90 are unaffected. A hard real-time loop should measure this rather than
assume the median.

## Configuration

### Topic

| Attribute | Default | Effect |
|---|---|---|
| `shm` | `False` | Publish through shared memory when available. |

```python
FRAME = Topic("camera/rgb", bytes, codec=RawCodec(Encoding.IMAGE_JPEG), shm=True)
```

### Transport

| Attribute | Default | Effect |
|---|---|---|
| `shared_memory` | `False` | Enable zenoh's shared-memory transport. |

```toml
[transport]
shared_memory = true
```

Required at **both** ends. A `shm=True` topic published to a peer without it
still delivers correctly, through the normal path. The publishing node logs a
warning when its own transport has it disabled.

### Node

| Attribute | Default | Effect |
|---|---|---|
| `shm_pool_bytes` | 64 MiB | Size of the shared-memory pool, created on first use. |

The pool is reclaimed on each allocation, so its size bounds frames in flight
rather than throughput. A larger pool reclaims less often, and reclamation is
what produces the latency outliers above.

### System requirements

Shared-memory segments are bounded by `RLIMIT_MEMLOCK`, not by the size of
`/dev/shm`. The common default is 8 MB, which is smaller than one 1080p frame.

```ini
[Service]
LimitMEMLOCK=infinity
```

```bash
docker run --ulimit memlock=-1:-1 …
```

For a login session, `/etc/security/limits.d/30-memlock.conf`:

```
<user> hard memlock unlimited
<user> soft memlock unlimited
```

On systemd hosts, PAM limits apply only to login sessions; services take
`DefaultLimitMEMLOCK` from `/etc/systemd/system.conf.d/`. Both need setting if
nodes run under systemd, and the change takes effect on reboot.

`zenode doctor` reports the module availability and the current limit.

## Behaviour

### Failures fall back

A node never fails to publish because shared memory is unavailable. Provider
creation failure, pool exhaustion, and errors from zenoh's SHM layer all result
in a normal publish of the same payload.

Fallbacks are counted as `NodeHealth.shm_fallbacks` and exported as
`zenode_node_shm_fallbacks_total`. A sustained non-zero rate means the topic is
running on the slower path — the failure mode is not an error but a silent loss
of the speedup, so it is worth an alert.

Warnings are rate-limited to one per 30 seconds, so a persistent failure does
not produce a log line per frame.

### It is not zero-copy

zenoh's Python objects — `ZShmMut`, `ZShm`, and `ZBytes` — expose `__bytes__`
but not the buffer protocol, verified on 1.9.0. `memoryview()` raises on all
three, on both the write and the read side.

Consequently the payload is copied into the segment on publish and out of it on
receive:

```
your bytes ──copy──> segment ──no copy──> receiver ──copy──> your bytes
```

The transport copy is removed; the two application copies are not. A NumPy view
onto a live shared frame is not reachable from Python today. This requires
buffer-protocol support in zenoh's PyO3 layer and cannot be worked around from
Python.

The measurements above already include both copies.

## Troubleshooting

**No speedup, `shm_fallbacks` rising.** The pool could not be created, almost
always `RLIMIT_MEMLOCK`. Check `ulimit -l` and `zenode doctor`.

**No speedup, `shm_fallbacks` zero.** Shared memory is disabled on the
transport at one end, so payloads are serialised normally despite being
allocated in the pool. Both ends need `shared_memory = true`.

**A warning that the topic declares `shm=True` while the transport does not.**
Exactly the case above, detected at publisher creation.

**Publishing slows down after a few frames.** The pool is exhausted and not
reclaiming. Raise `shm_pool_bytes`, or check that the subscriber is consuming.

**Occasional latency spikes.** Pool reclamation. Raise `shm_pool_bytes` to
reclaim less often.

## Example

`examples/shm_camera.py` publishes 1080p frames between two processes and takes
`--plain` to run the identical pipeline without shared memory:

```bash
python examples/shm_camera.py camera      # and: detector
python examples/shm_camera.py camera --plain
```

```
--plain    publish median 1.83 ms    receive age 1.38 ms
shm        publish median 0.27 ms    receive age 0.42 ms
```

---

# Implementation notes

## Publish side only

`Publisher._publish` asks the node's pool for a buffer and falls back to the
encoded bytes when it returns `None`. `Subscription` is untouched: zenoh hands
a shared-memory sample to `payload.to_bytes()` like any other, so there is no
receive-side branch to maintain and no way for a subscriber to be written
incorrectly.

This also means shared memory composes with any codec. It is a transport
detail applied after encoding, not a payload format.

## The zenoh API

```python
provider = ShmProvider.default_backend(MemoryLayout(size))
buffer = provider.alloc(n, GarbageCollect(inner_policy=Defragment()))
buffer[0:n] = payload
publisher.put(buffer)
```

Two details are load-bearing:

- **The default allocation policy never reclaims.** `JustAlloc` fills a pool
  after a handful of frames, after which every allocation fails — and because
  the fallback is a normal publish, it fails invisibly. `GarbageCollect` wrapping
  `Defragment` is what keeps a 30 Hz camera allocating indefinitely.
- **The provider must be created before the session** when `RLIMIT_MEMLOCK` is
  tight, because both draw on the same budget. zenode creates the pool lazily,
  which is why raising the limit is documented as a prerequisite rather than a
  suggestion.

## Failure handling

`ShmPool` catches `BaseException`, re-raising only `KeyboardInterrupt` and
`SystemExit`. This is deliberate: zenoh's allocation path can panic out of Rust
as `pyo3_runtime.PanicException`, which derives from `BaseException` rather than
`Exception`. An ordinary `except Exception` would let that terminate the
publishing thread — for a camera node, on a condition that is recoverable by
publishing normally.

A provider that fails to build is marked unavailable and never retried.
Retrying per frame would cost more than the copy shared memory was avoiding.

## Typing

`zenoh.shm` is imported through an untyped module handle rather than a direct
import. zenoh 1.9.0 ships stubs that disagree with the compiled module —
`MemoryLayout` declares an `alignment` parameter the runtime rejects — and marks
the whole surface `@_unstable`. Treating it as `Any` keeps a wrong stub from
becoming a build error, and suits an API that may change.

## Verification

Unit tests cover the pool's fallback behaviour, including a stand-in for a
`BaseException` panic and the rate limiting of warnings. They deliberately do
not assert on throughput.

Performance and the fact that samples are genuinely shared-memory backed are
verified out of band, across processes, by checking `payload.as_shm()` on the
receiving side. An SHM run that silently falls back produces numbers close to
the plain path, so any benchmark of this must confirm the payloads were backed
rather than infer it from timing.
