# Testing

Reference for `zenode.testing`: running nodes in-process, with no router and no
network configuration.

## Overview

`harness()` opens one peer-mode session with multicast disabled. zenoh routes
matching publishers and subscribers locally, so a typed round trip works
entirely in-process.

```python
import pytest
from zenode.testing import harness


@pytest.mark.integration
async def test_nav_publishes_a_command():
    async with harness() as h:
        await h.start_node(Nav, config=NavConfig(max_speed=1.0))
        commands = h.collect(MotionTopics.move)

        h.publisher(StateTopics.odometry).put(OdometryState(x=1.0))

        cmd = await commands.next()
        assert cmd.linear.x <= 1.0
```

The harness starts and stops every node it created, in reverse order, and
closes the session on exit.

## API

| Method | Purpose |
|---|---|
| `await start_node(node, *, config, namespace, **kwargs)` | Start a node on the harness session. |
| `publisher(topic)` | Publish as an external producer. |
| `collect(topic, *, mode, queue_size)` | Collect everything published on a topic. |
| `subscribe(topic, handler, **kwargs)` | Raw subscription; returns the `Subscription`. |
| `await call(service, request, *, timeout)` | Call a service under test. |
| `await stop_node(node)` | Stop one node early. |

`Collector` exposes `items`, `envelopes`, `await next(timeout=2.0)` and
`clear()`.

## Injecting fakes

Extra keyword arguments go to the node's `__init__`, which is how a node under
test receives a fake instead of the hardware it would otherwise open:

```python
await h.start_node(MotorNode, config=cfg, axes=[FakeAxis(), FakeAxis()])
```

For a node with a custom `__init__`, or one that needs setting up first, pass
an instance. The harness points it at its own session, so construct it without
transport arguments:

```python
node = MotorNode(axes=[FakeAxis()])
node.calibrate()
await h.start_node(node)
```

## Testing handlers directly

The decorators only stamp metadata, so a decorated handler is still an ordinary
method. Where a test is about the logic rather than the plumbing, call it:

```python
async def test_clamps_to_max_speed():
    nav = Nav(config=NavConfig(max_speed=0.5))
    nav.cmd = FakePublisher()
    await nav.on_pose(OdometryState(x=10.0))
    assert nav.cmd.last.linear.x == 0.5
```

This needs no harness, no session and no event-loop coordination. Prefer it for
logic; use the harness when the thing under test *is* the message flow.

## Timing

Two rules avoid almost all flakiness.

**Await the thing you are asserting on, not a sleep.** `Collector.next()` has a
timeout and fails loudly.

```python
cmd = await commands.next()          # yes
await asyncio.sleep(0.5)             # no
assert commands.items
```

**A collector receiving a message says nothing about other subscribers.**
Nodes subscribe independently, so `next()` returning does not mean a node under
test has handled anything. Poll for the state you care about:

```python
async def wait_for(predicate, timeout=2.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.02)
    return False


assert await wait_for(lambda: node.stopped)
```

Asserting on a second subscriber's state immediately after `next()` is the most
common source of flaky tests.

## Namespaces

`harness(namespace="test")` scopes every key, which isolates a test run from
anything else on the machine — including a real robot on the same LAN, since
multicast is disabled in the harness but a router may not be.

## Markers

Tests that use the harness open a real zenoh session and are slower than unit
tests. The project marks them:

```python
@pytest.mark.integration
async def test_something(): ...
```

```toml
[tool.pytest.ini_options]
markers = ["integration: exercises a real in-process zenoh session (slower)"]
```

```bash
pytest -m "not integration"    # fast pass
```

## Asserting over the contract

The `TopicSet` registry makes the contract itself testable:

```python
def test_every_topic_is_documented():
    for entry, topic in registered_topics("my_robot.contract"):
        assert topic.description, f"{entry.attr} has no description"


def test_command_topics_expire():
    for _, topic in registered_topics("my_robot.contract"):
        if topic.key.startswith("command/"):
            assert topic.max_age is not None
```

Pass your own module prefix — the registry is process-global and will contain
zenode's own topics as well as yours.
