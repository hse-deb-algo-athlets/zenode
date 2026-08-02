"""The test harness itself: how a node under test gets its dependencies.

A node whose whole job is talking to hardware is only testable if the test can
hand it a fake. Both routes are covered here — constructor keyword arguments
through ``start_node``, and an instance the test built itself — because
otherwise every hardware node grows its own differently-shaped seam that exists
only for tests.
"""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import BaseModel

from zenode import Node, NodeConfig, Topic, publish
from zenode.testing import harness


class Reading(BaseModel):
    value: float = 0.0


SENSOR = Topic("harness/sensor", Reading)


class FakeDevice:
    """Stands in for something that would otherwise open a socket at import time."""

    def __init__(self, value: float = 42.0) -> None:
        self.value = value
        self.closed = False

    def read(self) -> float:
        return self.value

    def close(self) -> None:
        self.closed = True


class SensorConfig(NodeConfig):
    scale: float = 1.0


class SensorNode(Node):
    """A node that owns a device — the shape every hardware node has."""

    name = "sensor"
    health_interval = None
    config: SensorConfig

    out = publish(SENSOR)

    def __init__(self, device: FakeDevice | None = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.device = device if device is not None else FakeDevice(0.0)

    async def on_start(self) -> None:
        self.out.put(Reading(value=self.device.read() * self.config.scale))

    async def on_stop(self) -> None:
        self.device.close()


@pytest.mark.integration
async def test_constructor_kwargs_reach_the_node():
    async with harness() as h:
        out = h.collect(SENSOR)
        node = await h.start_node(
            SensorNode, config=SensorConfig(scale=2.0), device=FakeDevice(21.0)
        )
        assert (await out.next()).value == 42.0
        assert node.config.scale == 2.0


@pytest.mark.integration
async def test_an_instance_can_be_started_as_is():
    """The general form: a node with any ``__init__`` the test wants to call."""
    device = FakeDevice(7.0)
    async with harness() as h:
        out = h.collect(SENSOR)
        node = await h.start_node(SensorNode(device, config=SensorConfig()))
        assert (await out.next()).value == 7.0
        assert node.device is device


@pytest.mark.integration
async def test_a_started_instance_is_shut_down_with_the_harness():
    device = FakeDevice()
    async with harness() as h:
        await h.start_node(SensorNode(device, config=SensorConfig()))
    assert device.closed, "the harness must shut down nodes it started, instances included"


@pytest.mark.integration
async def test_an_instance_does_not_own_the_harness_session():
    async with harness() as h:
        node = await h.start_node(SensorNode(FakeDevice(), config=SensorConfig()))
        await node.shutdown()
        h.publisher(SENSOR)  # the session is still open


@pytest.mark.integration
async def test_an_instance_plus_constructor_arguments_is_refused():
    async with harness() as h:
        with pytest.raises(TypeError, match="already-constructed"):
            await h.start_node(SensorNode(config=SensorConfig()), config=SensorConfig())
        with pytest.raises(TypeError, match="already-constructed"):
            await h.start_node(SensorNode(config=SensorConfig()), device=FakeDevice())


@pytest.mark.integration
async def test_the_harness_namespace_reaches_an_instance():
    async with harness("robodog") as h:
        node = await h.start_node(SensorNode(FakeDevice(), config=SensorConfig()))
        assert node.namespace == "robodog"
        assert node.key("state/x") == "robodog/state/x"


@pytest.mark.integration
async def test_a_running_node_refuses_to_adopt_another_session():
    async with harness() as h:
        node = await h.start_node(SensorNode(FakeDevice(), config=SensorConfig()))
        with pytest.raises(RuntimeError, match="already running"):
            node.adopt_session(h.session)
