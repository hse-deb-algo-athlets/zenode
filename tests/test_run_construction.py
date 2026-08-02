"""Node construction paths: custom __init__, run()'s builder, harness compat."""

from typing import Any

import pytest
from pydantic import BaseModel

from zenode import Node, NodeConfig, Topic
from zenode.errors import ConfigError
from zenode.node import _build_node
from zenode.testing import harness, local_transport


class Ping(BaseModel):
    value: int = 0


class GainConfig(NodeConfig):
    gain: float = 2.0


class CustomInitNode(Node):
    """A node with its own __init__: extra args have defaults, kwargs forwarded."""

    name = "custom-init"
    health_interval = None
    config: GainConfig

    def __init__(self, amplitude: float = 0.5, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.amplitude = amplitude
        self.seen: list[float] = []

    async def on_start(self) -> None:
        self.subscribe(Topic("test/custom/in", Ping), self.on_ping)

    async def on_ping(self, msg: Ping) -> None:
        self.seen.append(msg.value * self.amplitude * self.config.gain)


def test_build_from_class_uses_default_config():
    node = _build_node(CustomInitNode, None, local_transport(), None)
    assert isinstance(node, CustomInitNode)
    assert node.amplitude == 0.5
    assert node.config.gain == 2.0


def test_build_passes_instance_through():
    instance = CustomInitNode(amplitude=3.0, transport=local_transport())
    assert _build_node(instance, None, None, None) is instance


def test_build_rejects_instance_plus_overrides():
    instance = CustomInitNode(transport=local_transport())
    with pytest.raises(ConfigError, match="already-constructed"):
        _build_node(instance, None, local_transport(), None)


@pytest.mark.integration
async def test_custom_init_node_runs_in_harness():
    async with harness() as h:
        node = await h.start_node(CustomInitNode, config=GainConfig(gain=10.0))
        h.publisher(Topic("test/custom/in", Ping)).put(Ping(value=4))
        import asyncio
        import time

        deadline = time.monotonic() + 2.0
        while not node.seen and time.monotonic() < deadline:
            await asyncio.sleep(0.02)
        assert node.seen == [4 * 0.5 * 10.0]
