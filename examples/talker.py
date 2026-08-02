"""Publishes Twist at 10 Hz, keeps a latched status, serves a sum service.

    uv run python examples/talker.py

Demonstrates declarative wiring — ``publish()`` descriptors plus ``@serve``
and ``@every`` — and a node with its own ``__init__`` (extra constructor
parameters get defaults, framework kwargs are forwarded to ``super()``).

Discovery: this version relies on zenoh's multicast scouting. Where that is
blocked (ufw/firewalld often drop UDP 7446 — `zenode doctor` will tell you),
use the explicit-endpoint variant at the bottom instead.
"""

import math
import time
from typing import Any

from contract import DemoServices, DemoTopics, SumReply, SumRequest

from zenode import Node, every, publish, run, serve
from zenode.msgs import Twist, Vector3

DEMO_ENDPOINT = "tcp/127.0.0.1:17447"


class Talker(Node):
    name = "talker"

    cmd = publish(DemoTopics.cmd_vel)  # typed Publisher once the node starts
    status = publish(DemoTopics.status)

    def __init__(self, amplitude: float = 0.5, **kwargs: Any) -> None:
        super().__init__(**kwargs)  # first — wires config/transport/logging
        self.amplitude = amplitude

    async def on_start(self) -> None:
        # publish() descriptors are already materialized here; decorated
        # bindings (@every/@serve/@subscribe) activate right after on_start.
        self.status.put(SumReply(total=0.0))

    @every(0.1)
    async def tick(self) -> None:
        t = time.monotonic()
        self.cmd.put(Twist(linear=Vector3(x=self.amplitude * math.sin(t)), angular=Vector3(z=0.2)))

    @serve(DemoServices.sum)
    async def on_sum(self, req: SumRequest) -> SumReply:
        return SumReply(total=sum(req.values))


if __name__ == "__main__":
    run(Talker(amplitude=0.5))

    # Variants:
    #
    # No custom constructor args needed? Pass the class and run() also loads
    # [node.talker] config from zenode.toml/env for you:
    #   run(Talker)
    #
    # Explicit endpoint (works where multicast scouting is blocked):
    #   from zenode import TransportConfig
    #   run(Talker(amplitude=0.5, transport=TransportConfig(listen=[DEMO_ENDPOINT])))
    #
    # Instance + still honor zenode.toml/env for the transport:
    #   from zenode import load_transport_config
    #   run(Talker(amplitude=0.5, transport=load_transport_config()))
