"""Subscribes to the talker and calls its service once.

    uv run python examples/listener.py

Demonstrates declarative subscriptions (``@subscribe``). Uses default
multicast discovery to find the talker, matching talker.py.
"""

from contract import DemoServices, DemoTopics, SumReply, SumRequest
from talker import DEMO_ENDPOINT  # noqa: F401  (used by the variant below)

from zenode import Envelope, Node, run, subscribe
from zenode.msgs import Twist


class Listener(Node):
    name = "listener"

    @subscribe(DemoTopics.cmd_vel, mode="latest")
    async def on_cmd(self, msg: Twist, env: Envelope) -> None:
        age = env.age_s()
        self.log.info(
            "cmd_vel x=%+.2f",
            msg.linear.x,
            extra={"source": env.node, "age_ms": round((age or 0) * 1000, 1)},
        )

    @subscribe(DemoTopics.status)  # latched: last value arrives on join
    async def on_status(self, msg: SumReply) -> None:
        self.log.info("latched status on join", extra={"total": msg.total})

    async def on_start(self) -> None:
        # Imperative escape hatch: things only known at runtime stay here.
        await self.wait_for_nodes({"talker"}, timeout=10.0)
        reply = await self.call(DemoServices.sum, SumRequest(values=[1.0, 2.0, 3.5]))
        self.log.info("sum service replied", extra={"total": reply.total})


if __name__ == "__main__":
    run(Listener)

    # Explicit endpoint variant (works where multicast scouting is blocked):
    #   from zenode import TransportConfig
    #   run(Listener(transport=TransportConfig(connect=[DEMO_ENDPOINT])))
