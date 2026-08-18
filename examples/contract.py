"""The example contract: what talker and listener agree on."""

from pydantic import BaseModel

from zenode import Service, Topic, TopicSet
from zenode.msgs import Twist


class SumRequest(BaseModel):
    values: list[float] = []


class SumReply(BaseModel):
    total: float = 0.0


class DemoTopics(TopicSet):
    # trace=True makes cmd_vel a trace root: every message starts a trace that
    # follows the data downstream, so the listener's logs — and the service call
    # it makes — carry the same `trace=` id as the talker's publish.
    #
    # The QoS says what happens to this topic once a link is congested. A
    # velocity command outranks everything else here, and at 10 Hz it is small
    # and rare enough that express=True (send now, do not wait to batch) buys
    # latency cheaply — on a 30 Hz camera it would cost more than it returns.
    # congestion_control stays at the "drop" default on purpose: the next
    # command supersedes this one in 100 ms, so a full queue is a reason to
    # skip a message, never a reason to block the caller's event loop.
    cmd_vel = Topic(
        "demo/cmd_vel",
        Twist,
        max_age=1.0,
        trace=True,
        priority="real_time",
        express=True,
    )
    # Status is diagnostics, so it yields to the command stream. Latched, which
    # is also why "drop" is harmless here: a subscriber that misses this one
    # still gets the value from the publisher's cache when it joins.
    status = Topic("demo/status", SumReply, latched=True, priority="data_low")


class DemoServices(TopicSet):
    sum = Service("demo/sum", request=SumRequest, reply=SumReply)
