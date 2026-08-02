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
    cmd_vel = Topic("demo/cmd_vel", Twist, max_age=1.0, trace=True)
    status = Topic("demo/status", SumReply, latched=True)


class DemoServices(TopicSet):
    sum = Service("demo/sum", request=SumRequest, reply=SumReply)
