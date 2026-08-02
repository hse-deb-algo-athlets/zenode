import pytest
from pydantic import BaseModel

from zenode import (
    Service,
    Topic,
    TopicSet,
    find_topic,
    registered_entries,
    registered_services,
    registered_topics,
)
from zenode.errors import ContractError


class Msg(BaseModel):
    value: int = 0


def test_resolve_with_namespace():
    t = Topic("state/odometry", Msg)
    assert t.resolve("") == "state/odometry"
    assert t.resolve("robodog") == "robodog/state/odometry"


def test_absolute_ignores_namespace():
    t = Topic.absolute("livox/lidar", bytes)
    assert t.resolve("robodog") == "livox/lidar"


@pytest.mark.parametrize("bad", ["", "/lead", "trail/", "a//b", "a b"])
def test_key_validation(bad):
    with pytest.raises(ContractError):
        Topic(bad, Msg)


def test_semantics_validation():
    with pytest.raises(ContractError):
        Topic("k", Msg, max_age=0)
    with pytest.raises(ContractError):
        Topic("k", Msg, latched=True, history=0)


def test_topicset_registers():
    class DemoSet(TopicSet):
        ping = Topic("demo/registry/ping", Msg)
        svc = Service("demo/registry/svc", request=Msg, reply=Msg)

    assert find_topic("demo/registry/ping") is DemoSet.ping
    assert find_topic("ns/demo/registry/ping", "ns") is DemoSet.ping
    assert find_topic("demo/registry/missing") is None


def test_the_registry_can_be_filtered_by_owner(isolated_registry):
    """The registry is process-global; a contract test wants only its own."""

    class MineTopics(TopicSet):
        ping = Topic("mine/ping", Msg)
        svc = Service("mine/svc", request=Msg, reply=Msg)

    class TheirsTopics(TopicSet):
        ping = Topic("theirs/ping", Msg)

    owner = f"{__name__}.{MineTopics.__qualname__}"

    assert [e.attr for e in registered_entries(owner)] == ["ping", "svc"]
    assert [t.key for _, t in registered_topics(owner)] == ["mine/ping"]
    assert [s.key for _, s in registered_services(owner)] == ["mine/svc"]
    assert {t.key for _, t in registered_topics()} == {"mine/ping", TheirsTopics.ping.key}


# ---------------------------------------------------------------- trace_ratio


def test_trace_ratio_defaults_to_everything():
    assert Topic("t/a", Msg, trace=True).trace_ratio == 1.0


def test_trace_ratio_rejects_out_of_range():
    with pytest.raises(ContractError, match=r"between 0\.0 and 1\.0"):
        Topic("t/b", Msg, trace=True, trace_ratio=1.5)
    with pytest.raises(ContractError, match=r"between 0\.0 and 1\.0"):
        Topic("t/c", Msg, trace=True, trace_ratio=-0.1)


def test_trace_ratio_without_a_root_is_an_error():
    """It would silently do nothing: a non-root never starts a trace to sample."""
    with pytest.raises(ContractError, match="no effect without trace=True"):
        Topic("t/d", Msg, trace_ratio=0.1)
