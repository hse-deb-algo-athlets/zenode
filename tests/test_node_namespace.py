"""The namespace a subclass shares with the runtime.

``Node`` is the library's extension point, so every name it occupies is a name a
user cannot have. Two mechanisms keep that surface from biting: everything
private is mangled to ``_Node__*`` so it cannot collide at all, and the public
half is refused at import by ``__init_subclass__``. What is tested here is that
both hold, and — the reason this file exists — that the failure they replace was
silent: an instance attribute wins over a subclass method with no error at all.
"""

from __future__ import annotations

import pytest
from conftest import internals

from zenode import Node
from zenode.errors import ContractError
from zenode.metrics import ProcessStats
from zenode.node import _INSTANCE_API, _OVERRIDE_POINTS


def test_private_names_are_mangled_out_of_the_subclass_namespace() -> None:
    """The regression: ``_process`` used to be ``ProcessStats`` on every instance.

    A subclass method of that name was replaced during ``__init__`` and calling
    it raised ``TypeError`` on first use, far from the cause.
    """

    class Detector(Node):
        name = "ns-detector"

        def _process(self, frame: str) -> str:
            return f"processed {frame}"

        def _state(self) -> str:
            return "mine"

    node = Detector()
    assert node._process("frame") == "processed frame"
    assert node._state() == "mine"
    # …and the runtime still has its own, under the mangled name.
    assert isinstance(internals(node).process, ProcessStats)
    assert internals(node).state == node.state  # the public property still reads it


def test_no_unmangled_private_names_survive_on_an_instance() -> None:
    """Anything ``_x`` on a constructed node is a name a subclass cannot use."""

    class Bare(Node):
        name = "ns-bare"

    assert [k for k in vars(Bare()) if k.startswith("_") and not k.startswith("_Node__")] == []


def test_instance_api_list_matches_what_init_actually_sets() -> None:
    """``_INSTANCE_API`` is hand-maintained; ``dir(Node)`` cannot derive it."""

    class Bare(Node):
        name = "ns-api"

    assert {k for k in vars(Bare()) if not k.startswith("_")} == set(_INSTANCE_API)


@pytest.mark.parametrize("attr", ["state", "key", "subscribe", "every", "serve", "spawn"])
def test_redefining_public_api_is_refused(attr: str) -> None:
    with pytest.raises(ContractError, match=attr):
        type(f"Clash{attr}", (Node,), {"name": "ns-clash", attr: lambda self: None})


@pytest.mark.parametrize("attr", sorted(_INSTANCE_API))
def test_redefining_a_public_instance_attribute_is_refused(attr: str) -> None:
    """``log`` and ``namespace`` are the invisible half — not on ``dir(Node)``."""
    with pytest.raises(ContractError, match=attr):
        type(f"Clash{attr}", (Node,), {"name": "ns-clash", attr: lambda self: None})


def test_the_error_names_every_clash_and_the_way_out() -> None:
    with pytest.raises(ContractError) as excinfo:
        type("Multi", (Node,), {"name": "ns-multi", "state": 1, "key": 2})
    message = str(excinfo.value)
    assert "key, state" in message
    assert "on_start" in message  # points at what *is* redefinable


def test_override_points_are_allowed() -> None:
    """The documented extension surface must survive its own guard."""

    class Configured(Node):
        name = "ns-configured"
        health_interval = None
        allow_duplicates = False
        shm_pool_bytes = 1024
        trace_ring = 0
        start_timeout = 1.0
        shutdown_timeout = 1.0
        publish_logs_at = None

        async def on_start(self) -> None: ...
        async def on_stop(self) -> None: ...

    assert Configured.name == "ns-configured"
    assert set(vars(Configured)) & set(_OVERRIDE_POINTS)


def test_handler_names_are_unaffected() -> None:
    """The guard must not get in the way of ordinary node code."""

    class Ordinary(Node):
        name = "ns-ordinary"

        async def on_frame(self, msg: str) -> None: ...
        def _decode(self, msg: str) -> str:
            return msg

        def _is_blurred(self, value: float) -> bool:
            return value < 1.0

    assert Ordinary()


def test_deeper_subclasses_are_checked_too() -> None:
    class Base(Node):
        name = "ns-base"

    with pytest.raises(ContractError, match="session"):
        type("Derived", (Base,), {"session": None})
