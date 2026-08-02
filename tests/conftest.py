"""Fixtures shared across the suite.

Anything that more than one test module needs to set up — the in-process
harness, the global topic registry, CLI argument namespaces, a config file on
disk — lives here so the modules themselves stay about behavior.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import pytest

from zenode import topic as topic_module
from zenode.testing import Harness, harness

# ------------------------------------------------------------------ transport


@pytest.fixture
async def zen() -> AsyncIterator[Harness]:
    """An in-process harness with the default (empty) namespace."""
    async with harness() as h:
        yield h


# ------------------------------------------------------------------- registry


@pytest.fixture
def isolated_registry() -> Iterator[list[topic_module.RegisteredEntry]]:
    """Empty the process-wide TopicSet registry, and restore it afterwards.

    ``TopicSet`` registers on subclass creation, so every test module that
    declares one mutates global state at import time. Tests that assert on the
    registry's *contents* (the CLI's ``topics``/``echo``) need it to hold only
    what they put there.
    """
    saved = list(topic_module._REGISTRY)
    topic_module._REGISTRY.clear()
    yield topic_module._REGISTRY
    topic_module._REGISTRY[:] = saved


@pytest.fixture
def restore_sys_path() -> Iterator[None]:
    """Undo `sys.path` edits — `_load_contracts` prepends the cwd."""
    saved = sys.path[:]
    yield
    sys.path[:] = saved


# --------------------------------------------------------------------- config


@pytest.fixture
def no_ambient_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Neutralize config discovery: no ``$ZENODE_CONFIG``, no ``./zenode.toml``.

    The loaders fall back to the environment and the working directory, so a
    developer's own ``zenode.toml`` would otherwise leak into assertions.
    """
    monkeypatch.delenv("ZENODE_CONFIG", raising=False)
    monkeypatch.chdir(tmp_path)


@pytest.fixture
def config_file(tmp_path: Path) -> Path:
    """A config file exercising both sections and both key spellings."""
    path = tmp_path / "zenode.toml"
    path.write_text(
        """
[transport]
mode = "client"
connect = ["tcp/10.0.0.1:7447"]
namespace = "robodog"

[node.nav]
max-speed = 1.5

[node.joy]
this-key-does-not-exist-anywhere = true
"""
    )
    return path


# ------------------------------------------------------------------------ cli


@pytest.fixture
def cli_args() -> Any:
    """Build an ``argparse.Namespace`` like ``main()`` would, minus the parser.

    Command functions take a namespace; constructing one directly keeps their
    tests free of argparse wiring, which is tested separately.
    """

    def make(**overrides: Any) -> argparse.Namespace:
        base: dict[str, Any] = {
            "config": None,
            "connect": [],
            "mode": None,
            "namespace": None,
            "contract": [],
        }
        return argparse.Namespace(**{**base, **overrides})

    return make
