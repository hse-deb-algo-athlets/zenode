"""Layered configuration: defaults < TOML file < environment variables.

Three concerns, loaded independently so a broken section for one node can
never crash another:

- ``TransportConfig`` — the ``[transport]`` section: how to reach the zenoh
  network, plus the deployment namespace.
- per-node config — a node declares its own ``NodeConfig`` subclass; only
  the ``[node.<name>]`` section is validated for it.
- deployment-wide facts that belong to no single node (chassis geometry,
  frame ids, calibration) — :func:`load_section` reads any other section with
  the same precedence rules, so nodes that must agree on a value read it from
  one place.

Environment overrides use ``ZENODE_TRANSPORT__<FIELD>`` and
``ZENODE_<NODE>__<FIELD>`` (``__`` descends into nested models). Values are
parsed as JSON when possible, else taken as strings; comma-separated strings
are accepted for list fields.

The config file is found via (in order): an explicit path, ``$ZENODE_CONFIG``,
or ``./zenode.toml``. A missing file is not an error — defaults apply.
"""

from __future__ import annotations

import json
import os
import tomllib
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Literal, TypeVar

import zenoh
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from .errors import ConfigError

ENV_CONFIG_PATH = "ZENODE_CONFIG"
DEFAULT_CONFIG_FILE = "zenode.toml"


class NodeConfig(BaseModel):
    """Base class for per-node settings models.

    TOML keys may be written in kebab-case (``max-speed``) or snake_case
    (``max_speed``); the loader normalizes them. Unknown keys are rejected so
    typos fail loudly.
    """

    model_config = ConfigDict(extra="forbid")


C = TypeVar("C", bound=NodeConfig)
M = TypeVar("M", bound=BaseModel)


class TransportConfig(NodeConfig):
    """How a node reaches the zenoh network, and where the contract lives."""

    mode: Literal["peer", "client"] = "peer"
    connect: list[str] = Field(default_factory=list)
    listen: list[str] = Field(default_factory=list)
    namespace: str = ""
    multicast_scouting: bool = True
    shared_memory: bool = False
    """Enable zenoh's shared-memory transport. Needed at **both** ends before a
    ``Topic(shm=True)`` actually avoids a copy; without it such a topic still
    publishes correctly, just through the normal path."""
    timestamping: bool = True
    """HLC timestamps on published samples. Required for latched topics
    (zenoh-ext's cache needs it); routers have it on by default, plain
    peer/client sessions do not — so zenode enables it."""
    overrides: dict[str, Any] = Field(default_factory=dict)
    """Escape hatch: raw zenoh config entries, inserted as JSON5 by path
    (e.g. ``{"transport/link/tx/queue/congestion_control/wait_before_drop": 1000}``)."""

    def to_zenoh_config(self) -> zenoh.Config:
        cfg = zenoh.Config()
        cfg.insert_json5("mode", json.dumps(self.mode))
        if self.connect:
            cfg.insert_json5("connect/endpoints", json.dumps(self.connect))
        if self.listen:
            cfg.insert_json5("listen/endpoints", json.dumps(self.listen))
        cfg.insert_json5("scouting/multicast/enabled", json.dumps(self.multicast_scouting))
        cfg.insert_json5("timestamping/enabled", json.dumps(self.timestamping))
        cfg.insert_json5("transport/shared_memory/enabled", json.dumps(self.shared_memory))
        for path, value in self.overrides.items():
            cfg.insert_json5(path, json.dumps(value))
        return cfg


def find_config_file(
    path: str | Path | None = None, env: Mapping[str, str] | None = None
) -> Path | None:
    """Resolve the config file path; ``None`` if none exists."""
    environ = os.environ if env is None else env
    if path is not None:
        p = Path(path)
        if not p.is_file():
            raise ConfigError(f"config file not found: {p}")
        return p
    env_path = environ.get(ENV_CONFIG_PATH)
    if env_path:
        p = Path(env_path)
        if not p.is_file():
            raise ConfigError(f"${ENV_CONFIG_PATH} points to a missing file: {p}")
        return p
    default = Path(DEFAULT_CONFIG_FILE)
    return default if default.is_file() else None


def _normalize_keys(data: Any) -> Any:
    """Recursively turn kebab-case dict keys into snake_case."""
    if isinstance(data, dict):
        return {
            (k.replace("-", "_") if isinstance(k, str) else k): _normalize_keys(v)
            for k, v in data.items()
        }
    if isinstance(data, list):
        return [_normalize_keys(v) for v in data]
    return data


def _read_toml(path: Path) -> dict[str, Any]:
    try:
        with path.open("rb") as f:
            return tomllib.load(f)
    except tomllib.TOMLDecodeError as e:
        raise ConfigError(f"invalid TOML in {path}: {e}") from e


def _parse_env_value(raw: str) -> Any:
    try:
        return json.loads(raw)
    except ValueError:
        if "," in raw:
            return [part.strip() for part in raw.split(",") if part.strip()]
        return raw


def _env_prefix(section: str) -> str:
    return f"ZENODE_{section.upper().replace('-', '_')}__"


def _env_overrides(section: str, environ: Mapping[str, str]) -> dict[str, Any]:
    prefix = _env_prefix(section)
    result: dict[str, Any] = {}
    for key, raw in environ.items():
        if not key.startswith(prefix):
            continue
        path = [part.lower() for part in key.removeprefix(prefix).split("__") if part]
        if not path:
            continue
        target = result
        for part in path[:-1]:
            target = target.setdefault(part, {})
            if not isinstance(target, dict):
                raise ConfigError(f"conflicting environment overrides at {key}")
        target[path[-1]] = _parse_env_value(raw)
    return result


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _validate_section(
    model: type[M],
    raw_section: dict[str, Any],
    env_section: str,
    environ: Mapping[str, str],
    *,
    what: str,
) -> M:
    data = _deep_merge(_normalize_keys(raw_section), _env_overrides(env_section, environ))
    try:
        return model.model_validate(data)
    except ValidationError as e:
        raise ConfigError(f"invalid {what} configuration: {e}") from e


def _section_data(file: Path | None, section: str) -> dict[str, Any]:
    """The raw table at a dotted ``section`` path; ``{}`` if absent."""
    parts = section.split(".")
    if not section or any(not part for part in parts):
        raise ConfigError(f"section {section!r} must be a non-empty dotted path")
    if file is None:
        return {}
    data: Any = _read_toml(file)
    for depth, part in enumerate(parts):
        data = data.get(part, {})
        if isinstance(data, dict):
            continue
        where = ".".join(parts[: depth + 1])
        if depth < len(parts) - 1:
            raise ConfigError(f"[{where}] must be a table of tables")
        raise ConfigError(f"[{where}] must be a table")
    return data


def load_section(
    model: type[M],
    section: str,
    path: str | Path | None = None,
    env: Mapping[str, str] | None = None,
    *,
    env_section: str | None = None,
) -> M:
    """Load any config section — dotted paths descend into subtables.

    The general form behind :func:`load_transport_config` and
    :func:`load_node_config`, for values that belong to the deployment rather
    than to one node::

        [geometry]
        wheel-radius = 0.05

        geometry = load_section(Geometry, "geometry")

    Environment overrides use the last path segment
    (``ZENODE_GEOMETRY__WHEEL_RADIUS``) unless ``env_section`` says otherwise.
    A missing section is not an error — the model's defaults apply.
    """
    environ = os.environ if env is None else env
    file = find_config_file(path, environ)
    raw = _section_data(file, section)
    name = section.rsplit(".", 1)[-1] if env_section is None else env_section
    return _validate_section(model, raw, name, environ, what=f"[{section}]")


def load_transport_config(
    path: str | Path | None = None,
    env: Mapping[str, str] | None = None,
) -> TransportConfig:
    """Load ``[transport]`` + ``ZENODE_TRANSPORT__*`` overrides."""
    return load_section(TransportConfig, "transport", path, env)


def load_node_config(
    model: type[C],
    node_name: str,
    path: str | Path | None = None,
    env: Mapping[str, str] | None = None,
) -> C:
    """Load ``[node.<name>]`` + ``ZENODE_<NAME>__*`` overrides.

    Only this node's section is validated — other sections of the file may be
    arbitrarily broken without affecting this node.
    """
    return load_section(model, f"node.{node_name}", path, env, env_section=node_name)
