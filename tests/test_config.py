"""Layered configuration: defaults < TOML file < environment.

The ``config_file`` fixture lives in conftest.py — the CLI tests load it too.
"""

import pytest
from pydantic import Field

from zenode import NodeConfig, TransportConfig
from zenode.config import (
    ENV_CONFIG_PATH,
    _parse_env_value,
    find_config_file,
    load_node_config,
    load_section,
    load_transport_config,
)
from zenode.errors import ConfigError


class NavConfig(NodeConfig):
    max_speed: float = 0.5
    hold_reasons: list[str] = Field(default_factory=list)


def test_defaults_without_file(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    transport = load_transport_config(env={})
    assert transport == TransportConfig()


def test_transport_from_file(config_file):
    transport = load_transport_config(config_file, env={})
    assert transport.mode == "client"
    assert transport.connect == ["tcp/10.0.0.1:7447"]
    assert transport.namespace == "robodog"


def test_node_section_kebab_normalized(config_file):
    cfg = load_node_config(NavConfig, "nav", config_file, env={})
    assert cfg.max_speed == 1.5


def test_env_overrides_file(config_file):
    cfg = load_node_config(NavConfig, "nav", config_file, env={"ZENODE_NAV__MAX_SPEED": "2.25"})
    assert cfg.max_speed == 2.25


def test_env_list_parsing(config_file):
    transport = load_transport_config(
        config_file,
        env={"ZENODE_TRANSPORT__CONNECT": "tcp/a:7447, tcp/b:7447"},
    )
    assert transport.connect == ["tcp/a:7447", "tcp/b:7447"]


def test_broken_other_section_does_not_affect_this_node(config_file):
    # [node.joy] contains garbage; nav must load fine regardless.
    cfg = load_node_config(NavConfig, "nav", config_file, env={})
    assert cfg.max_speed == 1.5


def test_unknown_key_rejected_for_own_section(config_file):
    class JoyConfig(NodeConfig):
        deadzone: float = 0.1

    with pytest.raises(ConfigError):
        load_node_config(JoyConfig, "joy", config_file, env={})


def test_missing_explicit_file_fails():
    with pytest.raises(ConfigError):
        load_transport_config("/nonexistent/zenode.toml", env={})


def test_zenoh_config_builds(config_file):
    transport = load_transport_config(config_file, env={})
    cfg = transport.to_zenoh_config()
    assert '"tcp/10.0.0.1:7447"' in cfg.get_json("connect/endpoints")


# ------------------------------------------------------------------ discovery


@pytest.mark.usefixtures("no_ambient_config")
def test_no_file_anywhere_is_not_an_error():
    assert find_config_file() is None


def test_default_file_found_in_cwd(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "zenode.toml").write_text("[transport]\nmode = 'client'\n")
    found = find_config_file(env={})
    assert found is not None and found.name == "zenode.toml"


def test_env_var_points_at_the_file(config_file):
    assert find_config_file(env={ENV_CONFIG_PATH: str(config_file)}) == config_file


def test_env_var_pointing_at_a_missing_file_fails():
    """A typo in $ZENODE_CONFIG must be loud, not silently fall back."""
    with pytest.raises(ConfigError, match=ENV_CONFIG_PATH):
        find_config_file(env={ENV_CONFIG_PATH: "/nonexistent/zenode.toml"})


def test_explicit_path_beats_the_env_var(config_file, tmp_path):
    other = tmp_path / "other.toml"
    other.write_text("[transport]\n")
    assert find_config_file(other, env={ENV_CONFIG_PATH: str(config_file)}) == other


# ----------------------------------------------------------- shared sections


class Geometry(NodeConfig):
    """A deployment fact two nodes must agree on, owned by neither."""

    wheel_radius: float = 0.05
    base_radius: float = 0.16


@pytest.fixture
def geometry_file(tmp_path):
    path = tmp_path / "zenode.toml"
    path.write_text(
        """
[geometry]
wheel-radius = 0.0485
base-radius = 0.1625

[node.motor]
max-speed = 1.0
"""
    )
    return path


def test_a_shared_section_loads_like_a_node_section(geometry_file):
    geometry = load_section(Geometry, "geometry", geometry_file, env={})
    assert geometry.wheel_radius == 0.0485  # kebab-case normalized, same as [node.*]
    assert geometry.base_radius == 0.1625


def test_a_shared_section_takes_env_overrides(geometry_file):
    geometry = load_section(
        Geometry, "geometry", geometry_file, env={"ZENODE_GEOMETRY__WHEEL_RADIUS": "0.06"}
    )
    assert geometry.wheel_radius == 0.06


def test_a_missing_shared_section_falls_back_to_defaults(config_file):
    assert load_section(Geometry, "geometry", config_file, env={}) == Geometry()


def test_a_dotted_section_descends(geometry_file):
    """``load_node_config`` is this, with the ``node.`` prefix applied for you."""

    class MotorConfig(NodeConfig):
        max_speed: float = 0.0

    assert load_section(MotorConfig, "node.motor", geometry_file, env={}).max_speed == 1.0


def test_a_shared_section_that_is_not_a_table_says_which(tmp_path):
    path = tmp_path / "zenode.toml"
    path.write_text("geometry = 3\n")
    with pytest.raises(ConfigError, match=r"\[geometry\] must be a table"):
        load_section(Geometry, "geometry", path, env={})


@pytest.mark.parametrize("section", ["", "node.", ".node"])
def test_an_empty_section_path_is_rejected(section, config_file):
    with pytest.raises(ConfigError, match="dotted path"):
        load_section(Geometry, section, config_file, env={})


# ---------------------------------------------------------------- malformed


def test_invalid_toml_names_the_file(tmp_path):
    path = tmp_path / "zenode.toml"
    path.write_text("[transport\nmode = 'peer'")
    with pytest.raises(ConfigError, match="invalid TOML"):
        load_transport_config(path, env={})


def test_transport_must_be_a_table(tmp_path):
    path = tmp_path / "zenode.toml"
    path.write_text("transport = 3\n")
    with pytest.raises(ConfigError, match=r"\[transport\] must be a table"):
        load_transport_config(path, env={})


def test_node_must_be_a_table_of_tables(tmp_path):
    path = tmp_path / "zenode.toml"
    path.write_text("node = 'nope'\n")
    with pytest.raises(ConfigError, match=r"\[node\] must be a table of tables"):
        load_node_config(NavConfig, "nav", path, env={})


def test_node_section_must_be_a_table(tmp_path):
    path = tmp_path / "zenode.toml"
    path.write_text("[node]\nnav = 7\n")
    with pytest.raises(ConfigError, match=r"\[node\.nav\] must be a table"):
        load_node_config(NavConfig, "nav", path, env={})


def test_wrong_type_in_file_is_a_config_error(tmp_path):
    path = tmp_path / "zenode.toml"
    path.write_text("[node.nav]\nmax-speed = 'fast'\n")
    with pytest.raises(ConfigError, match=r"invalid \[node\.nav\] configuration"):
        load_node_config(NavConfig, "nav", path, env={})


# ------------------------------------------------------------- env overrides


@pytest.mark.parametrize(
    "raw,expected",
    [
        pytest.param("2.5", 2.5, id="json-number"),
        pytest.param("true", True, id="json-bool"),
        pytest.param('["a","b"]', ["a", "b"], id="json-list"),
        pytest.param("a, b ,", ["a", "b"], id="comma-separated"),
        pytest.param("robodog", "robodog", id="bare-string"),
        pytest.param("", "", id="empty"),
    ],
)
def test_env_value_parsing(raw, expected):
    assert _parse_env_value(raw) == expected


@pytest.mark.usefixtures("no_ambient_config")
def test_env_descends_into_nested_models():
    transport = load_transport_config(env={"ZENODE_TRANSPORT__OVERRIDES__SCOUTING__DELAY": "500"})
    assert transport.overrides == {"scouting": {"delay": 500}}


def test_env_merges_with_file_rather_than_replacing(config_file):
    transport = load_transport_config(config_file, env={"ZENODE_TRANSPORT__MODE": "peer"})
    assert transport.mode == "peer"
    assert transport.namespace == "robodog"  # untouched keys survive


@pytest.mark.usefixtures("no_ambient_config")
def test_conflicting_env_overrides_are_rejected():
    """``X=1`` and ``X__Y=2`` cannot both be true; say so instead of guessing."""
    with pytest.raises(ConfigError, match="conflicting"):
        load_transport_config(
            env={
                "ZENODE_TRANSPORT__OVERRIDES": "1",
                "ZENODE_TRANSPORT__OVERRIDES__NESTED": "2",
            }
        )


def test_env_prefix_is_scoped_to_its_section(config_file):
    """``ZENODE_NAV__*`` must not bleed into ``[transport]``."""
    transport = load_transport_config(config_file, env={"ZENODE_NAV__NAMESPACE": "wrong"})
    assert transport.namespace == "robodog"


@pytest.mark.usefixtures("no_ambient_config")
def test_dashed_node_names_map_to_underscored_env_vars():
    """``[node.front-nav]`` is overridden by ``ZENODE_FRONT_NAV__*``."""
    cfg = load_node_config(NavConfig, "front-nav", env={"ZENODE_FRONT_NAV__MAX_SPEED": "3.0"})
    assert cfg.max_speed == 3.0


# ------------------------------------------------------------- zenoh config


def test_zenoh_config_carries_listen_and_timestamping():
    transport = TransportConfig(listen=["tcp/0.0.0.0:7447"], timestamping=False)
    cfg = transport.to_zenoh_config()
    assert '"tcp/0.0.0.0:7447"' in cfg.get_json("listen/endpoints")
    assert cfg.get_json("timestamping/enabled") == "false"


def test_zenoh_config_applies_raw_overrides():
    """The escape hatch has to reach settings zenode does not model."""
    transport = TransportConfig(overrides={"scouting/multicast/interface": "eth0"})
    cfg = transport.to_zenoh_config()
    assert "eth0" in cfg.get_json("scouting/multicast/interface")
