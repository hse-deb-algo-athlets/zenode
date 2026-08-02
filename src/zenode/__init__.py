"""zenode — typed node framework for distributed robot systems on Eclipse Zenoh.

The pieces:

- :class:`Topic` / :class:`Service` / :class:`TopicSet` — the typed contract.
- :class:`Node` + :func:`run` — the node runtime and process entry point.
- :class:`TransportConfig` / :class:`NodeConfig` — layered configuration.
- ``zenode.testing`` — in-process test harness (no router needed).
- ``zenode.msgs`` — minimal standard messages (geometry, health).
- ``zenode.trace`` — W3C trace context following the data across nodes.
- ``zenode.otel`` — optional OpenTelemetry spans (``pip install 'zenode[otel]'``).
- ``zenode`` CLI — topics / echo / hz / health / logs / trace / export /
  nodes / doctor.
"""

from importlib.metadata import version as _version

from . import otel, trace
from .codec import Codec, PydanticJsonCodec, RawCodec, default_codec
from .config import (
    NodeConfig,
    TransportConfig,
    find_config_file,
    load_node_config,
    load_section,
    load_transport_config,
)
from .declarative import Binding, every, on_resume, on_silence, publish, serve, subscribe
from .envelope import Envelope
from .errors import (
    ConfigError,
    ContractError,
    DuplicateNodeError,
    ServiceError,
    ServiceTimeout,
    ZenodeError,
)
from .log import setup_logging
from .node import Node, run
from .presence import PresenceWatcher, list_nodes, list_nodes_async, presence_key
from .pubsub import OnDeadline, Publisher, Subscription
from .service import ServiceServer
from .timers import Timer
from .topic import (
    Service,
    Topic,
    TopicSet,
    find_topic,
    registered_entries,
    registered_services,
    registered_topics,
    resolve_key,
)

# Single source of truth: the version declared in pyproject.toml, read back from
# the installed distribution metadata.
__version__ = _version("zenode")

__all__ = [
    "Binding",
    "Codec",
    "ConfigError",
    "ContractError",
    "DuplicateNodeError",
    "Envelope",
    "Node",
    "NodeConfig",
    "OnDeadline",
    "PresenceWatcher",
    "Publisher",
    "PydanticJsonCodec",
    "RawCodec",
    "Service",
    "ServiceError",
    "ServiceServer",
    "ServiceTimeout",
    "Subscription",
    "Timer",
    "Topic",
    "TopicSet",
    "TransportConfig",
    "ZenodeError",
    "__version__",
    "default_codec",
    "every",
    "find_config_file",
    "find_topic",
    "list_nodes",
    "list_nodes_async",
    "load_node_config",
    "load_section",
    "load_transport_config",
    "on_resume",
    "on_silence",
    "otel",
    "presence_key",
    "publish",
    "registered_entries",
    "registered_services",
    "registered_topics",
    "resolve_key",
    "run",
    "serve",
    "setup_logging",
    "subscribe",
    "trace",
]
