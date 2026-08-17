"""Exception hierarchy for zenode."""

from __future__ import annotations


class ZenodeError(Exception):
    """Base class for all zenode errors."""


class ConfigError(ZenodeError):
    """Configuration could not be loaded or validated."""


class ContractError(ZenodeError):
    """A Topic/Service declaration is invalid."""


class DuplicateNodeError(ZenodeError):
    """A node with the same name is already live in the namespace."""


class StartTimeout(ZenodeError):
    """``on_start`` outran ``Node.start_timeout``."""


class ServiceError(ZenodeError):
    """A service call failed on the remote side."""


class ServiceTimeout(ServiceError):
    """A service call received no reply in time."""
