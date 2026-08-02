"""Small standard message set.

Deliberately minimal: geometry primitives and the node-health message. Domain
messages (robot commands, navigation types, …) belong to the application's
own contract package, where they can evolve with the robot.
"""

from .geometry import Pose, Pose2D, Quaternion, Transform, Twist, Vector3
from .health import NodeHealth, NodeState, health_key, health_pattern
from .log import LogRecordMsg, log_key, log_pattern

__all__ = [
    "LogRecordMsg",
    "NodeHealth",
    "NodeState",
    "Pose",
    "Pose2D",
    "Quaternion",
    "Transform",
    "Twist",
    "Vector3",
    "health_key",
    "health_pattern",
    "log_key",
    "log_pattern",
]
