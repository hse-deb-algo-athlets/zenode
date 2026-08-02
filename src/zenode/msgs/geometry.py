"""Geometry primitives (right-handed, SI units, quaternions in xyzw order)."""

from __future__ import annotations

import math

from pydantic import BaseModel


class Vector3(BaseModel):
    """Three components in a right-handed frame — FLU on a body, ENU in the world.

    The quantity is the field's, not this type's: metres for a position,
    m/s for a linear velocity, rad/s for an angular one.
    """

    x: float = 0.0
    y: float = 0.0
    z: float = 0.0


class Quaternion(BaseModel):
    """A rotation, **xyzw** order and unit length, defaulting to identity.

    The ``w``-last order matches ROS and Eigen; much of the aerospace and
    graphics literature puts ``w`` first. A mismatch produces a rotation that
    looks *almost* right, so never accept one whose order you have not checked.
    Producers normalize; consumers may assume normalized.
    """

    x: float = 0.0
    y: float = 0.0
    z: float = 0.0
    w: float = 1.0


class Pose(BaseModel):
    """Position in metres plus orientation, in whichever frame the sender means.

    Carries no ``frame_id``: use :class:`Transform` when the payload must state
    what it is relative to.
    """

    position: Vector3 = Vector3()
    orientation: Quaternion = Quaternion()


class Pose2D(BaseModel):
    """Planar pose; ``theta`` in radians."""

    x: float = 0.0
    y: float = 0.0
    theta: float = 0.0

    def distance_to(self, other: Pose2D) -> float:
        """Planar distance to ``other``, in metres; ignores ``theta``."""
        return math.hypot(other.x - self.x, other.y - self.y)


class Twist(BaseModel):
    """A velocity: ``linear`` in m/s, ``angular`` in rad/s, in the body frame.

    So on a ground robot ``linear.x`` drives forward and ``angular.z`` is yaw
    rate, counter-clockwise-positive.
    """

    linear: Vector3 = Vector3()
    angular: Vector3 = Vector3()


class Transform(BaseModel):
    """The pose of ``child_frame_id`` expressed in ``frame_id``.

    The reference shape for a spatial message (see ``docs/conventions.md`` §4):
    it names its frames and leaves time to the :class:`~zenode.Envelope`.
    zenode ships no transform tree, so composing these is the application's job.
    """

    translation: Vector3 = Vector3()
    rotation: Quaternion = Quaternion()
    frame_id: str = ""
    child_frame_id: str = ""
