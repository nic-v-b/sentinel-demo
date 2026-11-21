"""Strict adapter boundary, independent of any vendor's wire format.

Adapters resolve routing conflicts, classify supported motion and supply stable
IDs before entering this boundary. No raw payloads or media metadata belong here.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Protocol


class InvalidEvent(ValueError):
    """Safe fixed-category rejection; never includes input values."""


def identifier(value: object) -> str:
    """Require a bounded opaque string; never coerce or trim routing values."""
    if (type(value) is not str or not 1 <= len(value) <= 128
            or any(char.isspace() or not char.isprintable() for char in value)):
        raise InvalidEvent("invalid-identifier")
    return value


def utc_time(value: object) -> datetime:
    """Normalized timestamps must already be aware datetimes, never guesses."""
    if type(value) is not datetime or value.tzinfo is None:
        raise InvalidEvent("invalid-timestamp")
    try:
        if value.utcoffset() is None:
            raise InvalidEvent("invalid-timestamp")
        return value.astimezone(timezone.utc)
    except Exception:
        # tzinfo methods are callbacks supplied with the timestamp. Any ordinary
        # callback failure rejects this row without exposing its exception text.
        raise InvalidEvent("invalid-timestamp") from None


@dataclass(frozen=True)
class MotionEvent:
    camera_id: str = field(repr=False)
    event_id: str = field(repr=False)
    kind: str
    reported_at: datetime | None = field(repr=False)

    def __post_init__(self) -> None:
        identifier(self.camera_id)
        identifier(self.event_id)
        if type(self.kind) is not str or self.kind != "motion":
            raise InvalidEvent("unsupported-kind")
        if self.reported_at is not None:
            object.__setattr__(self, "reported_at", utc_time(self.reported_at))

    @classmethod
    def from_row(cls, row: object, expected_camera_id: str) -> MotionEvent:
        # No arbitrary mappings, implicit wrappers, aliases or nested metadata.
        if (type(row) is not dict
                or row.keys() != {"camera_id", "event_id", "kind", "reported_at"}):
            raise InvalidEvent("invalid-row-schema")
        event = cls(**row)
        if event.camera_id != expected_camera_id:
            raise InvalidEvent("camera-mismatch")
        return event


@dataclass(frozen=True)
class EventBatch:
    """One adapter-validated polling window, not a raw notification container.

    `complete` is explicit: the adapter must not mark partial/truncated history
    or an unsupported wire response complete. An empty complete window is valid.
    Rows are checked by MotionObserver, including runtime container/size checks.
    """

    rows: tuple[object, ...] = field(repr=False)
    complete: bool


class EventSource(Protocol):
    """Caller-owned asynchronous adapter. Both methods must honor cancellation.

    poll() must bound response bytes and parsing work before constructing rows.
    reconnect() repairs adapter transport/auth state; it must not wake a camera.
    The caller owns adapter setup/cleanup. This package never constructs one.
    """

    async def poll(self) -> EventBatch: ...

    async def reconnect(self) -> None: ...


@dataclass(frozen=True)
class ObservedMotion:
    event: MotionEvent = field(repr=False)
    received_at: datetime = field(repr=False)
    history_relation: str = field(default="unseen-after-baseline", init=False)
    freshness: str = field(default="unverified", init=False)
    physical_motion_at: None = field(default=None, init=False)
    latency_seconds: None = field(default=None, init=False)
