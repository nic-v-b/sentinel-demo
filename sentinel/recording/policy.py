"""Finite recording budgets, independent of adapters and storage."""

from dataclasses import dataclass
import math


def integer(value: object, maximum: int, *, minimum: int = 1) -> None:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError("invalid-recording-limit")


def seconds(value: object) -> None:
    if (type(value) not in (int, float) or not 0 < value <= 86400
            or not math.isfinite(value)):
        raise ValueError("invalid-recording-duration")


@dataclass(frozen=True)
class CapturePolicy:
    duration: float = 10
    request_timeout: float = 15
    max_bytes: int = 4 * 1024 * 1024
    retries: int = 0
    concurrency: int = 1
    max_seen: int = 4096

    def __post_init__(self) -> None:
        seconds(self.duration)
        seconds(self.request_timeout)
        integer(self.max_bytes, 1024 ** 3)
        integer(self.retries, 10, minimum=0)
        integer(self.concurrency, 32)
        integer(self.max_seen, 100_000)


@dataclass(frozen=True)
class StoragePolicy:
    max_bytes: int = 64 * 1024 * 1024
    max_clips: int = 128
    retention_seconds: float = 86400
    max_entries: int = 256

    def __post_init__(self) -> None:
        integer(self.max_bytes, 1024 ** 4)
        integer(self.max_clips, 100_000)
        seconds(self.retention_seconds)
        integer(self.max_entries, 200_000)
        if self.max_entries < self.max_clips + 2:
            raise ValueError("invalid-recording-limit")
