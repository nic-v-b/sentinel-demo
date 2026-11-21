"""Synthetic-first recording foundation. No provider, FFmpeg or network I/O."""

from .policy import CapturePolicy, StoragePolicy
from .recorder import (
    CaptureBackend, CaptureRequest, CaptureResult, CaptureSink, ClipStore,
    ClipWrite, Recorder, RecordingError,
)
from .storage import PrivateStore

__all__ = [
    "CaptureBackend", "CapturePolicy", "CaptureRequest", "CaptureResult",
    "CaptureSink", "ClipStore", "ClipWrite", "PrivateStore", "Recorder",
    "RecordingError", "StoragePolicy",
]
