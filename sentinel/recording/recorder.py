"""Direct, bounded observation admission; no workers, queue or live adapter."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Protocol

from sentinel.events import MotionEvent, ObservedMotion
from sentinel.events.schema import identifier, utc_time
from .policy import CapturePolicy


async def _bounded(operation, timeout: float):
    """Own and drain one child; timeout stays failure if cancellation is swallowed.

    Await a separate completion signal: its cancellation belongs to the caller,
    never to the child. This works without Task.cancelling() (Python 3.10).
    """
    loop = asyncio.get_running_loop()
    completion = loop.create_future()

    def signal(finished):
        if not completion.done():
            completion.set_result(finished)

    async def run():
        # A queued commit must not start after its caller has cancelled or its
        # budget expired, even if the child runs before the caller resumes.
        if not completion.done():
            return await operation

    child = asyncio.create_task(run())
    child.add_done_callback(lambda _: signal(True))
    timer = loop.call_later(timeout, signal, False)
    try:
        if not await completion:
            raise asyncio.TimeoutError
        return child.result()
    finally:
        timer.cancel()
        cancelled_during_cleanup = False
        try:
            if not child.done():
                child.cancel()
            while not child.done():
                try:
                    # wait() observes completion without raising the child's
                    # exception. Every cancellation here is the caller's,
                    # including one racing the child's cancellation completion.
                    await asyncio.wait((child,))
                except asyncio.CancelledError:
                    cancelled_during_cleanup = True
        finally:
            # Also close an operation whose wrapper was cancelled before start.
            operation.close()
            if child.done() and not child.cancelled():
                # Cancellation can race an already-failed child. Always
                # retrieve its exception so asyncio cannot log private text.
                child.exception()
        if cancelled_during_cleanup:
            raise asyncio.CancelledError from None


class RecordingError(Exception):
    """Only fixed recorder categories belong in public errors."""


class ClipWrite(Protocol):
    def write(self, data: bytes) -> None: ...
    async def complete(self) -> None: ...
    def abort(self) -> None: ...


class ClipStore(Protocol):
    """Implementations reserve capacity before returning a transaction.

    complete() must leave either a complete clip or an abortable transaction.
    abort() is synchronous, bounded and idempotent; failure fences the recorder.
    No backend may retain transactions or create unsupervised tasks.
    """

    def claim(self) -> None: ...
    def begin(self, max_bytes: int) -> ClipWrite: ...


@dataclass(frozen=True)
class CaptureRequest:
    duration: float
    max_bytes: int


class CaptureBackend(Protocol):
    """Caller-supplied cooperative producer; this package supplies no live one.

    Return only after all bytes are written and owned resources/tasks closed.
    Honor cancellation, never retain the sink, never spawn detached work.
    The sink accepts synthetic bytes only in this development foundation.
    """

    async def capture(self, request: CaptureRequest, sink: CaptureSink) -> None: ...


class CaptureSink:
    def __init__(self, transaction: ClipWrite, maximum: int,
                 deadline: float, loop: asyncio.AbstractEventLoop) -> None:
        self._transaction = transaction
        self._maximum = maximum
        self._deadline = deadline
        self._loop = loop
        self._bytes = 0
        self._closed = False
        self._failed = False

    def write(self, data: bytes) -> None:
        if (self._closed or self._failed or type(data) is not bytes
                or self._loop.time() >= self._deadline
                or len(data) > self._maximum - self._bytes):
            self._failed = True
            raise RecordingError("capture-bound")
        try:
            self._transaction.write(data)
        except asyncio.CancelledError:
            self._failed = True
            cancelled = True
        except Exception:
            self._failed = True
            cancelled = False
        else:
            self._bytes += len(data)
            return
        # Leave the storage handler: from None alone retains private context.
        if cancelled:
            raise asyncio.CancelledError from None
        raise RecordingError("capture-write-failed") from None


@dataclass(frozen=True)
class CaptureResult:
    status: str
    attempts: int = 0
    bytes_written: int = 0


@dataclass(frozen=True)
class _Admission:
    """Only validated exact primitives cross into admission; never caller fields."""

    event_id: str = field(repr=False)


class Recorder:
    """One event loop, one instance per storage owner, at-most-once admission.

    Busy events are not remembered: callers decide whether to resubmit them.
    Accepted IDs remain remembered after failure/cancellation; no eviction.
    Restart loses this fence: rebaseline the observer and do not replay saved
    observations. These objects validate shape, not provenance or freshness.
    """

    def __init__(self, camera_id: str, backend: CaptureBackend, store: ClipStore,
                 policy: CapturePolicy = CapturePolicy()) -> None:
        self._camera_id = identifier(camera_id)
        if type(policy) is not CapturePolicy:
            raise ValueError("invalid-recording-policy")
        self._backend = backend
        self._store = store
        self._policy = policy
        self._seen: set[str] = set()
        self._active = 0
        self._fenced = False
        self._fence_status = "cleanup-failed"
        self._loop: asyncio.AbstractEventLoop | None = None
        cancelled = False
        unavailable = False
        try:
            store.claim()
        except asyncio.CancelledError:
            cancelled = True
        except Exception:
            unavailable = True
        if cancelled:
            # Leave the backend handler before raising: retain no private
            # message, cause, context or backend traceback.
            raise asyncio.CancelledError from None
        if unavailable:
            raise RecordingError("storage-owner-unavailable") from None

    def _valid(self, observation: object) -> bool:
        return self._validate_admission(observation) is not None

    def _validate_admission(self, observation: object) -> _Admission | None:
        if type(observation) is not ObservedMotion:
            return None
        try:
            event = observation.event
            if type(event) is not MotionEvent:
                return None
            history_relation = observation.history_relation
            freshness = observation.freshness
            # Equality on arbitrary metadata can return a stateful truth object
            # that escapes this validation boundary through a chained `and`.
            if type(history_relation) is not str or type(freshness) is not str:
                return None
            camera_id = event.camera_id
            event_id = event.event_id
            # Revalidate even frozen dataclasses: ObservedMotion has no own
            # runtime validation, and Python objects are not a trust sandbox.
            # Validate the same identity snapshots used for comparison and
            # admission: timestamp callbacks can replace the caller's fields.
            MotionEvent(camera_id, event_id, event.kind, event.reported_at)
            utc_time(observation.received_at)
            if (camera_id == self._camera_id
                    and history_relation == "unseen-after-baseline"
                    and freshness == "unverified"
                    and observation.physical_motion_at is None
                    and observation.latency_seconds is None):
                return _Admission(event_id)
            return None
        except asyncio.CancelledError:
            # Timestamp/timezone and validation callbacks run before admission.
            # Preserve cancellation without retaining their private exceptions.
            pass
        except Exception:
            return None
        # Outside the callback handler, so even __context__ contains no input.
        raise asyncio.CancelledError from None

    async def record(self, observation: object) -> CaptureResult:
        loop = asyncio.get_running_loop()
        if self._loop is not None and self._loop is not loop:
            raise RecordingError("recorder-loop-mismatch")
        self._loop = loop
        admission = self._validate_admission(observation)
        if admission is None:
            return CaptureResult("rejected")
        if self._fenced:
            return CaptureResult(self._fence_status)
        event_id = admission.event_id
        if event_id in self._seen:
            return CaptureResult("duplicate")
        if len(self._seen) >= self._policy.max_seen:
            return CaptureResult("capacity")
        if self._active >= self._policy.concurrency:
            return CaptureResult("busy")
        # No await between checks and reservation.
        self._seen.add(event_id)
        self._active += 1
        try:
            for attempt in range(1, self._policy.retries + 2):
                transaction = None
                sink = None
                completed = False
                finalizing = False
                cancelled = False
                try:
                    transaction = self._store.begin(self._policy.max_bytes)
                    budget = min(self._policy.duration, self._policy.request_timeout)
                    deadline = loop.time() + budget
                    sink = CaptureSink(transaction, self._policy.max_bytes, deadline, loop)
                    await _bounded(self._backend.capture(
                        CaptureRequest(self._policy.duration, self._policy.max_bytes), sink), budget)
                    sink._closed = True
                    if sink._failed or sink._bytes == 0 or loop.time() >= deadline:
                        raise RecordingError("capture-incomplete")
                    if self._fenced:
                        raise RecordingError("recorder-fenced")
                    # Finalization has a separate finite request budget; the
                    # built-in store has no suspension points during commit.
                    final_deadline = loop.time() + self._policy.request_timeout
                    finalizing = True
                    await _bounded(transaction.complete(), self._policy.request_timeout)
                    if loop.time() >= final_deadline:
                        # A broken store may already have committed: never retry.
                        self._fenced = True
                        self._fence_status = "finalization-uncertain"
                        return CaptureResult("finalization-uncertain", attempt)
                    completed = True
                    return CaptureResult("completed", attempt, sink._bytes)
                except asyncio.CancelledError:
                    cancelled = True
                except Exception:
                    pass  # Never propagate backend exception text or paths.
                finally:
                    if sink is not None:
                        sink._closed = True
                    if transaction is not None and not completed:
                        if finalizing:
                            # Never retry after entering the publication boundary,
                            # even if a custom store swallowed timeout/cancellation.
                            self._fenced = True
                            self._fence_status = "finalization-uncertain"
                        try:
                            transaction.abort()
                        except asyncio.CancelledError:
                            # A storage backend can cancel during synchronous
                            # abort too. Fence uncertain cleanup and sanitize at
                            # the same public boundary as capture/finalization.
                            cancelled = True
                            self._fenced = True
                            self._fence_status = "cleanup-failed"
                        except Exception:
                            self._fenced = True
                            self._fence_status = "cleanup-failed"
                if cancelled:
                    # Raise outside the backend exception handler, after abort:
                    # no private message, cause, context or backend traceback.
                    raise asyncio.CancelledError from None
                if self._fenced:
                    return CaptureResult(self._fence_status, attempt)
            return CaptureResult("failed", attempt)
        finally:
            self._active -= 1
