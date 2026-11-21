"""Bounded, single-owner event state and cooperative asynchronous polling.

No I/O occurs except through an explicitly supplied adapter and async sleep.
IDs are never evicted: exhaustion fails closed rather than replaying history.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
import math
import time
from typing import AsyncIterator, Awaitable, Callable

from .schema import (
    EventBatch, EventSource, InvalidEvent, MotionEvent, ObservedMotion,
    identifier, utc_time,
)


def _integer(value: object, maximum: int) -> None:
    if type(value) is not int or not 1 <= value <= maximum:
        raise ValueError("invalid-limit")


def _seconds(value: object) -> None:
    if (type(value) not in (int, float) or not 0 < value <= 86400
            or not math.isfinite(value)):
        raise ValueError("invalid-duration")


@dataclass(frozen=True)
class Checkpoint:
    """Private in-memory state, not a persistence format or delivery receipt."""

    camera_id: str = field(repr=False)
    event_ids: frozenset[str] = field(repr=False)
    saturated: bool
    version: int = 1


@dataclass(frozen=True)
class BatchResult:
    status: str
    observations: tuple[ObservedMotion, ...] = field(default=(), repr=False)
    rejected_rows: int = 0
    duplicate_rows: int = 0
    historical_ids: int = 0


class MotionObserver:
    """Use from one task; do not mutate concurrently with polling.

    A complete, entirely valid first batch seeds history without emission.
    Malformed/incomplete batches invalidate readiness and suppress their valid
    rows too. The next clean batch rebaselines. This favors replay safety over
    recall during startup, outages and malformed-history recovery.
    """

    def __init__(self, camera_id: str, *, max_seen: int = 4096,
                 max_batch: int = 128) -> None:
        self._camera_id = identifier(camera_id)
        _integer(max_seen, 100_000)
        _integer(max_batch, 10_000)
        self._max_seen = max_seen
        self._max_batch = max_batch
        self._seen: set[str] = set()
        self._baseline_ready = False
        self._saturated = False
        self._polling = False

    @property
    def baseline_ready(self) -> bool:
        return self._baseline_ready

    @property
    def saturated(self) -> bool:
        return self._saturated

    @property
    def seen_count(self) -> int:
        return len(self._seen)

    def require_baseline(self) -> None:
        """Retain all IDs while fencing emission after transport uncertainty."""
        self._baseline_ready = False

    def checkpoint(self) -> Checkpoint:
        return Checkpoint(self._camera_id, frozenset(self._seen), self._saturated)

    @classmethod
    def restore(cls, checkpoint: Checkpoint, camera_id: str, *,
                max_seen: int = 4096, max_batch: int = 128) -> MotionObserver:
        observer = cls(camera_id, max_seen=max_seen, max_batch=max_batch)
        if (type(checkpoint) is not Checkpoint
                or type(checkpoint.version) is not int or checkpoint.version != 1
                or checkpoint.camera_id != observer._camera_id
                or type(checkpoint.event_ids) is not frozenset
                or len(checkpoint.event_ids) > max_seen
                or type(checkpoint.saturated) is not bool):
            raise ValueError("invalid-checkpoint")
        for event_id in checkpoint.event_ids:
            identifier(event_id)
        observer._seen = set(checkpoint.event_ids)
        observer._saturated = checkpoint.saturated
        # Never trust saved baseline readiness across a discontinuity.
        return observer

    def observe(self, batch: object, *, received_at: datetime) -> BatchResult:
        received_at = utc_time(received_at)
        if self._saturated:
            return BatchResult("capacity")
        if (type(batch) is not EventBatch or type(batch.rows) is not tuple
                or len(batch.rows) > self._max_batch
                or type(batch.complete) is not bool):
            self.require_baseline()
            return BatchResult("rejected", rejected_rows=1)

        events: dict[str, MotionEvent] = {}
        rejected = duplicates = 0
        for row in batch.rows:
            try:
                event = MotionEvent.from_row(row, self._camera_id)
                previous = events.get(event.event_id)
                if previous is not None:
                    if previous != event:
                        raise InvalidEvent("conflicting-stable-id")
                    duplicates += 1
                else:
                    events[event.event_id] = event
            except InvalidEvent:
                rejected += 1

        if rejected or not batch.complete:
            self.require_baseline()
            # No valid prefix is remembered or emitted from a rejected window.
            return BatchResult("rejected", rejected_rows=rejected,
                               duplicate_rows=duplicates)

        unseen = set(events) - self._seen
        duplicates += len(events) - len(unseen)
        if len(self._seen) + len(unseen) > self._max_seen:
            self._saturated = True
            self.require_baseline()
            return BatchResult("capacity", duplicate_rows=duplicates)

        self._seen.update(unseen)
        if not self._baseline_ready:
            self._baseline_ready = True
            return BatchResult("baseline", duplicate_rows=duplicates,
                               historical_ids=len(unseen))

        observations = tuple(ObservedMotion(events[key], received_at)
                             for key in sorted(unseen))
        return BatchResult("observed", observations, duplicate_rows=duplicates)


@dataclass(frozen=True)
class PollPolicy:
    max_attempts: int = 100
    max_duration: float = 300
    poll_interval: float = 5
    request_timeout: float = 10
    max_backoff: float = 60
    max_consecutive_failures: int = 5

    def __post_init__(self) -> None:
        _integer(self.max_attempts, 100_000)
        _integer(self.max_consecutive_failures, 100)
        for value in (self.max_duration, self.poll_interval,
                      self.request_timeout, self.max_backoff):
            _seconds(value)
        if self.max_backoff < self.poll_interval:
            raise ValueError("invalid-backoff")

    def delay(self, failures: int) -> float:
        # Bound the exponent even when called outside the runner. Do not clamp
        # it further: valid subsecond intervals can need more than 20 doublings.
        if type(failures) is not int or not 0 <= failures <= 100:
            raise ValueError("invalid-failure-count")
        return min(self.max_backoff,
                   self.poll_interval * 2 ** max(0, failures - 1))


@dataclass(frozen=True)
class PollResult:
    attempt: int
    status: str
    batch: BatchResult | None = None


async def _fetch_with_timeout(
    fetch: Callable[[], Awaitable[EventBatch]], timeout: float,
) -> EventBatch:
    """Own one attempt until drained, even through repeated caller cancellation.

    Cancellation of the completion signal belongs only to the caller. Never
    forward a second cancellation into child cleanup or return a late success.
    Uncooperative cleanup keeps this await and the observer's guard pending.
    """
    loop = asyncio.get_running_loop()
    completion = loop.create_future()

    def signal(finished):
        if not completion.done():
            completion.set_result(finished)

    async def run():
        # Do not start a queued fetch after cancellation or timeout is accepted.
        if not completion.done():
            return await fetch()

    child = asyncio.create_task(run())
    child.add_done_callback(lambda _: signal(True))
    timer = loop.call_later(timeout, signal, False)
    cancelled = False
    try:
        try:
            if not await completion:
                raise asyncio.TimeoutError
            batch = child.result()
        finally:
            timer.cancel()
            cancelled_during_cleanup = False
            if not child.done():
                child.cancel()
            while not child.done():
                try:
                    # wait() neither cancels the child nor raises its exception.
                    await asyncio.wait((child,))
                except asyncio.CancelledError:
                    cancelled_during_cleanup = True
            if not child.cancelled():
                # Retrieve even a failed child racing cancellation or timeout.
                child.exception()
            if cancelled_during_cleanup:
                raise asyncio.CancelledError from None
    except asyncio.CancelledError:
        cancelled = True
    # Leave all handlers: `from None` alone retains private exception context.
    if cancelled:
        raise asyncio.CancelledError from None
    return batch


async def poll_motion(
    source: EventSource,
    observer: MotionObserver,
    policy: PollPolicy = PollPolicy(),
    *,
    clock: Callable[[], float] = time.monotonic,
    wall_clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> AsyncIterator[PollResult]:
    """Yield bounded batches directly; no background worker or unbounded queue.

    Reconnect and poll share one attempt timeout. Adapters must be cooperative:
    Python cannot preempt blocking code or a coroutine suppressing cancellation.
    Consumers must close this iterator on early exit and close their adapter.
    State is recorded before yielding (at-most-once observation, not delivery).
    Cancellation/timeout drain the owned attempt before releasing the observer;
    cleanup that never finishes keeps ownership held rather than detaching work.
    """
    if observer._polling:
        raise RuntimeError("observer-already-polling")
    observer._polling = True
    failures = 0
    reconnect = False

    async def fetch() -> EventBatch:
        if reconnect:
            await source.reconnect()
        return await source.poll()

    try:
        deadline = clock() + policy.max_duration
        for attempt in range(1, policy.max_attempts + 1):
            remaining = deadline - clock()
            if remaining <= 0 or observer.saturated:
                return
            started = clock()
            timeout = min(policy.request_timeout, remaining)
            try:
                batch = await _fetch_with_timeout(fetch, timeout)
                received_at = wall_clock()
                # Also reject late returns from an adapter swallowing timeout
                # cancellation. Such adapters remain outside the timing contract.
                if clock() >= min(deadline, started + timeout):
                    raise asyncio.TimeoutError
            except asyncio.CancelledError:
                raise
            except Exception as error:
                observer.require_baseline()
                failures += 1
                reconnect = True
                result = PollResult(attempt, "timeout" if isinstance(
                    error, asyncio.TimeoutError) else "poll-failed")
            else:
                failures = 0
                reconnect = False
                result = PollResult(attempt, "received", observer.observe(
                    batch, received_at=received_at))

            yield result
            if (attempt == policy.max_attempts or observer.saturated
                    or failures >= policy.max_consecutive_failures):
                return
            remaining = deadline - clock()
            if remaining <= 0:
                return
            await sleep(min(policy.delay(failures), remaining))
    finally:
        observer.require_baseline()
        observer._polling = False
