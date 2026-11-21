"""Offline application wiring; only explicitly supplied synthetic components.

No adapter/backend construction, I/O at import, queue, task or durable receipt.
The caller exclusively owns components and closes its source and storage.
"""

from __future__ import annotations

import asyncio
from contextlib import aclosing
from dataclasses import dataclass
from datetime import datetime, timezone
import time
from typing import Awaitable, Callable

from sentinel.events import EventSource, MotionObserver, ObservedMotion, PollPolicy, poll_motion
from sentinel.recording import CaptureResult, Recorder


@dataclass(frozen=True)
class PipelineResult:
    """Fixed categories and finite counts only; never retain input or clips.

    polling-ended does not assert successful delivery or distinguish which
    polling budget ended the session. completed counts synthetic completions.
    """

    status: str
    polls: int = 0
    dispatched: int = 0
    completed: int = 0
    duplicates: int = 0
    rejected_windows: int = 0
    transport_failures: int = 0


async def run_pipeline(
    source: EventSource,
    observer: MotionObserver,
    recorder: Recorder,
    policy: PollPolicy = PollPolicy(),
    *,
    clock: Callable[[], float] = time.monotonic,
    wall_clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> PipelineResult:
    """Consume poll_motion sequentially, awaiting record before proceeding.

    Supply exclusively owned observer/recorder instances and cooperative
    synthetic source, capture and storage implementations. No component is
    constructed or closed here except the polling iterator. Its finally block
    releases polling ownership and requires a new baseline, retaining IDs.

    Stop on any recording outcome other than completed/duplicate. Never retry
    or replay observations here: observer history is updated for the entire
    batch before dispatch, so an interrupted suffix can be lost. Recorder
    admission/retries/fencing remain its responsibility. Cancellation propagates
    after cleanup as a fresh CancelledError without callback exception chains.
    Ordinary unexpected exceptions stop with pipeline-failed; cleanup failure
    also prevents a successful summary. No guarantee covers arbitrary
    BaseException, blocking callbacks or detached work in supplied components.
    """
    polls = dispatched = completed = duplicates = rejected = failures = 0
    status = "polling-ended"
    cancelled = False
    try:
        if (type(observer) is not MotionObserver or type(recorder) is not Recorder
                or type(policy) is not PollPolicy):
            return PipelineResult("invalid-components")
        async with aclosing(poll_motion(
            source, observer, policy, clock=clock, wall_clock=wall_clock, sleep=sleep,
        )) as polling:
            async for result in polling:
                polls += 1
                if result.status in ("poll-failed", "timeout"):
                    failures += 1
                    continue
                batch = result.batch
                if batch is None:
                    status = "pipeline-failed"
                    break
                if batch.status == "capacity":
                    status = "observer-capacity"
                    break
                if batch.status == "rejected":
                    rejected += 1
                    continue
                if batch.status == "baseline":
                    continue
                if batch.status != "observed":
                    status = "pipeline-failed"
                    break
                for observation in batch.observations:
                    if type(observation) is not ObservedMotion:
                        status = "pipeline-failed"
                        break
                    dispatched += 1
                    capture = await recorder.record(observation)
                    # Never stringify or compare arbitrary backend result fields.
                    if type(capture) is not CaptureResult or type(capture.status) is not str:
                        status = "pipeline-failed"
                        break
                    outcome = capture.status
                    if outcome == "completed":
                        completed += 1
                    elif outcome == "duplicate":
                        duplicates += 1
                    elif outcome in ("rejected", "busy", "capacity", "failed",
                                     "finalization-uncertain", "cleanup-failed"):
                        status = "recorder-" + outcome
                        break
                    else:
                        status = "pipeline-failed"
                        break
                if status != "polling-ended":
                    break
        if status == "polling-ended" and observer.saturated:
            status = "observer-capacity"
    except asyncio.CancelledError:
        cancelled = True
    except Exception:
        status = "pipeline-failed"
    # Raise outside the handler: `from None` alone retains private __context__.
    if cancelled:
        raise asyncio.CancelledError from None
    return PipelineResult(status, polls, dispatched, completed, duplicates, rejected, failures)
