"""Deterministic offline composition tests; literal synthetic bytes only."""

import asyncio
from contextlib import contextmanager
from dataclasses import replace
import gc
import traceback
import unittest
from unittest.mock import patch

from sentinel.events import BatchResult, EventBatch, MotionObserver, PollPolicy, PollResult
from sentinel.pipeline import PipelineResult, run_pipeline
from sentinel.recording import CapturePolicy, CaptureResult, Recorder
from test_sentinel_events import NOW, broken_timestamp
from test_sentinel_observer import (
    CleanupSource, FakeClock, SyntheticSource, loop_turn, manual_request_timers,
)
from test_sentinel_recording import MemoryStore, SyntheticBackend


CAMERA = "synthetic-pipeline-camera"
MARKER = "synthetic-private-pipeline-marker"


def row(key="synthetic-event"):
    return dict(camera_id=CAMERA, event_id=key, kind="motion", reported_at=None)


def batch(*rows, complete=True):
    return EventBatch(rows, complete)


class OwnedSource(SyntheticSource):
    """Explicit cleanup method to demonstrate that the runner never owns it."""

    def __init__(self, *responses, **kwargs):
        super().__init__(*responses, **kwargs)
        self.closed = False

    async def aclose(self):
        self.closed = True


class PipelineTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.observer = MotionObserver(CAMERA)
        self.backend = SyntheticBackend()
        self.store = MemoryStore()
        self.recorder = Recorder(CAMERA, self.backend, self.store)
        self.policy = PollPolicy(max_attempts=4, max_duration=100, poll_interval=1,
                                 request_timeout=10, max_backoff=4)

    async def run_source(self, source, **limits):
        before = asyncio.all_tasks()
        result = await run_pipeline(
            source, self.observer, self.recorder, replace(self.policy, **limits),
            clock=self.clock, wall_clock=lambda: NOW, sleep=self.clock.sleep,
        )
        self.assertFalse(asyncio.all_tasks() - before)
        self.assertFalse(self.observer._polling)
        self.assertFalse(self.observer.baseline_ready)
        self.assertFalse(source.active)
        self.assertEqual(self.store.active, 0)
        return result

    @contextmanager
    def manual_deadlines(self):
        """Keep recorder scheduling separate from its artificial 20 ms budgets."""
        loop = asyncio.get_running_loop()
        call_later = loop.call_later
        timers = []
        with patch.object(loop, "time", return_value=loop.time()) as clock:
            def schedule(delay, callback, *args, **kwargs):
                # Observer request timers keep their normal lifecycle.
                if delay != 0.02:
                    return call_later(delay, callback, *args, **kwargs)
                handle = asyncio.Handle(callback, args, loop, **kwargs)
                timers.append((handle, lambda: callback(*args)))
                return handle

            with patch.object(loop, "call_later", schedule):
                yield clock, timers

    async def run_with_finalization_timeout(self, source):
        before = asyncio.all_tasks()
        with self.manual_deadlines() as (clock, timers):
            task = asyncio.create_task(self.run_source(source))
            entered = asyncio.create_task(self.store.finalizing.wait())
            try:
                # A premature pipeline return must fail instead of waiting forever.
                await asyncio.wait((task, entered), return_when=asyncio.FIRST_COMPLETED)
                self.assertTrue(self.store.finalizing.is_set())
                self.assertFalse(task.done())
                self.assertEqual(len(timers), 2)
                self.assertTrue(timers[0][0].cancelled())  # Capture completed.
                self.assertFalse(timers[1][0].cancelled())
                clock.return_value += 0.02
                timers[1][1]()  # Exercise the real recorder timeout/cancel/drain.
                result = await task
                self.assertTrue(timers[1][0].cancelled())
            finally:
                for owned in (task, entered):
                    if not owned.done():
                        owned.cancel()
                await asyncio.gather(task, entered, return_exceptions=True)
        self.assertEqual(asyncio.all_tasks(), before)
        return result

    def assert_redacted(self, error, *, fresh=True):
        pending = [error]
        while pending:
            nested = pending.pop()
            for rendered in (str(nested), repr(nested), "".join(traceback.format_exception(
                    type(nested), nested, nested.__traceback__))):
                self.assertNotIn(MARKER, rendered)
            pending.extend(link for link in (nested.__cause__, nested.__context__)
                           if link is not None)
        if fresh:
            self.assertIsNone(error.__cause__)
            self.assertIsNone(error.__context__)

    async def test_history_then_new_event_and_repeats(self):
        source = OwnedSource(batch(row("old")), batch(row("old"), row("new")),
                             batch(row("new"), row("old")), batch(row("new")))
        result = await self.run_source(source)
        self.assertEqual(result, PipelineResult("polling-ended", polls=4,
                                               dispatched=1, completed=1))
        self.assertEqual(self.backend.calls, 1)
        self.assertEqual(self.store.clips, [b"synthetic-clip"])

    async def test_initial_history_never_records(self):
        result = await self.run_source(OwnedSource(batch(row("old"))), max_attempts=1)
        self.assertEqual(result.dispatched, 0)
        self.assertEqual(self.backend.calls, 0)

    async def test_old_provider_timestamp_is_not_physical_motion_evidence(self):
        old = row()
        old["reported_at"] = NOW.replace(year=2000)
        original = self.recorder.record
        observed = []

        async def inspect(item):
            observed.append(item)
            return await original(item)

        with patch.object(self.recorder, "record", inspect):
            result = await self.run_source(OwnedSource(batch(), batch(old)), max_attempts=2)
        self.assertEqual(result.completed, 1)
        self.assertEqual(observed[0].freshness, "unverified")
        self.assertIsNone(observed[0].physical_motion_at)
        self.assertIsNone(observed[0].latency_seconds)

    async def test_multiple_events_backpressure_without_prefetch_or_remaining_tasks(self):
        source = OwnedSource(batch(), batch(row("a"), row("b")), batch(row("c")))
        starts = [asyncio.Event(), asyncio.Event()]
        releases = [asyncio.Event(), asyncio.Event()]
        calls = []

        async def capture(request, sink):
            index = len(calls)
            calls.append(index)
            if index < 2:
                starts[index].set()
                await releases[index].wait()
            sink.write(b"synthetic")

        before = asyncio.all_tasks()
        with patch.object(self.backend, "capture", capture):
            task = asyncio.create_task(self.run_source(source, max_attempts=3))
            try:
                await asyncio.wait_for(starts[0].wait(), 2)
                self.assertEqual((source.polls, calls), (2, [0]))
                self.assertLessEqual(len(asyncio.all_tasks() - before), 2)
                releases[0].set()
                await asyncio.wait_for(starts[1].wait(), 2)
                self.assertEqual((source.polls, calls), (2, [0, 1]))
                self.assertEqual(len(self.store.clips), 1)
                releases[1].set()
                result = await task
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        self.assertEqual((result.completed, self.store.maximum_active), (3, 1))
        self.assertEqual(asyncio.all_tasks(), before)

    async def test_malformed_and_incomplete_windows_require_rebaseline(self):
        malformed = row("bad")
        malformed["extra"] = {"raw": MARKER}
        timestamp = row("bad-time")
        timestamp["reported_at"] = broken_timestamp()
        for rejected in (batch(row("prefix"), malformed), batch(row("prefix"), complete=False),
                         batch(row("prefix"), timestamp), batch(complete=False)):
            with self.subTest(window_type=type(rejected)):
                self.setUp()
                source = OwnedSource(batch(), rejected, batch(row("history")), batch(row("new")))
                result = await self.run_source(source)
                self.assertEqual((result.rejected_windows, result.completed), (1, 1))
                self.assertEqual(self.backend.calls, 1)

    async def test_transport_reconnect_rebaseline_retains_ids(self):
        source = OwnedSource(batch(), batch(row("seen")), RuntimeError(MARKER),
                             batch(row("outage")), batch(row("seen")), batch(row("new")))
        result = await self.run_source(source, max_attempts=6)
        self.assertEqual((result.completed, result.transport_failures), (2, 1))
        self.assertEqual(source.reconnects, 1)
        self.assertNotIn(MARKER, repr(result))

    async def test_reconnect_failures_exhaust_existing_failure_budget(self):
        source = OwnedSource(RuntimeError(MARKER), batch(),
                             reconnect_errors=[RuntimeError(MARKER)] * 2)
        result = await self.run_source(source, max_consecutive_failures=3)
        self.assertEqual((result.status, result.polls, result.transport_failures),
                         ("polling-ended", 3, 3))
        self.assertEqual((source.polls, source.reconnects), (1, 2))
        self.assertEqual(self.clock.delays, [1, 2])

    async def test_transport_timeout_uses_observer_recovery(self):
        class TimeoutSource(OwnedSource):
            async def poll(inner):
                if inner.polls == 1:
                    inner.polls += 1
                    raise asyncio.TimeoutError(MARKER)
                return await super(TimeoutSource, inner).poll()

        source = TimeoutSource(batch(), batch(row("history")), batch(row("new")))
        result = await self.run_source(source)
        self.assertEqual((result.completed, result.transport_failures), (1, 1))
        self.assertEqual(source.reconnects, 1)

    async def test_poll_duration_budget_exhaustion(self):
        source = OwnedSource(batch(), batch(row()))
        result = await self.run_source(source, max_duration=0.5)
        self.assertEqual((result.polls, result.dispatched), (1, 0))

    async def test_observer_capacity_fails_closed(self):
        self.observer = MotionObserver(CAMERA, max_seen=1)
        result = await self.run_source(OwnedSource(batch(row("old")), batch(row("new"))))
        self.assertEqual((result.status, result.dispatched), ("observer-capacity", 0))
        self.assertTrue(self.observer.saturated)

    async def test_already_saturated_observer_does_not_poll(self):
        self.observer = MotionObserver(CAMERA, max_seen=1)
        self.observer.observe(batch(row("a"), row("b")), received_at=NOW)
        source = OwnedSource(batch())
        result = await self.run_source(source)
        self.assertEqual((result.status, source.polls), ("observer-capacity", 0))

    async def test_recorder_capacity_stops_batch_and_closes_polling(self):
        self.store = MemoryStore()
        self.recorder = Recorder(CAMERA, self.backend, self.store, CapturePolicy(max_seen=1))
        source = OwnedSource(batch(), batch(row("a"), row("b"), row("c")), batch(row("d")))
        result = await self.run_source(source)
        self.assertEqual((result.status, result.dispatched, result.completed),
                         ("recorder-capacity", 2, 1))
        self.assertEqual((source.polls, self.backend.calls), (2, 1))
        # Entire observed batch is remembered even though its suffix was not delivered.
        self.assertEqual(self.observer.seen_count, 3)

    async def test_recording_failure_stops_without_pipeline_retry(self):
        self.backend.error_count = 10
        source = OwnedSource(batch(), batch(row("a"), row("b")), batch(row("c")))
        result = await self.run_source(source)
        self.assertEqual((result.status, source.polls, self.backend.calls),
                         ("recorder-failed", 2, 1))
        self.assertEqual(self.store.clips, [])

    async def test_storage_reservation_exhaustion_stops_before_capture(self):
        source = OwnedSource(batch(), batch(row("a"), row("b")), batch(row("c")))
        with patch.object(self.store, "begin", side_effect=OSError(MARKER)) as begin:
            result = await self.run_source(source)
        self.assertEqual((result.status, result.dispatched, source.polls),
                         ("recorder-failed", 1, 2))
        self.assertEqual((begin.call_count, self.backend.calls, self.store.clips), (1, 0, []))
        self.assertNotIn(MARKER, repr(result))

    async def test_retry_budget_is_owned_by_recorder(self):
        self.backend.error_count = 1
        self.store = MemoryStore()
        self.recorder = Recorder(CAMERA, self.backend, self.store, CapturePolicy(retries=1))
        result = await self.run_source(OwnedSource(batch(), batch(row())), max_attempts=2)
        self.assertEqual((result.dispatched, result.completed, self.backend.calls), (1, 1, 2))

    async def test_capture_timeout_stops_and_drains_backend(self):
        self.backend.wait = True
        self.store = MemoryStore()
        self.recorder = Recorder(CAMERA, self.backend, self.store,
                                 CapturePolicy(duration=0.02, request_timeout=1))
        result = await self.run_source(OwnedSource(batch(), batch(row("a"), row("b"))))
        self.assertEqual(result.status, "recorder-failed")
        self.assertEqual((self.backend.calls, self.backend.cleaned, self.store.clips), (1, 1, []))

    async def test_finalization_and_cleanup_fencing_stop_pipeline(self):
        for mode, expected in (("final_error", "finalization-uncertain"),
                               ("wait_finalize", "finalization-uncertain"),
                               ("abort_error", "cleanup-failed")):
            with self.subTest(mode=mode):
                self.setUp()
                setattr(self.store, mode, True)
                if mode == "abort_error":
                    self.backend.error_count = 1
                if mode == "wait_finalize":
                    self.store = MemoryStore()
                    self.store.wait_finalize = True
                    self.recorder = Recorder(CAMERA, self.backend, self.store,
                                             CapturePolicy(request_timeout=0.02))
                source = OwnedSource(batch(), batch(row("a"), row("b")), batch(row("c")))
                if mode == "wait_finalize":
                    result = await self.run_with_finalization_timeout(source)
                else:
                    result = await self.run_source(source)
                self.assertEqual(result.status, "recorder-" + expected)
                self.assertTrue(self.recorder._fenced)
                self.assertEqual((result.dispatched, result.completed, source.polls), (1, 0, 2))
                self.assertEqual((self.backend.calls, self.backend.cleaned), (1, 1))
                self.assertEqual(self.store.finalizing.is_set(), mode != "abort_error")
                self.assertEqual(self.store.final_cleanup, mode == "wait_finalize")
                self.assertEqual((self.store.clips, self.store.active, self.recorder._active),
                                 ([], 0, 0))
                # Fencing persists across a new polling run; no new capture begins.
                again = await self.run_source(OwnedSource(batch(), batch(row("d"))))
                self.assertEqual(again.status, "recorder-" + expected)
                self.assertEqual(self.backend.calls, 1)

    async def test_capture_deadline_before_finalization_stops_without_fencing(self):
        self.store = MemoryStore()
        self.store.wait_finalize = True
        self.recorder = Recorder(CAMERA, self.backend, self.store,
                                 CapturePolicy(request_timeout=0.02))
        capture = self.backend.capture
        captured_before_deadline = []
        source = OwnedSource(batch(), batch(row("a"), row("b")), batch(row("c")))
        with self.manual_deadlines() as (clock, timers):
            async def delayed_delivery(request, sink):
                await capture(request, sink)
                # Bytes were written in time; recorder delivery reaches the deadline.
                captured_before_deadline.append(clock.return_value < sink._deadline)
                clock.return_value += 0.02

            with patch.object(self.backend, "capture", delayed_delivery):
                result = await self.run_source(source)
            self.assertEqual(len(timers), 1)  # Finalization was never scheduled.
            self.assertTrue(timers[0][0].cancelled())
        self.assertEqual((result.status, result.dispatched, result.completed, source.polls),
                         ("recorder-failed", 1, 0, 2))
        self.assertEqual(captured_before_deadline, [True])
        self.assertEqual((self.backend.calls, self.backend.cleaned), (1, 1))
        self.assertFalse(self.store.finalizing.is_set())
        self.assertFalse(self.store.final_cleanup)
        self.assertFalse(self.recorder._fenced)
        self.assertEqual((self.store.clips, self.store.active, self.recorder._active), ([], 0, 0))

    async def test_already_fenced_recorder_does_not_capture(self):
        self.store.final_error = True
        await self.run_source(OwnedSource(batch(), batch(row("a"))), max_attempts=2)
        self.store.final_error = False
        result = await self.run_source(OwnedSource(batch(), batch(row("b"))), max_attempts=2)
        self.assertEqual((result.status, self.backend.calls), ("recorder-finalization-uncertain", 1))

    async def test_recorder_duplicate_is_counted_without_second_dedup_set(self):
        self.observer.observe(batch(), received_at=NOW)
        item = self.observer.observe(batch(row()), received_at=NOW).observations[0]
        await self.recorder.record(item)
        self.observer = MotionObserver(CAMERA)
        result = await self.run_source(OwnedSource(batch(), batch(row())), max_attempts=2)
        self.assertEqual((result.duplicates, result.completed, self.backend.calls), (1, 0, 1))

    async def test_unknown_or_malformed_recorder_results_are_not_exposed(self):
        class Hostile:
            def __str__(self):
                raise RuntimeError(MARKER)

            __repr__ = __str__

        for outcome in (CaptureResult(MARKER), CaptureResult(Hostile()), Hostile()):
            with self.subTest(result_type=type(outcome)):
                self.setUp()
                async def record(item):
                    return outcome
                with patch.object(self.recorder, "record", record):
                    result = await self.run_source(OwnedSource(batch(), batch(row())))
                self.assertEqual(result.status, "pipeline-failed")
                self.assertNotIn(MARKER, repr(result))

    async def test_rejected_and_busy_results_stop_without_resubmission(self):
        for outcome in ("rejected", "busy"):
            with self.subTest(outcome=outcome):
                self.setUp()
                async def record(item):
                    return CaptureResult(outcome)
                with patch.object(self.recorder, "record", record):
                    source = OwnedSource(batch(), batch(row("a"), row("b")))
                    result = await self.run_source(source)
                self.assertEqual((result.status, result.dispatched), ("recorder-" + outcome, 1))

    async def test_unexpected_record_exception_is_fixed_and_closes_iterator(self):
        async def record(item):
            raise RuntimeError(MARKER) from ValueError(MARKER)
        with patch.object(self.recorder, "record", record):
            result = await self.run_source(OwnedSource(batch(), batch(row())))
        self.assertEqual(result.status, "pipeline-failed")
        self.assertNotIn(MARKER, repr(result))

    async def test_unexpected_polling_callback_error_is_fixed(self):
        async def sleep(delay):
            raise RuntimeError(MARKER)
        before = asyncio.all_tasks()
        result = await run_pipeline(OwnedSource(batch()), self.observer, self.recorder,
                                    self.policy, sleep=sleep)
        self.assertEqual(result.status, "pipeline-failed")
        self.assertFalse(self.observer._polling)
        self.assertEqual(asyncio.all_tasks(), before)

    async def test_cancellation_during_poll_capture_and_finalization(self):
        for phase in ("poll", "capture", "finalization"):
            with self.subTest(phase=phase):
                self.setUp()
                started = asyncio.Event()
                source = OwnedSource(batch(), batch(row("a"), row("b")))
                if phase == "poll":
                    async def poll():
                        started.set()
                        await asyncio.Event().wait()
                    source.poll = poll
                elif phase == "capture":
                    self.backend.wait = True
                    started = self.backend.started
                else:
                    self.store.wait_finalize = True
                    started = self.store.finalizing
                before = asyncio.all_tasks()
                async def run():
                    try:
                        return await run_pipeline(
                            source, self.observer, self.recorder, self.policy,
                            clock=self.clock, wall_clock=lambda: NOW, sleep=self.clock.sleep)
                    except asyncio.CancelledError as error:
                        # Inspect at the pipeline boundary. Python 3.10 Task can
                        # add another sanitized cancellation when awaited.
                        self.assert_redacted(error)
                        raise
                task = asyncio.create_task(run())
                try:
                    await asyncio.wait_for(started.wait(), 2)
                    task.cancel(MARKER)
                    try:
                        await task
                    except asyncio.CancelledError as error:
                        self.assert_redacted(error, fresh=False)
                    else:
                        self.fail("cancellation was swallowed")
                finally:
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                self.assertEqual(asyncio.all_tasks(), before)
                self.assertFalse(self.observer._polling)
                self.assertFalse(self.observer.baseline_ready)
                self.assertEqual((self.store.active, self.store.clips), (0, []))
                self.assertFalse(source.closed)
                self.assertEqual(self.recorder._fenced, phase == "finalization")

    async def test_cancel_racing_successful_poll_never_starts_recording(self):
        for ordering in ("before-signal", "after-signal"):
            with self.subTest(ordering=ordering):
                self.setUp()
                accepted = []
                before = asyncio.all_tasks()

                def cancel():
                    accepted.append(task.cancel(MARKER))

                class RacingSource(OwnedSource):
                    async def poll(inner):
                        result = await super().poll()
                        if inner.polls == 2:
                            if ordering == "before-signal":
                                asyncio.get_running_loop().call_soon(cancel)
                            else:
                                asyncio.current_task().add_done_callback(lambda _: cancel())
                        return result

                source = RacingSource(batch(), batch(row("new")))

                async def run():
                    try:
                        return await self.run_source(source, max_attempts=2)
                    except asyncio.CancelledError as error:
                        self.assert_redacted(error)
                        raise

                with patch.object(self.recorder, "record", wraps=self.recorder.record) as record:
                    task = asyncio.create_task(run())
                    try:
                        try:
                            await task
                        except asyncio.CancelledError as error:
                            self.assert_redacted(error, fresh=False)
                        else:
                            self.fail("cancellation was swallowed")
                        self.assertEqual(accepted, [True])
                        record.assert_not_called()
                        self.assertEqual((self.backend.calls, self.store.clips, self.store.active),
                                         (0, [], 0))
                        self.assertEqual(source.polls, 2)
                        self.assertFalse(self.observer._polling)
                        self.assertFalse(self.observer.baseline_ready)
                        self.assertEqual(asyncio.all_tasks(), before)
                    finally:
                        task.cancel()
                        await asyncio.gather(task, return_exceptions=True)

    async def check_poll_cleanup_cancellation(self, *, timeout=False):
        for outcome in ("cancelled", "returned", "failed"):
            with self.subTest(outcome=outcome):
                self.setUp()
                self.observer.observe(batch(row("history")), received_at=NOW)
                source = CleanupSource(outcome=outcome, marker=MARKER,
                                       returned_batch=batch(row("new")))
                before = asyncio.all_tasks()
                incidents = []
                loop = asyncio.get_running_loop()
                old_handler = loop.get_exception_handler()
                loop.set_exception_handler(lambda loop, context: incidents.append(context))

                async def run():
                    try:
                        return await self.run_source(source, max_attempts=1)
                    except asyncio.CancelledError as error:
                        self.assert_redacted(error)
                        raise

                with manual_request_timers() as timers, \
                        patch.object(self.recorder, "record", wraps=self.recorder.record) as record:
                    task = asyncio.create_task(run())
                    try:
                        await asyncio.wait_for(source.entered.wait(), 2)
                        if timeout:
                            self.assertEqual(len(timers), 1)
                            timers[0][1]()
                        else:
                            self.assertTrue(task.cancel(MARKER))
                        await asyncio.wait_for(source.cleaning.wait(), 2)
                        for _ in range(2):
                            self.assertTrue(task.cancel(MARKER))
                            await loop_turn()
                            self.assertFalse(task.done())
                            self.assertTrue(source.active)
                            self.assertTrue(self.observer._polling)
                            record.assert_not_called()
                        source.release.set()
                        try:
                            await task
                        except asyncio.CancelledError as error:
                            self.assert_redacted(error, fresh=False)
                        else:
                            self.fail("cancellation was swallowed")
                        await asyncio.wait((source.worker,))
                        self.assertFalse(source.worker._log_traceback)
                        self.assertEqual(source.interrupted, 0)
                        self.assertFalse(source.active)
                        self.assertFalse(self.observer._polling)
                        self.assertFalse(self.observer.baseline_ready)
                        self.assertEqual((source.polls, source.reconnects), (1, 0))
                        record.assert_not_called()
                        self.assertEqual((self.backend.calls, self.store.active, self.store.clips),
                                         (0, 0, []))
                        self.assertEqual(asyncio.all_tasks(), before)
                        self.assertEqual(len(incidents), 0)
                    finally:
                        source.release.set()
                        if not task.done():
                            task.cancel()
                        owned = (task,) if source.worker is None else (task, source.worker)
                        await asyncio.gather(*owned, return_exceptions=True)
                        # Drop completed tasks and flush delayed diagnostic callbacks.
                        del owned
                        source.worker = None
                        task = None
                        gc.collect()
                        await loop_turn()
                        loop.set_exception_handler(old_handler)
                    self.assertEqual(len(incidents), 0)

    async def test_repeated_cancellation_owns_poll_cleanup_until_drained(self):
        await self.check_poll_cleanup_cancellation()

    async def test_cancellation_during_poll_timeout_cleanup_never_records(self):
        await self.check_poll_cleanup_cancellation(timeout=True)

    async def test_callback_cancellation_chains_are_sanitized(self):
        async def record(item):
            raise asyncio.CancelledError(MARKER) from RuntimeError(MARKER)
        before = asyncio.all_tasks()
        with patch.object(self.recorder, "record", record):
            try:
                await self.run_source(OwnedSource(batch(), batch(row())))
            except asyncio.CancelledError as error:
                self.assert_redacted(error)
            else:
                self.fail("cancellation was swallowed")
        self.assertEqual(asyncio.all_tasks(), before)
        self.assertFalse(self.observer._polling)

    async def test_source_and_storage_cleanup_stay_caller_owned(self):
        source = OwnedSource(batch(), batch(row()))
        with patch.object(self.store, "close", create=True) as close:
            await self.run_source(source, max_attempts=2)
            close.assert_not_called()
        self.assertFalse(source.closed)
        self.assertTrue(self.store.claimed)
        await source.aclose()
        self.assertTrue(source.closed)

    async def test_interrupted_suffix_is_not_replayed_after_restart(self):
        self.backend.error_count = 1
        await self.run_source(OwnedSource(batch(), batch(row("a"), row("b"))))
        self.backend.error_count = 0
        result = await self.run_source(OwnedSource(batch(), batch(row("a"), row("b"))))
        self.assertEqual((result.dispatched, self.backend.calls), (0, 1))

    async def test_invalid_components_reject_before_source_access(self):
        source = OwnedSource(batch())
        for components in ((None, self.recorder, self.policy),
                           (self.observer, None, self.policy),
                           (self.observer, self.recorder, None)):
            result = await run_pipeline(source, *components)
            self.assertEqual(result, PipelineResult("invalid-components"))
        self.assertEqual(source.polls, 0)

    async def test_active_observer_ownership_is_not_stolen(self):
        from sentinel.events import poll_motion

        source = OwnedSource(batch(), batch(row()))
        polling = poll_motion(source, self.observer, self.policy,
                              clock=self.clock, wall_clock=lambda: NOW, sleep=self.clock.sleep)
        try:
            await polling.__anext__()
            self.assertTrue(self.observer.baseline_ready)
            result = await run_pipeline(source, self.observer, self.recorder, self.policy)
            self.assertEqual(result.status, "pipeline-failed")
            self.assertTrue(self.observer._polling)
            self.assertTrue(self.observer.baseline_ready)
            self.assertEqual(source.polls, 1)
            self.assertEqual(self.backend.calls, 0)
        finally:
            await polling.aclose()
        self.assertFalse(self.observer._polling)

    async def test_default_results_and_logs_contain_no_input_metadata(self):
        source = OwnedSource(batch(row(MARKER)), batch(row(MARKER + "-new")))
        with patch("builtins.print") as output, patch("logging.Logger._log") as log:
            result = await self.run_source(source, max_attempts=2)
        output.assert_not_called()
        log.assert_not_called()
        for sensitive in (MARKER, CAMERA, NOW.isoformat(), "synthetic-clip"):
            self.assertNotIn(sensitive, str(result))
            self.assertNotIn(sensitive, repr(result))

    async def test_invalid_observation_from_broken_iterator_never_reaches_recorder(self):
        closed = []
        async def polling(*args, **kwargs):
            try:
                yield PollResult(1, "received", BatchResult("observed", (object(),)))
            finally:
                closed.append(True)
        with patch("sentinel.pipeline.poll_motion", polling), \
                patch.object(self.recorder, "record") as record:
            result = await self.run_source(OwnedSource())
        self.assertEqual(result.status, "pipeline-failed")
        record.assert_not_called()
        self.assertEqual(closed, [True])

    async def test_iterator_cleanup_failure_never_returns_success(self):
        async def polling(*args, **kwargs):
            try:
                yield PollResult(1, "received", BatchResult("capacity"))
            finally:
                raise RuntimeError(MARKER)
        with patch("sentinel.pipeline.poll_motion", polling):
            result = await self.run_source(OwnedSource())
        self.assertEqual(result.status, "pipeline-failed")
        self.assertNotIn(MARKER, repr(result))


if __name__ == "__main__":
    unittest.main()
