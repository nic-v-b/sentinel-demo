"""Finite polling against synthetic cooperative sources; no live adapters."""

import asyncio
from contextlib import contextmanager
from dataclasses import replace
import gc
import traceback
import unittest
from unittest.mock import patch

from sentinel.events import MotionObserver, PollPolicy, poll_motion
from test_sentinel_events import CAMERA, NOW, batch, broken_timestamp, row


PRIVATE = "synthetic-private-observer-marker"


async def loop_turn():
    """Fence callbacks already queued, without a wall-clock race delay."""
    loop = asyncio.get_running_loop()
    reached = loop.create_future()
    loop.call_soon(reached.set_result, None)
    await reached


@contextmanager
def manual_request_timers():
    """Fire the actual ten-second polling callback at an explicit phase."""
    loop = asyncio.get_running_loop()
    original = loop.call_later
    timers = []

    def schedule(delay, callback, *args, **kwargs):
        if delay != 10:
            return original(delay, callback, *args, **kwargs)
        handle = asyncio.Handle(callback, args, loop, **kwargs)
        timers.append((handle, lambda: callback(*args)))
        return handle

    with patch.object(loop, "call_later", schedule):
        yield timers


class FakeClock:
    def __init__(self):
        self.now = 0.0
        self.delays = []

    def __call__(self):
        return self.now

    async def sleep(self, delay):
        self.delays.append(delay)
        self.now += delay
        await asyncio.sleep(0)


class SyntheticSource:
    def __init__(self, *responses, reconnect_errors=()):
        self.responses = iter(responses)
        self.reconnect_errors = iter(reconnect_errors)
        self.polls = self.reconnects = 0
        self.active = False

    async def poll(self):
        self.polls += 1
        self.active = True
        try:
            value = next(self.responses)
            if isinstance(value, BaseException):
                raise value
            return value
        finally:
            self.active = False

    async def reconnect(self):
        self.reconnects += 1
        error = next(self.reconnect_errors, None)
        if error is not None:
            raise error


class CleanupSource(SyntheticSource):
    """First poll holds cleanup until released; later polls allow recovery."""

    def __init__(self, *responses, outcome="cancelled", marker=PRIVATE,
                 returned_batch=None):
        super().__init__(*responses)
        self.outcome = outcome
        self.marker = marker
        self.returned_batch = returned_batch if returned_batch is not None else batch(row("late"))
        self.entered = asyncio.Event()
        self.cleaning = asyncio.Event()
        self.release = asyncio.Event()
        self.worker = None
        self.interrupted = 0

    async def poll(self):
        if self.polls:
            return await super().poll()
        self.polls += 1
        self.worker = asyncio.current_task()
        self.active = True
        self.entered.set()
        try:
            await asyncio.Future()
        finally:
            self.cleaning.set()
            try:
                await self.release.wait()
                if self.outcome == "returned":
                    return self.returned_batch
                if self.outcome == "failed":
                    raise RuntimeError(self.marker) from ValueError(self.marker)
            except asyncio.CancelledError:
                self.interrupted += 1
                raise
            finally:
                self.active = False


class PolicyTests(unittest.TestCase):
    def test_invalid_counts_and_durations_are_rejected(self):
        for name in ("max_attempts", "max_consecutive_failures"):
            for value in (False, 0, -1, 1.5, float("inf"), 100_001):
                with self.subTest(name=name, value=value), self.assertRaises(ValueError):
                    PollPolicy(**{name: value})
        for name in ("max_duration", "poll_interval", "request_timeout", "max_backoff"):
            for value in (False, 0, -1, "1", float("nan"), float("inf"), 86_401, 10 ** 1000):
                with self.subTest(name=name, value=value), self.assertRaises(ValueError):
                    PollPolicy(**{name: value})
        with self.assertRaises(ValueError):
            PollPolicy(poll_interval=10, max_backoff=5)

    def test_backoff_is_exponential_then_capped(self):
        policy = PollPolicy(poll_interval=2, max_backoff=9)
        self.assertEqual([policy.delay(n) for n in (0, 1, 2, 3, 4, 100)], [2, 2, 4, 8, 9, 9])

    def test_small_intervals_keep_doubling_until_the_configured_cap(self):
        policy = PollPolicy(poll_interval=0.000001, max_backoff=60)
        self.assertAlmostEqual(policy.delay(22), 2.097152)
        self.assertEqual(policy.delay(27), 60)
        self.assertEqual(policy.delay(100), 60)

    def test_backoff_rejects_invalid_failure_counts(self):
        for value in (False, -1, 1.0, 101, 10 ** 1000):
            with self.subTest(value=value), self.assertRaises(ValueError):
                PollPolicy().delay(value)


class PollingTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.observer = MotionObserver(CAMERA)
        self.clock = FakeClock()
        self.policy = PollPolicy(max_attempts=4, max_duration=100,
                                 poll_interval=1, max_backoff=4,
                                 request_timeout=10, max_consecutive_failures=4)

    def stream(self, source, **policy):
        return poll_motion(source, self.observer, replace(self.policy, **policy),
                           clock=self.clock, wall_clock=lambda: NOW, sleep=self.clock.sleep)

    async def collect(self, source, **policy):
        return [result async for result in self.stream(source, **policy)]

    def assert_safe_cancellation(self, error, *, fresh=False):
        pending = [error]
        while pending:
            nested = pending.pop()
            for rendered in (str(nested), repr(nested), "".join(traceback.format_exception(
                    type(nested), nested, nested.__traceback__))):
                self.assertNotIn(PRIVATE, rendered)
            pending.extend(link for link in (nested.__cause__, nested.__context__)
                           if link is not None)
        if fresh:
            self.assertIsNone(error.__context__)
            self.assertIsNone(error.__cause__)

    async def test_cancel_racing_successful_poll_never_observes(self):
        for ordering in ("before-signal", "after-signal"):
            with self.subTest(ordering=ordering):
                self.setUp()
                self.observer.observe(batch(row("history")), received_at=NOW)
                accepted = []
                yielded = []
                before = asyncio.all_tasks()

                def cancel():
                    accepted.append(task.cancel(PRIVATE))

                class RacingSource(SyntheticSource):
                    async def poll(inner):
                        inner.polls += 1
                        if ordering == "before-signal":
                            asyncio.get_running_loop().call_soon(cancel)
                        else:
                            asyncio.current_task().add_done_callback(lambda _: cancel())
                        return batch(row("new"))

                async def consume():
                    try:
                        async for result in self.stream(source, max_attempts=1):
                            yielded.append(result)
                    except asyncio.CancelledError as error:
                        self.assert_safe_cancellation(error, fresh=True)
                        raise

                source = RacingSource()
                task = asyncio.create_task(consume())
                try:
                    with self.assertRaises(asyncio.CancelledError):
                        await task
                    self.assertEqual(accepted, [True])
                    self.assertEqual(yielded, [])
                    self.assertEqual(self.observer.seen_count, 1)
                    self.assertFalse(self.observer._polling)
                    self.assertFalse(self.observer.baseline_ready)
                    self.assertEqual(asyncio.all_tasks(), before)
                finally:
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)

    async def check_cleanup_cancellation(self, *, timeout=False, race=False):
        for outcome in ("cancelled", "returned", "failed"):
            with self.subTest(outcome=outcome):
                self.setUp()
                self.observer.observe(batch(row("history")), received_at=NOW)
                source = CleanupSource(outcome=outcome)
                before = asyncio.all_tasks()
                incidents = []
                loop = asyncio.get_running_loop()
                old_handler = loop.get_exception_handler()
                loop.set_exception_handler(lambda loop, context: incidents.append(context))

                async def consume():
                    try:
                        return await self.collect(source, max_attempts=1)
                    except asyncio.CancelledError as error:
                        self.assert_safe_cancellation(error, fresh=True)
                        raise

                with manual_request_timers() as timers:
                    task = asyncio.create_task(consume())
                    try:
                        await asyncio.wait_for(source.entered.wait(), 2)
                        if timeout:
                            self.assertEqual(len(timers), 1)
                            timers[0][1]()
                        else:
                            self.assertTrue(task.cancel(PRIVATE))
                        await asyncio.wait_for(source.cleaning.wait(), 2)
                        self.assertTrue(task.cancel(PRIVATE))
                        if race:
                            source.release.set()
                        else:
                            await loop_turn()
                            self.assertFalse(task.done())
                            self.assertTrue(source.active)
                            self.assertTrue(self.observer._polling)
                            # A stalled cleanup fences reuse, however long it lasts.
                            other = self.stream(SyntheticSource(batch()))
                            try:
                                with self.assertRaisesRegex(RuntimeError, "observer-already-polling"):
                                    await other.__anext__()
                            finally:
                                await other.aclose()
                            self.assertTrue(task.cancel(PRIVATE))
                            await loop_turn()
                            self.assertFalse(task.done())
                            source.release.set()
                        try:
                            await task
                        except asyncio.CancelledError as error:
                            self.assert_safe_cancellation(error)
                        else:
                            self.fail("cancellation was swallowed")
                        await asyncio.wait((source.worker,))
                        # Inspect before the test's failure-cleanup gather can retrieve it.
                        self.assertFalse(source.worker._log_traceback)
                        self.assertEqual(source.interrupted, 0)
                        self.assertFalse(source.active)
                        self.assertFalse(self.observer._polling)
                        self.assertFalse(self.observer.baseline_ready)
                        self.assertEqual(self.observer.seen_count, 1)
                        self.assertEqual((source.polls, source.reconnects), (1, 0))
                        self.assertTrue(timers[0][0].cancelled())
                        self.assertEqual(asyncio.all_tasks(), before)
                        self.assertEqual(len(incidents), 0)
                    finally:
                        source.release.set()
                        if not task.done():
                            task.cancel()
                        owned = (task,) if source.worker is None else (task, source.worker)
                        await asyncio.gather(*owned, return_exceptions=True)
                        loop.set_exception_handler(old_handler)

    async def test_repeated_cancellation_drains_poll_cleanup(self):
        await self.check_cleanup_cancellation()

    async def test_cancel_during_timeout_cleanup_keeps_poll_owned(self):
        await self.check_cleanup_cancellation(timeout=True)

    async def test_cancel_racing_timeout_cleanup_completion_propagates(self):
        await self.check_cleanup_cancellation(timeout=True, race=True)

    async def test_timeout_cleanup_outcomes_reconnect_and_rebaseline(self):
        for outcome in ("cancelled", "returned", "failed"):
            with self.subTest(outcome=outcome):
                self.setUp()
                source = CleanupSource(batch(row("history")), batch(row("new")), outcome=outcome)
                before = asyncio.all_tasks()
                with manual_request_timers() as timers:
                    task = asyncio.create_task(self.collect(source, max_attempts=3))
                    try:
                        await asyncio.wait_for(source.entered.wait(), 2)
                        timers[0][1]()
                        await asyncio.wait_for(source.cleaning.wait(), 2)
                        self.assertFalse(task.done())
                        self.assertTrue(self.observer._polling)
                        source.release.set()
                        results = await task
                        self.assertEqual([x.status for x in results], ["timeout", "received", "received"])
                        self.assertEqual([x.batch.status for x in results[1:]], ["baseline", "observed"])
                        self.assertEqual([x.event.event_id for x in results[2].batch.observations], ["new"])
                        self.assertEqual((source.polls, source.reconnects), (3, 1))
                        self.assertEqual(self.clock.delays, [1, 1])
                        self.assertEqual(self.observer.seen_count, 2)
                        self.assertFalse(self.observer._polling)
                        self.assertFalse(source.worker._log_traceback)
                        self.assertTrue(all(handle.cancelled() for handle, _ in timers))
                        self.assertNotIn(PRIVATE, repr(results))
                        self.assertEqual(asyncio.all_tasks(), before)
                    finally:
                        source.release.set()
                        task.cancel()
                        await asyncio.gather(task, source.worker, return_exceptions=True)

    async def test_cancellation_before_queued_fetch_never_contacts_source(self):
        source = SyntheticSource(batch(row()))
        before = asyncio.all_tasks()
        create_task = asyncio.create_task

        def cancel_before_start(operation):
            asyncio.current_task().cancel(PRIVATE)
            return create_task(operation)

        with patch("sentinel.events.observer.asyncio.create_task", cancel_before_start):
            try:
                await self.collect(source)
            except asyncio.CancelledError as error:
                self.assert_safe_cancellation(error, fresh=True)
            else:
                self.fail("cancellation was swallowed")
        self.assertEqual(source.polls, 0)
        self.assertFalse(self.observer._polling)
        self.assertEqual(asyncio.all_tasks(), before)

    async def test_adapter_cancellation_chains_are_sanitized(self):
        async def poll():
            raise asyncio.CancelledError(PRIVATE) from RuntimeError(PRIVATE)

        source = SyntheticSource()
        source.poll = poll
        before = asyncio.all_tasks()
        try:
            await self.collect(source)
        except asyncio.CancelledError as error:
            self.assert_safe_cancellation(error, fresh=True)
        else:
            self.fail("adapter cancellation was swallowed")
        self.assertFalse(self.observer._polling)
        self.assertEqual(asyncio.all_tasks(), before)

    async def test_cancel_racing_failed_poll_retrieves_private_exception(self):
        before = asyncio.all_tasks()
        loop = asyncio.get_running_loop()
        incidents = []
        accepted = []
        old_handler = loop.get_exception_handler()
        loop.set_exception_handler(lambda loop, context: incidents.append(context))

        def cancel():
            accepted.append(task.cancel(PRIVATE))

        class FailedSource(SyntheticSource):
            worker = None

            async def poll(inner):
                inner.worker = asyncio.current_task()
                inner.worker.add_done_callback(lambda _: cancel())
                raise RuntimeError(PRIVATE) from ValueError(PRIVATE)

        source = FailedSource()
        task = asyncio.create_task(self.collect(source, max_attempts=1))
        try:
            try:
                await task
            except asyncio.CancelledError as error:
                self.assert_safe_cancellation(error)
            else:
                self.fail("cancellation was swallowed")
            self.assertEqual(accepted, [True])
            self.assertFalse(source.worker._log_traceback)
            self.assertFalse(self.observer._polling)
            self.assertFalse(self.observer.baseline_ready)
            self.assertEqual(asyncio.all_tasks(), before)
            self.assertEqual(len(incidents), 0)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            source.worker = None
            task = None
            gc.collect()
            await loop_turn()
            loop.set_exception_handler(old_handler)
        self.assertEqual(len(incidents), 0)

    async def test_baseline_new_event_repeats_and_finite_poll_count(self):
        source = SyntheticSource(batch(row("old")), batch(row("old"), row("new")),
                                 batch(row("new"), row("old")), batch())
        results = await self.collect(source)
        self.assertEqual([x.batch.status for x in results], ["baseline", "observed", "observed", "observed"])
        observed = [event.event.event_id for result in results for event in result.batch.observations]
        self.assertEqual(observed, ["new"])
        self.assertEqual(source.polls, 4)
        self.assertEqual(self.clock.delays, [1, 1, 1])
        self.assertFalse(self.observer.baseline_ready)
        self.assertFalse(self.observer._polling)

    async def test_failure_reconnect_rebaseline_then_observe(self):
        source = SyntheticSource(batch(), RuntimeError("synthetic-private-error"),
                                 batch(row("outage-history")), batch(row("new")))
        results = await self.collect(source)
        self.assertEqual([x.status for x in results], ["received", "poll-failed", "received", "received"])
        self.assertEqual(results[2].batch.status, "baseline")
        self.assertEqual([x.event.event_id for x in results[3].batch.observations], ["new"])
        self.assertEqual(source.reconnects, 1)
        self.assertNotIn("synthetic-private-error", repr(results))

    async def test_reconnect_empty_window_cannot_reemit_previously_observed_id(self):
        source = SyntheticSource(batch(), batch(row("observed")), RuntimeError(),
                                 batch(), batch(row("observed")), batch(row("next")))
        results = await self.collect(source, max_attempts=6)
        observed = [x.event.event_id for result in results if result.batch
                    for x in result.batch.observations]
        self.assertEqual(observed, ["observed", "next"])
        self.assertEqual(results[3].batch.status, "baseline")
        self.assertEqual(results[4].batch.observations, ())

    async def test_consecutive_failures_bound_retries_and_backoff(self):
        source = SyntheticSource(*[RuntimeError("synthetic") for _ in range(8)])
        results = await self.collect(source, max_attempts=20, max_consecutive_failures=5)
        self.assertEqual(len(results), 5)
        self.assertEqual(source.polls, 5)
        self.assertEqual(source.reconnects, 4)
        self.assertEqual(self.clock.delays, [1, 2, 4, 4])

    async def test_success_resets_backoff_and_failure_budget(self):
        source = SyntheticSource(RuntimeError(), RuntimeError(), batch(), RuntimeError(), batch())
        results = await self.collect(source, max_attempts=5, max_consecutive_failures=3)
        self.assertEqual(len(results), 5)
        self.assertEqual(self.clock.delays, [1, 2, 1, 1])
        self.assertEqual(source.reconnects, 3)

    async def test_reconnect_failure_uses_same_failure_budget(self):
        source = SyntheticSource(RuntimeError(), batch(), reconnect_errors=[RuntimeError(), RuntimeError()])
        results = await self.collect(source, max_consecutive_failures=3)
        self.assertEqual([x.status for x in results], ["poll-failed"] * 3)
        self.assertEqual(source.polls, 1)
        self.assertEqual(source.reconnects, 2)

    async def test_duration_bounds_sleep_and_prevents_next_poll(self):
        source = SyntheticSource(RuntimeError(), RuntimeError(), batch())
        results = await self.collect(source, max_duration=2.5)
        self.assertEqual(len(results), 2)
        self.assertEqual(self.clock.delays, [1, 1.5])
        self.assertEqual(source.polls, 2)

    async def test_page_arriving_at_deadline_is_not_seeded_or_emitted(self):
        clock = self.clock

        class LateSource(SyntheticSource):
            async def poll(self):
                clock.now += 2
                return batch(row())

        source = LateSource()
        results = await self.collect(source, max_duration=2, request_timeout=10)
        self.assertEqual([x.status for x in results], ["timeout"])
        self.assertEqual(self.observer.seen_count, 0)
        self.assertFalse(self.observer.baseline_ready)

    async def test_request_deadline_applies_even_before_session_deadline(self):
        clock = self.clock

        class LateSource(SyntheticSource):
            async def poll(self):
                clock.now += 2
                return batch(row())

        results = await self.collect(LateSource(), request_timeout=1, max_attempts=1)
        self.assertEqual(results[0].status, "timeout")
        self.assertEqual(self.observer.seen_count, 0)

    async def test_async_timeout_cancels_poll_and_can_recover(self):
        entered = asyncio.Event()

        class HangingOnce(SyntheticSource):
            cancelled = False

            async def poll(self):
                if not self.cancelled:
                    self.active = True
                    entered.set()
                    try:
                        await asyncio.Future()
                    finally:
                        self.active = False
                        self.cancelled = True
                return await super().poll()

        source = HangingOnce(batch(), batch(row("new")))
        before = asyncio.all_tasks()
        with manual_request_timers() as timers:
            task = asyncio.create_task(self.collect(source, max_attempts=3))
            try:
                await asyncio.wait_for(entered.wait(), 2)
                self.assertEqual(len(timers), 1)
                self.assertTrue(source.active)
                # Expire only the hanging attempt, after it owns the poll.
                # Recovery must not depend on completing within 20 real ms.
                timers[0][1]()
                results = await task
                self.assertEqual([x.status for x in results], ["timeout", "received", "received"])
                self.assertEqual(results[1].batch.status, "baseline")
                self.assertEqual(len(results[2].batch.observations), 1)
                self.assertTrue(source.cancelled)
                self.assertFalse(source.active)
                self.assertEqual(source.reconnects, 1)
                self.assertEqual(len(timers), 3)
                self.assertTrue(all(handle.cancelled() for handle, _ in timers))
                self.assertEqual(asyncio.all_tasks(), before)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    async def test_reconnect_is_covered_by_request_timeout(self):
        entered = asyncio.Event()

        class HangingReconnect(SyntheticSource):
            cancelled = False

            async def reconnect(self):
                entered.set()
                try:
                    await asyncio.Future()
                finally:
                    self.cancelled = True

        source = HangingReconnect(RuntimeError())
        before = asyncio.all_tasks()
        with manual_request_timers() as timers:
            task = asyncio.create_task(self.collect(source, max_attempts=2))
            try:
                await asyncio.wait_for(entered.wait(), 2)
                self.assertEqual(len(timers), 2)
                # The second attempt's single budget must cancel reconnect.
                timers[1][1]()
                results = await task
                self.assertEqual([x.status for x in results], ["poll-failed", "timeout"])
                self.assertTrue(source.cancelled)
                self.assertEqual(source.polls, 1)
                self.assertTrue(all(handle.cancelled() for handle, _ in timers))
                self.assertEqual(asyncio.all_tasks(), before)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    async def test_cancellation_during_poll_propagates_and_leaves_no_worker(self):
        entered = asyncio.Event()

        class WaitingSource(SyntheticSource):
            async def poll(self):
                self.active = True
                entered.set()
                try:
                    await asyncio.Future()
                finally:
                    self.active = False

        source = WaitingSource()
        task = asyncio.create_task(self.collect(source))
        await asyncio.wait_for(entered.wait(), 1)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertFalse(source.active)
        self.assertFalse(self.observer._polling)
        self.assertFalse(self.observer.baseline_ready)
        self.assertEqual(self.observer.seen_count, 0)

    async def test_adapter_cancellation_is_not_retried_as_failure(self):
        source = SyntheticSource(asyncio.CancelledError(), batch())
        with self.assertRaises(asyncio.CancelledError):
            await self.collect(source)
        self.assertEqual(source.polls, 1)
        self.assertEqual(source.reconnects, 0)

    async def test_cancellation_during_backoff_preserves_seen_and_releases_owner(self):
        entered = asyncio.Event()

        async def waiting_sleep(delay):
            entered.set()
            await asyncio.Future()

        source = SyntheticSource(batch(row("old")), RuntimeError())

        async def consume():
            return [x async for x in poll_motion(source, self.observer, self.policy,
                                                wall_clock=lambda: NOW, sleep=waiting_sleep)]

        task = asyncio.create_task(consume())
        await asyncio.wait_for(entered.wait(), 1)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(self.observer.seen_count, 1)
        self.assertFalse(self.observer._polling)
        self.assertFalse(self.observer.baseline_ready)

    async def test_malformed_window_does_not_poison_next_clean_baseline(self):
        source = SyntheticSource(batch(row("a"), {}), batch(row("a")),
                                 batch(row("a"), row("b")), batch(row("b")))
        results = await self.collect(source)
        self.assertEqual([x.batch.status for x in results], ["rejected", "baseline", "observed", "observed"])
        self.assertEqual([x.event.event_id for x in results[2].batch.observations], ["b"])
        self.assertEqual(source.reconnects, 0)

    async def test_invalid_top_level_results_are_bounded_and_recoverable(self):
        source = SyntheticSource([], None, batch(), batch(row()))
        results = await self.collect(source)
        self.assertEqual([x.batch.status for x in results], ["rejected", "rejected", "baseline", "observed"])

    async def test_timezone_failure_rejects_whole_window_and_polling_recovers(self):
        source = SyntheticSource(
            batch(row("history")),
            batch(row("valid-prefix"), {**row("bad"), "reported_at": broken_timestamp()}),
            batch(row("valid-prefix"), row("bad")),
            batch(row("history"), row("bad"), row("next")),
        )
        results = await self.collect(source)
        self.assertEqual([x.batch.status for x in results],
                         ["baseline", "rejected", "baseline", "observed"])
        self.assertEqual(results[1].batch.rejected_rows, 1)
        self.assertEqual(results[1].batch.observations, ())
        self.assertEqual(results[2].batch.historical_ids, 2)
        self.assertEqual([x.event.event_id for x in results[3].batch.observations], ["next"])
        self.assertEqual(source.reconnects, 0)
        self.assertEqual(self.observer.seen_count, 4)
        self.assertNotIn("synthetic-private-timezone-detail", repr(results))

    async def test_capacity_stops_polling_without_sleep_or_retry(self):
        self.observer = MotionObserver(CAMERA, max_seen=1)
        source = SyntheticSource(batch(row("a")), batch(row("b")), batch(row("c")))
        results = await self.collect(source)
        self.assertEqual([x.batch.status for x in results], ["baseline", "capacity"])
        self.assertEqual(source.polls, 2)
        self.assertEqual(self.clock.delays, [1])
        self.assertEqual(self.observer.seen_count, 1)

    async def test_saturated_restore_does_not_contact_source(self):
        self.observer = MotionObserver(CAMERA, max_seen=1)
        self.observer.observe(batch(row("a"), row("b")), received_at=NOW)
        self.observer = MotionObserver.restore(self.observer.checkpoint(), CAMERA)
        source = SyntheticSource()
        self.assertEqual(await self.collect(source), [])
        self.assertEqual(source.polls, 0)

    async def test_early_close_fences_restart_without_replaying_old_event(self):
        source = SyntheticSource(batch(), batch(row("old")))
        stream = self.stream(source)
        await stream.__anext__()
        result = await stream.__anext__()
        self.assertEqual(len(result.batch.observations), 1)
        await stream.aclose()
        self.assertFalse(self.observer.baseline_ready)
        self.assertFalse(self.observer._polling)
        recovered = await self.collect(SyntheticSource(batch(), batch(row("old")),
                                                       batch(row("new"))), max_attempts=3)
        self.assertEqual(recovered[1].batch.observations, ())
        self.assertEqual([x.event.event_id for x in recovered[2].batch.observations], ["new"])

    async def test_consumer_backpressure_does_not_launch_background_polls(self):
        source = SyntheticSource(batch(), batch(row()))
        stream = self.stream(source)
        await stream.__anext__()
        for _ in range(3):
            await asyncio.sleep(0)
        self.assertEqual(source.polls, 1)
        self.assertEqual(self.clock.delays, [])
        await stream.aclose()

    async def test_duration_expired_while_consumer_pauses_prevents_next_poll(self):
        source = SyntheticSource(batch(), batch(row()))
        stream = self.stream(source)
        await stream.__anext__()
        self.clock.now += 101
        with self.assertRaises(StopAsyncIteration):
            await stream.__anext__()
        self.assertEqual(source.polls, 1)

    async def test_two_pollers_cannot_share_observer(self):
        source = SyntheticSource(batch())
        first = self.stream(source)
        await first.__anext__()
        second = self.stream(source)
        with self.assertRaisesRegex(RuntimeError, "observer-already-polling"):
            await second.__anext__()
        self.assertTrue(self.observer._polling)
        await first.aclose()
        self.assertFalse(self.observer._polling)

    async def test_clock_failure_during_startup_releases_observer(self):
        def broken_clock():
            raise RuntimeError("synthetic-clock-failure")

        stream = poll_motion(SyntheticSource(), self.observer, clock=broken_clock)
        with self.assertRaisesRegex(RuntimeError, "synthetic-clock-failure"):
            await stream.__anext__()
        self.assertFalse(self.observer._polling)
        self.assertFalse(self.observer.baseline_ready)


if __name__ == "__main__":
    unittest.main()
