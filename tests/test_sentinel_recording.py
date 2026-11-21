"""Offline synthetic lifecycle tests; real disk security runs only on POSIX."""

import asyncio
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timezone, tzinfo
import errno
import gc
import os
from pathlib import Path
import tempfile
import time
import traceback
import unittest
from unittest.mock import patch

from sentinel.events import EventBatch, MotionEvent, MotionObserver, ObservedMotion
from sentinel.recording import (
    CapturePolicy, CaptureRequest, PrivateStore, Recorder, RecordingError, StoragePolicy,
)
from sentinel.recording.storage import FOOTER, HEADER, NAMESPACE, OVERHEAD, _write_all


NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
SECRET = "synthetic-sensitive-marker"


def observation(key="event"):
    return ObservedMotion(MotionEvent("synthetic-camera", key, "motion", None), NOW)


class MemoryWrite:
    """Test-only storage; no security or persistence claims."""

    def __init__(self, store):
        self.store = store
        self.data = bytearray()
        self.done = False

    def write(self, data):
        if self.store.write_error:
            raise OSError(errno.ENOSPC, SECRET)
        self.data.extend(data)

    async def complete(self):
        self.store.finalizing.set()
        if self.store.wait_finalize:
            try:
                await asyncio.Event().wait()
            finally:
                self.store.final_cleanup = True
        if self.store.final_error:
            raise OSError(SECRET)
        self.store.clips.append(bytes(self.data))
        self.done = True
        self.store.active -= 1

    def abort(self):
        if self.done:
            return
        self.done = True
        self.store.active -= 1
        self.data.clear()
        if self.store.abort_error:
            raise OSError(SECRET)


class MemoryStore:
    def __init__(self):
        self.clips = []
        self.active = 0
        self.maximum_active = 0
        self.write_error = self.final_error = self.abort_error = False
        self.wait_finalize = False
        self.final_cleanup = False
        self.finalizing = asyncio.Event()
        self.claimed = False

    def claim(self):
        if self.claimed:
            raise RecordingError("storage-owner-unavailable")
        self.claimed = True

    def begin(self, maximum):
        self.active += 1
        self.maximum_active = max(self.maximum_active, self.active)
        return MemoryWrite(self)


class SyntheticBackend:
    def __init__(self):
        self.calls = 0
        self.requests = []
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.wait = False
        self.cleaned = 0
        self.error_count = 0
        self.data = b"synthetic-clip"
        self.sink = None

    async def capture(self, request, sink):
        self.calls += 1
        self.requests.append(request)
        self.sink = sink
        self.started.set()
        try:
            sink.write(self.data)
            if self.wait:
                await self.release.wait()
            if self.calls <= self.error_count:
                raise RuntimeError(SECRET)
        finally:
            self.cleaned += 1


class PolicyTests(unittest.TestCase):
    def test_invalid_capture_durations(self):
        for field in ("duration", "request_timeout"):
            for value in (0, -1, True, None, "1", float("inf"), float("nan"), 86401):
                with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                    CapturePolicy(**{field: value})

    def test_invalid_capture_integer_limits(self):
        for field in ("max_bytes", "concurrency", "max_seen", "retries"):
            for value in (-1, True, 1.5, None, 10 ** 15):
                with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                    CapturePolicy(**{field: value})
        for field in ("max_bytes", "concurrency", "max_seen"):
            with self.assertRaises(ValueError):
                CapturePolicy(**{field: 0})

    def test_invalid_storage_limits(self):
        for field in ("max_bytes", "max_clips", "max_entries", "retention_seconds"):
            for value in (0, -1, True, None, float("inf"), float("nan")):
                with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                    StoragePolicy(**{field: value})
        with self.assertRaises(ValueError):
            StoragePolicy(max_clips=20, max_entries=10)

    def test_unsupported_platform_fails_before_path_access(self):
        with patch("sentinel.recording.storage.os.name", "nt"):
            with self.assertRaisesRegex(RecordingError, "^unsupported-storage-platform$"):
                PrivateStore(SECRET)

    def test_partial_and_zero_write(self):
        with patch("sentinel.recording.storage.os.write", side_effect=[1, 2]) as write:
            _write_all(-1, b"abc")
            self.assertEqual(write.call_count, 2)
        with patch("sentinel.recording.storage.os.write", return_value=0):
            with self.assertRaisesRegex(RecordingError, "storage-write-failed"):
                _write_all(-1, b"abc")


class LifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.backend = SyntheticBackend()
        self.store = MemoryStore()
        self._recorder = None

    @property
    def recorder(self):
        if self._recorder is None:
            self._recorder = self.make()
        return self._recorder

    def make(self, **limits):
        return Recorder("synthetic-camera", self.backend, self.store, CapturePolicy(**limits))

    def assert_redacted_chain(self, error, *originals):
        markers = (SECRET, "synthetic-private-cause-marker")
        pending = [error]
        visited = set()
        while pending:
            nested = pending.pop()
            if id(nested) in visited:
                continue
            visited.add(id(nested))
            for original in originals:
                self.assertIsNot(nested, original)
            rendered = (str(nested), repr(nested), "".join(traceback.format_exception(
                type(nested), nested, nested.__traceback__)))
            for marker in markers:
                for text in rendered:
                    self.assertNotIn(marker, text)
            pending.extend(link for link in (nested.__cause__, nested.__context__)
                           if link is not None)
        self.assertIsNone(error.__cause__)
        self.assertIsNone(error.__context__)

    def metadata_comparison(self, outcome=None, chained=False):
        calls = {"comparison": 0, "truth": 0}
        private_cause = RuntimeError("synthetic-private-cause-marker")
        private_context = ValueError(SECRET)
        private_error = outcome(SECRET) if outcome is not None else None

        class StatefulTruth:
            def __bool__(self):
                calls["truth"] += 1
                if calls["truth"] == 1:
                    return False
                if private_error is None:
                    return True
                if chained:
                    try:
                        raise private_context
                    except ValueError:
                        raise private_error from private_cause
                raise private_error

        class MalformedMetadata:
            def __eq__(self, other):
                calls["comparison"] += 1
                return StatefulTruth()

        return MalformedMetadata(), calls, (private_error, private_cause, private_context)

    async def assert_metadata_rejected(self, field, value, calls, originals=()):
        self.backend = SyntheticBackend()
        self.store = MemoryStore()
        recorder = self.make()
        item = observation()
        object.__setattr__(item, field, value)
        self.assertIs(type(item), ObservedMotion)
        before = asyncio.all_tasks()
        publications = []
        original_complete = MemoryWrite.complete

        async def complete(transaction):
            publications.append(transaction)
            await original_complete(transaction)

        with patch.object(self.store, "begin", wraps=self.store.begin) as begin, \
                patch.object(MemoryWrite, "complete", complete):
            try:
                result = await recorder.record(item)
            except (Exception, asyncio.CancelledError) as error:
                # Inspect while the traceback and its recursive chains exist.
                self.assert_redacted_chain(error, *originals)
                self.fail("malformed metadata raised instead of rejecting")
            self.assertEqual(result.status, "rejected")
            for marker in (SECRET, "synthetic-private-cause-marker"):
                self.assertNotIn(marker, repr(result))
            self.assertEqual(calls, {"comparison": 0, "truth": 0})
            self.assertIs(recorder._valid(item), False)
            self.assertEqual(calls, {"comparison": 0, "truth": 0})
            begin.assert_not_called()
            self.assertEqual(publications, [])
        self.assertEqual((self.backend.calls, self.backend.sink), (0, None))
        self.assertEqual((self.store.clips, self.store.active, self.store.maximum_active,
                          recorder._active), ([], 0, 0, 0))
        self.assertEqual(recorder._seen, set())
        self.assertFalse(recorder._fenced)
        self.assertEqual(asyncio.all_tasks(), before)
        # Rejected metadata must not consume the stable ID or fence the recorder.
        self.assertEqual((await recorder.record(observation())).status, "completed")
        self.assertEqual((self.backend.calls, self.store.clips, self.store.active,
                          recorder._active), (1, [self.backend.data], 0, 0))
        self.assertEqual(asyncio.all_tasks(), before)

    async def test_metadata_comparison_exceptions_before_admission(self):
        for field in ("history_relation", "freshness"):
            for outcome in (RuntimeError, asyncio.CancelledError):
                for chained in (False, True):
                    with self.subTest(field=field, outcome=outcome, chained=chained):
                        value, calls, originals = self.metadata_comparison(outcome, chained)
                        await self.assert_metadata_rejected(field, value, calls, originals)

    async def test_metadata_comparison_truth_flip_before_admission(self):
        for field in ("history_relation", "freshness"):
            with self.subTest(field=field):
                value, calls, _ = self.metadata_comparison()
                await self.assert_metadata_rejected(field, value, calls)

    async def test_metadata_string_subclasses_before_admission(self):
        for field, expected in (("history_relation", "unseen-after-baseline"),
                                ("freshness", "unverified")):
            with self.subTest(field=field):
                calls = {"comparison": 0, "truth": 0}

                class MetadataString(str):
                    def __eq__(self, other):
                        calls["comparison"] += 1
                        return True

                await self.assert_metadata_rejected(field, MetadataString(expected), calls)

    async def test_observation_validation_returns_builtin_bool(self):
        recorder = self.recorder
        self.assertIs(recorder._valid(observation()), True)
        for item in (None, object(), observation().event,
                     replace(observation(), event=None),
                     replace(observation(), received_at=None),
                     observation("different-event")):
            with self.subTest(item_type=type(item)):
                self.assertIs(type(recorder._valid(item)), bool)
        for field in ("history_relation", "freshness", "physical_motion_at",
                      "latency_seconds"):
            for value in (None, False, 1, [], {}, "invalid"):
                with self.subTest(field=field, value_type=type(value)):
                    item = observation()
                    object.__setattr__(item, field, value)
                    self.assertIs(type(recorder._valid(item)), bool)

    async def test_observation_validation_camera_identity_snapshot(self):
        for outcome in (None, RuntimeError, asyncio.CancelledError):
            with self.subTest(outcome=outcome):
                self.backend = SyntheticBackend()
                self.store = MemoryStore()
                recorder = self.make()
                value, calls, originals = self.metadata_comparison(outcome, chained=True)

                def with_mutating_timezone():
                    item = observation()

                    class MutatingTimezone(tzinfo):
                        def utcoffset(self, timestamp):
                            object.__setattr__(item.event, "camera_id", value)
                            return timezone.utc.utcoffset(timestamp)

                    return replace(item, received_at=NOW.replace(tzinfo=MutatingTimezone()))

                # Validate the same exact primitive that is compared, even if a
                # later timestamp callback alters the caller's original event.
                self.assertIs(recorder._valid(with_mutating_timezone()), True)
                self.assertEqual(calls, {"comparison": 0, "truth": 0})
                before = asyncio.all_tasks()
                try:
                    result = await recorder.record(with_mutating_timezone())
                except (Exception, asyncio.CancelledError) as error:
                    self.assert_redacted_chain(error, *originals)
                    self.fail("timestamp mutation escaped validation")
                self.assertEqual(result.status, "completed")
                self.assertEqual(calls, {"comparison": 0, "truth": 0})
                self.assertEqual((self.backend.calls, self.store.clips, self.store.active,
                                  recorder._active), (1, [self.backend.data], 0, 0))
                self.assertEqual(asyncio.all_tasks(), before)

    def event_id_timezone_mutation(self, field, replacement_kind):
        item = observation("stable-original")
        calls = dict(timezone=0, hash=0, equality=0, string=0, repr=0)
        private_cause = RuntimeError("synthetic-private-cause-marker")
        private_context = ValueError(SECRET)
        outcome = (asyncio.CancelledError if replacement_kind == "cancel" else RuntimeError)
        private_error = outcome(SECRET)

        def private_failure():
            try:
                raise private_context
            except ValueError:
                raise private_error from private_cause

        class PrivateIdentity:
            def __hash__(self):
                calls["hash"] += 1
                private_failure()

            def __eq__(self, other):
                calls["equality"] += 1
                private_failure()

            def __str__(self):
                calls["string"] += 1
                private_failure()

            def __repr__(self):
                calls["repr"] += 1
                private_failure()

        replacement = (None if replacement_kind == "none" else
                       "stable-replacement" if replacement_kind == "string" else
                       PrivateIdentity())

        class MutatingTimezone(tzinfo):
            def utcoffset(self, timestamp):
                calls["timezone"] += 1
                object.__setattr__(item.event, "event_id", replacement)
                return timezone.utc.utcoffset(timestamp)

        timestamp = NOW.replace(tzinfo=MutatingTimezone())
        # Avoid invoking timezone callbacks during fixture construction.
        object.__setattr__(item if field == "received_at" else item.event, field, timestamp)
        return item, calls, (private_error, private_cause, private_context)

    async def record_identity_mutation(self, recorder, field, replacement_kind):
        item, calls, originals = self.event_id_timezone_mutation(field, replacement_kind)
        try:
            result = await recorder.record(item)
        except (Exception, asyncio.CancelledError) as error:
            self.assert_redacted_chain(error, *originals)
            self.fail("event-ID mutation raised instead of using the validated ID")
        for marker in (SECRET, "synthetic-private-cause-marker"):
            for rendered in (str(result), repr(result)):
                self.assertNotIn(marker, rendered)
        # utc_time checks awareness and converts once; no second validation pass.
        self.assertEqual(calls, dict(timezone=2, hash=0, equality=0, string=0, repr=0))
        return result

    async def test_event_id_snapshot_duplicate_after_timezone_mutation(self):
        for field in ("received_at", "reported_at"):
            for replacement_kind in ("none", "string", "error", "cancel"):
                with self.subTest(field=field, replacement=replacement_kind):
                    self.backend = SyntheticBackend()
                    self.store = MemoryStore()
                    recorder = self.make(max_seen=2)
                    self.assertEqual((await recorder.record(observation("stable-original"))).status,
                                     "completed")
                    before = asyncio.all_tasks()
                    publications = []
                    original_complete = MemoryWrite.complete

                    async def complete(transaction):
                        publications.append(transaction)
                        await original_complete(transaction)

                    for at_capacity in (False, True):
                        with patch.object(self.store, "begin", wraps=self.store.begin) as begin, \
                                patch.object(MemoryWrite, "complete", complete), \
                                patch("asyncio.create_task", wraps=asyncio.create_task) as create_task:
                            result = await self.record_identity_mutation(
                                recorder, field, replacement_kind)
                            self.assertEqual((result.status, result.attempts, result.bytes_written),
                                             ("duplicate", 0, 0))
                            begin.assert_not_called()
                            self.assertEqual(publications, [])
                            create_task.assert_not_called()
                        count = 2 if at_capacity else 1
                        self.assertEqual((self.backend.calls, self.store.clips),
                                         (count, [self.backend.data] * count))
                        self.assertEqual((self.store.active, recorder._active), (0, 0))
                        self.assertEqual(self.store.maximum_active, 1)
                        self.assertEqual(recorder._seen, {"stable-original", "legitimate-new"}
                                         if at_capacity else {"stable-original"})
                        self.assertTrue(all(type(key) is str for key in recorder._seen))
                        self.assertFalse(recorder._fenced)
                        self.assertEqual(asyncio.all_tasks(), before)
                        if not at_capacity:
                            self.assertEqual((await recorder.record(
                                observation("legitimate-new"))).status, "completed")

    async def test_event_id_snapshot_new_admission_after_timezone_mutation(self):
        for field in ("received_at", "reported_at"):
            for replacement_kind in ("none", "string", "error", "cancel"):
                with self.subTest(field=field, replacement=replacement_kind):
                    self.backend = SyntheticBackend()
                    self.store = MemoryStore()
                    recorder = self.make(max_seen=1)
                    before = asyncio.all_tasks()
                    with patch.object(self.store, "begin", wraps=self.store.begin) as begin:
                        result = await self.record_identity_mutation(recorder, field, replacement_kind)
                        self.assertEqual((result.status, result.attempts, result.bytes_written),
                                         ("completed", 1, len(self.backend.data)))
                        begin.assert_called_once_with(recorder._policy.max_bytes)
                        self.assertEqual(recorder._seen, {"stable-original"})
                        self.assertTrue(all(type(key) is str for key in recorder._seen))
                        self.assertEqual((await recorder.record(
                            observation("stable-original"))).status, "duplicate")
                        self.assertEqual((await recorder.record(
                            observation("stable-replacement"))).status, "capacity")
                        self.assertEqual(begin.call_count, 1)
                    self.assertEqual((self.backend.calls, self.store.clips), (1, [self.backend.data]))
                    self.assertEqual((self.store.active, recorder._active), (0, 0))
                    self.assertEqual(self.store.maximum_active, 1)
                    self.assertFalse(recorder._fenced)
                    self.assertEqual(asyncio.all_tasks(), before)

    async def test_invalid_event_id_rejected_before_timezone_repair(self):
        for field in ("received_at", "reported_at"):
            for invalid in (None, "", "invalid id", object()):
                with self.subTest(field=field, invalid_type=type(invalid)):
                    self.backend = SyntheticBackend()
                    self.store = MemoryStore()
                    recorder = self.make()
                    item, calls, _ = self.event_id_timezone_mutation(field, "string")
                    object.__setattr__(item.event, "event_id", invalid)
                    before = asyncio.all_tasks()
                    with patch.object(self.store, "begin", wraps=self.store.begin) as begin, \
                            patch.object(MemoryWrite, "complete") as complete, \
                            patch("asyncio.create_task", wraps=asyncio.create_task) as create_task:
                        self.assertEqual((await recorder.record(item)).status, "rejected")
                        self.assertIs(recorder._valid(item), False)
                        begin.assert_not_called()
                        complete.assert_not_called()
                        create_task.assert_not_called()
                    self.assertEqual(calls, dict(timezone=0, hash=0, equality=0, string=0, repr=0))
                    self.assertEqual((self.backend.calls, self.backend.sink), (0, None))
                    self.assertEqual((self.store.clips, self.store.active,
                                      self.store.maximum_active, recorder._active), ([], 0, 0, 0))
                    self.assertEqual(recorder._seen, set())
                    self.assertFalse(recorder._fenced)
                    self.assertEqual(asyncio.all_tasks(), before)
                    self.assertEqual((await recorder.record(observation("stable-original"))).status,
                                     "completed")

    async def test_observation_timezone_exceptions_before_admission(self):
        for outcome in (asyncio.CancelledError, RuntimeError):
            for field in ("received_at", "reported_at"):
                for fail_on in (1, 2):
                    with self.subTest(outcome=outcome, field=field, fail_on=fail_on):
                        self.backend = SyntheticBackend()
                        self.store = MemoryStore()
                        recorder = self.make()
                        private_cause = RuntimeError("synthetic-private-cause-marker")
                        private_error = outcome(SECRET)

                        class PrivateTimezone(tzinfo):
                            calls = 0

                            def utcoffset(self, value):
                                self.calls += 1
                                if self.calls == fail_on:
                                    raise private_error from private_cause
                                return timezone.utc.utcoffset(value)

                        timestamp = NOW.replace(tzinfo=PrivateTimezone())
                        item = observation()
                        if field == "received_at":
                            item = replace(item, received_at=timestamp)
                        else:
                            # MotionEvent normalizes on construction; revalidation
                            # must still defend an exact, subsequently altered event.
                            object.__setattr__(item.event, field, timestamp)
                        self.assertIs(type(item), ObservedMotion)
                        self.assertIs(type(timestamp), datetime)
                        before = asyncio.all_tasks()
                        with patch.object(self.store, "begin", wraps=self.store.begin) as begin:
                            try:
                                result = await recorder.record(item)
                            except asyncio.CancelledError as error:
                                self.assertIs(outcome, asyncio.CancelledError)
                                self.assertIs(type(error), asyncio.CancelledError)
                                self.assertEqual(str(error), "")
                                self.assert_redacted_chain(error, private_error, private_cause)
                            else:
                                self.assertIs(outcome, RuntimeError)
                                self.assertEqual(result.status, "rejected")
                                self.assertNotIn(SECRET, repr(result))
                            begin.assert_not_called()
                        self.assertEqual(timestamp.tzinfo.calls, fail_on)
                        self.assertEqual((self.backend.calls, self.backend.sink), (0, None))
                        self.assertEqual((self.store.clips, self.store.active,
                                          self.store.maximum_active, recorder._active), ([], 0, 0, 0))
                        self.assertEqual(recorder._seen, set())
                        self.assertFalse(recorder._fenced)
                        self.assertEqual(asyncio.all_tasks(), before)
                        # Rejection/cancellation cannot consume this stable ID.
                        self.assertEqual((await recorder.record(observation())).status, "completed")

    async def test_constructor_claim_failure_chain_is_redacted(self):
        private_cause = RuntimeError("synthetic-private-cause-marker")
        private_error = OSError(SECRET)

        def private_claim():
            raise private_error from private_cause

        before = asyncio.all_tasks()
        with patch.object(self.store, "claim", private_claim), \
                patch.object(self.store, "begin", wraps=self.store.begin) as begin:
            try:
                self.make()
            except RecordingError as error:
                self.assertEqual(str(error), "storage-owner-unavailable")
                self.assert_redacted_chain(error, private_error, private_cause)
            else:
                self.fail("storage claim failure did not propagate")
            begin.assert_not_called()
        self.assertFalse(self.store.claimed)
        self.assertEqual((self.backend.calls, self.store.active, self.store.clips), (0, 0, []))
        self.assertEqual(asyncio.all_tasks(), before)
        self.make()

    async def test_swallowed_write_cancellation_never_publishes(self):
        private_cause = RuntimeError("synthetic-private-cause-marker")
        private_error = asyncio.CancelledError(SECRET)

        def private_write(transaction, data):
            raise private_error from private_cause

        async def capture(request, sink):
            self.backend.calls += 1
            sink.write(b"synthetic-prefix")
            with patch.object(MemoryWrite, "write", private_write):
                try:
                    sink.write(b"synthetic-suffix")
                except asyncio.CancelledError:
                    pass  # A broken backend cannot turn this into a valid clip.

        before = asyncio.all_tasks()
        self.backend.capture = capture
        result = await self.recorder.record(observation())
        self.assertEqual((result.status, result.attempts), ("failed", 1))
        self.assertEqual((self.backend.calls, self.store.active, self.store.clips), (1, 0, []))
        self.assertFalse(self.store.finalizing.is_set())
        self.assertEqual(self.recorder._active, 0)
        self.assertEqual(asyncio.all_tasks(), before)
        self.assertEqual((await self.recorder.record(observation())).status, "duplicate")

    async def test_sink_write_exception_chains_are_redacted(self):
        for outcome in (RuntimeError, asyncio.CancelledError):
            with self.subTest(outcome=outcome):
                self.backend = SyntheticBackend()
                self.store = MemoryStore()
                recorder = self.make()
                private_cause = RuntimeError("synthetic-private-cause-marker")
                private_error = outcome(SECRET)
                sink_errors = []

                def private_write(transaction, data):
                    raise private_error from private_cause

                async def capture(request, sink):
                    self.backend.calls += 1
                    try:
                        sink.write(b"synthetic")
                    except (RuntimeError, RecordingError, asyncio.CancelledError) as error:
                        sink_errors.append(error)
                        raise

                self.backend.capture = capture
                before = asyncio.all_tasks()
                with patch.object(MemoryWrite, "write", private_write):
                    try:
                        result = await recorder.record(observation())
                    except asyncio.CancelledError as error:
                        self.assertIs(outcome, asyncio.CancelledError)
                        self.assert_redacted_chain(error, private_error, private_cause)
                    else:
                        self.assertIs(outcome, RuntimeError)
                        self.assertEqual(result.status, "failed")
                self.assertEqual(len(sink_errors), 1)
                self.assertIs(type(sink_errors[0]), asyncio.CancelledError
                              if outcome is asyncio.CancelledError else RecordingError)
                self.assert_redacted_chain(sink_errors[0], private_error, private_cause)
                self.assertEqual((self.backend.calls, self.store.clips, self.store.active,
                                  recorder._active), (1, [], 0, 0))
                self.assertFalse(self.store.finalizing.is_set())
                self.assertEqual(asyncio.all_tasks(), before)

    async def test_record_callback_exception_boundaries(self):
        for phase in ("begin", "capture-call", "capture-await", "complete-call",
                      "complete-await", "abort"):
            for outcome in (RuntimeError, asyncio.CancelledError):
                with self.subTest(phase=phase, outcome=outcome):
                    self.backend = SyntheticBackend()
                    self.store = MemoryStore()
                    recorder = self.make(retries=0)
                    private_cause = RuntimeError("synthetic-private-cause-marker")
                    private_error = outcome(SECRET)
                    abort = MemoryWrite.abort
                    calls = []

                    def private_call(*args):
                        calls.append(phase)
                        if phase == "abort":
                            abort(*args)  # A cooperative store releases its reservation.
                        raise private_error from private_cause

                    async def private_await(*args):
                        private_call(*args)

                    if phase == "begin":
                        target, method = self.store, "begin"
                    elif phase.startswith("capture"):
                        target, method = self.backend, "capture"
                    elif phase.startswith("complete"):
                        target, method = MemoryWrite, "complete"
                    else:
                        target, method = MemoryWrite, "abort"
                        self.backend.error_count = 1
                    callback = private_await if phase.endswith("await") else private_call
                    expected = ("finalization-uncertain" if phase.startswith("complete")
                                else "cleanup-failed" if phase == "abort" else "failed")
                    before = asyncio.all_tasks()
                    with patch.object(target, method, callback):
                        try:
                            result = await recorder.record(observation())
                        except asyncio.CancelledError as error:
                            self.assertIs(outcome, asyncio.CancelledError)
                            self.assertIs(type(error), asyncio.CancelledError)
                            self.assertEqual(str(error), "")
                            self.assert_redacted_chain(error, private_error, private_cause)
                        else:
                            self.assertIs(outcome, RuntimeError)
                            self.assertEqual((result.status, result.attempts), (expected, 1))
                            self.assertNotIn(SECRET, repr(result))
                    self.assertEqual(calls, [phase])
                    self.assertEqual((self.store.clips, self.store.active, recorder._active),
                                     ([], 0, 0))
                    self.assertEqual(asyncio.all_tasks(), before)
                    self.assertEqual(self.backend.calls,
                                     1 if phase.startswith("complete") or phase == "abort" else 0)
                    self.assertEqual(recorder._fenced,
                                     phase.startswith("complete") or phase == "abort")
                    status = expected if recorder._fenced else "duplicate"
                    self.assertEqual((await recorder.record(observation())).status, status)

    @contextmanager
    def manual_deadlines(self):
        """Control phase expiry without spending capture's 20 ms on scheduling."""
        loop = asyncio.get_running_loop()
        timers = []
        with patch.object(loop, "time", return_value=loop.time()) as clock:
            def call_later(delay, callback, *args, **kwargs):
                self.assertEqual(delay, 0.02)
                handle = asyncio.Handle(callback, args, loop, **kwargs)
                timers.append((handle, lambda: callback(*args)))
                return handle

            with patch.object(loop, "call_later", call_later):
                yield clock, timers

    async def expire_finalization(self, recorder):
        before = asyncio.all_tasks()
        with self.manual_deadlines() as (clock, timers):
            task = asyncio.create_task(recorder.record(observation()))
            entered = asyncio.create_task(self.store.finalizing.wait())
            try:
                # Fail promptly if recording ends without entering finalization.
                await asyncio.wait((task, entered), return_when=asyncio.FIRST_COMPLETED)
                self.assertTrue(self.store.finalizing.is_set())
                self.assertEqual(len(timers), 2)
                self.assertTrue(timers[0][0].cancelled())  # Capture is over.
                self.assertFalse(timers[1][0].cancelled())
                clock.return_value += 0.02
                timers[1][1]()  # Expire the real _bounded completion signal.
                result = await task
                self.assertTrue(timers[1][0].cancelled())
            finally:
                for owned in (task, entered):
                    if not owned.done():
                        owned.cancel()
                await asyncio.gather(task, entered, return_exceptions=True)
        self.assertEqual(asyncio.all_tasks(), before)
        return result

    async def test_observer_to_completed_clip_and_historical_exclusion(self):
        observer = MotionObserver("synthetic-camera")
        def row(key):
            return dict(camera_id="synthetic-camera", event_id=key, kind="motion", reported_at=NOW)
        baseline = observer.observe(EventBatch((row("old"),), True), received_at=NOW)
        self.assertEqual(baseline.observations, ())
        new = observer.observe(EventBatch((row("old"), row("new")), True), received_at=NOW)
        result = await self.recorder.record(new.observations[0])
        self.assertEqual((result.status, result.attempts, result.bytes_written),
                         ("completed", 1, len(b"synthetic-clip")))
        self.assertEqual(self.store.clips, [b"synthetic-clip"])
        self.assertEqual(observer.observe(EventBatch((row("new"),), True), received_at=NOW).observations, ())

    async def test_duplicate_after_completion(self):
        await self.recorder.record(observation())
        self.assertEqual((await self.recorder.record(observation())).status, "duplicate")
        self.assertEqual(self.backend.calls, 1)

    async def test_raw_events_and_batches_rejected(self):
        for value in ({}, observation().event, EventBatch((), True), None):
            self.assertEqual((await self.recorder.record(value)).status, "rejected")
        self.assertEqual(self.backend.calls, 0)

    async def test_malformed_observations_rejected(self):
        for item in (ObservedMotion(None, NOW), replace(observation(), received_at=NOW.replace(tzinfo=None)),
                     replace(observation(), event=MotionEvent("other", "x", "motion", None))):
            self.assertEqual((await self.recorder.record(item)).status, "rejected")
        for field, value in (("history_relation", "baseline"), ("freshness", "fresh"),
                             ("physical_motion_at", NOW), ("latency_seconds", 0)):
            item = observation()
            object.__setattr__(item, field, value)
            self.assertEqual((await self.recorder.record(item)).status, "rejected")

    async def test_old_provider_timestamp_does_not_determine_freshness(self):
        item = replace(observation(), event=MotionEvent("synthetic-camera", "x", "motion", NOW))
        self.assertEqual((await self.recorder.record(item)).status, "completed")

    async def test_duplicate_in_flight_and_busy_backpressure(self):
        self.backend.wait = True
        task = asyncio.create_task(self.recorder.record(observation()))
        await self.backend.started.wait()
        self.assertEqual((await self.recorder.record(observation())).status, "duplicate")
        self.assertEqual((await self.recorder.record(observation("next"))).status, "busy")
        self.backend.release.set()
        await task
        self.assertEqual((await self.recorder.record(observation("next"))).status, "completed")
        self.assertEqual(self.store.maximum_active, 1)

    async def test_concurrency_two_has_no_queue(self):
        recorder = self.make(concurrency=2)
        self.backend.wait = True
        first = asyncio.create_task(recorder.record(observation("1")))
        second = asyncio.create_task(recorder.record(observation("2")))
        while self.backend.calls < 2:
            await asyncio.sleep(0)
        self.assertEqual((await recorder.record(observation("3"))).status, "busy")
        self.assertEqual(self.store.maximum_active, 2)
        self.backend.release.set()
        await asyncio.gather(first, second)

    async def test_seen_capacity_never_evicts(self):
        recorder = self.make(max_seen=1)
        await recorder.record(observation())
        self.assertEqual((await recorder.record(observation("other"))).status, "capacity")
        self.assertEqual((await recorder.record(observation())).status, "duplicate")

    async def test_finite_retry_exhaustion(self):
        self.backend.error_count = 100
        result = await self.make(retries=2).record(observation())
        self.assertEqual((result.status, result.attempts), ("failed", 3))
        self.assertEqual(self.backend.calls, 3)
        self.assertEqual((self.store.clips, self.store.active), ([], 0))

    async def test_retry_can_succeed_without_partial_publication(self):
        self.backend.error_count = 1
        result = await self.make(retries=2).record(observation())
        self.assertEqual((result.status, result.attempts), ("completed", 2))
        self.assertEqual(self.store.clips, [b"synthetic-clip"])

    async def test_capture_duration_limit(self):
        self.backend.wait = True
        result = await self.make(duration=0.02, request_timeout=1).record(observation())
        self.assertEqual(result.status, "failed")
        self.assertEqual(self.backend.cleaned, 1)
        self.assertEqual(self.store.active, 0)

    async def test_request_timeout_limit(self):
        self.backend.wait = True
        result = await self.make(duration=1, request_timeout=0.02).record(observation())
        self.assertEqual(result.status, "failed")
        self.assertEqual(self.backend.cleaned, 1)
        self.assertEqual(self.store.clips, [])

    async def test_exact_byte_limit_and_request(self):
        recorder = self.make(max_bytes=len(self.backend.data))
        self.assertEqual((await recorder.record(observation())).status, "completed")
        self.assertEqual(self.backend.requests, [CaptureRequest(10, len(self.backend.data))])

    async def test_byte_overflow_aborts(self):
        self.assertEqual((await self.make(max_bytes=2).record(observation())).status, "failed")
        self.assertEqual((self.store.clips, self.store.active), ([], 0))

    async def test_swallowed_byte_error_still_fails(self):
        async def capture(request, sink):
            sink.write(b"a")
            try:
                sink.write(b"too-much")
            except RecordingError:
                pass
        self.backend.capture = capture
        self.assertEqual((await self.make(max_bytes=2).record(observation())).status, "failed")
        self.assertEqual(self.store.clips, [])

    async def test_nonbytes_rejected(self):
        self.backend.data = bytearray(b"synthetic")
        self.assertEqual((await self.recorder.record(observation())).status, "failed")

    async def test_empty_capture_is_not_completed(self):
        self.backend.data = b""
        self.assertEqual((await self.recorder.record(observation())).status, "failed")
        self.assertEqual(self.store.clips, [])

    async def test_swallowed_timeout_is_rejected(self):
        async def capture(request, sink):
            sink.write(b"synthetic")
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                return
        self.backend.capture = capture
        self.assertEqual((await self.make(duration=0.02).record(observation())).status, "failed")
        self.assertEqual(self.store.clips, [])

    async def test_cancel_capture_cleans_and_remembers_id(self):
        self.backend.wait = True
        task = asyncio.create_task(self.recorder.record(observation()))
        await self.backend.started.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual((self.backend.cleaned, self.store.active, self.recorder._active), (1, 0, 0))
        self.assertEqual((await self.recorder.record(observation())).status, "duplicate")

    async def test_cancel_finalization_cleans(self):
        self.store.wait_finalize = True
        before = asyncio.all_tasks()
        task = asyncio.create_task(self.recorder.record(observation()))
        await self.store.finalizing.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(self.store.final_cleanup)
        self.assertEqual((self.store.clips, self.store.active), ([], 0))
        self.assertEqual(self.recorder._active, 0)
        self.assertEqual(asyncio.all_tasks(), before)
        self.assertEqual((await self.recorder.record(observation("other"))).status,
                         "finalization-uncertain")
        self.assertEqual(self.backend.calls, 1)

    async def test_capture_deadline_before_finalization_never_publishes(self):
        self.store.wait_finalize = True
        recorder = self.make(request_timeout=0.02)
        capture = self.backend.capture
        before = asyncio.all_tasks()
        with self.manual_deadlines() as (clock, timers):
            async def delayed_delivery(request, sink):
                await capture(request, sink)
                # Capture returned, but its caller cannot resume before expiry.
                clock.return_value += 0.02

            with patch.object(self.backend, "capture", delayed_delivery):
                result = await recorder.record(observation())
            self.assertEqual(len(timers), 1)  # No finalization timer was armed.
            self.assertTrue(timers[0][0].cancelled())
        self.assertEqual((result.status, result.attempts), ("failed", 1))
        self.assertEqual((self.backend.calls, self.backend.cleaned), (1, 1))
        self.assertFalse(self.store.finalizing.is_set())
        self.assertFalse(self.store.final_cleanup)
        self.assertFalse(recorder._fenced)
        self.assertEqual((self.store.clips, self.store.active, recorder._active), ([], 0, 0))
        self.assertEqual((await recorder.record(observation())).status, "duplicate")
        self.assertEqual(asyncio.all_tasks(), before)

    async def test_finalization_timeout_cleans(self):
        self.store.wait_finalize = True
        recorder = self.make(request_timeout=0.02, retries=3)
        result = await self.expire_finalization(recorder)
        self.assertEqual((result.status, result.attempts), ("finalization-uncertain", 1))
        self.assertTrue(self.store.final_cleanup)
        self.assertEqual((self.store.clips, self.store.active, recorder._active), ([], 0, 0))
        self.assertEqual((await recorder.record(observation("other"))).status,
                         "finalization-uncertain")
        self.assertEqual(self.backend.calls, 1)

    async def test_finalization_failure_aborts(self):
        self.store.final_error = True
        self.assertEqual((await self.recorder.record(observation())).status, "finalization-uncertain")
        self.assertEqual((self.store.clips, self.store.active), ([], 0))

    async def test_disk_full_is_redacted_and_aborted(self):
        self.store.write_error = True
        result = await self.recorder.record(observation(SECRET))
        self.assertEqual(result.status, "failed")
        self.assertNotIn(SECRET, repr(result))
        self.assertEqual((self.store.clips, self.store.active), ([], 0))

    async def test_cleanup_failure_fences_future_capture(self):
        self.backend.error_count = 10
        self.store.abort_error = True
        recorder = self.make(retries=3)
        result = await recorder.record(observation())
        self.assertEqual((result.status, result.attempts), ("cleanup-failed", 1))
        self.assertEqual(self.backend.calls, 1)
        self.assertEqual((await recorder.record(observation())).status, "cleanup-failed")

    async def test_cancel_cleanup_failure_fences(self):
        self.backend.wait = True
        self.store.abort_error = True
        task = asyncio.create_task(self.recorder.record(observation()))
        await self.backend.started.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual((await self.recorder.record(observation("x"))).status, "cleanup-failed")

    async def test_retained_sink_closed_after_success_or_failure(self):
        await self.recorder.record(observation())
        with self.assertRaisesRegex(RecordingError, "^capture-bound$"):
            self.backend.sink.write(b"late")
        self.assertEqual(self.store.clips, [b"synthetic-clip"])

    async def test_errors_and_reprs_do_not_expose_private_fields(self):
        recorder = Recorder(SECRET, self.backend, self.store)
        item = ObservedMotion(MotionEvent(SECRET, SECRET, "motion", None), NOW)
        for value in (recorder, item, item.event, self.store):
            self.assertNotIn(SECRET, repr(value))
        await recorder.record(item)
        with self.assertRaises(RecordingError) as error:
            self.backend.sink.write(SECRET.encode())
        self.assertNotIn(SECRET, repr(error.exception))

    async def test_restart_deduplication_is_not_durable(self):
        await self.recorder.record(observation())
        old_clips = self.store.clips
        self.store = MemoryStore()
        await self.make().record(observation())
        self.assertEqual(len(old_clips) + len(self.store.clips), 2)

    async def test_second_recorder_cannot_bypass_shared_store_bounds(self):
        self.recorder
        with self.assertRaisesRegex(RecordingError, "storage-owner-unavailable"):
            self.make()

    async def test_constructor_claim_cancellation_is_redacted_without_ownership(self):
        backend_cause = RuntimeError(SECRET)
        backend_cancel = asyncio.CancelledError(SECRET)

        def private_claim():
            try:
                raise backend_cause
            except RuntimeError as cause:
                raise backend_cancel from cause

        before = asyncio.all_tasks()
        with patch.object(self.store, "claim", side_effect=private_claim) as claim, \
                patch.object(self.store, "begin", wraps=self.store.begin) as begin:
            # Inspect inside except: assertRaises clears traceback data.
            try:
                self.make(retries=2)
            except asyncio.CancelledError as error:
                self.assertIs(type(error), asyncio.CancelledError)
                self.assertEqual(str(error), "")
                self.assertNotIn(SECRET, repr(error))
                self.assertNotIn(SECRET, "".join(traceback.format_exception(
                    type(error), error, error.__traceback__)))
                pending = [error]
                visited = set()
                while pending:
                    nested = pending.pop()
                    if id(nested) in visited:
                        continue
                    visited.add(id(nested))
                    self.assertIsNot(nested, backend_cancel)
                    self.assertIsNot(nested, backend_cause)
                    self.assertNotIn(SECRET, str(nested))
                    self.assertNotIn(SECRET, repr(nested))
                    pending.extend(link for link in (nested.__cause__, nested.__context__)
                                   if link is not None)
                self.assertIsNone(error.__cause__)
                self.assertIsNone(error.__context__)
            else:
                self.fail("storage claim cancellation did not propagate")
            claim.assert_called_once_with()
            begin.assert_not_called()
        self.assertFalse(self.store.claimed)
        self.assertEqual((self.store.clips, self.store.active, self.store.maximum_active),
                         ([], 0, 0))
        self.assertEqual((self.backend.calls, self.backend.requests, self.backend.sink),
                         (0, [], None))
        self.assertEqual(asyncio.all_tasks(), before)
        # Failed construction leaves the store available for its first owner.
        self.make()
        self.assertTrue(self.store.claimed)

    async def test_late_finalization_never_retries(self):
        self.store.wait_finalize = True
        original = MemoryWrite.complete
        async def swallowed(transaction):
            try:
                await original(transaction)
            except asyncio.CancelledError:
                transaction.store.clips.append(bytes(transaction.data))
                transaction.done = True
                transaction.store.active -= 1
        recorder = self.make(request_timeout=0.02, retries=3)
        with patch.object(MemoryWrite, "complete", swallowed):
            result = await self.expire_finalization(recorder)
        self.assertEqual((result.status, result.attempts), ("finalization-uncertain", 1))
        self.assertTrue(self.store.final_cleanup)
        self.assertEqual((self.store.clips, self.store.active, recorder._active),
                         ([b"synthetic-clip"], 0, 0))
        self.assertEqual((await recorder.record(observation("other"))).status, "finalization-uncertain")
        self.assertEqual(self.backend.calls, 1)

    async def test_cancellation_or_failure_after_commit_never_retries(self):
        complete = MemoryWrite.complete
        for outcome in ("cancelled", "failed"):
            with self.subTest(outcome=outcome):
                self.backend = SyntheticBackend()
                self.store = MemoryStore()
                recorder = self.make(retries=3)
                committed = asyncio.Event()

                async def after_commit(transaction):
                    await complete(transaction)
                    committed.set()
                    if outcome == "failed":
                        raise RuntimeError(SECRET)
                    await asyncio.Event().wait()

                before = asyncio.all_tasks()
                with patch.object(MemoryWrite, "complete", after_commit):
                    task = asyncio.create_task(recorder.record(observation()))
                    await committed.wait()
                    if outcome == "cancelled":
                        task.cancel()
                        with self.assertRaises(asyncio.CancelledError):
                            await task
                    else:
                        result = await task
                        self.assertEqual((result.status, result.attempts),
                                         ("finalization-uncertain", 1))
                self.assertEqual((self.store.clips, self.store.active, recorder._active),
                                 ([b"synthetic-clip"], 0, 0))
                self.assertEqual(asyncio.all_tasks(), before)
                self.assertEqual((await recorder.record(observation("other"))).status,
                                 "finalization-uncertain")
                self.assertEqual(self.backend.calls, 1)

    async def test_no_owned_tasks_after_timeout(self):
        before = asyncio.all_tasks()
        self.backend.wait = True
        await self.make(duration=0.02).record(observation())
        self.assertEqual(asyncio.all_tasks(), before)

    async def test_repeated_cancellation_drains_cooperative_cleanup(self):
        cleaning = asyncio.Event()
        finish_cleanup = asyncio.Event()
        async def capture(request, sink):
            self.backend.started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cleaning.set()
                await finish_cleanup.wait()
        self.backend.capture = capture
        before = asyncio.all_tasks()
        task = asyncio.create_task(self.recorder.record(observation()))
        await self.backend.started.wait()
        task.cancel()
        await cleaning.wait()
        task.cancel()
        await asyncio.sleep(0)
        self.assertFalse(task.done())
        finish_cleanup.set()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(self.store.active, 0)
        self.assertEqual(asyncio.all_tasks(), before)

    async def test_caller_cancel_racing_timeout_cleanup_never_retries(self):
        loop = asyncio.get_running_loop()
        real_call_later = loop.call_later
        for outcome in ("cancelled", "returned", "failed"):
            with self.subTest(outcome=outcome):
                self.backend = SyntheticBackend()
                self.store = MemoryStore()
                recorder = self.make(retries=2)
                cleaning = asyncio.Event()
                finish_cleanup = asyncio.Event()
                timers = []

                def manual_timer(delay, callback, *args, **kwargs):
                    if delay == 10:
                        timers.append(lambda: callback(*args))
                        return asyncio.Handle(callback, args, loop)
                    return real_call_later(delay, callback, *args, **kwargs)

                async def capture(request, sink):
                    self.backend.calls += 1
                    self.backend.started.set()
                    sink.write(b"synthetic")
                    if self.backend.calls > 1:
                        return  # A forbidden retry fails assertions, never hangs.
                    try:
                        await asyncio.Event().wait()
                    except asyncio.CancelledError:
                        cleaning.set()
                        await finish_cleanup.wait()
                        if outcome == "returned":
                            return
                        if outcome == "failed":
                            raise RuntimeError(SECRET)
                        raise

                self.backend.capture = capture
                before = asyncio.all_tasks()
                with patch.object(loop, "call_later", manual_timer):
                    task = asyncio.create_task(recorder.record(observation()))
                    await self.backend.started.wait()
                    self.assertEqual(len(timers), 1)
                    timers[0]()  # Explicitly expire capture; no wall-clock race.
                    await cleaning.wait()
                    self.assertEqual((await recorder.record(observation())).status, "duplicate")
                    task.cancel()
                    finish_cleanup.set()  # Child and caller now resume together.
                    with self.assertRaises(asyncio.CancelledError):
                        await task
                self.assertEqual(self.backend.calls, 1)
                self.assertEqual((self.store.clips, self.store.active, recorder._active), ([], 0, 0))
                self.assertEqual(asyncio.all_tasks(), before)
                self.assertEqual((await recorder.record(observation())).status, "duplicate")

    async def test_backend_cancellation_traceback_is_redacted(self):
        for phase in ("capture", "finalization"):
            with self.subTest(phase=phase):
                self.backend = SyntheticBackend()
                self.store = MemoryStore()
                recorder = self.make(retries=2)

                async def private_cancel(*args):
                    if phase == "capture":
                        self.backend.calls += 1
                    try:
                        raise RuntimeError(SECRET)
                    except RuntimeError as cause:
                        raise asyncio.CancelledError(SECRET) from cause

                before = asyncio.all_tasks()
                target, method = ((MemoryWrite, "complete") if phase == "finalization"
                                  else (self.backend, "capture"))
                with patch.object(target, method, private_cancel):
                    # Inspect inside except: assertRaises clears traceback data.
                    try:
                        await recorder.record(observation())
                    except asyncio.CancelledError as error:
                        self.assertEqual(str(error), "")
                        self.assertNotIn(SECRET, repr(error))
                        self.assertNotIn(SECRET, "".join(traceback.format_exception(
                            type(error), error, error.__traceback__)))
                        self.assertIsNone(error.__cause__)
                        self.assertIsNone(error.__context__)
                    else:
                        self.fail("backend cancellation did not propagate")
                self.assertEqual((self.store.clips, self.store.active), ([], 0))
                self.assertEqual(self.backend.calls, 1)
                self.assertEqual(asyncio.all_tasks(), before)
                status = "duplicate" if phase == "capture" else "finalization-uncertain"
                self.assertEqual((await recorder.record(observation())).status, status)

    async def test_cancellation_before_queued_publication_never_commits(self):
        loop = asyncio.get_running_loop()
        create_task = asyncio.create_task
        final_children = []
        recorder = self.make(retries=2)

        def cancel_queued_commit(operation, *args, **kwargs):
            child = create_task(operation, *args, **kwargs)
            if self.backend.cleaned:
                final_children.append(child)
                task.cancel(SECRET)
            return child

        before = asyncio.all_tasks()
        task = create_task(recorder.record(observation()))
        with patch("sentinel.recording.recorder.asyncio.create_task", cancel_queued_commit):
            try:
                await task
            except asyncio.CancelledError as error:
                self.assertNotIn(SECRET, "".join(traceback.format_exception(
                    type(error), error, error.__traceback__)))
            else:
                self.fail("caller cancellation did not propagate")
        self.assertEqual(len(final_children), 1)
        self.assertTrue(final_children[0].done())
        self.assertFalse(self.store.finalizing.is_set())
        self.assertEqual(recorder._active, 0)
        self.assertEqual(self.backend.calls, 1)
        self.assertEqual((self.store.clips, self.store.active), ([], 0))
        self.assertEqual(asyncio.all_tasks(), before)
        self.assertEqual((await recorder.record(observation())).status, "finalization-uncertain")

    async def test_cleanup_cancellation_traceback_is_redacted_and_fenced(self):
        self.backend.error_count = 1
        recorder = self.make(retries=2)
        abort = MemoryWrite.abort

        def private_abort(transaction):
            abort(transaction)
            try:
                raise RuntimeError(SECRET)
            except RuntimeError as cause:
                raise asyncio.CancelledError(SECRET) from cause

        before = asyncio.all_tasks()
        with patch.object(MemoryWrite, "abort", private_abort):
            try:
                await recorder.record(observation())
            except asyncio.CancelledError as error:
                self.assertEqual(str(error), "")
                self.assertNotIn(SECRET, repr(error))
                self.assertNotIn(SECRET, "".join(traceback.format_exception(
                    type(error), error, error.__traceback__)))
                self.assertIsNone(error.__cause__)
                self.assertIsNone(error.__context__)
            else:
                self.fail("cleanup cancellation did not propagate")
        self.assertEqual(self.backend.calls, 1)
        self.assertEqual((self.store.clips, self.store.active), ([], 0))
        self.assertEqual(asyncio.all_tasks(), before)
        self.assertEqual((await recorder.record(observation())).status, "cleanup-failed")

    async def test_failed_admission_keeps_duplicate_fence(self):
        self.backend.error_count = 1
        self.assertEqual((await self.recorder.record(observation())).status, "failed")
        self.assertEqual((await self.recorder.record(observation())).status, "duplicate")
        self.assertEqual(self.backend.calls, 1)

    async def test_reservation_failure_is_redacted_and_bounded(self):
        recorder = self.make(retries=2)
        with patch.object(self.store, "begin", side_effect=OSError(SECRET)) as begin:
            result = await recorder.record(observation())
        self.assertEqual((result.status, result.attempts, begin.call_count), ("failed", 3, 3))
        self.assertNotIn(SECRET, repr(result))
        self.assertEqual(self.backend.calls, 0)

    async def test_cancel_racing_backend_failure_never_logs_exception(self):
        loop = asyncio.get_running_loop()
        old_handler = loop.get_exception_handler()
        incidents = []
        loop.set_exception_handler(lambda loop, context: incidents.append(context))
        async def capture(request, sink):
            task.cancel()
            raise RuntimeError(SECRET)
        self.backend.capture = capture
        try:
            task = asyncio.create_task(self.recorder.record(observation()))
            with self.assertRaises(asyncio.CancelledError):
                await task
            del task
            gc.collect()
            await asyncio.sleep(0)
            self.assertEqual(incidents, [])
            self.assertEqual(self.store.active, 0)
        finally:
            loop.set_exception_handler(old_handler)


@unittest.skipUnless(os.name == "posix", "POSIX descriptor/permission security requires owner-run Linux")
class PrivateStorageTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.root.chmod(0o700)
        self.store = PrivateStore(self.root)
        self.addCleanup(self.store.close)
        self.directory = self.root / NAMESPACE

    async def complete(self, data=b"synthetic", maximum=32):
        transaction = self.store.begin(maximum)
        transaction.write(data)
        await transaction.complete()
        return next(self.directory.glob("*.clip"))

    async def test_real_store_event_to_completed_synthetic_container(self):
        result = await Recorder("synthetic-camera", SyntheticBackend(), self.store).record(observation())
        self.assertEqual(result.status, "completed")
        clip = next(self.directory.glob("*.clip"))
        self.assertEqual(clip.read_bytes(), HEADER + b"synthetic-clip" + FOOTER + (14).to_bytes(8, "big"))
        self.assertEqual(clip.stat().st_mode & 0o777, 0o600)
        self.assertEqual(self.directory.stat().st_mode & 0o777, 0o700)

    async def test_exclusive_lifetime_lock(self):
        with self.assertRaises(RecordingError):
            PrivateStore(self.root)

    async def test_atomic_publication_and_abort_idempotence(self):
        transaction = self.store.begin(32)
        transaction.write(b"synthetic")
        self.assertEqual(list(self.directory.glob("*.clip")), [])
        await transaction.complete()
        transaction.abort()
        self.assertEqual(len(list(self.directory.glob("*.clip"))), 1)
        self.assertEqual(list(self.directory.glob("*.part")), [])

    async def test_exclusive_final_destination_never_replaced(self):
        transaction = self.store.begin(32)
        transaction.write(b"synthetic")
        destination = self.directory / (transaction._name[:-5] + ".clip")
        destination.write_bytes(b"unrelated-synthetic")
        with self.assertRaises(RecordingError):
            await transaction.complete()
        transaction.abort()
        self.assertEqual(destination.read_bytes(), b"unrelated-synthetic")

    async def test_duplicate_random_filename_never_replaces(self):
        transaction = self.store.begin(32)
        from types import SimpleNamespace
        with patch("sentinel.recording.storage.uuid.uuid4", return_value=SimpleNamespace(hex=transaction._name[:-5])):
            with self.assertRaises(RecordingError):
                self.store.begin(32)
        transaction.abort()

    async def test_direct_transaction_overflow_cannot_complete(self):
        transaction = self.store.begin(1)
        with self.assertRaises(RecordingError):
            transaction.write(b"oversized")
        with self.assertRaises(RecordingError):
            await transaction.complete()
        transaction.abort()
        self.assertEqual(list(self.directory.glob("*.clip")), [])

    async def test_empty_transaction_cannot_complete(self):
        transaction = self.store.begin(1)
        with self.assertRaises(RecordingError):
            await transaction.complete()
        transaction.abort()

    async def test_disk_full_after_partial_write(self):
        transaction = self.store.begin(32)
        real_write = os.write
        calls = 0
        def fail(fd, data):
            nonlocal calls
            calls += 1
            if calls == 1:
                return real_write(fd, data[:2])
            raise OSError(errno.ENOSPC, SECRET)
        with patch("sentinel.recording.storage.os.write", side_effect=fail):
            with self.assertRaises(RecordingError) as error:
                transaction.write(b"synthetic")
        self.assertNotIn(SECRET, str(error.exception))
        transaction.abort()
        self.assertEqual(list(self.directory.glob("*.part")), [])

    async def test_header_disk_full_cleanup(self):
        with patch("sentinel.recording.storage.os.write", side_effect=OSError(errno.ENOSPC, SECRET)):
            with self.assertRaises(RecordingError):
                self.store.begin(32)
        self.assertEqual(list(self.directory.glob("*.part")), [])
        self.assertEqual(self.store._active, {})

    async def test_quota_reserves_active_and_preserves_existing(self):
        self.store.close()
        self.store = PrivateStore(self.root, StoragePolicy(max_bytes=2 * (32 + OVERHEAD), max_clips=2))
        self.addCleanup(self.store.close)
        clip = await self.complete()
        original = clip.read_bytes()
        transaction = self.store.begin(32)
        with self.assertRaisesRegex(RecordingError, "storage-quota"):
            self.store.begin(32)
        transaction.abort()
        self.assertEqual(clip.read_bytes(), original)

    async def test_quota_counts_container_overhead(self):
        self.store.close()
        self.store = PrivateStore(self.root, StoragePolicy(max_bytes=OVERHEAD + 1))
        self.addCleanup(self.store.close)
        with self.assertRaisesRegex(RecordingError, "storage-quota"):
            self.store.begin(2)

    async def test_byte_quota_independent_of_count_limit(self):
        self.store.close()
        self.store = PrivateStore(self.root, StoragePolicy(max_bytes=OVERHEAD + 32, max_clips=10))
        self.addCleanup(self.store.close)
        transaction = self.store.begin(32)
        with self.assertRaisesRegex(RecordingError, "storage-quota"):
            self.store.begin(1)
        transaction.abort()
        transaction = self.store.begin(32)
        transaction.abort()

    async def test_clip_count_quota_independent_of_byte_limit(self):
        self.store.close()
        self.store = PrivateStore(self.root, StoragePolicy(max_clips=1))
        self.addCleanup(self.store.close)
        clip = await self.complete()
        with self.assertRaisesRegex(RecordingError, "storage-quota"):
            self.store.begin(1)
        self.assertTrue(clip.exists())

    async def test_finalization_directory_fsync_failure_keeps_valid_clip(self):
        transaction = self.store.begin(32)
        transaction.write(b"synthetic")
        real_fsync = os.fsync
        calls = 0
        def fail_directory(fd):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise OSError(SECRET)
            return real_fsync(fd)
        with patch("sentinel.recording.storage.os.fsync", side_effect=fail_directory):
            with self.assertRaises(RecordingError):
                await transaction.complete()
        with self.assertRaises(RecordingError):
            transaction.abort()
        self.assertEqual(len(list(self.directory.glob("*.clip"))), 1)

    async def test_retention_only_expired_owned_files(self):
        clip = await self.complete()
        unrelated = self.directory / "unrelated.txt"
        unrelated.write_bytes(b"synthetic")
        nested = self.directory / "unrelated-directory"
        nested.mkdir()
        (nested / ("a" * 32 + ".part")).write_bytes(b"synthetic")
        os.utime(clip, (1, 1))
        self.store.recover()
        self.assertFalse(clip.exists())
        self.assertTrue(unrelated.exists())
        self.assertTrue(next(nested.iterdir()).exists())

    async def test_retention_reclaims_entry_capacity_on_first_reservation(self):
        self.store.close()
        self.store = PrivateStore(self.root, StoragePolicy(
            max_bytes=OVERHEAD + 32, max_clips=1, max_entries=3))
        self.addCleanup(self.store.close)
        expired = await self.complete()
        os.utime(expired, (1, 1))
        backend = SyntheticBackend()
        recorder = Recorder("synthetic-camera", backend, self.store,
                            CapturePolicy(max_bytes=32))
        # The first reservation must reclaim both bytes/count and entry space.
        result = await recorder.record(observation())
        self.assertEqual((result.status, result.attempts, backend.calls), ("completed", 1, 1))
        self.assertFalse(expired.exists())
        clips = list(self.directory.glob("*.clip"))
        self.assertEqual(len(clips), 1)
        self.assertEqual(clips[0].read_bytes(), HEADER + backend.data + FOOTER
                         + len(backend.data).to_bytes(8, "big"))
        self.assertEqual(self.store._active, {})
        self.assertEqual(list(self.directory.glob("*.part")), [])
        with self.assertRaisesRegex(RecordingError, "storage-recovery-required"):
            self.store.begin(32)  # Entry limit still reserves publication space.
        for limits in ((1, 4), (2, 4)):
            with self.subTest(max_clips=limits[0]):
                self.store.close()
                self.store = PrivateStore(self.root, StoragePolicy(
                    max_bytes=OVERHEAD + 32, max_clips=limits[0], max_entries=limits[1]))
                self.addCleanup(self.store.close)
                # With room for directory entries, clip count then byte quota
                # independently reject a second full-sized reservation.
                with self.assertRaisesRegex(RecordingError, "storage-quota"):
                    self.store.begin(32)
                self.assertTrue(clips[0].exists())
        (self.directory / "unrelated-synthetic").write_bytes(b"synthetic")
        self.store.recover()
        self.assertTrue((self.directory / "unrelated-synthetic").exists())
        self.assertTrue(clips[0].exists())

    async def test_retention_uncertain_deletion_fences_reservations(self):
        for operation in ("unlink", "fsync"):
            with self.subTest(operation=operation):
                expired = await self.complete()
                os.utime(expired, (1, 1))
                with patch("sentinel.recording.storage.os." + operation,
                           side_effect=OSError(SECRET)):
                    with self.assertRaisesRegex(RecordingError, "storage-write-failed"):
                        self.store.begin(32)
                self.assertEqual(self.store._active, {})
                self.assertEqual(list(self.directory.glob("*.part")), [])
                with self.assertRaisesRegex(RecordingError, "storage-unavailable"):
                    self.store.begin(32)
                self.assertEqual(expired.exists(), operation == "unlink")
                self.store.close()
                self.store = PrivateStore(self.root)
                self.addCleanup(self.store.close)

    async def test_retention_clock_rollback_preserves_future_clip(self):
        clip = await self.complete()
        os.utime(clip, (time.time() + 100, time.time() + 100))
        self.store.recover()
        self.assertTrue(clip.exists())

    async def test_retention_deletes_in_deterministic_age_then_name_order(self):
        names = []
        for _ in range(3):
            transaction = self.store.begin(32)
            transaction.write(b"synthetic")
            await transaction.complete()
            names.append(transaction._name[:-5] + ".clip")
        for name in names:
            os.utime(self.directory / name, ns=(1_000_000_000, 1_000_000_000))
        os.utime(self.directory / names[-1], ns=(0, 0))
        expected = [names[-1]] + sorted(names[:-1])
        with patch("sentinel.recording.storage.os.unlink", wraps=os.unlink) as unlink:
            self.store.recover()
        self.assertEqual([call.args[0] for call in unlink.call_args_list], expected)

    async def test_interrupted_write_recovery_is_idempotent(self):
        part = self.directory / ("a" * 32 + ".part")
        part.write_bytes(HEADER + b"partial-synthetic")
        part.chmod(0o600)
        self.store.recover()
        self.store.recover()
        self.assertFalse(part.exists())
        self.assertEqual(list(self.directory.glob("*.clip")), [])

    async def test_crash_after_link_recovers_one_completed_clip(self):
        clip = await self.complete()
        part = clip.with_suffix(".part")
        os.link(clip, part)
        self.store.recover()
        self.assertTrue(clip.exists())
        self.assertFalse(part.exists())
        self.assertEqual(clip.stat().st_nlink, 1)

    async def test_truncated_header_preserved_and_blocks_recovery(self):
        part = self.directory / ("a" * 32 + ".part")
        part.write_bytes(HEADER[:4])
        part.chmod(0o600)
        with self.assertRaisesRegex(RecordingError, "storage-recovery-failed"):
            self.store.recover()
        self.assertTrue(part.exists())

    async def test_truncated_complete_never_accepted(self):
        clip = await self.complete()
        clip.write_bytes(HEADER + b"partial-synthetic")
        with self.assertRaises(RecordingError):
            self.store.recover()
        self.assertTrue(clip.exists())

    async def test_recovery_permission_failure_fences(self):
        part = self.directory / ("a" * 32 + ".part")
        part.write_bytes(HEADER + b"synthetic")
        part.chmod(0o600)
        with patch("sentinel.recording.storage.os.unlink", side_effect=PermissionError(SECRET)):
            with self.assertRaises(RecordingError) as error:
                self.store.recover()
        self.assertNotIn(SECRET, repr(error.exception))
        self.assertTrue(part.exists())
        with self.assertRaises(RecordingError):
            self.store.begin(32)

    async def test_recovery_and_close_refuse_active_transactions(self):
        transaction = self.store.begin(32)
        for method in (self.store.recover, self.store.close):
            with self.assertRaisesRegex(RecordingError, "storage-busy"):
                method()
        transaction.abort()

    async def test_abort_failure_closes_descriptor_and_fences(self):
        transaction = self.store.begin(32)
        with patch("sentinel.recording.storage.os.unlink", side_effect=PermissionError(SECRET)):
            with self.assertRaisesRegex(RecordingError, "storage-cleanup-failed"):
                transaction.abort()
        self.assertEqual(transaction._fd, -1)
        with self.assertRaises(RecordingError):
            self.store.begin(32)

    async def test_post_link_failure_fences_without_retry_or_deletion(self):
        transaction = self.store.begin(32)
        transaction.write(b"synthetic")
        with patch("sentinel.recording.storage.os.unlink", side_effect=PermissionError(SECRET)):
            with self.assertRaises(RecordingError):
                await transaction.complete()
            with self.assertRaises(RecordingError):
                transaction.abort()
        self.assertEqual(len(list(self.directory.glob("*.clip"))), 1)
        self.store.close()
        self.store = PrivateStore(self.root)
        self.addCleanup(self.store.close)
        self.assertEqual(list(self.directory.glob("*.part")), [])

    async def test_file_fsync_failure_never_publishes(self):
        transaction = self.store.begin(32)
        transaction.write(b"synthetic")
        with patch("sentinel.recording.storage.os.fsync", side_effect=OSError(SECRET)):
            with self.assertRaises(RecordingError):
                await transaction.complete()
        transaction.abort()
        self.assertEqual(list(self.directory.glob("*.clip")), [])

    async def test_owned_symlink_rejected_without_following(self):
        target = self.root / "synthetic-unrelated"
        target.write_bytes(b"synthetic")
        (self.directory / ("a" * 32 + ".part")).symlink_to(target)
        with self.assertRaises(RecordingError):
            self.store.recover()
        self.assertEqual(target.read_bytes(), b"synthetic")

    async def test_unrelated_symlink_is_not_traversed_or_removed(self):
        link = self.directory / "unrelated-link"
        link.symlink_to(self.root / "missing-synthetic")
        self.store.recover()
        self.assertTrue(link.is_symlink())

    async def test_external_hardlink_rejected(self):
        clip = await self.complete()
        external = self.root / "external-synthetic"
        os.link(clip, external)
        with self.assertRaises(RecordingError):
            self.store.recover()
        self.assertTrue(external.exists())

    async def test_managed_fifo_rejected_without_blocking(self):
        os.mkfifo(self.directory / ("a" * 32 + ".part"), 0o600)
        with self.assertRaises(RecordingError):
            self.store.recover()

    async def test_unsafe_roots_relative_traversal_permissions_and_git(self):
        for root in (Path("relative"), self.root / ".." / self.root.name):
            with self.assertRaises(RecordingError):
                PrivateStore(root)
        unsafe = self.root / "unsafe"
        unsafe.mkdir(mode=0o755)
        with self.assertRaises(RecordingError):
            PrivateStore(unsafe)
        repository = self.root / "synthetic-repository"
        repository.mkdir(mode=0o700)
        (repository / ".git").mkdir()
        child = repository / "child"
        child.mkdir(mode=0o700)
        with self.assertRaises(RecordingError):
            PrivateStore(child)

    async def test_symlink_root_and_ancestor_rejected(self):
        alias = self.root / "alias"
        alias.symlink_to(self.root, target_is_directory=True)
        for root in (alias, alias / NAMESPACE):
            with self.assertRaises(RecordingError):
                PrivateStore(root)

    async def test_namespace_symlink_rejected(self):
        root = self.root / "another"
        root.mkdir(mode=0o700)
        (root / NAMESPACE).symlink_to(self.directory, target_is_directory=True)
        with self.assertRaises(RecordingError):
            PrivateStore(root)

    async def test_unsafe_file_permissions_preserved(self):
        clip = await self.complete()
        clip.chmod(0o644)
        with self.assertRaises(RecordingError):
            self.store.recover()
        self.assertTrue(clip.exists())

    async def test_entry_scan_bound_preserves_unrelated_files(self):
        self.store.close()
        self.store = PrivateStore(self.root, StoragePolicy(max_clips=1, max_entries=3))
        self.addCleanup(self.store.close)
        for number in range(4):
            (self.directory / ("unrelated-" + str(number))).write_bytes(b"synthetic")
        with self.assertRaises(RecordingError):
            self.store.recover()
        self.assertEqual(len(list(self.directory.glob("unrelated-*"))), 4)

    async def test_paths_and_identifiers_absent_from_errors_and_reprs(self):
        self.assertNotIn(str(self.root), repr(self.store))
        transaction = self.store.begin(32)
        self.assertNotIn(str(self.root), repr(transaction))
        transaction.abort()
        with self.assertRaises(RecordingError) as error:
            PrivateStore(self.root / SECRET)
        self.assertNotIn(SECRET, repr(error.exception))


if __name__ == "__main__":
    unittest.main()
