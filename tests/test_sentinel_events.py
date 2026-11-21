"""Synthetic adapter rows only. No archive imports, credentials or live I/O."""

from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone, tzinfo
import unittest

from sentinel.events import (
    Checkpoint, EventBatch, InvalidEvent, MotionEvent, MotionObserver,
)


NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
CAMERA = "synthetic-camera-a"


def row(event_id="synthetic-event-a", **changes):
    return dict(camera_id=CAMERA, event_id=event_id, kind="motion",
                reported_at=NOW, **changes)


def batch(*rows, complete=True):
    return EventBatch(tuple(rows), complete=complete)


def broken_timestamp():
    class BrokenTimezone(tzinfo):
        def utcoffset(self, dt):
            raise RuntimeError("synthetic-private-timezone-detail")

    return NOW.replace(tzinfo=BrokenTimezone())


class SchemaTests(unittest.TestCase):
    def test_exact_schema_and_utc_normalization(self):
        local = NOW.astimezone(timezone(timedelta(hours=3)))
        event = MotionEvent.from_row({**row(), "reported_at": local}, CAMERA)
        self.assertEqual(event.reported_at, NOW)
        self.assertIs(event.reported_at.tzinfo, timezone.utc)
        self.assertEqual(event.camera_id, CAMERA)

    def test_missing_fields_fail_closed(self):
        for key in row():
            with self.subTest(key=key), self.assertRaises(InvalidEvent):
                MotionEvent.from_row({k: v for k, v in row().items() if k != key}, CAMERA)

    def test_conflicting_aliases_and_unknown_nested_data_fail_closed(self):
        for key, value in (("device_id", "synthetic-camera-b"),
                           ("eventId", "synthetic-event-b"), ("payload", row()),
                           ("audioUrl", []), ("videoUrl", []), ("extra", None)):
            with self.subTest(key=key), self.assertRaises(InvalidEvent):
                MotionEvent.from_row({**row(), key: value}, CAMERA)

    def test_invalid_row_containers_and_wrappers(self):
        for value in (None, False, [], (), "synthetic", {"data": row()}, [row()]):
            with self.subTest(value=type(value)), self.assertRaises(InvalidEvent):
                MotionEvent.from_row(value, CAMERA)

    def test_identity_types_lengths_and_whitespace_are_strict(self):
        for value in (None, False, True, 0, 1.0, {}, [], "", " ", " a", "a ",
                      "a\nb", "a\x00b", "a\u200bb", "a" * 129):
            for key in ("camera_id", "event_id"):
                with self.subTest(key=key, value=type(value)), self.assertRaises(InvalidEvent):
                    MotionEvent.from_row({**row(), key: value}, CAMERA)

    def test_zero_string_is_a_valid_stable_id(self):
        self.assertEqual(MotionEvent.from_row(row("0"), CAMERA).event_id, "0")

    def test_camera_mismatch_is_rejected(self):
        with self.assertRaisesRegex(InvalidEvent, "camera-mismatch"):
            MotionEvent.from_row({**row(), "camera_id": "synthetic-camera-b"}, CAMERA)

    def test_alarm_numbers_and_nonmotion_are_not_classified_here(self):
        for value in (None, True, 1, 2, "person", "PIR", "Motion", "unknown", {}):
            with self.subTest(value=type(value)), self.assertRaises(InvalidEvent):
                MotionEvent.from_row({**row(), "kind": value}, CAMERA)

    def test_invalid_and_ambiguous_timestamps_are_rejected(self):
        for value in (True, 0, 1700000000, float("nan"), float("inf"),
                      "2026-01-01T00:00:00Z", "20260101000000", {}, [],
                      NOW.replace(tzinfo=None)):
            with self.subTest(value=type(value)), self.assertRaises(InvalidEvent):
                MotionEvent.from_row({**row(), "reported_at": value}, CAMERA)

    def test_unknown_and_future_reported_time_are_explicit(self):
        self.assertIsNone(MotionEvent.from_row({**row(), "reported_at": None}, CAMERA).reported_at)
        future = NOW + timedelta(days=10)
        self.assertEqual(MotionEvent.from_row({**row(), "reported_at": future}, CAMERA).reported_at,
                         future)

    def test_timezone_without_offset_or_overflow_is_rejected(self):
        class NoOffset(tzinfo):
            def utcoffset(self, dt):
                return None

        class InvalidOffset(tzinfo):
            def utcoffset(self, dt):
                return "synthetic-invalid-offset"

        for value in (NOW.replace(tzinfo=NoOffset()), NOW.replace(tzinfo=InvalidOffset()),
                      datetime.min.replace(tzinfo=timezone(timedelta(hours=1)))):
            with self.subTest(timezone=type(value.tzinfo)), self.assertRaises(InvalidEvent):
                MotionEvent.from_row({**row(), "reported_at": value}, CAMERA)

    def test_dataclass_construction_is_also_validated_and_immutable(self):
        with self.assertRaises(InvalidEvent):
            MotionEvent(CAMERA, True, "motion", NOW)
        event = MotionEvent.from_row(row(), CAMERA)
        with self.assertRaises(FrozenInstanceError):
            event.event_id = "synthetic-change"

    def test_timezone_callback_failure_has_a_fixed_safe_rejection(self):
        with self.assertRaises(InvalidEvent) as caught:
            MotionEvent.from_row({**row(), "reported_at": broken_timestamp()}, CAMERA)
        self.assertEqual(str(caught.exception), "invalid-timestamp")
        self.assertIsNone(caught.exception.__cause__)
        self.assertTrue(caught.exception.__suppress_context__)

    def test_safe_representations_and_fixed_rejection_messages(self):
        marker = "synthetic-private-marker"
        event = MotionEvent.from_row(row(marker), CAMERA)
        self.assertNotIn(marker, repr(event))
        self.assertNotIn(CAMERA, repr(event))
        self.assertNotIn(marker, repr(batch(row(marker))))
        with self.assertRaises(InvalidEvent) as caught:
            MotionEvent.from_row({**row(), "camera_id": marker}, CAMERA)
        self.assertEqual(str(caught.exception), "camera-mismatch")


class ObserverTests(unittest.TestCase):
    def setUp(self):
        self.observer = MotionObserver(CAMERA)

    def observe(self, value):
        return self.observer.observe(value, received_at=NOW + timedelta(seconds=10))

    def ready(self):
        self.assertEqual(self.observe(batch()).status, "baseline")

    def test_first_complete_batch_is_historical_even_with_future_time(self):
        result = self.observe(batch(row(), {**row("future"), "reported_at": NOW + timedelta(days=2)}))
        self.assertEqual(result.status, "baseline")
        self.assertEqual(result.historical_ids, 2)
        self.assertEqual(result.observations, ())
        self.assertTrue(self.observer.baseline_ready)

    def test_empty_complete_baseline_then_valid_motion(self):
        self.ready()
        result = self.observe(batch(row()))
        self.assertEqual(result.status, "observed")
        self.assertEqual(len(result.observations), 1)
        event = result.observations[0]
        self.assertEqual(event.event.event_id, "synthetic-event-a")
        self.assertEqual(event.received_at, NOW + timedelta(seconds=10))
        self.assertEqual(event.event.reported_at, NOW)
        self.assertEqual(event.freshness, "unverified")
        self.assertEqual(event.history_relation, "unseen-after-baseline")
        self.assertIsNone(event.physical_motion_at)
        self.assertIsNone(event.latency_seconds)

    def test_baseline_ids_remain_suppressed_on_later_polls(self):
        self.observe(batch(row()))
        for _ in range(5):
            result = self.observe(batch(row()))
            self.assertEqual(result.observations, ())
            self.assertEqual(result.duplicate_rows, 1)

    def test_reordered_batches_and_duplicate_ids(self):
        self.ready()
        result = self.observe(batch(row("b"), row("a"), row("b")))
        self.assertEqual([x.event.event_id for x in result.observations], ["a", "b"])
        self.assertEqual(result.duplicate_rows, 1)
        repeated = self.observe(batch(row("a"), row("b"), row("a")))
        self.assertEqual(repeated.observations, ())
        self.assertEqual(repeated.duplicate_rows, 3)

    def test_distinct_ids_with_same_timestamp_are_distinct_events(self):
        self.ready()
        self.assertEqual(len(self.observe(batch(row("a"), row("b"))).observations), 2)

    def test_changed_timestamp_does_not_reemit_a_known_id(self):
        self.ready()
        self.observe(batch(row()))
        self.assertEqual(self.observe(batch({**row(), "reported_at": None})).observations, ())

    def test_unseen_old_timestamp_is_only_unseen_after_baseline(self):
        self.ready()
        result = self.observe(batch({**row(), "reported_at": NOW - timedelta(days=500)}))
        self.assertEqual(result.observations[0].freshness, "unverified")
        self.assertIsNone(result.observations[0].latency_seconds)

    def test_unknown_reported_timestamp_can_be_observed_with_stable_id(self):
        self.ready()
        self.assertIsNone(self.observe(batch({**row(), "reported_at": None})).observations[0].event.reported_at)

    def test_invalid_batches_invalidate_readiness_without_mutating_seen(self):
        for value in (None, [], {}, batch(complete=False), EventBatch([], True),
                      EventBatch((), 1), batch(*[row()] * 129)):
            self.ready()
            with self.subTest(container=type(value)):
                self.assertEqual(self.observe(value).status, "rejected")
                self.assertFalse(self.observer.baseline_ready)
                self.assertEqual(self.observer.seen_count, 0)

    def test_invalid_row_isolates_whole_window_without_valid_prefix(self):
        self.ready()
        result = self.observe(batch(row("valid"), {**row("bad"), "camera_id": "other"}))
        self.assertEqual(result.rejected_rows, 1)
        self.assertEqual(result.observations, ())
        self.assertEqual(self.observer.seen_count, 0)
        self.assertFalse(self.observer.baseline_ready)
        # Corrected old rows are seeded on the first clean recovery page.
        recovered = self.observe(batch(row("valid"), row("bad")))
        self.assertEqual(recovered.status, "baseline")
        self.assertEqual(recovered.historical_ids, 2)
        self.assertEqual(self.observe(batch(row("valid"), row("bad"))).observations, ())
        self.assertEqual(len(self.observe(batch(row("next"))).observations), 1)

    def test_repeated_bad_rows_never_create_history_or_readiness(self):
        for _ in range(10):
            self.assertEqual(self.observe(batch(row("valid"), {})).status, "rejected")
        self.assertEqual(self.observer.seen_count, 0)
        self.assertFalse(self.observer.baseline_ready)

    def test_conflicting_metadata_for_same_id_rejects_regardless_of_order(self):
        a, b = row(), {**row(), "reported_at": NOW + timedelta(seconds=1)}
        for rows in ((a, b), (b, a)):
            self.ready()
            result = self.observe(batch(*rows))
            self.assertEqual(result.status, "rejected")
            self.assertEqual(result.rejected_rows, 1)
            self.assertEqual(self.observer.seen_count, 0)

    def test_missing_identity_does_not_borrow_sibling_or_batch_context(self):
        self.ready()
        missing = {k: v for k, v in row("missing").items() if k != "camera_id"}
        self.assertEqual(self.observe(batch(row(), missing)).status, "rejected")
        self.assertEqual(self.observer.seen_count, 0)

    def test_incomplete_empty_batch_cannot_establish_baseline(self):
        self.assertEqual(self.observe(batch(complete=False)).status, "rejected")
        self.assertFalse(self.observer.baseline_ready)

    def test_batch_boundary_inclusive(self):
        self.observer = MotionObserver(CAMERA, max_batch=2)
        self.ready()
        self.assertEqual(len(self.observe(batch(row("a"), row("b"))).observations), 2)
        self.assertEqual(self.observe(batch(row("c"), row("d"), row("e"))).status, "rejected")
        self.assertEqual(self.observer.seen_count, 2)

    def test_capacity_never_evicts_history_or_emits_partial_batch(self):
        self.observer = MotionObserver(CAMERA, max_seen=2)
        self.observe(batch(row("history")))
        self.assertEqual(self.observe(batch(row("history"), row("a"), row("b"))).status, "capacity")
        self.assertEqual(self.observer.seen_count, 1)
        self.assertTrue(self.observer.saturated)
        for _ in range(100):
            self.assertEqual(self.observe(batch(row("history"), row("a"))).observations, ())
        self.assertEqual(self.observer.seen_count, 1)
        self.assertFalse(self.observer.baseline_ready)

    def test_exact_capacity_still_suppresses_repeats(self):
        self.observer = MotionObserver(CAMERA, max_seen=2)
        self.observe(batch(row("a")))
        self.assertEqual(len(self.observe(batch(row("b"))).observations), 1)
        self.assertFalse(self.observer.saturated)
        self.assertEqual(self.observe(batch(row("a"), row("b"))).duplicate_rows, 2)
        self.assertEqual(self.observe(batch(row("c"))).status, "capacity")

    def test_many_distinct_batches_cannot_grow_state_past_capacity(self):
        self.observer = MotionObserver(CAMERA, max_seen=32)
        self.ready()
        emitted = []
        for index in range(500):
            result = self.observe(batch(row(f"synthetic-{index}")))
            emitted.extend(result.observations)
            self.assertLessEqual(self.observer.seen_count, 32)
        self.assertEqual(len(emitted), 32)
        self.assertEqual(self.observer.seen_count, 32)
        self.assertTrue(self.observer.saturated)

    def test_receipt_time_is_validated_before_state_mutation(self):
        with self.assertRaises(InvalidEvent):
            self.observer.observe(batch(row()), received_at=NOW.replace(tzinfo=None))
        self.assertEqual(self.observer.seen_count, 0)
        self.assertFalse(self.observer.baseline_ready)

    def test_input_mutation_cannot_change_retained_observation(self):
        self.ready()
        original = row()
        result = self.observe(batch(original))
        original["event_id"] = "mutated"
        self.assertEqual(result.observations[0].event.event_id, "synthetic-event-a")
        self.assertIn("synthetic-event-a", self.observer.checkpoint().event_ids)

    def test_configuration_rejects_unbounded_or_invalid_limits(self):
        for value in (False, 0, -1, 1.0, "2", 100_001):
            with self.subTest(value=value), self.assertRaises(ValueError):
                MotionObserver(CAMERA, max_seen=value)
        for value in (False, 0, -1, 1.0, 10_001):
            with self.subTest(value=value), self.assertRaises(ValueError):
                MotionObserver(CAMERA, max_batch=value)
        with self.assertRaises(InvalidEvent):
            MotionObserver("")


class RecoveryTests(unittest.TestCase):
    def observe(self, observer, value):
        return observer.observe(value, received_at=NOW)

    def checkpoint(self):
        observer = MotionObserver(CAMERA)
        self.observe(observer, batch(row("history")))
        self.observe(observer, batch(row("emitted")))
        return observer.checkpoint()

    def test_restore_rebaselines_and_does_not_reemit_old_ids(self):
        restored = MotionObserver.restore(self.checkpoint(), CAMERA)
        self.assertFalse(restored.baseline_ready)
        # Even previously unseen history at recovery is suppressed.
        result = self.observe(restored, batch(row("emitted"), row("outage")))
        self.assertEqual(result.status, "baseline")
        self.assertEqual(result.historical_ids, 1)
        self.assertEqual(self.observe(restored, batch(row("history"), row("outage"))).observations, ())
        self.assertEqual(len(self.observe(restored, batch(row("next"))).observations), 1)

    def test_restore_after_empty_rebaseline_still_remembers_old_ids(self):
        restored = MotionObserver.restore(self.checkpoint(), CAMERA)
        self.observe(restored, batch())
        self.assertEqual(self.observe(restored, batch(row("emitted"), row("history"))).observations, ())

    def test_checkpoint_is_immutable_snapshot_and_repr_omits_identifiers(self):
        checkpoint = self.checkpoint()
        self.assertIs(type(checkpoint.event_ids), frozenset)
        self.assertNotIn(CAMERA, repr(checkpoint))
        self.assertNotIn("emitted", repr(checkpoint))
        with self.assertRaises(FrozenInstanceError):
            checkpoint.saturated = True
        restored = MotionObserver.restore(checkpoint, CAMERA)
        self.observe(restored, batch(row("other")))
        self.assertNotIn("other", checkpoint.event_ids)

    def test_saturation_survives_restore(self):
        observer = MotionObserver(CAMERA, max_seen=1)
        self.observe(observer, batch(row("a"), row("b")))
        restored = MotionObserver.restore(observer.checkpoint(), CAMERA, max_seen=1)
        self.assertTrue(restored.saturated)
        self.assertEqual(self.observe(restored, batch()).status, "capacity")

    def test_malformed_mismatched_or_oversized_checkpoint_rejected(self):
        for value in (None, {}, Checkpoint("other", frozenset(), False),
                      Checkpoint(CAMERA, {"a"}, False), Checkpoint(CAMERA, frozenset({True}), False),
                      Checkpoint(CAMERA, frozenset(), 1), Checkpoint(CAMERA, frozenset(), False, 2),
                      Checkpoint(CAMERA, frozenset(), False, True)):
            with self.subTest(value=type(value)), self.assertRaises(ValueError):
                MotionObserver.restore(value, CAMERA)
        with self.assertRaises(ValueError):
            MotionObserver.restore(self.checkpoint(), CAMERA, max_seen=1)


if __name__ == "__main__":
    unittest.main()
