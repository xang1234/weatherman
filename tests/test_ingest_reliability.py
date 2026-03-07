"""Tests for ingestion reliability controls (wx-ir5.2.3)."""

from __future__ import annotations

from pathlib import Path

import pytest

from weatherman.ingest.retry import RetryPolicy
from weatherman.ingest.queue import IngestEvent, IngestQueue


# -- RetryPolicy tests --


class TestRetryPolicy:
    def test_defaults(self):
        p = RetryPolicy()
        assert p.max_attempts == 5
        assert p.base_delay_s == 1.0
        assert p.max_delay_s == 300.0
        assert p.jitter_factor == 0.5

    def test_exponential_backoff_without_jitter(self):
        p = RetryPolicy(base_delay_s=1.0, max_delay_s=300.0, jitter_factor=0.0)
        assert p.delay_for_attempt(0) == 1.0    # 1 * 2^0
        assert p.delay_for_attempt(1) == 2.0    # 1 * 2^1
        assert p.delay_for_attempt(2) == 4.0    # 1 * 2^2
        assert p.delay_for_attempt(3) == 8.0    # 1 * 2^3

    def test_delay_clamped_to_max(self):
        p = RetryPolicy(base_delay_s=1.0, max_delay_s=10.0, jitter_factor=0.0)
        assert p.delay_for_attempt(10) == 10.0  # 1 * 2^10 = 1024, clamped

    def test_jitter_within_bounds(self):
        p = RetryPolicy(base_delay_s=10.0, max_delay_s=300.0, jitter_factor=0.5)
        for _ in range(100):
            delay = p.delay_for_attempt(0)
            assert 5.0 <= delay <= 15.0  # 10 ± 50%

    def test_should_retry(self):
        p = RetryPolicy(max_attempts=3)
        assert p.should_retry(0) is True   # first attempt
        assert p.should_retry(1) is True   # second attempt
        assert p.should_retry(2) is False  # third attempt = last

    def test_invalid_max_attempts(self):
        with pytest.raises(ValueError, match="max_attempts"):
            RetryPolicy(max_attempts=0)

    def test_invalid_base_delay(self):
        with pytest.raises(ValueError, match="base_delay_s"):
            RetryPolicy(base_delay_s=0)

    def test_invalid_max_delay(self):
        with pytest.raises(ValueError, match="max_delay_s"):
            RetryPolicy(base_delay_s=10, max_delay_s=5)

    def test_invalid_jitter(self):
        with pytest.raises(ValueError, match="jitter_factor"):
            RetryPolicy(jitter_factor=1.5)


# -- IngestEvent tests --


class TestIngestEvent:
    def test_create(self):
        ev = IngestEvent.create(
            run_id="20260306T00Z",
            model="gfs",
            variable="tmp_2m",
            forecast_hour=6,
            search_pattern=":TMP:2 m above ground:",
        )
        assert ev.run_id == "20260306T00Z"
        assert ev.model == "gfs"
        assert ev.attempt == 0
        assert ev.last_error is None
        assert len(ev.event_id) == 12

    def test_record_failure(self):
        ev = IngestEvent.create(
            run_id="20260306T00Z", model="gfs",
            variable="tmp_2m", forecast_hour=0,
            search_pattern=":TMP:",
        )
        ev.record_failure("Connection timeout")
        assert ev.attempt == 1
        assert ev.last_error == "Connection timeout"
        assert len(ev.error_history) == 1
        assert ev.error_history[0]["attempt"] == 0

    def test_json_roundtrip(self):
        ev = IngestEvent.create(
            run_id="20260306T00Z", model="gfs",
            variable="ugrd_10m", forecast_hour=12,
            search_pattern=":UGRD:",
        )
        ev.record_failure("Timeout")
        restored = IngestEvent.from_json(ev.to_json())
        assert restored.event_id == ev.event_id
        assert restored.attempt == 1
        assert restored.last_error == "Timeout"
        assert len(restored.error_history) == 1


# -- IngestQueue tests --


class TestIngestQueue:
    @pytest.fixture()
    def queue(self, tmp_path: Path) -> IngestQueue:
        return IngestQueue(tmp_path / "queue", retry_policy=RetryPolicy(max_attempts=3))

    def _make_event(self) -> IngestEvent:
        return IngestEvent.create(
            run_id="20260306T00Z", model="gfs",
            variable="tmp_2m", forecast_hour=0,
            search_pattern=":TMP:2 m above ground:",
        )

    def test_enqueue_and_take(self, queue: IngestQueue):
        ev = self._make_event()
        queue.enqueue(ev)
        assert queue.pending_count() == 1
        taken = queue.take()
        assert taken is not None
        assert taken.event_id == ev.event_id
        assert queue.pending_count() == 0

    def test_take_empty_returns_none(self, queue: IngestQueue):
        assert queue.take() is None

    def test_complete(self, queue: IngestQueue):
        ev = self._make_event()
        queue.enqueue(ev)
        taken = queue.take()
        assert taken is not None
        queue.complete(taken)
        # Event is in completed dir
        completed_path = queue._event_path("completed", ev.event_id)
        assert completed_path.exists()

    def test_fail_retries(self, queue: IngestQueue):
        ev = self._make_event()
        queue.enqueue(ev)
        taken = queue.take()
        assert taken is not None

        # First failure — should go back to pending
        queue.fail(taken, "Connection refused")
        assert queue.pending_count() == 1
        assert queue.dlq_count() == 0

    def test_fail_exhausts_retries_to_dlq(self, queue: IngestQueue):
        """After max_attempts failures, event moves to DLQ."""
        ev = self._make_event()
        queue.enqueue(ev)

        for i in range(3):  # max_attempts=3
            taken = queue.take()
            if taken is None:
                # Retry delay not yet passed — update next_retry_at to now
                path = queue._event_path("pending", ev.event_id)
                event = IngestEvent.from_json(path.read_text())
                event.next_retry_at = None
                path.write_text(event.to_json())
                taken = queue.take()
            assert taken is not None, f"Failed to take on attempt {i}"
            queue.fail(taken, f"Error {i}")

        assert queue.pending_count() == 0
        assert queue.dlq_count() == 1

    def test_dlq_preserves_error_history(self, queue: IngestQueue):
        ev = self._make_event()
        queue.enqueue(ev)

        for i in range(3):
            taken = queue.take()
            if taken is None:
                path = queue._event_path("pending", ev.event_id)
                event = IngestEvent.from_json(path.read_text())
                event.next_retry_at = None
                path.write_text(event.to_json())
                taken = queue.take()
            assert taken is not None
            queue.fail(taken, f"Error {i}")

        dlq_events = queue.list_dlq()
        assert len(dlq_events) == 1
        assert len(dlq_events[0].error_history) == 3
        assert dlq_events[0].error_history[0]["error"] == "Error 0"
        assert dlq_events[0].error_history[2]["error"] == "Error 2"

    def test_replay_from_dlq(self, queue: IngestQueue):
        ev = self._make_event()
        queue.enqueue(ev)

        for i in range(3):
            taken = queue.take()
            if taken is None:
                path = queue._event_path("pending", ev.event_id)
                event = IngestEvent.from_json(path.read_text())
                event.next_retry_at = None
                path.write_text(event.to_json())
                taken = queue.take()
            assert taken is not None
            queue.fail(taken, f"Error {i}")

        assert queue.dlq_count() == 1
        replayed = queue.replay_from_dlq(ev.event_id)
        assert replayed.attempt == 0
        assert replayed.next_retry_at is None
        assert queue.dlq_count() == 0
        assert queue.pending_count() == 1

    def test_replay_from_dlq_missing_raises(self, queue: IngestQueue):
        with pytest.raises(FileNotFoundError):
            queue.replay_from_dlq("nonexistent")

    def test_replay_all_dlq(self, queue: IngestQueue):
        # Put 2 events through to DLQ
        for _ in range(2):
            ev = self._make_event()
            queue.enqueue(ev)
            for i in range(3):
                taken = queue.take()
                if taken is None:
                    path = queue._event_path("pending", ev.event_id)
                    event = IngestEvent.from_json(path.read_text())
                    event.next_retry_at = None
                    path.write_text(event.to_json())
                    taken = queue.take()
                assert taken is not None
                queue.fail(taken, f"Error {i}")

        assert queue.dlq_count() == 2
        replayed = queue.replay_all_dlq()
        assert len(replayed) == 2
        assert queue.dlq_count() == 0
        assert queue.pending_count() == 2

    def test_fail_records_retry_delay(self, queue: IngestQueue):
        ev = self._make_event()
        queue.enqueue(ev)
        taken = queue.take()
        assert taken is not None
        queue.fail(taken, "Timeout")

        # Read back the pending event — should have next_retry_at set
        path = queue._event_path("pending", ev.event_id)
        requeued = IngestEvent.from_json(path.read_text())
        assert requeued.next_retry_at is not None

    def test_take_skips_future_retry(self, queue: IngestQueue):
        """Events with future next_retry_at are skipped by take()."""
        ev = self._make_event()
        ev.next_retry_at = "2099-12-31T23:59:59+00:00"
        queue.enqueue(ev)
        assert queue.take() is None  # skipped because retry is in the future

    def test_multiple_events_ordering(self, queue: IngestQueue):
        """Events are taken in filename-sorted order (UUID-based)."""
        events = [self._make_event() for _ in range(3)]
        for ev in events:
            queue.enqueue(ev)
        assert queue.pending_count() == 3
        taken_ids = []
        for _ in range(3):
            t = queue.take()
            assert t is not None
            taken_ids.append(t.event_id)
            queue.complete(t)
        assert len(taken_ids) == 3

    def test_replay_preserves_error_history(self, queue: IngestQueue):
        """Error history is preserved across replays for audit trail."""
        ev = self._make_event()
        queue.enqueue(ev)

        # Exhaust retries to DLQ
        for i in range(3):
            taken = queue.take()
            if taken is None:
                path = queue._event_path("pending", ev.event_id)
                event = IngestEvent.from_json(path.read_text())
                event.next_retry_at = None
                path.write_text(event.to_json())
                taken = queue.take()
            assert taken is not None
            queue.fail(taken, f"Round1-Error{i}")

        # Replay from DLQ
        replayed = queue.replay_from_dlq(ev.event_id)
        assert replayed.attempt == 0
        assert len(replayed.error_history) == 3  # history preserved

        # Exhaust retries again
        for i in range(3):
            taken = queue.take()
            if taken is None:
                path = queue._event_path("pending", ev.event_id)
                event = IngestEvent.from_json(path.read_text())
                event.next_retry_at = None
                path.write_text(event.to_json())
                taken = queue.take()
            assert taken is not None
            queue.fail(taken, f"Round2-Error{i}")

        dlq_events = queue.list_dlq()
        assert len(dlq_events) == 1
        assert len(dlq_events[0].error_history) == 6  # both rounds

    def test_subdirectories_created(self, tmp_path: Path):
        queue_dir = tmp_path / "new_queue"
        IngestQueue(queue_dir)
        for subdir in IngestQueue.SUBDIRS:
            assert (queue_dir / subdir).is_dir()
