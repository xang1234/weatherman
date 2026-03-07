"""File-backed durable queue with dead-letter queue for ingestion events.

Events are persisted as individual JSON files in a directory structure:

    <queue_dir>/
        pending/         # events waiting to be processed
        in_progress/     # events currently being processed
        completed/       # successfully processed events
        dlq/             # events that exhausted all retries

Each event file contains the full context needed for replay:
what was being downloaded, which attempt we're on, and error history.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from weatherman.ingest.retry import RetryPolicy

logger = logging.getLogger(__name__)


@dataclass
class IngestEvent:
    """An ingestion event with full context for retry/replay."""

    event_id: str
    run_id: str
    model: str
    variable: str
    forecast_hour: int
    search_pattern: str
    created_at: str
    attempt: int = 0
    last_error: str | None = None
    error_history: list[dict[str, Any]] = field(default_factory=list)
    next_retry_at: str | None = None

    @classmethod
    def create(
        cls,
        *,
        run_id: str,
        model: str,
        variable: str,
        forecast_hour: int,
        search_pattern: str,
    ) -> IngestEvent:
        """Create a new ingestion event."""
        return cls(
            event_id=uuid4().hex[:12],
            run_id=run_id,
            model=model,
            variable=variable,
            forecast_hour=forecast_hour,
            search_pattern=search_pattern,
            created_at=datetime.now(timezone.utc).isoformat(),
        )

    def record_failure(self, error: str) -> None:
        """Record a failed attempt."""
        self.error_history.append({
            "attempt": self.attempt,
            "error": error,
            "failed_at": datetime.now(timezone.utc).isoformat(),
        })
        self.last_error = error
        self.attempt += 1

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> IngestEvent:
        return cls(**data)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)

    @classmethod
    def from_json(cls, text: str) -> IngestEvent:
        return cls.from_dict(json.loads(text))


class IngestQueue:
    """File-backed durable queue with DLQ for ingestion events.

    Directory layout:
        pending/       — queued events awaiting processing
        in_progress/   — events currently being worked on
        completed/     — successfully finished events
        dlq/           — failed events that exhausted retries
    """

    SUBDIRS = ("pending", "in_progress", "completed", "dlq")

    def __init__(self, queue_dir: Path, *, retry_policy: RetryPolicy | None = None) -> None:
        self._dir = queue_dir
        self._retry_policy = retry_policy or RetryPolicy()
        for subdir in self.SUBDIRS:
            (self._dir / subdir).mkdir(parents=True, exist_ok=True)

    @property
    def retry_policy(self) -> RetryPolicy:
        return self._retry_policy

    def _event_path(self, subdir: str, event_id: str) -> Path:
        return self._dir / subdir / f"{event_id}.json"

    def _save_event(self, subdir: str, event: IngestEvent) -> Path:
        """Atomically write an event to a subdirectory."""
        path = self._event_path(subdir, event.event_id)
        tmp = path.with_name(f"{path.name}.tmp.{os.getpid()}.{threading.get_ident()}")
        try:
            tmp.write_text(event.to_json(), encoding="utf-8")
            tmp.rename(path)
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise
        return path

    def _move_event(self, event_id: str, from_dir: str, to_dir: str) -> None:
        """Move an event file between subdirectories."""
        src = self._event_path(from_dir, event_id)
        dst = self._event_path(to_dir, event_id)
        src.rename(dst)

    def _load_event(self, path: Path) -> IngestEvent:
        return IngestEvent.from_json(path.read_text(encoding="utf-8"))

    # -- Public API --

    def enqueue(self, event: IngestEvent) -> Path:
        """Add an event to the pending queue."""
        path = self._save_event("pending", event)
        logger.info("Enqueued event %s: %s/%s fxx=%03d",
                     event.event_id, event.model, event.variable, event.forecast_hour)
        return path

    def take(self) -> IngestEvent | None:
        """Take the next pending event for processing.

        Moves the event from pending/ to in_progress/. Returns None if
        the queue is empty. Events with a future next_retry_at are skipped.
        """
        now = datetime.now(timezone.utc)
        pending_dir = self._dir / "pending"
        for path in sorted(pending_dir.glob("*.json")):
            event = self._load_event(path)
            if event.next_retry_at:
                retry_at = datetime.fromisoformat(event.next_retry_at)
                if retry_at > now:
                    continue
            try:
                self._move_event(event.event_id, "pending", "in_progress")
            except FileNotFoundError:
                continue  # another worker claimed it
            logger.info("Processing event %s (attempt %d)", event.event_id, event.attempt)
            return event
        return None

    def complete(self, event: IngestEvent) -> None:
        """Mark an event as successfully completed."""
        self._move_event(event.event_id, "in_progress", "completed")
        logger.info("Completed event %s", event.event_id)

    def fail(self, event: IngestEvent, error: str) -> None:
        """Record a failure. Requeue for retry or move to DLQ."""
        event.record_failure(error)

        in_prog = self._event_path("in_progress", event.event_id)

        if self._retry_policy.should_retry(event.attempt - 1):
            delay = self._retry_policy.delay_for_attempt(event.attempt - 1)
            retry_at = datetime.fromtimestamp(
                time.time() + delay, tz=timezone.utc
            )
            event.next_retry_at = retry_at.isoformat()
            # Remove from in_progress first, then write to pending.
            # On crash after unlink but before save, the event is lost
            # (preferable to duplication — lost events surface as missing
            # data, duplicates cause silent double-processing).
            in_prog.unlink(missing_ok=True)
            self._save_event("pending", event)
            logger.warning(
                "Event %s failed (attempt %d/%d): %s — retry in %.1fs",
                event.event_id, event.attempt, self._retry_policy.max_attempts,
                error, delay,
            )
        else:
            # Exhausted retries — move to DLQ
            in_prog.unlink(missing_ok=True)
            self._save_event("dlq", event)
            logger.error(
                "Event %s moved to DLQ after %d attempts: %s",
                event.event_id, event.attempt, error,
            )

    def pending_count(self) -> int:
        return sum(1 for _ in (self._dir / "pending").glob("*.json"))

    def dlq_count(self) -> int:
        return sum(1 for _ in (self._dir / "dlq").glob("*.json"))

    def list_dlq(self) -> list[IngestEvent]:
        """List all events in the dead-letter queue."""
        dlq_dir = self._dir / "dlq"
        events = []
        for path in sorted(dlq_dir.glob("*.json")):
            events.append(self._load_event(path))
        return events

    def replay_from_dlq(self, event_id: str) -> IngestEvent:
        """Move a DLQ event back to pending for reprocessing.

        Resets the attempt counter and clears retry timing.

        Raises:
            FileNotFoundError: If the event is not in the DLQ.
        """
        path = self._event_path("dlq", event_id)
        if not path.exists():
            raise FileNotFoundError(f"Event {event_id} not found in DLQ")
        event = self._load_event(path)
        event.attempt = 0
        event.next_retry_at = None
        path.unlink()
        self._save_event("pending", event)
        logger.info("Replayed DLQ event %s back to pending", event_id)
        return event

    def replay_all_dlq(self) -> list[IngestEvent]:
        """Move all DLQ events back to pending."""
        replayed = []
        for event in self.list_dlq():
            replayed.append(self.replay_from_dlq(event.event_id))
        return replayed
