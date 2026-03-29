"""File-backed event journal for cross-process SSE replay and signaling."""

from __future__ import annotations

import json
import os
from pathlib import Path

from weatherman.events.bus import ServerEvent


class EventJournal:
    """Append-only JSONL event journal with a file-backed monotonic counter."""

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._counter_path = self._path.with_suffix(self._path.suffix + ".counter")
        self._path.parent.mkdir(parents=True, exist_ok=True)

    @property
    def path(self) -> Path:
        return self._path

    def next_event_id(self) -> str:
        import fcntl

        with self._counter_path.open("a+", encoding="utf-8") as fh:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
            fh.seek(0)
            raw = fh.read().strip()
            current = int(raw) if raw else 0
            next_id = current + 1
            fh.seek(0)
            fh.truncate()
            fh.write(str(next_id))
            fh.flush()
            os.fsync(fh.fileno())
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        return str(next_id)

    def append(self, event: ServerEvent) -> None:
        import fcntl

        payload = json.dumps(
            {
                "id": event.id,
                "event": event.event,
                "data": event.data,
                "tenant_id": event.tenant_id,
            },
            separators=(",", ":"),
        )
        with self._path.open("a+", encoding="utf-8") as fh:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
            fh.write(payload + "\n")
            fh.flush()
            os.fsync(fh.fileno())
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)

    def read_after(
        self,
        after_id: str | None,
        tenant_id: str,
    ) -> list[ServerEvent]:
        if not self._path.exists():
            return []
        cutoff = _parse_event_id(after_id)
        events: list[ServerEvent] = []
        with self._path.open("r", encoding="utf-8") as fh:
            for raw_line in fh:
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                    event = ServerEvent(
                        id=str(payload["id"]),
                        event=str(payload["event"]),
                        data=str(payload["data"]),
                        tenant_id=str(payload.get("tenant_id", "*")),
                    )
                except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                    # Ignore partial/truncated/corrupt trailing lines.
                    continue
                if _parse_event_id(event.id) <= cutoff:
                    continue
                if event.tenant_id not in {"*", tenant_id}:
                    continue
                events.append(event)
        return events


def _parse_event_id(raw: str | None) -> int:
    try:
        return int(raw) if raw is not None else 0
    except (TypeError, ValueError):
        return 0
