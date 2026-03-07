"""Run catalog index — append-only registry of published runs per model.

The catalog replaces a naive latest.json single-pointer approach.  It provides
run history, rollback capability, and enough metadata for retention/GC.

Schema (catalog.json):

    {
        "schema_version": 1,
        "model": "gfs",
        "current_run_id": "20260306T12Z",
        "runs": [
            {
                "run_id": "20260306T00Z",
                "status": "superseded",
                "published_at": "2026-03-06T01:23:45Z",
                "superseded_at": "2026-03-06T13:45:00Z",
                "expired_at": null,
                "asset_manifest_path": "models/gfs/runs/20260306T00Z/ui/manifest.json",
                "processing_version": "1.0.0"
            },
            ...
        ]
    }

Append-only semantics: entries are added, never removed.  Only status
and the corresponding timestamp fields transition forward:
    published -> superseded -> expired
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

from weatherman.storage.paths import RunID, StorageLayout

SCHEMA_VERSION = 1


class RunStatus(Enum):
    """Lifecycle status of a catalog run entry."""

    PUBLISHED = "published"
    SUPERSEDED = "superseded"
    EXPIRED = "expired"


# Valid forward transitions
_VALID_TRANSITIONS: dict[RunStatus, set[RunStatus]] = {
    RunStatus.PUBLISHED: {RunStatus.SUPERSEDED, RunStatus.EXPIRED},
    RunStatus.SUPERSEDED: {RunStatus.EXPIRED},
    RunStatus.EXPIRED: set(),
}


@dataclass
class RunEntry:
    """A single run entry in the catalog."""

    run_id: RunID
    status: RunStatus
    published_at: datetime
    superseded_at: datetime | None = None
    expired_at: datetime | None = None
    asset_manifest_path: str = ""
    processing_version: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": str(self.run_id),
            "status": self.status.value,
            "published_at": self.published_at.isoformat(),
            "superseded_at": self.superseded_at.isoformat() if self.superseded_at else None,
            "expired_at": self.expired_at.isoformat() if self.expired_at else None,
            "asset_manifest_path": self.asset_manifest_path,
            "processing_version": self.processing_version,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RunEntry:
        return cls(
            run_id=RunID(data["run_id"]),
            status=RunStatus(data["status"]),
            published_at=datetime.fromisoformat(data["published_at"]),
            superseded_at=(
                datetime.fromisoformat(data["superseded_at"])
                if data.get("superseded_at")
                else None
            ),
            expired_at=(
                datetime.fromisoformat(data["expired_at"])
                if data.get("expired_at")
                else None
            ),
            asset_manifest_path=data.get("asset_manifest_path", ""),
            processing_version=data.get("processing_version", ""),
        )


@dataclass
class RunCatalog:
    """Append-only index of published runs for a single model.

    Entries are never removed — only their status transitions forward.
    The ``current_run_id`` points to the active (most-recently published) run.
    """

    model: str
    current_run_id: RunID | None = None
    runs: list[RunEntry] = field(default_factory=list)
    schema_version: int = SCHEMA_VERSION

    # -- Query helpers --

    def get_entry(self, run_id: RunID) -> RunEntry | None:
        """Look up a run entry by ID, or None if not found."""
        for entry in self.runs:
            if entry.run_id == run_id:
                return entry
        return None

    def published_runs(self) -> list[RunEntry]:
        """Return entries that are still in 'published' status."""
        return [e for e in self.runs if e.status == RunStatus.PUBLISHED]

    def superseded_runs(self) -> list[RunEntry]:
        """Return entries in 'superseded' status (candidates for GC)."""
        return [e for e in self.runs if e.status == RunStatus.SUPERSEDED]

    # -- Mutations (append-only) --

    def publish_run(
        self,
        run_id: RunID,
        *,
        layout: StorageLayout,
        published_at: datetime | None = None,
        processing_version: str = "",
    ) -> RunEntry:
        """Register a new run as published and make it current.

        Automatically supersedes the previous current run.

        Raises:
            ValueError: If a run with this ID already exists.
        """
        if self.get_entry(run_id) is not None:
            raise ValueError(f"Run {run_id} already exists in catalog")

        now = published_at or datetime.now(timezone.utc)

        # Supersede the previous current run
        if self.current_run_id is not None:
            prev = self.get_entry(self.current_run_id)
            if prev is not None and prev.status == RunStatus.PUBLISHED:
                self._transition(prev, RunStatus.SUPERSEDED, at=now)

        entry = RunEntry(
            run_id=run_id,
            status=RunStatus.PUBLISHED,
            published_at=now,
            asset_manifest_path=layout.manifest_path(run_id),
            processing_version=processing_version,
        )
        self.runs.append(entry)
        self.current_run_id = run_id
        return entry

    def rollback_to(self, run_id: RunID) -> RunEntry:
        """Roll back current_run_id to a previous run.

        The target run must exist and not be expired.  The current run
        is superseded.

        Raises:
            ValueError: If the target run doesn't exist or is expired.
        """
        target = self.get_entry(run_id)
        if target is None:
            raise ValueError(f"Run {run_id} not found in catalog")
        if target.status == RunStatus.EXPIRED:
            raise ValueError(f"Cannot roll back to expired run {run_id}")

        now = datetime.now(timezone.utc)

        # Supersede current if it's still published
        if self.current_run_id is not None and self.current_run_id != run_id:
            current = self.get_entry(self.current_run_id)
            if current is not None and current.status == RunStatus.PUBLISHED:
                self._transition(current, RunStatus.SUPERSEDED, at=now)

        # Re-promote the target to PUBLISHED so that published_runs()
        # correctly reflects the active run.
        if target.status == RunStatus.SUPERSEDED:
            target.status = RunStatus.PUBLISHED
            target.superseded_at = None
        self.current_run_id = run_id
        return target

    def expire_run(self, run_id: RunID, *, expired_at: datetime | None = None) -> RunEntry:
        """Mark a run as expired (assets eligible for deletion).

        Raises:
            ValueError: If the run is current or doesn't exist.
        """
        entry = self.get_entry(run_id)
        if entry is None:
            raise ValueError(f"Run {run_id} not found in catalog")
        if self.current_run_id == run_id:
            raise ValueError(f"Cannot expire current run {run_id}")

        now = expired_at or datetime.now(timezone.utc)
        self._transition(entry, RunStatus.EXPIRED, at=now)
        return entry

    @staticmethod
    def _transition(entry: RunEntry, to: RunStatus, *, at: datetime) -> None:
        """Apply a valid forward status transition."""
        if to not in _VALID_TRANSITIONS[entry.status]:
            raise ValueError(
                f"Invalid transition {entry.status.value} -> {to.value} "
                f"for run {entry.run_id}"
            )
        entry.status = to
        if to == RunStatus.SUPERSEDED:
            entry.superseded_at = at
        elif to == RunStatus.EXPIRED:
            entry.expired_at = at

    # -- Serialization --

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "model": self.model,
            "current_run_id": str(self.current_run_id) if self.current_run_id else None,
            "runs": [e.to_dict() for e in self.runs],
        }

    def to_json(self, *, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RunCatalog:
        version = data.get("schema_version", 0)
        if version != SCHEMA_VERSION:
            raise ValueError(
                f"Unsupported catalog schema version {version} "
                f"(expected {SCHEMA_VERSION})"
            )
        current = data.get("current_run_id")
        return cls(
            model=data["model"],
            current_run_id=RunID(current) if current else None,
            runs=[RunEntry.from_dict(r) for r in data.get("runs", [])],
            schema_version=version,
        )

    @classmethod
    def from_json(cls, text: str) -> RunCatalog:
        return cls.from_dict(json.loads(text))

    @classmethod
    def new(cls, model: str) -> RunCatalog:
        """Create an empty catalog for a model."""
        return cls(model=model)

    # -- Atomic file I/O (for local/test use; S3 uses PUT) --

    def save(self, path: Path) -> None:
        """Atomic write: write to PID-unique tmp file then rename."""
        tmp = path.with_name(f"{path.name}.tmp.{os.getpid()}")
        try:
            tmp.write_text(self.to_json(), encoding="utf-8")
            tmp.rename(path)
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise

    @classmethod
    def load(cls, path: Path) -> RunCatalog:
        """Load catalog from a JSON file."""
        return cls.from_json(path.read_text(encoding="utf-8"))
