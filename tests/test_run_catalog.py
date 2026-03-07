"""Tests for the run catalog index (append-only run registry)."""

from datetime import datetime, timezone
from pathlib import Path

import pytest

from weatherman.storage.catalog import (
    SCHEMA_VERSION,
    RunCatalog,
    RunEntry,
    RunStatus,
)
from weatherman.storage.paths import RunID, StorageLayout


@pytest.fixture
def layout():
    return StorageLayout("gfs")


@pytest.fixture
def catalog():
    return RunCatalog.new("gfs")


@pytest.fixture
def run_00z():
    return RunID("20260306T00Z")


@pytest.fixture
def run_06z():
    return RunID("20260306T06Z")


@pytest.fixture
def run_12z():
    return RunID("20260306T12Z")


class TestRunStatus:
    def test_values(self):
        assert RunStatus.PUBLISHED.value == "published"
        assert RunStatus.SUPERSEDED.value == "superseded"
        assert RunStatus.EXPIRED.value == "expired"


class TestRunEntry:
    def test_round_trip(self):
        ts = datetime(2026, 3, 6, 1, 0, 0, tzinfo=timezone.utc)
        entry = RunEntry(
            run_id=RunID("20260306T00Z"),
            status=RunStatus.PUBLISHED,
            published_at=ts,
            asset_manifest_path="models/gfs/runs/20260306T00Z/ui/manifest.json",
            processing_version="1.0.0",
        )
        d = entry.to_dict()
        restored = RunEntry.from_dict(d)
        assert restored.run_id == entry.run_id
        assert restored.status == entry.status
        assert restored.published_at == entry.published_at
        assert restored.superseded_at is None
        assert restored.expired_at is None
        assert restored.asset_manifest_path == entry.asset_manifest_path
        assert restored.processing_version == entry.processing_version

    def test_round_trip_with_timestamps(self):
        ts1 = datetime(2026, 3, 6, 1, 0, 0, tzinfo=timezone.utc)
        ts2 = datetime(2026, 3, 6, 7, 0, 0, tzinfo=timezone.utc)
        ts3 = datetime(2026, 3, 10, 0, 0, 0, tzinfo=timezone.utc)
        entry = RunEntry(
            run_id=RunID("20260306T00Z"),
            status=RunStatus.EXPIRED,
            published_at=ts1,
            superseded_at=ts2,
            expired_at=ts3,
        )
        restored = RunEntry.from_dict(entry.to_dict())
        assert restored.superseded_at == ts2
        assert restored.expired_at == ts3


class TestRunCatalogNew:
    def test_new_catalog_is_empty(self, catalog):
        assert catalog.model == "gfs"
        assert catalog.current_run_id is None
        assert catalog.runs == []
        assert catalog.schema_version == SCHEMA_VERSION

    def test_schema_version(self, catalog):
        assert catalog.to_dict()["schema_version"] == SCHEMA_VERSION


class TestPublishRun:
    def test_first_publish(self, catalog, layout, run_00z):
        ts = datetime(2026, 3, 6, 1, 0, 0, tzinfo=timezone.utc)
        entry = catalog.publish_run(run_00z, layout=layout, published_at=ts)

        assert entry.status == RunStatus.PUBLISHED
        assert entry.published_at == ts
        assert entry.asset_manifest_path == layout.manifest_path(run_00z)
        assert catalog.current_run_id == run_00z
        assert len(catalog.runs) == 1

    def test_second_publish_supersedes_previous(self, catalog, layout, run_00z, run_06z):
        ts1 = datetime(2026, 3, 6, 1, 0, 0, tzinfo=timezone.utc)
        ts2 = datetime(2026, 3, 6, 7, 0, 0, tzinfo=timezone.utc)

        catalog.publish_run(run_00z, layout=layout, published_at=ts1)
        catalog.publish_run(run_06z, layout=layout, published_at=ts2)

        assert catalog.current_run_id == run_06z
        assert len(catalog.runs) == 2

        prev = catalog.get_entry(run_00z)
        assert prev is not None
        assert prev.status == RunStatus.SUPERSEDED
        assert prev.superseded_at == ts2

        current = catalog.get_entry(run_06z)
        assert current is not None
        assert current.status == RunStatus.PUBLISHED

    def test_publish_duplicate_raises(self, catalog, layout, run_00z):
        catalog.publish_run(run_00z, layout=layout)
        with pytest.raises(ValueError, match="already exists"):
            catalog.publish_run(run_00z, layout=layout)

    def test_publish_with_processing_version(self, catalog, layout, run_00z):
        entry = catalog.publish_run(run_00z, layout=layout, processing_version="2.1.0")
        assert entry.processing_version == "2.1.0"

    def test_three_publishes_chain(self, catalog, layout, run_00z, run_06z, run_12z):
        """Publishing three runs supersedes each predecessor in turn."""
        ts1 = datetime(2026, 3, 6, 1, 0, 0, tzinfo=timezone.utc)
        ts2 = datetime(2026, 3, 6, 7, 0, 0, tzinfo=timezone.utc)
        ts3 = datetime(2026, 3, 6, 13, 0, 0, tzinfo=timezone.utc)

        catalog.publish_run(run_00z, layout=layout, published_at=ts1)
        catalog.publish_run(run_06z, layout=layout, published_at=ts2)
        catalog.publish_run(run_12z, layout=layout, published_at=ts3)

        assert catalog.current_run_id == run_12z
        assert catalog.get_entry(run_00z).status == RunStatus.SUPERSEDED
        assert catalog.get_entry(run_06z).status == RunStatus.SUPERSEDED
        assert catalog.get_entry(run_12z).status == RunStatus.PUBLISHED


class TestQueryHelpers:
    def test_get_entry_missing(self, catalog, run_00z):
        assert catalog.get_entry(run_00z) is None

    def test_published_runs(self, catalog, layout, run_00z, run_06z):
        catalog.publish_run(run_00z, layout=layout)
        catalog.publish_run(run_06z, layout=layout)

        published = catalog.published_runs()
        assert len(published) == 1
        assert published[0].run_id == run_06z

    def test_superseded_runs(self, catalog, layout, run_00z, run_06z):
        catalog.publish_run(run_00z, layout=layout)
        catalog.publish_run(run_06z, layout=layout)

        superseded = catalog.superseded_runs()
        assert len(superseded) == 1
        assert superseded[0].run_id == run_00z


class TestRollback:
    def test_rollback_to_previous(self, catalog, layout, run_00z, run_06z):
        ts1 = datetime(2026, 3, 6, 1, 0, 0, tzinfo=timezone.utc)
        ts2 = datetime(2026, 3, 6, 7, 0, 0, tzinfo=timezone.utc)

        catalog.publish_run(run_00z, layout=layout, published_at=ts1)
        catalog.publish_run(run_06z, layout=layout, published_at=ts2)

        target = catalog.rollback_to(run_00z)
        assert catalog.current_run_id == run_00z
        # 06z was published, now superseded by the rollback
        assert catalog.get_entry(run_06z).status == RunStatus.SUPERSEDED
        # target is re-promoted to PUBLISHED
        assert target.status == RunStatus.PUBLISHED
        assert target.superseded_at is None

    def test_rollback_to_missing_raises(self, catalog, run_00z):
        with pytest.raises(ValueError, match="not found"):
            catalog.rollback_to(run_00z)

    def test_rollback_to_expired_raises(self, catalog, layout, run_00z, run_06z):
        catalog.publish_run(run_00z, layout=layout)
        catalog.publish_run(run_06z, layout=layout)
        catalog.expire_run(run_00z)

        with pytest.raises(ValueError, match="expired"):
            catalog.rollback_to(run_00z)

    def test_rollback_to_current_is_noop(self, catalog, layout, run_00z):
        catalog.publish_run(run_00z, layout=layout)
        target = catalog.rollback_to(run_00z)
        assert catalog.current_run_id == run_00z
        assert target.status == RunStatus.PUBLISHED


class TestExpireRun:
    def test_expire_superseded(self, catalog, layout, run_00z, run_06z):
        catalog.publish_run(run_00z, layout=layout)
        catalog.publish_run(run_06z, layout=layout)

        ts = datetime(2026, 3, 10, 0, 0, 0, tzinfo=timezone.utc)
        entry = catalog.expire_run(run_00z, expired_at=ts)
        assert entry.status == RunStatus.EXPIRED
        assert entry.expired_at == ts

    def test_expire_current_raises(self, catalog, layout, run_00z):
        catalog.publish_run(run_00z, layout=layout)
        with pytest.raises(ValueError, match="Cannot expire current"):
            catalog.expire_run(run_00z)

    def test_expire_missing_raises(self, catalog, run_00z):
        with pytest.raises(ValueError, match="not found"):
            catalog.expire_run(run_00z)

    def test_expire_already_expired_raises(self, catalog, layout, run_00z, run_06z):
        catalog.publish_run(run_00z, layout=layout)
        catalog.publish_run(run_06z, layout=layout)
        catalog.expire_run(run_00z)

        with pytest.raises(ValueError, match="Invalid transition"):
            catalog.expire_run(run_00z)


class TestStatusTransitions:
    def test_published_to_superseded(self, catalog, layout, run_00z, run_06z):
        catalog.publish_run(run_00z, layout=layout)
        catalog.publish_run(run_06z, layout=layout)
        assert catalog.get_entry(run_00z).status == RunStatus.SUPERSEDED

    def test_published_to_expired_directly(self, catalog, layout, run_00z, run_06z):
        """A published run can be expired directly (skip superseded)."""
        catalog.publish_run(run_00z, layout=layout)
        # Make 06z current so 00z isn't current anymore, but 00z stays published
        # Actually publish_run auto-supersedes, so let's test expire on a non-current published
        # We need a different approach: expire a published run that isn't current
        # publish_run always supersedes previous, so published->expired direct
        # only works if we have a more complex scenario.
        # Let's just verify the transition map allows it via _transition directly.
        from weatherman.storage.catalog import _VALID_TRANSITIONS
        assert RunStatus.EXPIRED in _VALID_TRANSITIONS[RunStatus.PUBLISHED]

    def test_superseded_to_published_invalid(self):
        """Cannot go backwards from superseded to published."""
        from weatherman.storage.catalog import _VALID_TRANSITIONS
        assert RunStatus.PUBLISHED not in _VALID_TRANSITIONS[RunStatus.SUPERSEDED]

    def test_expired_is_terminal(self):
        from weatherman.storage.catalog import _VALID_TRANSITIONS
        assert _VALID_TRANSITIONS[RunStatus.EXPIRED] == set()


class TestSerialization:
    def test_empty_catalog_round_trip(self, catalog):
        json_str = catalog.to_json()
        restored = RunCatalog.from_json(json_str)
        assert restored.model == "gfs"
        assert restored.current_run_id is None
        assert restored.runs == []

    def test_populated_catalog_round_trip(self, catalog, layout, run_00z, run_06z):
        ts1 = datetime(2026, 3, 6, 1, 0, 0, tzinfo=timezone.utc)
        ts2 = datetime(2026, 3, 6, 7, 0, 0, tzinfo=timezone.utc)

        catalog.publish_run(run_00z, layout=layout, published_at=ts1, processing_version="1.0.0")
        catalog.publish_run(run_06z, layout=layout, published_at=ts2, processing_version="1.0.1")

        json_str = catalog.to_json()
        restored = RunCatalog.from_json(json_str)

        assert restored.model == "gfs"
        assert restored.current_run_id == run_06z
        assert len(restored.runs) == 2

        e0 = restored.get_entry(run_00z)
        assert e0.status == RunStatus.SUPERSEDED
        assert e0.processing_version == "1.0.0"

        e1 = restored.get_entry(run_06z)
        assert e1.status == RunStatus.PUBLISHED
        assert e1.processing_version == "1.0.1"

    def test_unsupported_schema_version_raises(self):
        data = {"schema_version": 999, "model": "gfs", "runs": []}
        with pytest.raises(ValueError, match="Unsupported catalog schema version"):
            RunCatalog.from_dict(data)

    def test_json_is_valid_json(self, catalog, layout, run_00z):
        catalog.publish_run(run_00z, layout=layout)
        import json
        parsed = json.loads(catalog.to_json())
        assert parsed["schema_version"] == SCHEMA_VERSION
        assert parsed["model"] == "gfs"
        assert len(parsed["runs"]) == 1


class TestAtomicFileIO:
    def test_save_and_load(self, catalog, layout, run_00z, tmp_path):
        catalog.publish_run(run_00z, layout=layout, processing_version="1.0.0")

        path = tmp_path / "catalog.json"
        catalog.save(path)

        assert path.exists()
        # PID-unique tmp file is cleaned up by rename
        import glob
        assert not glob.glob(str(path) + ".tmp.*")

        loaded = RunCatalog.load(path)
        assert loaded.model == "gfs"
        assert loaded.current_run_id == run_00z
        assert len(loaded.runs) == 1

    def test_save_overwrites(self, catalog, layout, run_00z, run_06z, tmp_path):
        """Saving twice overwrites the previous catalog."""
        path = tmp_path / "catalog.json"

        catalog.publish_run(run_00z, layout=layout)
        catalog.save(path)

        catalog.publish_run(run_06z, layout=layout)
        catalog.save(path)

        loaded = RunCatalog.load(path)
        assert loaded.current_run_id == run_06z
        assert len(loaded.runs) == 2


class TestAppendOnlySemantics:
    def test_runs_list_only_grows(self, catalog, layout, run_00z, run_06z, run_12z):
        """The runs list only ever gets longer — entries are never removed."""
        catalog.publish_run(run_00z, layout=layout)
        assert len(catalog.runs) == 1

        catalog.publish_run(run_06z, layout=layout)
        assert len(catalog.runs) == 2

        catalog.publish_run(run_12z, layout=layout)
        assert len(catalog.runs) == 3

        catalog.expire_run(run_00z)
        assert len(catalog.runs) == 3  # still 3, not removed

    def test_rollback_does_not_remove_entries(self, catalog, layout, run_00z, run_06z):
        catalog.publish_run(run_00z, layout=layout)
        catalog.publish_run(run_06z, layout=layout)
        catalog.rollback_to(run_00z)
        assert len(catalog.runs) == 2  # both still present


class TestRollbackEdgeCases:
    """Cover rollback interactions with publish and expire."""

    def test_published_runs_after_rollback(self, catalog, layout, run_00z, run_06z):
        """published_runs() returns the rolled-back-to run."""
        catalog.publish_run(run_00z, layout=layout)
        catalog.publish_run(run_06z, layout=layout)
        catalog.rollback_to(run_00z)

        published = catalog.published_runs()
        assert len(published) == 1
        assert published[0].run_id == run_00z

    def test_publish_after_rollback(self, catalog, layout, run_00z, run_06z, run_12z):
        """publish A -> publish B -> rollback A -> publish C works correctly."""
        ts1 = datetime(2026, 3, 6, 1, 0, 0, tzinfo=timezone.utc)
        ts2 = datetime(2026, 3, 6, 7, 0, 0, tzinfo=timezone.utc)
        ts3 = datetime(2026, 3, 6, 13, 0, 0, tzinfo=timezone.utc)

        catalog.publish_run(run_00z, layout=layout, published_at=ts1)
        catalog.publish_run(run_06z, layout=layout, published_at=ts2)
        catalog.rollback_to(run_00z)
        catalog.publish_run(run_12z, layout=layout, published_at=ts3)

        assert catalog.current_run_id == run_12z
        assert len(catalog.runs) == 3
        # A was re-promoted by rollback, then superseded by C
        assert catalog.get_entry(run_00z).status == RunStatus.SUPERSEDED
        # B was superseded by the rollback
        assert catalog.get_entry(run_06z).status == RunStatus.SUPERSEDED
        # C is the new current
        assert catalog.get_entry(run_12z).status == RunStatus.PUBLISHED

    def test_expire_rolled_back_current_raises(self, catalog, layout, run_00z, run_06z):
        """Cannot expire the current run even after rollback."""
        catalog.publish_run(run_00z, layout=layout)
        catalog.publish_run(run_06z, layout=layout)
        catalog.rollback_to(run_00z)

        with pytest.raises(ValueError, match="Cannot expire current"):
            catalog.expire_run(run_00z)

    def test_at_most_one_published(self, catalog, layout, run_00z, run_06z, run_12z):
        """At any point, at most one entry has PUBLISHED status."""
        ts1 = datetime(2026, 3, 6, 1, 0, 0, tzinfo=timezone.utc)
        ts2 = datetime(2026, 3, 6, 7, 0, 0, tzinfo=timezone.utc)
        ts3 = datetime(2026, 3, 6, 13, 0, 0, tzinfo=timezone.utc)

        catalog.publish_run(run_00z, layout=layout, published_at=ts1)
        assert len(catalog.published_runs()) == 1

        catalog.publish_run(run_06z, layout=layout, published_at=ts2)
        assert len(catalog.published_runs()) == 1

        catalog.rollback_to(run_00z)
        assert len(catalog.published_runs()) == 1

        catalog.publish_run(run_12z, layout=layout, published_at=ts3)
        assert len(catalog.published_runs()) == 1
