"""Tests for the atomic publish pipeline (staging → published + catalog update)."""

import pytest
import sqlalchemy as sa

from weatherman.storage.catalog import RunCatalog, RunStatus
from weatherman.storage.lifecycle import RunLifecycle, RunState
from weatherman.storage.object_store import LocalObjectStore
from weatherman.storage.paths import RunID, StorageLayout
from weatherman.storage.publish import (
    publish_run,
    PublishError,
    PublishVerificationError,
)


RUN = RunID("20260306T00Z")
RUN2 = RunID("20260306T12Z")
MODEL = "gfs"
VERSION = "1.0.0"


@pytest.fixture
def store(tmp_path):
    return LocalObjectStore(tmp_path)


@pytest.fixture
def layout():
    return StorageLayout(MODEL)


@pytest.fixture
def catalog():
    return RunCatalog.new(MODEL)


@pytest.fixture
def lifecycle():
    engine = sa.create_engine("sqlite:///:memory:")
    lc = RunLifecycle(engine)
    lc.create_tables()
    return lc


def _stage_artifacts(store, layout, run_id):
    """Create fake staging artifacts."""
    prefix = layout.staging_prefix(run_id)
    store.write_bytes(f"{prefix}/zarr/{run_id}.zarr/.zmetadata", b'{"zarr": true}')
    store.write_bytes(f"{prefix}/cogs/wind_speed/000.tif", b"fake-tif-000")
    store.write_bytes(f"{prefix}/cogs/wind_speed/003.tif", b"fake-tif-003")
    store.write_bytes(f"{prefix}/stac/item.json", b'{"type": "Feature"}')
    store.write_bytes(f"{prefix}/ui/manifest.json", b'{"layers": []}')


def _advance_to_validated(lifecycle, run_id, version=VERSION):
    """Register a run and advance lifecycle to VALIDATED."""
    lifecycle.register(MODEL, run_id, version)
    for s in [RunState.INGESTING, RunState.STAGED, RunState.VALIDATED]:
        lifecycle.transition(MODEL, run_id, version, s)


class TestPublishHappyPath:
    def test_publish_copies_artifacts_to_published_location(
        self, store, layout, catalog, lifecycle
    ):
        _stage_artifacts(store, layout, RUN)
        _advance_to_validated(lifecycle, RUN)

        publish_run(
            store=store, layout=layout, catalog=catalog,
            lifecycle=lifecycle, run_id=RUN, processing_version=VERSION,
        )

        # Artifacts should exist under runs/
        assert store.exists(f"{layout.run_prefix(RUN)}/zarr/{RUN}.zarr/.zmetadata")
        assert store.exists(f"{layout.run_prefix(RUN)}/cogs/wind_speed/000.tif")
        assert store.exists(f"{layout.run_prefix(RUN)}/cogs/wind_speed/003.tif")
        assert store.exists(f"{layout.run_prefix(RUN)}/stac/item.json")
        assert store.exists(f"{layout.run_prefix(RUN)}/ui/manifest.json")

    def test_publish_updates_catalog(self, store, layout, catalog, lifecycle):
        _stage_artifacts(store, layout, RUN)
        _advance_to_validated(lifecycle, RUN)

        publish_run(
            store=store, layout=layout, catalog=catalog,
            lifecycle=lifecycle, run_id=RUN, processing_version=VERSION,
        )

        assert catalog.current_run_id == RUN
        entry = catalog.get_entry(RUN)
        assert entry is not None
        assert entry.status == RunStatus.PUBLISHED

    def test_publish_saves_catalog_to_store(self, store, layout, catalog, lifecycle):
        _stage_artifacts(store, layout, RUN)
        _advance_to_validated(lifecycle, RUN)

        publish_run(
            store=store, layout=layout, catalog=catalog,
            lifecycle=lifecycle, run_id=RUN, processing_version=VERSION,
        )

        assert store.exists(layout.catalog_path)
        saved = RunCatalog.from_json(
            store.read_bytes(layout.catalog_path).decode()
        )
        assert saved.current_run_id == RUN

    def test_publish_transitions_lifecycle_to_published(
        self, store, layout, catalog, lifecycle
    ):
        _stage_artifacts(store, layout, RUN)
        _advance_to_validated(lifecycle, RUN)

        publish_run(
            store=store, layout=layout, catalog=catalog,
            lifecycle=lifecycle, run_id=RUN, processing_version=VERSION,
        )

        assert lifecycle.get_state(MODEL, RUN, VERSION) == RunState.PUBLISHED

    def test_publish_cleans_up_staging(self, store, layout, catalog, lifecycle):
        _stage_artifacts(store, layout, RUN)
        _advance_to_validated(lifecycle, RUN)

        staging_prefix = layout.staging_prefix(RUN)
        assert len(store.list_keys(staging_prefix)) == 5

        publish_run(
            store=store, layout=layout, catalog=catalog,
            lifecycle=lifecycle, run_id=RUN, processing_version=VERSION,
        )

        assert store.list_keys(staging_prefix) == []

    def test_artifact_data_preserved(self, store, layout, catalog, lifecycle):
        _stage_artifacts(store, layout, RUN)
        _advance_to_validated(lifecycle, RUN)

        publish_run(
            store=store, layout=layout, catalog=catalog,
            lifecycle=lifecycle, run_id=RUN, processing_version=VERSION,
        )

        data = store.read_bytes(f"{layout.run_prefix(RUN)}/cogs/wind_speed/000.tif")
        assert data == b"fake-tif-000"


class TestPublishSupersedes:
    def test_second_publish_supersedes_first(
        self, store, layout, catalog, lifecycle
    ):
        # Publish first run
        _stage_artifacts(store, layout, RUN)
        _advance_to_validated(lifecycle, RUN)
        publish_run(
            store=store, layout=layout, catalog=catalog,
            lifecycle=lifecycle, run_id=RUN, processing_version=VERSION,
        )

        # Publish second run
        _stage_artifacts(store, layout, RUN2)
        _advance_to_validated(lifecycle, RUN2)
        publish_run(
            store=store, layout=layout, catalog=catalog,
            lifecycle=lifecycle, run_id=RUN2, processing_version=VERSION,
        )

        assert catalog.current_run_id == RUN2
        assert catalog.get_entry(RUN).status == RunStatus.SUPERSEDED
        assert catalog.get_entry(RUN2).status == RunStatus.PUBLISHED


class TestPublishFailures:
    def test_no_staging_artifacts_raises(self, store, layout, catalog, lifecycle):
        _advance_to_validated(lifecycle, RUN)

        with pytest.raises(PublishError, match="No staging artifacts"):
            publish_run(
                store=store, layout=layout, catalog=catalog,
                lifecycle=lifecycle, run_id=RUN, processing_version=VERSION,
            )

    def test_catalog_not_updated_on_copy_failure(
        self, store, layout, catalog, lifecycle
    ):
        """If artifact copy fails, catalog should remain unchanged."""
        _stage_artifacts(store, layout, RUN)
        _advance_to_validated(lifecycle, RUN)

        # Make the store fail on copy by making the destination unwritable
        class FailingStore(LocalObjectStore):
            def __init__(self, root):
                super().__init__(root)
                self._call_count = 0

            def copy(self, src, dst):
                self._call_count += 1
                if self._call_count > 2:
                    raise OSError("Disk full")
                super().copy(src, dst)

        failing_store = FailingStore(store._root)

        with pytest.raises(OSError, match="Disk full"):
            publish_run(
                store=failing_store, layout=layout, catalog=catalog,
                lifecycle=lifecycle, run_id=RUN, processing_version=VERSION,
            )

        # Catalog should not have been updated
        assert catalog.current_run_id is None
        assert catalog.get_entry(RUN) is None

    def test_verification_failure_does_not_update_catalog(
        self, store, layout, catalog, lifecycle
    ):
        _stage_artifacts(store, layout, RUN)
        _advance_to_validated(lifecycle, RUN)

        # Store that copies but then claims files don't exist
        class VerifyFailStore(LocalObjectStore):
            def exists(self, key):
                if "runs/" in key:
                    return False
                return super().exists(key)

        failing_store = VerifyFailStore(store._root)

        with pytest.raises(PublishVerificationError, match="missing after copy"):
            publish_run(
                store=failing_store, layout=layout, catalog=catalog,
                lifecycle=lifecycle, run_id=RUN, processing_version=VERSION,
            )

        assert catalog.current_run_id is None

    def test_staging_cleanup_failure_is_not_fatal(
        self, store, layout, catalog, lifecycle
    ):
        """Staging cleanup failure should not prevent publish from succeeding."""
        _stage_artifacts(store, layout, RUN)
        _advance_to_validated(lifecycle, RUN)

        class NoDeleteStore(LocalObjectStore):
            def delete(self, key):
                raise OSError("Permission denied")

        no_delete_store = NoDeleteStore(store._root)

        # Should succeed despite delete failures
        publish_run(
            store=no_delete_store, layout=layout, catalog=catalog,
            lifecycle=lifecycle, run_id=RUN, processing_version=VERSION,
        )

        assert catalog.current_run_id == RUN
        assert lifecycle.get_state(MODEL, RUN, VERSION) == RunState.PUBLISHED


class TestLocalObjectStore:
    def test_list_keys(self, store, layout):
        _stage_artifacts(store, layout, RUN)
        keys = store.list_keys(layout.staging_prefix(RUN))
        assert len(keys) == 5

    def test_list_keys_empty_prefix(self, store):
        assert store.list_keys("nonexistent/prefix") == []

    def test_copy_creates_parent_dirs(self, store):
        store.write_bytes("src/file.txt", b"hello")
        store.copy("src/file.txt", "deep/nested/dir/file.txt")
        assert store.read_bytes("deep/nested/dir/file.txt") == b"hello"

    def test_delete_missing_key_no_error(self, store):
        store.delete("nonexistent/key")  # should not raise

    def test_exists(self, store):
        assert not store.exists("foo.txt")
        store.write_bytes("foo.txt", b"bar")
        assert store.exists("foo.txt")

    def test_write_read_roundtrip(self, store):
        store.write_bytes("data/test.bin", b"\x00\x01\x02")
        assert store.read_bytes("data/test.bin") == b"\x00\x01\x02"
