"""Tests for per-model publish locks."""

import threading
import time

import pytest
import sqlalchemy as sa

from weatherman.storage.catalog import RunCatalog
from weatherman.storage.lifecycle import RunLifecycle, RunState
from weatherman.storage.locks import (
    FilePublishLock,
    NullPublishLock,
    PublishLockTimeout,
)
from weatherman.storage.object_store import LocalObjectStore
from weatherman.storage.paths import RunID, StorageLayout
from weatherman.storage.publish import publish_run


MODEL = "gfs"
VERSION = "1.0.0"


def _stage_artifacts(store, layout, run_id):
    prefix = layout.staging_prefix(run_id)
    store.write_bytes(f"{prefix}/zarr/{run_id}.zarr/.zmetadata", b'{"zarr": true}')
    store.write_bytes(f"{prefix}/cogs/wind_speed/000.tif", b"fake-tif")
    store.write_bytes(f"{prefix}/ui/manifest.json", b'{"layers": []}')


def _advance_to_validated(lifecycle, run_id, version=VERSION):
    lifecycle.register(MODEL, run_id, version)
    for s in [RunState.INGESTING, RunState.STAGED, RunState.VALIDATED]:
        lifecycle.transition(MODEL, run_id, version, s)


@pytest.fixture
def store(tmp_path):
    return LocalObjectStore(tmp_path / "objects")


@pytest.fixture
def layout():
    return StorageLayout(MODEL)


@pytest.fixture
def lock_dir(tmp_path):
    return tmp_path / "locks"


class TestFilePublishLock:
    def test_lock_creates_lock_file(self, lock_dir):
        lock = FilePublishLock(lock_dir)
        with lock("gfs"):
            assert (lock_dir / "gfs.publish.lock").exists()

    def test_lock_is_reentrant_across_calls(self, lock_dir):
        """Same process can acquire sequentially without deadlock."""
        lock = FilePublishLock(lock_dir)
        with lock("gfs"):
            pass
        with lock("gfs"):
            pass

    def test_different_models_do_not_block(self, lock_dir):
        """Locks for different models are independent."""
        lock = FilePublishLock(lock_dir)
        with lock("gfs"):
            with lock("ecmwf"):
                assert (lock_dir / "gfs.publish.lock").exists()
                assert (lock_dir / "ecmwf.publish.lock").exists()

    def test_timeout_raises(self, lock_dir):
        """A second holder times out if the first doesn't release."""
        lock = FilePublishLock(lock_dir, timeout=0.3)
        acquired = threading.Event()
        release = threading.Event()

        def hold_lock():
            with lock("gfs"):
                acquired.set()
                release.wait(timeout=5)

        t = threading.Thread(target=hold_lock)
        t.start()
        acquired.wait(timeout=2)

        try:
            with pytest.raises(PublishLockTimeout, match="gfs"):
                with lock("gfs"):
                    pass  # should not reach here
        finally:
            release.set()
            t.join(timeout=2)

    def test_lock_released_on_exception(self, lock_dir):
        """Lock is released even if the body raises."""
        lock = FilePublishLock(lock_dir, timeout=1.0)

        with pytest.raises(RuntimeError, match="boom"):
            with lock("gfs"):
                raise RuntimeError("boom")

        # Should be able to re-acquire immediately
        with lock("gfs"):
            pass

    def test_serializes_concurrent_publishes(self, lock_dir):
        """Two threads publishing the same model run sequentially."""
        lock = FilePublishLock(lock_dir, timeout=5.0)
        order = []
        barrier = threading.Barrier(2, timeout=2)

        def worker(name):
            barrier.wait()
            with lock("gfs"):
                order.append(f"{name}_start")
                time.sleep(0.05)
                order.append(f"{name}_end")

        threads = [
            threading.Thread(target=worker, args=(n,))
            for n in ("A", "B")
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        # One must fully complete before the other starts
        assert order[0].endswith("_start")
        assert order[1].endswith("_end")
        assert order[0][0] == order[1][0]  # same worker


class TestNullPublishLock:
    def test_null_lock_is_noop(self):
        lock = NullPublishLock()
        with lock("gfs"):
            pass  # should not raise


class TestPublishRunWithLock:
    def test_publish_with_file_lock(self, store, layout, lock_dir):
        catalog = RunCatalog.new(MODEL)
        engine = sa.create_engine("sqlite:///:memory:")
        lifecycle = RunLifecycle(engine)
        lifecycle.create_tables()

        run_id = RunID("20260306T00Z")
        _stage_artifacts(store, layout, run_id)
        _advance_to_validated(lifecycle, run_id)

        lock = FilePublishLock(lock_dir)
        publish_run(
            store=store, layout=layout, catalog=catalog,
            lifecycle=lifecycle, run_id=run_id,
            processing_version=VERSION, lock=lock,
        )

        assert catalog.current_run_id == run_id
        assert lifecycle.get_state(MODEL, run_id, VERSION) == RunState.PUBLISHED

    def test_publish_defaults_to_null_lock(self, store, layout):
        """Omitting lock= still works (backward compatible)."""
        catalog = RunCatalog.new(MODEL)
        engine = sa.create_engine("sqlite:///:memory:")
        lifecycle = RunLifecycle(engine)
        lifecycle.create_tables()

        run_id = RunID("20260306T00Z")
        _stage_artifacts(store, layout, run_id)
        _advance_to_validated(lifecycle, run_id)

        publish_run(
            store=store, layout=layout, catalog=catalog,
            lifecycle=lifecycle, run_id=run_id,
            processing_version=VERSION,
        )

        assert catalog.current_run_id == run_id
