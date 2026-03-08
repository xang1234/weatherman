"""Tests for the local dev pipeline publish step."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone

from weatherman.events.router import get_event_bus, init_event_bus, shutdown_event_bus
from weatherman.storage.catalog import RunCatalog
from weatherman.storage.object_store import LocalObjectStore
from weatherman.storage.paths import RunID, StorageLayout

from scripts.run_pipeline import PROCESSING_VERSION, step_publish_run

MODEL = "gfs"
RUN = RunID("20260309T00Z")


def run(coro):
    return asyncio.run(coro)


def _stage_pipeline_artifacts(store: LocalObjectStore, layout: StorageLayout, run_id: RunID) -> None:
    store.write_bytes(f"{layout.staging_zarr_path(run_id)}/.zmetadata", b'{"zarr": true}')
    store.write_bytes(layout.staging_cog_path(run_id, "temperature", 0), b"fake-cog")
    store.write_bytes(layout.staging_manifest_path(run_id), b'{"layers": ["temperature"]}')


def test_step_publish_run_uses_canonical_publish_and_emits_event(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    store = LocalObjectStore(data_dir)
    layout = StorageLayout(MODEL)
    _stage_pipeline_artifacts(store, layout, RUN)

    init_event_bus()
    try:
        bus = get_event_bus()

        async def _test() -> None:
            async with bus.subscribe("default") as queue:
                step_publish_run(RUN, store, layout, data_dir)

                assert store.exists(f"{layout.run_prefix(RUN)}/zarr/{RUN}.zarr/.zmetadata")
                assert store.exists(layout.cog_path(RUN, "temperature", 0))
                assert store.exists(layout.manifest_path(RUN))
                assert store.list_keys(layout.staging_prefix(RUN)) == []

                catalog = RunCatalog.from_json(
                    store.read_bytes(layout.catalog_path).decode("utf-8")
                )
                assert catalog.current_run_id == RUN
                entry = catalog.get_entry(RUN)
                assert entry is not None
                assert entry.processing_version == PROCESSING_VERSION
                assert entry.published_at <= datetime.now(timezone.utc)

                event = await asyncio.wait_for(queue.get(), timeout=1)
                assert event.event == "run.published"
                payload = json.loads(event.data)
                assert payload["model"] == MODEL
                assert payload["run_id"] == str(RUN)

        run(_test())
    finally:
        shutdown_event_bus()
