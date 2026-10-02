"""The pipeline records each run's tile encoding ranges in its manifest (#83)."""

from __future__ import annotations

import json
from pathlib import Path

from scripts.run_pipeline import step_write_manifest
from weatherman.storage.manifest import ValueRange
from weatherman.storage.object_store import LocalObjectStore
from weatherman.storage.paths import RunID, StorageLayout


def test_manifest_carries_the_ranges_the_tiles_used(tmp_path: Path):
    run, layout, store = RunID("20260306T00Z"), StorageLayout("gfs"), LocalObjectStore(tmp_path)
    ranges = {"temperature": ValueRange(-55.0, 55.0), "wind_u": ValueRange(-60.0, 60.0)}
    step_write_manifest(run, [0, 3], store, layout, {"temperature", "wind_u"}, data_ranges=ranges)

    manifest = json.loads(store.read_bytes(layout.staging_manifest_path(run)))
    assert manifest["data_ranges"] == {
        "temperature": {"min": -55.0, "max": 55.0},
        "wind_u": {"min": -60.0, "max": 60.0},
    }
