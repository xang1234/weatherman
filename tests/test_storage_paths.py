"""Tests for storage path construction and RunID validation."""

from datetime import datetime, timezone

import pytest

from weatherman.storage.paths import RunID, StorageLayout
from weatherman.storage.config import StorageConfig


class TestRunID:
    def test_valid_run_id(self):
        rid = RunID("20260306T00Z")
        assert rid.value == "20260306T00Z"
        assert str(rid) == "20260306T00Z"

    def test_valid_run_id_all_cycles(self):
        for hour in (0, 6, 12, 18):
            rid = RunID(f"20260306T{hour:02d}Z")
            assert rid.cycle_hour == hour

    def test_invalid_format_raises(self):
        with pytest.raises(ValueError, match="Invalid run ID"):
            RunID("2026-03-06T00Z")
        with pytest.raises(ValueError, match="Invalid run ID"):
            RunID("20260306T0Z")  # single digit hour
        with pytest.raises(ValueError, match="Invalid run ID"):
            RunID("20260306")
        with pytest.raises(ValueError, match="Invalid run ID"):
            RunID("")

    def test_from_cycle(self):
        dt = datetime(2026, 3, 6, tzinfo=timezone.utc)
        rid = RunID.from_cycle(dt, 12)
        assert rid.value == "20260306T12Z"

    def test_from_cycle_invalid_hour(self):
        dt = datetime(2026, 3, 6, tzinfo=timezone.utc)
        with pytest.raises(ValueError, match="Cycle hour must be 0-23"):
            RunID.from_cycle(dt, 25)

    def test_from_datetime(self):
        dt = datetime(2026, 3, 6, 18, 30, 0, tzinfo=timezone.utc)
        rid = RunID.from_datetime(dt)
        assert rid.value == "20260306T18Z"

    def test_date_str(self):
        rid = RunID("20260306T00Z")
        assert rid.date_str == "20260306"

    def test_cycle_hour(self):
        assert RunID("20260306T18Z").cycle_hour == 18

    def test_as_datetime(self):
        rid = RunID("20260306T12Z")
        dt = rid.as_datetime
        assert dt == datetime(2026, 3, 6, 12, 0, 0, tzinfo=timezone.utc)

    def test_lexicographic_ordering(self):
        """Run IDs sort chronologically when sorted lexicographically."""
        ids = [
            RunID("20260306T18Z"),
            RunID("20260306T00Z"),
            RunID("20260307T06Z"),
            RunID("20260306T12Z"),
        ]
        sorted_ids = sorted(ids)
        assert [r.value for r in sorted_ids] == [
            "20260306T00Z",
            "20260306T12Z",
            "20260306T18Z",
            "20260307T06Z",
        ]

    def test_rejects_invalid_date(self):
        with pytest.raises(ValueError, match="out of range"):
            RunID("20261306T00Z")  # month 13

    def test_rejects_invalid_hour(self):
        with pytest.raises(ValueError, match="out of range"):
            RunID("20260306T25Z")  # hour 25

    def test_ordering_gt(self):
        assert RunID("20260307T00Z") > RunID("20260306T00Z")

    def test_ordering_le(self):
        assert RunID("20260306T00Z") <= RunID("20260306T00Z")
        assert RunID("20260306T00Z") <= RunID("20260306T12Z")

    def test_ordering_ge(self):
        assert RunID("20260306T12Z") >= RunID("20260306T00Z")
        assert RunID("20260306T12Z") >= RunID("20260306T12Z")

    def test_immutable(self):
        rid = RunID("20260306T00Z")
        with pytest.raises(AttributeError):
            rid.value = "20260307T00Z"  # type: ignore[misc]


class TestStorageLayout:
    @pytest.fixture
    def gfs(self):
        return StorageLayout("gfs")

    @pytest.fixture
    def run_id(self):
        return RunID("20260306T00Z")

    def test_model_name_validation(self):
        StorageLayout("gfs")  # ok
        StorageLayout("gfs_wave")  # ok - underscores allowed
        StorageLayout("icon_global")  # ok
        with pytest.raises(ValueError, match="lowercase alphanumeric"):
            StorageLayout("")
        with pytest.raises(ValueError, match="lowercase alphanumeric"):
            StorageLayout("gfs-wave")  # hyphens not allowed (use underscores)
        with pytest.raises(ValueError, match="lowercase alphanumeric"):
            StorageLayout("GFS")  # uppercase not allowed

    def test_model_prefix(self, gfs):
        assert gfs.model_prefix == "models/gfs"

    def test_catalog_path(self, gfs):
        assert gfs.catalog_path == "models/gfs/catalog.json"

    def test_run_prefix(self, gfs, run_id):
        assert gfs.run_prefix(run_id) == "models/gfs/runs/20260306T00Z"

    def test_zarr_path(self, gfs, run_id):
        assert (
            gfs.zarr_path(run_id)
            == "models/gfs/runs/20260306T00Z/zarr/20260306T00Z.zarr"
        )

    def test_cog_path(self, gfs, run_id):
        assert (
            gfs.cog_path(run_id, "temperature", 6)
            == "models/gfs/runs/20260306T00Z/cogs/temperature/006.tif"
        )

    def test_cog_path_zero_padded(self, gfs, run_id):
        """Forecast hours are zero-padded to 3 digits for consistent sorting."""
        assert gfs.cog_path(run_id, "wind_speed", 0).endswith("000.tif")
        assert gfs.cog_path(run_id, "wind_speed", 120).endswith("120.tif")

    def test_vectors_prefix(self, gfs, run_id):
        assert (
            gfs.vectors_prefix(run_id)
            == "models/gfs/runs/20260306T00Z/vectors"
        )

    def test_stac_item_path(self, gfs, run_id):
        assert (
            gfs.stac_item_path(run_id)
            == "models/gfs/runs/20260306T00Z/stac/item.json"
        )

    def test_manifest_path(self, gfs, run_id):
        assert (
            gfs.manifest_path(run_id)
            == "models/gfs/runs/20260306T00Z/ui/manifest.json"
        )

    # Staging paths mirror published paths under staging/

    def test_staging_prefix(self, gfs, run_id):
        assert (
            gfs.staging_prefix(run_id)
            == "models/gfs/staging/20260306T00Z"
        )

    def test_staging_zarr_path(self, gfs, run_id):
        assert (
            gfs.staging_zarr_path(run_id)
            == "models/gfs/staging/20260306T00Z/zarr/20260306T00Z.zarr"
        )

    def test_staging_cog_path(self, gfs, run_id):
        assert (
            gfs.staging_cog_path(run_id, "temperature", 6)
            == "models/gfs/staging/20260306T00Z/cogs/temperature/006.tif"
        )

    def test_staging_vs_published_are_distinct(self, gfs, run_id):
        """Staging and published paths never overlap."""
        assert "staging" in gfs.staging_prefix(run_id)
        assert "staging" not in gfs.run_prefix(run_id)
        assert "runs" in gfs.run_prefix(run_id)
        assert "runs" not in gfs.staging_prefix(run_id)

    def test_cog_path_rejects_path_traversal(self, gfs, run_id):
        with pytest.raises(ValueError, match="lowercase alphanumeric"):
            gfs.cog_path(run_id, "../../etc/passwd", 0)

    def test_cog_path_rejects_invalid_layer(self, gfs, run_id):
        with pytest.raises(ValueError):
            gfs.cog_path(run_id, "My Layer", 0)

    def test_staging_cog_path_rejects_invalid_layer(self, gfs, run_id):
        with pytest.raises(ValueError):
            gfs.staging_cog_path(run_id, "../bad", 0)


class TestStorageConfig:
    def test_full_path_no_prefix(self):
        cfg = StorageConfig(bucket="wx-data")
        assert cfg.full_path("models/gfs/catalog.json") == "models/gfs/catalog.json"

    def test_full_path_with_prefix(self):
        cfg = StorageConfig(bucket="wx-data", prefix="prod")
        assert cfg.full_path("models/gfs/catalog.json") == "prod/models/gfs/catalog.json"

    def test_s3_uri(self):
        cfg = StorageConfig(bucket="wx-data")
        assert (
            cfg.s3_uri("models/gfs/catalog.json")
            == "s3://wx-data/models/gfs/catalog.json"
        )

    def test_s3_uri_with_prefix(self):
        cfg = StorageConfig(bucket="wx-data", prefix="prod")
        assert (
            cfg.s3_uri("models/gfs/catalog.json")
            == "s3://wx-data/prod/models/gfs/catalog.json"
        )

    def test_integration_layout_plus_config(self):
        """StorageLayout paths compose cleanly with StorageConfig."""
        cfg = StorageConfig(bucket="maritime-wx", prefix="v1")
        layout = StorageLayout("gfs")
        run_id = RunID("20260306T00Z")

        uri = cfg.s3_uri(layout.cog_path(run_id, "temperature", 6))
        assert uri == (
            "s3://maritime-wx/v1/models/gfs/runs/20260306T00Z"
            "/cogs/temperature/006.tif"
        )
