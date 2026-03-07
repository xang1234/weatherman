"""Tests for the GRIB2 to Zarr conversion pipeline."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import numpy as np
import pytest
import xarray as xr
import zarr

from weatherman.processing.geo import normalize_grid, normalize_longitude
from weatherman.processing.grib2_to_zarr import (
    ConversionResult,
    convert_grib2_to_zarr,
    finalize_store,
    ingest_grib2_file,
    init_zarr_store,
)
from weatherman.storage.zarr_schema import (
    ChunkSpec,
    GridResolution,
    VariableDef,
    ZarrSchema,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

# Small schema for fast tests (not the full 721×1440 GFS grid)
# 45° step: lat = [90, 45, 0, -45, -90] (5 pts), lon = [-180..135] (8 pts)
SMALL_GRID_SPECS = {
    "step": 45.0,
    "lat_count": 5,
    "lon_count": 8,
}


@pytest.fixture
def small_schema():
    """A small schema for testing (5 lat × 8 lon, 3 forecast hours)."""
    # Patch grid specs temporarily for the small test grid
    from weatherman.storage import zarr_schema

    original = zarr_schema._GRID_SPECS.copy()
    test_grid = GridResolution.GFS_025  # reuse the enum value
    zarr_schema._GRID_SPECS[test_grid] = SMALL_GRID_SPECS

    schema = ZarrSchema(
        grid=test_grid,
        forecast_hours=(0, 3, 6),
        variables={
            "tmp_2m": VariableDef(
                name="tmp_2m",
                long_name="Temperature at 2m",
                units="K",
                grib_key=":TMP:2 m above ground:",
                level="2 m above ground",
                chunks=ChunkSpec(time=1, lat=4, lon=4),
            ),
            "ugrd_10m": VariableDef(
                name="ugrd_10m",
                long_name="U-wind at 10m",
                units="m/s",
                grib_key=":UGRD:10 m above ground:",
                level="10 m above ground",
                chunks=ChunkSpec(time=1, lat=4, lon=4),
            ),
        },
        global_attrs={"Conventions": "CF-1.8", "source": "test"},
    )

    yield schema

    # Restore original specs
    zarr_schema._GRID_SPECS.update(original)


def _make_fake_grib_dataset(
    n_lat: int, n_lon: int, lon_convention: str = "0_360"
) -> xr.Dataset:
    """Create a synthetic dataset mimicking cfgrib output.

    Returns a fresh dataset each call (safe for repeated use in mocks).
    """
    data = np.random.randn(n_lat, n_lon).astype(np.float32)

    if lon_convention == "0_360":
        lon = np.linspace(0, 360 - (360 / n_lon), n_lon)
    else:
        lon = np.linspace(-180, 180 - (360 / n_lon), n_lon)

    lat = np.linspace(90, -90, n_lat)

    return xr.Dataset(
        {"t2m": xr.DataArray(data, dims=["latitude", "longitude"])},
        coords={"latitude": lat, "longitude": lon},
    )


def _mock_open_dataset(n_lat: int, n_lon: int, lon_convention: str = "0_360"):
    """Return a mock for xr.open_dataset that supports context manager protocol.

    Each call produces a fresh dataset to avoid closed-dataset reuse bugs.
    """
    from unittest.mock import MagicMock

    def _open(*args, **kwargs):
        ds = _make_fake_grib_dataset(n_lat, n_lon, lon_convention)
        cm = MagicMock(wraps=ds)
        cm.__enter__ = MagicMock(return_value=ds)
        cm.__exit__ = MagicMock(return_value=False)
        return cm

    return _open


# ---------------------------------------------------------------------------
# init_zarr_store tests
# ---------------------------------------------------------------------------

class TestInitZarrStore:
    def test_creates_store_directory(self, tmp_path, small_schema):
        store = tmp_path / "test.zarr"
        result = init_zarr_store(small_schema, store)
        assert result == store
        assert store.exists()

    def test_creates_coordinate_arrays(self, tmp_path, small_schema):
        store = init_zarr_store(small_schema, tmp_path / "test.zarr")
        root = zarr.open_group(str(store), mode="r")

        # Time coordinate
        time_arr = root["time"][:]
        np.testing.assert_array_equal(time_arr, [0, 3, 6])
        assert root["time"].attrs["units"] == "hours since model init time"

        # Lat coordinate
        lat_arr = root["lat"][:]
        assert lat_arr[0] == pytest.approx(90.0)
        assert lat_arr[-1] == pytest.approx(-90.0)
        assert len(lat_arr) == 5

        # Lon coordinate
        lon_arr = root["lon"][:]
        assert lon_arr[0] == pytest.approx(-180.0)
        assert len(lon_arr) == 8

    def test_creates_data_variables(self, tmp_path, small_schema):
        store = init_zarr_store(small_schema, tmp_path / "test.zarr")
        root = zarr.open_group(str(store), mode="r")

        assert "tmp_2m" in dict(root.members())
        assert "ugrd_10m" in dict(root.members())

        tmp = root["tmp_2m"]
        assert tmp.shape == (3, 5, 8)
        assert tmp.metadata.dimension_names == ("time", "lat", "lon")
        assert tmp.attrs["units"] == "K"
        assert tmp.attrs["level"] == "2 m above ground"

    def test_data_variables_filled_with_nan(self, tmp_path, small_schema):
        store = init_zarr_store(small_schema, tmp_path / "test.zarr")
        root = zarr.open_group(str(store), mode="r")
        data = root["tmp_2m"][:]
        assert np.all(np.isnan(data))

    def test_global_attributes(self, tmp_path, small_schema):
        store = init_zarr_store(small_schema, tmp_path / "test.zarr")
        root = zarr.open_group(str(store), mode="r")
        assert root.attrs["Conventions"] == "CF-1.8"
        assert root.attrs["source"] == "test"

    def test_creates_parent_directories(self, tmp_path, small_schema):
        store = tmp_path / "deep" / "nested" / "path" / "test.zarr"
        init_zarr_store(small_schema, store)
        assert store.exists()


# ---------------------------------------------------------------------------
# normalize_longitude tests
# ---------------------------------------------------------------------------

class TestNormalizeLongitude:
    def test_rolls_0_360_to_neg180_180(self):
        n_lon = 8
        src_lon = np.linspace(0, 360 - (360 / n_lon), n_lon)
        # Data: values 0..7 so we can track the roll
        data = np.arange(n_lon, dtype=np.float32).reshape(1, n_lon)

        rolled = normalize_longitude(data, src_lon)

        # 180° is at index 4 (0, 45, 90, 135, 180, 225, 270, 315)
        # After roll: [180, 225, 270, 315, 0, 45, 90, 135] → [4, 5, 6, 7, 0, 1, 2, 3]
        expected = np.array([[4, 5, 6, 7, 0, 1, 2, 3]], dtype=np.float32)
        np.testing.assert_array_equal(rolled, expected)

    def test_preserves_neg180_180(self):
        n_lon = 8
        src_lon = np.linspace(-180, 180 - (360 / n_lon), n_lon)
        data = np.arange(n_lon, dtype=np.float32).reshape(1, n_lon)

        result = normalize_longitude(data, src_lon)
        np.testing.assert_array_equal(result, data)

    def test_handles_2d_data(self):
        n_lat, n_lon = 3, 8
        src_lon = np.linspace(0, 360 - (360 / n_lon), n_lon)
        data = np.arange(n_lat * n_lon, dtype=np.float32).reshape(n_lat, n_lon)

        rolled = normalize_longitude(data, src_lon)
        assert rolled.shape == data.shape
        # Each row should be rolled the same way
        for row in range(n_lat):
            assert rolled[row, 0] == data[row, 4]


# ---------------------------------------------------------------------------
# ingest_grib2_file tests
# ---------------------------------------------------------------------------

class TestIngestGrib2File:
    def test_ingests_single_file(self, tmp_path, small_schema):
        store = init_zarr_store(small_schema, tmp_path / "test.zarr")

        grib_file = tmp_path / "test.grib2"
        grib_file.touch()

        # Build expected data using the same normalization path as production
        ref_ds = _make_fake_grib_dataset(5, 8, lon_convention="0_360")
        expected_result = normalize_grid(
            ref_ds["t2m"].values,
            ref_ds.coords["latitude"].values,
            ref_ds.coords["longitude"].values,
        )
        expected_data = expected_result.data

        # Mock returns this exact dataset (as a context manager)
        from unittest.mock import MagicMock
        def _open(*a, **kw):
            cm = MagicMock(wraps=ref_ds)
            cm.__enter__ = MagicMock(return_value=ref_ds)
            cm.__exit__ = MagicMock(return_value=False)
            return cm

        with patch("weatherman.processing.grib2_to_zarr.xr.open_dataset", side_effect=_open):
            ingest_grib2_file(store, small_schema, "tmp_2m", 0, grib_file)

        root = zarr.open_group(str(store), mode="r")
        written = root["tmp_2m"][0, :, :]
        np.testing.assert_array_almost_equal(written, expected_data)

    def test_writes_to_correct_time_index(self, tmp_path, small_schema):
        store = init_zarr_store(small_schema, tmp_path / "test.zarr")

        grib_file = tmp_path / "test.grib2"
        grib_file.touch()

        with patch("weatherman.processing.grib2_to_zarr.xr.open_dataset",
                    side_effect=_mock_open_dataset(5, 8, "neg180_180")):
            # Write to forecast hour 6 (time index 2)
            ingest_grib2_file(store, small_schema, "tmp_2m", 6, grib_file)

        root = zarr.open_group(str(store), mode="r")
        # Time indices 0 and 1 should still be NaN
        assert np.all(np.isnan(root["tmp_2m"][0, :, :]))
        assert np.all(np.isnan(root["tmp_2m"][1, :, :]))
        # Time index 2 should have data
        assert not np.any(np.isnan(root["tmp_2m"][2, :, :]))

    def test_writes_provenance_to_store(self, tmp_path, small_schema):
        """Provenance attributes are written at init time."""
        from weatherman.processing.geo import GridProvenance

        prov = GridProvenance(
            source_crs="EPSG:4326",
            source_lon_convention="0_360",
            source_lat_order="north_to_south",
            source_grid_resolution=45.0,
            source_grid_shape=(5, 8),
            steps_applied=("lon_roll_0_360_to_neg180_180",),
        )
        store = init_zarr_store(small_schema, tmp_path / "test.zarr", provenance=prov)

        root = zarr.open_group(str(store), mode="r")
        assert root.attrs["provenance:source_crs"] == "EPSG:4326"
        assert root.attrs["provenance:source_lon_convention"] == "0_360"
        assert "lon_roll" in root.attrs["provenance:normalization_steps"][0]

    def test_rejects_unknown_variable(self, tmp_path, small_schema):
        store = init_zarr_store(small_schema, tmp_path / "test.zarr")
        grib_file = tmp_path / "test.grib2"
        grib_file.touch()

        with pytest.raises(ValueError, match="not in schema"):
            ingest_grib2_file(store, small_schema, "nonexistent", 0, grib_file)

    def test_rejects_unknown_forecast_hour(self, tmp_path, small_schema):
        store = init_zarr_store(small_schema, tmp_path / "test.zarr")
        grib_file = tmp_path / "test.grib2"
        grib_file.touch()

        with pytest.raises(ValueError, match="not in schema"):
            ingest_grib2_file(store, small_schema, "tmp_2m", 999, grib_file)

    def test_rejects_missing_file(self, tmp_path, small_schema):
        store = init_zarr_store(small_schema, tmp_path / "test.zarr")
        with pytest.raises(FileNotFoundError):
            ingest_grib2_file(
                store, small_schema, "tmp_2m", 0, tmp_path / "missing.grib2"
            )

    def test_rejects_shape_mismatch(self, tmp_path, small_schema):
        store = init_zarr_store(small_schema, tmp_path / "test.zarr")
        grib_file = tmp_path / "test.grib2"
        grib_file.touch()

        # Wrong shape: 10×16 instead of 5×8
        with patch("weatherman.processing.grib2_to_zarr.xr.open_dataset",
                    side_effect=_mock_open_dataset(10, 16)):
            with pytest.raises(ValueError, match="Shape mismatch"):
                ingest_grib2_file(store, small_schema, "tmp_2m", 0, grib_file)


# ---------------------------------------------------------------------------
# finalize_store tests
# ---------------------------------------------------------------------------

class TestFinalizeStore:
    def test_consolidates_metadata(self, tmp_path, small_schema):
        store = init_zarr_store(small_schema, tmp_path / "test.zarr")
        finalize_store(store)

        # Should be openable via open_consolidated
        root = zarr.open_consolidated(str(store))
        assert "tmp_2m" in dict(root.members())


# ---------------------------------------------------------------------------
# convert_grib2_to_zarr (full pipeline) tests
# ---------------------------------------------------------------------------

class TestConvertGrib2ToZarr:
    def _setup_fake_gribs(
        self, tmp_path: Path, small_schema: ZarrSchema
    ) -> Path:
        """Create fake GRIB2 files in the expected directory layout."""
        grib_dir = tmp_path / "staging"
        for var_name in small_schema.variables:
            var_dir = grib_dir / "grib2" / var_name
            var_dir.mkdir(parents=True)
            for fhour in small_schema.forecast_hours:
                (var_dir / f"f{fhour:03d}.grib2").touch()
        return grib_dir

    def test_full_pipeline(self, tmp_path, small_schema):
        grib_dir = self._setup_fake_gribs(tmp_path, small_schema)
        store_path = tmp_path / "output.zarr"

        with patch("weatherman.processing.grib2_to_zarr.xr.open_dataset",
                    side_effect=_mock_open_dataset(5, 8)):
            result = convert_grib2_to_zarr(small_schema, grib_dir, store_path)

        assert result.success
        assert result.variables_written == 6  # 2 vars × 3 hours
        assert result.hours_written == 3
        assert result.store_path == store_path

        # Verify the store is valid
        root = zarr.open_consolidated(str(store_path))
        assert "tmp_2m" in dict(root.members())
        assert "ugrd_10m" in dict(root.members())
        # No NaN left (all hours written for both variables)
        assert not np.any(np.isnan(root["tmp_2m"][:]))
        assert not np.any(np.isnan(root["ugrd_10m"][:]))

    def test_partial_failure(self, tmp_path, small_schema):
        """Missing GRIB2 files result in errors but don't abort."""
        grib_dir = tmp_path / "staging"
        # Only create files for tmp_2m, not ugrd_10m
        var_dir = grib_dir / "grib2" / "tmp_2m"
        var_dir.mkdir(parents=True)
        for fhour in small_schema.forecast_hours:
            (var_dir / f"f{fhour:03d}.grib2").touch()

        store_path = tmp_path / "output.zarr"

        with patch("weatherman.processing.grib2_to_zarr.xr.open_dataset",
                    side_effect=_mock_open_dataset(5, 8)):
            result = convert_grib2_to_zarr(small_schema, grib_dir, store_path)

        assert not result.success
        assert result.variables_written == 3  # tmp_2m × 3 hours
        assert len(result.errors) == 3  # ugrd_10m × 3 hours missing

    def test_result_properties(self, tmp_path, small_schema):
        result = ConversionResult(store_path=tmp_path)
        assert result.success  # no errors = success
        result.errors.append("test error")
        assert not result.success
