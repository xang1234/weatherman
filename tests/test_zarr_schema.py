"""Tests for the Zarr dataset schema definition."""

import numpy as np
import pytest

from weatherman.storage.zarr_schema import (
    DEFAULT_CHUNKS,
    DEFAULT_COMPRESSOR,
    GFS_SCHEMA,
    ChunkSpec,
    CompressionCodec,
    GridResolution,
    VariableDef,
    ZarrSchema,
    make_lat_array,
    make_lon_array,
)


class TestGridResolution:
    def test_gfs_025_dimensions(self):
        grid = GridResolution.GFS_025
        assert grid.lat_count == 721
        assert grid.lon_count == 1440
        assert grid.step == 0.25

    def test_lat_array_shape_and_bounds(self):
        arr = make_lat_array(GridResolution.GFS_025)
        assert arr.shape == (721,)
        assert arr.dtype == np.float32
        assert arr[0] == pytest.approx(90.0)
        assert arr[-1] == pytest.approx(-90.0)

    def test_lat_array_descending(self):
        arr = make_lat_array(GridResolution.GFS_025)
        assert np.all(np.diff(arr) < 0), "lat must be north-to-south (descending)"

    def test_lon_array_shape_and_bounds(self):
        arr = make_lon_array(GridResolution.GFS_025)
        assert arr.shape == (1440,)
        assert arr.dtype == np.float32
        assert arr[0] == pytest.approx(-180.0)
        # Half-open: last value should be 179.75, not 180.0
        assert arr[-1] == pytest.approx(179.75)

    def test_lon_convention_no_wrap(self):
        arr = make_lon_array(GridResolution.GFS_025)
        assert arr.min() >= -180.0
        assert arr.max() < 180.0


class TestChunkSpec:
    def test_defaults(self):
        c = ChunkSpec()
        assert c.time == 1
        assert c.lat == 512
        assert c.lon == 512

    def test_as_tuple(self):
        assert DEFAULT_CHUNKS.as_tuple() == (1, 512, 512)

    def test_custom_chunks(self):
        c = ChunkSpec(time=2, lat=256, lon=256)
        assert c.as_tuple() == (2, 256, 256)


class TestCompressionCodec:
    def test_defaults(self):
        c = CompressionCodec()
        assert c.cname == "zstd"
        assert c.clevel == 3
        assert c.shuffle == 1

    def test_to_numcodecs(self):
        codec = DEFAULT_COMPRESSOR.to_numcodecs()
        assert codec.cname == "zstd"
        assert codec.clevel == 3
        assert codec.shuffle == 1


class TestVariableDef:
    def test_phase1_variables_exist(self):
        from weatherman.storage.zarr_schema import PHASE1_VARIABLES

        assert "tmp_2m" in PHASE1_VARIABLES
        assert "ugrd_10m" in PHASE1_VARIABLES
        assert "vgrd_10m" in PHASE1_VARIABLES
        assert "apcp_sfc" in PHASE1_VARIABLES
        assert "prmsl" in PHASE1_VARIABLES
        assert "tcdc_atm" in PHASE1_VARIABLES

    def test_variable_dims(self):
        v = VariableDef(
            name="test", long_name="Test", units="K",
            grib_key=":TEST:",
        )
        assert v.dims == ("time", "lat", "lon")

    def test_variable_defaults(self):
        v = VariableDef(
            name="test", long_name="Test", units="K",
            grib_key=":TEST:",
        )
        assert v.dtype == "float32"
        assert np.isnan(v.fill_value)
        assert v.level is None

    def test_equality_with_nan_fill_value(self):
        """Two VariableDefs with NaN fill_value should compare equal."""
        a = VariableDef(name="x", long_name="X", units="K", grib_key=":X:")
        b = VariableDef(name="x", long_name="X", units="K", grib_key=":X:")
        assert a == b
        assert hash(a) == hash(b)

    def test_grib_keys_match_gfs_downloader(self):
        """Ensure schema GRIB keys match the GFS downloader patterns."""
        from weatherman.ingest.gfs import DEFAULT_SEARCH_PATTERNS
        from weatherman.storage.zarr_schema import PHASE1_VARIABLES

        for var_name, var_def in PHASE1_VARIABLES.items():
            assert var_name in DEFAULT_SEARCH_PATTERNS, (
                f"Schema variable {var_name} not found in GFS downloader"
            )
            assert var_def.grib_key == DEFAULT_SEARCH_PATTERNS[var_name], (
                f"GRIB key mismatch for {var_name}: "
                f"schema={var_def.grib_key!r}, "
                f"downloader={DEFAULT_SEARCH_PATTERNS[var_name]!r}"
            )


class TestZarrSchema:
    def test_gfs_schema_shape(self):
        assert GFS_SCHEMA.shape == (41, 721, 1440)

    def test_gfs_schema_forecast_hours(self):
        assert GFS_SCHEMA.forecast_hours[0] == 0
        assert GFS_SCHEMA.forecast_hours[-1] == 120
        assert len(GFS_SCHEMA.forecast_hours) == 41

    def test_time_array(self):
        t = GFS_SCHEMA.time_array
        assert t.dtype == np.int32
        assert len(t) == 41
        assert t[0] == 0
        assert t[-1] == 120

    def test_global_attrs_cf_convention(self):
        assert GFS_SCHEMA.global_attrs["Conventions"] == "CF-1.8"

    def test_estimated_uncompressed_size(self):
        """Verify the ADR's storage estimate is in the right ballpark."""
        n_times, n_lat, n_lon = GFS_SCHEMA.shape
        n_vars = len(GFS_SCHEMA.variables)
        bytes_per_element = 4  # float32
        total = n_times * n_vars * n_lat * n_lon * bytes_per_element
        mb = total / (1024 * 1024)
        # ~170 MB per variable uncompressed (41 steps × 721 × 1440 × 4 bytes)
        assert 400 < mb < 1200, f"Unexpected size estimate: {mb:.0f} MB"

    def test_chunk_covers_grid_efficiently(self):
        """512x512 chunks on a 1440x721 grid should produce ~6 chunks/timestep."""
        lat_chunks = -(-GFS_SCHEMA.grid.lat_count // DEFAULT_CHUNKS.lat)  # ceil div
        lon_chunks = -(-GFS_SCHEMA.grid.lon_count // DEFAULT_CHUNKS.lon)
        total_spatial = lat_chunks * lon_chunks
        assert 4 <= total_spatial <= 8, f"Expected ~6 chunks, got {total_spatial}"
