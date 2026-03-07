"""Tests for geospatial normalization."""

from __future__ import annotations

import numpy as np
import pytest

from weatherman.processing.geo import (
    GridProvenance,
    NormalizationResult,
    detect_lat_order,
    detect_lon_convention,
    needs_lat_flip,
    needs_lon_normalization,
    normalize_grid,
    normalize_latitude,
    normalize_longitude,
    validate_geographic_crs,
)


# ---------------------------------------------------------------------------
# Longitude normalization
# ---------------------------------------------------------------------------

class TestNormalizeLongitude:
    def test_rolls_0_360_to_neg180_180(self):
        n_lon = 8
        src_lon = np.linspace(0, 360 - (360 / n_lon), n_lon)
        data = np.arange(n_lon, dtype=np.float32).reshape(1, n_lon)

        rolled = normalize_longitude(data, src_lon)

        # 180° is at index 4 → after roll: [4, 5, 6, 7, 0, 1, 2, 3]
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
        for row in range(n_lat):
            assert rolled[row, 0] == data[row, 4]

    def test_gfs_025_grid(self):
        """Simulate the real GFS 0.25° grid (1440 points)."""
        src_lon = np.linspace(0, 359.75, 1440)
        data = np.arange(1440, dtype=np.float32).reshape(1, 1440)

        rolled = normalize_longitude(data, src_lon)
        # Index 720 in source is 180°; should become index 0
        assert rolled[0, 0] == 720
        # Index 0 in source is 0°; should become index 720
        assert rolled[0, 720] == 0


class TestDetectLonConvention:
    def test_detects_0_360(self):
        lon = np.array([0, 90, 180, 270, 359])
        assert detect_lon_convention(lon) == "0_360"

    def test_detects_neg180_180(self):
        lon = np.array([-180, -90, 0, 90, 179])
        assert detect_lon_convention(lon) == "neg180_180"

    def test_needs_normalization(self):
        assert needs_lon_normalization(np.array([0, 90, 180, 270]))
        assert not needs_lon_normalization(np.array([-180, -90, 0, 90]))


# ---------------------------------------------------------------------------
# Latitude normalization
# ---------------------------------------------------------------------------

class TestNormalizeLatitude:
    def test_flips_south_to_north(self):
        src_lat = np.array([-90, -45, 0, 45, 90], dtype=np.float32)
        data = np.arange(5 * 4, dtype=np.float32).reshape(5, 4)

        flipped = normalize_latitude(data, src_lat)
        # Row 0 should now be what was row 4 (90°)
        np.testing.assert_array_equal(flipped[0], data[4])
        np.testing.assert_array_equal(flipped[4], data[0])

    def test_preserves_north_to_south(self):
        src_lat = np.array([90, 45, 0, -45, -90], dtype=np.float32)
        data = np.arange(5 * 4, dtype=np.float32).reshape(5, 4)

        result = normalize_latitude(data, src_lat)
        np.testing.assert_array_equal(result, data)

    def test_handles_single_lat(self):
        src_lat = np.array([45.0])
        data = np.array([[1, 2, 3]], dtype=np.float32)
        result = normalize_latitude(data, src_lat)
        np.testing.assert_array_equal(result, data)


class TestDetectLatOrder:
    def test_north_to_south(self):
        assert detect_lat_order(np.array([90, 0, -90])) == "north_to_south"

    def test_south_to_north(self):
        assert detect_lat_order(np.array([-90, 0, 90])) == "south_to_north"

    def test_needs_flip(self):
        assert needs_lat_flip(np.array([-90, 0, 90]))
        assert not needs_lat_flip(np.array([90, 0, -90]))


# ---------------------------------------------------------------------------
# CRS validation
# ---------------------------------------------------------------------------

class TestValidateGeographicCRS:
    def test_epsg_4326(self):
        assert validate_geographic_crs(None, 4326) == "EPSG:4326"

    def test_no_crs_assumes_4326(self):
        result = validate_geographic_crs(None, None)
        assert "4326" in result
        assert "assumed" in result

    def test_projected_crs_raises(self):
        wkt = "PROJCS[\"WGS 84 / UTM zone 10N\", ...]"
        with pytest.raises(ValueError, match="projected"):
            validate_geographic_crs(wkt, 32610)

    def test_other_geographic_crs_accepted(self):
        result = validate_geographic_crs("GEOGCS[\"NAD83\"]", 4269)
        assert result == "EPSG:4269"


# ---------------------------------------------------------------------------
# GridProvenance
# ---------------------------------------------------------------------------

class TestGridProvenance:
    def test_to_attrs(self):
        prov = GridProvenance(
            source_crs="EPSG:4326",
            source_lon_convention="0_360",
            source_lat_order="north_to_south",
            source_grid_resolution=0.25,
            source_grid_shape=(721, 1440),
            steps_applied=("lon_roll_0_360_to_neg180_180",),
        )
        attrs = prov.to_attrs()

        assert attrs["provenance:source_crs"] == "EPSG:4326"
        assert attrs["provenance:source_lon_convention"] == "0_360"
        assert attrs["provenance:source_grid_resolution"] == 0.25
        assert attrs["provenance:source_grid_shape"] == [721, 1440]
        assert "lon_roll" in attrs["provenance:normalization_steps"][0]


# ---------------------------------------------------------------------------
# Combined normalize_grid
# ---------------------------------------------------------------------------

class TestNormalizeGrid:
    def test_normalizes_both_lon_and_lat(self):
        n_lat, n_lon = 5, 8
        src_lat = np.linspace(-90, 90, n_lat)  # south-to-north
        src_lon = np.linspace(0, 360 - (360 / n_lon), n_lon)  # 0-360

        data = np.arange(n_lat * n_lon, dtype=np.float32).reshape(n_lat, n_lon)

        result = normalize_grid(data, src_lat, src_lon)

        assert isinstance(result, NormalizationResult)
        assert result.data.shape == data.shape

        # Check provenance recorded both steps
        assert "lon_roll_0_360_to_neg180_180" in result.provenance.steps_applied
        assert "lat_flip_south_to_north" in result.provenance.steps_applied
        assert result.provenance.source_lon_convention == "0_360"
        assert result.provenance.source_lat_order == "south_to_north"

    def test_no_normalization_needed(self):
        n_lat, n_lon = 5, 8
        src_lat = np.linspace(90, -90, n_lat)  # north-to-south
        src_lon = np.linspace(-180, 180 - (360 / n_lon), n_lon)  # -180 to 180

        data = np.arange(n_lat * n_lon, dtype=np.float32).reshape(n_lat, n_lon)

        result = normalize_grid(data, src_lat, src_lon)

        np.testing.assert_array_equal(result.data, data)
        assert result.provenance.steps_applied == ()
        assert result.provenance.source_lon_convention == "neg180_180"
        assert result.provenance.source_lat_order == "north_to_south"

    def test_provenance_captures_grid_info(self):
        src_lat = np.linspace(90, -90, 5)
        src_lon = np.linspace(-180, 135, 8)
        data = np.zeros((5, 8), dtype=np.float32)

        result = normalize_grid(
            data, src_lat, src_lon,
            source_crs="EPSG:4326",
            grid_resolution=0.25,
        )

        assert result.provenance.source_crs == "EPSG:4326"
        assert result.provenance.source_grid_resolution == 0.25
        assert result.provenance.source_grid_shape == (5, 8)
