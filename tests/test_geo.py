"""Tests for geospatial normalization."""

from __future__ import annotations

import numpy as np
import pytest

from weatherman.processing.geo import (
    GridProvenance,
    LonRange,
    NormalizationResult,
    crosses_antimeridian,
    detect_lat_order,
    detect_lon_convention,
    extract_longitudes,
    interpolate_at_point,
    needs_lat_flip,
    needs_lon_normalization,
    normalize_grid,
    normalize_latitude,
    normalize_longitude,
    split_antimeridian,
    validate_geographic_crs,
    wrap_longitude,
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


# ---------------------------------------------------------------------------
# Anti-meridian handling
# ---------------------------------------------------------------------------

class TestWrapLongitude:
    def test_already_canonical(self):
        assert wrap_longitude(0.0) == 0.0
        assert wrap_longitude(-180.0) == -180.0
        assert wrap_longitude(179.0) == 179.0

    def test_wraps_positive_overflow(self):
        assert wrap_longitude(190.0) == pytest.approx(-170.0)
        assert wrap_longitude(360.0) == pytest.approx(0.0)
        assert wrap_longitude(540.0) == pytest.approx(-180.0)

    def test_wraps_negative_overflow(self):
        assert wrap_longitude(-200.0) == pytest.approx(160.0)
        assert wrap_longitude(-360.0) == pytest.approx(0.0)

    def test_boundary_180_wraps_to_neg180(self):
        # 180.0 should wrap to -180.0 (half-open interval)
        assert wrap_longitude(180.0) == pytest.approx(-180.0)


class TestCrossesAntimeridian:
    def test_normal_range_does_not_cross(self):
        assert not crosses_antimeridian(-10, 10)
        assert not crosses_antimeridian(-180, 180)
        assert not crosses_antimeridian(0, 90)

    def test_detects_crossing(self):
        # Tokyo (140E) to San Francisco (-122 / 238E)
        assert crosses_antimeridian(140, -122)
        # Fiji region
        assert crosses_antimeridian(177, -179)

    def test_crossing_with_unwrapped_input(self):
        # 170 to 190 should cross (190 wraps to -170)
        assert crosses_antimeridian(170, 190)

    def test_zero_width_does_not_cross(self):
        assert not crosses_antimeridian(170, 170)


class TestSplitAntimeridian:
    def test_no_split_needed(self):
        ranges = split_antimeridian(-10, 10)
        assert len(ranges) == 1
        assert ranges[0] == LonRange(west=-10.0, east=10.0)

    def test_splits_crossing(self):
        ranges = split_antimeridian(170, -170)
        assert len(ranges) == 2
        assert ranges[0].west == pytest.approx(170.0)
        assert ranges[0].east == pytest.approx(180.0)
        assert ranges[1].west == pytest.approx(-180.0)
        assert ranges[1].east == pytest.approx(-170.0)

    def test_splits_with_unwrapped_input(self):
        ranges = split_antimeridian(170, 200)  # 200 -> -160
        assert len(ranges) == 2
        assert ranges[1].east == pytest.approx(-160.0)

    def test_wide_pacific_crossing(self):
        # Tokyo to SF: 140E to 122W
        ranges = split_antimeridian(140, -122)
        assert len(ranges) == 2
        assert ranges[0].west == pytest.approx(140.0)
        assert ranges[0].east == pytest.approx(180.0)
        assert ranges[1].west == pytest.approx(-180.0)
        assert ranges[1].east == pytest.approx(-122.0)


class TestExtractLongitudes:
    """Test longitude extraction with and without anti-meridian crossing."""

    @pytest.fixture()
    def global_grid(self):
        """A simple 1° global grid in canonical [-180, 180) convention."""
        lon = np.arange(-180, 180, dtype=np.float64)  # 360 points
        lat = np.arange(90, -91, -1, dtype=np.float64)  # 181 points
        # Data: each cell value = longitude of that column (for easy verification)
        data = np.broadcast_to(lon[np.newaxis, :], (len(lat), len(lon))).copy()
        return data, lat, lon

    def test_simple_range(self, global_grid):
        data, _lat, lon = global_grid
        sub_data, sub_lon = extract_longitudes(data, lon, -10, 10)

        assert sub_lon[0] == pytest.approx(-10.0)
        assert sub_lon[-1] == pytest.approx(10.0)
        assert sub_data.shape[-1] == len(sub_lon)

    def test_antimeridian_crossing(self, global_grid):
        data, _lat, lon = global_grid
        sub_data, sub_lon = extract_longitudes(data, lon, 170, -170)

        # Should contain lons from 170..179, then -180..-170
        assert sub_lon[0] == pytest.approx(170.0)
        assert sub_lon[-1] == pytest.approx(-170.0)
        # Total = 10 (170..179) + 11 (-180..-170) = 21
        assert len(sub_lon) == 21
        # Verify data values match column longitudes
        np.testing.assert_array_equal(sub_data[0, :], sub_lon)

    def test_pacific_route(self, global_grid):
        """Simulate a Tokyo-to-SF route bbox."""
        data, _lat, lon = global_grid
        sub_data, sub_lon = extract_longitudes(data, lon, 140, -122)

        # West side: 140..179 = 40 points
        # East side: -180..-122 = 59 points
        assert len(sub_lon) == 40 + 59
        assert sub_lon[0] == pytest.approx(140.0)
        assert sub_lon[-1] == pytest.approx(-122.0)

    def test_no_crossing_full_hemisphere(self, global_grid):
        data, _lat, lon = global_grid
        sub_data, sub_lon = extract_longitudes(data, lon, 0, 179)

        assert sub_lon[0] == pytest.approx(0.0)
        assert sub_lon[-1] == pytest.approx(179.0)
        assert len(sub_lon) == 180


class TestInterpolateAtPoint:
    """Test bilinear interpolation, including anti-meridian wraparound."""

    @pytest.fixture()
    def temp_grid(self):
        """4x8 grid where value = latitude + longitude (for easy checks)."""
        lat = np.array([90.0, 30.0, -30.0, -90.0])  # descending
        lon = np.linspace(-180, 135, 8)  # step = 45°
        lat_grid, lon_grid = np.meshgrid(lat, lon, indexing="ij")
        data = (lat_grid + lon_grid).astype(np.float64)
        return data, lat, lon

    def test_exact_grid_point(self, temp_grid):
        data, lat, lon = temp_grid
        # At exact grid point (30, 0): value should be 30 + 0 = 30
        result = interpolate_at_point(data, lat, lon, 30.0, 0.0)
        assert result == pytest.approx(30.0)

    def test_midpoint_interpolation(self, temp_grid):
        data, lat, lon = temp_grid
        # Midpoint between (90, 0) and (30, 45): average of 4 corners
        # (90,0)=90, (90,45)=135, (30,0)=30, (30,45)=75
        result = interpolate_at_point(data, lat, lon, 60.0, 22.5)
        expected = (90 + 135 + 30 + 75) / 4.0
        assert result == pytest.approx(expected)

    def test_antimeridian_wraparound(self):
        """Interpolation near ±180° should wrap correctly."""
        # 4 lons: -180, -90, 0, 90 (step=90), 3 lats: 90, 0, -90
        lat = np.array([90.0, 0.0, -90.0])
        lon = np.array([-180.0, -90.0, 0.0, 90.0])
        # Simple data: column index as value
        data = np.array([
            [0.0, 1.0, 2.0, 3.0],
            [0.0, 1.0, 2.0, 3.0],
            [0.0, 1.0, 2.0, 3.0],
        ])

        # Query at lon=179 (near +180, between lon[3]=90 and lon[0]=-180 wrapped)
        # i0 = 3 (90°), i1 = 0 (-180° wrapped)
        # dx = (-180 - 90) + 360 = 90  (wrap distance)
        # dlon = 179 - 90 = 89
        # fx = 89/90
        result = interpolate_at_point(data, lat, lon, 0.0, 179.0)
        expected = 3.0 * (1 - 89 / 90) + 0.0 * (89 / 90)
        assert result == pytest.approx(expected, abs=1e-10)

    def test_antimeridian_negative_side(self):
        """Interpolation just past -180° (e.g. -179°)."""
        lat = np.array([90.0, 0.0, -90.0])
        lon = np.array([-180.0, -90.0, 0.0, 90.0])
        data = np.array([
            [10.0, 20.0, 30.0, 40.0],
            [10.0, 20.0, 30.0, 40.0],
            [10.0, 20.0, 30.0, 40.0],
        ])

        # lon=-179 is between lon[3]=90 (wrapped) and lon[0]=-180
        # i0 = searchsorted(-179, right) - 1 => should give i0 at the
        # wrap edge. Let's verify directly:
        result = interpolate_at_point(data, lat, lon, 0.0, -179.0)
        # -179 is 1° east of -180. Between lon[3]=90 and lon[0]=-180:
        # dx = (-180 - 90) + 360 = 90
        # Actually i0 for -179 in [-180, -90, 0, 90]: searchsorted(-179, right)-1 = 0
        # i1 = 1, lon[0]=-180, lon[1]=-90, dx=90, dlon=1
        # fx = 1/90
        expected = 10.0 * (1 - 1 / 90) + 20.0 * (1 / 90)
        assert result == pytest.approx(expected, abs=1e-10)

    def test_unwrapped_longitude_input(self):
        """Longitude > 180 should be wrapped before interpolation."""
        lat = np.array([90.0, 0.0, -90.0])
        lon = np.array([-180.0, -90.0, 0.0, 90.0])
        data = np.ones((3, 4)) * 5.0

        # 190° wraps to -170°
        result = interpolate_at_point(data, lat, lon, 0.0, 190.0)
        assert result == pytest.approx(5.0)

    def test_out_of_lat_range_raises(self):
        lat = np.array([90.0, 0.0, -90.0])
        lon = np.array([-180.0, 0.0])
        data = np.ones((3, 2))

        with pytest.raises(ValueError, match="Latitude"):
            interpolate_at_point(data, lat, lon, 95.0, 0.0)


class TestAntimeridianGFS025:
    """Integration test with a realistic GFS 0.25° grid."""

    @pytest.fixture()
    def gfs_grid(self):
        """Simulate a normalized GFS 0.25° global grid."""
        lon = np.linspace(-180, 179.75, 1440)
        lat = np.linspace(90, -90, 721)
        # Data: temperature-like values that vary smoothly
        lat_grid, lon_grid = np.meshgrid(lat, lon, indexing="ij")
        data = 15.0 + 10.0 * np.cos(np.radians(lat_grid)) * np.cos(
            np.radians(lon_grid)
        )
        return data, lat, lon

    def test_extract_fiji_region(self, gfs_grid):
        """Fiji straddles the anti-meridian (~177E to ~178W)."""
        data, _lat, lon = gfs_grid
        sub_data, sub_lon = extract_longitudes(data, lon, 176, -178)

        # 176 to 179.75 = 16 points, -180 to -178 = 9 points
        assert len(sub_lon) == 16 + 9
        assert sub_data.shape[-1] == len(sub_lon)

    def test_interpolation_continuity_across_antimeridian(self, gfs_grid):
        """Values should be continuous across ±180°."""
        data, lat, lon = gfs_grid

        # Sample points stepping across the anti-meridian at equator
        test_lons = [179.0, 179.5, 179.9, -179.9, -179.5, -179.0]
        values = [
            interpolate_at_point(data, lat, lon, 0.0, tlon)
            for tlon in test_lons
        ]

        # With cosine data at equator, values near ±180° should be
        # close to 15 + 10*cos(0)*cos(180) = 15 - 10 = 5.0
        for v in values:
            assert v == pytest.approx(5.0, abs=0.5)

        # Consecutive values should be smooth (no discontinuity)
        for i in range(len(values) - 1):
            assert abs(values[i + 1] - values[i]) < 1.0
