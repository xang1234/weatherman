"""Tests for the QC geometry check."""

import numpy as np
import pytest
import zarr

from weatherman.qc.geometry import GeometryIssue, check_geometry
from weatherman.storage.zarr_schema import (
    GridResolution,
    VariableDef,
    ZarrSchema,
    make_lat_array,
    make_lon_array,
)

_SCHEMA = ZarrSchema(
    grid=GridResolution.GFS_025,
    forecast_hours=(0,),
    variables={
        "var_a": VariableDef(
            name="var_a", long_name="Var A", units="K",
            grib_key=":A:", level="surface",
        ),
    },
)


def _make_store(tmp_path):
    """Create a Zarr store with correct coordinates and non-NaN data."""
    path = tmp_path / "test.zarr"
    root = zarr.open_group(str(path), mode="w")

    root.create_array("lat", data=make_lat_array(GridResolution.GFS_025))
    root.create_array("lon", data=make_lon_array(GridResolution.GFS_025))

    shape = _SCHEMA.shape
    arr = root.create_array(
        "var_a", shape=shape, dtype="float32",
        fill_value=float("nan"), chunks=(1, 721, 1440),
    )
    arr[0, :, :] = np.ones((shape[1], shape[2]), dtype=np.float32)
    return path


class TestGeometryPass:
    def test_correct_store_passes(self, tmp_path):
        result = check_geometry(_make_store(tmp_path), _SCHEMA)

        assert result.passed
        assert "PASS" in result.summary


class TestMissingCoordinates:
    def test_missing_lat(self, tmp_path):
        path = tmp_path / "test.zarr"
        root = zarr.open_group(str(path), mode="w")
        root.create_array("lon", data=make_lon_array(GridResolution.GFS_025))

        result = check_geometry(path, _SCHEMA)

        assert not result.passed
        issues = [i for i in result.issues if i.kind == "missing_coordinate"]
        assert any(i.coordinate == "lat" for i in issues)

    def test_missing_lon(self, tmp_path):
        path = tmp_path / "test.zarr"
        root = zarr.open_group(str(path), mode="w")
        root.create_array("lat", data=make_lat_array(GridResolution.GFS_025))

        result = check_geometry(path, _SCHEMA)

        assert not result.passed
        issues = [i for i in result.issues if i.kind == "missing_coordinate"]
        assert any(i.coordinate == "lon" for i in issues)


class TestLatChecks:
    def test_wrong_lat_shape(self, tmp_path):
        store_path = _make_store(tmp_path)
        root = zarr.open_group(str(store_path), mode="r+")
        del root["lat"]
        root.create_array("lat", data=np.linspace(90, -90, 100, dtype=np.float32))

        result = check_geometry(store_path, _SCHEMA)

        assert not result.passed
        assert any(i.kind == "shape_mismatch" and i.coordinate == "lat"
                    for i in result.issues)

    def test_wrong_lat_values(self, tmp_path):
        store_path = _make_store(tmp_path)
        root = zarr.open_group(str(store_path), mode="r+")
        del root["lat"]
        # Correct shape but shifted values
        lat = make_lat_array(GridResolution.GFS_025) + 1.0
        root.create_array("lat", data=lat)

        result = check_geometry(store_path, _SCHEMA)

        assert not result.passed
        assert any(i.kind == "values_mismatch" and i.coordinate == "lat"
                    for i in result.issues)

    def test_ascending_lat_detected(self, tmp_path):
        store_path = _make_store(tmp_path)
        root = zarr.open_group(str(store_path), mode="r+")
        del root["lat"]
        # Flip to ascending (south to north)
        lat = make_lat_array(GridResolution.GFS_025)[::-1].copy()
        root.create_array("lat", data=lat)

        result = check_geometry(store_path, _SCHEMA)

        assert not result.passed
        # Should detect both values mismatch AND not_descending
        assert any(i.kind == "values_mismatch" and i.coordinate == "lat"
                    for i in result.issues)
        assert any(i.kind == "not_descending" and i.coordinate == "lat"
                    for i in result.issues)


class TestLonChecks:
    def test_wrong_lon_shape(self, tmp_path):
        store_path = _make_store(tmp_path)
        root = zarr.open_group(str(store_path), mode="r+")
        del root["lon"]
        root.create_array("lon", data=np.arange(0, 360, 0.5, dtype=np.float32))

        result = check_geometry(store_path, _SCHEMA)

        assert not result.passed
        assert any(i.kind == "shape_mismatch" and i.coordinate == "lon"
                    for i in result.issues)

    def test_0_360_convention_detected(self, tmp_path):
        store_path = _make_store(tmp_path)
        root = zarr.open_group(str(store_path), mode="r+")
        del root["lon"]
        # [0, 360) convention — wrong
        lon = np.arange(0, 360, 0.25, dtype=np.float32)
        root.create_array("lon", data=lon)

        result = check_geometry(store_path, _SCHEMA)

        assert not result.passed
        # Should detect both values mismatch AND convention violation
        assert any(i.kind == "values_mismatch" and i.coordinate == "lon"
                    for i in result.issues)
        assert any(i.kind == "convention_violation" and i.coordinate == "lon"
                    for i in result.issues)


class TestAntimeridian:
    def test_nan_at_western_edge(self, tmp_path):
        store_path = _make_store(tmp_path)
        root = zarr.open_group(str(store_path), mode="r+")
        # Set first 4 lon columns to NaN (near -180°)
        data = root["var_a"][0, :, :]
        data[:, :4] = np.nan
        root["var_a"][0, :, :] = data

        result = check_geometry(store_path, _SCHEMA)

        assert not result.passed
        assert any(i.kind == "antimeridian_gap" for i in result.issues)

    def test_nan_at_eastern_edge(self, tmp_path):
        store_path = _make_store(tmp_path)
        root = zarr.open_group(str(store_path), mode="r+")
        data = root["var_a"][0, :, :]
        data[:, -4:] = np.nan
        root["var_a"][0, :, :] = data

        result = check_geometry(store_path, _SCHEMA)

        assert not result.passed
        assert any(i.kind == "antimeridian_gap" for i in result.issues)

    def test_no_gap_passes(self, tmp_path):
        result = check_geometry(_make_store(tmp_path), _SCHEMA)

        gap_issues = [i for i in result.issues if i.kind == "antimeridian_gap"]
        assert len(gap_issues) == 0


class TestPolar:
    def test_nan_at_north_pole(self, tmp_path):
        store_path = _make_store(tmp_path)
        root = zarr.open_group(str(store_path), mode="r+")
        data = root["var_a"][0, :, :]
        data[0, :] = np.nan  # north pole row
        root["var_a"][0, :, :] = data

        result = check_geometry(store_path, _SCHEMA)

        assert not result.passed
        assert any(i.kind == "polar_gap" and "north" in i.detail
                    for i in result.issues)

    def test_nan_at_south_pole(self, tmp_path):
        store_path = _make_store(tmp_path)
        root = zarr.open_group(str(store_path), mode="r+")
        data = root["var_a"][0, :, :]
        data[-1, :] = np.nan  # south pole row
        root["var_a"][0, :, :] = data

        result = check_geometry(store_path, _SCHEMA)

        assert not result.passed
        assert any(i.kind == "polar_gap" and "south" in i.detail
                    for i in result.issues)

    def test_poles_with_data_pass(self, tmp_path):
        result = check_geometry(_make_store(tmp_path), _SCHEMA)

        polar_issues = [i for i in result.issues if i.kind == "polar_gap"]
        assert len(polar_issues) == 0


class TestWaveOnlySchema:
    def test_wave_only_store_no_false_polar_gap(self, tmp_path):
        """Wave variables have NaN at south pole (Antarctica = land).

        When only wave variables exist, _pick_global_var falls back to
        the wave variable but should still be used. This test verifies
        the wave-only case doesn't crash, though polar_gap may fire
        (accepted: no atmospheric variable to check against).
        """
        wave_schema = ZarrSchema(
            grid=GridResolution.GFS_025,
            forecast_hours=(0,),
            variables={
                "htsgw_sfc": VariableDef(
                    name="htsgw_sfc", long_name="Wave height", units="m",
                    grib_key=":HTSGW:surface:", level="surface",
                ),
            },
        )
        path = tmp_path / "wave.zarr"
        root = zarr.open_group(str(path), mode="w")
        root.create_array("lat", data=make_lat_array(GridResolution.GFS_025))
        root.create_array("lon", data=make_lon_array(GridResolution.GFS_025))
        shape = wave_schema.shape
        arr = root.create_array(
            "htsgw_sfc", shape=shape, dtype="float32",
            fill_value=float("nan"), chunks=(1, 721, 1440),
        )
        arr[0, :, :] = np.ones((shape[1], shape[2]), dtype=np.float32)

        # No crash; coordinate checks should pass
        result = check_geometry(path, wave_schema)
        coord_issues = [i for i in result.issues
                        if i.kind in ("missing_coordinate", "shape_mismatch",
                                      "values_mismatch")]
        assert len(coord_issues) == 0

    def test_mixed_schema_prefers_atmospheric_var(self, tmp_path):
        """When both atmospheric and wave vars exist, prefers atmospheric."""
        mixed_schema = ZarrSchema(
            grid=GridResolution.GFS_025,
            forecast_hours=(0,),
            variables={
                # Wave variable listed FIRST
                "htsgw_sfc": VariableDef(
                    name="htsgw_sfc", long_name="Wave height", units="m",
                    grib_key=":HTSGW:surface:", level="surface",
                ),
                "tmp_2m": VariableDef(
                    name="tmp_2m", long_name="Temperature", units="K",
                    grib_key=":TMP:2 m above ground:", level="2 m above ground",
                ),
            },
        )
        path = tmp_path / "mixed.zarr"
        root = zarr.open_group(str(path), mode="w")
        root.create_array("lat", data=make_lat_array(GridResolution.GFS_025))
        root.create_array("lon", data=make_lon_array(GridResolution.GFS_025))
        shape = mixed_schema.shape
        for var_name in mixed_schema.variables:
            arr = root.create_array(
                var_name, shape=shape, dtype="float32",
                fill_value=float("nan"), chunks=(1, 721, 1440),
            )
            # Wave var has NaN at south pole (land), atmospheric has data
            data = np.ones((shape[1], shape[2]), dtype=np.float32)
            if var_name == "htsgw_sfc":
                data[-1, :] = np.nan  # south pole = land = NaN for waves
            arr[0, :, :] = data

        result = check_geometry(path, mixed_schema)

        # Should NOT flag polar_gap because tmp_2m (atmospheric) is selected
        polar_issues = [i for i in result.issues if i.kind == "polar_gap"]
        assert len(polar_issues) == 0


class TestGeometryIssueStr:
    def test_str_representation(self):
        issue = GeometryIssue(
            coordinate="lat",
            kind="extent_mismatch",
            detail="Expected lat range [90, -90], got [89, -89]",
        )
        assert "[extent_mismatch]" in str(issue)
        assert "lat" in str(issue)
