"""Tests for the QC completeness check."""

import numpy as np
import pytest
import zarr

from weatherman.qc.completeness import CompletenessIssue, check_completeness
from weatherman.storage.zarr_schema import (
    ChunkSpec,
    GridResolution,
    VariableDef,
    ZarrSchema,
)

# Minimal schema: only 2 forecast hours, 2 variables, standard GFS grid.
# Tests write small non-NaN values to specific slices rather than filling
# the entire array, keeping test runtime reasonable.
_SCHEMA = ZarrSchema(
    grid=GridResolution.GFS_025,
    forecast_hours=(0, 3),
    variables={
        "var_a": VariableDef(
            name="var_a", long_name="Var A", units="K",
            grib_key=":A:", level="surface",
        ),
        "var_b": VariableDef(
            name="var_b", long_name="Var B", units="m/s",
            grib_key=":B:", level="surface",
        ),
    },
)


def _make_store(tmp_path):
    """Create a Zarr store matching _SCHEMA with all slices non-NaN."""
    path = tmp_path / "test.zarr"
    root = zarr.open_group(str(path), mode="w")
    shape = _SCHEMA.shape  # (2, 721, 1440)
    for var_name in _SCHEMA.variables:
        arr = root.create_array(
            var_name, shape=shape, dtype="float32",
            fill_value=float("nan"), chunks=(1, 721, 1440),
        )
        # Write ones (non-NaN) to each time slice
        for t in range(shape[0]):
            arr[t, :, :] = np.ones((shape[1], shape[2]), dtype=np.float32)
    return path


class TestCompletenessPass:
    def test_full_store_passes(self, tmp_path):
        result = check_completeness(_make_store(tmp_path), _SCHEMA)

        assert result.passed
        assert result.expected_variables == 2
        assert result.present_variables == 2
        assert result.expected_hours == 2
        assert result.complete_hours == 2
        assert result.issues == []
        assert "PASS" in result.summary


class TestMissingVariable:
    def test_detects_deleted_variable(self, tmp_path):
        store_path = _make_store(tmp_path)
        root = zarr.open_group(str(store_path), mode="r+")
        del root["var_b"]

        result = check_completeness(store_path, _SCHEMA)

        assert not result.passed
        assert result.present_variables == 1
        assert len(result.issues) == 1
        assert result.issues[0].kind == "missing_variable"
        assert result.issues[0].variable == "var_b"

    def test_detects_all_variables_missing(self, tmp_path):
        store_path = _make_store(tmp_path)
        root = zarr.open_group(str(store_path), mode="r+")
        del root["var_a"]
        del root["var_b"]

        result = check_completeness(store_path, _SCHEMA)

        assert not result.passed
        assert result.present_variables == 0
        missing_vars = {i.variable for i in result.issues}
        assert missing_vars == {"var_a", "var_b"}


class TestMissingHours:
    def test_detects_nan_forecast_hour(self, tmp_path):
        store_path = _make_store(tmp_path)
        # Wipe time index 1 (forecast_hour=3) for var_a
        root = zarr.open_group(str(store_path), mode="r+")
        root["var_a"][1, :, :] = np.nan

        result = check_completeness(store_path, _SCHEMA)

        assert not result.passed
        issues = [i for i in result.issues if i.kind == "missing_hours"]
        assert len(issues) == 1
        assert issues[0].variable == "var_a"
        assert "3" in issues[0].detail
        # Hour 3 incomplete because var_a is missing
        assert result.complete_hours == 1

    def test_all_hours_missing_for_one_variable(self, tmp_path):
        store_path = _make_store(tmp_path)
        root = zarr.open_group(str(store_path), mode="r+")
        root["var_b"][0, :, :] = np.nan
        root["var_b"][1, :, :] = np.nan

        result = check_completeness(store_path, _SCHEMA)

        assert not result.passed
        issue = [i for i in result.issues if i.variable == "var_b"][0]
        assert issue.kind == "missing_hours"
        assert "2/2" in issue.detail
        assert result.complete_hours == 0


class TestShapeMismatch:
    def test_detects_wrong_shape(self, tmp_path):
        store_path = _make_store(tmp_path)
        root = zarr.open_group(str(store_path), mode="r+")
        del root["var_a"]
        root.create_array(
            "var_a", shape=(2, 100, 100), dtype="float32",
            fill_value=float("nan"),
        )

        result = check_completeness(store_path, _SCHEMA)

        assert not result.passed
        shape_issues = [i for i in result.issues if i.kind == "shape_mismatch"]
        assert len(shape_issues) == 1
        assert shape_issues[0].variable == "var_a"


class TestCompletenessIssueStr:
    def test_str_representation(self):
        issue = CompletenessIssue(
            variable="tmp_2m",
            kind="missing_variable",
            detail="Array 'tmp_2m' not found in store",
        )
        assert str(issue) == "[missing_variable] tmp_2m: Array 'tmp_2m' not found in store"


class TestSummary:
    def test_fail_summary(self, tmp_path):
        store_path = _make_store(tmp_path)
        root = zarr.open_group(str(store_path), mode="r+")
        del root["var_a"]

        result = check_completeness(store_path, _SCHEMA)

        assert "FAIL" in result.summary
        assert "1/2 variables" in result.summary
