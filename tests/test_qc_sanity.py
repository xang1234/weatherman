"""Tests for the QC sanity check."""

import numpy as np
import pytest
import zarr

from weatherman.qc.sanity import (
    PHYSICAL_BOUNDS,
    PhysicalBounds,
    SanityIssue,
    check_sanity,
)
from weatherman.storage.zarr_schema import (
    GridResolution,
    VariableDef,
    ZarrSchema,
)

# Minimal schema: 2 hours, 2 variables, GFS grid.
_SCHEMA = ZarrSchema(
    grid=GridResolution.GFS_025,
    forecast_hours=(0, 3),
    variables={
        "tmp_2m": VariableDef(
            name="tmp_2m", long_name="Temperature at 2m", units="K",
            grib_key=":TMP:2 m above ground:", level="2 m above ground",
        ),
        "htsgw_sfc": VariableDef(
            name="htsgw_sfc", long_name="Significant wave height", units="m",
            grib_key=":HTSGW:surface:", level="surface",
        ),
    },
)

# Custom tight bounds for testing (so we don't need extreme values)
_TEST_BOUNDS = {
    "tmp_2m": PhysicalBounds(min=200.0, max=320.0, max_nan_fraction=0.05),
    "htsgw_sfc": PhysicalBounds(min=0.0, max=30.0, max_nan_fraction=0.80),
}


def _make_store(tmp_path, fill_value=280.0, wave_fill=5.0):
    """Create a Zarr store with reasonable values for both variables."""
    path = tmp_path / "test.zarr"
    root = zarr.open_group(str(path), mode="w")
    shape = _SCHEMA.shape  # (2, 721, 1440)
    for var_name in _SCHEMA.variables:
        arr = root.create_array(
            var_name, shape=shape, dtype="float32",
            fill_value=float("nan"), chunks=(1, 721, 1440),
        )
        val = fill_value if var_name == "tmp_2m" else wave_fill
        for t in range(shape[0]):
            arr[t, :, :] = np.full((shape[1], shape[2]), val, dtype=np.float32)
    return path


class TestSanityPass:
    def test_good_values_pass(self, tmp_path):
        result = check_sanity(_make_store(tmp_path), _SCHEMA, _TEST_BOUNDS)

        assert result.passed
        assert result.variables_checked == 2
        assert "PASS" in result.summary


class TestOutOfBounds:
    def test_detects_value_below_min(self, tmp_path):
        store_path = _make_store(tmp_path)
        root = zarr.open_group(str(store_path), mode="r+")
        # Set one cell to 100 K — below physical min of 200 K
        data = np.full((721, 1440), 280.0, dtype=np.float32)
        data[0, 0] = 100.0
        root["tmp_2m"][0, :, :] = data

        result = check_sanity(store_path, _SCHEMA, _TEST_BOUNDS)

        assert not result.passed
        oob = [i for i in result.issues if i.kind == "out_of_bounds"]
        assert len(oob) == 1
        assert oob[0].variable == "tmp_2m"
        assert "100" in oob[0].detail

    def test_detects_value_above_max(self, tmp_path):
        store_path = _make_store(tmp_path)
        root = zarr.open_group(str(store_path), mode="r+")
        data = np.full((721, 1440), 280.0, dtype=np.float32)
        data[360, 720] = 400.0  # above 320 K max
        root["tmp_2m"][1, :, :] = data

        result = check_sanity(store_path, _SCHEMA, _TEST_BOUNDS)

        assert not result.passed
        oob = [i for i in result.issues if i.kind == "out_of_bounds"]
        assert len(oob) == 1
        assert "fhour=3" in oob[0].detail

    def test_values_at_boundary_pass(self, tmp_path):
        store_path = _make_store(tmp_path)
        root = zarr.open_group(str(store_path), mode="r+")
        # Exactly at bounds should pass
        data = np.full((721, 1440), 200.0, dtype=np.float32)
        root["tmp_2m"][0, :, :] = data
        data[:] = 320.0
        root["tmp_2m"][1, :, :] = data

        result = check_sanity(store_path, _SCHEMA, _TEST_BOUNDS)

        oob = [i for i in result.issues if i.kind == "out_of_bounds"]
        assert len(oob) == 0


class TestAllZeros:
    def test_detects_all_zeros_slice(self, tmp_path):
        store_path = _make_store(tmp_path)
        root = zarr.open_group(str(store_path), mode="r+")
        root["tmp_2m"][0, :, :] = np.zeros((721, 1440), dtype=np.float32)

        result = check_sanity(store_path, _SCHEMA, _TEST_BOUNDS)

        assert not result.passed
        zeros = [i for i in result.issues if i.kind == "all_zeros"]
        assert len(zeros) == 1
        assert zeros[0].variable == "tmp_2m"
        assert "fhour=0" in zeros[0].detail

    def test_mostly_zeros_with_some_values_pass(self, tmp_path):
        store_path = _make_store(tmp_path)
        root = zarr.open_group(str(store_path), mode="r+")
        data = np.zeros((721, 1440), dtype=np.float32)
        data[0, 0] = 280.0  # one non-zero cell
        root["tmp_2m"][0, :, :] = data

        result = check_sanity(store_path, _SCHEMA, _TEST_BOUNDS)

        zeros = [i for i in result.issues if i.kind == "all_zeros"]
        assert len(zeros) == 0


class TestExcessiveNaN:
    def test_detects_high_nan_fraction(self, tmp_path):
        store_path = _make_store(tmp_path)
        root = zarr.open_group(str(store_path), mode="r+")
        # Set 10% of cells to NaN (threshold is 5%)
        data = np.full((721, 1440), 280.0, dtype=np.float32)
        nan_count = int(721 * 1440 * 0.10)
        flat = data.ravel()
        flat[:nan_count] = np.nan
        root["tmp_2m"][0, :, :] = data

        result = check_sanity(store_path, _SCHEMA, _TEST_BOUNDS)

        assert not result.passed
        nan_issues = [i for i in result.issues if i.kind == "excessive_nan"]
        assert len(nan_issues) == 1
        assert nan_issues[0].variable == "tmp_2m"

    def test_wave_high_nan_fraction_within_threshold(self, tmp_path):
        """Wave variables expect ~70% NaN over land — should pass at 0.80 threshold."""
        store_path = _make_store(tmp_path)
        root = zarr.open_group(str(store_path), mode="r+")
        data = np.full((721, 1440), 5.0, dtype=np.float32)
        nan_count = int(721 * 1440 * 0.70)
        flat = data.ravel()
        flat[:nan_count] = np.nan
        root["htsgw_sfc"][0, :, :] = data
        root["htsgw_sfc"][1, :, :] = data

        result = check_sanity(store_path, _SCHEMA, _TEST_BOUNDS)

        nan_issues = [
            i for i in result.issues
            if i.kind == "excessive_nan" and i.variable == "htsgw_sfc"
        ]
        assert len(nan_issues) == 0


class TestSkipAllZeros:
    def test_accumulation_field_all_zeros_passes(self, tmp_path):
        """apcp_sfc at fhour=0 is legitimately all zeros — should not flag."""
        schema = ZarrSchema(
            grid=GridResolution.GFS_025,
            forecast_hours=(0, 3),
            variables={
                "apcp_sfc": VariableDef(
                    name="apcp_sfc", long_name="Total precipitation",
                    units="kg/m^2", grib_key=":APCP:surface:", level="surface",
                ),
            },
        )
        bounds = {"apcp_sfc": PhysicalBounds(min=0.0, max=2000.0, skip_all_zeros=True)}
        path = tmp_path / "test.zarr"
        root = zarr.open_group(str(path), mode="w")
        shape = schema.shape
        arr = root.create_array(
            "apcp_sfc", shape=shape, dtype="float32",
            fill_value=float("nan"), chunks=(1, 721, 1440),
        )
        arr[0, :, :] = np.zeros((shape[1], shape[2]), dtype=np.float32)
        arr[1, :, :] = np.full((shape[1], shape[2]), 5.0, dtype=np.float32)

        result = check_sanity(path, schema, bounds)

        zeros = [i for i in result.issues if i.kind == "all_zeros"]
        assert len(zeros) == 0


class TestInfiniteValues:
    def test_detects_inf(self, tmp_path):
        store_path = _make_store(tmp_path)
        root = zarr.open_group(str(store_path), mode="r+")
        data = np.full((721, 1440), 280.0, dtype=np.float32)
        data[0, 0] = np.inf
        root["tmp_2m"][0, :, :] = data

        result = check_sanity(store_path, _SCHEMA, _TEST_BOUNDS)

        assert not result.passed
        oob = [i for i in result.issues if i.kind == "out_of_bounds"]
        assert any("infinite" in i.detail for i in oob)

    def test_inf_not_counted_as_nan(self, tmp_path):
        """Infinities should not inflate the NaN fraction."""
        store_path = _make_store(tmp_path)
        root = zarr.open_group(str(store_path), mode="r+")
        data = np.full((721, 1440), 280.0, dtype=np.float32)
        data[0, 0] = np.inf  # 1 cell out of ~1M — well under 5% NaN threshold
        root["tmp_2m"][0, :, :] = data

        result = check_sanity(store_path, _SCHEMA, _TEST_BOUNDS)

        nan_issues = [
            i for i in result.issues
            if i.kind == "excessive_nan" and i.variable == "tmp_2m"
        ]
        assert len(nan_issues) == 0


class TestMissingVariable:
    def test_skips_missing_variables(self, tmp_path):
        """Missing variables are the completeness check's job, not sanity's."""
        store_path = _make_store(tmp_path)
        root = zarr.open_group(str(store_path), mode="r+")
        del root["htsgw_sfc"]

        result = check_sanity(store_path, _SCHEMA, _TEST_BOUNDS)

        assert result.variables_checked == 1
        assert result.passed


class TestNoBoundsConfigured:
    def test_skips_unconfigured_variable(self, tmp_path):
        store_path = _make_store(tmp_path)
        # Only configure bounds for one variable
        partial_bounds = {"tmp_2m": _TEST_BOUNDS["tmp_2m"]}

        result = check_sanity(store_path, _SCHEMA, partial_bounds)

        assert result.variables_checked == 1


class TestSanityIssueStr:
    def test_str_representation(self):
        issue = SanityIssue(
            variable="tmp_2m",
            kind="out_of_bounds",
            detail="fhour=0: values [100, 280] outside [200, 320]",
        )
        s = str(issue)
        assert "[out_of_bounds]" in s
        assert "tmp_2m" in s


class TestDefaultBounds:
    def test_all_schema_variables_have_bounds(self):
        """Every variable in layers.yaml should have physical bounds."""
        from weatherman.storage.zarr_schema import GFS_SCHEMA

        for var_name in GFS_SCHEMA.variables:
            assert var_name in PHYSICAL_BOUNDS, (
                f"Variable '{var_name}' has no entry in PHYSICAL_BOUNDS"
            )
