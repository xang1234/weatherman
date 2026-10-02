"""Quality checks before a run is published (#71)."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest
import zarr

from scripts.run_pipeline import QualityCheckFailed, step_quality_check
from weatherman.storage.paths import RunID, StorageLayout
from weatherman.storage.zarr_schema import GridResolution, make_lat_array, make_lon_array

RUN = RunID("20260306T00Z")
LAYOUT = StorageLayout("gfs")
HOURS = [0, 3]
LAYERS = {"temperature", "wind_speed", "wave_height"}

# Plausible values, as the Zarr writer stores them (temperature in °C, pressure in Pa).
GOOD = {
    "tmp_2m": 15.0,
    "ugrd_10m": 5.0,
    "vgrd_10m": -3.0,
    "prmsl": 101_300.0,
    "htsgw_sfc": 2.0,
    "perpw_sfc": 9.0,
    "dirpw_sfc": 270.0,
}
WAVES = {"htsgw_sfc", "perpw_sfc", "dirpw_sfc"}


def _stage(data_dir: Path, values: dict[str, float], *, empty: tuple[str, int] | None = None) -> None:
    """Write a staged Zarr store holding `values`, optionally one all-NaN hour."""
    grid = GridResolution.GFS_025
    root = zarr.open_group(str(data_dir / LAYOUT.staging_zarr_path(RUN)), mode="w")
    root.create_array("lat", data=make_lat_array(grid))
    root.create_array("lon", data=make_lon_array(grid))
    root.create_array("time", data=np.array(HOURS, dtype=np.int32))
    shape = (len(HOURS), grid.lat_count, grid.lon_count)
    for name, value in values.items():
        data = np.full(shape, value, dtype=np.float32)
        if name in WAVES:
            data[:, :200, :] = np.nan  # "land": ocean-only fields have gaps
        if empty and empty[0] == name:
            data[HOURS.index(empty[1])] = np.nan
        root.create_array(name, data=data)


def _check(data_dir: Path) -> set[str]:
    return step_quality_check(RUN, HOURS, data_dir, LAYOUT, LAYERS)


def test_a_good_run_publishes_every_layer(tmp_path: Path):
    _stage(tmp_path, GOOD)
    assert _check(tmp_path) == LAYERS


def test_an_empty_forecast_hour_in_a_core_variable_blocks_publishing(tmp_path: Path):
    _stage(tmp_path, GOOD, empty=("ugrd_10m", 3))
    with pytest.raises(QualityCheckFailed, match="ugrd_10m"):
        _check(tmp_path)


def test_a_missing_core_variable_blocks_publishing(tmp_path: Path):
    _stage(tmp_path, {k: v for k, v in GOOD.items() if k != "vgrd_10m"})
    with pytest.raises(QualityCheckFailed, match="vgrd_10m"):
        _check(tmp_path)


def test_temperature_in_kelvin_is_out_of_bounds(tmp_path: Path):
    """The store holds °C; Kelvin values mean the units went wrong somewhere."""
    _stage(tmp_path, {**GOOD, "tmp_2m": 288.0})
    with pytest.raises(QualityCheckFailed, match="tmp_2m"):
        _check(tmp_path)


def test_a_bad_wave_field_drops_only_the_wave_layer(tmp_path: Path):
    _stage(tmp_path, GOOD, empty=("htsgw_sfc", 0))
    assert _check(tmp_path) == {"temperature", "wind_speed"}


def test_missing_pressure_only_warns(tmp_path: Path):
    _stage(tmp_path, {k: v for k, v in GOOD.items() if k != "prmsl"})
    assert _check(tmp_path) == LAYERS
