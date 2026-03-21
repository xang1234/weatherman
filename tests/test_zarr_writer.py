"""Tests for GRIB2 → Zarr conversion."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest
import rasterio
import zarr
from rasterio.transform import from_bounds

from weatherman.processing.zarr_writer import grib2_dir_to_zarr
from weatherman.storage.zarr_schema import GridResolution


def _write_fake_grib2(path: Path, width: int, height: int, value: float) -> None:
    """Write a minimal GeoTIFF that mimics a GFS GRIB2 file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    transform = from_bounds(0, -90, 360, 90, width, height)
    data = np.full((height, width), value, dtype=np.float32)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=transform,
    ) as dst:
        dst.write(data, 1)


@pytest.fixture
def grib2_dir(tmp_path: Path) -> Path:
    """Create a fake GRIB2 directory with two variables and two forecast hours."""
    base = tmp_path / "grib2"
    grid = GridResolution.GFS_025
    w, h = grid.lon_count, grid.lat_count

    # Write tmp_2m for hours 0 and 3
    _write_fake_grib2(base / "tmp_2m" / "f000.grib2", w, h, 300.0)
    _write_fake_grib2(base / "tmp_2m" / "f003.grib2", w, h, 301.0)

    # Write ugrd_10m for hour 0 only (hour 3 missing)
    _write_fake_grib2(base / "ugrd_10m" / "f000.grib2", w, h, 5.0)

    return base


def test_zarr_structure(grib2_dir: Path, tmp_path: Path) -> None:
    """Zarr store has correct coordinate arrays and variable shapes."""
    zarr_path = tmp_path / "output.zarr"
    hours = [0, 3]

    written = grib2_dir_to_zarr(grib2_dir, zarr_path, hours)

    assert "tmp_2m" in written
    assert "ugrd_10m" in written

    root = zarr.open_group(str(zarr_path), mode="r")

    # Coordinate arrays
    assert "lat" in root
    assert "lon" in root
    assert "time" in root
    assert list(root["time"][:]) == [0, 3]

    lat = np.asarray(root["lat"][:])
    lon = np.asarray(root["lon"][:])
    assert lat[0] == pytest.approx(90.0)
    assert lat[-1] == pytest.approx(-90.0)
    assert lon[0] == pytest.approx(-180.0)
    assert lon[-1] < 180.0

    # Variable shapes
    grid = GridResolution.GFS_025
    assert root["tmp_2m"].shape == (2, grid.lat_count, grid.lon_count)
    assert root["ugrd_10m"].shape == (2, grid.lat_count, grid.lon_count)


def test_zarr_values(grib2_dir: Path, tmp_path: Path) -> None:
    """Written values are correct and NaN fills missing hours."""
    zarr_path = tmp_path / "output.zarr"
    written = grib2_dir_to_zarr(grib2_dir, zarr_path, [0, 3])

    root = zarr.open_group(str(zarr_path), mode="r")

    # tmp_2m has data for both hours
    assert root["tmp_2m"][0, 0, 0] == pytest.approx(300.0, abs=0.1)
    assert root["tmp_2m"][1, 0, 0] == pytest.approx(301.0, abs=0.1)

    # ugrd_10m has data for hour 0, NaN for hour 3
    assert root["ugrd_10m"][0, 0, 0] == pytest.approx(5.0, abs=0.1)
    assert np.isnan(root["ugrd_10m"][1, 0, 0])


def test_zarr_variable_metadata(grib2_dir: Path, tmp_path: Path) -> None:
    """Variables have correct attrs from PHASE1_VARIABLES."""
    zarr_path = tmp_path / "output.zarr"
    grib2_dir_to_zarr(grib2_dir, zarr_path, [0, 3])

    root = zarr.open_group(str(zarr_path), mode="r")
    assert root["tmp_2m"].attrs["long_name"] == "Temperature at 2m above ground"
    assert root["tmp_2m"].attrs["units"] == "K"


def test_missing_variable_skipped(tmp_path: Path) -> None:
    """Variables without GRIB2 directories are skipped, not errored."""
    grib2_dir = tmp_path / "empty_grib2"
    grib2_dir.mkdir()
    zarr_path = tmp_path / "output.zarr"

    written = grib2_dir_to_zarr(grib2_dir, zarr_path, [0])
    assert len(written) == 0
