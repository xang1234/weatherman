"""GRIB2 to Zarr conversion for the canonical weather store.

Reads individual GRIB2 variable files produced by the downloader and writes
a single Zarr group per model run.  The Zarr store is the canonical format
for point/trajectory queries via the EDR API.

The GRIB2 files are on the GFS 0.25° global grid (1440×721) with longitude
in the 0–360° convention.  This writer rolls the longitude axis so the Zarr
store uses the -180 to 180° convention expected by ``zarr_schema.py``.

Usage:
    grib2_dir_to_zarr(
        grib2_dir=Path(".data/models/gfs/grib2/20260321T00Z/grib2"),
        zarr_path=Path(".data/models/gfs/runs/20260321T00Z/zarr/20260321T00Z.zarr"),
        forecast_hours=[0, 3, 6, 9, 12],
    )
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import rasterio
import zarr
from zarr.codecs import BloscCodec

from weatherman.processing.cog import _read_band_as_float32
from weatherman.storage.zarr_schema import (
    PHASE1_VARIABLES,
    GridResolution,
    make_lat_array,
    make_lon_array,
)

logger = logging.getLogger(__name__)

# GFS GRIB2 uses 0–360° longitude.  Rolling by half the grid width
# converts to the -180–180° convention used by the Zarr schema.
_LON_ROLL = 720  # 1440 / 2

_BLOSC_CODEC = BloscCodec(cname="zstd", clevel=3, shuffle="shuffle")

_GRID = GridResolution.GFS_025


def grib2_dir_to_zarr(
    grib2_dir: Path,
    zarr_path: Path,
    forecast_hours: list[int],
) -> list[str]:
    """Convert downloaded GRIB2 files into a Zarr store.

    Args:
        grib2_dir: Directory containing per-variable subdirectories, each
            with ``f{fhour:03d}.grib2`` files.
        zarr_path: Output path for the Zarr group.
        forecast_hours: List of forecast hours to include.

    Returns:
        List of variable names successfully written.
    """
    zarr_path.parent.mkdir(parents=True, exist_ok=True)
    root = zarr.open_group(str(zarr_path), mode="w")

    lat_arr = make_lat_array(_GRID)
    lon_arr = make_lon_array(_GRID)
    time_arr = np.array(sorted(forecast_hours), dtype=np.int32)

    root.create_array("lat", data=lat_arr)
    root.create_array("lon", data=lon_arr)
    root.create_array("time", data=time_arr)

    n_times = len(time_arr)
    n_lat = _GRID.lat_count
    n_lon = _GRID.lon_count

    written_vars: list[str] = []

    for var_name, var_def in PHASE1_VARIABLES.items():
        var_dir = grib2_dir / var_name
        if not var_dir.is_dir():
            logger.warning("Skipping %s — no GRIB2 directory at %s", var_name, var_dir)
            continue

        arr = root.create_array(
            var_name,
            shape=(n_times, n_lat, n_lon),
            chunks=var_def.chunks.as_tuple(),
            dtype=var_def.dtype,
            fill_value=var_def.fill_value,
            compressors=_BLOSC_CODEC,
        )
        arr.attrs["long_name"] = var_def.long_name
        arr.attrs["units"] = var_def.units
        if var_def.level:
            arr.attrs["level"] = var_def.level

        found_count = 0
        for t_idx, fhour in enumerate(time_arr):
            grib2_file = var_dir / f"f{int(fhour):03d}.grib2"
            if not grib2_file.exists():
                logger.debug("Missing %s f%03d — filling with NaN", var_name, fhour)
                continue

            with rasterio.open(grib2_file) as src:
                data = _read_band_as_float32(src)

            data = np.roll(data, _LON_ROLL, axis=1)
            arr[t_idx, :, :] = data
            found_count += 1

        if found_count:
            written_vars.append(var_name)
            logger.info("Wrote %s (%d/%d hours)", var_name, found_count, n_times)
        else:
            logger.warning("No GRIB2 files found for %s — variable is empty", var_name)

    logger.info(
        "Zarr store created: %s (%d variables, %d hours)",
        zarr_path, len(written_vars), n_times,
    )
    return written_vars
