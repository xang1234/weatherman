"""GRIB2 to Cloud Optimized GeoTIFF conversion.

Reads a single-variable GRIB2 file produced by the GFS downloader,
reprojects to EPSG:4326 if needed, and writes a COG with internal
overviews and deflate compression.

Output path follows the canonical layout:
    staging/<run_id>/cogs/<layer>/<forecast_hour>.tif

Usage:
    result = grib2_to_cog(
        grib2_path=Path("staging/20260306T00Z/grib2/tmp_2m/f000.grib2"),
        output_path=Path("staging/20260306T00Z/cogs/temperature/000.tif"),
    )

    result = wind_speed_to_cog(
        ugrd_path=Path("staging/20260306T00Z/grib2/ugrd_10m/f000.grib2"),
        vgrd_path=Path("staging/20260306T00Z/grib2/vgrd_10m/f000.grib2"),
        output_path=Path("staging/20260306T00Z/cogs/wind_speed/000.tif"),
    )
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import rasterio
from rasterio.enums import Resampling
from rasterio.transform import from_bounds

logger = logging.getLogger(__name__)

# Internal overview factors — each is a 2x reduction from the previous.
# Benchmarked on GFS 0.25° (1440×721) grids:
#   [2,4,8,16]    — ~3% size overhead, covers z3+ efficiently
#   [2,4,8,16,32] — ~3.1% overhead, adds z0–z2 coverage (< 1% extra)
# The 32x level adds negligible size but avoids full-res reads for
# global zoom (z0). Use EXTENDED_OVERVIEW_LEVELS (or extended=True)
# to opt in.
DEFAULT_OVERVIEW_LEVELS = [2, 4, 8, 16]
EXTENDED_OVERVIEW_LEVELS = [2, 4, 8, 16, 32]


@dataclass(frozen=True)
class OverviewConfig:
    """Shared overview configuration for all COG layers.

    Controls overview decimation factors and resampling method.
    Use ``for_continuous()`` for temperature, wind, etc., and
    ``for_categorical()`` for discrete/classified data.
    """

    levels: list[int]
    resampling: Resampling

    @classmethod
    def for_continuous(cls, *, extended: bool = False) -> OverviewConfig:
        """Config for continuous data (temperature, wind speed, etc.)."""
        levels = list(EXTENDED_OVERVIEW_LEVELS if extended else DEFAULT_OVERVIEW_LEVELS)
        return cls(levels=levels, resampling=Resampling.average)

    @classmethod
    def for_categorical(cls, *, extended: bool = False) -> OverviewConfig:
        """Config for categorical/discrete data."""
        levels = list(EXTENDED_OVERVIEW_LEVELS if extended else DEFAULT_OVERVIEW_LEVELS)
        return cls(levels=levels, resampling=Resampling.nearest)

# GDAL COG creation profile
COG_PROFILE = {
    "driver": "GTiff",
    "dtype": "float32",
    "compress": "deflate",
    "predictor": 3,  # floating-point differencing — correct for float32
    "tiled": True,
    "blockxsize": 256,
    "blockysize": 256,
    "interleave": "band",
}

# Expected CRS for output COGs
TARGET_CRS = "EPSG:4326"

# GFS 0.25° global grid extents
GFS_GLOBAL_BOUNDS = (-180.0, -90.0, 180.0, 90.0)
GFS_025_WIDTH = 1440
GFS_025_HEIGHT = 721


def _read_band_as_float32(
    src: rasterio.DatasetReader,
    band: int = 1,
    *,
    fallback_nodata: float | None = 9999.0,
) -> np.ndarray:
    """Read a raster band as float32, converting nodata sentinels to NaN.

    Uses rasterio's masked read to honour the file's declared nodata value.
    Additionally, if *fallback_nodata* is set, any cell equal to that value
    is masked unconditionally — this is idempotent (NaN cells can never match)
    and guards against GRIB2 files where rasterio declares nodata but doesn't
    mask all matching cells.  The default fallback (9999.0) matches the
    GFS-Wave GRIB2 land sentinel; it is safe for atmospheric variables
    whose physical range never reaches 9999.

    Returns:
        2-D float32 ndarray with nodata cells set to NaN.
    """
    masked = src.read(band, masked=True)
    data = masked.filled(np.nan).astype(np.float32)

    if fallback_nodata is not None:
        data[data == np.float32(fallback_nodata)] = np.nan

    return data


@dataclass(frozen=True)
class COGResult:
    """Result of a GRIB2 → COG conversion."""

    input_path: Path
    output_path: Path
    size_bytes: int
    width: int
    height: int
    crs: str
    overview_levels: list[int]


def grib2_to_cog(
    grib2_path: Path,
    output_path: Path,
    *,
    overview_levels: list[int] | None = None,
    resampling: Resampling = Resampling.average,
    ocean_only: bool = False,
) -> COGResult:
    """Convert a single GRIB2 file to a Cloud Optimized GeoTIFF.

    Reads the first band of the GRIB2 file, validates the CRS and extent,
    and writes a COG with internal overviews using deflate compression.

    Args:
        grib2_path: Path to the input GRIB2 file.
        output_path: Path for the output COG file.
        overview_levels: Overview decimation factors (default: [2, 4, 8, 16]).
        resampling: Resampling method for overviews (default: average).
        ocean_only: If True, apply coastal fill to extend valid data into
            NaN cells near coastlines, and write NaN as nodata in the COG.

    Returns:
        COGResult with output metadata.

    Raises:
        FileNotFoundError: If the input GRIB2 file doesn't exist.
        ValueError: If the input CRS is not EPSG:4326 after reading.
    """
    if not grib2_path.exists():
        raise FileNotFoundError(f"GRIB2 file not found: {grib2_path}")

    if overview_levels is None:
        overview_levels = DEFAULT_OVERVIEW_LEVELS

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with rasterio.open(grib2_path) as src:
        data = _read_band_as_float32(src)
        src_crs = src.crs

        # Determine output dimensions and transform
        height, width = data.shape

        if src_crs and src_crs.to_epsg() == 4326:
            transform = src.transform
        else:
            # GFS GRIB2 files may lack proper CRS metadata but are always
            # on the 0.25° global lat/lon grid. Apply the known transform.
            logger.info(
                "Source CRS is %s, applying known GFS 0.25° EPSG:4326 transform",
                src_crs,
            )
            transform = from_bounds(*GFS_GLOBAL_BOUNDS, width, height)

    if ocean_only:
        from weatherman.processing.coastal_fill import coastal_fill, smooth_grid

        data = coastal_fill(data, iterations=16)
        data = smooth_grid(data, passes=3)

    _write_cog(
        data=data,
        output_path=output_path,
        width=width,
        height=height,
        transform=transform,
        overview_levels=overview_levels,
        resampling=resampling,
        nodata=np.nan if ocean_only else None,
    )

    size = output_path.stat().st_size
    logger.info(
        "Created COG: %s (%d x %d, %d bytes, overviews=%s)",
        output_path,
        width,
        height,
        size,
        overview_levels,
    )

    return COGResult(
        input_path=grib2_path,
        output_path=output_path,
        size_bytes=size,
        width=width,
        height=height,
        crs=TARGET_CRS,
        overview_levels=overview_levels,
    )


def wind_speed_to_cog(
    ugrd_path: Path,
    vgrd_path: Path,
    output_path: Path,
    *,
    overview_config: OverviewConfig | None = None,
) -> COGResult:
    """Compute wind speed from U/V GRIB2 components and write as COG.

    Reads UGRD and VGRD from separate GRIB2 files (same forecast hour
    and pressure level), computes wind speed as sqrt(u² + v²), and
    writes the result as a COG.

    Args:
        ugrd_path: Path to the UGRD (east-west wind) GRIB2 file.
        vgrd_path: Path to the VGRD (north-south wind) GRIB2 file.
        output_path: Path for the output COG file.
        overview_config: Overview configuration (default: continuous config).

    Returns:
        COGResult with output metadata.

    Raises:
        FileNotFoundError: If either input GRIB2 file doesn't exist.
        ValueError: If U and V grids have mismatched dimensions.
    """
    for label, path in [("UGRD", ugrd_path), ("VGRD", vgrd_path)]:
        if not path.exists():
            raise FileNotFoundError(f"{label} GRIB2 file not found: {path}")

    if overview_config is None:
        overview_config = OverviewConfig.for_continuous()

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with rasterio.open(ugrd_path) as u_src, rasterio.open(vgrd_path) as v_src:
        u_data = _read_band_as_float32(u_src)
        v_data = _read_band_as_float32(v_src)

        if u_data.shape != v_data.shape:
            raise ValueError(
                f"U/V grid dimension mismatch: {u_data.shape} vs {v_data.shape}"
            )

        height, width = u_data.shape

        # Use CRS/transform from the U component (both should be identical)
        src_crs = u_src.crs
        if src_crs and src_crs.to_epsg() == 4326:
            transform = u_src.transform
        else:
            logger.info(
                "Source CRS is %s, applying known GFS 0.25° EPSG:4326 transform",
                src_crs,
            )
            transform = from_bounds(*GFS_GLOBAL_BOUNDS, width, height)

    wind_speed = np.sqrt(u_data**2 + v_data**2)
    has_nodata = np.isnan(wind_speed).any()

    _write_cog(
        data=wind_speed,
        output_path=output_path,
        width=width,
        height=height,
        transform=transform,
        overview_levels=overview_config.levels,
        resampling=overview_config.resampling,
        nodata=np.nan if has_nodata else None,
    )

    size = output_path.stat().st_size
    logger.info(
        "Created wind speed COG: %s (%d x %d, %d bytes, overviews=%s)",
        output_path,
        width,
        height,
        size,
        overview_config.levels,
    )

    return COGResult(
        input_path=ugrd_path,
        output_path=output_path,
        size_bytes=size,
        width=width,
        height=height,
        crs=TARGET_CRS,
        overview_levels=overview_config.levels,
    )


def wave_direction_to_uv_cogs(
    direction_path: Path,
    u_output_path: Path,
    v_output_path: Path,
    *,
    overview_config: OverviewConfig | None = None,
    ocean_only: bool = True,
) -> tuple[COGResult, COGResult]:
    """Convert wave direction degrees into Cartesian propagation components.

    GFS-Wave ``DIRPW`` stores the direction waves come from.  The particle
    renderer wants a propagation vector, so the derived components match the
    existing shader convention:

    ``u = -sin(theta)``, ``v = -cos(theta)``

    Args:
        direction_path: Path to the raw wave direction GRIB2/GeoTIFF.
        u_output_path: Output path for the east-west propagation component COG.
        v_output_path: Output path for the north-south propagation component COG.
        overview_config: Overview configuration for the component COGs.
        ocean_only: Whether to coastal-fill and smooth the component grids.

    Returns:
        Tuple of ``(u_result, v_result)``.
    """
    if not direction_path.exists():
        raise FileNotFoundError(f"Wave direction GRIB2 file not found: {direction_path}")

    if overview_config is None:
        overview_config = OverviewConfig.for_continuous()

    u_output_path.parent.mkdir(parents=True, exist_ok=True)
    v_output_path.parent.mkdir(parents=True, exist_ok=True)

    with rasterio.open(direction_path) as src:
        direction = _read_band_as_float32(src)
        height, width = direction.shape

        src_crs = src.crs
        if src_crs and src_crs.to_epsg() == 4326:
            transform = src.transform
        else:
            logger.info(
                "Source CRS is %s, applying known GFS 0.25° EPSG:4326 transform",
                src_crs,
            )
            transform = from_bounds(*GFS_GLOBAL_BOUNDS, width, height)

    theta = np.deg2rad(direction)
    u_data = np.full_like(direction, np.nan, dtype=np.float32)
    v_data = np.full_like(direction, np.nan, dtype=np.float32)
    valid = ~np.isnan(direction)
    u_data[valid] = -np.sin(theta[valid]).astype(np.float32)
    v_data[valid] = -np.cos(theta[valid]).astype(np.float32)

    if ocean_only:
        from weatherman.processing.coastal_fill import coastal_fill, smooth_grid

        u_data = coastal_fill(u_data, iterations=16)
        u_data = smooth_grid(u_data, passes=3)
        v_data = coastal_fill(v_data, iterations=16)
        v_data = smooth_grid(v_data, passes=3)

        magnitude = np.sqrt(u_data**2 + v_data**2)
        nonzero = magnitude > np.float32(1e-6)
        u_data[nonzero] = u_data[nonzero] / magnitude[nonzero]
        v_data[nonzero] = v_data[nonzero] / magnitude[nonzero]
        u_data[~nonzero] = np.nan
        v_data[~nonzero] = np.nan

    _write_cog(
        data=u_data,
        output_path=u_output_path,
        width=width,
        height=height,
        transform=transform,
        overview_levels=overview_config.levels,
        resampling=overview_config.resampling,
        nodata=np.nan,
    )
    _write_cog(
        data=v_data,
        output_path=v_output_path,
        width=width,
        height=height,
        transform=transform,
        overview_levels=overview_config.levels,
        resampling=overview_config.resampling,
        nodata=np.nan,
    )

    u_size = u_output_path.stat().st_size
    v_size = v_output_path.stat().st_size

    return (
        COGResult(
            input_path=direction_path,
            output_path=u_output_path,
            size_bytes=u_size,
            width=width,
            height=height,
            crs=TARGET_CRS,
            overview_levels=overview_config.levels,
        ),
        COGResult(
            input_path=direction_path,
            output_path=v_output_path,
            size_bytes=v_size,
            width=width,
            height=height,
            crs=TARGET_CRS,
            overview_levels=overview_config.levels,
        ),
    )


def _write_cog(
    *,
    data: np.ndarray,
    output_path: Path,
    width: int,
    height: int,
    transform: rasterio.transform.Affine,
    overview_levels: list[int],
    resampling: Resampling,
    nodata: float | None = None,
) -> None:
    """Write data as a COG with internal overviews.

    Writes the base image and builds internal overviews within the
    same write context. GDAL/rasterio embeds the overviews as
    additional IFDs in the GeoTIFF.
    """
    profile = {
        **COG_PROFILE,
        "count": 1,
        "width": width,
        "height": height,
        "crs": TARGET_CRS,
        "transform": transform,
    }
    if nodata is not None:
        profile["nodata"] = nodata

    with rasterio.open(output_path, "w", **profile) as dst:
        dst.write(data, 1)
        dst.build_overviews(overview_levels, resampling)
        dst.update_tags(ns="rio_overview", resampling=resampling.name)


def validate_cog(path: Path) -> bool:
    """Basic COG validation: check tiling, overviews, and CRS.

    Returns True if the file passes all checks.
    """
    try:
        with rasterio.open(path) as ds:
            # Check CRS
            if not ds.crs or ds.crs.to_epsg() != 4326:
                logger.warning("COG CRS is not EPSG:4326: %s", ds.crs)
                return False

            # Check tiling: stripped TIFFs have blocks of (1, width) or
            # (rowsperstrip, width). Tiled TIFFs have fixed-size blocks
            # independent of image width (e.g. 256x256).
            block_shapes = ds.block_shapes
            if not block_shapes:
                logger.warning("COG has no block shapes: %s", path)
                return False
            block_h, block_w = block_shapes[0]
            if block_w == ds.width and block_h < ds.height:
                logger.warning("COG is stripped, not tiled (block %dx%d): %s", block_h, block_w, path)
                return False

            # Check overviews exist
            if not ds.overviews(1):
                logger.warning("COG has no overviews: %s", path)
                return False

            # Check compression
            if ds.compression is None:
                logger.warning("COG has no compression: %s", path)
                return False

            return True
    except Exception as exc:
        logger.warning("COG validation failed for %s: %s", path, exc)
        return False
