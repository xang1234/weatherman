"""Geospatial normalization for weather grid data.

Enforces the canonical coordinate conventions for the platform:

    Longitude:  [-180, 180)  half-open
    Latitude:   [90, -90]    north-to-south (descending)
    CRS:        EPSG:4326    (WGS 84 geographic)

All source data passes through these functions before entering the
Zarr store or COG pipeline, guaranteeing a single consistent convention
across the platform.

Source grid metadata and normalization steps are captured as provenance
so downstream consumers can verify data integrity.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import numpy as np

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Provenance
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class GridProvenance:
    """Records source grid info and normalization steps applied.

    Written as Zarr global attributes under the ``provenance:`` prefix
    so downstream consumers know exactly what transformations were applied.
    """

    source_crs: str
    source_lon_convention: str           # "0_360" or "neg180_180"
    source_lat_order: str                # "north_to_south" or "south_to_north"
    source_grid_resolution: float        # degrees (e.g. 0.25)
    source_grid_shape: tuple[int, int]   # (lat_count, lon_count)
    steps_applied: tuple[str, ...] = ()  # e.g. ("lon_roll", "lat_flip")

    def to_attrs(self) -> dict[str, object]:
        """Serialize to a dict suitable for Zarr/NetCDF attributes."""
        return {
            "provenance:source_crs": self.source_crs,
            "provenance:source_lon_convention": self.source_lon_convention,
            "provenance:source_lat_order": self.source_lat_order,
            "provenance:source_grid_resolution": self.source_grid_resolution,
            "provenance:source_grid_shape": list(self.source_grid_shape),
            "provenance:normalization_steps": list(self.steps_applied),
        }


# ---------------------------------------------------------------------------
# Longitude normalization
# ---------------------------------------------------------------------------

def normalize_longitude(data: np.ndarray, src_lon: np.ndarray) -> np.ndarray:
    """Roll data from [0, 360) longitude convention to [-180, 180).

    GFS GRIB2 files use longitude [0, 360). Our canonical store uses
    [-180, 180). This function detects the convention and rolls the
    data along the longitude axis so that the first column corresponds
    to -180 degrees.

    Args:
        data: Array with longitude as the last axis. Shape (..., lon).
        src_lon: 1-D longitude coordinate from the source file.

    Returns:
        Rolled data array (same shape, new longitude order).
    """
    if src_lon[0] >= 0 and src_lon[-1] > 180:
        # For a global [0, 360) grid, the halfway point is always at 180 degrees.
        # Using len//2 avoids float-precision issues with searchsorted.
        roll_idx = len(src_lon) // 2
        return np.roll(data, -roll_idx, axis=-1)
    return data


def detect_lon_convention(src_lon: np.ndarray) -> str:
    """Detect the longitude convention of a source grid.

    Returns:
        "0_360" if longitudes are in [0, 360) range.
        "neg180_180" if longitudes are in [-180, 180) range.
    """
    if src_lon[0] >= 0 and src_lon[-1] > 180:
        return "0_360"
    return "neg180_180"


def needs_lon_normalization(src_lon: np.ndarray) -> bool:
    """Check if longitude data needs to be rolled to [-180, 180)."""
    return detect_lon_convention(src_lon) == "0_360"


# ---------------------------------------------------------------------------
# Latitude normalization
# ---------------------------------------------------------------------------

def normalize_latitude(data: np.ndarray, src_lat: np.ndarray) -> np.ndarray:
    """Flip data to north-to-south latitude order if needed.

    Our canonical store uses descending latitude (90 -> -90).
    Some sources provide south-to-north (ascending) order.

    Args:
        data: Array with latitude as the second-to-last axis.
              Shape (..., lat, lon).
        src_lat: 1-D latitude coordinate from the source file.

    Returns:
        Data array with latitude in north-to-south order.
    """
    if len(src_lat) >= 2 and src_lat[0] < src_lat[-1]:
        # South-to-north — flip the lat axis
        return np.flip(data, axis=-2)
    return data


def detect_lat_order(src_lat: np.ndarray) -> str:
    """Detect latitude ordering of a source grid.

    Returns:
        "north_to_south" if descending (90 -> -90).
        "south_to_north" if ascending (-90 -> 90).
    """
    if len(src_lat) >= 2 and src_lat[0] < src_lat[-1]:
        return "south_to_north"
    return "north_to_south"


def needs_lat_flip(src_lat: np.ndarray) -> bool:
    """Check if latitude data needs to be flipped to north-to-south."""
    return detect_lat_order(src_lat) == "south_to_north"


# ---------------------------------------------------------------------------
# CRS validation
# ---------------------------------------------------------------------------

def validate_geographic_crs(crs_wkt: str | None, epsg: int | None) -> str:
    """Validate that the source CRS is a geographic (lat/lon) system.

    GFS data is always on a geographic WGS 84 grid, but GRIB2 files
    may report the CRS differently depending on the eccodes version.
    We accept EPSG:4326 or missing CRS (common with cfgrib).

    Args:
        crs_wkt: WKT string of the source CRS, or None if missing.
        epsg: EPSG code of the source CRS, or None if missing.

    Returns:
        Canonical CRS string for provenance recording.

    Raises:
        ValueError: If the CRS is a projected system (not geographic).
    """
    if epsg == 4326:
        return "EPSG:4326"

    if crs_wkt is None and epsg is None:
        # cfgrib often returns no CRS — assume geographic for GFS
        logger.debug("No CRS metadata; assuming EPSG:4326 for GFS data")
        return "EPSG:4326 (assumed)"

    if crs_wkt and ("PROJCS" in crs_wkt or "PROJCRS" in crs_wkt):
        raise ValueError(
            f"Source CRS appears to be projected, not geographic. "
            f"EPSG={epsg}, WKT starts with: {crs_wkt[:100]}"
        )

    # Other geographic CRS (e.g. WGS 72, NAD 83) — accept with warning
    logger.warning(
        "Source CRS is not EPSG:4326 (EPSG=%s). Treating as geographic.",
        epsg,
    )
    return f"EPSG:{epsg}" if epsg else "unknown_geographic"


# ---------------------------------------------------------------------------
# Combined normalization
# ---------------------------------------------------------------------------

@dataclass
class NormalizationResult:
    """Result of normalizing a 2-D weather grid."""

    data: np.ndarray
    provenance: GridProvenance


def normalize_grid(
    data: np.ndarray,
    src_lat: np.ndarray,
    src_lon: np.ndarray,
    *,
    source_crs: str = "EPSG:4326",
    grid_resolution: float = 0.25,
) -> NormalizationResult:
    """Apply all geospatial normalizations to a weather grid.

    Normalizes longitude convention and latitude order, then records
    what was done as provenance metadata.

    Args:
        data: 2-D array (lat, lon) of field values.
        src_lat: 1-D latitude coordinate from the source.
        src_lon: 1-D longitude coordinate from the source.
        source_crs: CRS string for provenance recording.
        grid_resolution: Grid resolution in degrees.

    Returns:
        NormalizationResult with normalized data and provenance.
    """
    steps: list[str] = []

    lon_convention = detect_lon_convention(src_lon)
    lat_order = detect_lat_order(src_lat)
    src_shape = (len(src_lat), len(src_lon))

    # Normalize longitude
    if needs_lon_normalization(src_lon):
        data = normalize_longitude(data, src_lon)
        steps.append("lon_roll_0_360_to_neg180_180")

    # Normalize latitude
    if needs_lat_flip(src_lat):
        data = normalize_latitude(data, src_lat)
        steps.append("lat_flip_south_to_north")

    provenance = GridProvenance(
        source_crs=source_crs,
        source_lon_convention=lon_convention,
        source_lat_order=lat_order,
        source_grid_resolution=grid_resolution,
        source_grid_shape=src_shape,
        steps_applied=tuple(steps),
    )

    return NormalizationResult(data=data, provenance=provenance)
