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
        # Find the index where 180° falls in the source grid.
        # 180.0 is exactly representable in IEEE 754, and weather grids
        # use clean fractional steps (0.25°, 0.5°, 1°), so searchsorted
        # is reliable here.
        roll_idx = int(np.searchsorted(src_lon, 180.0))
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
# Anti-meridian handling
# ---------------------------------------------------------------------------

def wrap_longitude(lon: float) -> float:
    """Wrap a longitude value into the canonical [-180, 180) range.

    Handles arbitrary inputs (e.g. 190 -> -170, -200 -> 160).

    Args:
        lon: Longitude in degrees (any range).

    Returns:
        Longitude in [-180, 180).
    """
    return ((lon + 180.0) % 360.0) - 180.0


def crosses_antimeridian(lon_west: float, lon_east: float) -> bool:
    """Detect whether a longitude range crosses the anti-meridian (±180°).

    Uses the convention that *lon_west* is the western edge and
    *lon_east* is the eastern edge of the query region.  A crossing
    is indicated when the wrapped western edge is greater than the
    wrapped eastern edge (i.e. the range wraps around ±180°).

    Both inputs are wrapped to [-180, 180) before comparison, so
    callers can pass raw user input (e.g. 170 to 190).

    Args:
        lon_west: Western longitude bound (degrees).
        lon_east: Eastern longitude bound (degrees).

    Returns:
        True if the range crosses the anti-meridian.
    """
    return wrap_longitude(lon_west) > wrap_longitude(lon_east)


@dataclass(frozen=True)
class LonRange:
    """A contiguous longitude range within [-180, 180)."""

    west: float
    east: float


def split_antimeridian(
    lon_west: float, lon_east: float,
) -> tuple[LonRange, ...]:
    """Split a longitude range into sub-ranges that don't cross ±180°.

    If the range does not cross the anti-meridian a single ``LonRange``
    is returned.  Otherwise two ranges are returned:
    ``[lon_west, 180)`` and ``[-180, lon_east]``.

    Both inputs are wrapped to [-180, 180) first.

    Args:
        lon_west: Western longitude bound (degrees).
        lon_east: Eastern longitude bound (degrees).

    Returns:
        One or two ``LonRange`` objects covering the requested extent.
    """
    w = wrap_longitude(lon_west)
    e = wrap_longitude(lon_east)

    if w <= e:
        return (LonRange(west=w, east=e),)

    # Crosses anti-meridian: split into west-side and east-side ranges
    return (
        LonRange(west=w, east=180.0),   # western portion up to +180
        LonRange(west=-180.0, east=e),   # eastern portion from -180
    )


def _lon_slice(lon_coords: np.ndarray, rng: LonRange) -> slice:
    """Return an index slice into *lon_coords* for the given range.

    Assumes *lon_coords* is sorted ascending in [-180, 180).
    """
    i_start = int(np.searchsorted(lon_coords, rng.west, side="left"))
    i_stop = int(np.searchsorted(lon_coords, rng.east, side="right"))
    return slice(i_start, i_stop)


def extract_longitudes(
    data: np.ndarray,
    lon_coords: np.ndarray,
    lon_west: float,
    lon_east: float,
) -> tuple[np.ndarray, np.ndarray]:
    """Extract a longitude sub-range from a grid, handling anti-meridian.

    Splits the request when it crosses ±180° and concatenates the two
    halves so the returned array has contiguous longitude coverage from
    west to east.

    Args:
        data: Array with longitude as the last axis.
        lon_coords: 1-D sorted longitude coordinate array in [-180, 180).
        lon_west: Western longitude bound (degrees, any range).
        lon_east: Eastern longitude bound (degrees, any range).

    Returns:
        Tuple of (sub_data, sub_lon) with matching longitude axes.
    """
    ranges = split_antimeridian(lon_west, lon_east)

    if len(ranges) == 1:
        s = _lon_slice(lon_coords, ranges[0])
        return data[..., s], lon_coords[s]

    # Two ranges — extract each and concatenate along the lon axis
    parts_data = []
    parts_lon = []
    for rng in ranges:
        s = _lon_slice(lon_coords, rng)
        parts_data.append(data[..., s])
        parts_lon.append(lon_coords[s])

    return (
        np.concatenate(parts_data, axis=-1),
        np.concatenate(parts_lon, axis=0),
    )


def interpolate_at_point(
    data: np.ndarray,
    lat_coords: np.ndarray,
    lon_coords: np.ndarray,
    lat: float,
    lon: float,
) -> float:
    """Bilinear interpolation on a regular grid, anti-meridian-safe.

    For points near ±180°, the function wraps the grid so that the
    four surrounding cells are always available, even when they straddle
    the boundary.

    Args:
        data: 2-D array (lat, lon).
        lat_coords: 1-D latitude coordinate (descending, north-to-south).
        lon_coords: 1-D longitude coordinate (ascending, [-180, 180)).
        lat: Query latitude (degrees).
        lon: Query longitude (degrees, any range).

    Returns:
        Interpolated scalar value.

    Raises:
        ValueError: If the query point is outside the latitude range.
    """
    lon = wrap_longitude(lon)
    n_lat, n_lon = data.shape

    # --- latitude index (descending) ---
    # lat_coords is descending, so flip for searchsorted
    lat_desc = lat_coords  # already descending
    if lat > lat_desc[0] or lat < lat_desc[-1]:
        raise ValueError(
            f"Latitude {lat} outside grid range [{lat_desc[-1]}, {lat_desc[0]}]"
        )
    # searchsorted needs ascending; use flipped view
    lat_asc = lat_desc[::-1]
    j_asc = int(np.searchsorted(lat_asc, lat, side="right")) - 1
    j_asc = np.clip(j_asc, 0, n_lat - 2)
    # Map back to descending index
    j0 = n_lat - 2 - j_asc
    j1 = j0 + 1

    # --- longitude index (ascending, wrapping) ---
    i0 = int(np.searchsorted(lon_coords, lon, side="right")) - 1

    # Handle wraparound: if i0 is at the last index or before the first
    if i0 < 0:
        i0 = n_lon - 1
    i1 = (i0 + 1) % n_lon

    # Longitude fractions (handle wrap distance)
    lon0 = lon_coords[i0]
    lon1 = lon_coords[i1]
    dx = lon1 - lon0
    if dx <= 0:
        # Wraparound: lon0 is near +180, lon1 is near -180
        dx += 360.0
    dlon = lon - lon0
    if dlon < 0:
        dlon += 360.0
    fx = dlon / dx

    # Latitude fractions (descending: j0 is higher lat than j1)
    lat0 = lat_coords[j0]
    lat1 = lat_coords[j1]
    fy = (lat0 - lat) / (lat0 - lat1)

    # Bilinear weights
    v00 = float(data[j0, i0])
    v01 = float(data[j0, i1])
    v10 = float(data[j1, i0])
    v11 = float(data[j1, i1])

    return (
        v00 * (1 - fx) * (1 - fy)
        + v01 * fx * (1 - fy)
        + v10 * (1 - fx) * fy
        + v11 * fx * fy
    )


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
