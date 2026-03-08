"""Geometry check — verify grid coordinates and spatial coverage.

Checks a Zarr store for:
  - Correct grid extent (lat spans [-90, 90], lon spans [-180, 180))
  - Uniform coordinate spacing matching the schema grid resolution
  - Coordinate monotonicity (lat descending, lon ascending)
  - Longitude convention is [-180, 180), not [0, 360)
  - Anti-meridian continuity (no data gap at ±180° boundary)
  - Polar coverage (data exists at extreme latitudes)

Usage::

    from weatherman.qc.geometry import check_geometry
    from weatherman.storage.zarr_schema import GFS_SCHEMA

    result = check_geometry("/path/to/run.zarr", GFS_SCHEMA)
    if not result.passed:
        for issue in result.issues:
            print(issue)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import zarr

from weatherman.storage.zarr_schema import ZarrSchema

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class GeometryIssue:
    """A single geometry problem found during the check."""

    coordinate: str  # "lat", "lon", or variable name
    kind: str
    detail: str

    def __str__(self) -> str:
        return f"[{self.kind}] {self.coordinate}: {self.detail}"


@dataclass
class GeometryResult:
    """Aggregate result of a geometry check."""

    store_path: str
    issues: list[GeometryIssue] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return len(self.issues) == 0

    @property
    def summary(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        return f"Geometry {status}: {len(self.issues)} issue(s)"


# Tolerance for floating-point coordinate comparisons (float32 precision)
_COORD_ATOL = 1e-4


# Variables with large expected NaN coverage (ocean-only data).
# These are unsuitable for anti-meridian and polar gap checks because
# they legitimately have NaN over all land areas.
_OCEAN_ONLY_VARS = frozenset({"htsgw_sfc", "perpw_sfc", "dirpw_sfc"})


def _pick_global_var(root: zarr.Group, schema: ZarrSchema) -> str | None:
    """Select a variable suitable for spatial coverage checks.

    Prefers globally-filled atmospheric variables over ocean-only wave
    variables, which have legitimate NaN over land (e.g., Antarctica).
    """
    fallback = None
    for var_name in schema.variables:
        if var_name not in root:
            continue
        if var_name not in _OCEAN_ONLY_VARS:
            return var_name
        if fallback is None:
            fallback = var_name
    return fallback


def check_geometry(
    store_path: str | Path,
    schema: ZarrSchema,
) -> GeometryResult:
    """Check a Zarr store's grid geometry against the schema.

    Args:
        store_path: Path to the Zarr store.
        schema: The expected dataset schema.

    Returns:
        A ``GeometryResult`` with details of any problems.
    """
    store_path = Path(store_path)
    result = GeometryResult(store_path=str(store_path))

    root = zarr.open_group(str(store_path), mode="r")

    _check_lat(root, schema, result)
    _check_lon(root, schema, result)
    _check_antimeridian(root, schema, result)
    _check_polar(root, schema, result)

    if result.passed:
        logger.info("QC geometry: PASS for %s", store_path)
    else:
        logger.warning(
            "QC geometry: FAIL for %s — %d issue(s)",
            store_path,
            len(result.issues),
        )

    return result


def _check_lat(
    root: zarr.Group, schema: ZarrSchema, result: GeometryResult
) -> None:
    """Verify latitude coordinate array."""
    if "lat" not in root:
        result.issues.append(
            GeometryIssue("lat", "missing_coordinate", "No 'lat' array in store")
        )
        return

    lat = np.asarray(root["lat"][:])
    expected_lat = schema.lat_array

    # Shape
    if lat.shape != expected_lat.shape:
        result.issues.append(
            GeometryIssue(
                "lat", "shape_mismatch",
                f"Expected shape {expected_lat.shape}, got {lat.shape}",
            )
        )
        return

    # Values match expected
    if not np.allclose(lat, expected_lat, atol=_COORD_ATOL):
        max_diff = float(np.max(np.abs(lat - expected_lat)))
        result.issues.append(
            GeometryIssue(
                "lat", "values_mismatch",
                f"Latitude values differ from schema (max diff: {max_diff:.6f}°)",
            )
        )

    # Monotonicity: must be descending (north to south)
    diffs = np.diff(lat)
    if not np.all(diffs <= 0):
        n_increasing = int(np.sum(diffs > 0))
        result.issues.append(
            GeometryIssue(
                "lat", "not_descending",
                f"Latitude must be descending (N→S), found {n_increasing} "
                f"increasing step(s)",
            )
        )

    # Extent
    if not (np.isclose(lat[0], 90.0, atol=_COORD_ATOL)
            and np.isclose(lat[-1], -90.0, atol=_COORD_ATOL)):
        result.issues.append(
            GeometryIssue(
                "lat", "extent_mismatch",
                f"Expected lat range [90, -90], got [{lat[0]:.4f}, {lat[-1]:.4f}]",
            )
        )

    # Uniform spacing
    if len(lat) > 1:
        steps = np.abs(diffs)
        expected_step = schema.grid.step
        if not np.allclose(steps, expected_step, atol=_COORD_ATOL):
            result.issues.append(
                GeometryIssue(
                    "lat", "non_uniform_spacing",
                    f"Expected uniform {expected_step}° spacing, "
                    f"got range [{float(np.min(steps)):.6f}, "
                    f"{float(np.max(steps)):.6f}]",
                )
            )


def _check_lon(
    root: zarr.Group, schema: ZarrSchema, result: GeometryResult
) -> None:
    """Verify longitude coordinate array."""
    if "lon" not in root:
        result.issues.append(
            GeometryIssue("lon", "missing_coordinate", "No 'lon' array in store")
        )
        return

    lon = np.asarray(root["lon"][:])
    expected_lon = schema.lon_array

    # Shape
    if lon.shape != expected_lon.shape:
        result.issues.append(
            GeometryIssue(
                "lon", "shape_mismatch",
                f"Expected shape {expected_lon.shape}, got {lon.shape}",
            )
        )
        return

    # Values match expected
    if not np.allclose(lon, expected_lon, atol=_COORD_ATOL):
        max_diff = float(np.max(np.abs(lon - expected_lon)))
        result.issues.append(
            GeometryIssue(
                "lon", "values_mismatch",
                f"Longitude values differ from schema (max diff: {max_diff:.6f}°)",
            )
        )

    # Monotonicity: must be ascending
    diffs = np.diff(lon)
    if not np.all(diffs > 0):
        n_bad = int(np.sum(diffs <= 0))
        result.issues.append(
            GeometryIssue(
                "lon", "not_ascending",
                f"Longitude must be ascending, found {n_bad} non-increasing step(s)",
            )
        )

    # Convention: must be [-180, 180)
    lon_min = float(np.min(lon))
    lon_max = float(np.max(lon))
    expected_max = 180.0 - schema.grid.step  # e.g., 179.75 for 0.25°

    if lon_min < -180.0 - _COORD_ATOL:
        result.issues.append(
            GeometryIssue(
                "lon", "convention_violation",
                f"Longitude values below -180°: min={lon_min:.4f}",
            )
        )
    if lon_max > expected_max + _COORD_ATOL:
        result.issues.append(
            GeometryIssue(
                "lon", "convention_violation",
                f"Longitude max {lon_max:.4f}° exceeds expected "
                f"{expected_max}° for [-180, 180) convention",
            )
        )

    # Uniform spacing
    if len(lon) > 1:
        expected_step = schema.grid.step
        if not np.allclose(diffs, expected_step, atol=_COORD_ATOL):
            result.issues.append(
                GeometryIssue(
                    "lon", "non_uniform_spacing",
                    f"Expected uniform {expected_step}° spacing, "
                    f"got range [{float(np.min(diffs)):.6f}, "
                    f"{float(np.max(diffs)):.6f}]",
                )
            )


def _check_antimeridian(
    root: zarr.Group, schema: ZarrSchema, result: GeometryResult
) -> None:
    """Check for data gaps at the anti-meridian (±180° boundary).

    The anti-meridian is the seam where the longitude array transitions
    from the last column (near +180°) to the first column (at -180°).
    If the longitude normalization was applied incorrectly, a stripe of
    NaN/missing data often appears at these indices.
    """
    if "lon" not in root:
        return

    lon = np.asarray(root["lon"][:])
    if len(lon) == 0:
        return

    # Check first and last few longitude columns for any data variable
    edge_width = min(4, len(lon))
    west_cols = slice(0, edge_width)           # near -180°
    east_cols = slice(len(lon) - edge_width, len(lon))  # near +180°

    check_var = _pick_global_var(root, schema)
    if check_var is None:
        return

    arr = root[check_var]
    if not np.issubdtype(arr.dtype, np.floating):
        return

    # Check first time step only (sufficient for detecting systematic gaps)
    time_slice = arr[0, :, :]

    west_data = time_slice[:, west_cols]
    east_data = time_slice[:, east_cols]

    if np.all(np.isnan(west_data)):
        result.issues.append(
            GeometryIssue(
                "lon", "antimeridian_gap",
                f"All-NaN at western edge (lon ≈ {lon[0]:.2f}° to "
                f"{lon[edge_width - 1]:.2f}°) — possible longitude "
                f"normalization error",
            )
        )

    if np.all(np.isnan(east_data)):
        result.issues.append(
            GeometryIssue(
                "lon", "antimeridian_gap",
                f"All-NaN at eastern edge (lon ≈ {lon[len(lon) - edge_width]:.2f}° "
                f"to {lon[-1]:.2f}°) — possible longitude normalization error",
            )
        )


def _check_polar(
    root: zarr.Group, schema: ZarrSchema, result: GeometryResult
) -> None:
    """Check for data coverage at polar latitudes.

    GFS provides global coverage including poles. If the latitude
    normalization flipped incorrectly, the polar rows may be swapped
    or contain NaN.
    """
    if "lat" not in root:
        return

    check_var = _pick_global_var(root, schema)
    if check_var is None:
        return

    arr = root[check_var]
    if not np.issubdtype(arr.dtype, np.floating):
        return

    lat = np.asarray(root["lat"][:])
    time_slice = arr[0, :, :]

    # Check north pole row (first row, lat ≈ 90°)
    if np.isclose(lat[0], 90.0, atol=_COORD_ATOL):
        north_row = time_slice[0, :]
        if np.all(np.isnan(north_row)):
            result.issues.append(
                GeometryIssue(
                    "lat", "polar_gap",
                    "All-NaN at north pole (lat=90°) — possible "
                    "latitude normalization error",
                )
            )

    # Check south pole row (last row, lat ≈ -90°)
    if np.isclose(lat[-1], -90.0, atol=_COORD_ATOL):
        south_row = time_slice[-1, :]
        if np.all(np.isnan(south_row)):
            result.issues.append(
                GeometryIssue(
                    "lat", "polar_gap",
                    "All-NaN at south pole (lat=-90°) — possible "
                    "latitude normalization error",
                )
            )
