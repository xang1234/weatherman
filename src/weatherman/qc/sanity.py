"""Sanity check — verify data values are physically reasonable.

Checks a Zarr store for:
  - Out-of-bounds values (outside known physical limits per variable)
  - All-zeros time slices (suspicious for most weather variables)
  - Excessive NaN fraction (nodata mask problems)

Physical bounds are intentionally wider than display ranges in
``layers.yaml``.  They answer "is this data from planet Earth?",
not "will this look good on the map?".

Usage::

    from weatherman.qc.sanity import check_sanity
    from weatherman.storage.zarr_schema import GFS_SCHEMA

    result = check_sanity("/path/to/run.zarr", GFS_SCHEMA)
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


# ---------------------------------------------------------------------------
# Physical bounds — "is this data from Earth?"
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class PhysicalBounds:
    """Acceptable min/max for a variable's values (NaN excluded)."""

    min: float
    max: float
    max_nan_fraction: float = 0.05  # default: flag if >5% NaN


# Bounds are wider than display ranges.  Sources:
#   Temperature: record low ~184 K (Vostok), record high ~330 K (Death Valley)
#   Wind components: jet stream peaks ~120 m/s, allow signed
#   Precipitation: extreme event accumulations up to ~1000 kg/m^2
#   Pressure: lowest recorded ~870 hPa, highest ~1084 hPa
#   Cloud cover: percentage, clamped [0, 100]
#   Wave height: max rogue wave ~30 m
#   Wave period: long swells up to ~30 s
#   Wave direction: degrees [0, 360]
PHYSICAL_BOUNDS: dict[str, PhysicalBounds] = {
    "tmp_2m": PhysicalBounds(min=150.0, max=350.0),
    "ugrd_10m": PhysicalBounds(min=-150.0, max=150.0),
    "vgrd_10m": PhysicalBounds(min=-150.0, max=150.0),
    "apcp_sfc": PhysicalBounds(min=0.0, max=2000.0),
    "prmsl": PhysicalBounds(min=85000.0, max=110000.0),
    "tcdc_atm": PhysicalBounds(min=0.0, max=100.0),
    # Wave variables: NaN over land is expected (~70% of global grid)
    "htsgw_sfc": PhysicalBounds(min=0.0, max=35.0, max_nan_fraction=0.80),
    "perpw_sfc": PhysicalBounds(min=0.0, max=40.0, max_nan_fraction=0.80),
    "dirpw_sfc": PhysicalBounds(min=0.0, max=360.0, max_nan_fraction=0.80),
}


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SanityIssue:
    """A single sanity problem found during the check."""

    variable: str
    kind: str  # "out_of_bounds", "all_zeros", "excessive_nan"
    detail: str

    def __str__(self) -> str:
        return f"[{self.kind}] {self.variable}: {self.detail}"


@dataclass
class SanityResult:
    """Aggregate result of a sanity check."""

    store_path: str
    variables_checked: int = 0
    issues: list[SanityIssue] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return len(self.issues) == 0

    @property
    def summary(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        return (
            f"Sanity {status}: "
            f"{self.variables_checked} variables checked, "
            f"{len(self.issues)} issue(s)"
        )


# ---------------------------------------------------------------------------
# Core check
# ---------------------------------------------------------------------------

def check_sanity(
    store_path: str | Path,
    schema: ZarrSchema,
    bounds: dict[str, PhysicalBounds] | None = None,
) -> SanityResult:
    """Check a Zarr store for physically unreasonable values.

    For each variable in the schema that exists in the store, checks:
      1. All finite values are within physical bounds
      2. No time slice is entirely zeros
      3. NaN fraction is within the allowed threshold

    Variables without configured bounds are skipped with a debug log.

    Args:
        store_path: Path to the Zarr store.
        schema: The expected dataset schema.
        bounds: Physical bounds per variable name.  Defaults to
            ``PHYSICAL_BOUNDS`` if not provided.

    Returns:
        A ``SanityResult`` with details of any problems.
    """
    store_path = Path(store_path)
    bounds = bounds if bounds is not None else PHYSICAL_BOUNDS
    result = SanityResult(store_path=str(store_path))

    root = zarr.open_group(str(store_path), mode="r")

    for var_name in schema.variables:
        if var_name not in root:
            continue  # completeness check handles missing variables

        var_bounds = bounds.get(var_name)
        if var_bounds is None:
            logger.debug(
                "QC sanity: no physical bounds configured for '%s', skipping",
                var_name,
            )
            continue

        arr = root[var_name]
        result.variables_checked += 1

        _check_variable(arr, var_name, var_bounds, schema, result)

    if result.passed:
        logger.info("QC sanity: PASS for %s", store_path)
    else:
        logger.warning(
            "QC sanity: FAIL for %s — %d issue(s)",
            store_path,
            len(result.issues),
        )

    return result


def _check_variable(
    arr: zarr.Array,
    var_name: str,
    bounds: PhysicalBounds,
    schema: ZarrSchema,
    result: SanityResult,
) -> None:
    """Run all sanity checks on a single variable."""
    for time_idx, fhour in enumerate(schema.forecast_hours):
        time_slice = arr[time_idx, :, :]

        is_float = np.issubdtype(time_slice.dtype, np.floating)
        if is_float:
            finite_mask = np.isfinite(time_slice)
            finite_vals = time_slice[finite_mask]
            nan_count = np.sum(~finite_mask)
        else:
            finite_vals = time_slice.ravel()
            nan_count = 0

        total_cells = time_slice.size

        # -- Check 1: Out-of-bounds values --
        if finite_vals.size > 0:
            val_min = float(np.min(finite_vals))
            val_max = float(np.max(finite_vals))

            if val_min < bounds.min or val_max > bounds.max:
                result.issues.append(
                    SanityIssue(
                        variable=var_name,
                        kind="out_of_bounds",
                        detail=(
                            f"fhour={fhour}: values [{val_min:.4g}, {val_max:.4g}] "
                            f"outside physical bounds [{bounds.min}, {bounds.max}]"
                        ),
                    )
                )
                logger.warning(
                    "QC sanity: %s fhour=%d out of bounds "
                    "[%.4g, %.4g] vs [%g, %g]",
                    var_name, fhour, val_min, val_max,
                    bounds.min, bounds.max,
                )

        # -- Check 2: All-zeros slice --
        if finite_vals.size > 0 and np.all(finite_vals == 0.0):
            result.issues.append(
                SanityIssue(
                    variable=var_name,
                    kind="all_zeros",
                    detail=f"fhour={fhour}: entire time slice is zero",
                )
            )
            logger.warning(
                "QC sanity: %s fhour=%d is all zeros", var_name, fhour,
            )

        # -- Check 3: Excessive NaN fraction --
        if total_cells > 0:
            nan_frac = nan_count / total_cells
            if nan_frac > bounds.max_nan_fraction:
                result.issues.append(
                    SanityIssue(
                        variable=var_name,
                        kind="excessive_nan",
                        detail=(
                            f"fhour={fhour}: NaN fraction {nan_frac:.1%} "
                            f"exceeds threshold {bounds.max_nan_fraction:.0%}"
                        ),
                    )
                )
                logger.warning(
                    "QC sanity: %s fhour=%d NaN fraction %.1f%% > %.0f%%",
                    var_name, fhour, nan_frac * 100,
                    bounds.max_nan_fraction * 100,
                )
