"""Completeness check — verify all expected data exists in a Zarr store.

Compares the contents of a Zarr store against a ``ZarrSchema`` to detect:
  - Missing variables (arrays not present in the store)
  - Missing forecast hours (time slices that are entirely NaN)
  - Shape mismatches (array shape differs from schema expectation)

Usage::

    from weatherman.qc.completeness import check_completeness
    from weatherman.storage.zarr_schema import GFS_SCHEMA

    result = check_completeness("/path/to/run.zarr", GFS_SCHEMA)
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
class CompletenessIssue:
    """A single completeness problem found during the check."""

    variable: str
    kind: str  # "missing_variable", "shape_mismatch", "missing_hours"
    detail: str

    def __str__(self) -> str:
        return f"[{self.kind}] {self.variable}: {self.detail}"


@dataclass
class CompletenessResult:
    """Aggregate result of a completeness check."""

    store_path: str
    expected_variables: int = 0
    present_variables: int = 0
    expected_hours: int = 0
    complete_hours: int = 0
    issues: list[CompletenessIssue] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return len(self.issues) == 0

    @property
    def summary(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        return (
            f"Completeness {status}: "
            f"{self.present_variables}/{self.expected_variables} variables, "
            f"{self.complete_hours}/{self.expected_hours} hours complete, "
            f"{len(self.issues)} issue(s)"
        )


def check_completeness(
    store_path: str | Path,
    schema: ZarrSchema,
) -> CompletenessResult:
    """Check a Zarr store for completeness against the given schema.

    Opens the store in read-only mode, iterates over all expected
    variables and forecast hours, and reports any gaps.

    Args:
        store_path: Path to the Zarr store (local filesystem).
        schema: The expected dataset schema.

    Returns:
        A ``CompletenessResult`` with details of any missing data.
    """
    store_path = Path(store_path)
    result = CompletenessResult(
        store_path=str(store_path),
        expected_variables=len(schema.variables),
        expected_hours=len(schema.forecast_hours),
    )

    root = zarr.open_group(str(store_path), mode="r")
    expected_shape = schema.shape

    # Track which hours are complete across ALL variables
    hour_complete_counts: dict[int, int] = {h: 0 for h in schema.forecast_hours}

    for var_name, var_def in schema.variables.items():
        # Check variable exists
        if var_name not in root:
            result.issues.append(
                CompletenessIssue(
                    variable=var_name,
                    kind="missing_variable",
                    detail=f"Array '{var_name}' not found in store",
                )
            )
            logger.warning("QC completeness: missing variable '%s'", var_name)
            continue

        result.present_variables += 1
        arr = root[var_name]

        # Check shape
        if arr.shape != expected_shape:
            result.issues.append(
                CompletenessIssue(
                    variable=var_name,
                    kind="shape_mismatch",
                    detail=(
                        f"Expected shape {expected_shape}, got {arr.shape}"
                    ),
                )
            )
            logger.warning(
                "QC completeness: shape mismatch for '%s': expected %s, got %s",
                var_name,
                expected_shape,
                arr.shape,
            )
            continue

        # Check each forecast hour for all-NaN slices
        missing_hours: list[int] = []
        for time_idx, fhour in enumerate(schema.forecast_hours):
            time_slice = arr[time_idx, :, :]
            if np.all(np.isnan(time_slice)):
                missing_hours.append(fhour)
            else:
                hour_complete_counts[fhour] += 1

        if missing_hours:
            result.issues.append(
                CompletenessIssue(
                    variable=var_name,
                    kind="missing_hours",
                    detail=(
                        f"{len(missing_hours)}/{len(schema.forecast_hours)} "
                        f"hours are all-NaN: {missing_hours}"
                    ),
                )
            )
            logger.warning(
                "QC completeness: variable '%s' missing %d forecast hours: %s",
                var_name,
                len(missing_hours),
                missing_hours,
            )

    # A forecast hour is "complete" only when ALL variables have data for it
    n_vars = len(schema.variables)
    result.complete_hours = sum(
        1 for count in hour_complete_counts.values() if count == n_vars
    )

    if result.passed:
        logger.info("QC completeness: PASS for %s", store_path)
    else:
        logger.warning(
            "QC completeness: FAIL for %s — %d issue(s)",
            store_path,
            len(result.issues),
        )

    return result
