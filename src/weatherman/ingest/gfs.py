"""Herbie-based GFS GRIB2 downloader.

Downloads GFS forecast variables for a given model cycle and stages
the raw GRIB2 files into the staging area of object storage.

Usage:
    result = download_gfs_cycle(
        run_id=RunID("20260306T00Z"),
        staging_dir=Path("/tmp/staging"),
    )
"""

from __future__ import annotations

import logging
import shutil
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from herbie import Herbie

from weatherman.storage.paths import RunID

logger = logging.getLogger(__name__)

# GFS atmospheric variables
DEFAULT_SEARCH_PATTERNS: dict[str, str] = {
    "tmp_2m": ":TMP:2 m above ground:",
    "ugrd_10m": ":UGRD:10 m above ground:",
    "vgrd_10m": ":VGRD:10 m above ground:",
    "apcp_sfc": ":APCP:surface:",
    "prmsl": ":PRMSL:mean sea level:",
    "tcdc_atm": ":TCDC:entire atmosphere:(?!.*ave)",
}

# GFS-Wave (WW3) variables — accessed via Herbie model="gfs_wave"
DEFAULT_WAVE_SEARCH_PATTERNS: dict[str, str] = {
    "htsgw_sfc": ":HTSGW:surface:",
    "perpw_sfc": ":PERPW:surface:",
    "dirpw_sfc": ":DIRPW:surface:",
}

# GFS cycles run every 6 hours
GFS_CYCLE_HOURS = (0, 6, 12, 18)

# Default forecast hours to download (0 through 120 at 3h intervals)
DEFAULT_FORECAST_HOURS = list(range(0, 121, 3))


@dataclass(frozen=True)
class DownloadResult:
    """Result of a single GRIB2 variable/forecast-hour download."""

    variable: str
    forecast_hour: int
    local_path: Path
    size_bytes: int


@dataclass
class CycleDownloadResult:
    """Aggregate result for an entire GFS cycle download."""

    run_id: RunID
    downloads: list[DownloadResult] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def success_count(self) -> int:
        return len(self.downloads)

    @property
    def error_count(self) -> int:
        return len(self.errors)

    @property
    def total_bytes(self) -> int:
        return sum(d.size_bytes for d in self.downloads)


def _herbie_for_hour(
    run_id: RunID,
    forecast_hour: int,
    *,
    model: str = "gfs",
    product: str = "pgrb2.0p25",
) -> Herbie:
    """Create a Herbie instance for a specific forecast hour."""
    cycle_dt = run_id.as_datetime
    return Herbie(
        cycle_dt.strftime("%Y-%m-%d %H:%M"),
        model=model,
        product=product,
        fxx=forecast_hour,
    )


def download_variable(
    run_id: RunID,
    forecast_hour: int,
    variable_name: str,
    search_pattern: str,
    staging_dir: Path,
    *,
    model: str = "gfs",
    product: str = "pgrb2.0p25",
) -> DownloadResult:
    """Download a single GFS/GFS-Wave variable for one forecast hour.

    Uses Herbie's subset download (GRIB2 index-based) to pull only the
    needed variable, not the full file.

    Args:
        run_id: The model run identifier.
        forecast_hour: Forecast hour (e.g. 0, 3, 6, ...).
        variable_name: Short name for the variable (e.g. "tmp_2m").
        search_pattern: Herbie search/regex pattern for the variable.
        staging_dir: Local directory to stage downloaded files.
        model: Herbie model name (default: "gfs").
        product: Herbie product name (default: "pgrb2.0p25").

    Returns:
        DownloadResult with the local path and file size.

    Raises:
        FileNotFoundError: If download produces no file.
        RuntimeError: If Herbie cannot find the data.
    """
    h = _herbie_for_hour(run_id, forecast_hour, model=model, product=product)

    dest_dir = staging_dir / "grib2" / variable_name
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_file = dest_dir / f"f{forecast_hour:03d}.grib2"

    logger.info(
        "Downloading %s fxx=%03d for %s",
        variable_name,
        forecast_hour,
        run_id,
    )

    # Herbie downloads to its cache and returns the Path.
    # Guard against silent failures where the file doesn't exist.
    downloaded = Path(h.download(search_pattern))
    if not downloaded.exists():
        raise FileNotFoundError(
            f"Herbie download returned no file for {variable_name} "
            f"fxx={forecast_hour} run={run_id}"
        )

    shutil.copy2(downloaded, dest_file)

    size = dest_file.stat().st_size
    logger.info(
        "Downloaded %s fxx=%03d: %s (%d bytes)",
        variable_name,
        forecast_hour,
        dest_file,
        size,
    )

    return DownloadResult(
        variable=variable_name,
        forecast_hour=forecast_hour,
        local_path=dest_file,
        size_bytes=size,
    )


def download_gfs_cycle(
    run_id: RunID,
    staging_dir: Path,
    *,
    forecast_hours: list[int] | None = None,
    variables: dict[str, str] | None = None,
    model: str = "gfs",
    product: str = "pgrb2.0p25",
) -> CycleDownloadResult:
    """Download all required variables for a model cycle.

    Iterates over forecast hours and variables, downloading each via
    Herbie's index-based subsetting. Partial failures are captured in
    the result rather than aborting the entire cycle.

    Args:
        run_id: The GFS cycle to download (e.g. RunID("20260306T00Z")).
        staging_dir: Local directory for staging GRIB2 files.
        forecast_hours: Which forecast hours to fetch (default: 0-120 by 3h).
        variables: Dict of {name: herbie_search_pattern} (default: atmo GFS set).
        model: Herbie model name (default: "gfs").
        product: Herbie product name (default: "pgrb2.0p25").

    Returns:
        CycleDownloadResult with per-file results and any errors.
    """
    if forecast_hours is None:
        forecast_hours = DEFAULT_FORECAST_HOURS
    if variables is None:
        variables = DEFAULT_SEARCH_PATTERNS

    cycle_hour = run_id.cycle_hour
    if cycle_hour not in GFS_CYCLE_HOURS:
        raise ValueError(
            f"Run {run_id} has cycle hour {cycle_hour}, "
            f"but GFS only runs at {GFS_CYCLE_HOURS}"
        )

    # Ensure staging directory matches the canonical layout
    run_staging = staging_dir / str(run_id)
    run_staging.mkdir(parents=True, exist_ok=True)

    result = CycleDownloadResult(run_id=run_id)

    logger.info(
        "Starting %s download for %s: %d hours x %d variables",
        model,
        run_id,
        len(forecast_hours),
        len(variables),
    )

    for fhour in forecast_hours:
        for var_name, search_pattern in variables.items():
            try:
                dl = download_variable(
                    run_id=run_id,
                    forecast_hour=fhour,
                    variable_name=var_name,
                    search_pattern=search_pattern,
                    staging_dir=run_staging,
                    model=model,
                    product=product,
                )
                result.downloads.append(dl)
            except Exception as exc:
                msg = f"{var_name} fxx={fhour:03d}: {exc}"
                logger.warning("Download failed: %s", msg)
                result.errors.append(msg)

    logger.info(
        "GFS download complete for %s: %d ok, %d errors, %.1f MB total",
        run_id,
        result.success_count,
        result.error_count,
        result.total_bytes / (1024 * 1024),
    )

    return result


def check_cycle_available(run_id: RunID) -> bool:
    """Check if a GFS cycle's data is available on any source.

    Probes forecast hour 0 to see if the cycle has been published.
    """
    try:
        h = _herbie_for_hour(run_id, forecast_hour=0)
        # Herbie's inventory fetches the .idx file; if it exists, data is available
        inv = h.inventory()
        return len(inv) > 0
    except Exception:
        logger.debug("Cycle %s not yet available", run_id)
        return False


def latest_available_cycle() -> RunID | None:
    """Find the most recent GFS cycle that has data available.

    Generates the last 5 distinct GFS cycles in reverse chronological
    order and returns the first one that has data.
    """
    from datetime import timedelta, timezone

    now = datetime.now(timezone.utc)

    # Snap to the most recent cycle hour at or before now
    latest_hour = max(h for h in GFS_CYCLE_HOURS if h <= now.hour)
    base = now.replace(hour=latest_hour, minute=0, second=0, microsecond=0)

    # Check the last 5 cycles (covers ~24h)
    seen: set[str] = set()
    for i in range(5):
        candidate = base - timedelta(hours=6 * i)
        run_id = RunID.from_datetime(candidate)
        if run_id.value in seen:
            continue
        seen.add(run_id.value)
        if check_cycle_available(run_id):
            logger.info("Latest available cycle: %s", run_id)
            return run_id

    logger.warning("No available GFS cycle found in last 24 hours")
    return None
