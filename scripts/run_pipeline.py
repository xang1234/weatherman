#!/usr/bin/env python
"""End-to-end GFS weather pipeline for local development.

Downloads GFS forecast data, generates Cloud Optimized GeoTIFFs,
and writes the catalog + manifest so the weatherman server can
serve weather tiles via TiTiler.

Usage:
    # Fetch the latest available GFS cycle (forecast hour 0 only):
    uv run python scripts/run_pipeline.py

    # Specific cycle with multiple forecast hours:
    uv run python scripts/run_pipeline.py --run-id 20260308T00Z --hours 0,3,6,9,12

    # Custom data directory:
    uv run python scripts/run_pipeline.py --data-dir /tmp/wx-data

After running, start the stack:
    uv run python scripts/run_titiler.py              # TiTiler on :8080
    WEATHERMAN_DATA_DIR=.data TITILER_COG_ROOT=.data uv run python -m weatherman
    cd frontend && npx vite dev
"""

from __future__ import annotations

import argparse
import logging
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import sqlalchemy as sa

from weatherman.events.emissions import emit_run_published
from weatherman.ingest.gfs import (
    DEFAULT_SEARCH_PATTERNS,
    download_gfs_cycle,
    latest_available_cycle,
)
from weatherman.processing.cog import grib2_to_cog, wind_speed_to_cog
from weatherman.storage.catalog import RunCatalog
from weatherman.storage.lifecycle import DuplicateRun, RunLifecycle, RunState
from weatherman.storage.manifest import (
    LayerConfig,
    ManifestConfig,
    ValueRange,
    build_manifest,
)
from weatherman.storage.object_store import LocalObjectStore
from weatherman.storage.paths import RunID, StorageLayout
from weatherman.storage.publish import publish_run

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("pipeline")


# Only download variables we can render (have colormaps defined):
#   temperature   ← tmp_2m
#   wind_speed    ← ugrd_10m + vgrd_10m
#   precipitation ← apcp_sfc
PIPELINE_VARIABLES = {
    k: DEFAULT_SEARCH_PATTERNS[k]
    for k in ("tmp_2m", "ugrd_10m", "vgrd_10m", "apcp_sfc")
}

# Layer definitions matching colormaps.py
LAYER_CONFIGS = [
    LayerConfig(
        id="temperature",
        display_name="Temperature at 2m",
        unit="°C",
        palette_name="temperature",
        value_range=ValueRange(min=-55.0, max=55.0),
    ),
    LayerConfig(
        id="wind_speed",
        display_name="Wind Speed at 10m",
        unit="m/s",
        palette_name="wind_speed",
        value_range=ValueRange(min=0.0, max=50.0),
    ),
    LayerConfig(
        id="precipitation",
        display_name="Total Precipitation",
        unit="kg/m^2",
        palette_name="precipitation",
        value_range=ValueRange(min=0.0, max=250.0),
    ),
]

PROCESSING_VERSION = "local-dev"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="GFS weather pipeline — download, process, publish locally.",
    )
    parser.add_argument(
        "--run-id",
        help="GFS cycle to fetch (e.g. 20260308T00Z). Default: latest available.",
    )
    parser.add_argument(
        "--hours",
        default="0",
        help="Comma-separated forecast hours to download (default: 0).",
    )
    parser.add_argument(
        "--data-dir",
        default=".data",
        help="Local data directory (default: .data/).",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Pipeline steps
# ---------------------------------------------------------------------------


def step_download(
    run_id: RunID,
    forecast_hours: list[int],
    staging_dir: Path,
) -> Path:
    """Download GFS GRIB2 files. Returns the run staging directory."""
    logger.info(
        "Step 1/4: Downloading GFS GRIB2 for %s (hours: %s)",
        run_id,
        forecast_hours,
    )
    result = download_gfs_cycle(
        run_id=run_id,
        staging_dir=staging_dir,
        forecast_hours=forecast_hours,
        variables=PIPELINE_VARIABLES,
    )
    logger.info(
        "Download complete: %d files (%.1f MB), %d errors",
        result.success_count,
        result.total_bytes / (1024 * 1024),
        result.error_count,
    )
    if result.error_count > 0:
        for err in result.errors:
            logger.warning("  %s", err)

    # download_gfs_cycle writes to staging_dir/<run_id>/grib2/...
    return staging_dir / str(run_id)


def step_generate_cogs(
    run_id: RunID,
    forecast_hours: list[int],
    grib2_dir: Path,
    data_dir: Path,
    layout: StorageLayout,
) -> set[str]:
    """Generate COGs for all layers and forecast hours.

    Returns the set of layer IDs that had at least one COG generated.
    """
    logger.info("Step 2/4: Generating Cloud Optimized GeoTIFFs")
    total = 0
    generated_layers: set[str] = set()

    for fhour in forecast_hours:
        # Temperature (direct: tmp_2m → temperature)
        tmp_grib = grib2_dir / "grib2" / "tmp_2m" / f"f{fhour:03d}.grib2"
        if tmp_grib.exists():
            cog_path = data_dir / layout.staging_cog_path(run_id, "temperature", fhour)
            grib2_to_cog(tmp_grib, cog_path)
            total += 1
            generated_layers.add("temperature")
            logger.info("  temperature/f%03d", fhour)

        # Wind speed (derived: sqrt(u² + v²))
        u_grib = grib2_dir / "grib2" / "ugrd_10m" / f"f{fhour:03d}.grib2"
        v_grib = grib2_dir / "grib2" / "vgrd_10m" / f"f{fhour:03d}.grib2"
        if u_grib.exists() and v_grib.exists():
            cog_path = data_dir / layout.staging_cog_path(run_id, "wind_speed", fhour)
            wind_speed_to_cog(u_grib, v_grib, cog_path)
            total += 1
            generated_layers.add("wind_speed")
            logger.info("  wind_speed/f%03d", fhour)

        # Precipitation (direct: apcp_sfc → precipitation)
        apcp_grib = grib2_dir / "grib2" / "apcp_sfc" / f"f{fhour:03d}.grib2"
        if apcp_grib.exists():
            cog_path = data_dir / layout.staging_cog_path(run_id, "precipitation", fhour)
            grib2_to_cog(apcp_grib, cog_path)
            total += 1
            generated_layers.add("precipitation")
            logger.info("  precipitation/f%03d", fhour)

    logger.info("Generated %d COGs for layers: %s", total, sorted(generated_layers))
    return generated_layers


def step_write_manifest(
    run_id: RunID,
    forecast_hours: list[int],
    store: LocalObjectStore,
    layout: StorageLayout,
    generated_layers: set[str],
) -> None:
    """Write the UI manifest for the frontend.

    Only includes layers that have actual COG data (from generated_layers).
    """
    logger.info("Step 3/4: Writing UI manifest")
    active_layers = [lc for lc in LAYER_CONFIGS if lc.id in generated_layers]
    if not active_layers:
        logger.warning("No layers generated — skipping manifest write")
        return
    config = ManifestConfig(
        model="gfs",
        run_id=run_id,
        published_at=datetime.now(timezone.utc),
        resolution_km=25.0,
        layers=active_layers,
        forecast_hours=forecast_hours,
    )
    manifest = build_manifest(config)
    manifest_path = layout.staging_manifest_path(run_id)
    store.write_bytes(manifest_path, manifest.to_json().encode("utf-8"))
    logger.info("  %s (layers: %s)", manifest_path, [l.id for l in active_layers])

def _lifecycle_for_data_dir(data_dir: Path) -> RunLifecycle:
    """Create or open the local pipeline lifecycle database."""
    engine = sa.create_engine(f"sqlite:///{data_dir / '.pipeline-lifecycle.sqlite3'}")
    lifecycle = RunLifecycle(engine)
    lifecycle.create_tables()
    return lifecycle


def _emit_run_published_if_available(model: str, run_id: RunID, published_at: datetime) -> None:
    """Emit run.published when an in-process event bus is available."""
    try:
        emit_run_published(model=model, run_id=run_id, published_at=published_at)
    except RuntimeError:
        logger.debug("No in-process event bus available for run.published emission")


def step_publish_run(
    run_id: RunID,
    store: LocalObjectStore,
    layout: StorageLayout,
    data_dir: Path,
) -> None:
    """Publish staged artifacts via the canonical publish helper."""
    logger.info("Step 4/4: Publishing staged artifacts")
    catalog_path = layout.catalog_path
    if store.exists(catalog_path):
        catalog = RunCatalog.from_json(store.read_bytes(catalog_path).decode("utf-8"))
    else:
        catalog = RunCatalog.new("gfs")

    lifecycle = _lifecycle_for_data_dir(data_dir)

    if catalog.get_entry(run_id) is not None:
        logger.warning(
            "Run %s already exists in catalog; falling back to current-pointer update",
            run_id,
        )
        catalog.rollback_to(run_id)
        store.write_bytes(catalog_path, catalog.to_json().encode("utf-8"))
        return

    try:
        lifecycle.register("gfs", run_id, PROCESSING_VERSION)
    except DuplicateRun:
        logger.info(
            "Lifecycle already exists for %s/%s; reusing existing row",
            layout.model, run_id,
        )
    for state in (RunState.INGESTING, RunState.STAGED, RunState.VALIDATED):
        try:
            lifecycle.transition(
                "gfs",
                run_id,
                PROCESSING_VERSION,
                state,
                context="local dev pipeline",
            )
        except Exception as exc:
            logger.debug("Skipping lifecycle transition to %s: %s", state.value, exc)

    publish_run(
        store=store,
        layout=layout,
        catalog=catalog,
        lifecycle=lifecycle,
        run_id=run_id,
        processing_version=PROCESSING_VERSION,
        on_published=_emit_run_published_if_available,
    )
    logger.info("  Current run: %s", catalog.current_run_id)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    args = parse_args()
    data_dir = Path(args.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)

    forecast_hours = [int(h.strip()) for h in args.hours.split(",")]

    # Resolve run ID
    if args.run_id:
        run_id = RunID(args.run_id)
        logger.info("Using specified run: %s", run_id)
    else:
        logger.info("Finding latest available GFS cycle...")
        run_id = latest_available_cycle()
        if run_id is None:
            logger.error(
                "No available GFS cycle found. Try specifying --run-id."
            )
            sys.exit(1)
        logger.info("Latest available: %s", run_id)

    store = LocalObjectStore(data_dir)
    layout = StorageLayout("gfs")

    # Download GRIB2 to a temp directory.
    # Herbie caches downloads internally, so re-runs are fast even though
    # the temp directory is cleaned up after COG generation.
    with tempfile.TemporaryDirectory(prefix="gfs-pipeline-") as tmpdir:
        grib2_dir = step_download(run_id, forecast_hours, Path(tmpdir))
        generated_layers = step_generate_cogs(
            run_id, forecast_hours, grib2_dir, data_dir, layout,
        )

    step_write_manifest(run_id, forecast_hours, store, layout, generated_layers)
    step_publish_run(run_id, store, layout, data_dir)

    logger.info("")
    logger.info("=" * 60)
    logger.info("Pipeline complete: %s", run_id)
    logger.info("  COGs:     %s/models/gfs/runs/%s/cogs/", data_dir, run_id)
    logger.info("  Manifest: %s/%s", data_dir, layout.manifest_path(run_id))
    logger.info("  Catalog:  %s/%s", data_dir, layout.catalog_path)
    logger.info("")
    logger.info("To view weather on the map:")
    logger.info("  1. uv run python scripts/run_titiler.py")
    logger.info(
        "  2. WEATHERMAN_DATA_DIR=%s TITILER_COG_ROOT=%s"
        " uv run python -m weatherman",
        data_dir,
        data_dir,
    )
    logger.info("  3. cd frontend && npx vite dev")
    logger.info("  4. Open http://localhost:5173 → select a weather layer")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
