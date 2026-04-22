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
from datetime import datetime, timezone
from pathlib import Path
from typing import TypedDict

import sqlalchemy as sa
from weatherman.events.emissions import emit_run_published
from weatherman.ingest.gfs import (
    DEFAULT_SEARCH_PATTERNS,
    DEFAULT_WAVE_SEARCH_PATTERNS,
    download_gfs_cycle,
    latest_available_cycle,
)
from weatherman.processing.cog import (
    OverviewConfig,
    grib2_to_cog,
    wave_direction_to_uv_cogs,
    wind_speed_to_cog,
)
from weatherman.processing.zarr_writer import grib2_dir_to_zarr
from weatherman.processing.data_tiles import (
    MAX_DATA_TILE_ZOOM,
    data_tile_resampling_for_layer,
    generate_all_data_tiles,
)
from weatherman.storage.catalog import RunCatalog, RunStatus
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
PIPELINE_VARIABLES = {
    k: DEFAULT_SEARCH_PATTERNS[k]
    for k in ("tmp_2m", "ugrd_10m", "vgrd_10m")
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
        id="wave_height",
        display_name="Significant Wave Height",
        unit="m",
        palette_name="wave_height",
        value_range=ValueRange(min=0.0, max=15.0),
    ),
]

PROCESSING_VERSION = "local-dev"


class ModelConfig(TypedDict):
    atmo_model: str
    atmo_product: str
    atmo_member: str | int | None
    wave_model: str
    wave_product: str
    wave_member: str | int | None
    resolution_km: float


# Model configurations for supported NWP sources.
# Each entry maps to the Herbie constructor arguments needed for
# both the atmospheric and wave download passes.
MODEL_CONFIGS: dict[str, ModelConfig] = {
    "gfs": {
        "atmo_model": "gfs",
        "atmo_product": "pgrb2.0p25",
        "atmo_member": None,
        "wave_model": "gfs_wave",
        "wave_product": "global.0p25",
        "wave_member": None,
        "resolution_km": 25.0,
    },
    "gefs": {
        "atmo_model": "gefs",
        "atmo_product": "atmos.25",
        "atmo_member": "mean",
        "wave_model": "gefs",
        "wave_product": "wave",
        "wave_member": "mean",
        "resolution_km": 25.0,
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Weather pipeline — download, process, publish locally.",
    )
    parser.add_argument(
        "--model",
        choices=list(MODEL_CONFIGS),
        default="gfs",
        help="NWP model to run (default: gfs).",
    )
    parser.add_argument(
        "--run-id",
        help="Model cycle to fetch (e.g. 20260308T00Z). Default: latest available.",
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
    parser.add_argument(
        "--max-runs",
        type=int,
        default=2,
        help="Max runs to keep per model (default: 2 = current + 1 previous).",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Pipeline steps
# ---------------------------------------------------------------------------


def step_download(
    run_id: RunID,
    forecast_hours: list[int],
    staging_dir: Path,
    *,
    model_config: ModelConfig,
) -> Path:
    """Download GRIB2 files for the configured model. Returns the run staging directory."""
    logger.info(
        "Step 1/5: Downloading %s GRIB2 for %s (hours: %s)",
        model_config["atmo_model"],
        run_id,
        forecast_hours,
    )
    result = download_gfs_cycle(
        run_id=run_id,
        staging_dir=staging_dir,
        forecast_hours=forecast_hours,
        variables=PIPELINE_VARIABLES,
        model=model_config["atmo_model"],
        product=model_config["atmo_product"],
        member=model_config["atmo_member"],
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

    # Wave download — separate model/product, same cycle/hours.
    # Wave data may not be available for all cycles; log and continue.
    logger.info("Downloading wave data (%s) for %s", model_config["wave_model"], run_id)
    try:
        wave_result = download_gfs_cycle(
            run_id=run_id,
            staging_dir=staging_dir,
            forecast_hours=forecast_hours,
            variables=DEFAULT_WAVE_SEARCH_PATTERNS,
            model=model_config["wave_model"],
            product=model_config["wave_product"],
            member=model_config["wave_member"],
        )
        logger.info(
            "Wave download: %d files (%.1f MB), %d errors",
            wave_result.success_count,
            wave_result.total_bytes / (1024 * 1024),
            wave_result.error_count,
        )
    except Exception as exc:
        logger.warning("Wave download failed (non-fatal): %s", exc)

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
    logger.info("Step 2/5: Generating Cloud Optimized GeoTIFFs")
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

            # Wind U/V components (raw): needed by the WebGL vector pipeline
            # which interpolates in Cartesian space then reconstructs speed.
            u_cog = data_dir / layout.staging_cog_path(run_id, "wind_u", fhour)
            grib2_to_cog(u_grib, u_cog)
            total += 1
            generated_layers.add("wind_u")
            logger.info("  wind_u/f%03d", fhour)

            v_cog = data_dir / layout.staging_cog_path(run_id, "wind_v", fhour)
            grib2_to_cog(v_grib, v_cog)
            total += 1
            generated_layers.add("wind_v")
            logger.info("  wind_v/f%03d", fhour)

        # Wave height and period are continuous ocean-only fields.
        wave_height_grib = grib2_dir / "grib2" / "htsgw_sfc" / f"f{fhour:03d}.grib2"
        if wave_height_grib.exists():
            cog_path = data_dir / layout.staging_cog_path(run_id, "wave_height", fhour)
            grib2_to_cog(wave_height_grib, cog_path, ocean_only=True)
            total += 1
            generated_layers.add("wave_height")
            logger.info("  wave_height/f%03d", fhour)

        wave_period_grib = grib2_dir / "grib2" / "perpw_sfc" / f"f{fhour:03d}.grib2"
        if wave_period_grib.exists():
            cog_path = data_dir / layout.staging_cog_path(run_id, "wave_period", fhour)
            grib2_to_cog(wave_period_grib, cog_path, ocean_only=True)
            total += 1
            generated_layers.add("wave_period")
            logger.info("  wave_period/f%03d", fhour)

        # Wave direction → Cartesian U/V components for the wave particle renderer.
        # No standalone scalar COG — circular data (degrees) is only consumed
        # as decomposed U/V by the GPU pipeline.
        wave_dir_grib = grib2_dir / "grib2" / "dirpw_sfc" / f"f{fhour:03d}.grib2"
        if wave_dir_grib.exists():
            dir_u_cog = data_dir / layout.staging_cog_path(run_id, "wave_dir_u", fhour)
            dir_v_cog = data_dir / layout.staging_cog_path(run_id, "wave_dir_v", fhour)
            wave_direction_to_uv_cogs(
                wave_dir_grib,
                dir_u_cog,
                dir_v_cog,
                overview_config=OverviewConfig.for_continuous(),
            )
            total += 2
            generated_layers.update({"wave_dir_u", "wave_dir_v"})
            logger.info("  wave_dir_u/f%03d", fhour)
            logger.info("  wave_dir_v/f%03d", fhour)

    logger.info("Generated %d COGs for layers: %s", total, sorted(generated_layers))
    return generated_layers


def step_generate_data_tiles(
    run_id: RunID,
    forecast_hours: list[int],
    data_dir: Path,
    store: LocalObjectStore,
    layout: StorageLayout,
    generated_layers: set[str],
    max_zoom: int = MAX_DATA_TILE_ZOOM,
) -> int:
    """Pre-generate static data tiles for each layer/hour.

    These tiles are placed in staging alongside the COGs and auto-publish
    with the rest of the run artifacts.

    Returns total tile count for logging.
    """
    from weatherman.tiling.colormaps import get_value_range

    logger.info("Step 3/5: Pre-generating data tiles (z0–z%d)", max_zoom)
    total = 0

    for layer in sorted(generated_layers):
        try:
            vmin, vmax = get_value_range(layer)
        except KeyError:
            logger.warning("  No value range for layer '%s', skipping data tiles", layer)
            continue

        for fhour in forecast_hours:
            cog_key = layout.staging_cog_path(run_id, layer, fhour)
            cog_path = data_dir / cog_key
            if not cog_path.exists():
                continue

            for tile_format in ("png", "f16"):
                count = 0
                tiles = generate_all_data_tiles(
                    str(cog_path),
                    vmin,
                    vmax,
                    max_zoom=max_zoom,
                    resampling=data_tile_resampling_for_layer(layer),
                    tile_format=tile_format,
                )
                try:
                    for z, x, y, tile_bytes in tiles:
                        tile_key = layout.staging_data_tile_path(
                            run_id, layer, fhour, z, x, y,
                            tile_format=tile_format,
                        )
                        store.write_bytes(tile_key, tile_bytes)
                        count += 1
                finally:
                    tiles.close()

                total += count
                logger.info(
                    "  %s/f%03d (%s): %d tiles",
                    layer, fhour, tile_format, count,
                )

    logger.info("Generated %d data tiles total", total)
    return total


def step_generate_zarr(
    run_id: RunID,
    forecast_hours: list[int],
    grib2_dir: Path,
    data_dir: Path,
    layout: StorageLayout,
) -> None:
    """Generate a Zarr store from GRIB2 files for EDR point/trajectory queries."""
    logger.info("Generating Zarr store for EDR queries")
    zarr_path = data_dir / layout.staging_zarr_path(run_id)
    grib2_vars_dir = grib2_dir / "grib2" if (grib2_dir / "grib2").is_dir() else grib2_dir
    written = grib2_dir_to_zarr(grib2_vars_dir, zarr_path, forecast_hours)
    logger.info("  Zarr: %s (%d variables)", zarr_path, len(written))


def step_write_manifest(
    run_id: RunID,
    forecast_hours: list[int],
    store: LocalObjectStore,
    layout: StorageLayout,
    generated_layers: set[str],
    *,
    model: str = "gfs",
    resolution_km: float = 25.0,
) -> None:
    """Write the UI manifest for the frontend.

    Only includes layers that have actual COG data (from generated_layers).
    """
    logger.info("Step 4/5: Writing UI manifest")
    active_layers = [lc for lc in LAYER_CONFIGS if lc.id in generated_layers]
    if not active_layers:
        logger.warning("No layers generated — skipping manifest write")
        return
    config = ManifestConfig(
        model=model,
        run_id=run_id,
        published_at=datetime.now(timezone.utc),
        resolution_km=resolution_km,
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
    *,
    model: str = "gfs",
) -> None:
    """Publish staged artifacts via the canonical publish helper."""
    logger.info("Step 5/5: Publishing staged artifacts")
    catalog_path = layout.catalog_path
    if store.exists(catalog_path):
        catalog = RunCatalog.from_json(store.read_bytes(catalog_path).decode("utf-8"))
    else:
        catalog = RunCatalog.new(model)

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
        lifecycle.register(model, run_id, PROCESSING_VERSION)
    except DuplicateRun:
        logger.info(
            "Lifecycle already exists for %s/%s; reusing existing row",
            layout.model, run_id,
        )
    for state in (RunState.INGESTING, RunState.STAGED, RunState.VALIDATED):
        try:
            lifecycle.transition(
                model,
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


def step_cleanup_old_runs(
    catalog: RunCatalog,
    store: LocalObjectStore,
    layout: StorageLayout,
    *,
    max_runs: int,
) -> None:
    """Expire and delete old runs, keeping only max_runs most recent."""
    superseded = sorted(
        catalog.superseded_runs(),
        key=lambda e: e.superseded_at or e.published_at,
        reverse=True,
    )

    # Keep max_runs - 1 superseded (1 slot is for the current published run)
    keep = max(max_runs - 1, 0)
    to_expire = superseded[keep:]

    if not to_expire:
        logger.info("Cleanup: nothing to expire (max_runs=%d)", max_runs)
        return

    for entry in to_expire:
        logger.info("Cleanup: expiring %s", entry.run_id)
        catalog.expire_run(entry.run_id)

        run_prefix = layout.run_prefix(entry.run_id)
        store.delete(run_prefix)
        logger.info("  Deleted %s", run_prefix)

        grib2_prefix = f"{layout.model_prefix}/grib2/{entry.run_id}"
        store.delete(grib2_prefix)
        logger.info("  Deleted %s", grib2_prefix)

    store.write_bytes(layout.catalog_path, catalog.to_json().encode("utf-8"))
    logger.info("Cleanup: expired %d run(s), kept %d", len(to_expire), keep + 1)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    args = parse_args()
    model = args.model
    model_config = MODEL_CONFIGS[model]
    data_dir = Path(args.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)

    forecast_hours = [int(h.strip()) for h in args.hours.split(",")]

    # Resolve run ID
    if args.run_id:
        run_id = RunID(args.run_id)
        logger.info("Using specified run: %s", run_id)
    else:
        logger.info("Finding latest available %s cycle...", model)
        run_id = latest_available_cycle(
            model=model_config["atmo_model"],
            product=model_config["atmo_product"],
            member=model_config["atmo_member"],
        )
        if run_id is None:
            logger.error(
                "No available %s cycle found. Try specifying --run-id.", model,
            )
            sys.exit(1)
        logger.info("Latest available: %s", run_id)

    store = LocalObjectStore(data_dir)
    layout = StorageLayout(model)

    # Skip pipeline if this run is already published
    catalog_path = layout.catalog_path
    if store.exists(catalog_path):
        catalog = RunCatalog.from_json(store.read_bytes(catalog_path).decode("utf-8"))
        entry = catalog.get_entry(run_id)
        if entry is not None and entry.status in (RunStatus.PUBLISHED, RunStatus.SUPERSEDED):
            logger.info(
                "Run %s already exists in catalog (status: %s) — skipping pipeline.",
                run_id,
                entry.status.value,
            )
            # Still clean up old runs even when skipping download
            step_cleanup_old_runs(
                catalog, store, layout, max_runs=args.max_runs,
            )
            sys.exit(0)

    # Store GRIB2 files persistently so they survive container restarts.
    # Individual files are cache-checked in download_variable().
    grib2_base = data_dir / "models" / model / "grib2"
    grib2_base.mkdir(parents=True, exist_ok=True)
    grib2_dir = step_download(run_id, forecast_hours, grib2_base, model_config=model_config)
    generated_layers = step_generate_cogs(
        run_id, forecast_hours, grib2_dir, data_dir, layout,
    )

    step_generate_zarr(run_id, forecast_hours, grib2_dir, data_dir, layout)
    step_generate_data_tiles(
        run_id, forecast_hours, data_dir, store, layout, generated_layers,
    )
    step_write_manifest(
        run_id, forecast_hours, store, layout, generated_layers,
        model=model, resolution_km=model_config["resolution_km"],
    )
    step_publish_run(run_id, store, layout, data_dir, model=model)
    published_catalog = RunCatalog.from_json(
        store.read_bytes(layout.catalog_path).decode("utf-8"),
    )
    step_cleanup_old_runs(
        published_catalog, store, layout, max_runs=args.max_runs,
    )

    logger.info("")
    logger.info("=" * 60)
    logger.info("Pipeline complete (%s): %s", model, run_id)
    logger.info("  COGs:     %s/models/%s/runs/%s/cogs/", data_dir, model, run_id)
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
