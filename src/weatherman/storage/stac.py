"""STAC Item generation with processing provenance for weather model runs.

Generates a valid STAC Item per run that includes:
- Spatial extent (bbox, geometry)
- Temporal extent (run datetime + forecast range)
- Assets (one per COG file)
- Processing provenance via the processing extension
  (source URIs, pipeline version, QC results)
- Lifecycle timestamps via the timestamps extension

Reference:
  - https://github.com/radiantearth/stac-spec/blob/master/item-spec/item-spec.md
  - https://stac-extensions.github.io/processing/v1.2.0/schema.json
  - https://stac-extensions.github.io/timestamps/v1.1.0/schema.json
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import pystac
from pystac.extensions.timestamps import TimestampsExtension

from weatherman.storage.paths import RunID, StorageLayout

PROCESSING_EXTENSION = (
    "https://stac-extensions.github.io/processing/v1.2.0/schema.json"
)


@dataclass(frozen=True)
class QCResult:
    """Result of a single quality-control check."""

    name: str
    passed: bool
    message: str = ""


@dataclass(frozen=True)
class RunProvenance:
    """Processing provenance metadata for a weather model run.

    Captures everything needed to answer "where did this data come from?"
    """

    source_uris: list[str]
    processing_version: str
    herbie_version: str
    qc_results: list[QCResult]
    ingestion_started_at: datetime
    processing_completed_at: datetime
    published_at: datetime | None = None


@dataclass
class COGAsset:
    """Describes a single COG file to include as a STAC asset."""

    layer: str
    forecast_hour: int
    href: str


@dataclass
class StacItemConfig:
    """Configuration for building a STAC Item for a model run.

    Attributes:
        model: Model name (e.g. "gfs").
        run_id: The model run identifier.
        provenance: Processing provenance metadata.
        cog_assets: List of COG assets in this run.
        bbox: Bounding box [west, south, east, north] in WGS84.
        geometry: GeoJSON geometry dict (defaults to bbox polygon).
        forecast_hours: Range of forecast hours (min, max) for temporal extent.
        extra_properties: Additional properties to include in the STAC Item.
    """

    model: str
    run_id: RunID
    provenance: RunProvenance
    cog_assets: list[COGAsset]
    bbox: list[float] = field(default_factory=lambda: [-180.0, -90.0, 180.0, 90.0])
    geometry: dict[str, Any] | None = None
    forecast_hours: tuple[int, int] = (0, 384)
    extra_properties: dict[str, Any] = field(default_factory=dict)


def _bbox_to_polygon(bbox: list[float]) -> dict[str, Any]:
    """Convert a bbox to a GeoJSON Polygon geometry."""
    w, s, e, n = bbox
    return {
        "type": "Polygon",
        "coordinates": [[[w, s], [e, s], [e, n], [w, n], [w, s]]],
    }


def _qc_results_to_dict(results: list[QCResult]) -> dict[str, Any]:
    """Serialize QC results into a dict keyed by check name."""
    return {
        r.name: {"passed": r.passed, "message": r.message} for r in results
    }


def build_stac_item(config: StacItemConfig, layout: StorageLayout) -> pystac.Item:
    """Build a STAC Item for a weather model run.

    The Item is self-contained and validates against the STAC spec.
    Processing provenance is stored using the processing extension fields.
    Lifecycle timestamps use the timestamps extension.

    Args:
        config: Configuration describing the run and its provenance.
        layout: Storage layout for constructing asset hrefs.

    Returns:
        A pystac.Item ready for serialization to JSON.
    """
    run_id = config.run_id
    prov = config.provenance
    geometry = config.geometry or _bbox_to_polygon(config.bbox)

    run_dt = run_id.as_datetime

    # Temporal extent: start = run datetime, end = last forecast hour
    start_dt = run_dt
    end_dt = datetime(
        run_dt.year,
        run_dt.month,
        run_dt.day,
        run_dt.hour,
        tzinfo=timezone.utc,
    )
    # Add forecast hours as timedelta
    from datetime import timedelta

    end_dt = run_dt + timedelta(hours=config.forecast_hours[1])

    item = pystac.Item(
        id=f"{config.model}-{run_id.value}",
        geometry=geometry,
        bbox=config.bbox,
        datetime=run_dt,
        properties={
            "start_datetime": start_dt.isoformat(),
            "end_datetime": end_dt.isoformat(),
            **config.extra_properties,
        },
        stac_extensions=[PROCESSING_EXTENSION],
    )

    # -- Processing extension fields (manual, not built into pystac) --
    item.properties["processing:software"] = {
        "weatherman": prov.processing_version,
        "herbie": prov.herbie_version,
    }
    item.properties["processing:version"] = prov.processing_version
    item.properties["processing:source_uris"] = prov.source_uris
    item.properties["processing:qc_results"] = _qc_results_to_dict(prov.qc_results)
    item.properties["processing:ingestion_started_at"] = (
        prov.ingestion_started_at.isoformat()
    )
    item.properties["processing:processing_completed_at"] = (
        prov.processing_completed_at.isoformat()
    )

    # -- Timestamps extension --
    ts_ext = TimestampsExtension.ext(item, add_if_missing=True)
    if prov.published_at is not None:
        ts_ext.published = prov.published_at

    # -- Assets: one per COG --
    for cog in config.cog_assets:
        asset_key = f"{cog.layer}_f{cog.forecast_hour:03d}"
        item.add_asset(
            asset_key,
            pystac.Asset(
                href=cog.href,
                media_type=pystac.MediaType.COG,
                roles=["data"],
                title=f"{cog.layer} forecast hour {cog.forecast_hour}",
                extra_fields={
                    "weatherman:layer": cog.layer,
                    "weatherman:forecast_hour": cog.forecast_hour,
                },
            ),
        )

    # Zarr asset (the canonical dataset)
    zarr_path = layout.zarr_path(run_id)
    item.add_asset(
        "zarr",
        pystac.Asset(
            href=zarr_path,
            media_type="application/x-zarr",
            roles=["data", "source"],
            title=f"Canonical Zarr store for {run_id}",
        ),
    )

    return item


def build_stac_item_json(config: StacItemConfig, layout: StorageLayout) -> dict:
    """Build a STAC Item and return it as a JSON-serializable dict."""
    item = build_stac_item(config, layout)
    return item.to_dict()
