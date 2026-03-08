"""AIS refresh orchestration: ingest a day, rebuild snapshot, emit refresh event."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from pathlib import Path

import duckdb

from weatherman.ais.ingest import load_day
from weatherman.ais.snapshot import build_snapshot
from weatherman.events.emissions import emit_ais_refreshed
from weatherman.observability.metrics import AIS_INGEST_TO_VISIBLE_SECONDS

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AISRefreshResult:
    """Summary of an AIS refresh run."""

    snapshot_date: date
    tenant_id: str
    rows_loaded: int
    vessels_visible: int
    event_emitted: bool


def refresh_day(
    parquet_path: str | Path,
    *,
    load_date: date,
    tenant_id: str,
    con: duckdb.DuckDBPyConnection,
    emit_event: bool = True,
) -> AISRefreshResult:
    """Load a day of AIS data, rebuild the latest-position snapshot, and emit SSE."""
    rows_loaded = load_day(
        parquet_path,
        load_date=load_date,
        tenant_id=tenant_id,
        con=con,
    )
    vessels_visible = build_snapshot(
        snapshot_date=load_date,
        tenant_id=tenant_id,
        con=con,
    )

    lag_seconds = (
        datetime.now(timezone.utc)
        - datetime.combine(load_date, time.min, tzinfo=timezone.utc)
    ).total_seconds()
    AIS_INGEST_TO_VISIBLE_SECONDS.labels(tenant_id=tenant_id).set(lag_seconds)

    event_emitted = False
    if emit_event:
        emit_ais_refreshed(
            ais_date=load_date,
            tile_url_template=f"/ais/tiles/{load_date}/{{z}}/{{x}}/{{y}}.pbf",
        )
        event_emitted = True

    logger.info(
        "AIS refresh complete",
        extra={
            "load_date": str(load_date),
            "tenant_id": tenant_id,
            "rows_loaded": rows_loaded,
            "vessels_visible": vessels_visible,
            "event_emitted": event_emitted,
        },
    )
    return AISRefreshResult(
        snapshot_date=load_date,
        tenant_id=tenant_id,
        rows_loaded=rows_loaded,
        vessels_visible=vessels_visible,
        event_emitted=event_emitted,
    )
