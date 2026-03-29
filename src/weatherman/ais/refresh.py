"""AIS refresh orchestration: ingest a day, rebuild snapshot, emit refresh event."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from enum import StrEnum
from pathlib import Path

import duckdb

from weatherman.ais.ingest import load_day
from weatherman.ais.neptune import NeptuneConfig, load_day_from_neptune
from weatherman.ais.snapshot import build_snapshot
from weatherman.events.emissions import emit_ais_refreshed
from weatherman.observability.metrics import AIS_INGEST_TO_VISIBLE_SECONDS

logger = logging.getLogger(__name__)


class AISBackend(StrEnum):
    """Supported AIS ingest backends."""

    LEGACY_PARQUET = "legacy_parquet"
    NEPTUNE = "neptune"


@dataclass(frozen=True)
class AISRefreshResult:
    """Summary of an AIS refresh run."""

    snapshot_date: date
    tenant_id: str
    rows_loaded: int
    vessels_visible: int
    event_emitted: bool


def _finalize_refresh(
    *,
    load_date: date,
    tenant_id: str,
    rows_loaded: int,
    con: duckdb.DuckDBPyConnection,
    emit_event: bool,
) -> AISRefreshResult:
    """Rebuild snapshot, update metrics, and optionally emit SSE."""
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
    return _finalize_refresh(
        load_date=load_date,
        tenant_id=tenant_id,
        rows_loaded=rows_loaded,
        con=con,
        emit_event=emit_event,
    )


def refresh_neptune_day(
    *,
    load_date: date,
    tenant_id: str,
    con: duckdb.DuckDBPyConnection,
    config: NeptuneConfig,
    emit_event: bool = True,
    download: bool = True,
) -> AISRefreshResult:
    """Load a day of AIS data from Neptune, rebuild snapshot, and emit SSE."""
    rows_loaded = load_day_from_neptune(
        load_date=load_date,
        tenant_id=tenant_id,
        con=con,
        config=config,
        download=download,
    )
    return _finalize_refresh(
        load_date=load_date,
        tenant_id=tenant_id,
        rows_loaded=rows_loaded,
        con=con,
        emit_event=emit_event,
    )


def refresh_day_from_backend(
    *,
    backend: AISBackend,
    load_date: date,
    tenant_id: str,
    con: duckdb.DuckDBPyConnection,
    emit_event: bool = True,
    parquet_path: str | Path | None = None,
    neptune_config: NeptuneConfig | None = None,
    download: bool = True,
) -> AISRefreshResult:
    """Dispatch daily AIS refresh to the requested backend."""
    if backend == AISBackend.LEGACY_PARQUET:
        if parquet_path is None:
            raise ValueError("parquet_path is required when backend=legacy_parquet")
        return refresh_day(
            parquet_path,
            load_date=load_date,
            tenant_id=tenant_id,
            con=con,
            emit_event=emit_event,
        )

    if backend == AISBackend.NEPTUNE:
        if neptune_config is None:
            raise ValueError("neptune_config is required when backend=neptune")
        return refresh_neptune_day(
            load_date=load_date,
            tenant_id=tenant_id,
            con=con,
            config=neptune_config,
            emit_event=emit_event,
            download=download,
        )

    raise ValueError(f"Unsupported AIS backend: {backend}")
