"""Neptune-backed AIS ingest and live refresh helpers.

This module bridges Neptune's canonical AIS positions dataset into
Weatherman's existing ``ais_positions`` DuckDB schema so the current
snapshot, tile, and track APIs can remain unchanged.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import json
import logging
import os
from importlib import import_module
from dataclasses import dataclass, replace
from datetime import date
from pathlib import Path

import duckdb

from weatherman.ais.ingest import load_day_from_select

logger = logging.getLogger(__name__)


DEFAULT_NEPTUNE_SOURCES = ("noaa",)
DEFAULT_NEPTUNE_STORE_ROOT = Path(".data/neptune")
DEFAULT_NEPTUNE_LANDING_DIR = Path(".data/neptune-live")


@dataclass(frozen=True)
class NeptuneConfig:
    """Configuration for archival Neptune access."""

    store_root: Path = DEFAULT_NEPTUNE_STORE_ROOT
    sources: tuple[str, ...] = DEFAULT_NEPTUNE_SOURCES
    merge: str = "best"
    bbox: tuple[float, float, float, float] | None = None
    mmsi: tuple[int, ...] | None = None
    api_keys: dict[str, str] | None = None
    raw_policy: str = "metadata"
    overwrite: bool = False


@dataclass(frozen=True)
class NeptuneLiveConfig:
    """Configuration for Neptune streaming ingestion."""

    source: str = "aisstream"
    landing_dir: Path = DEFAULT_NEPTUNE_LANDING_DIR
    api_key: str = ""
    bbox: tuple[float, float, float, float] | None = None
    mmsi: tuple[int, ...] | None = None
    max_messages: int | None = None
    cleanup: bool = False
    flush_interval_s: int = 60


@dataclass(frozen=True)
class NeptuneLiveResult:
    """Summary of a Neptune live ingest cycle."""

    source: str
    dates_refreshed: tuple[date, ...]
    records_promoted: int
    shard_files: int


def neptune_config_from_env() -> NeptuneConfig:
    """Build a Neptune archival config from environment variables."""
    sources_raw = os.environ.get("NEPTUNE_SOURCES", ",".join(DEFAULT_NEPTUNE_SOURCES))
    sources = tuple(part.strip() for part in sources_raw.split(",") if part.strip())
    if not sources:
        sources = DEFAULT_NEPTUNE_SOURCES

    return NeptuneConfig(
        store_root=Path(os.environ.get("NEPTUNE_STORE_ROOT", str(DEFAULT_NEPTUNE_STORE_ROOT))),
        sources=sources,
        merge=os.environ.get("NEPTUNE_MERGE", "best"),
        bbox=_parse_bbox(os.environ.get("NEPTUNE_BBOX")),
        mmsi=_parse_mmsi(os.environ.get("NEPTUNE_MMSI")),
        api_keys=_parse_api_keys(sources, os.environ.get("NEPTUNE_API_KEYS_JSON")),
        raw_policy=os.environ.get("NEPTUNE_RAW_POLICY", "metadata"),
        overwrite=_parse_bool(os.environ.get("NEPTUNE_OVERWRITE")),
    )


def live_config_from_env() -> NeptuneLiveConfig:
    """Build a Neptune live ingest config from environment variables."""
    return NeptuneLiveConfig(
        source=os.environ.get("NEPTUNE_LIVE_SOURCE", "aisstream"),
        landing_dir=Path(
            os.environ.get("NEPTUNE_LIVE_LANDING_DIR", str(DEFAULT_NEPTUNE_LANDING_DIR))
        ),
        api_key=os.environ.get("NEPTUNE_LIVE_API_KEY", ""),
        bbox=_parse_bbox(os.environ.get("NEPTUNE_LIVE_BBOX")),
        mmsi=_parse_mmsi(os.environ.get("NEPTUNE_LIVE_MMSI")),
        max_messages=_parse_optional_int(os.environ.get("NEPTUNE_LIVE_MAX_MESSAGES")),
        cleanup=_parse_bool(os.environ.get("NEPTUNE_LIVE_CLEANUP")),
        flush_interval_s=_parse_optional_int(os.environ.get("NEPTUNE_LIVE_FLUSH_INTERVAL")) or 60,
    )


def load_day_from_neptune(
    *,
    load_date: date,
    tenant_id: str,
    con: duckdb.DuckDBPyConnection,
    config: NeptuneConfig,
    download: bool = True,
) -> int:
    """Load one AIS day from Neptune canonical positions into ``ais_positions``."""
    Neptune = _import_neptune_archive()
    neptune = Neptune(
        load_date.isoformat(),
        sources=list(config.sources),
        merge=config.merge,
        bbox=config.bbox,
        mmsi=list(config.mmsi) if config.mmsi else None,
        cache_dir=config.store_root,
        raw_policy=config.raw_policy,
        overwrite=config.overwrite,
        api_keys=config.api_keys or None,
    )
    if download:
        neptune.download()

    positions = neptune.positions().collect()
    position_columns = set(getattr(positions, "columns", []))
    if "timestamp" not in position_columns:
        raise ValueError("Neptune positions data is missing required 'timestamp' column")
    if "mmsi" not in position_columns:
        raise ValueError("Neptune positions data is missing required 'mmsi' column")

    relation_name = f"neptune_positions_{load_date:%Y%m%d}"
    con.register(relation_name, positions)
    try:
        column_types = _relation_column_types(con, relation_name)
        return load_day_from_select(
            _neptune_select_sql(relation_name, column_types),
            load_date=load_date,
            tenant_id=tenant_id,
            con=con,
            params={"load_date": load_date, "tenant_id": tenant_id},
            log_context={
                "backend": "neptune",
                "neptune_store_root": str(config.store_root),
                "neptune_sources": list(config.sources),
                "neptune_merge": config.merge,
                "download": download,
            },
        )
    finally:
        try:
            con.unregister(relation_name)
        except Exception:
            logger.debug("DuckDB relation %s could not be unregistered cleanly", relation_name)


def run_neptune_live_ingest(
    *,
    live_config: NeptuneLiveConfig,
    archival_config: NeptuneConfig,
    db_path: str | Path,
    tenant_id: str,
    emit_event: bool = True,
) -> NeptuneLiveResult:
    """Run one Neptune live ingest cycle and bridge promoted dates into DuckDB."""
    NeptuneStream, StreamConfig, ParquetSink, promote_landing, run_with_reconnect = (
        _import_neptune_streaming()
    )
    connect_and_stream = _import_neptune_stream_source(live_config.source)
    from weatherman.ais.db import AISDatabase
    from weatherman.ais.refresh import refresh_neptune_day

    refreshed_dates: set[date] = set()
    records_promoted = 0
    shard_files = 0
    db = AISDatabase(db_path)
    try:
        con = db.connect()

        def _promote_and_refresh() -> None:
            nonlocal records_promoted, shard_files
            promotions = promote_landing(
                live_config.landing_dir,
                archival_config.store_root,
                live_config.source,
                cleanup=live_config.cleanup,
            )
            for result in promotions:
                refresh_date = date.fromisoformat(result.date)
                refreshed_dates.add(refresh_date)
                records_promoted += result.record_count
                shard_files += len(result.shard_files)
                refresh_neptune_day(
                    load_date=refresh_date,
                    tenant_id=tenant_id,
                    con=con,
                    config=replace(
                        archival_config,
                        sources=(live_config.source,),
                    ),
                    emit_event=emit_event,
                    download=False,
                )

        async def _consume() -> None:
            sink = ParquetSink(live_config.landing_dir, source=live_config.source)
            config = StreamConfig(
                source=live_config.source,
                api_key=live_config.api_key,
                bbox=live_config.bbox,
                mmsi=list(live_config.mmsi) if live_config.mmsi else None,
                flush_interval_s=live_config.flush_interval_s,
            )

            async with NeptuneStream(config=config) as stream:
                producer_task = asyncio.create_task(
                    run_with_reconnect(
                        stream,
                        _build_stream_connect_fn(
                            connect_and_stream=connect_and_stream,
                            stream=stream,
                            live_config=live_config,
                        ),
                    )
                )
                batch: list[dict[str, object]] = []
                batch_size = 100
                loop = asyncio.get_running_loop()
                last_promotion_at = loop.time()
                stream_iter = stream.__aiter__()

                try:
                    while True:
                        timeout = max(
                            0.0,
                            float(live_config.flush_interval_s) - (loop.time() - last_promotion_at),
                        )
                        try:
                            message = await asyncio.wait_for(
                                stream_iter.__anext__(),
                                timeout=timeout,
                            )
                        except asyncio.TimeoutError:
                            if batch:
                                await sink.write(batch)
                                batch = []
                            await sink.flush()
                            _promote_and_refresh()
                            last_promotion_at = loop.time()
                            if producer_task.done():
                                producer_task.result()
                                if getattr(stream, "_message_queue").empty():
                                    break
                            continue
                        except StopAsyncIteration:
                            break

                        batch.append(message)
                        if len(batch) >= batch_size:
                            await sink.write(batch)
                            batch = []

                        if (
                            live_config.max_messages is not None
                            and stream.stats.messages_delivered >= live_config.max_messages
                        ):
                            break
                finally:
                    if batch:
                        await sink.write(batch)
                    await sink.flush()
                    _promote_and_refresh()

                if producer_task.done():
                    producer_task.result()
                else:
                    producer_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await producer_task

        asyncio.run(_consume())
    finally:
        db.close()

    return NeptuneLiveResult(
        source=live_config.source,
        dates_refreshed=tuple(sorted(refreshed_dates)),
        records_promoted=records_promoted,
        shard_files=shard_files,
    )


def _import_neptune_archive():
    try:
        from neptune_ais import Neptune
    except ImportError as exc:
        raise RuntimeError(
            "AIS backend 'neptune' requires the optional dependency "
            "'neptune-ais[sql,parquet,stream]'."
        ) from exc
    return Neptune


def _import_neptune_streaming():
    try:
        from neptune_ais.sinks import ParquetSink, promote_landing
        from neptune_ais.stream import NeptuneStream, StreamConfig, run_with_reconnect
    except ImportError as exc:
        raise RuntimeError(
            "Neptune live ingest requires the optional dependency "
            "'neptune-ais[sql,parquet,stream]'."
        ) from exc
    return NeptuneStream, StreamConfig, ParquetSink, promote_landing, run_with_reconnect


def _import_neptune_stream_source(source: str):
    try:
        from neptune_ais.adapters.registry import load_all_adapters

        load_all_adapters()
        module = import_module(f"neptune_ais.adapters.{source}")
        return getattr(module, "connect_and_stream")
    except (ImportError, AttributeError, ModuleNotFoundError) as exc:
        raise RuntimeError(
            f"Neptune live ingest source {source!r} is unavailable or does not "
            "expose connect_and_stream()."
        ) from exc


def _build_stream_connect_fn(*, connect_and_stream, stream, live_config: NeptuneLiveConfig):
    params = inspect.signature(connect_and_stream).parameters
    kwargs: dict[str, object] = {}
    if "api_key" in params:
        kwargs["api_key"] = live_config.api_key
    if "bbox" in params:
        kwargs["bbox"] = live_config.bbox
    if "mmsi" in params and live_config.mmsi:
        kwargs["mmsi"] = list(live_config.mmsi)

    async def _connect() -> None:
        await connect_and_stream(stream, **kwargs)

    return _connect


def _neptune_select_sql(relation_name: str, column_types: dict[str, str]) -> str:
    columns = set(column_types)
    timestamp, timestamp_date = _timestamp_expr(column_types, "timestamp")
    source = _column_expr(columns, "source", "VARCHAR")
    source_record_id = _column_expr(columns, "source_record_id", "VARCHAR")
    ship_type = _column_expr(columns, "ship_type", "VARCHAR")
    imo = _column_expr(columns, "imo", "VARCHAR")
    destination = _column_expr(columns, "destination", "VARCHAR")

    shiptype = f"""(
        CASE
            WHEN {ship_type} IS NULL OR trim({ship_type}) = '' THEN NULL
            WHEN TRY_CAST({ship_type} AS INTEGER) BETWEEN 70 AND 79 THEN 'Cargo'
            WHEN TRY_CAST({ship_type} AS INTEGER) BETWEEN 80 AND 89 THEN 'Tanker'
            WHEN regexp_matches(lower({ship_type}), '(bulk|cargo|container)') THEN 'Cargo'
            WHEN regexp_matches(lower({ship_type}), '(tanker|oil|chemical|lpg|lng)') THEN 'Tanker'
            ELSE {ship_type}
        END
    )"""
    imommsi = f"""(
        CASE
            WHEN {imo} IS NOT NULL AND trim({imo}) <> ''
                THEN concat({imo}, '-', CAST(mmsi AS VARCHAR))
            ELSE CAST(mmsi AS VARCHAR)
        END
    )"""
    movementid = f"""(
        COALESCE(
            NULLIF({source_record_id}, ''),
            md5(
                concat_ws(
                    '|',
                    COALESCE({source}, ''),
                    CAST(mmsi AS VARCHAR),
                    strftime({timestamp}, '%Y-%m-%dT%H:%M:%S.%f'),
                    COALESCE(CAST(lat AS VARCHAR), ''),
                    COALESCE(CAST(lon AS VARCHAR), '')
                )
            )
        )
    )"""

    return f"""SELECT
        {imo}                                              AS imo,
        CAST(mmsi AS BIGINT)                               AS mmsi,
        {imommsi}                                          AS imommsi,
        CAST(NULL AS VARCHAR)                              AS lrimoshipno,
        {movementid}                                       AS movementid,
        {_column_expr(columns, "vessel_name", "VARCHAR")}  AS vessel_name,
        {shiptype}                                         AS shiptype,
        CAST(NULL AS VARCHAR)                              AS vessel_class,
        CAST(NULL AS BIGINT)                               AS dwt,
        {_column_expr(columns, "callsign", "VARCHAR")}     AS callsign,
        {_column_expr(columns, "beam", "DOUBLE")}          AS beam,
        {_column_expr(columns, "length", "DOUBLE")}        AS length,
        {timestamp}                                        AS "timestamp",
        {timestamp_date}                                   AS "date",
        {_column_expr(columns, "lat", "DOUBLE")}           AS lat,
        {_column_expr(columns, "lon", "DOUBLE")}           AS lon,
        {_column_expr(columns, "sog", "DOUBLE")}           AS sog,
        {_column_expr(columns, "heading", "DOUBLE")}       AS heading,
        {_column_expr(columns, "draught", "DOUBLE")}       AS draught,
        CAST(NULL AS DOUBLE)                               AS max_draught,
        {_column_expr(columns, "nav_status", "VARCHAR")}   AS movestatus,
        {destination}                                      AS destination,
        {destination}                                      AS destinationtidied,
        {_column_expr(columns, "eta", "TIMESTAMP")}        AS eta,
        CAST(NULL AS VARCHAR)                              AS additionalinfo,
        $tenant_id                                         AS tenant_id
    FROM {relation_name}
    WHERE {timestamp_date} = $load_date"""


def _relation_column_types(
    con: duckdb.DuckDBPyConnection,
    relation_name: str,
) -> dict[str, str]:
    rows = con.execute(f"DESCRIBE {relation_name}").fetchall()
    return {str(name): str(data_type) for name, data_type, *_ in rows}


def _timestamp_expr(
    column_types: dict[str, str],
    name: str,
) -> tuple[str, str]:
    if name not in column_types:
        null_timestamp = "CAST(NULL AS TIMESTAMP)"
        return null_timestamp, "CAST(NULL AS DATE)"

    column = _quote_identifier(name)
    column_type = column_types[name].upper()

    if "TIMESTAMP WITH TIME ZONE" in column_type:
        timestamp = f"CAST({column} AT TIME ZONE 'UTC' AS TIMESTAMP)"
        return timestamp, f"CAST({column} AT TIME ZONE 'UTC' AS DATE)"

    if column_type == "VARCHAR":
        has_utc_offset = f"regexp_matches({column}, '(Z|[+-][0-9]{{2}}:[0-9]{{2}})$')"
        timestamp = f"""(
            CASE
                WHEN {has_utc_offset}
                    THEN CAST(TRY_CAST({column} AS TIMESTAMPTZ) AT TIME ZONE 'UTC' AS TIMESTAMP)
                ELSE CAST({column} AS TIMESTAMP)
            END
        )"""
        timestamp_date = f"""(
            CASE
                WHEN {has_utc_offset}
                    THEN CAST(TRY_CAST({column} AS TIMESTAMPTZ) AT TIME ZONE 'UTC' AS DATE)
                ELSE CAST(CAST({column} AS TIMESTAMP) AS DATE)
            END
        )"""
        return timestamp, timestamp_date

    timestamp = f"CAST({column} AS TIMESTAMP)"
    return timestamp, f"CAST({timestamp} AS DATE)"


def _column_expr(columns: set[str], name: str, sql_type: str) -> str:
    if name in columns:
        return f"CAST({_quote_identifier(name)} AS {sql_type})"
    return f"CAST(NULL AS {sql_type})"


def _quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _parse_bbox(raw: str | None) -> tuple[float, float, float, float] | None:
    if not raw:
        return None
    parts = [part.strip() for part in raw.split(",") if part.strip()]
    if len(parts) != 4:
        raise ValueError("BBox must be four comma-separated floats: west,south,east,north")
    west, south, east, north = (float(part) for part in parts)
    return west, south, east, north


def _parse_mmsi(raw: str | None) -> tuple[int, ...] | None:
    if not raw:
        return None
    values = tuple(int(part.strip()) for part in raw.split(",") if part.strip())
    return values or None


def _parse_api_keys(
    sources: tuple[str, ...],
    raw_json: str | None,
) -> dict[str, str] | None:
    api_keys: dict[str, str] = {}
    if raw_json:
        decoded = json.loads(raw_json)
        if not isinstance(decoded, dict):
            raise ValueError("NEPTUNE_API_KEYS_JSON must decode to an object")
        api_keys.update({str(key): str(value) for key, value in decoded.items()})

    for source in sources:
        env_key = f"NEPTUNE_{source.upper()}_API_KEY"
        if env_key in os.environ:
            api_keys[source] = os.environ[env_key]

    return api_keys or None


def _parse_bool(raw: str | None) -> bool:
    if raw is None:
        return False
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _parse_optional_int(raw: str | None) -> int | None:
    if raw is None or not raw.strip():
        return None
    return int(raw)
