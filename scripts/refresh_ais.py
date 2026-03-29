#!/usr/bin/env python
"""Refresh one AIS day into DuckDB and rebuild the map snapshot.

Usage:
    uv run python scripts/refresh_ais.py '.data/ais/movement_date=2026-03-08/*' 2026-03-08
    uv run python scripts/refresh_ais.py 2026-03-08 --backend neptune
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections.abc import Sequence
from datetime import date
from pathlib import Path

from weatherman.ais.db import AISDatabase
from weatherman.ais.neptune import NeptuneConfig, neptune_config_from_env
from weatherman.ais.refresh import AISBackend, refresh_day_from_backend

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh one AIS day into DuckDB.")
    parser.add_argument(
        "inputs",
        nargs="+",
        help=(
            "For backend=legacy_parquet: parquet_path snapshot_date "
            "(also accepts snapshot_date parquet_path). "
            "For backend=neptune: snapshot_date."
        ),
    )
    parser.add_argument("--db-path", default="ais.duckdb", help="DuckDB path")
    parser.add_argument("--tenant-id", default="default", help="Tenant identifier")
    parser.add_argument(
        "--backend",
        choices=[backend.value for backend in AISBackend],
        default=AISBackend.LEGACY_PARQUET.value,
        help="AIS ingest backend to use",
    )
    parser.add_argument(
        "--emit-event",
        action="store_true",
        help="Emit ais.refreshed on an in-process SSE event bus",
    )
    parser.add_argument(
        "--neptune-store-root",
        help="Neptune cache/store root (defaults to NEPTUNE_STORE_ROOT or .data/neptune)",
    )
    parser.add_argument(
        "--neptune-sources",
        help="Comma-separated Neptune source IDs",
    )
    parser.add_argument(
        "--neptune-merge",
        help="Neptune merge mode (best, union, prefer:<source>)",
    )
    parser.add_argument(
        "--neptune-api-keys-json",
        help="JSON object of Neptune API keys by source ID",
    )
    parser.add_argument(
        "--neptune-bbox",
        help="Optional bbox: west,south,east,north",
    )
    parser.add_argument(
        "--neptune-mmsi",
        help="Optional comma-separated MMSI filter list",
    )
    parser.add_argument(
        "--neptune-overwrite",
        action="store_true",
        help="Force Neptune to re-download raw data even if cached",
    )
    return parser.parse_args(argv)


def _looks_like_iso_date(value: str) -> bool:
    try:
        date.fromisoformat(value)
    except ValueError:
        return False
    return True


def resolve_inputs(
    *,
    backend: AISBackend,
    inputs: Sequence[str],
) -> tuple[date, str | None]:
    if backend == AISBackend.LEGACY_PARQUET:
        if len(inputs) != 2:
            raise ValueError(
                "backend=legacy_parquet requires both parquet_path and snapshot_date"
            )
        first, second = inputs
        if _looks_like_iso_date(first) and not _looks_like_iso_date(second):
            return date.fromisoformat(first), second
        if _looks_like_iso_date(second):
            return date.fromisoformat(second), first
        raise ValueError(
            "backend=legacy_parquet requires one parquet_path and one snapshot_date"
        )

    if len(inputs) != 1:
        raise ValueError("backend=neptune requires exactly one snapshot_date argument")
    return date.fromisoformat(inputs[0]), None


def _build_neptune_config(args: argparse.Namespace) -> NeptuneConfig:
    config = neptune_config_from_env()

    sources = config.sources
    if args.neptune_sources:
        sources = tuple(
            part.strip() for part in args.neptune_sources.split(",") if part.strip()
        )

    api_keys = config.api_keys
    if args.neptune_api_keys_json:
        decoded = json.loads(args.neptune_api_keys_json)
        if not isinstance(decoded, dict):
            raise ValueError("--neptune-api-keys-json must decode to an object")
        api_keys = {str(key): str(value) for key, value in decoded.items()}

    if args.neptune_bbox:
        bbox = tuple(float(part.strip()) for part in args.neptune_bbox.split(","))  # type: ignore[assignment]
        if len(bbox) != 4:
            raise ValueError("--neptune-bbox must have four comma-separated floats")
    else:
        bbox = config.bbox

    if args.neptune_mmsi:
        mmsi = tuple(int(part.strip()) for part in args.neptune_mmsi.split(",") if part.strip())
    else:
        mmsi = config.mmsi

    return NeptuneConfig(
        store_root=Path(args.neptune_store_root) if args.neptune_store_root else config.store_root,
        sources=sources,
        merge=args.neptune_merge or config.merge,
        bbox=bbox,
        mmsi=mmsi,
        api_keys=api_keys,
        raw_policy=config.raw_policy,
        overwrite=args.neptune_overwrite or config.overwrite,
    )


def main() -> None:
    args = parse_args()
    backend = AISBackend(args.backend)
    snapshot_date, parquet_path = resolve_inputs(backend=backend, inputs=args.inputs)

    db = AISDatabase(args.db_path)
    try:
        con = db.connect()
        result = refresh_day_from_backend(
            backend=backend,
            load_date=snapshot_date,
            tenant_id=args.tenant_id,
            con=con,
            emit_event=args.emit_event,
            parquet_path=parquet_path,
            neptune_config=_build_neptune_config(args) if backend == AISBackend.NEPTUNE else None,
        )
    except Exception as exc:
        print(f"AIS refresh failed: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        db.close()

    print(
        "AIS refresh complete: "
        f"date={result.snapshot_date} rows={result.rows_loaded} "
        f"vessels={result.vessels_visible} emitted={result.event_emitted}"
    )


if __name__ == "__main__":
    main()
