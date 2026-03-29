#!/usr/bin/env python
"""Run Neptune live AIS ingest and bridge promoted days into Weatherman.

Usage:
    uv run python scripts/stream_ais_neptune.py --source aisstream --db-path ais.duckdb
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

from weatherman.ais.neptune import (
    NeptuneConfig,
    NeptuneLiveConfig,
    live_config_from_env,
    neptune_config_from_env,
    run_neptune_live_ingest,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Neptune live AIS ingest.")
    parser.add_argument(
        "--db-path",
        default=os.environ.get("AIS_DB_PATH", "ais.duckdb"),
        help="DuckDB path (defaults to AIS_DB_PATH or ais.duckdb)",
    )
    parser.add_argument(
        "--tenant-id",
        default=os.environ.get("AIS_TENANT_ID", "default"),
        help="Tenant identifier (defaults to AIS_TENANT_ID or default)",
    )
    parser.add_argument("--source", help="Neptune live source ID (default: aisstream)")
    parser.add_argument("--landing-dir", help="Landing directory for live Parquet batches")
    parser.add_argument("--api-key", help="Live source API key")
    parser.add_argument("--bbox", help="Optional bbox: west,south,east,north")
    parser.add_argument("--mmsi", help="Optional comma-separated MMSI filter list")
    parser.add_argument("--max-messages", type=int, help="Stop after this many messages")
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Delete landing files after successful promotion",
    )
    parser.add_argument(
        "--flush-interval-s",
        type=int,
        help="Live sink flush interval in seconds",
    )
    parser.add_argument(
        "--emit-event",
        action="store_true",
        help="Emit ais.refreshed after promoted days are refreshed",
    )
    parser.add_argument("--neptune-store-root", help="Neptune canonical store root")
    parser.add_argument("--neptune-merge", help="Neptune merge mode")
    parser.add_argument("--neptune-api-keys-json", help="Archival Neptune API keys JSON")
    parser.add_argument("--neptune-bbox", help="Archival bbox override")
    parser.add_argument("--neptune-sources", help="Archival sources override")
    parser.add_argument(
        "--neptune-overwrite",
        action="store_true",
        help="Force archival Neptune downloads when refreshes run",
    )
    return parser.parse_args()


def _parse_bbox(raw: str | None):
    if not raw:
        return None
    parts = tuple(float(part.strip()) for part in raw.split(",") if part.strip())
    if len(parts) != 4:
        raise ValueError("BBox must be four comma-separated floats")
    return parts


def _parse_mmsi(raw: str | None):
    if not raw:
        return None
    values = tuple(int(part.strip()) for part in raw.split(",") if part.strip())
    return values or None


def _archival_config(args: argparse.Namespace) -> NeptuneConfig:
    config = neptune_config_from_env()
    api_keys = config.api_keys
    if args.neptune_api_keys_json:
        decoded = json.loads(args.neptune_api_keys_json)
        if not isinstance(decoded, dict):
            raise ValueError("--neptune-api-keys-json must decode to an object")
        api_keys = {str(key): str(value) for key, value in decoded.items()}

    sources = config.sources
    if args.neptune_sources:
        sources = tuple(part.strip() for part in args.neptune_sources.split(",") if part.strip())

    return NeptuneConfig(
        store_root=Path(args.neptune_store_root) if args.neptune_store_root else config.store_root,
        sources=sources,
        merge=args.neptune_merge or config.merge,
        bbox=_parse_bbox(args.neptune_bbox) or config.bbox,
        mmsi=config.mmsi,
        api_keys=api_keys,
        raw_policy=config.raw_policy,
        overwrite=args.neptune_overwrite or config.overwrite,
    )


def _live_config(args: argparse.Namespace) -> NeptuneLiveConfig:
    config = live_config_from_env()
    return NeptuneLiveConfig(
        source=args.source or config.source,
        landing_dir=Path(args.landing_dir) if args.landing_dir else config.landing_dir,
        api_key=args.api_key if args.api_key is not None else config.api_key,
        bbox=_parse_bbox(args.bbox) or config.bbox,
        mmsi=_parse_mmsi(args.mmsi) or config.mmsi,
        max_messages=args.max_messages if args.max_messages is not None else config.max_messages,
        cleanup=args.cleanup or config.cleanup,
        flush_interval_s=(
            args.flush_interval_s if args.flush_interval_s is not None else config.flush_interval_s
        ),
    )


def main() -> None:
    args = parse_args()
    try:
        result = run_neptune_live_ingest(
            live_config=_live_config(args),
            archival_config=_archival_config(args),
            db_path=args.db_path,
            tenant_id=args.tenant_id,
            emit_event=args.emit_event,
        )
    except Exception as exc:
        print(f"Neptune live ingest failed: {exc}", file=sys.stderr)
        sys.exit(1)

    refreshed = ",".join(d.isoformat() for d in result.dates_refreshed) or "-"
    print(
        "Neptune live ingest complete: "
        f"source={result.source} dates={refreshed} "
        f"records={result.records_promoted} shards={result.shard_files}"
    )


if __name__ == "__main__":
    main()
