#!/usr/bin/env python
"""Refresh one AIS day into DuckDB and rebuild the map snapshot.

Usage:
    uv run python scripts/refresh_ais.py '.data/ais/movement_date=2026-03-08/*' 2026-03-08
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date

from weatherman.ais.db import AISDatabase
from weatherman.ais.refresh import refresh_day

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh one AIS day into DuckDB.")
    parser.add_argument("parquet_path", help="Glob path to the day's Parquet files")
    parser.add_argument("snapshot_date", help="AIS date in YYYY-MM-DD format")
    parser.add_argument("--db-path", default="ais.duckdb", help="DuckDB path")
    parser.add_argument("--tenant-id", default="default", help="Tenant identifier")
    parser.add_argument(
        "--emit-event",
        action="store_true",
        help="Emit ais.refreshed on an in-process SSE event bus",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    snapshot_date = date.fromisoformat(args.snapshot_date)

    db = AISDatabase(args.db_path)
    try:
        con = db.connect()
        result = refresh_day(
            args.parquet_path,
            load_date=snapshot_date,
            tenant_id=args.tenant_id,
            con=con,
            emit_event=args.emit_event,
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
