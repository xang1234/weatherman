#!/usr/bin/env python
"""Compare legacy and Neptune AIS outputs for a given snapshot date.

Usage:
    uv run python scripts/compare_ais_parity.py \
      --legacy-db-path .data/legacy-ais.duckdb \
      --neptune-db-path .data/ais.duckdb \
      --snapshot-date 2026-03-08 \
      --bbox 100,0,110,10 \
      --track 211234567,2026-03-07,2026-03-08 \
      --tile 4,14,9
"""

from __future__ import annotations

import argparse
from datetime import date

from weatherman.ais.parity import (
    BBoxCheck,
    TileCheck,
    TrackCheck,
    compare_ais_db_paths,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare legacy and Neptune AIS outputs.")
    parser.add_argument("--legacy-db-path", required=True, help="Legacy DuckDB path")
    parser.add_argument("--neptune-db-path", required=True, help="Neptune DuckDB path")
    parser.add_argument("--snapshot-date", required=True, help="Snapshot date (YYYY-MM-DD)")
    parser.add_argument("--tenant-id", default="default", help="Tenant ID to compare")
    parser.add_argument(
        "--bbox",
        action="append",
        default=[],
        help="BBox check in west,south,east,north form; may be repeated",
    )
    parser.add_argument(
        "--tile",
        action="append",
        default=[],
        help="Tile check in z,x,y form; may be repeated",
    )
    parser.add_argument(
        "--track",
        action="append",
        default=[],
        help="Track check in mmsi,start_date,end_date form; may be repeated",
    )
    parser.add_argument(
        "--output",
        choices=("text", "json"),
        default="text",
        help="Output format",
    )
    return parser.parse_args()


def _parse_bbox(raw: str) -> BBoxCheck:
    west, south, east, north = (float(part.strip()) for part in raw.split(","))
    return BBoxCheck(west=west, south=south, east=east, north=north)


def _parse_tile(raw: str) -> TileCheck:
    z, x, y = (int(part.strip()) for part in raw.split(","))
    return TileCheck(z=z, x=x, y=y)


def _parse_track(raw: str) -> TrackCheck:
    mmsi, start_date, end_date = (part.strip() for part in raw.split(","))
    return TrackCheck(
        mmsi=int(mmsi),
        start_date=date.fromisoformat(start_date),
        end_date=date.fromisoformat(end_date),
    )


def main() -> int:
    args = parse_args()
    report = compare_ais_db_paths(
        legacy_db_path=args.legacy_db_path,
        neptune_db_path=args.neptune_db_path,
        snapshot_date=date.fromisoformat(args.snapshot_date),
        tenant_id=args.tenant_id,
        bbox_checks=tuple(_parse_bbox(raw) for raw in args.bbox),
        tile_checks=tuple(_parse_tile(raw) for raw in args.tile),
        track_checks=tuple(_parse_track(raw) for raw in args.track),
    )
    if args.output == "json":
        print(report.to_json())
    else:
        print(f"snapshot_date={report.snapshot_date.isoformat()} tenant_id={report.tenant_id}")
        print(f"status={'ok' if report.ok else 'mismatch'}")
        for check in report.checks:
            prefix = "OK" if check.match else "DIFF"
            print(f"{prefix} {check.name}")
            if not check.match:
                print(f"  legacy={check.legacy}")
                print(f"  neptune={check.neptune}")
    return 0 if report.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
