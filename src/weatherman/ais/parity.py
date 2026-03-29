"""Parity helpers for comparing legacy and Neptune-backed AIS datasets."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any

import duckdb

from weatherman.ais.mvt import GeneratedTile, generate_tile_with_stats
from weatherman.ais.tracks import query_track


@dataclass(frozen=True, slots=True)
class BBoxCheck:
    west: float
    south: float
    east: float
    north: float
    limit: int = 1000


@dataclass(frozen=True, slots=True)
class TileCheck:
    z: int
    x: int
    y: int


@dataclass(frozen=True, slots=True)
class TrackCheck:
    mmsi: int
    start_date: date
    end_date: date


@dataclass(frozen=True, slots=True)
class CheckResult:
    name: str
    legacy: Any
    neptune: Any
    match: bool


@dataclass(frozen=True, slots=True)
class ParityReport:
    snapshot_date: date
    tenant_id: str
    checks: tuple[CheckResult, ...]

    @property
    def ok(self) -> bool:
        return all(check.match for check in self.checks)

    @property
    def mismatches(self) -> tuple[CheckResult, ...]:
        return tuple(check for check in self.checks if not check.match)

    def to_dict(self) -> dict[str, Any]:
        return {
            "snapshot_date": self.snapshot_date.isoformat(),
            "tenant_id": self.tenant_id,
            "ok": self.ok,
            "checks": [asdict(check) for check in self.checks],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, sort_keys=True)


def compare_ais_databases(
    *,
    legacy_con: duckdb.DuckDBPyConnection,
    neptune_con: duckdb.DuckDBPyConnection,
    snapshot_date: date,
    tenant_id: str,
    bbox_checks: tuple[BBoxCheck, ...] = (),
    tile_checks: tuple[TileCheck, ...] = (),
    track_checks: tuple[TrackCheck, ...] = (),
) -> ParityReport:
    checks: list[CheckResult] = [
        _compare_snapshot_summary(
            legacy_con=legacy_con,
            neptune_con=neptune_con,
            snapshot_date=snapshot_date,
            tenant_id=tenant_id,
        )
    ]

    for bbox in bbox_checks:
        checks.append(
            _compare_bbox(
                legacy_con=legacy_con,
                neptune_con=neptune_con,
                snapshot_date=snapshot_date,
                tenant_id=tenant_id,
                bbox=bbox,
            )
        )
    for tile in tile_checks:
        checks.append(
            _compare_tile(
                legacy_con=legacy_con,
                neptune_con=neptune_con,
                snapshot_date=snapshot_date,
                tenant_id=tenant_id,
                tile=tile,
            )
        )
    for track in track_checks:
        checks.append(
            _compare_track(
                legacy_con=legacy_con,
                neptune_con=neptune_con,
                tenant_id=tenant_id,
                track=track,
            )
        )

    return ParityReport(
        snapshot_date=snapshot_date,
        tenant_id=tenant_id,
        checks=tuple(checks),
    )


def compare_ais_db_paths(
    *,
    legacy_db_path: str | Path,
    neptune_db_path: str | Path,
    snapshot_date: date,
    tenant_id: str,
    bbox_checks: tuple[BBoxCheck, ...] = (),
    tile_checks: tuple[TileCheck, ...] = (),
    track_checks: tuple[TrackCheck, ...] = (),
) -> ParityReport:
    legacy_con = duckdb.connect(str(legacy_db_path), read_only=True)
    neptune_con = duckdb.connect(str(neptune_db_path), read_only=True)
    try:
        return compare_ais_databases(
            legacy_con=legacy_con,
            neptune_con=neptune_con,
            snapshot_date=snapshot_date,
            tenant_id=tenant_id,
            bbox_checks=bbox_checks,
            tile_checks=tile_checks,
            track_checks=track_checks,
        )
    finally:
        legacy_con.close()
        neptune_con.close()


def _compare_snapshot_summary(
    *,
    legacy_con: duckdb.DuckDBPyConnection,
    neptune_con: duckdb.DuckDBPyConnection,
    snapshot_date: date,
    tenant_id: str,
) -> CheckResult:
    legacy_summary = _snapshot_summary(
        legacy_con,
        snapshot_date=snapshot_date,
        tenant_id=tenant_id,
    )
    neptune_summary = _snapshot_summary(
        neptune_con,
        snapshot_date=snapshot_date,
        tenant_id=tenant_id,
    )
    return CheckResult(
        name="snapshot.summary",
        legacy=legacy_summary,
        neptune=neptune_summary,
        match=legacy_summary == neptune_summary,
    )


def _snapshot_summary(
    con: duckdb.DuckDBPyConnection,
    *,
    snapshot_date: date,
    tenant_id: str,
) -> dict[str, Any]:
    row = con.execute(
        """
        SELECT
            COUNT(*) AS vessel_count,
            COUNT(DISTINCT mmsi) AS distinct_mmsi,
            MIN("timestamp") AS first_seen,
            MAX("timestamp") AS last_seen
        FROM ais_snapshot
        WHERE "date" = $snapshot_date
          AND tenant_id = $tenant_id
        """,
        {"snapshot_date": snapshot_date, "tenant_id": tenant_id},
    ).fetchone()
    first_seen = row[2].isoformat() if row[2] is not None else None
    last_seen = row[3].isoformat() if row[3] is not None else None
    return {
        "vessel_count": row[0],
        "distinct_mmsi": row[1],
        "first_seen": first_seen,
        "last_seen": last_seen,
    }


def _compare_bbox(
    *,
    legacy_con: duckdb.DuckDBPyConnection,
    neptune_con: duckdb.DuckDBPyConnection,
    snapshot_date: date,
    tenant_id: str,
    bbox: BBoxCheck,
) -> CheckResult:
    legacy_rows = _bbox_rows(
        legacy_con,
        snapshot_date=snapshot_date,
        tenant_id=tenant_id,
        bbox=bbox,
    )
    neptune_rows = _bbox_rows(
        neptune_con,
        snapshot_date=snapshot_date,
        tenant_id=tenant_id,
        bbox=bbox,
    )
    name = (
        "bbox:"
        f"{bbox.west},{bbox.south},{bbox.east},{bbox.north}"
        f":limit={bbox.limit}"
    )
    return CheckResult(name=name, legacy=legacy_rows, neptune=neptune_rows, match=legacy_rows == neptune_rows)


def _bbox_rows(
    con: duckdb.DuckDBPyConnection,
    *,
    snapshot_date: date,
    tenant_id: str,
    bbox: BBoxCheck,
) -> list[dict[str, Any]]:
    rows = con.execute(
        """
        SELECT
            mmsi,
            ROUND(lat, 6) AS lat,
            ROUND(lon, 6) AS lon,
            ROUND(sog, 1) AS sog,
            ROUND(heading, 1) AS heading,
            shiptype,
            destination,
            CAST(eta AS VARCHAR) AS eta
        FROM ais_snapshot
        WHERE "date" = $snapshot_date
          AND tenant_id = $tenant_id
          AND lon BETWEEN $west AND $east
          AND lat BETWEEN $south AND $north
        ORDER BY mmsi
        LIMIT $limit
        """,
        {
            "snapshot_date": snapshot_date,
            "tenant_id": tenant_id,
            "west": bbox.west,
            "south": bbox.south,
            "east": bbox.east,
            "north": bbox.north,
            "limit": bbox.limit,
        },
    ).fetchall()
    return [
        {
            "mmsi": row[0],
            "lat": row[1],
            "lon": row[2],
            "sog": row[3],
            "heading": row[4],
            "shiptype": row[5],
            "destination": row[6],
            "eta": row[7],
        }
        for row in rows
    ]


def _compare_tile(
    *,
    legacy_con: duckdb.DuckDBPyConnection,
    neptune_con: duckdb.DuckDBPyConnection,
    snapshot_date: date,
    tenant_id: str,
    tile: TileCheck,
) -> CheckResult:
    legacy_tile = _tile_summary(
        generate_tile_with_stats(
            con=legacy_con,
            snapshot_date=snapshot_date,
            tenant_id=tenant_id,
            z=tile.z,
            x=tile.x,
            y=tile.y,
        )
    )
    neptune_tile = _tile_summary(
        generate_tile_with_stats(
            con=neptune_con,
            snapshot_date=snapshot_date,
            tenant_id=tenant_id,
            z=tile.z,
            x=tile.x,
            y=tile.y,
        )
    )
    return CheckResult(
        name=f"tile:{tile.z}/{tile.x}/{tile.y}",
        legacy=legacy_tile,
        neptune=neptune_tile,
        match=legacy_tile == neptune_tile,
    )


def _tile_summary(tile: GeneratedTile) -> dict[str, Any]:
    return {
        "feature_count": tile.feature_count,
        "raw_feature_count": tile.raw_feature_count,
        "thinned": tile.thinned,
        "tile_bytes": len(tile.tile_bytes),
        "tile_sha256": hashlib.sha256(tile.tile_bytes).hexdigest(),
    }


def _compare_track(
    *,
    legacy_con: duckdb.DuckDBPyConnection,
    neptune_con: duckdb.DuckDBPyConnection,
    tenant_id: str,
    track: TrackCheck,
) -> CheckResult:
    legacy_track = _track_rows(legacy_con, tenant_id=tenant_id, track=track)
    neptune_track = _track_rows(neptune_con, tenant_id=tenant_id, track=track)
    return CheckResult(
        name=f"track:{track.mmsi}:{track.start_date.isoformat()}:{track.end_date.isoformat()}",
        legacy=legacy_track,
        neptune=neptune_track,
        match=legacy_track == neptune_track,
    )


def _track_rows(
    con: duckdb.DuckDBPyConnection,
    *,
    tenant_id: str,
    track: TrackCheck,
) -> list[dict[str, Any]]:
    points = query_track(
        mmsi=track.mmsi,
        start_date=track.start_date,
        end_date=track.end_date,
        tenant_id=tenant_id,
        con=con,
    )
    return [
        {
            "lat": round(point.lat, 6),
            "lon": round(point.lon, 6),
            "sog": round(point.sog, 1) if point.sog is not None else None,
            "heading": round(point.heading, 1) if point.heading is not None else None,
            "timestamp": point.timestamp.isoformat(),
        }
        for point in points
    ]
