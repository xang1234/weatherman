"""Tests for weatherman.ais.tracks — time-ordered vessel track queries."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pytest

from weatherman.ais.tracks import TrackPoint, query_track
from weatherman.ais.ingest import load_day
from tests.conftest_ais import (
    _write_test_parquet,
    ROW_BULK_CARRIER,
    ROW_GRAIN_STAR,
    ais_db,
    ais_con,
    parquet_dir,
)


class TestQueryTrack:
    """Basic track query tests."""

    def test_returns_positions_for_vessel(
        self, parquet_dir: Path, ais_con: duckdb.DuckDBPyConnection
    ) -> None:
        load_day(
            f"{parquet_dir}/movement_date=2025-12-25/*",
            load_date=date(2025, 12, 25),
            tenant_id="default",
            con=ais_con,
        )
        track = query_track(
            mmsi=211234567,
            start_date=date(2025, 12, 25),
            end_date=date(2025, 12, 25),
            tenant_id="default",
            con=ais_con,
        )
        assert len(track) == 1
        assert isinstance(track[0], TrackPoint)
        assert track[0].lat == 1.35
        assert track[0].lon == 103.8
        assert track[0].sog == 12.5
        assert track[0].heading == 245.0

    def test_filters_by_mmsi(
        self, parquet_dir: Path, ais_con: duckdb.DuckDBPyConnection
    ) -> None:
        """Only returns positions for the requested vessel."""
        load_day(
            f"{parquet_dir}/movement_date=2025-12-25/*",
            load_date=date(2025, 12, 25),
            tenant_id="default",
            con=ais_con,
        )
        track = query_track(
            mmsi=211234567,
            start_date=date(2025, 12, 25),
            end_date=date(2025, 12, 25),
            tenant_id="default",
            con=ais_con,
        )
        assert len(track) == 1
        # Other vessel (MMSI 311234567) should not appear
        other_track = query_track(
            mmsi=311234567,
            start_date=date(2025, 12, 25),
            end_date=date(2025, 12, 25),
            tenant_id="default",
            con=ais_con,
        )
        assert len(other_track) == 1
        assert other_track[0].lat == 51.5  # MV GRAIN STAR


class TestTimeOrdering:
    """Track positions must be ordered by timestamp ascending."""

    def test_ascending_timestamp_order(
        self, tmp_path: Path, ais_con: duckdb.DuckDBPyConnection
    ) -> None:
        early = ROW_BULK_CARRIER.replace(
            "TIMESTAMP '2025-12-25 12:00:00'",
            "TIMESTAMP '2025-12-25 06:00:00'",
        ).replace(
            "'mov-001'               AS movementid",
            "'mov-010'               AS movementid",
        )
        mid = ROW_BULK_CARRIER.replace(
            "'mov-001'               AS movementid",
            "'mov-011'               AS movementid",
        )
        late = ROW_BULK_CARRIER.replace(
            "TIMESTAMP '2025-12-25 12:00:00'",
            "TIMESTAMP '2025-12-25 18:00:00'",
        ).replace(
            "'mov-001'               AS movementid",
            "'mov-012'               AS movementid",
        )

        _write_test_parquet(
            tmp_path,
            "movement_date=2025-12-25",
            f"{late} UNION ALL {early} UNION ALL {mid}",
        )
        load_day(
            f"{tmp_path}/movement_date=2025-12-25/*",
            load_date=date(2025, 12, 25),
            tenant_id="default",
            con=ais_con,
        )

        track = query_track(
            mmsi=211234567,
            start_date=date(2025, 12, 25),
            end_date=date(2025, 12, 25),
            tenant_id="default",
            con=ais_con,
        )
        assert len(track) == 3
        assert track[0].timestamp.hour == 6
        assert track[1].timestamp.hour == 12
        assert track[2].timestamp.hour == 18


class TestDateRange:
    """Track query respects date range boundaries."""

    def test_multi_day_track(
        self, tmp_path: Path, ais_con: duckdb.DuckDBPyConnection
    ) -> None:
        """Query spanning multiple days returns positions from all days."""
        _write_test_parquet(
            tmp_path, "movement_date=2025-12-25", ROW_BULK_CARRIER
        )
        day2 = ROW_BULK_CARRIER.replace(
            "TIMESTAMP '2025-12-25 12:00:00'",
            "TIMESTAMP '2025-12-26 08:00:00'",
        ).replace(
            "'mov-001'               AS movementid",
            "'mov-020'               AS movementid",
        )
        _write_test_parquet(tmp_path, "movement_date=2025-12-26", day2)

        for d in (date(2025, 12, 25), date(2025, 12, 26)):
            load_day(
                f"{tmp_path}/movement_date={d}/*",
                load_date=d,
                tenant_id="default",
                con=ais_con,
            )

        track = query_track(
            mmsi=211234567,
            start_date=date(2025, 12, 25),
            end_date=date(2025, 12, 26),
            tenant_id="default",
            con=ais_con,
        )
        assert len(track) == 2
        assert track[0].timestamp.day == 25
        assert track[1].timestamp.day == 26

    def test_date_range_excludes_outside(
        self, tmp_path: Path, ais_con: duckdb.DuckDBPyConnection
    ) -> None:
        """Positions outside the date range are not returned."""
        _write_test_parquet(
            tmp_path, "movement_date=2025-12-25", ROW_BULK_CARRIER
        )
        load_day(
            f"{tmp_path}/movement_date=2025-12-25/*",
            load_date=date(2025, 12, 25),
            tenant_id="default",
            con=ais_con,
        )

        track = query_track(
            mmsi=211234567,
            start_date=date(2025, 12, 26),
            end_date=date(2025, 12, 27),
            tenant_id="default",
            con=ais_con,
        )
        assert len(track) == 0


class TestTenantIsolation:
    """Track query only returns data for the specified tenant."""

    def test_filters_by_tenant(
        self, parquet_dir: Path, ais_con: duckdb.DuckDBPyConnection
    ) -> None:
        load_day(
            f"{parquet_dir}/movement_date=2025-12-25/*",
            load_date=date(2025, 12, 25),
            tenant_id="tenant-a",
            con=ais_con,
        )
        load_day(
            f"{parquet_dir}/movement_date=2025-12-25/*",
            load_date=date(2025, 12, 25),
            tenant_id="tenant-b",
            con=ais_con,
        )

        track_a = query_track(
            mmsi=211234567,
            start_date=date(2025, 12, 25),
            end_date=date(2025, 12, 25),
            tenant_id="tenant-a",
            con=ais_con,
        )
        track_b = query_track(
            mmsi=211234567,
            start_date=date(2025, 12, 25),
            end_date=date(2025, 12, 25),
            tenant_id="tenant-b",
            con=ais_con,
        )
        assert len(track_a) == 1
        assert len(track_b) == 1


class TestEmptyResults:
    """Edge cases that return empty tracks."""

    def test_nonexistent_vessel(
        self, parquet_dir: Path, ais_con: duckdb.DuckDBPyConnection
    ) -> None:
        load_day(
            f"{parquet_dir}/movement_date=2025-12-25/*",
            load_date=date(2025, 12, 25),
            tenant_id="default",
            con=ais_con,
        )
        track = query_track(
            mmsi=999999999,
            start_date=date(2025, 12, 25),
            end_date=date(2025, 12, 25),
            tenant_id="default",
            con=ais_con,
        )
        assert track == []

    def test_empty_database(
        self, ais_con: duckdb.DuckDBPyConnection
    ) -> None:
        track = query_track(
            mmsi=211234567,
            start_date=date(2025, 12, 25),
            end_date=date(2025, 12, 25),
            tenant_id="default",
            con=ais_con,
        )
        assert track == []
