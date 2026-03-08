"""Tests for weatherman.ais.snapshot — latest-position-per-vessel snapshot."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pytest

from weatherman.ais.snapshot import build_snapshot, ensure_snapshot_schema
from weatherman.ais.ingest import load_day
from tests.conftest_ais import (
    _write_test_parquet,
    ROW_BULK_CARRIER,
    ROW_GRAIN_STAR,
    ais_db,
    ais_con,
    parquet_dir,
)


class TestBuildSnapshot:
    """Core snapshot generation tests."""

    def test_returns_vessel_count(
        self, parquet_dir: Path, ais_con: duckdb.DuckDBPyConnection
    ) -> None:
        load_day(
            f"{parquet_dir}/movement_date=2025-12-25/*",
            load_date=date(2025, 12, 25),
            tenant_id="default",
            con=ais_con,
        )
        count = build_snapshot(
            snapshot_date=date(2025, 12, 25),
            tenant_id="default",
            con=ais_con,
        )
        assert count == 2

    def test_snapshot_contains_required_columns(
        self, parquet_dir: Path, ais_con: duckdb.DuckDBPyConnection
    ) -> None:
        """Snapshot must include heading and shiptype for arrow rendering."""
        load_day(
            f"{parquet_dir}/movement_date=2025-12-25/*",
            load_date=date(2025, 12, 25),
            tenant_id="default",
            con=ais_con,
        )
        build_snapshot(
            snapshot_date=date(2025, 12, 25),
            tenant_id="default",
            con=ais_con,
        )
        row = ais_con.execute(
            "SELECT mmsi, vessel_name, shiptype, heading, sog, lat, lon, "
            'vessel_class, dwt, destination, destinationtidied, "date" '
            "FROM ais_snapshot WHERE mmsi = 211234567"
        ).fetchone()
        assert row[0] == 211234567          # mmsi
        assert row[1] == "MV BULK CARRIER"  # vessel_name
        assert row[2] == "Cargo"            # shiptype (for color coding)
        assert row[3] == 245.0              # heading (for arrow rotation)
        assert row[4] == 12.5               # sog
        assert row[5] == 1.35               # lat
        assert row[6] == 103.8              # lon
        assert row[7] == "Capesize"         # vessel_class
        assert row[8] == 180000             # dwt
        assert row[9] == "SINGAPORE"        # destination
        assert row[10] == "Singapore"       # destinationtidied
        assert row[11] == date(2025, 12, 25)  # date


class TestLatestPositionPick:
    """Verify the window function picks the most recent position per vessel."""

    def test_picks_latest_timestamp(
        self, tmp_path: Path, ais_con: duckdb.DuckDBPyConnection
    ) -> None:
        """When a vessel has multiple reports in a day, snapshot picks the latest."""
        # Two reports for the same vessel at different times
        early_report = ROW_BULK_CARRIER.replace(
            "TIMESTAMP '2025-12-25 12:00:00'",
            "TIMESTAMP '2025-12-25 06:00:00'",
        ).replace("12.5                    AS speed", "8.0                     AS speed")
        late_report = ROW_BULK_CARRIER.replace(
            "'mov-001'               AS movementid",
            "'mov-003'               AS movementid",
        )

        _write_test_parquet(
            tmp_path,
            "movement_date=2025-12-25",
            f"{early_report} UNION ALL {late_report}",
        )
        load_day(
            f"{tmp_path}/movement_date=2025-12-25/*",
            load_date=date(2025, 12, 25),
            tenant_id="default",
            con=ais_con,
        )
        build_snapshot(
            snapshot_date=date(2025, 12, 25),
            tenant_id="default",
            con=ais_con,
        )

        rows = ais_con.execute(
            'SELECT mmsi, sog, "timestamp" FROM ais_snapshot WHERE mmsi = 211234567'
        ).fetchall()
        assert len(rows) == 1
        # Must pick the 12:00 report (speed=12.5), not the 06:00 one (speed=8.0)
        assert rows[0][1] == 12.5  # sog from late report
        assert rows[0][1] != 8.0   # NOT the early report's sog
        assert rows[0][2].hour == 12  # timestamp from the later report


class TestIdempotency:
    """Rebuilding snapshot for the same date replaces previous data."""

    def test_rebuild_replaces(
        self, parquet_dir: Path, ais_con: duckdb.DuckDBPyConnection
    ) -> None:
        load_day(
            f"{parquet_dir}/movement_date=2025-12-25/*",
            load_date=date(2025, 12, 25),
            tenant_id="default",
            con=ais_con,
        )
        build_snapshot(
            snapshot_date=date(2025, 12, 25),
            tenant_id="default",
            con=ais_con,
        )
        # Rebuild — should replace, not duplicate
        count = build_snapshot(
            snapshot_date=date(2025, 12, 25),
            tenant_id="default",
            con=ais_con,
        )
        assert count == 2
        total = ais_con.execute(
            "SELECT COUNT(*) FROM ais_snapshot"
        ).fetchone()[0]
        assert total == 2


class TestTenantIsolation:
    """Snapshot rebuild for one tenant doesn't affect another."""

    def test_rebuild_does_not_affect_other_tenant(
        self, parquet_dir: Path, ais_con: duckdb.DuckDBPyConnection
    ) -> None:
        # Load data for both tenants
        for tid in ("tenant-a", "tenant-b"):
            load_day(
                f"{parquet_dir}/movement_date=2025-12-25/*",
                load_date=date(2025, 12, 25),
                tenant_id=tid,
                con=ais_con,
            )
            build_snapshot(
                snapshot_date=date(2025, 12, 25),
                tenant_id=tid,
                con=ais_con,
            )

        total = ais_con.execute(
            "SELECT COUNT(*) FROM ais_snapshot"
        ).fetchone()[0]
        assert total == 4  # 2 vessels x 2 tenants

        # Rebuild tenant-a — tenant-b untouched
        build_snapshot(
            snapshot_date=date(2025, 12, 25),
            tenant_id="tenant-a",
            con=ais_con,
        )
        tenant_b = ais_con.execute(
            "SELECT COUNT(*) FROM ais_snapshot WHERE tenant_id = 'tenant-b'"
        ).fetchone()[0]
        assert tenant_b == 2


class TestEmptyDate:
    """Snapshot for a date with no data returns zero."""

    def test_no_data_returns_zero(
        self, ais_con: duckdb.DuckDBPyConnection,
    ) -> None:
        count = build_snapshot(
            snapshot_date=date(2099, 1, 1),
            tenant_id="default",
            con=ais_con,
        )
        assert count == 0


class TestMultiDate:
    """Snapshots for different dates are independent."""

    def test_two_dates_independent(
        self, tmp_path: Path, ais_con: duckdb.DuckDBPyConnection
    ) -> None:
        # Day 1
        _write_test_parquet(
            tmp_path, "movement_date=2025-12-25", ROW_BULK_CARRIER
        )
        # Day 2
        day2 = ROW_GRAIN_STAR.replace(
            "TIMESTAMP '2025-12-25 14:30:00'",
            "TIMESTAMP '2025-12-26 14:30:00'",
        )
        _write_test_parquet(tmp_path, "movement_date=2025-12-26", day2)

        for d in (date(2025, 12, 25), date(2025, 12, 26)):
            load_day(
                f"{tmp_path}/movement_date={d}/*",
                load_date=d,
                tenant_id="default",
                con=ais_con,
            )
            build_snapshot(snapshot_date=d, tenant_id="default", con=ais_con)

        total = ais_con.execute(
            "SELECT COUNT(*) FROM ais_snapshot"
        ).fetchone()[0]
        assert total == 2  # one vessel per date

        # Rebuild day 1 doesn't touch day 2
        build_snapshot(
            snapshot_date=date(2025, 12, 25),
            tenant_id="default",
            con=ais_con,
        )
        total = ais_con.execute(
            "SELECT COUNT(*) FROM ais_snapshot"
        ).fetchone()[0]
        assert total == 2
