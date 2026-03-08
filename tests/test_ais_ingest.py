"""Tests for weatherman.ais.ingest — Daily Parquet to DuckDB load pipeline."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pytest

from weatherman.ais.ingest import load_day
from tests.conftest_ais import (
    _write_test_parquet,
    ROW_BULK_CARRIER,
    ROW_GRAIN_STAR,
    ais_db,
    ais_con,
    parquet_dir,
)


class TestLoadDay:
    """Core daily load tests."""

    def test_loads_rows(
        self, parquet_dir: Path, ais_con: duckdb.DuckDBPyConnection
    ) -> None:
        count = load_day(
            f"{parquet_dir}/movement_date=2025-12-25/*",
            load_date=date(2025, 12, 25),
            tenant_id="default",
            con=ais_con,
        )
        assert count == 2

    def test_rows_in_table(
        self, parquet_dir: Path, ais_con: duckdb.DuckDBPyConnection
    ) -> None:
        load_day(
            f"{parquet_dir}/movement_date=2025-12-25/*",
            load_date=date(2025, 12, 25),
            tenant_id="default",
            con=ais_con,
        )
        rows = ais_con.execute(
            "SELECT imo, mmsi, vessel_name, sog, tenant_id "
            "FROM ais_positions ORDER BY mmsi"
        ).fetchall()
        assert len(rows) == 2
        assert rows[0] == ("9876543", 211234567, "MV BULK CARRIER", 12.5, "default")
        assert rows[1] == ("1234567", 311234567, "MV GRAIN STAR", 8.3, "default")

    def test_normalized_columns(
        self, parquet_dir: Path, ais_con: duckdb.DuckDBPyConnection
    ) -> None:
        """Verify column renames applied during load."""
        load_day(
            f"{parquet_dir}/movement_date=2025-12-25/*",
            load_date=date(2025, 12, 25),
            tenant_id="default",
            con=ais_con,
        )
        row = ais_con.execute(
            'SELECT lat, lon, sog, vessel_name, "date" '
            "FROM ais_positions WHERE mmsi = 211234567"
        ).fetchone()
        assert row[0] == 1.35     # lat (was latitude)
        assert row[1] == 103.8    # lon (was longitude)
        assert row[2] == 12.5     # sog (was speed)
        assert row[3] == "MV BULK CARRIER"  # vessel_name (was shipname)
        assert row[4] == date(2025, 12, 25)  # date (was movement_date)


class TestIdempotency:
    """Re-loading the same date replaces data atomically."""

    def test_reload_same_date_replaces(
        self, parquet_dir: Path, ais_con: duckdb.DuckDBPyConnection
    ) -> None:
        # Load once
        load_day(
            f"{parquet_dir}/movement_date=2025-12-25/*",
            load_date=date(2025, 12, 25),
            tenant_id="default",
            con=ais_con,
        )
        # Load again — should replace, not duplicate
        count = load_day(
            f"{parquet_dir}/movement_date=2025-12-25/*",
            load_date=date(2025, 12, 25),
            tenant_id="default",
            con=ais_con,
        )
        assert count == 2
        total = ais_con.execute(
            "SELECT COUNT(*) FROM ais_positions"
        ).fetchone()[0]
        assert total == 2

    def test_reload_three_times(
        self, parquet_dir: Path, ais_con: duckdb.DuckDBPyConnection
    ) -> None:
        for _ in range(3):
            load_day(
                f"{parquet_dir}/movement_date=2025-12-25/*",
                load_date=date(2025, 12, 25),
                tenant_id="default",
                con=ais_con,
            )
        total = ais_con.execute(
            "SELECT COUNT(*) FROM ais_positions"
        ).fetchone()[0]
        assert total == 2


class TestMultiDay:
    """Loading different dates accumulates rows independently."""

    def test_two_days_independent(
        self, tmp_path: Path, ais_con: duckdb.DuckDBPyConnection
    ) -> None:
        # Day 1
        _write_test_parquet(
            tmp_path,
            "movement_date=2025-12-25",
            ROW_BULK_CARRIER,
        )
        # Day 2 — same vessel, different timestamp
        day2_sql = ROW_GRAIN_STAR.replace(
            "TIMESTAMP '2025-12-25 14:30:00'",
            "TIMESTAMP '2025-12-26 14:30:00'",
        )
        _write_test_parquet(tmp_path, "movement_date=2025-12-26", day2_sql)

        load_day(
            f"{tmp_path}/movement_date=2025-12-25/*",
            load_date=date(2025, 12, 25),
            tenant_id="default",
            con=ais_con,
        )
        load_day(
            f"{tmp_path}/movement_date=2025-12-26/*",
            load_date=date(2025, 12, 26),
            tenant_id="default",
            con=ais_con,
        )

        total = ais_con.execute(
            "SELECT COUNT(*) FROM ais_positions"
        ).fetchone()[0]
        assert total == 2  # one row per day

        # Reloading day 1 doesn't touch day 2
        load_day(
            f"{tmp_path}/movement_date=2025-12-25/*",
            load_date=date(2025, 12, 25),
            tenant_id="default",
            con=ais_con,
        )
        total = ais_con.execute(
            "SELECT COUNT(*) FROM ais_positions"
        ).fetchone()[0]
        assert total == 2


class TestTenantIsolation:
    """Different tenants' data is isolated during reload."""

    def test_reload_does_not_affect_other_tenant(
        self, parquet_dir: Path, ais_con: duckdb.DuckDBPyConnection
    ) -> None:
        # Load for tenant A
        load_day(
            f"{parquet_dir}/movement_date=2025-12-25/*",
            load_date=date(2025, 12, 25),
            tenant_id="tenant-a",
            con=ais_con,
        )
        # Load for tenant B
        load_day(
            f"{parquet_dir}/movement_date=2025-12-25/*",
            load_date=date(2025, 12, 25),
            tenant_id="tenant-b",
            con=ais_con,
        )
        total = ais_con.execute(
            "SELECT COUNT(*) FROM ais_positions"
        ).fetchone()[0]
        assert total == 4  # 2 per tenant

        # Reload tenant A — should not touch tenant B
        load_day(
            f"{parquet_dir}/movement_date=2025-12-25/*",
            load_date=date(2025, 12, 25),
            tenant_id="tenant-a",
            con=ais_con,
        )
        total = ais_con.execute(
            "SELECT COUNT(*) FROM ais_positions"
        ).fetchone()[0]
        assert total == 4

        tenant_b_count = ais_con.execute(
            "SELECT COUNT(*) FROM ais_positions WHERE tenant_id = 'tenant-b'"
        ).fetchone()[0]
        assert tenant_b_count == 2


class TestTransactionSafety:
    """Verify rollback on failure preserves existing data."""

    def test_bad_parquet_path_rolls_back(
        self, parquet_dir: Path, ais_con: duckdb.DuckDBPyConnection
    ) -> None:
        # Load valid data first
        load_day(
            f"{parquet_dir}/movement_date=2025-12-25/*",
            load_date=date(2025, 12, 25),
            tenant_id="default",
            con=ais_con,
        )
        assert ais_con.execute(
            "SELECT COUNT(*) FROM ais_positions"
        ).fetchone()[0] == 2

        # Attempt load with bad path — should fail and rollback
        with pytest.raises(duckdb.IOException):
            load_day(
                "/nonexistent/path/*.parquet",
                load_date=date(2025, 12, 25),
                tenant_id="default",
                con=ais_con,
            )

        # Original data should be preserved
        count = ais_con.execute(
            "SELECT COUNT(*) FROM ais_positions"
        ).fetchone()[0]
        assert count == 2
