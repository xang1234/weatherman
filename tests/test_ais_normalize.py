"""Tests for weatherman.ais.normalize — Parquet schema normalization."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pytest

from weatherman.ais.db import AISDatabase
from weatherman.ais.normalize import NORMALIZE_SQL, normalize_query


@pytest.fixture()
def parquet_dir(tmp_path: Path) -> Path:
    """Create a minimal Hive-partitioned Parquet dataset for testing.

    Writes two rows into a single partition (movement_date=2025-12-25).
    """
    con = duckdb.connect(":memory:")
    partition_dir = tmp_path / "movement_date=2025-12-25"
    partition_dir.mkdir()
    out_file = partition_dir / "part-0.parquet"

    con.execute(f"""
        COPY (
            SELECT
                '9876543-211234567'     AS imommsi,
                180000                  AS dwt,
                12.5                    AS max_draught,
                'Capesize'              AS vessel_class,
                CAST(211234567 AS BIGINT) AS mmsi,
                '9876543'               AS lrimoshipno,
                TIMESTAMP '2025-12-25 12:00:00' AS movementdatetime,
                1.35                    AS latitude,
                103.8                   AS longitude,
                'MV BULK CARRIER'       AS shipname,
                'Cargo'                 AS shiptype,
                45.0                    AS beam,
                14.2                    AS draught,
                292.0                   AS length,
                12.5                    AS speed,
                245.0                   AS heading,
                'SINGAPORE'             AS destination,
                'Singapore'             AS destinationtidied,
                TIMESTAMP '2025-12-30 06:00:00' AS eta,
                'Under way using engine' AS movestatus,
                'ABCD1'                 AS callsign,
                NULL                    AS additionalinfo,
                'mov-001'               AS movementid
            UNION ALL
            SELECT
                '1234567-311234567',
                85000,
                10.0,
                'Supramax',
                CAST(311234567 AS BIGINT),
                '1234567',
                TIMESTAMP '2025-12-25 14:30:00',
                51.5,
                -0.1,
                'MV GRAIN STAR',
                'Cargo',
                32.0,
                11.0,
                190.0,
                8.3,
                180.0,
                'ROTTERDAM',
                'Rotterdam',
                TIMESTAMP '2025-12-28 10:00:00',
                'At anchor',
                'EFGH2',
                'Some info',
                'mov-002'
        ) TO '{out_file}' (FORMAT PARQUET)
    """)
    con.close()
    return tmp_path


@pytest.fixture()
def ais_con() -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB connection with spatial extension."""
    db = AISDatabase(":memory:")
    con = db.connect()
    yield con
    db.close()


class TestNormalizeQuery:
    """Test the normalization SQL against synthetic Parquet data."""

    def test_returns_all_canonical_columns(
        self, parquet_dir: Path, ais_con: duckdb.DuckDBPyConnection
    ) -> None:
        result = normalize_query(
            f"{parquet_dir}/**/*",
            tenant_id="test-tenant",
            con=ais_con,
        )
        col_names = [desc[0] for desc in result.description]
        expected = [
            "imo", "mmsi", "imommsi", "lrimoshipno", "movementid",
            "vessel_name", "shiptype", "vessel_class", "dwt", "callsign",
            "beam", "length",
            "timestamp", "date", "lat", "lon",
            "sog", "heading", "draught", "max_draught", "movestatus",
            "destination", "destinationtidied", "eta",
            "additionalinfo", "tenant_id",
        ]
        assert col_names == expected

    def test_imo_parsed_from_imommsi(
        self, parquet_dir: Path, ais_con: duckdb.DuckDBPyConnection
    ) -> None:
        result = normalize_query(
            f"{parquet_dir}/**/*",
            tenant_id="default",
            con=ais_con,
        )
        rows = result.fetchall()
        imos = {r[0] for r in rows}
        assert imos == {"9876543", "1234567"}

    def test_column_renames(
        self, parquet_dir: Path, ais_con: duckdb.DuckDBPyConnection
    ) -> None:
        result = normalize_query(
            f"{parquet_dir}/**/*",
            tenant_id="default",
            con=ais_con,
        )
        rows = result.fetchall()
        col_names = [desc[0] for desc in result.description]

        # Find the first row (MV BULK CARRIER)
        row = next(r for r in rows if r[col_names.index("mmsi")] == 211234567)

        assert row[col_names.index("vessel_name")] == "MV BULK CARRIER"
        assert float(row[col_names.index("lat")]) == pytest.approx(1.35)
        assert float(row[col_names.index("lon")]) == pytest.approx(103.8)
        assert float(row[col_names.index("sog")]) == pytest.approx(12.5)

    def test_tenant_id_assigned(
        self, parquet_dir: Path, ais_con: duckdb.DuckDBPyConnection
    ) -> None:
        result = normalize_query(
            f"{parquet_dir}/**/*",
            tenant_id="acme-corp",
            con=ais_con,
        )
        rows = result.fetchall()
        col_names = [desc[0] for desc in result.description]
        tid_idx = col_names.index("tenant_id")
        tenant_ids = {r[tid_idx] for r in rows}
        assert tenant_ids == {"acme-corp"}

    def test_all_source_columns_retained(
        self, parquet_dir: Path, ais_con: duckdb.DuckDBPyConnection
    ) -> None:
        """All source data should be accessible via canonical names."""
        result = normalize_query(
            f"{parquet_dir}/**/*",
            tenant_id="default",
            con=ais_con,
        )
        rows = result.fetchall()
        col_names = [desc[0] for desc in result.description]
        row = next(r for r in rows if r[col_names.index("mmsi")] == 211234567)

        # Original composite key preserved
        assert row[col_names.index("imommsi")] == "9876543-211234567"
        # Passthrough columns
        assert row[col_names.index("vessel_class")] == "Capesize"
        assert row[col_names.index("dwt")] == 180000
        assert row[col_names.index("destination")] == "SINGAPORE"
        assert row[col_names.index("destinationtidied")] == "Singapore"
        assert row[col_names.index("callsign")] == "ABCD1"
        assert float(row[col_names.index("heading")]) == pytest.approx(245.0)
        assert float(row[col_names.index("draught")]) == pytest.approx(14.2)
        assert float(row[col_names.index("max_draught")]) == pytest.approx(12.5)
        assert row[col_names.index("movestatus")] == "Under way using engine"

    def test_hive_partition_date_captured(
        self, parquet_dir: Path, ais_con: duckdb.DuckDBPyConnection
    ) -> None:
        """movement_date from Hive partition should appear as 'date'."""
        result = normalize_query(
            f"{parquet_dir}/**/*",
            tenant_id="default",
            con=ais_con,
        )
        rows = result.fetchall()
        col_names = [desc[0] for desc in result.description]
        date_idx = col_names.index("date")
        dates = {r[date_idx] for r in rows}
        assert date(2025, 12, 25) in dates

    def test_row_count_matches_source(
        self, parquet_dir: Path, ais_con: duckdb.DuckDBPyConnection
    ) -> None:
        result = normalize_query(
            f"{parquet_dir}/**/*",
            tenant_id="default",
            con=ais_con,
        )
        rows = result.fetchall()
        assert len(rows) == 2


class TestNormalizeInsertInto:
    """Test that normalized output can be inserted into ais_positions table."""

    def test_insert_into_ais_positions(
        self, parquet_dir: Path, ais_con: duckdb.DuckDBPyConnection
    ) -> None:
        """End-to-end: normalize Parquet → INSERT INTO ais_positions."""
        ais_con.execute(f"""
            INSERT INTO ais_positions
            SELECT * FROM (
                {NORMALIZE_SQL}
            )
        """, {"parquet_path": f"{parquet_dir}/**/*", "tenant_id": "default"})

        rows = ais_con.execute(
            "SELECT imo, mmsi, vessel_name, sog, tenant_id "
            "FROM ais_positions ORDER BY mmsi"
        ).fetchall()
        assert len(rows) == 2

        # First row (lower MMSI)
        assert rows[0][0] == "9876543"
        assert rows[0][1] == 211234567
        assert rows[0][2] == "MV BULK CARRIER"
        assert rows[0][3] == 12.5
        assert rows[0][4] == "default"

        # Second row
        assert rows[1][0] == "1234567"
        assert rows[1][1] == 311234567
        assert rows[1][2] == "MV GRAIN STAR"
