"""Shared fixtures for AIS tests (db, normalize, ingest)."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from weatherman.ais.db import AISDatabase


@pytest.fixture()
def ais_db() -> AISDatabase:
    """In-memory AISDatabase instance."""
    db = AISDatabase(":memory:")
    db.connect()
    yield db
    db.close()


@pytest.fixture()
def ais_con(ais_db: AISDatabase) -> duckdb.DuckDBPyConnection:
    """DuckDB connection from the in-memory AISDatabase."""
    return ais_db.connection


def _write_test_parquet(tmp_path: Path, partition: str, rows_sql: str) -> None:
    """Helper: write rows into a Hive-partitioned Parquet file."""
    con = duckdb.connect(":memory:")
    partition_dir = tmp_path / partition
    partition_dir.mkdir(parents=True, exist_ok=True)
    out_file = partition_dir / "part-0.parquet"
    con.execute(f"COPY ({rows_sql}) TO '{out_file}' (FORMAT PARQUET)")
    con.close()


ROW_BULK_CARRIER = """\
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
    'mov-001'               AS movementid\
"""

ROW_GRAIN_STAR = """\
SELECT
    '1234567-311234567'     AS imommsi,
    85000                   AS dwt,
    10.0                    AS max_draught,
    'Supramax'              AS vessel_class,
    CAST(311234567 AS BIGINT) AS mmsi,
    '1234567'               AS lrimoshipno,
    TIMESTAMP '2025-12-25 14:30:00' AS movementdatetime,
    51.5                    AS latitude,
    -0.1                    AS longitude,
    'MV GRAIN STAR'         AS shipname,
    'Cargo'                 AS shiptype,
    32.0                    AS beam,
    11.0                    AS draught,
    190.0                   AS length,
    8.3                     AS speed,
    180.0                   AS heading,
    'ROTTERDAM'             AS destination,
    'Rotterdam'             AS destinationtidied,
    TIMESTAMP '2025-12-28 10:00:00' AS eta,
    'At anchor'             AS movestatus,
    'EFGH2'                 AS callsign,
    'Some info'             AS additionalinfo,
    'mov-002'               AS movementid\
"""


@pytest.fixture()
def parquet_dir(tmp_path: Path) -> Path:
    """Hive-partitioned Parquet with two rows for 2025-12-25."""
    _write_test_parquet(
        tmp_path,
        "movement_date=2025-12-25",
        f"{ROW_BULK_CARRIER} UNION ALL {ROW_GRAIN_STAR}",
    )
    return tmp_path
