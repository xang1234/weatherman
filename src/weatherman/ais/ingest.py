"""Daily Parquet to DuckDB load pipeline for AIS position data.

Implements idempotent daily ingest: for a given date, deletes any existing
rows and inserts freshly normalized data from Parquet in a single transaction.
Re-loading the same date replaces data atomically.

Usage::

    from weatherman.ais.db import AISDatabase
    from weatherman.ais.ingest import load_day

    db = AISDatabase("ais.duckdb")
    con = db.connect()
    count = load_day(
        ".data/ais/movement_date=2025-12-25/*",
        load_date=date(2025, 12, 25),
        tenant_id="default",
        con=con,
    )
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import duckdb

from weatherman.ais.normalize import NORMALIZE_SQL

logger = logging.getLogger(__name__)


def load_day(
    parquet_path: str | Path,
    *,
    load_date: date,
    tenant_id: str,
    con: duckdb.DuckDBPyConnection,
) -> int:
    """Load a single day's AIS data into DuckDB, replacing existing data.

    The operation is atomic: DELETE + INSERT run inside a transaction.
    If the INSERT fails, the previous data for that date is preserved.

    Parameters
    ----------
    parquet_path:
        Glob pattern pointing to the day's Parquet files.
        Example: ``".data/ais/movement_date=2025-12-25/*"``
    load_date:
        The date being loaded. Used to clear existing rows (idempotency).
    tenant_id:
        Tenant identifier stamped on every row.
    con:
        An open read-write DuckDB connection.

    Returns
    -------
    int
        Number of rows loaded.
    """
    path_str = str(parquet_path)
    logger.info(
        "Loading AIS day",
        extra={
            "load_date": str(load_date),
            "tenant_id": tenant_id,
            "parquet_path": path_str,
        },
    )

    con.begin()
    try:
        # Clear existing data for this date + tenant (idempotent reload)
        deleted = con.execute(
            'DELETE FROM ais_positions WHERE "date" = $load_date AND tenant_id = $tenant_id',
            {"load_date": load_date, "tenant_id": tenant_id},
        ).fetchone()

        # Insert normalized Parquet data
        con.execute(
            f"INSERT INTO ais_positions SELECT * FROM ({NORMALIZE_SQL})",
            {"parquet_path": path_str, "tenant_id": tenant_id},
        )

        # Count inserted rows
        row_count = con.execute(
            'SELECT COUNT(*) FROM ais_positions WHERE "date" = $load_date AND tenant_id = $tenant_id',
            {"load_date": load_date, "tenant_id": tenant_id},
        ).fetchone()[0]

        con.commit()
    except Exception:
        con.rollback()
        raise

    deleted_count = deleted[0] if deleted else 0
    logger.info(
        "AIS day loaded",
        extra={
            "load_date": str(load_date),
            "rows_loaded": row_count,
            "rows_deleted": deleted_count,
            "tenant_id": tenant_id,
        },
    )
    return row_count
