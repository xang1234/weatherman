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


def load_day_from_select(
    select_sql: str,
    *,
    load_date: date,
    tenant_id: str,
    con: duckdb.DuckDBPyConnection,
    params: dict[str, object] | None = None,
    log_context: dict[str, object] | None = None,
) -> int:
    """Replace one AIS day using the rows produced by *select_sql*.

    Parameters
    ----------
    select_sql:
        SQL query that returns rows matching the ``ais_positions`` schema.
        The query is used as ``INSERT INTO ais_positions {select_sql}``.
    load_date:
        The date being loaded. Used for idempotent replacement.
    tenant_id:
        Tenant identifier stamped on the rows being replaced.
    con:
        An open read-write DuckDB connection.
    params:
        Optional bound parameters for *select_sql*.
    log_context:
        Extra values added to the structured logger context.

    Returns
    -------
    int
        Number of rows loaded for the given day and tenant.
    """
    context = {
        "load_date": str(load_date),
        "tenant_id": tenant_id,
    }
    if log_context:
        context.update(log_context)

    logger.info("Loading AIS day", extra=context)

    con.begin()
    try:
        deleted = con.execute(
            'DELETE FROM ais_positions WHERE "date" = $load_date AND tenant_id = $tenant_id',
            {"load_date": load_date, "tenant_id": tenant_id},
        ).fetchone()

        con.execute(
            f"INSERT INTO ais_positions {select_sql}",
            params or {},
        )

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
            **context,
            "rows_loaded": row_count,
            "rows_deleted": deleted_count,
        },
    )
    return row_count


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
    return load_day_from_select(
        f"SELECT * FROM ({NORMALIZE_SQL})",
        load_date=load_date,
        tenant_id=tenant_id,
        con=con,
        params={"parquet_path": path_str, "tenant_id": tenant_id},
        log_context={"parquet_path": path_str},
    )
