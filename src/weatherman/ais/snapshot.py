"""Snapshot table generation — latest position per vessel for a given date.

Generates the ``ais_snapshot`` table containing one row per vessel at their
most recent reported position for a given AIS date.  This is what the map
displays: directional arrows rotated by heading and color-coded by ship type.

The snapshot is rebuilt idempotently: calling :func:`build_snapshot` for the
same date replaces previous rows atomically.

Usage::

    from weatherman.ais.snapshot import build_snapshot

    row_count = build_snapshot(
        snapshot_date=date(2025, 12, 25),
        tenant_id="default",
        con=con,
    )
"""

from __future__ import annotations

import logging
from datetime import date

import duckdb

logger = logging.getLogger(__name__)

# DDL for the snapshot table — one row per vessel per date.
AIS_SNAPSHOT_DDL = """\
CREATE TABLE IF NOT EXISTS ais_snapshot (
    mmsi            BIGINT,
    imo             VARCHAR,
    vessel_name     VARCHAR,
    shiptype        VARCHAR,
    vessel_class    VARCHAR,
    dwt             BIGINT,
    lat             DOUBLE,
    lon             DOUBLE,
    sog             DOUBLE,
    heading         DOUBLE,
    draught         DOUBLE,
    destination     VARCHAR,
    destinationtidied VARCHAR,
    eta             TIMESTAMP,
    movestatus      VARCHAR,
    "timestamp"     TIMESTAMP,
    "date"          DATE,
    tenant_id       VARCHAR NOT NULL
);
"""

# Index on date for efficient daily lookups and tile generation.
AIS_SNAPSHOT_DATE_INDEX_DDL = """\
CREATE INDEX IF NOT EXISTS idx_ais_snapshot_date
    ON ais_snapshot ("date");
"""

# Window query: latest position per vessel for a given date + tenant.
# ROW_NUMBER partitioned by MMSI, ordered by timestamp DESC picks the
# most recent report.  Ties broken arbitrarily (both are equally "latest").
SNAPSHOT_QUERY = """\
SELECT
    mmsi, imo, vessel_name, shiptype, vessel_class, dwt,
    lat, lon, sog, heading, draught,
    destination, destinationtidied, eta, movestatus,
    "timestamp", "date", tenant_id
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY mmsi
            ORDER BY "timestamp" DESC
        ) AS _rn
    FROM ais_positions
    WHERE "date" = $snapshot_date
      AND tenant_id = $tenant_id
)
WHERE _rn = 1
"""


def ensure_snapshot_schema(con: duckdb.DuckDBPyConnection) -> None:
    """Create the ais_snapshot table and indexes if they don't exist."""
    con.execute(AIS_SNAPSHOT_DDL)
    con.execute(AIS_SNAPSHOT_DATE_INDEX_DDL)


def build_snapshot(
    *,
    snapshot_date: date,
    tenant_id: str,
    con: duckdb.DuckDBPyConnection,
) -> int:
    """Build the latest-position-per-vessel snapshot for a single date.

    The operation is idempotent: existing snapshot rows for the same
    date + tenant are replaced atomically within a transaction.

    Parameters
    ----------
    snapshot_date:
        The AIS date to snapshot.
    tenant_id:
        Tenant identifier — only positions for this tenant are included.
    con:
        An open read-write DuckDB connection with ``ais_positions`` populated.

    Returns
    -------
    int
        Number of unique vessels in the snapshot.
    """
    logger.info(
        "Building AIS snapshot",
        extra={"snapshot_date": str(snapshot_date), "tenant_id": tenant_id},
    )

    ensure_snapshot_schema(con)

    con.begin()
    try:
        # Clear existing snapshot for this date + tenant
        con.execute(
            'DELETE FROM ais_snapshot WHERE "date" = $snapshot_date AND tenant_id = $tenant_id',
            {"snapshot_date": snapshot_date, "tenant_id": tenant_id},
        )

        # Insert latest position per vessel
        con.execute(
            f"INSERT INTO ais_snapshot {SNAPSHOT_QUERY}",
            {"snapshot_date": snapshot_date, "tenant_id": tenant_id},
        )

        # Count inserted rows
        row_count = con.execute(
            'SELECT COUNT(*) FROM ais_snapshot WHERE "date" = $snapshot_date AND tenant_id = $tenant_id',
            {"snapshot_date": snapshot_date, "tenant_id": tenant_id},
        ).fetchone()[0]

        con.commit()
    except Exception:
        con.rollback()
        raise

    logger.info(
        "AIS snapshot built",
        extra={
            "snapshot_date": str(snapshot_date),
            "vessels": row_count,
            "tenant_id": tenant_id,
        },
    )
    return row_count
