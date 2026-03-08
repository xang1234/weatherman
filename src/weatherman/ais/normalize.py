"""Parquet schema normalization for AIS position data.

Transforms raw source Parquet columns into the canonical ``ais_positions``
schema defined in :mod:`weatherman.ais.db`.  The normalization is expressed
as a SQL SELECT so DuckDB can push it down into the Parquet scan — no
intermediate Python copies.

Source data is pre-filtered to dry bulk vessels; no additional vessel-type
filtering is applied here.

Transformations applied:
- Column renames: latitude→lat, longitude→lon, speed→sog,
  shipname→vessel_name, movementdatetime→timestamp, movement_date→date
- Derived column: ``imo`` parsed from ``imommsi`` ('{imo}-{mmsi}')
- Constant column: ``tenant_id`` assigned from caller
- All other source columns pass through unchanged
"""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)

# SQL that reads source Parquet and projects into canonical schema.
# The $parquet_path and $tenant_id parameters are bound at call time.
NORMALIZE_SQL = """\
SELECT
    -- Derived: parse imo from composite key
    split_part(imommsi, '-', 1)     AS imo,
    mmsi,
    imommsi,
    lrimoshipno,
    movementid,

    -- Renames
    shipname                        AS vessel_name,
    shiptype,
    vessel_class,
    dwt,
    callsign,
    beam,
    length,

    movementdatetime                AS "timestamp",
    movement_date                   AS "date",
    latitude                        AS lat,
    longitude                       AS lon,
    speed                           AS sog,
    heading,
    draught,
    max_draught,
    movestatus,

    destination,
    destinationtidied,
    eta,

    additionalinfo,
    $tenant_id                      AS tenant_id
FROM read_parquet($parquet_path, hive_partitioning = true)
"""


def normalize_query(
    parquet_path: str | Path,
    *,
    tenant_id: str,
    con: duckdb.DuckDBPyConnection,
) -> duckdb.DuckDBPyRelation:
    """Return a DuckDB relation with normalized AIS data from Parquet files.

    The relation is lazy — no data is materialized until the caller
    consumes it (e.g., via ``INSERT INTO ... SELECT``).

    Parameters
    ----------
    parquet_path:
        Glob pattern or directory for the source Parquet files.
        Example: ``".data/ais/**/*"`` or ``".data/ais/movement_date=2025-12-25/*"``.
    tenant_id:
        Tenant identifier to stamp on every row.
    con:
        An open DuckDB connection (from :class:`~weatherman.ais.db.AISDatabase`).

    Returns
    -------
    duckdb.DuckDBPyRelation
        A lazy relation with columns matching the ``ais_positions`` schema.
    """
    path_str = str(parquet_path)
    logger.info(
        "Normalizing AIS Parquet",
        extra={"parquet_path": path_str, "tenant_id": tenant_id},
    )
    return con.execute(
        NORMALIZE_SQL,
        {"parquet_path": path_str, "tenant_id": tenant_id},
    )
