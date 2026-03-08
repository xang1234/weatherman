"""DuckDB setup with spatial extension for AIS position data.

Provides a managed connection to a DuckDB database with the spatial
extension installed and the ``ais_positions`` table created. The schema
matches the source Parquet columns from the AIS data provider, with an
additional ``tenant_id`` column assigned during ingest.

Usage::

    from weatherman.ais.db import AISDatabase

    db = AISDatabase("ais.duckdb")
    con = db.connect()
    # ... use con for queries ...
    db.close()

DuckDB is single-writer but our writes are daily batch (one writer).
Reads are concurrent and safe.
"""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)

# Schema version — bump when altering the table DDL so that downstream
# migrations (manual for now) know what to expect.
SCHEMA_VERSION = 1

AIS_POSITIONS_DDL = """\
CREATE TABLE IF NOT EXISTS ais_positions (
    -- Core identifiers
    imommsi         VARCHAR,        -- composite '{imo}-{mmsi}' from source
    mmsi            BIGINT,
    lrimoshipno     VARCHAR,        -- LR/IMO ship number
    movementid      VARCHAR,        -- row-level unique ID from source

    -- Vessel info
    shipname        VARCHAR,
    shiptype        VARCHAR,        -- e.g. 'Cargo'
    vessel_class    VARCHAR,        -- e.g. 'Capesize', 'Valemax'
    dwt             BIGINT,         -- deadweight tonnage
    callsign        VARCHAR,
    beam            DOUBLE,
    length          DOUBLE,

    -- Position / movement
    movementdatetime TIMESTAMP,     -- position timestamp
    movement_date   DATE,           -- partition key
    latitude        DOUBLE,
    longitude       DOUBLE,
    speed           DOUBLE,         -- SOG
    heading         DOUBLE,
    draught         DOUBLE,
    max_draught     DOUBLE,
    movestatus      VARCHAR,        -- nav status

    -- Destination
    destination         VARCHAR,
    destinationtidied   VARCHAR,    -- cleaned destination
    eta                 TIMESTAMP,

    -- Platform
    additionalinfo  VARCHAR,
    tenant_id       VARCHAR NOT NULL
);
"""

# Index on movement_date for efficient daily queries (snapshot, tile generation).
AIS_DATE_INDEX_DDL = """\
CREATE INDEX IF NOT EXISTS idx_ais_positions_date
    ON ais_positions (movement_date);
"""

# Index on mmsi for vessel-level lookups (track queries).
AIS_MMSI_INDEX_DDL = """\
CREATE INDEX IF NOT EXISTS idx_ais_positions_mmsi
    ON ais_positions (mmsi);
"""


class AISDatabase:
    """Managed DuckDB connection with spatial extension and AIS schema.

    Parameters
    ----------
    path:
        Path to the DuckDB file.  Use ``":memory:"`` for testing.
    read_only:
        Open in read-only mode (safe for concurrent readers).
    """

    def __init__(self, path: str | Path, *, read_only: bool = False) -> None:
        self._path = str(path)
        self._read_only = read_only
        self._con: duckdb.DuckDBPyConnection | None = None

    def connect(self) -> duckdb.DuckDBPyConnection:
        """Open the database, install spatial extension, and ensure schema exists.

        Returns the DuckDB connection for direct use.
        """
        if self._con is not None:
            return self._con

        logger.info(
            "Opening AIS database",
            extra={"path": self._path, "read_only": self._read_only},
        )
        self._con = duckdb.connect(self._path, read_only=self._read_only)
        self._con.install_extension("spatial")
        self._con.load_extension("spatial")

        if not self._read_only:
            self._ensure_schema()

        return self._con

    def close(self) -> None:
        """Close the database connection."""
        if self._con is not None:
            self._con.close()
            self._con = None
            logger.info("Closed AIS database", extra={"path": self._path})

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        """Return the active connection, raising if not yet connected."""
        if self._con is None:
            raise RuntimeError("AISDatabase not connected — call connect() first")
        return self._con

    def _ensure_schema(self) -> None:
        """Create the ais_positions table and indexes if they don't exist."""
        assert self._con is not None
        self._con.execute(AIS_POSITIONS_DDL)
        self._con.execute(AIS_DATE_INDEX_DDL)
        self._con.execute(AIS_MMSI_INDEX_DDL)
        logger.info("AIS schema ensured (version %d)", SCHEMA_VERSION)
