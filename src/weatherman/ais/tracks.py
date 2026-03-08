"""Track table generation — time-ordered position history per vessel.

Provides a view (``ais_tracks``) over ``ais_positions`` that projects only
the columns needed for track playback, ordered by timestamp.  A convenience
function :func:`query_track` fetches a single vessel's track for a date range.

The downstream consumer is the Phase 3 track playback visualization
(wx-0pg.6.2), which renders positions as a line on the map, color-coded
by SOG or time, with animated dot playback.

Usage::

    from weatherman.ais.tracks import query_track

    positions = query_track(
        mmsi=211234567,
        start_date=date(2025, 12, 20),
        end_date=date(2025, 12, 25),
        tenant_id="default",
        con=con,
    )
    # Returns list of TrackPoint namedtuples, time-ordered ascending.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime

import duckdb

logger = logging.getLogger(__name__)

# View that projects track-relevant columns from ais_positions, ordered
# by vessel then time.  No data duplication — reads directly from the
# source table using existing indexes (idx_ais_positions_mmsi,
# idx_ais_positions_date).
AIS_TRACKS_VIEW_DDL = """\
CREATE VIEW IF NOT EXISTS ais_tracks AS
SELECT
    mmsi, imo, vessel_name,
    lat, lon, sog, heading,
    "timestamp", "date", tenant_id
FROM ais_positions
ORDER BY mmsi, "timestamp"
"""


@dataclass(frozen=True, slots=True)
class TrackPoint:
    """A single position in a vessel's track."""

    lat: float
    lon: float
    sog: float
    heading: float
    timestamp: datetime


def ensure_tracks_schema(con: duckdb.DuckDBPyConnection) -> None:
    """Create the ais_tracks view if it doesn't exist."""
    con.execute(AIS_TRACKS_VIEW_DDL)


def query_track(
    *,
    mmsi: int,
    start_date: date,
    end_date: date,
    tenant_id: str,
    con: duckdb.DuckDBPyConnection,
) -> list[TrackPoint]:
    """Fetch time-ordered positions for a single vessel over a date range.

    Parameters
    ----------
    mmsi:
        The vessel's MMSI identifier.
    start_date:
        Start of date range (inclusive).
    end_date:
        End of date range (inclusive).
    tenant_id:
        Tenant identifier for data isolation.
    con:
        An open DuckDB connection.

    Returns
    -------
    list[TrackPoint]
        Positions ordered by timestamp ascending (oldest first).
    """
    rows = con.execute(
        'SELECT lat, lon, sog, heading, "timestamp" '
        "FROM ais_tracks "
        "WHERE mmsi = $mmsi "
        '  AND "date" BETWEEN $start_date AND $end_date '
        "  AND tenant_id = $tenant_id "
        'ORDER BY "timestamp"',
        {
            "mmsi": mmsi,
            "start_date": start_date,
            "end_date": end_date,
            "tenant_id": tenant_id,
        },
    ).fetchall()

    return [
        TrackPoint(lat=r[0], lon=r[1], sog=r[2], heading=r[3], timestamp=r[4])
        for r in rows
    ]
