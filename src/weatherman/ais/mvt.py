"""Generate MVT (Mapbox Vector Tiles) from AIS snapshot data.

Converts vessel positions from the daily snapshot into Protocol Buffer-encoded
vector tiles for efficient map rendering.  Each tile contains a ``vessels``
layer with point features for every vessel whose latest position falls within
the tile's geographic bounds.

Tile properties per vessel (chosen for map rendering + basic popup)::

    mmsi, vessel_name, sog, heading, shiptype, vessel_class,
    dwt, destination, destinationtidied, eta

Usage::

    tile_bytes = generate_tile(
        con=con,
        snapshot_date=date(2026, 3, 8),
        tenant_id="default",
        z=4, x=8, y=5,
    )

Coordinate system: tiles follow the Web Mercator (EPSG:3857) XYZ scheme used
by MapLibre / Leaflet / OpenLayers.  Geographic bounds for each tile are
computed from the standard slippy-map formula.
"""

from __future__ import annotations

import math
from datetime import date

import duckdb
import mapbox_vector_tile as mvt

# Column list for the SELECT — must match the property dict construction below.
_TILE_COLUMNS = (
    "mmsi, vessel_name, sog, heading, shiptype, vessel_class, "
    "dwt, destination, destinationtidied, eta, lat, lon"
)

_TILE_QUERY = f"""\
SELECT {_TILE_COLUMNS}
FROM ais_snapshot
WHERE "date" = $snapshot_date
  AND tenant_id = $tenant_id
  AND lon >= $west AND lon < $east
  AND lat >= $south AND lat < $north
"""

# MVT spec default extent (4096 = standard for Mapbox/MapLibre).
DEFAULT_EXTENT = 4096

# Zoom range supported for AIS tiles.
MIN_ZOOM = 0
MAX_ZOOM = 12


def tile_bounds(z: int, x: int, y: int) -> tuple[float, float, float, float]:
    """Return the WGS-84 bounding box (west, south, east, north) for a tile.

    Uses the standard Web Mercator slippy-map tile scheme.
    """
    n = 2**z
    west = x / n * 360.0 - 180.0
    east = (x + 1) / n * 360.0 - 180.0
    north = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * y / n))))
    south = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n))))
    return west, south, east, north


def _row_to_feature(
    row: tuple,
) -> dict:
    """Convert a DuckDB result row into a GeoJSON-like feature dict.

    Coordinates are in WGS-84 (lon, lat) — the MVT encoder's
    ``quantize_bounds`` option handles the projection into tile space.
    """
    mmsi, vessel_name, sog, heading, shiptype, vessel_class, \
        dwt, destination, destinationtidied, eta, lat, lon = row

    properties: dict = {"mmsi": mmsi}

    # Only include non-null string properties to keep tile size small.
    if vessel_name:
        properties["vessel_name"] = vessel_name
    if shiptype:
        properties["shiptype"] = shiptype
    if vessel_class:
        properties["vessel_class"] = vessel_class
    if destination:
        properties["destination"] = destination
    if destinationtidied:
        properties["destinationtidied"] = destinationtidied
    if eta is not None:
        properties["eta"] = str(eta)

    # Numeric properties — always include (0 is a valid value).
    if sog is not None:
        properties["sog"] = round(sog, 1)
    if heading is not None:
        properties["heading"] = round(heading, 1)
    if dwt is not None:
        properties["dwt"] = dwt

    return {
        "geometry": f"POINT({lon} {lat})",
        "properties": properties,
    }


def generate_tile(
    *,
    con: duckdb.DuckDBPyConnection,
    snapshot_date: date,
    tenant_id: str,
    z: int,
    x: int,
    y: int,
    extent: int = DEFAULT_EXTENT,
) -> bytes:
    """Generate a single MVT tile from the AIS snapshot.

    Parameters
    ----------
    con:
        Open DuckDB connection with ``ais_snapshot`` populated.
    snapshot_date:
        The AIS date to render.
    tenant_id:
        Tenant identifier — only vessels for this tenant are included.
    z, x, y:
        Tile coordinates in XYZ scheme.
    extent:
        MVT tile extent (default 4096).

    Returns
    -------
    bytes
        Encoded MVT tile, or ``b""`` if no vessels fall within the tile.
    """
    west, south, east, north = tile_bounds(z, x, y)

    rows = con.execute(
        _TILE_QUERY,
        {
            "snapshot_date": snapshot_date,
            "tenant_id": tenant_id,
            "west": west,
            "south": south,
            "east": east,
            "north": north,
        },
    ).fetchall()

    if not rows:
        return b""

    features = [_row_to_feature(row) for row in rows]

    return mvt.encode(
        [{
            "name": "vessels",
            "features": features,
        }],
        default_options={
            "quantize_bounds": (west, south, east, north),
            "extents": extent,
        },
    )
