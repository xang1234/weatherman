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
from dataclasses import dataclass
from datetime import date

import duckdb
import mapbox_vector_tile as mvt

# Column list for the SELECT — must match the property dict construction below.
_TILE_COLUMNS = (
    "imommsi, mmsi, vessel_name, sog, heading, shiptype, vessel_class, "
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

# Antimeridian-crossing variant: when west > east (e.g., west=170, east=-170),
# split into two ranges covering both sides of the ±180° boundary.
_TILE_QUERY_ANTIMERIDIAN = f"""\
SELECT {_TILE_COLUMNS}
FROM ais_snapshot
WHERE "date" = $snapshot_date
  AND tenant_id = $tenant_id
  AND (lon >= $west OR lon < $east)
  AND lat >= $south AND lat < $north
"""

# MVT spec default extent (4096 = standard for Mapbox/MapLibre).
DEFAULT_EXTENT = 4096

# Zoom range supported for AIS tiles.
MIN_ZOOM = 0
MAX_ZOOM = 12

# Low zooms need thinning to prevent giant global tiles.
_LOD_GRID_SIZE = {
    0: 24,
    1: 32,
    2: 48,
    3: 64,
}


@dataclass(frozen=True)
class GeneratedTile:
    """Encoded AIS tile plus lightweight rendering stats."""

    tile_bytes: bytes
    feature_count: int
    raw_feature_count: int
    thinned: bool


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


# Web Mercator (EPSG:3857) half-circumference in meters.
_HALF_CIRCUMFERENCE = 20037508.342789244


def _lon_to_mx(lon: float) -> float:
    """Convert WGS-84 longitude to Web Mercator X (meters)."""
    return lon * _HALF_CIRCUMFERENCE / 180.0


def _lat_to_my(lat: float) -> float:
    """Convert WGS-84 latitude to Web Mercator Y (meters)."""
    lat_rad = math.radians(lat)
    return math.log(math.tan(math.pi / 4 + lat_rad / 2)) * _HALF_CIRCUMFERENCE / math.pi


def _row_to_feature(
    row: tuple,
) -> dict:
    """Convert a DuckDB result row into a GeoJSON-like feature dict.

    Coordinates are projected from WGS-84 to Web Mercator (EPSG:3857) so that
    ``quantize_bounds`` (also in Mercator) correctly maps them into tile space.
    """
    imommsi, mmsi, vessel_name, sog, heading, shiptype, vessel_class, \
        dwt, destination, destinationtidied, eta, lat, lon = row

    mx = _lon_to_mx(lon)
    my = _lat_to_my(lat)

    properties: dict = {"imommsi": imommsi, "mmsi": mmsi}

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
        "geometry": f"POINT({mx} {my})",
        "properties": properties,
    }


def _mercator_bounds(
    west: float,
    south: float,
    east: float,
    north: float,
) -> tuple[float, float, float, float]:
    """Convert WGS-84 tile bounds to Web Mercator bounds."""
    return (
        _lon_to_mx(west),
        _lat_to_my(south),
        _lon_to_mx(east),
        _lat_to_my(north),
    )


def _thin_rows(
    rows: list[tuple],
    *,
    merc_bounds: tuple[float, float, float, float],
    z: int,
) -> list[tuple]:
    """Reduce low-zoom vessel density by keeping one vessel per coarse cell."""
    grid_size = _LOD_GRID_SIZE.get(z)
    if grid_size is None or len(rows) <= grid_size:
        return rows

    west, south, east, north = merc_bounds
    width = east - west
    height = north - south
    if width <= 0 or height <= 0:
        return rows

    kept: dict[tuple[int, int], tuple] = {}
    for row in rows:
        lat = float(row[-2])
        lon = float(row[-1])
        mx = _lon_to_mx(lon)
        my = _lat_to_my(lat)
        x_bucket = int((mx - west) / width * grid_size)
        y_bucket = int((my - south) / height * grid_size)
        x_bucket = max(0, min(grid_size - 1, x_bucket))
        y_bucket = max(0, min(grid_size - 1, y_bucket))
        bucket = (x_bucket, y_bucket)

        current = kept.get(bucket)
        if current is None:
            kept[bucket] = row
            continue

        # Prefer the vessel with the higher speed, then the larger DWT.
        current_sog = float(current[3] or 0.0)
        current_dwt = float(current[7] or 0.0)
        new_sog = float(row[3] or 0.0)
        new_dwt = float(row[7] or 0.0)
        if (new_sog, new_dwt) > (current_sog, current_dwt):
            kept[bucket] = row

    return list(kept.values())


def generate_tile_with_stats(
    *,
    con: duckdb.DuckDBPyConnection,
    snapshot_date: date,
    tenant_id: str,
    z: int,
    x: int,
    y: int,
    extent: int = DEFAULT_EXTENT,
) -> GeneratedTile:
    """Generate a single MVT tile plus feature-count stats."""
    west, south, east, north = tile_bounds(z, x, y)

    # At the antimeridian, west > east (e.g., tile spanning 170° to -170°).
    query = _TILE_QUERY_ANTIMERIDIAN if west > east else _TILE_QUERY

    rows = con.execute(
        query,
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
        return GeneratedTile(
            tile_bytes=b"",
            feature_count=0,
            raw_feature_count=0,
            thinned=False,
        )

    merc_bounds = _mercator_bounds(west, south, east, north)
    filtered_rows = _thin_rows(rows, merc_bounds=merc_bounds, z=z)
    features = [_row_to_feature(row) for row in filtered_rows]

    tile_bytes = mvt.encode(
        [{
            "name": "vessels",
            "features": features,
        }],
        default_options={
            "quantize_bounds": merc_bounds,
            "extents": extent,
        },
    )

    return GeneratedTile(
        tile_bytes=tile_bytes,
        feature_count=len(features),
        raw_feature_count=len(rows),
        thinned=len(filtered_rows) != len(rows),
    )


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
    return generate_tile_with_stats(
        con=con,
        snapshot_date=snapshot_date,
        tenant_id=tenant_id,
        z=z,
        x=x,
        y=y,
        extent=extent,
    ).tile_bytes
