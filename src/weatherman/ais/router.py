"""AIS endpoints — vector tiles, bbox queries, and track playback.

URL patterns::

    /ais/tiles/{snapshot_date}/{z}/{x}/{y}.pbf   — single MVT tile
    /ais/tiles/{snapshot_date}/tilejson.json      — TileJSON metadata
    /ais/tiles/latest                             — latest snapshot date
    /ais/bbox                                     — vessels in bounding box (GeoJSON)
    /ais/tracks/{mmsi}                            — vessel track (GeoJSON LineString)

Tiles are date-keyed and immutable: once a day's snapshot is built, its tiles
never change.  This enables aggressive ``Cache-Control: immutable`` headers
for CDN and browser caching.

The ``vessels`` layer in each tile contains point features with properties
suitable for rendering directional arrows color-coded by ship type.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Annotated, Optional

import duckdb
from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, Response
from fastapi.responses import JSONResponse

from weatherman.ais.mvt import MAX_ZOOM, MIN_ZOOM, GeneratedTile, generate_tile_with_stats
from weatherman.ais.tracks import query_track
from weatherman.observability.metrics import AIS_TILE_BYTES, AIS_TILE_FEATURES
from weatherman.tenancy import get_tenant_id

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ais/tiles", tags=["ais-tiles"])

CACHE_IMMUTABLE = "public, max-age=31536000, immutable"
CONTENT_TYPE_MVT = "application/vnd.mapbox-vector-tile"


class AISTileService:
    """Serves MVT tiles from the AIS snapshot table.

    Holds a read-only DuckDB connection for concurrent tile queries.
    """

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._con: duckdb.DuckDBPyConnection | None = None

    def connect(self) -> None:
        """Open a read-only DuckDB connection."""
        self._con = duckdb.connect(self._db_path, read_only=True)

    def close(self) -> None:
        """Close the DuckDB connection."""
        if self._con is not None:
            self._con.close()
            self._con = None

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        if self._con is None:
            raise RuntimeError("AISTileService not connected")
        return self._con

    def get_tile(
        self,
        *,
        snapshot_date: date,
        tenant_id: str,
        z: int,
        x: int,
        y: int,
    ) -> GeneratedTile:
        """Generate an MVT tile for the given parameters."""
        return generate_tile_with_stats(
            con=self.connection,
            snapshot_date=snapshot_date,
            tenant_id=tenant_id,
            z=z,
            x=x,
            y=y,
        )

    def latest_snapshot_date(self) -> date | None:
        """Return the newest available AIS snapshot date."""
        row = self.connection.execute(
            'SELECT MAX("date") FROM ais_snapshot'
        ).fetchone()
        if row is None or row[0] is None:
            return None
        return row[0]


# Module-level singleton
_service: Optional[AISTileService] = None


def init_ais_tile_service(db_path: str) -> AISTileService | None:
    """Initialize the AIS tile service. Call once at app startup.

    Returns None if the database file does not exist (AIS not yet ingested).
    """
    global _service
    if _service is not None:
        raise RuntimeError(
            "AISTileService already initialized — call shutdown_ais_tile_service() first"
        )

    import os

    if not os.path.exists(db_path):
        logger.info(
            "AIS database not found, tile service disabled",
            extra={"db_path": db_path},
        )
        return None

    _service = AISTileService(db_path)
    _service.connect()
    logger.info("AIS tile service started", extra={"db_path": db_path})
    return _service


def shutdown_ais_tile_service() -> None:
    """Shut down the AIS tile service. Call at app shutdown."""
    global _service
    if _service is not None:
        _service.close()
        _service = None
        logger.info("AIS tile service stopped")


def get_ais_tile_service() -> AISTileService:
    """FastAPI dependency that returns the AISTileService singleton."""
    if _service is None:
        raise HTTPException(
            status_code=503,
            detail="AIS tile service not available (database not ingested yet)",
        )
    return _service


# -- Route handlers --


@router.get(
    "/latest",
    summary="Latest available AIS snapshot date",
)
async def get_latest_snapshot_date(
    svc: AISTileService = Depends(get_ais_tile_service),
) -> JSONResponse:
    """Return the latest available AIS snapshot date for frontend bootstrap."""
    snapshot_date = svc.latest_snapshot_date()
    if snapshot_date is None:
        raise HTTPException(status_code=404, detail="No AIS snapshots available")
    return JSONResponse(
        content={"snapshot_date": snapshot_date.isoformat()},
        headers={"Cache-Control": "public, max-age=60, must-revalidate"},
    )


@router.get(
    "/{snapshot_date}/{z}/{x}/{y}.pbf",
    summary="Get an AIS vector tile",
    response_class=Response,
)
def get_ais_tile(
    snapshot_date: date,
    z: Annotated[int, Path(ge=MIN_ZOOM, le=MAX_ZOOM)],
    x: Annotated[int, Path(ge=0)],
    y: Annotated[int, Path(ge=0)],
    tenant_id: str = Depends(get_tenant_id),
    svc: AISTileService = Depends(get_ais_tile_service),
) -> Response:
    """Serve an MVT tile of vessel positions for the given date.

    The ``vessels`` layer contains point features with properties:
    mmsi, vessel_name, sog, heading, shiptype, vessel_class, dwt,
    destination, destinationtidied, eta.

    Tiles are immutable — the same date always returns the same data.
    """
    # Validate tile x/y against zoom level
    max_tile = 2**z - 1
    if x > max_tile or y > max_tile:
        raise HTTPException(
            status_code=400,
            detail=f"Tile {z}/{x}/{y} out of range (max {max_tile} at zoom {z})",
        )

    generated = svc.get_tile(
        snapshot_date=snapshot_date,
        tenant_id=tenant_id,
        z=z,
        x=x,
        y=y,
    )
    zoom_label = str(z)
    thinned_label = "true" if generated.thinned else "false"
    AIS_TILE_FEATURES.labels(zoom=zoom_label, thinned=thinned_label).observe(
        generated.feature_count
    )
    AIS_TILE_BYTES.labels(zoom=zoom_label, thinned=thinned_label).observe(
        len(generated.tile_bytes)
    )

    if not generated.tile_bytes:
        # Empty tile — return 204 with cache headers so clients don't re-request.
        return Response(
            status_code=204,
            headers={"Cache-Control": CACHE_IMMUTABLE},
        )

    return Response(
        content=generated.tile_bytes,
        media_type=CONTENT_TYPE_MVT,
        headers={"Cache-Control": CACHE_IMMUTABLE},
    )


@router.get(
    "/{snapshot_date}/tilejson.json",
    summary="TileJSON metadata for AIS tiles",
)
async def get_ais_tilejson(
    request: Request,
    snapshot_date: date,
) -> JSONResponse:
    """Return a TileJSON document for MapLibre integration.

    The tile URL template uses ``.pbf`` extension and includes the
    ``{z}/{x}/{y}`` placeholders expected by MapLibre GL JS.
    """
    base = str(request.base_url).rstrip("/")
    tile_url = f"{base}/ais/tiles/{snapshot_date}/{{z}}/{{x}}/{{y}}.pbf"

    data = {
        "tilejson": "3.0.0",
        "name": "ais-vessels",
        "description": f"AIS vessel positions for {snapshot_date}",
        "tiles": [tile_url],
        "minzoom": MIN_ZOOM,
        "maxzoom": MAX_ZOOM,
        "bounds": [-180, -90, 180, 90],
        "vector_layers": [
            {
                "id": "vessels",
                "description": "Vessel positions from AIS snapshot",
                "fields": {
                    "mmsi": "Number — Maritime Mobile Service Identity",
                    "vessel_name": "String — Vessel name",
                    "sog": "Number — Speed over ground (knots)",
                    "heading": "Number — Heading (degrees)",
                    "shiptype": "String — Ship type (Cargo, Tanker, etc.)",
                    "vessel_class": "String — Vessel class (Capesize, etc.)",
                    "dwt": "Number — Deadweight tonnage",
                    "destination": "String — Reported destination",
                    "destinationtidied": "String — Cleaned destination",
                    "eta": "String — Estimated time of arrival",
                },
            }
        ],
    }

    return JSONResponse(
        content=data,
        headers={"Cache-Control": CACHE_IMMUTABLE},
    )


# ── AIS feature query router ──────────────────────────────────────────

query_router = APIRouter(prefix="/ais", tags=["ais-queries"])


@query_router.get(
    "/bbox",
    summary="Query vessels within a bounding box",
)
def get_vessels_bbox(
    west: Annotated[float, Query(ge=-180, le=180)],
    south: Annotated[float, Query(ge=-90, le=90)],
    east: Annotated[float, Query(ge=-180, le=180)],
    north: Annotated[float, Query(ge=-90, le=90)],
    snapshot_date: date | None = None,
    limit: Annotated[int, Query(ge=1, le=5000)] = 1000,
    tenant_id: str = Depends(get_tenant_id),
    svc: AISTileService = Depends(get_ais_tile_service),
) -> JSONResponse:
    """Return GeoJSON FeatureCollection of vessels in the bounding box.

    If ``snapshot_date`` is omitted, uses the latest available date.
    Results are capped at ``limit`` (default 1000, max 5000).
    """
    if snapshot_date is None:
        snapshot_date = svc.latest_snapshot_date()
        if snapshot_date is None:
            raise HTTPException(status_code=404, detail="No AIS snapshots available")

    con = svc.connection
    rows = con.execute(
        """
        SELECT mmsi, vessel_name, lat, lon, sog, heading,
               shiptype, vessel_class, dwt, destination, destinationtidied, eta
        FROM ais_snapshot
        WHERE "date" = $snapshot_date
          AND tenant_id = $tenant_id
          AND lon BETWEEN $west AND $east
          AND lat BETWEEN $south AND $north
        LIMIT $limit
        """,
        {
            "snapshot_date": snapshot_date,
            "tenant_id": tenant_id,
            "west": west,
            "south": south,
            "east": east,
            "north": north,
            "limit": limit,
        },
    ).fetchall()

    features = []
    for row in rows:
        mmsi, name, lat, lon, sog, heading, shiptype, vessel_class, dwt, dest, dest_tidy, eta = row
        props: dict = {"mmsi": mmsi}
        if name:
            props["vessel_name"] = name
        if sog is not None:
            props["sog"] = sog
        if heading is not None:
            props["heading"] = heading
        if shiptype:
            props["shiptype"] = shiptype
        if vessel_class:
            props["vessel_class"] = vessel_class
        if dwt is not None:
            props["dwt"] = dwt
        if dest:
            props["destination"] = dest
        if dest_tidy:
            props["destinationtidied"] = dest_tidy
        if eta is not None:
            props["eta"] = str(eta)

        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [lon, lat],
            },
            "properties": props,
        })

    return JSONResponse(
        content={
            "type": "FeatureCollection",
            "features": features,
        },
        headers={"Cache-Control": "public, max-age=60"},
    )


@query_router.get(
    "/tracks/{mmsi}",
    summary="Get vessel track as GeoJSON LineString",
)
def get_vessel_track(
    mmsi: int,
    start_date: date | None = None,
    end_date: date | None = None,
    tenant_id: str = Depends(get_tenant_id),
    svc: AISTileService = Depends(get_ais_tile_service),
) -> JSONResponse:
    """Return a GeoJSON Feature with a LineString geometry for the vessel's track.

    Defaults to the last 7 days if no date range is specified.
    Properties include timestamps and SOG for each point.
    """
    if end_date is None:
        latest = svc.latest_snapshot_date()
        end_date = latest if latest else date.today()
    if start_date is None:
        start_date = end_date - timedelta(days=7)

    con = svc.connection
    points = query_track(
        mmsi=mmsi,
        start_date=start_date,
        end_date=end_date,
        tenant_id=tenant_id,
        con=con,
    )

    if not points:
        raise HTTPException(
            status_code=404,
            detail=f"No track data found for MMSI {mmsi} between {start_date} and {end_date}",
        )

    coordinates = [[p.lon, p.lat] for p in points]
    timestamps = [p.timestamp.isoformat() for p in points]
    sog_values = [p.sog for p in points]

    return JSONResponse(
        content={
            "type": "Feature",
            "geometry": {
                "type": "LineString",
                "coordinates": coordinates,
            },
            "properties": {
                "mmsi": mmsi,
                "timestamps": timestamps,
                "sog": sog_values,
                "point_count": len(points),
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
            },
        },
        headers={"Cache-Control": "public, max-age=300"},
    )
