"""AIS vector tile endpoint — serves MVT tiles from the daily snapshot.

URL patterns::

    /ais/tiles/{snapshot_date}/{z}/{x}/{y}.pbf   — single MVT tile
    /ais/tiles/{snapshot_date}/tilejson.json      — TileJSON metadata

Tiles are date-keyed and immutable: once a day's snapshot is built, its tiles
never change.  This enables aggressive ``Cache-Control: immutable`` headers
for CDN and browser caching.

The ``vessels`` layer in each tile contains point features with properties
suitable for rendering directional arrows color-coded by ship type.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Annotated, Optional

import duckdb
from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, Response
from fastapi.responses import JSONResponse

from weatherman.ais.mvt import MAX_ZOOM, MIN_ZOOM, generate_tile
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
    ) -> bytes:
        """Generate an MVT tile for the given parameters."""
        return generate_tile(
            con=self.connection,
            snapshot_date=snapshot_date,
            tenant_id=tenant_id,
            z=z,
            x=x,
            y=y,
        )


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
    "/{snapshot_date}/{z}/{x}/{y}.pbf",
    summary="Get an AIS vector tile",
    response_class=Response,
)
async def get_ais_tile(
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

    tile_bytes = svc.get_tile(
        snapshot_date=snapshot_date,
        tenant_id=tenant_id,
        z=z,
        x=x,
        y=y,
    )

    if not tile_bytes:
        # Empty tile — return 204 with cache headers so clients don't re-request.
        return Response(
            status_code=204,
            headers={"Cache-Control": CACHE_IMMUTABLE},
        )

    return Response(
        content=tile_bytes,
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
