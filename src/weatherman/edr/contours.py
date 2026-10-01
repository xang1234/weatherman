"""Isobar endpoint: mean-sea-level pressure contours as GeoJSON.

Contoured on request from the run's Zarr store and cached per run and hour.
A published run never changes, so responses are cacheable for a long time.
"""

from __future__ import annotations

import gzip
import json
import logging
from functools import lru_cache

import numpy as np
from fastapi import APIRouter, Depends, Header, HTTPException, Response

from weatherman.edr.position import EDRService, get_edr_service
from weatherman.processing.contours import isobars_geojson

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/contours", tags=["contours"])


@lru_cache(maxsize=64)
def _isobars_json(model: str, run_id: str, forecast_hour: int) -> bytes:
    """Contour one forecast hour (about 0.2 s) and keep the encoded result."""
    svc = get_edr_service()
    root = svc.open_zarr_store(model, svc.resolve_run_id(model, run_id))
    if "prmsl" not in root:
        raise HTTPException(status_code=404, detail=f"No pressure field in {model}/{run_id}")
    times = np.asarray(root["time"][:]).tolist()
    if forecast_hour not in times:
        raise HTTPException(status_code=404, detail=f"Forecast hour {forecast_hour} not in {model}/{run_id}")
    field = np.asarray(root["prmsl"][times.index(forecast_hour)])
    if np.isnan(field).all():
        raise HTTPException(status_code=404, detail=f"No pressure data for hour {forecast_hour}")
    geojson = isobars_geojson(field, np.asarray(root["lat"][:]), np.asarray(root["lon"][:]))
    return json.dumps(geojson, separators=(",", ":")).encode()


@router.get(
    "/{model}/{run_id}/prmsl/{forecast_hour}",
    summary="Isobars and H/L centres for one forecast hour (GeoJSON)",
)
def isobars(
    model: str,
    run_id: str,
    forecast_hour: int,
    accept_encoding: str | None = Header(default=None),
    svc: EDRService = Depends(get_edr_service),
) -> Response:
    # Sync on purpose: contouring is CPU work, so FastAPI runs it in its
    # threadpool rather than on the event loop.
    if run_id == "latest":
        # Not cached under "latest": the current run changes.
        run_id = str(svc.resolve_run_id(model, run_id))
    body = _isobars_json(model, run_id, forecast_hour)
    headers = {"Cache-Control": "public, max-age=86400, immutable", "Vary": "Accept-Encoding"}
    if accept_encoding and "gzip" in accept_encoding:
        return Response(gzip.compress(body), media_type="application/geo+json",
                        headers={**headers, "Content-Encoding": "gzip"})
    return Response(body, media_type="application/geo+json", headers=headers)
