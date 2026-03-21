"""EDR trajectory endpoint — weather sampling along a route.

Accepts a GeoJSON LineString, resamples it into equidistant points,
and returns bilinear-interpolated weather data for every sample point
across all forecast hours.

    POST /v1/edr/collections/{model}/instances/{run_id}/trajectory
"""

from __future__ import annotations

import hashlib
import logging
import time
from typing import Annotated, Any

import numpy as np
from fastapi import APIRouter, Body, Depends, Header, HTTPException, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from weatherman.caching import etag_matches as _etag_matches
from weatherman.edr.position import (
    EDRService,
    _IMMUTABLE_CACHE_CONTROL,
    _LATEST_CACHE_CONTROL,
    _build_interpolation_plan,
    get_edr_service,
)
from weatherman.edr.resample import SamplePoint, resample_linestring
from weatherman.processing.geo import wrap_longitude
from weatherman.storage.paths import RunID

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1/edr", tags=["edr"])


class TrajectoryRequest(BaseModel):
    """GeoJSON-like body for trajectory queries."""

    type: str = Field("LineString", description="Must be 'LineString'")
    coordinates: list[list[float]] = Field(
        ..., description="Array of [lon, lat] pairs", min_length=2,
    )
    num_samples: int = Field(
        default=20, ge=2, le=200,
        description="Number of equidistant sample points along the route",
    )
    speed_knots: float | None = Field(
        default=None, gt=0,
        description="Vessel speed for ETA calculation (knots)",
    )


def _compute_trajectory_etag(
    model: str,
    run_id: str,
    coordinates: list[list[float]],
    num_samples: int,
) -> str:
    parts = [
        model,
        run_id,
        str(num_samples),
        "|".join(f"{c[0]:.6f},{c[1]:.6f}" for c in coordinates),
    ]
    digest = hashlib.sha256("|".join(parts).encode()).hexdigest()[:16]
    return f'"{digest}"'


def _sample_trajectory(
    svc: EDRService,
    model: str,
    run_id: RunID,
    samples: list[SamplePoint],
) -> dict[str, Any]:
    """Sample all variables at all forecast hours for each route point."""
    root = svc.open_zarr_store(model, run_id)

    lat_coords = np.asarray(root["lat"][:])
    lon_coords = np.asarray(root["lon"][:])
    time_coords = np.asarray(root["time"][:])

    available_vars = [
        name for name in root.keys() if name not in ("lat", "lon", "time")
    ]

    plans = []
    for sp in samples:
        wrapped_lon = wrap_longitude(sp.lon)
        plan = _build_interpolation_plan(lat_coords, lon_coords, sp.lat, wrapped_lon)
        plans.append(plan)

    n_times = len(time_coords)
    n_samples = len(samples)

    # Compute spatial bounding box of all sample grid indices for slab reads.
    # This reads one small slab per time step instead of per-column reads,
    # reducing chunk I/O from O(samples × times) to O(times).
    all_j = [p.j0 for p in plans] + [p.j1 for p in plans]
    all_i = [p.i0 for p in plans] + [p.i1 for p in plans]
    j_min, j_max = min(all_j), max(all_j)
    i_min, i_max = min(all_i), max(all_i)

    # Detect antimeridian crossing (i indices wrap around the array)
    i_span = i_max - i_min
    n_lon = len(lon_coords)
    use_slab = i_span < n_lon // 2  # if span > half the globe, fall back to column reads

    ranges: dict[str, Any] = {}
    parameters: dict[str, Any] = {}

    for var_name in available_vars:
        arr = root[var_name]
        meta = {
            "long_name": str(arr.attrs.get("long_name", var_name)),
            "units": str(arr.attrs.get("units", "")),
        }
        parameters[var_name] = {
            "type": "Parameter",
            "observedProperty": {"label": {"en": meta["long_name"]}},
            "unit": {"symbol": meta["units"]},
        }

        values = np.full((n_samples, n_times), np.nan, dtype=np.float32)

        if use_slab:
            # Single read: all time steps × spatial bounding box (~few MB)
            cube = np.asarray(arr[:, j_min:j_max + 1, i_min:i_max + 1])
            for s_idx, plan in enumerate(plans):
                j0r, j1r = plan.j0 - j_min, plan.j1 - j_min
                i0r, i1r = plan.i0 - i_min, plan.i1 - i_min
                blended = (
                    cube[:, j0r, i0r] * plan.w00
                    + cube[:, j0r, i1r] * plan.w01
                    + cube[:, j1r, i0r] * plan.w10
                    + cube[:, j1r, i1r] * plan.w11
                )
                values[s_idx, :] = blended
        else:
            for s_idx, plan in enumerate(plans):
                v00 = np.asarray(arr[:, plan.j0, plan.i0])
                v01 = np.asarray(arr[:, plan.j0, plan.i1])
                v10 = np.asarray(arr[:, plan.j1, plan.i0])
                v11 = np.asarray(arr[:, plan.j1, plan.i1])
                values[s_idx, :] = (
                    v00 * plan.w00 + v01 * plan.w01
                    + v10 * plan.w10 + v11 * plan.w11
                )

        values_list = [
            [None if np.isnan(v) else round(float(v), 4) for v in row]
            for row in values
        ]

        ranges[var_name] = {
            "type": "NdArray",
            "dataType": "float",
            "axisNames": ["composite", "t"],
            "shape": [n_samples, n_times],
            "values": values_list,
        }

    return {
        "type": "Coverage",
        "domain": {
            "type": "Domain",
            "domainType": "Trajectory",
            "axes": {
                "composite": {
                    "dataType": "tuple",
                    "coordinates": ["x", "y"],
                    "values": [
                        [round(sp.lon, 6), round(sp.lat, 6)] for sp in samples
                    ],
                },
                "t": {
                    "values": [int(h) for h in time_coords],
                },
            },
            "referencing": [
                {
                    "coordinates": ["x", "y"],
                    "system": {"type": "GeographicCRS", "id": "EPSG:4326"},
                },
            ],
        },
        "parameters": parameters,
        "ranges": ranges,
        "route": {
            "distances_nm": [round(sp.distance_nm, 1) for sp in samples],
            "total_nm": round(samples[-1].distance_nm, 1) if samples else 0,
        },
    }


@router.post(
    "/collections/{model}/instances/{run_id}/trajectory",
    summary="EDR trajectory query — weather along a route",
)
def edr_trajectory(
    model: str,
    run_id: str,
    body: TrajectoryRequest = Body(...),
    if_none_match: Annotated[str | None, Header()] = None,
    svc: EDRService = Depends(get_edr_service),
) -> Response:
    """Sample weather data along a route for all forecast hours.

    Returns a CoverageJSON-like response with 2D arrays:
    ``ranges[variable].shape = [num_samples, num_forecast_hours]``.
    """
    if body.type != "LineString":
        raise HTTPException(status_code=400, detail=f"Expected type 'LineString', got '{body.type}'")

    is_latest = run_id == "latest"
    resolved = svc.resolve_run_id(model, run_id)

    coords = [(c[0], c[1]) for c in body.coordinates]

    etag = _compute_trajectory_etag(model, str(resolved), body.coordinates, body.num_samples)
    if if_none_match and _etag_matches(if_none_match, etag):
        cache = _LATEST_CACHE_CONTROL if is_latest else _IMMUTABLE_CACHE_CONTROL
        return Response(status_code=304, headers={"ETag": etag, "Cache-Control": cache})

    started = time.monotonic()
    samples = resample_linestring(coords, num_samples=body.num_samples)
    result = _sample_trajectory(svc, model, resolved, samples)

    if body.speed_knots:
        result["route"]["speed_knots"] = body.speed_knots
        result["route"]["eta_hours"] = [
            round(sp.distance_nm / body.speed_knots, 2) for sp in samples
        ]

    elapsed = time.monotonic() - started
    logger.info(
        "Trajectory query: %s/%s, %d samples, %.1fms",
        model, resolved, body.num_samples, elapsed * 1000,
    )

    cache = _LATEST_CACHE_CONTROL if is_latest else _IMMUTABLE_CACHE_CONTROL
    return JSONResponse(
        content=result,
        headers={"ETag": etag, "Cache-Control": cache},
    )
