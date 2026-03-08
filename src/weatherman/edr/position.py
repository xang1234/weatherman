"""OGC API – EDR position endpoint.

Implements ``GET /v1/edr/position`` following the OGC API – Environmental Data
Retrieval specification.  Given a POINT(lon lat) coordinate and optional
parameter/datetime filters, reads the current published Zarr store and returns
a CoverageJSON time-series response.

Query parameters (EDR spec):
    coords:           WKT POINT(lon lat) — required
    parameter-name:   Comma-separated variable names (default: all)
    datetime:         ISO 8601 interval or instant (default: all forecast hours)

Response: CoverageJSON (application/prs.coverage+json)
"""

from __future__ import annotations

import hashlib
import logging
import re

from weatherman.caching import etag_matches as _etag_matches
from typing import Annotated, Any, Callable

import numpy as np
import zarr
from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response
from fastapi.responses import JSONResponse

from weatherman.processing.geo import interpolate_at_point, wrap_longitude
from weatherman.storage.catalog import RunCatalog
from weatherman.storage.paths import RunID, StorageLayout

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1/edr", tags=["edr"])

CatalogLoader = Callable[[str], RunCatalog]
ZarrOpener = Callable[[str], zarr.Group]

# WKT POINT regex — tolerant of whitespace
_POINT_RE = re.compile(
    r"^\s*POINT\s*\(\s*"
    r"(?P<lon>[+-]?\d+(?:\.\d+)?)"
    r"\s+"
    r"(?P<lat>[+-]?\d+(?:\.\d+)?)"
    r"\s*\)\s*$",
    re.IGNORECASE,
)


def parse_wkt_point(coords: str) -> tuple[float, float]:
    """Parse a WKT POINT string into (lon, lat).

    Raises:
        ValueError: If the string is not a valid WKT POINT.
    """
    m = _POINT_RE.match(coords)
    if not m:
        raise ValueError(
            f"Invalid WKT POINT: '{coords}'. "
            f"Expected format: POINT(lon lat)"
        )
    return float(m.group("lon")), float(m.group("lat"))


def parse_datetime_filter(
    dt_param: str | None,
    available_hours: np.ndarray,
) -> np.ndarray:
    """Parse the EDR datetime parameter into forecast hour indices.

    Supports:
        None / ".."       → all hours
        "3"               → single forecast hour
        "3/12"  or "3..12" → inclusive range of forecast hours

    Returns:
        Boolean mask over *available_hours*.

    Raises:
        ValueError: If the parameter is malformed or hours are out of range.
    """
    if dt_param is None or dt_param.strip() in ("", "..", "../.."):
        return np.ones(len(available_hours), dtype=bool)

    dt_param = dt_param.strip()

    # Range: "start/end" or "start..end"
    for sep in ("/", ".."):
        if sep in dt_param:
            parts = dt_param.split(sep, maxsplit=1)
            try:
                start_h = int(parts[0])
                end_h = int(parts[1])
            except ValueError:
                raise ValueError(
                    f"Invalid datetime range: '{dt_param}'. "
                    f"Expected integer forecast hours like '0/24' or '0..24'."
                )
            return (available_hours >= start_h) & (available_hours <= end_h)

    # Single value
    try:
        hour = int(dt_param)
    except ValueError:
        raise ValueError(
            f"Invalid datetime value: '{dt_param}'. "
            f"Expected an integer forecast hour."
        )
    return available_hours == hour


def compute_etag(
    model: str,
    run_id: str,
    lon: float,
    lat: float,
    parameter_names: list[str] | None,
    datetime_filter: str | None,
) -> str:
    """Compute a deterministic ETag from the resolved query inputs.

    Since published runs are immutable, the same (run_id + query) always
    produces the same result — making this a stable cache key.
    """
    parts = [
        model,
        str(run_id),
        f"{lon:.6f}",
        f"{lat:.6f}",
        ",".join(sorted(parameter_names)) if parameter_names else "*",
        datetime_filter or "*",
    ]
    digest = hashlib.sha256("|".join(parts).encode()).hexdigest()[:16]
    return f'"{digest}"'



# _etag_matches is imported from weatherman.caching


# One year; published runs are immutable so this is safe.
_IMMUTABLE_CACHE_CONTROL = "public, max-age=31536000, immutable"
# "latest" resolves to a mutable alias — short cache + must-revalidate.
_LATEST_CACHE_CONTROL = "public, max-age=60, must-revalidate"


def _build_coverage_json(
    lon: float,
    lat: float,
    forecast_hours: list[int],
    parameters: dict[str, list[float | None]],
    variable_metadata: dict[str, dict[str, str]],
) -> dict[str, Any]:
    """Build a CoverageJSON response for a position query.

    Follows the CoverageJSON 1.0 specification for a PointSeries domain.
    """
    # Ensure all numeric values are native Python types (not numpy)
    lon = float(lon)
    lat = float(lat)
    forecast_hours = [int(h) for h in forecast_hours]

    cov_parameters: dict[str, Any] = {}
    cov_ranges: dict[str, Any] = {}

    for var_name, values in parameters.items():
        meta = variable_metadata.get(var_name, {})
        cov_parameters[var_name] = {
            "type": "Parameter",
            "observedProperty": {
                "label": {"en": meta.get("long_name", var_name)},
            },
            "unit": {
                "symbol": meta.get("units", ""),
            },
        }
        cov_ranges[var_name] = {
            "type": "NdArray",
            "dataType": "float",
            "axisNames": ["t"],
            "shape": [len(forecast_hours)],
            "values": [float(v) if v is not None else None for v in values],
        }

    return {
        "type": "Coverage",
        "domain": {
            "type": "Domain",
            "domainType": "PointSeries",
            "axes": {
                "x": {"values": [lon]},
                "y": {"values": [lat]},
                "t": {"values": forecast_hours},
            },
            "referencing": [
                {
                    "coordinates": ["x", "y"],
                    "system": {
                        "type": "GeographicCRS",
                        "id": "http://www.opengis.net/def/crs/EPSG/0/4326",
                    },
                },
                {
                    "coordinates": ["t"],
                    "system": {
                        "type": "TemporalRS",
                        "calendar": "Gregorian",
                        "description": "Forecast hours since model init time",
                    },
                },
            ],
        },
        "parameters": cov_parameters,
        "ranges": cov_ranges,
    }


# ---------------------------------------------------------------------------
# Service class
# ---------------------------------------------------------------------------


class EDRService:
    """Handles EDR position queries against published Zarr stores."""

    def __init__(
        self,
        catalog_loader: CatalogLoader,
        zarr_opener: ZarrOpener,
    ) -> None:
        self._catalog_loader = catalog_loader
        self._zarr_opener = zarr_opener

    def resolve_run_id(self, model: str, run_id_or_latest: str) -> RunID:
        """Resolve 'latest' to the current published run, or validate a literal."""
        if run_id_or_latest == "latest":
            catalog = self._catalog_loader(model)
            if catalog.current_run_id is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"No published run for model '{model}'",
                )
            return catalog.current_run_id
        try:
            return RunID(run_id_or_latest)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    def zarr_path(self, model: str, run_id: RunID) -> str:
        """Build the Zarr store path for a given model run."""
        layout = StorageLayout(model)
        return layout.zarr_path(run_id)

    def query_position(
        self,
        model: str,
        run_id: RunID,
        lon: float,
        lat: float,
        parameter_names: list[str] | None,
        datetime_filter: str | None,
    ) -> dict[str, Any]:
        """Execute a position query against the Zarr store.

        Returns a CoverageJSON dict.
        """
        zarr_store_path = self.zarr_path(model, run_id)

        try:
            root = self._zarr_opener(zarr_store_path)
        except Exception as exc:
            logger.error("Failed to open Zarr store at %s: %s", zarr_store_path, exc)
            raise HTTPException(
                status_code=404,
                detail=f"Zarr store not found for {model}/{run_id}",
            ) from exc

        lat_coords = np.asarray(root["lat"][:])
        lon_coords = np.asarray(root["lon"][:])
        time_coords = np.asarray(root["time"][:])

        # Validate latitude
        wrapped_lon = wrap_longitude(lon)
        if lat < float(lat_coords.min()) or lat > float(lat_coords.max()):
            raise HTTPException(
                status_code=400,
                detail=f"Latitude {lat} outside grid range "
                f"[{float(lat_coords.min())}, {float(lat_coords.max())}]",
            )

        # Resolve datetime filter
        try:
            time_mask = parse_datetime_filter(datetime_filter, time_coords)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        selected_hours = time_coords[time_mask]
        if len(selected_hours) == 0:
            raise HTTPException(
                status_code=400,
                detail=f"No forecast hours match datetime filter '{datetime_filter}'",
            )

        # Determine which variables to query
        available_vars = [
            name for name in root.keys()
            if name not in ("lat", "lon", "time")
        ]

        if parameter_names is not None:
            missing = [p for p in parameter_names if p not in available_vars]
            if missing:
                raise HTTPException(
                    status_code=400,
                    detail=f"Unknown parameters: {missing}. "
                    f"Available: {available_vars}",
                )
            query_vars = parameter_names
        else:
            query_vars = available_vars

        # Sample each variable at the query point for each time step
        parameters: dict[str, list[float | None]] = {}
        variable_metadata: dict[str, dict[str, str]] = {}

        time_indices = np.where(time_mask)[0]

        for var_name in query_vars:
            arr = root[var_name]
            variable_metadata[var_name] = {
                "long_name": str(arr.attrs.get("long_name", var_name)),
                "units": str(arr.attrs.get("units", "")),
            }

            values: list[float | None] = []
            for t_idx in time_indices:
                slab = np.asarray(arr[int(t_idx), :, :])
                val = interpolate_at_point(
                    slab, lat_coords, lon_coords, lat, wrapped_lon,
                )
                values.append(None if np.isnan(val) else round(val, 4))
            parameters[var_name] = values

        return _build_coverage_json(
            lon=float(wrapped_lon),
            lat=float(lat),
            forecast_hours=[int(h) for h in selected_hours],
            parameters=parameters,
            variable_metadata=variable_metadata,
        )


# ---------------------------------------------------------------------------
# Module-level service instance
# ---------------------------------------------------------------------------

_service: EDRService | None = None


def init_edr_service(
    catalog_loader: CatalogLoader,
    zarr_opener: ZarrOpener,
) -> EDRService:
    """Initialize the module-level EDRService. Call once at app startup."""
    global _service
    if _service is not None:
        raise RuntimeError(
            "EDRService already initialized — call shutdown_edr_service() first"
        )
    _service = EDRService(catalog_loader, zarr_opener)
    return _service


def shutdown_edr_service() -> None:
    """Tear down the EDRService. Call from FastAPI lifespan shutdown."""
    global _service
    _service = None


def get_edr_service() -> EDRService:
    """FastAPI dependency that returns the EDRService singleton."""
    if _service is None:
        raise RuntimeError(
            "EDRService not initialized — call init_edr_service() at startup"
        )
    return _service


# ---------------------------------------------------------------------------
# Route handler
# ---------------------------------------------------------------------------


@router.get(
    "/collections/{model}/instances/{run_id}/position",
    summary="EDR position query — point forecast time-series",
)
async def edr_position(
    model: str,
    run_id: str,
    coords: Annotated[
        str,
        Query(description="WKT POINT(lon lat)"),
    ],
    parameter_name: Annotated[
        str | None,
        Query(
            alias="parameter-name",
            description="Comma-separated variable names",
        ),
    ] = None,
    datetime_filter: Annotated[
        str | None,
        Query(
            alias="datetime",
            description="Forecast hour or range (e.g. '0', '0/24')",
        ),
    ] = None,
    if_none_match: Annotated[
        str | None,
        Header(description="ETag for conditional request"),
    ] = None,
    svc: EDRService = Depends(get_edr_service),
) -> Response:
    """Return a CoverageJSON time-series at the given point.

    Supports 'latest' as run_id to resolve to the current published run.
    Responses include ETag and Cache-Control headers for HTTP caching.
    """
    try:
        lon, lat = parse_wkt_point(coords)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    resolved_run_id = svc.resolve_run_id(model, run_id)
    wrapped_lon = wrap_longitude(lon)

    param_list: list[str] | None = None
    if parameter_name:
        param_list = [p.strip() for p in parameter_name.split(",") if p.strip()]

    etag = compute_etag(
        model=model,
        run_id=str(resolved_run_id),
        lon=wrapped_lon,
        lat=lat,
        parameter_names=param_list,
        datetime_filter=datetime_filter,
    )

    cache_control = (
        _LATEST_CACHE_CONTROL if run_id == "latest" else _IMMUTABLE_CACHE_CONTROL
    )

    # 304 Not Modified if the client already has this exact response
    if if_none_match and _etag_matches(if_none_match, etag):
        return Response(
            status_code=304,
            headers={"ETag": etag, "Cache-Control": cache_control},
        )

    result = svc.query_position(
        model=model,
        run_id=resolved_run_id,
        lon=lon,
        lat=lat,
        parameter_names=param_list,
        datetime_filter=datetime_filter,
    )

    return JSONResponse(
        content=result,
        media_type="application/prs.coverage+json",
        headers={
            "ETag": etag,
            "Cache-Control": cache_control,
        },
    )
