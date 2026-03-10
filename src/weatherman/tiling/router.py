"""XYZ / OGC Tiles API endpoint.

Exposes tile endpoints that proxy to TiTiler, translating clean URLs into
the COG path + colormap + rescale parameters that TiTiler expects.

URL patterns:
    /tiles/{model}/{run_id}/{layer}/{forecast_hour}/{z}/{x}/{y}.png
    /tiles/{model}/latest/{layer}/{forecast_hour}/{z}/{x}/{y}.png
    /tiles/{model}/{run_id}/tilejson.json  (per-layer via ?layer=...)
"""

from typing import Annotated, Callable, Optional
from urllib.parse import quote, urlencode

import httpx
import rasterio
import rasterio.io
from fastapi import APIRouter, Depends, HTTPException, Path, Query, Response
from fastapi.responses import JSONResponse

from weatherman.storage.catalog import RunCatalog
from weatherman.storage.config import StorageConfig
from weatherman.storage.paths import RunID, StorageLayout
from weatherman.tiling.colormaps import COLORMAPS, get_colormap, get_value_range
from weatherman.tiling.data_encoder import encode_float_to_rgba, rgba_to_png_bytes

router = APIRouter(prefix="/tiles", tags=["tiles"])

# The app provides a function that loads a RunCatalog for a given model name.
CatalogLoader = Callable[[str], RunCatalog]


class TileService:
    """Resolves tile requests to TiTiler proxy calls.

    Holds references to the storage config, catalog loader, and TiTiler
    base URL so that route handlers stay thin.
    """

    def __init__(
        self,
        storage: StorageConfig,
        titiler_base_url: str,
        catalog_loader: CatalogLoader,
        *,
        cog_root: str | None = None,
    ) -> None:
        self._storage = storage
        self._titiler_url = titiler_base_url.rstrip("/")
        self._catalog_loader = catalog_loader
        self._cog_root = cog_root
        self._client = httpx.AsyncClient(timeout=30.0)

    async def close(self) -> None:
        await self._client.aclose()

    def resolve_run_id(self, model: str, run_id_or_latest: str) -> RunID:
        """Resolve 'latest' to the current published run_id, or validate a literal."""
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

    def cog_s3_uri(self, model: str, run_id: RunID, layer: str, forecast_hour: int) -> str:
        """Build the URI for a COG asset.

        Returns a local file path when ``cog_root`` is set (local dev with
        TiTiler reading from a Docker volume mount), or an S3 URI otherwise.
        """
        try:
            layout = StorageLayout(model)
            relative = layout.cog_path(run_id, layer, forecast_hour)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        if self._cog_root:
            return f"{self._cog_root}/{relative}"
        return self._storage.s3_uri(relative)

    # Cache headers: published run tiles are immutable; 'latest' alias gets short TTL
    CACHE_IMMUTABLE = "public, max-age=31536000, immutable"
    CACHE_LATEST = "public, max-age=60"

    async def fetch_tile(
        self,
        cog_uri: str,
        layer: str,
        z: int,
        x: int,
        y: int,
        *,
        is_latest: bool = False,
    ) -> Response:
        """Proxy a tile request to TiTiler."""
        try:
            colormap = get_colormap(layer)
        except KeyError:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown layer '{layer}'. Available: {list(COLORMAPS.keys())}",
            )

        params: dict[str, str] = {
            "url": cog_uri,
            "rescale": colormap.rescale_range(),
            "colormap": colormap.to_json(),
            "resampling": "bilinear",
        }

        titiler_path = f"{self._titiler_url}/cog/tiles/WebMercatorQuad/{z}/{x}/{y}.png"

        try:
            resp = await self._client.get(titiler_path, params=params)
        except httpx.TimeoutException:
            raise HTTPException(status_code=504, detail="TiTiler request timed out")
        except httpx.RequestError as exc:
            raise HTTPException(
                status_code=502, detail=f"TiTiler unreachable: {exc}"
            ) from exc

        if resp.status_code == 404:
            raise HTTPException(status_code=404, detail="Tile not found")
        if resp.status_code != 200:
            raise HTTPException(
                status_code=502,
                detail=f"TiTiler returned {resp.status_code}",
            )

        cache_control = self.CACHE_LATEST if is_latest else self.CACHE_IMMUTABLE
        return Response(
            content=resp.content,
            media_type="image/png",
            headers={"Cache-Control": cache_control},
        )

    async def fetch_data_tile(
        self,
        cog_uri: str,
        layer: str,
        z: int,
        x: int,
        y: int,
        *,
        is_latest: bool = False,
    ) -> Response:
        """Fetch raw float data from TiTiler and return as encoded RGBA PNG.

        Instead of a colorized tile, this returns float values packed into
        R (low byte) and G (high byte) channels for GPU-side decoding.
        The B channel flags nodata pixels (0xFF = nodata).
        """
        try:
            vmin, vmax = get_value_range(layer)
        except KeyError:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown layer '{layer}'. Available: {list(COLORMAPS.keys())}",
            )

        # Request raw float32 data from TiTiler as numpy-compatible format
        params: dict[str, str] = {
            "url": cog_uri,
            "resampling": "bilinear",
        }

        titiler_path = (
            f"{self._titiler_url}/cog/tiles/WebMercatorQuad/{z}/{x}/{y}"
            f"@1x.tif"
        )

        try:
            resp = await self._client.get(titiler_path, params=params)
        except httpx.TimeoutException:
            raise HTTPException(status_code=504, detail="TiTiler request timed out")
        except httpx.RequestError as exc:
            raise HTTPException(
                status_code=502, detail=f"TiTiler unreachable: {exc}"
            ) from exc

        if resp.status_code == 404:
            raise HTTPException(status_code=404, detail="Tile not found")
        if resp.status_code != 200:
            raise HTTPException(
                status_code=502,
                detail=f"TiTiler returned {resp.status_code}",
            )

        # Parse the GeoTIFF response with rasterio (handles GDAL GeoTIFFs properly)
        try:
            with rasterio.io.MemoryFile(resp.content) as memfile:
                with memfile.open() as dataset:
                    data = dataset.read(1).astype("float32")
                    nodata = dataset.nodata
        except Exception as exc:
            raise HTTPException(
                status_code=502,
                detail=f"Failed to decode TiTiler response: {exc}",
            ) from exc

        # Encode to RGBA PNG, passing nodata sentinel from the GeoTIFF metadata
        rgba = encode_float_to_rgba(data, vmin, vmax, nodata=nodata)
        png_bytes = rgba_to_png_bytes(rgba)

        cache_control = self.CACHE_LATEST if is_latest else self.CACHE_IMMUTABLE
        return Response(
            content=png_bytes,
            media_type="image/png",
            headers={
                "Cache-Control": cache_control,
                "X-Value-Range": f"{vmin},{vmax}",
            },
        )

    def build_tilejson(
        self,
        layer: str,
        cog_uri: str,
    ) -> dict:
        """Build a TileJSON response with TiTiler-native tile URLs.

        Returns tile URL templates that point directly to TiTiler's
        ``/cog/tiles`` endpoint with colormap and rescale baked into
        the query string.  The browser fetches tiles from TiTiler
        without proxying through Weatherman.
        """
        try:
            colormap = get_colormap(layer)
        except KeyError:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown layer '{layer}'. Available: {list(COLORMAPS.keys())}",
            )

        # urlencode for simple params; quote the colormap JSON separately
        # to avoid double-encoding (the URL goes into TileJSON as-is and
        # MapLibre sends it verbatim — no extra decode step).
        qs = urlencode({
            "url": cog_uri,
            "rescale": colormap.rescale_range(),
            "resampling": "bilinear",
        })
        cmap_encoded = quote(colormap.to_json(), safe="")
        tile_url = (
            f"/cog/tiles/WebMercatorQuad/{{z}}/{{x}}/{{y}}.png"
            f"?{qs}&colormap={cmap_encoded}"
        )

        return {
            "tilejson": "3.0.0",
            "name": layer,
            "tiles": [tile_url],
            "minzoom": 0,
            "maxzoom": 8,
            "bounds": [-180, -90, 180, 90],
        }


# Module-level service instance, set by init_tile_service()
_service: Optional[TileService] = None


async def shutdown_tile_service() -> None:
    """Close the TileService's HTTP client. Call from FastAPI lifespan shutdown."""
    global _service
    if _service is not None:
        await _service.close()
        _service = None


def init_tile_service(
    storage: StorageConfig,
    titiler_base_url: str,
    catalog_loader: CatalogLoader,
    *,
    cog_root: str | None = None,
) -> TileService:
    """Initialize the module-level TileService. Call once at app startup."""
    global _service
    if _service is not None:
        raise RuntimeError("TileService already initialized — call shutdown_tile_service() first")
    _service = TileService(storage, titiler_base_url, catalog_loader, cog_root=cog_root)
    return _service


def get_tile_service() -> TileService:
    """FastAPI dependency that returns the TileService singleton."""
    if _service is None:
        raise RuntimeError("TileService not initialized — call init_tile_service() at startup")
    return _service


# -- Route handlers --


@router.get(
    "/{model}/{run_id}/{layer}/{forecast_hour}/{z}/{x}/{y}.png",
    summary="Get a map tile",
    response_class=Response,
)
async def get_tile(
    model: str,
    run_id: str,
    layer: str,
    forecast_hour: Annotated[int, Path(ge=0)],
    z: int,
    x: int,
    y: int,
    svc: TileService = Depends(get_tile_service),
) -> Response:
    """Serve a PNG tile for the given model/run/layer/forecast hour.

    Supports 'latest' as run_id to resolve to the current published run.
    """
    is_latest = run_id == "latest"
    resolved_run_id = svc.resolve_run_id(model, run_id)
    cog_uri = svc.cog_s3_uri(model, resolved_run_id, layer, forecast_hour)
    return await svc.fetch_tile(cog_uri, layer, z, x, y, is_latest=is_latest)


@router.get(
    "/{model}/{run_id}/{layer}/{forecast_hour}/data/{z}/{x}/{y}.png",
    summary="Get a data-encoded tile for GPU rendering",
    response_class=Response,
)
async def get_data_tile(
    model: str,
    run_id: str,
    layer: str,
    forecast_hour: Annotated[int, Path(ge=0)],
    z: int,
    x: int,
    y: int,
    svc: TileService = Depends(get_tile_service),
) -> Response:
    """Serve a data-encoded RGBA PNG for GPU-side decoding.

    Float values are packed into R (low byte) + G (high byte) as a 16-bit
    unsigned integer. B channel flags nodata (0xFF). A is always 0xFF.

    The X-Value-Range response header contains 'min,max' for client decode.
    Supports 'latest' as run_id.
    """
    is_latest = run_id == "latest"
    resolved_run_id = svc.resolve_run_id(model, run_id)
    cog_uri = svc.cog_s3_uri(model, resolved_run_id, layer, forecast_hour)
    return await svc.fetch_data_tile(cog_uri, layer, z, x, y, is_latest=is_latest)


@router.get(
    "/{model}/{run_id}/tilejson.json",
    summary="TileJSON metadata for MapLibre",
)
async def get_tilejson(
    model: str,
    run_id: str,
    layer: Annotated[str, Query(description="Weather layer name")] = "temperature",
    forecast_hour: Annotated[int, Query(description="Forecast hour", ge=0)] = 0,
    svc: TileService = Depends(get_tile_service),
) -> dict:
    """Return a TileJSON document with direct TiTiler tile URLs.

    Tile URL templates point to TiTiler's ``/cog/tiles`` endpoint so
    the browser fetches tiles directly — no Weatherman proxy hop.

    Supports 'latest' as run_id.
    """
    is_latest = run_id == "latest"
    resolved_run_id = svc.resolve_run_id(model, run_id)
    cog_uri = svc.cog_s3_uri(model, resolved_run_id, layer, forecast_hour)

    data = svc.build_tilejson(layer, cog_uri)
    cache_control = svc.CACHE_LATEST if is_latest else svc.CACHE_IMMUTABLE
    return JSONResponse(content=data, headers={"Cache-Control": cache_control})
