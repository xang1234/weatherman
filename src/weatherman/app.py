"""FastAPI application factory — composes all Phase 1 modules into a runnable server.

Usage:
    from weatherman.app import create_app
    app = create_app()

Or via CLI:
    uv run python -m weatherman
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator, Callable

import httpx
from fastapi import APIRouter, FastAPI, Header, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from weatherman.health import DependencyChecker, clear_checks, register_check
from weatherman.health import router as health_router
from weatherman.observability import (
    PrometheusMiddleware,
    metrics_endpoint,
    setup_logging,
    setup_tracing,
    shutdown_tracing,
)
from weatherman.caching import (
    CACHE_IMMUTABLE,
    CACHE_REVALIDATE,
    compute_content_etag,
    etag_matches,
)
from weatherman.storage.catalog import RunCatalog
from weatherman.storage.config import StorageConfig
from weatherman.storage.manifest import ColorStop, UIManifest
from weatherman.tiling.colormaps import COLORMAPS
from weatherman.storage.object_store import LocalObjectStore, ObjectStore
from weatherman.storage.paths import RunID, StorageLayout
from weatherman.edr.position import (
    init_edr_service,
    shutdown_edr_service,
)
from weatherman.edr.position import router as edr_router
from weatherman.events.router import (
    init_event_bus,
    shutdown_event_bus,
)
from weatherman.events import router as events_router
from weatherman.tenancy import TenantMiddleware
from weatherman.ais.router import (
    init_ais_tile_service,
    shutdown_ais_tile_service,
)
from weatherman.ais.router import router as ais_tile_router
from weatherman.tiling.router import (
    CatalogLoader,
    init_tile_service,
    shutdown_tile_service,
)
from weatherman.tiling.router import router as tile_router

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# TiTiler health check
# ---------------------------------------------------------------------------


class TiTilerHealthCheck:
    """Dependency checker for TiTiler reachability."""

    name = "titiler"
    critical = True

    def __init__(self, base_url: str) -> None:
        self._url = f"{base_url.rstrip('/')}/api"

    async def check(self) -> bool:
        try:
            async with httpx.AsyncClient(timeout=1.5) as client:
                resp = await client.get(self._url)
                return resp.status_code == 200
        except Exception:
            return False


# ---------------------------------------------------------------------------
# Catalog / manifest API routes
# ---------------------------------------------------------------------------


def _make_api_router(store: ObjectStore) -> APIRouter:
    """Build the /api router with catalog and manifest endpoints."""

    api_router = APIRouter(prefix="/api", tags=["api"])

    @api_router.get("/catalog/{model}", summary="Run catalog for a model")
    async def get_catalog(
        model: str,
        if_none_match: str | None = Header(None),
    ) -> Response:
        try:
            layout = StorageLayout(model)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

        try:
            data = store.read_bytes(layout.catalog_path)
        except (FileNotFoundError, OSError):
            raise HTTPException(
                status_code=404,
                detail=f"No catalog found for model '{model}'",
            )

        etag = compute_content_etag(data)

        if if_none_match and etag_matches(if_none_match, etag):
            return Response(
                status_code=304,
                headers={"ETag": etag, "Cache-Control": CACHE_REVALIDATE},
            )

        catalog = RunCatalog.from_json(data.decode("utf-8"))
        return JSONResponse(
            content=catalog.to_dict(),
            headers={
                "ETag": etag,
                "Cache-Control": CACHE_REVALIDATE,
            },
        )

    @api_router.get(
        "/manifest/{model}/{run_id}",
        summary="UI manifest for a model run",
    )
    async def get_manifest(
        model: str,
        run_id: str,
        if_none_match: str | None = Header(None),
    ) -> Response:
        try:
            layout = StorageLayout(model)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

        try:
            parsed_run_id = RunID(run_id)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

        manifest_path = layout.manifest_path(parsed_run_id)
        try:
            data = store.read_bytes(manifest_path)
        except (FileNotFoundError, OSError):
            raise HTTPException(
                status_code=404,
                detail=f"No manifest found for {model}/{run_id}",
            )

        etag = compute_content_etag(data)

        if if_none_match and etag_matches(if_none_match, etag):
            return Response(
                status_code=304,
                headers={"ETag": etag, "Cache-Control": CACHE_REVALIDATE},
            )

        manifest = UIManifest.from_json(data.decode("utf-8"))
        content = manifest.to_dict()

        # Enrich layers with color_stops from the live colormap registry
        # so that manifests built before color_stops existed still get them.
        for layer in content.get("layers", []):
            if not layer.get("color_stops"):
                cmap = COLORMAPS.get(layer.get("palette_name", ""))
                if cmap and cmap.stops:
                    layer["color_stops"] = [
                        {"position": pos, "color": list(rgb)}
                        for pos, rgb in cmap.stops
                    ]

        return JSONResponse(
            content=content,
            headers={
                "ETag": etag,
                "Cache-Control": CACHE_REVALIDATE,
            },
        )

    return api_router


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------


def _make_object_store(data_dir: str | None) -> ObjectStore:
    """Create the object store backend."""
    resolved = data_dir or os.environ.get("WEATHERMAN_DATA_DIR")
    if resolved:
        return LocalObjectStore(Path(resolved))
    raise RuntimeError(
        "No storage backend configured. "
        "Set WEATHERMAN_DATA_DIR for local dev, or implement S3ObjectStore for production."
    )


def _make_storage_config() -> StorageConfig:
    """Build StorageConfig from environment variables."""
    return StorageConfig(
        bucket=os.environ.get("STORAGE_BUCKET", "weatherman"),
        endpoint_url=os.environ.get("STORAGE_ENDPOINT_URL"),
        region=os.environ.get("STORAGE_REGION", "us-east-1"),
        prefix=os.environ.get("STORAGE_PREFIX", ""),
    )


def _make_zarr_opener(store: ObjectStore) -> Callable[[str], Any]:
    """Create a Zarr opener that resolves paths through the object store.

    For LocalObjectStore this means opening from the filesystem.
    """
    import zarr

    def open_zarr(zarr_path: str) -> zarr.Group:
        if isinstance(store, LocalObjectStore):
            full_path = str(store._root / zarr_path)
            return zarr.open_group(full_path, mode="r")
        # Future: S3-backed zarr.open_group via fsspec
        raise NotImplementedError("S3 Zarr opener not yet implemented")

    return open_zarr


def _make_catalog_loader(store: ObjectStore) -> CatalogLoader:
    """Create a catalog loader callable shared between API routes and TileService."""

    def load_catalog(model: str) -> RunCatalog:
        layout = StorageLayout(model)
        data = store.read_bytes(layout.catalog_path)
        return RunCatalog.from_json(data.decode("utf-8"))

    return load_catalog


def create_app(
    *,
    data_dir: str | None = None,
    titiler_base_url: str | None = None,
) -> FastAPI:
    """Create and configure the Weatherman FastAPI application.

    Args:
        data_dir: Local data directory (overrides WEATHERMAN_DATA_DIR env var).
        titiler_base_url: TiTiler URL (overrides TITILER_BASE_URL env var).
    """
    store = _make_object_store(data_dir)
    storage_config = _make_storage_config()
    titiler_url = titiler_base_url or os.environ.get(
        "TITILER_BASE_URL", "http://localhost:8080"
    )
    catalog_loader = _make_catalog_loader(store)
    zarr_opener = _make_zarr_opener(store)

    ais_db_path = os.environ.get("AIS_DB_PATH", "ais.duckdb")

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        # Startup
        setup_logging(service_name="weatherman")
        setup_tracing(service_name="weatherman")
        cog_root_env = os.environ.get("TITILER_COG_ROOT")
        cog_root = str(Path(cog_root_env).resolve()) if cog_root_env else None
        titiler_public_url = os.environ.get("TITILER_PUBLIC_URL")
        init_tile_service(
            storage_config, titiler_url, catalog_loader,
            cog_root=cog_root, store=store,
            titiler_public_url=titiler_public_url,
        )
        init_edr_service(catalog_loader, zarr_opener)
        init_ais_tile_service(ais_db_path)
        init_event_bus()
        register_check(TiTilerHealthCheck(titiler_url))
        logger.info("Weatherman started", extra={"titiler_url": titiler_url})
        yield
        # Shutdown
        shutdown_event_bus()
        shutdown_ais_tile_service()
        shutdown_edr_service()
        await shutdown_tile_service()
        shutdown_tracing()
        clear_checks()

    app = FastAPI(
        title="Weatherman API",
        description="Maritime Weather & Dry Bulk AIS Platform",
        version="0.1.0",
        lifespan=lifespan,
    )

    # Middleware — order matters (outermost first)
    cors_origins = os.environ.get("CORS_ORIGINS", "http://localhost:5173").split(",")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins,
        allow_methods=["GET"],
        allow_headers=["*"],
    )
    app.add_middleware(PrometheusMiddleware)
    app.add_middleware(TenantMiddleware)

    # Routers
    app.include_router(_make_api_router(store))
    app.include_router(health_router)
    app.include_router(tile_router)
    app.include_router(ais_tile_router)
    app.include_router(edr_router)
    app.include_router(events_router)

    # Metrics endpoint (plain Starlette route, not a router)
    app.add_route("/metrics", metrics_endpoint, methods=["GET"])

    return app
