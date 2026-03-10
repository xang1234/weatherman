"""Tests for the XYZ / OGC Tiles API endpoint."""

import io
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import httpx
import numpy as np
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from PIL import Image

from weatherman.storage.catalog import RunCatalog
from weatherman.storage.config import StorageConfig
from weatherman.storage.paths import RunID, StorageLayout
from weatherman.tiling.data_encoder import decode_rgba_to_float
from weatherman.tiling.router import (
    TileService,
    init_tile_service,
    router,
    shutdown_tile_service,
)

# -- Fixtures --

STORAGE = StorageConfig(bucket="wx-data", endpoint_url="http://minio:9000")
TITILER_URL = "http://titiler:8080"
RUN = RunID("20260306T12Z")


def _make_catalog(model: str) -> RunCatalog:
    """Build a catalog with one published run."""
    catalog = RunCatalog.new(model)
    layout = StorageLayout(model)
    catalog.publish_run(
        RUN,
        layout=layout,
        published_at=datetime(2026, 3, 6, 13, 0, tzinfo=timezone.utc),
    )
    return catalog


def _catalog_loader(model: str) -> RunCatalog:
    return _make_catalog(model)


def _empty_catalog_loader(model: str) -> RunCatalog:
    return RunCatalog.new(model)


@pytest.fixture()
def app():
    """Create a FastAPI app with the tile router for testing."""
    import weatherman.tiling.router as mod
    # Ensure clean state before init
    mod._service = None
    app = FastAPI()
    init_tile_service(STORAGE, TITILER_URL, _catalog_loader)
    app.include_router(router)
    yield app
    # Clean up module-level state
    mod._service = None


@pytest.fixture()
def client(app):
    return TestClient(app)


# -- TileService unit tests --


class TestTileServiceResolveRunId:
    def test_resolve_literal_run_id(self):
        svc = TileService(STORAGE, TITILER_URL, _catalog_loader)
        result = svc.resolve_run_id("gfs", "20260306T12Z")
        assert result == RUN

    def test_resolve_latest(self):
        svc = TileService(STORAGE, TITILER_URL, _catalog_loader)
        result = svc.resolve_run_id("gfs", "latest")
        assert result == RUN

    def test_resolve_latest_no_published_run(self):
        svc = TileService(STORAGE, TITILER_URL, _empty_catalog_loader)
        with pytest.raises(Exception) as exc_info:
            svc.resolve_run_id("gfs", "latest")
        assert "No published run" in str(exc_info.value.detail)

    def test_resolve_invalid_run_id(self):
        svc = TileService(STORAGE, TITILER_URL, _catalog_loader)
        with pytest.raises(Exception) as exc_info:
            svc.resolve_run_id("gfs", "not-a-run-id")
        assert exc_info.value.status_code == 400


class TestTileServiceCogUri:
    def test_cog_uri_construction(self):
        svc = TileService(STORAGE, TITILER_URL, _catalog_loader)
        uri = svc.cog_s3_uri("gfs", RUN, "temperature", 6)
        assert uri == "s3://wx-data/models/gfs/runs/20260306T12Z/cogs/temperature/006.tif"

    def test_cog_uri_with_prefix(self):
        storage = StorageConfig(bucket="wx-data", prefix="prod")
        svc = TileService(storage, TITILER_URL, _catalog_loader)
        uri = svc.cog_s3_uri("gfs", RUN, "wind_speed", 0)
        assert uri == "s3://wx-data/prod/models/gfs/runs/20260306T12Z/cogs/wind_speed/000.tif"

    def test_cog_uri_invalid_model_returns_400(self):
        svc = TileService(STORAGE, TITILER_URL, _catalog_loader)
        with pytest.raises(Exception) as exc_info:
            svc.cog_s3_uri("../../etc", RUN, "temperature", 0)
        assert exc_info.value.status_code == 400

    def test_cog_uri_invalid_layer_returns_400(self):
        svc = TileService(STORAGE, TITILER_URL, _catalog_loader)
        with pytest.raises(Exception) as exc_info:
            svc.cog_s3_uri("gfs", RUN, "INVALID-LAYER!", 0)
        assert exc_info.value.status_code == 400


class TestTileServiceTileJson:
    def test_build_tilejson(self):
        svc = TileService(STORAGE, TITILER_URL, _catalog_loader)
        cog_uri = "s3://wx-data/models/gfs/runs/20260306T12Z/cogs/temperature/000.tif"
        result = svc.build_tilejson("temperature", cog_uri)
        assert result["tilejson"] == "3.0.0"
        assert result["name"] == "temperature"
        assert len(result["tiles"]) == 1
        tile_url = result["tiles"][0]
        assert tile_url.startswith("/cog/tiles/WebMercatorQuad/")
        assert "{z}" in tile_url
        assert "temperature" in tile_url
        assert "rescale=" in tile_url
        assert "colormap=" in tile_url
        assert result["bounds"] == [-180, -90, 180, 90]

    def test_build_tilejson_unknown_layer(self):
        svc = TileService(STORAGE, TITILER_URL, _catalog_loader)
        with pytest.raises(Exception) as exc_info:
            svc.build_tilejson("nonexistent", "s3://wx-data/cogs/fake.tif")
        assert exc_info.value.status_code == 400
        assert "Unknown layer" in str(exc_info.value.detail)


class TestInitTileService:
    def test_double_init_raises(self):
        import weatherman.tiling.router as mod
        mod._service = None
        init_tile_service(STORAGE, TITILER_URL, _catalog_loader)
        with pytest.raises(RuntimeError, match="already initialized"):
            init_tile_service(STORAGE, TITILER_URL, _catalog_loader)
        mod._service = None


# -- Integration tests with TestClient --


class TestTileEndpoint:
    def test_tile_request_proxies_to_titiler(self, client):
        """Tile request should proxy to TiTiler and return PNG."""
        fake_png = b"\x89PNG\r\n\x1a\nfake"

        mock_response = httpx.Response(200, content=fake_png)

        with patch.object(
            httpx.AsyncClient, "get", new_callable=AsyncMock, return_value=mock_response
        ):
            resp = client.get("/tiles/gfs/20260306T12Z/temperature/0/1/2/3.png")

        assert resp.status_code == 200
        assert resp.headers["content-type"] == "image/png"
        assert resp.content == fake_png

    def test_tile_with_latest_alias(self, client):
        fake_png = b"\x89PNG\r\n\x1a\nfake"
        mock_response = httpx.Response(200, content=fake_png)

        with patch.object(
            httpx.AsyncClient, "get", new_callable=AsyncMock, return_value=mock_response
        ) as mock_get:
            resp = client.get("/tiles/gfs/latest/temperature/0/1/2/3.png")

        assert resp.status_code == 200
        # Verify the TiTiler URL was called (not a 'latest' passthrough)
        call_url = mock_get.call_args.args[0]
        assert "cog/tiles/WebMercatorQuad/1/2/3.png" in call_url

    def test_tile_published_run_has_immutable_cache(self, client):
        """Published run tiles get immutable caching headers."""
        fake_png = b"\x89PNG\r\n\x1a\nfake"
        mock_response = httpx.Response(200, content=fake_png)

        with patch.object(
            httpx.AsyncClient, "get", new_callable=AsyncMock, return_value=mock_response
        ):
            resp = client.get("/tiles/gfs/20260306T12Z/temperature/0/1/2/3.png")

        assert resp.status_code == 200
        assert resp.headers["cache-control"] == "public, max-age=31536000, immutable"

    def test_tile_latest_alias_has_short_cache(self, client):
        """'latest' alias tiles get short TTL cache headers."""
        fake_png = b"\x89PNG\r\n\x1a\nfake"
        mock_response = httpx.Response(200, content=fake_png)

        with patch.object(
            httpx.AsyncClient, "get", new_callable=AsyncMock, return_value=mock_response
        ):
            resp = client.get("/tiles/gfs/latest/temperature/0/1/2/3.png")

        assert resp.status_code == 200
        assert resp.headers["cache-control"] == "public, max-age=60"

    def test_tile_unknown_layer(self, client):
        resp = client.get("/tiles/gfs/20260306T12Z/nonexistent/0/1/2/3.png")
        assert resp.status_code == 400
        assert "Unknown layer" in resp.json()["detail"]

    def test_tile_invalid_run_id(self, client):
        resp = client.get("/tiles/gfs/bad-id/temperature/0/1/2/3.png")
        assert resp.status_code == 400

    def test_tile_negative_forecast_hour(self, client):
        resp = client.get("/tiles/gfs/20260306T12Z/temperature/-1/1/2/3.png")
        assert resp.status_code == 422  # FastAPI validation error

    def test_tile_titiler_404(self, client):
        mock_response = httpx.Response(404)

        with patch.object(
            httpx.AsyncClient, "get", new_callable=AsyncMock, return_value=mock_response
        ):
            resp = client.get("/tiles/gfs/20260306T12Z/temperature/0/1/2/3.png")

        assert resp.status_code == 404
        assert "cache-control" not in resp.headers

    def test_tile_titiler_500_returns_502(self, client):
        mock_response = httpx.Response(500)

        with patch.object(
            httpx.AsyncClient, "get", new_callable=AsyncMock, return_value=mock_response
        ):
            resp = client.get("/tiles/gfs/20260306T12Z/temperature/0/1/2/3.png")

        assert resp.status_code == 502
        assert "cache-control" not in resp.headers

    def test_tile_titiler_timeout_returns_504(self, client):
        with patch.object(
            httpx.AsyncClient, "get", new_callable=AsyncMock,
            side_effect=httpx.TimeoutException("timed out"),
        ):
            resp = client.get("/tiles/gfs/20260306T12Z/temperature/0/1/2/3.png")

        assert resp.status_code == 504
        assert "timed out" in resp.json()["detail"]
        assert "cache-control" not in resp.headers


class TestTileJsonEndpoint:
    def test_tilejson_default_layer(self, client):
        resp = client.get("/tiles/gfs/20260306T12Z/tilejson.json")
        assert resp.status_code == 200
        data = resp.json()
        assert data["tilejson"] == "3.0.0"
        assert data["name"] == "temperature"
        tile_url = data["tiles"][0]
        # Tile URL points directly to TiTiler with COG path baked in
        assert tile_url.startswith("/cog/tiles/WebMercatorQuad/")
        assert "temperature" in tile_url
        assert "rescale=" in tile_url

    def test_tilejson_with_layer_param(self, client):
        resp = client.get("/tiles/gfs/20260306T12Z/tilejson.json?layer=wind_speed&forecast_hour=6")
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "wind_speed"
        tile_url = data["tiles"][0]
        assert "wind_speed" in tile_url
        assert "006.tif" in tile_url

    def test_tilejson_latest_resolves(self, client):
        resp = client.get("/tiles/gfs/latest/tilejson.json")
        assert resp.status_code == 200
        data = resp.json()
        # 'latest' should be resolved to actual run_id in the COG path
        assert "20260306T12Z" in data["tiles"][0]

    def test_tilejson_has_titiler_url(self, client):
        resp = client.get("/tiles/gfs/20260306T12Z/tilejson.json")
        data = resp.json()
        # Tile URLs are relative /cog/tiles paths (not absolute http)
        assert data["tiles"][0].startswith("/cog/tiles/")

    def test_tilejson_published_run_has_immutable_cache(self, client):
        resp = client.get("/tiles/gfs/20260306T12Z/tilejson.json")
        assert resp.status_code == 200
        assert resp.headers["cache-control"] == "public, max-age=31536000, immutable"

    def test_tilejson_latest_has_short_cache(self, client):
        resp = client.get("/tiles/gfs/latest/tilejson.json")
        assert resp.status_code == 200
        assert resp.headers["cache-control"] == "public, max-age=60"

    def test_tilejson_unknown_layer(self, client):
        resp = client.get("/tiles/gfs/20260306T12Z/tilejson.json?layer=nonexistent")
        assert resp.status_code == 400


def _make_fake_tiff(values: np.ndarray) -> bytes:
    """Create a single-band TIFF from a float32 array for mocking TiTiler."""
    img = Image.fromarray(values.astype(np.float32), mode="F")
    buf = io.BytesIO()
    img.save(buf, format="TIFF")
    return buf.getvalue()


class TestDataTileEndpoint:
    def test_data_tile_returns_encoded_png(self, client):
        """Data tile should return an RGBA PNG with encoded float values."""
        values = np.full((256, 256), 25.0, dtype=np.float32)
        tiff_bytes = _make_fake_tiff(values)
        mock_response = httpx.Response(200, content=tiff_bytes)

        with patch.object(
            httpx.AsyncClient, "get", new_callable=AsyncMock, return_value=mock_response
        ):
            resp = client.get("/tiles/gfs/20260306T12Z/temperature/0/data/1/2/3.png")

        assert resp.status_code == 200
        assert resp.headers["content-type"] == "image/png"
        assert "x-value-range" in resp.headers
        assert resp.headers["x-value-range"] == "-55.0,55.0"

    def test_data_tile_round_trip_accuracy(self, client):
        """Encoded data tile should decode back within 0.1% error."""
        rng = np.random.default_rng(99)
        values = rng.uniform(-55.0, 55.0, size=(256, 256)).astype(np.float32)
        tiff_bytes = _make_fake_tiff(values)
        mock_response = httpx.Response(200, content=tiff_bytes)

        with patch.object(
            httpx.AsyncClient, "get", new_callable=AsyncMock, return_value=mock_response
        ):
            resp = client.get("/tiles/gfs/20260306T12Z/temperature/0/data/1/2/3.png")

        # Decode the returned PNG
        img = Image.open(io.BytesIO(resp.content))
        rgba = np.array(img)
        decoded, mask = decode_rgba_to_float(rgba, -55.0, 55.0)

        assert not np.any(mask)
        max_error = np.max(np.abs(decoded - values))
        value_range = 55.0 - (-55.0)
        assert max_error < value_range * 0.001

    def test_data_tile_latest_alias(self, client):
        values = np.full((256, 256), 10.0, dtype=np.float32)
        tiff_bytes = _make_fake_tiff(values)
        mock_response = httpx.Response(200, content=tiff_bytes)

        with patch.object(
            httpx.AsyncClient, "get", new_callable=AsyncMock, return_value=mock_response
        ):
            resp = client.get("/tiles/gfs/latest/temperature/0/data/1/2/3.png")

        assert resp.status_code == 200
        assert resp.headers["cache-control"] == "public, max-age=60"

    def test_data_tile_published_run_immutable_cache(self, client):
        values = np.full((256, 256), 10.0, dtype=np.float32)
        tiff_bytes = _make_fake_tiff(values)
        mock_response = httpx.Response(200, content=tiff_bytes)

        with patch.object(
            httpx.AsyncClient, "get", new_callable=AsyncMock, return_value=mock_response
        ):
            resp = client.get("/tiles/gfs/20260306T12Z/temperature/0/data/1/2/3.png")

        assert resp.headers["cache-control"] == "public, max-age=31536000, immutable"

    def test_data_tile_unknown_layer(self, client):
        resp = client.get("/tiles/gfs/20260306T12Z/nonexistent/0/data/1/2/3.png")
        assert resp.status_code == 400
        assert "Unknown layer" in resp.json()["detail"]

    def test_data_tile_titiler_404(self, client):
        mock_response = httpx.Response(404)
        with patch.object(
            httpx.AsyncClient, "get", new_callable=AsyncMock, return_value=mock_response
        ):
            resp = client.get("/tiles/gfs/20260306T12Z/temperature/0/data/1/2/3.png")
        assert resp.status_code == 404

    def test_data_tile_titiler_timeout(self, client):
        with patch.object(
            httpx.AsyncClient, "get", new_callable=AsyncMock,
            side_effect=httpx.TimeoutException("timed out"),
        ):
            resp = client.get("/tiles/gfs/20260306T12Z/temperature/0/data/1/2/3.png")
        assert resp.status_code == 504
