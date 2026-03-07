"""Tests for create_app() factory composition (wx-rec).

Verifies the app factory correctly wires middleware, lifespan,
health endpoints, metrics, and configuration.
"""

from __future__ import annotations

import pytest
from starlette.testclient import TestClient

from weatherman.app import _make_object_store, create_app
from weatherman.storage.paths import StorageLayout


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def data_dir(tmp_path):
    """Minimal data dir so the app can start (no catalog data needed)."""
    return tmp_path


@pytest.fixture()
def client(data_dir):
    app = create_app(data_dir=str(data_dir), titiler_base_url="http://localhost:9999")
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c


# ---------------------------------------------------------------------------
# Health endpoints
# ---------------------------------------------------------------------------

class TestHealthEndpoints:
    def test_liveness_probe(self, client):
        resp = client.get("/health/live")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "alive"
        assert "timestamp" in body

    def test_readiness_probe_with_titiler_down(self, client):
        """TiTiler is unreachable in tests, so readiness returns 503."""
        resp = client.get("/health/ready")
        assert resp.status_code == 503
        body = resp.json()
        assert body["status"] == "not_ready"
        assert "titiler" in body["checks"]


# ---------------------------------------------------------------------------
# Metrics endpoint
# ---------------------------------------------------------------------------

class TestMetricsEndpoint:
    def test_returns_prometheus_format(self, client):
        resp = client.get("/metrics")
        assert resp.status_code == 200
        assert "text/plain" in resp.headers["content-type"]


# ---------------------------------------------------------------------------
# CORS middleware
# ---------------------------------------------------------------------------

class TestCORSMiddleware:
    def test_preflight_returns_cors_headers(self, client):
        resp = client.options(
            "/api/catalog/gfs",
            headers={
                "Origin": "http://localhost:5173",
                "Access-Control-Request-Method": "GET",
            },
        )
        assert resp.status_code == 200
        assert resp.headers["access-control-allow-origin"] == "http://localhost:5173"

    def test_cors_rejects_unknown_origin(self, client):
        resp = client.options(
            "/api/catalog/gfs",
            headers={
                "Origin": "http://evil.example.com",
                "Access-Control-Request-Method": "GET",
            },
        )
        assert "access-control-allow-origin" not in resp.headers


# ---------------------------------------------------------------------------
# Storage backend configuration
# ---------------------------------------------------------------------------

class TestStorageConfig:
    def test_missing_data_dir_raises(self, monkeypatch):
        monkeypatch.delenv("WEATHERMAN_DATA_DIR", raising=False)
        with pytest.raises(RuntimeError, match="No storage backend configured"):
            _make_object_store(None)

    def test_data_dir_param_overrides_env(self, tmp_path, monkeypatch):
        monkeypatch.setenv("WEATHERMAN_DATA_DIR", "/should/not/be/used")
        store = _make_object_store(str(tmp_path))
        assert store._root == tmp_path

    def test_env_var_fallback(self, tmp_path, monkeypatch):
        monkeypatch.setenv("WEATHERMAN_DATA_DIR", str(tmp_path))
        store = _make_object_store(None)
        assert store._root == tmp_path


# ---------------------------------------------------------------------------
# App metadata
# ---------------------------------------------------------------------------

class TestAppMetadata:
    def test_openapi_schema(self, client):
        resp = client.get("/openapi.json")
        assert resp.status_code == 200
        schema = resp.json()
        assert schema["info"]["title"] == "Weatherman API"
        assert "/api/catalog/{model}" in schema["paths"]
        assert "/api/manifest/{model}/{run_id}" in schema["paths"]
        assert "/health/live" in schema["paths"]
        assert "/health/ready" in schema["paths"]
