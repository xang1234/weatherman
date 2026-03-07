"""End-to-end integration tests for the composed FastAPI app (wx-22y).

Verifies that all Phase 1 modules are correctly wired together:
routers mounted, middleware active, lifespan initialised.

Detailed route/factory tests live in test_api_routes.py,
test_app_factory.py, and test_titiler_health.py. This file
focuses on cross-cutting integration concerns.
"""

from __future__ import annotations

import pytest
from starlette.testclient import TestClient

from weatherman.app import create_app
from weatherman.storage.catalog import RunCatalog
from weatherman.storage.paths import RunID, StorageLayout


@pytest.fixture()
def data_dir(tmp_path):
    """Populate tmp_path with test data for model 'gfs'."""
    model = "gfs"
    run_id = "20260306T00Z"
    layout = StorageLayout(model)

    catalog = RunCatalog.new(model)
    catalog.publish_run(RunID(run_id), layout=layout)

    catalog_path = tmp_path / layout.catalog_path
    catalog_path.parent.mkdir(parents=True, exist_ok=True)
    catalog.save(catalog_path)

    return tmp_path


@pytest.fixture()
def client(data_dir):
    app = create_app(data_dir=str(data_dir), titiler_base_url="http://localhost:9999")
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c


class TestMiddlewareIntegration:
    def test_tenant_middleware_runs_on_requests(self, client):
        """TenantMiddleware sets request.state.tenant_id without errors."""
        resp = client.get("/health/live")
        assert resp.status_code == 200

    def test_prometheus_middleware_records_requests(self, client):
        """PrometheusMiddleware increments counters visible at /metrics."""
        client.get("/health/live")
        resp = client.get("/metrics")
        assert resp.status_code == 200
        body = resp.text
        assert "http_requests_total" in body
        assert "http_request_duration_seconds" in body

    def test_middleware_ordering_cors_outermost(self, client):
        """CORS middleware handles preflight before other middleware runs."""
        resp = client.options(
            "/api/catalog/gfs",
            headers={
                "Origin": "http://localhost:5173",
                "Access-Control-Request-Method": "GET",
            },
        )
        assert resp.status_code == 200
        assert "access-control-allow-origin" in resp.headers


class TestRouterMounting:
    def test_all_expected_routes_mounted(self, client):
        """Verify all Phase 1 route groups appear in the OpenAPI schema."""
        resp = client.get("/openapi.json")
        paths = resp.json()["paths"]
        assert "/api/catalog/{model}" in paths
        assert "/api/manifest/{model}/{run_id}" in paths
        assert "/health/live" in paths
        assert "/health/ready" in paths

    def test_metrics_route_outside_openapi(self, client):
        """/metrics is a plain Starlette route, not in OpenAPI."""
        resp = client.get("/openapi.json")
        paths = resp.json()["paths"]
        assert "/metrics" not in paths
        # But it still works
        assert client.get("/metrics").status_code == 200


class TestLifespanIntegration:
    def test_titiler_health_check_registered(self, client):
        """Lifespan registers TiTilerHealthCheck in readiness probe."""
        resp = client.get("/health/ready")
        body = resp.json()
        assert "titiler" in body["checks"]

    def test_lifespan_shutdown_cleans_up(self, data_dir):
        """After TestClient exits, health check registry is cleared."""
        from weatherman.health import _checkers

        app = create_app(data_dir=str(data_dir), titiler_base_url="http://localhost:9999")
        with TestClient(app):
            assert any(c.name == "titiler" for c in _checkers)

        # After context manager exit, lifespan shutdown runs clear_checks()
        assert not any(c.name == "titiler" for c in _checkers)
