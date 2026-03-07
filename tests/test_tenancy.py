"""Tests for tenant identification middleware and dependency."""

from __future__ import annotations

from unittest.mock import patch

import structlog
from fastapi import Depends, FastAPI, Request
from starlette.testclient import TestClient

from weatherman.tenancy import (
    DEFAULT_TENANT,
    TenantMiddleware,
    get_tenant_id,
)


def _make_app() -> FastAPI:
    app = FastAPI()
    app.add_middleware(TenantMiddleware)

    @app.get("/test/tenant")
    async def tenant_endpoint(tenant_id: str = Depends(get_tenant_id)):
        return {"tenant_id": tenant_id}

    @app.get("/test/state")
    async def state_endpoint(request: Request):
        return {"tenant_id": request.state.tenant_id}

    return app


client = TestClient(_make_app())


class TestGetTenantId:
    """Tests for the get_tenant_id FastAPI dependency."""

    def test_returns_default_tenant(self):
        resp = client.get("/test/tenant")
        assert resp.status_code == 200
        assert resp.json()["tenant_id"] == DEFAULT_TENANT

    def test_default_tenant_value(self):
        assert DEFAULT_TENANT == "default"


class TestTenantMiddleware:
    """Tests for TenantMiddleware request processing."""

    def test_sets_request_state(self):
        resp = client.get("/test/state")
        assert resp.status_code == 200
        assert resp.json()["tenant_id"] == DEFAULT_TENANT

    def test_binds_structlog_context(self):
        """Verify tenant_id is bound to structlog context during request."""
        captured_context = {}

        app = FastAPI()
        app.add_middleware(TenantMiddleware)

        @app.get("/test/context")
        async def context_endpoint():
            ctx = structlog.contextvars.get_contextvars()
            captured_context.update(ctx)
            return {"ok": True}

        test_client = TestClient(app)
        test_client.get("/test/context")
        assert captured_context.get("tenant_id") == DEFAULT_TENANT

    def test_clears_context_after_request(self):
        """Verify structlog context is cleared after the request completes."""
        app = FastAPI()
        app.add_middleware(TenantMiddleware)

        @app.get("/test/noop")
        async def noop():
            return {"ok": True}

        test_client = TestClient(app)
        test_client.get("/test/noop")
        # After request completes, context should be cleared
        ctx = structlog.contextvars.get_contextvars()
        assert "tenant_id" not in ctx

    def test_clears_context_on_error(self):
        """Context is cleared even if the endpoint raises."""
        app = FastAPI()
        app.add_middleware(TenantMiddleware)

        @app.get("/test/error")
        async def error_endpoint():
            raise ValueError("boom")

        test_client = TestClient(app, raise_server_exceptions=False)
        test_client.get("/test/error")
        ctx = structlog.contextvars.get_contextvars()
        assert "tenant_id" not in ctx


class TestDependencyOverride:
    """Tests verifying get_tenant_id can be overridden for testing."""

    def test_override_tenant_id(self):
        app = FastAPI()
        app.add_middleware(TenantMiddleware)

        @app.get("/test/tenant")
        async def tenant_endpoint(tenant_id: str = Depends(get_tenant_id)):
            return {"tenant_id": tenant_id}

        app.dependency_overrides[get_tenant_id] = lambda: "tenant-abc"
        test_client = TestClient(app)

        resp = test_client.get("/test/tenant")
        assert resp.json()["tenant_id"] == "tenant-abc"

        app.dependency_overrides.clear()


class TestFutureExtraction:
    """Tests for the extraction method that will be extended later."""

    def test_extract_returns_default_for_unauthenticated(self):
        """Without auth headers, extraction returns default tenant."""
        resp = client.get("/test/tenant")
        assert resp.json()["tenant_id"] == DEFAULT_TENANT

    def test_extract_ignores_unknown_headers(self):
        """Random headers don't affect tenant extraction."""
        resp = client.get(
            "/test/tenant", headers={"X-Custom": "something"}
        )
        assert resp.json()["tenant_id"] == DEFAULT_TENANT
