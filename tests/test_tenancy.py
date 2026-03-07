"""Tests for tenant identification, scoping, and query isolation."""

from __future__ import annotations

import sqlalchemy as sa
import structlog
from fastapi import Depends, FastAPI, Request
from starlette.testclient import TestClient

from weatherman.tenancy import (
    DEFAULT_TENANT,
    TenantMiddleware,
    TenantRepository,
    get_tenant_id,
    shared_cache_key,
    tenant_cache_key,
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


# -- Middleware and dependency tests --


class TestGetTenantId:
    def test_returns_default_tenant(self):
        resp = client.get("/test/tenant")
        assert resp.status_code == 200
        assert resp.json()["tenant_id"] == DEFAULT_TENANT

    def test_default_tenant_value(self):
        assert DEFAULT_TENANT == "default"


class TestTenantMiddleware:
    def test_sets_request_state(self):
        resp = client.get("/test/state")
        assert resp.status_code == 200
        assert resp.json()["tenant_id"] == DEFAULT_TENANT

    def test_binds_structlog_context(self):
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

    def test_unbinds_tenant_after_request(self):
        captured_contexts: list[dict] = []

        app = FastAPI()
        app.add_middleware(TenantMiddleware)

        @app.get("/test/capture")
        async def capture():
            ctx = structlog.contextvars.get_contextvars()
            captured_contexts.append(dict(ctx))
            return {"ok": True}

        test_client = TestClient(app)
        test_client.get("/test/capture")
        assert captured_contexts[0].get("tenant_id") == DEFAULT_TENANT

        test_client.get("/test/capture")
        assert captured_contexts[1].get("tenant_id") == DEFAULT_TENANT
        assert len(captured_contexts) == 2

    def test_unbinds_tenant_on_error(self):
        captured_contexts: list[dict] = []

        app = FastAPI()
        app.add_middleware(TenantMiddleware)

        @app.get("/test/error")
        async def error_endpoint():
            raise ValueError("boom")

        @app.get("/test/capture")
        async def capture():
            ctx = structlog.contextvars.get_contextvars()
            captured_contexts.append(dict(ctx))
            return {"ok": True}

        test_client = TestClient(app, raise_server_exceptions=False)
        test_client.get("/test/error")
        test_client.get("/test/capture")
        assert captured_contexts[0].get("tenant_id") == DEFAULT_TENANT

    def test_does_not_clear_other_context_keys(self):
        captured_contexts: list[dict] = []

        app = FastAPI()
        app.add_middleware(TenantMiddleware)

        @app.get("/test/with-extra")
        async def with_extra():
            structlog.contextvars.bind_contextvars(request_id="req-123")
            ctx = structlog.contextvars.get_contextvars()
            captured_contexts.append(dict(ctx))
            return {"ok": True}

        test_client = TestClient(app)
        test_client.get("/test/with-extra")
        assert captured_contexts[0]["tenant_id"] == DEFAULT_TENANT
        assert captured_contexts[0]["request_id"] == "req-123"


class TestDependencyOverride:
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
    def test_extract_returns_default_for_unauthenticated(self):
        resp = client.get("/test/tenant")
        assert resp.json()["tenant_id"] == DEFAULT_TENANT

    def test_extract_ignores_unknown_headers(self):
        resp = client.get(
            "/test/tenant", headers={"X-Custom": "something"}
        )
        assert resp.json()["tenant_id"] == DEFAULT_TENANT


# -- Cache key tests --


class TestTenantCacheKey:
    def test_basic_key(self):
        key = tenant_cache_key("acme", "preferences", "dashboard")
        assert key == "tenant:acme:preferences:dashboard"

    def test_default_tenant_key(self):
        key = tenant_cache_key(DEFAULT_TENANT, "routes", "saved")
        assert key == "tenant:default:routes:saved"

    def test_single_part(self):
        key = tenant_cache_key("acme", "settings")
        assert key == "tenant:acme:settings"

    def test_different_tenants_produce_different_keys(self):
        k1 = tenant_cache_key("acme", "prefs")
        k2 = tenant_cache_key("globex", "prefs")
        assert k1 != k2
        assert "acme" in k1
        assert "globex" in k2


class TestSharedCacheKey:
    def test_tile_key(self):
        key = shared_cache_key("tiles", "gfs", "20260306T00Z", "4", "3", "7")
        assert key == "shared:tiles:gfs:20260306T00Z:4:3:7"

    def test_catalog_key(self):
        key = shared_cache_key("catalog", "gfs")
        assert key == "shared:catalog:gfs"

    def test_no_tenant_prefix(self):
        key = shared_cache_key("tiles", "gfs")
        assert "tenant" not in key


# -- TenantRepository tests --


_metadata = sa.MetaData()

_test_table = sa.Table(
    "test_items",
    _metadata,
    sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
    sa.Column("tenant_id", sa.String(64), nullable=False, index=True),
    sa.Column("name", sa.String(128), nullable=False),
    sa.Column("value", sa.String(256), nullable=True),
)

_no_tenant_table = sa.Table(
    "no_tenant_items",
    _metadata,
    sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
    sa.Column("name", sa.String(128), nullable=False),
)


def _make_engine() -> sa.Engine:
    engine = sa.create_engine("sqlite:///:memory:")
    _metadata.create_all(engine)
    return engine


class TestTenantRepositoryValidation:
    def test_rejects_table_without_tenant_id(self):
        engine = _make_engine()
        try:
            TenantRepository(engine, _no_tenant_table)
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "tenant_id" in str(e)

    def test_accepts_table_with_tenant_id(self):
        engine = _make_engine()
        repo = TenantRepository(engine, _test_table)
        assert repo.table is _test_table


class TestTenantRepositoryCRUD:
    def test_insert_and_select_one(self):
        engine = _make_engine()
        repo = TenantRepository(engine, _test_table)

        row_id = repo.insert("acme", name="item1", value="val1")
        assert row_id is not None

        row = repo.select_one("acme", _test_table.c.name == "item1")
        assert row is not None
        assert row.tenant_id == "acme"
        assert row.name == "item1"
        assert row.value == "val1"

    def test_select_one_returns_none_for_missing(self):
        engine = _make_engine()
        repo = TenantRepository(engine, _test_table)

        row = repo.select_one("acme", _test_table.c.name == "nonexistent")
        assert row is None

    def test_select_many(self):
        engine = _make_engine()
        repo = TenantRepository(engine, _test_table)

        repo.insert("acme", name="a", value="1")
        repo.insert("acme", name="b", value="2")
        repo.insert("acme", name="c", value="3")

        rows = repo.select_many("acme", order_by=_test_table.c.name)
        assert len(rows) == 3
        assert [r.name for r in rows] == ["a", "b", "c"]

    def test_select_many_with_filter(self):
        engine = _make_engine()
        repo = TenantRepository(engine, _test_table)

        repo.insert("acme", name="a", value="x")
        repo.insert("acme", name="b", value="y")

        rows = repo.select_many("acme", _test_table.c.value == "x")
        assert len(rows) == 1
        assert rows[0].name == "a"

    def test_update(self):
        engine = _make_engine()
        repo = TenantRepository(engine, _test_table)

        repo.insert("acme", name="item1", value="old")
        count = repo.update(
            "acme", _test_table.c.name == "item1", value="new"
        )
        assert count == 1

        row = repo.select_one("acme", _test_table.c.name == "item1")
        assert row.value == "new"

    def test_delete(self):
        engine = _make_engine()
        repo = TenantRepository(engine, _test_table)

        repo.insert("acme", name="item1", value="val1")
        count = repo.delete("acme", _test_table.c.name == "item1")
        assert count == 1

        row = repo.select_one("acme", _test_table.c.name == "item1")
        assert row is None


class TestTenantIsolation:
    """Verify that queries are always scoped to the requesting tenant."""

    def test_cannot_read_other_tenant_data(self):
        engine = _make_engine()
        repo = TenantRepository(engine, _test_table)

        repo.insert("acme", name="secret", value="acme-data")
        repo.insert("globex", name="secret", value="globex-data")

        # Acme can only see their own data
        acme_row = repo.select_one("acme", _test_table.c.name == "secret")
        assert acme_row.value == "acme-data"

        # Globex can only see their own data
        globex_row = repo.select_one("globex", _test_table.c.name == "secret")
        assert globex_row.value == "globex-data"

    def test_cannot_list_other_tenant_data(self):
        engine = _make_engine()
        repo = TenantRepository(engine, _test_table)

        repo.insert("acme", name="a1", value="1")
        repo.insert("acme", name="a2", value="2")
        repo.insert("globex", name="g1", value="3")

        acme_rows = repo.select_many("acme")
        assert len(acme_rows) == 2
        assert all(r.tenant_id == "acme" for r in acme_rows)

        globex_rows = repo.select_many("globex")
        assert len(globex_rows) == 1
        assert globex_rows[0].tenant_id == "globex"

    def test_cannot_update_other_tenant_data(self):
        engine = _make_engine()
        repo = TenantRepository(engine, _test_table)

        repo.insert("acme", name="item", value="original")
        repo.insert("globex", name="item", value="original")

        # Globex tries to update — only affects their own row
        count = repo.update("globex", _test_table.c.name == "item", value="hacked")
        assert count == 1

        # Acme's data is untouched
        acme_row = repo.select_one("acme", _test_table.c.name == "item")
        assert acme_row.value == "original"

    def test_cannot_delete_other_tenant_data(self):
        engine = _make_engine()
        repo = TenantRepository(engine, _test_table)

        repo.insert("acme", name="item", value="keep")
        repo.insert("globex", name="item", value="delete")

        # Globex deletes their item
        count = repo.delete("globex", _test_table.c.name == "item")
        assert count == 1

        # Acme's item still exists
        acme_row = repo.select_one("acme", _test_table.c.name == "item")
        assert acme_row is not None
        assert acme_row.value == "keep"

    def test_insert_always_sets_tenant_id(self):
        """Even if caller doesn't mention tenant_id, it's set automatically."""
        engine = _make_engine()
        repo = TenantRepository(engine, _test_table)

        repo.insert("acme", name="auto", value="test")

        # Verify via raw SQL that tenant_id was set
        with engine.connect() as conn:
            row = conn.execute(
                sa.select(_test_table).where(_test_table.c.name == "auto")
            ).first()
            assert row.tenant_id == "acme"
