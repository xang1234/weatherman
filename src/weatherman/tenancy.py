"""Tenant identification and scoping for multi-tenancy support.

Extracts tenant_id from the request context and provides utilities for
tenant-scoped data access. For MVP, uses a hardcoded default tenant.

Components:
- TenantMiddleware: sets request.state.tenant_id + structlog context
- get_tenant_id: FastAPI dependency for endpoint signatures
- TenantRepository: base class enforcing tenant_id in all SQL queries
- tenant_cache_key / shared_cache_key: cache key construction helpers

Data classification:
- Tenant-scoped: user preferences, saved routes, API keys, usage tracking
  → always include tenant_id in queries and cache keys
- Shared: weather runs, COGs, tiles, catalog
  → never include tenant_id (weather data is the same for all tenants)
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import sqlalchemy as sa
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response

from weatherman.observability.logging import bind_context, unbind_context

logger = logging.getLogger(__name__)

DEFAULT_TENANT = "default"


# -- Request-level tenant extraction --


async def get_tenant_id(request: Request) -> str:
    """FastAPI dependency that returns the tenant_id for the current request.

    Reads from request.state.tenant_id (set by TenantMiddleware).
    Falls back to DEFAULT_TENANT if middleware hasn't run.
    """
    return getattr(request.state, "tenant_id", DEFAULT_TENANT)


class TenantMiddleware(BaseHTTPMiddleware):
    """Middleware that extracts tenant_id and binds it to request + log context.

    For MVP: always uses DEFAULT_TENANT.
    Future: extract from JWT ``tenant`` claim or API key scope.
    """

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        tenant_id = self._extract_tenant_id(request)
        request.state.tenant_id = tenant_id
        bind_context(tenant_id=tenant_id)
        try:
            response = await call_next(request)
            return response
        finally:
            unbind_context("tenant_id")

    @staticmethod
    def _extract_tenant_id(request: Request) -> str:
        """Extract tenant_id from the request.

        MVP: returns DEFAULT_TENANT for all requests.
        Future implementation will check:
        1. JWT claims (Authorization header → decode → tenant claim)
        2. API key scope (X-API-Key header → lookup → tenant)
        3. Fall back to DEFAULT_TENANT for unauthenticated requests
        """
        return DEFAULT_TENANT


# -- Cache key helpers --


def tenant_cache_key(tenant_id: str, *parts: str) -> str:
    """Build a cache key scoped to a specific tenant.

    Use for tenant-specific data: preferences, saved routes, usage.

    >>> tenant_cache_key("acme", "preferences", "dashboard")
    'tenant:acme:preferences:dashboard'
    """
    if ":" in tenant_id:
        raise ValueError(f"tenant_id must not contain ':': {tenant_id!r}")
    return f"tenant:{tenant_id}:" + ":".join(parts)


def shared_cache_key(*parts: str) -> str:
    """Build a cache key for shared (non-tenant-scoped) data.

    Use for weather data: tiles, run catalog, manifests.

    >>> shared_cache_key("tiles", "gfs", "20260306T00Z", "4", "3", "7")
    'shared:tiles:gfs:20260306T00Z:4:3:7'
    """
    return "shared:" + ":".join(parts)


# -- Tenant-scoped SQL repository --


class TenantRepository:
    """Base class for repositories that manage tenant-scoped data.

    Enforces tenant_id in every query by construction. Subclasses provide
    the table and implement domain-specific methods using the helpers.

    Every row written includes tenant_id. Every query filters by tenant_id.
    Cross-tenant queries are structurally impossible through this interface.

    Usage::

        class PreferencesRepo(TenantRepository):
            def __init__(self, engine: sa.Engine):
                super().__init__(engine, preferences_table)

            def get_dashboard(self, tenant_id: str, user_id: str) -> dict | None:
                row = self.select_one(tenant_id, self.table.c.user_id == user_id)
                return row._asdict() if row else None
    """

    def __init__(self, engine: sa.Engine, table: sa.Table) -> None:
        if "tenant_id" not in {c.name for c in table.columns}:
            raise ValueError(
                f"Table '{table.name}' must have a 'tenant_id' column "
                f"to be used with TenantRepository"
            )
        self._engine = engine
        self._table = table

    @property
    def table(self) -> sa.Table:
        return self._table

    def _tenant_filter(self, tenant_id: str) -> sa.ColumnElement:
        """Return the WHERE clause that scopes queries to a tenant."""
        return self._table.c.tenant_id == tenant_id

    def select_one(
        self,
        tenant_id: str,
        *extra_filters: sa.ColumnElement,
    ) -> Any:
        """Select a single row scoped to tenant_id.

        Returns the row or None.
        """
        query = (
            sa.select(self._table)
            .where(self._tenant_filter(tenant_id), *extra_filters)
        )
        with self._engine.connect() as conn:
            return conn.execute(query).first()

    def select_many(
        self,
        tenant_id: str,
        *extra_filters: sa.ColumnElement,
        order_by: sa.ColumnElement | None = None,
    ) -> list[Any]:
        """Select multiple rows scoped to tenant_id."""
        query = (
            sa.select(self._table)
            .where(self._tenant_filter(tenant_id), *extra_filters)
        )
        if order_by is not None:
            query = query.order_by(order_by)
        with self._engine.connect() as conn:
            return conn.execute(query).fetchall()

    def insert(
        self,
        tenant_id: str,
        **values: Any,
    ) -> Any:
        """Insert a row with tenant_id automatically included."""
        values["tenant_id"] = tenant_id
        with self._engine.begin() as conn:
            result = conn.execute(self._table.insert().values(**values))
            return result.inserted_primary_key[0]

    def update(
        self,
        tenant_id: str,
        *filters: sa.ColumnElement,
        **values: Any,
    ) -> int:
        """Update rows scoped to tenant_id. Returns number of rows updated."""
        if "tenant_id" in values:
            raise ValueError("tenant_id cannot be changed via update()")
        query = (
            self._table.update()
            .where(self._tenant_filter(tenant_id), *filters)
            .values(**values)
        )
        with self._engine.begin() as conn:
            result = conn.execute(query)
            return result.rowcount

    def delete(
        self,
        tenant_id: str,
        *filters: sa.ColumnElement,
    ) -> int:
        """Delete rows scoped to tenant_id. Returns number of rows deleted."""
        query = (
            self._table.delete()
            .where(self._tenant_filter(tenant_id), *filters)
        )
        with self._engine.begin() as conn:
            result = conn.execute(query)
            return result.rowcount
