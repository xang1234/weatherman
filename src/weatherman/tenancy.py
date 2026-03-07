"""Tenant identification for multi-tenancy support.

Extracts tenant_id from the request context and makes it available
to downstream services. For MVP, returns a hardcoded default tenant.

Usage::

    from weatherman.tenancy import get_tenant_id

    @router.get("/tiles/{z}/{x}/{y}")
    async def get_tile(tenant_id: str = Depends(get_tenant_id)):
        ...  # tenant_id is available for scoping queries, paths, etc.
"""

from __future__ import annotations

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response

from weatherman.observability.logging import bind_context, clear_context

DEFAULT_TENANT = "default"


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
            clear_context()

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
