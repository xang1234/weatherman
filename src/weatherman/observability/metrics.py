"""Prometheus metrics for Weatherman services.

Exposes operational metrics via a /metrics endpoint:
- HTTP request latency histogram (by method, endpoint, status)
- HTTP request count (by method, endpoint, status)
- Active HTTP connections gauge
- Pipeline step duration histogram (by step name)
- Pipeline error counter (by step name)

Histogram bucket boundaries are tuned per metric type:
- Tile/HTTP latency: milliseconds (10ms–5s)
- Pipeline durations: seconds (1s–600s)

High-cardinality labels (run_id, trace_id) are intentionally excluded.
Process-level metrics (CPU, memory, open FDs) are provided automatically
by the prometheus-client default collectors.
"""

from __future__ import annotations

import time
from typing import Callable

from prometheus_client import Counter, Gauge, Histogram
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Match
from starlette.types import ASGIApp, Receive, Scope, Send

# ---------------------------------------------------------------------------
# HTTP request metrics
# ---------------------------------------------------------------------------

# Bucket boundaries in seconds (matching the bead spec's ms values)
_HTTP_LATENCY_BUCKETS = (
    0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5, 5.0,
)

REQUEST_LATENCY = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency in seconds",
    labelnames=["method", "endpoint", "status"],
    buckets=_HTTP_LATENCY_BUCKETS,
)

REQUEST_COUNT = Counter(
    "http_requests_total",
    "Total HTTP requests",
    labelnames=["method", "endpoint", "status"],
)

ACTIVE_CONNECTIONS = Gauge(
    "http_active_connections",
    "Number of in-flight HTTP requests",
)

# ---------------------------------------------------------------------------
# Pipeline metrics
# ---------------------------------------------------------------------------

_PIPELINE_DURATION_BUCKETS = (
    1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0,
)

PIPELINE_STEP_DURATION = Histogram(
    "pipeline_step_duration_seconds",
    "Pipeline step duration in seconds",
    labelnames=["step"],
    buckets=_PIPELINE_DURATION_BUCKETS,
)

PIPELINE_ERRORS = Counter(
    "pipeline_errors_total",
    "Pipeline step errors",
    labelnames=["step"],
)


# ---------------------------------------------------------------------------
# Pipeline timing helper
# ---------------------------------------------------------------------------

def time_pipeline_step(step: str) -> "_PipelineTimer":
    """Context manager to time and record a pipeline step.

    Usage::

        with time_pipeline_step("fetch_gfs"):
            download_data()
    """
    return _PipelineTimer(step)


class _PipelineTimer:
    def __init__(self, step: str) -> None:
        self._step = step
        self._start = 0.0

    def __enter__(self) -> "_PipelineTimer":
        self._start = time.monotonic()
        return self

    def __exit__(self, exc_type: type | None, *_: object) -> None:
        elapsed = time.monotonic() - self._start
        PIPELINE_STEP_DURATION.labels(step=self._step).observe(elapsed)
        if exc_type is not None:
            PIPELINE_ERRORS.labels(step=self._step).inc()


# ---------------------------------------------------------------------------
# ASGI middleware for automatic HTTP metrics
# ---------------------------------------------------------------------------

def _resolve_endpoint(scope: Scope) -> str:
    """Extract the matched route pattern from the ASGI scope.

    Returns the route template (e.g. "/tiles/{model}/{run_id}/...") rather
    than the actual URL path to keep label cardinality bounded.
    """
    app: ASGIApp | None = scope.get("app")
    if app is None:
        return scope.get("path", "unknown")

    routes = getattr(app, "routes", None)
    if routes is None:
        return scope.get("path", "unknown")

    for route in routes:
        match, _ = route.matches(scope)
        if match == Match.FULL:
            return getattr(route, "path", scope.get("path", "unknown"))

    return scope.get("path", "unknown")


class PrometheusMiddleware:
    """ASGI middleware that records HTTP request metrics.

    Add to a FastAPI/Starlette app::

        app.add_middleware(PrometheusMiddleware)
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        ACTIVE_CONNECTIONS.inc()
        start = time.monotonic()
        status_code = 500  # default in case of unhandled exception

        async def send_wrapper(message: dict) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            elapsed = time.monotonic() - start
            ACTIVE_CONNECTIONS.dec()

            method = scope.get("method", "UNKNOWN")
            endpoint = _resolve_endpoint(scope)
            status = str(status_code)

            REQUEST_LATENCY.labels(
                method=method, endpoint=endpoint, status=status,
            ).observe(elapsed)
            REQUEST_COUNT.labels(
                method=method, endpoint=endpoint, status=status,
            ).inc()


# ---------------------------------------------------------------------------
# /metrics endpoint
# ---------------------------------------------------------------------------

def metrics_endpoint() -> Response:
    """FastAPI/Starlette route handler that serves Prometheus metrics."""
    from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
    )
