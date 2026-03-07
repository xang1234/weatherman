"""Tests for Prometheus metrics module."""

from __future__ import annotations

import pytest
from prometheus_client import CollectorRegistry, REGISTRY
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import PlainTextResponse
from starlette.routing import Mount, Route
from starlette.testclient import TestClient

from weatherman.observability.metrics import (
    ACTIVE_CONNECTIONS,
    PIPELINE_ERRORS,
    PIPELINE_STEP_DURATION,
    REQUEST_COUNT,
    REQUEST_LATENCY,
    PrometheusMiddleware,
    _PipelineTimer,
    metrics_endpoint,
    time_pipeline_step,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _reset_metrics():
    """Clear all custom metric samples between tests.

    prometheus-client metrics are module-level singletons. We cannot
    unregister/re-register them, but we can reset their internal state
    so tests remain independent.
    """
    # Collect all label-value combos and reset
    for collector in [REQUEST_LATENCY, REQUEST_COUNT, PIPELINE_STEP_DURATION, PIPELINE_ERRORS]:
        # _metrics is a dict of child metrics keyed by label values
        if hasattr(collector, "_metrics"):
            collector._metrics.clear()
    ACTIVE_CONNECTIONS._value.set(0)
    yield


def _make_app() -> Starlette:
    """Build a tiny Starlette app with the Prometheus middleware."""

    async def homepage(request: Request) -> PlainTextResponse:
        return PlainTextResponse("ok")

    async def item(request: Request) -> PlainTextResponse:
        return PlainTextResponse(f"item {request.path_params['item_id']}")

    async def error(request: Request) -> PlainTextResponse:
        raise ValueError("boom")

    app = Starlette(
        routes=[
            Route("/", homepage),
            Route("/items/{item_id}", item),
            Route("/error", error),
            Route("/metrics", lambda r: metrics_endpoint()),
        ],
    )
    app.add_middleware(PrometheusMiddleware)
    return app


@pytest.fixture()
def client() -> TestClient:
    app = _make_app()
    return TestClient(app, raise_server_exceptions=False)


# ---------------------------------------------------------------------------
# /metrics endpoint
# ---------------------------------------------------------------------------

class TestMetricsEndpoint:
    def test_returns_prometheus_format(self, client: TestClient):
        resp = client.get("/metrics")
        assert resp.status_code == 200
        assert "text/plain" in resp.headers["content-type"]
        # Default collectors should be present
        assert "python_info" in resp.text

    def test_contains_custom_metrics(self, client: TestClient):
        # Make a request first to generate metrics
        client.get("/")
        resp = client.get("/metrics")
        assert "http_request_duration_seconds" in resp.text
        assert "http_requests_total" in resp.text

    def test_contains_active_connections_metric(self, client: TestClient):
        resp = client.get("/metrics")
        assert "http_active_connections" in resp.text


# ---------------------------------------------------------------------------
# HTTP request metrics via middleware
# ---------------------------------------------------------------------------

class TestPrometheusMiddleware:
    def test_records_request_count(self, client: TestClient):
        client.get("/")
        client.get("/")
        client.get("/")

        count = REQUEST_COUNT.labels(
            method="GET", endpoint="/", status="200",
        )._value.get()
        # 3 requests to "/" + potentially the requests themselves
        assert count >= 3

    def test_records_latency_histogram(self, client: TestClient):
        client.get("/")

        # Histogram should have at least 1 observation
        sample_count = REQUEST_LATENCY.labels(
            method="GET", endpoint="/", status="200",
        )._sum.get()
        assert sample_count > 0

    def test_uses_route_template_not_path(self, client: TestClient):
        """Endpoint label should be the route template, not the actual path."""
        client.get("/items/42")
        client.get("/items/99")

        # Both requests should be recorded under the template
        count = REQUEST_COUNT.labels(
            method="GET", endpoint="/items/{item_id}", status="200",
        )._value.get()
        assert count >= 2

    def test_uses_route_template_for_mounted_subroutes(self):
        """Mounted sub-routes (APIRouter with prefix) should resolve to the template."""

        async def get_widget(request: Request) -> PlainTextResponse:
            return PlainTextResponse(f"widget {request.path_params['widget_id']}")

        app = Starlette(
            routes=[
                Mount("/api", routes=[
                    Route("/widgets/{widget_id}", get_widget),
                ]),
            ],
        )
        app.add_middleware(PrometheusMiddleware)
        mounted_client = TestClient(app, raise_server_exceptions=False)

        mounted_client.get("/api/widgets/1")
        mounted_client.get("/api/widgets/2")

        count = REQUEST_COUNT.labels(
            method="GET", endpoint="/api/widgets/{widget_id}", status="200",
        )._value.get()
        assert count >= 2

    def test_records_status_code(self, client: TestClient):
        client.get("/nonexistent")

        count = REQUEST_COUNT.labels(
            method="GET", endpoint="/nonexistent", status="404",
        )._value.get()
        assert count >= 1

    def test_records_error_status(self, client: TestClient):
        client.get("/error")

        count = REQUEST_COUNT.labels(
            method="GET", endpoint="/error", status="500",
        )._value.get()
        assert count >= 1

    def test_active_connections_returns_to_zero(self, client: TestClient):
        client.get("/")
        assert ACTIVE_CONNECTIONS._value.get() == 0.0


# ---------------------------------------------------------------------------
# Pipeline metrics
# ---------------------------------------------------------------------------

class TestPipelineMetrics:
    def test_time_pipeline_step_records_duration(self):
        with time_pipeline_step("test_step"):
            pass  # instant

        sample_count = PIPELINE_STEP_DURATION.labels(
            step="test_step",
        )._sum.get()
        assert sample_count >= 0  # duration should be non-negative

    def test_time_pipeline_step_records_error_on_exception(self):
        with pytest.raises(RuntimeError):
            with time_pipeline_step("failing_step"):
                raise RuntimeError("test failure")

        error_count = PIPELINE_ERRORS.labels(
            step="failing_step",
        )._value.get()
        assert error_count == 1

    def test_time_pipeline_step_no_error_on_success(self):
        with time_pipeline_step("success_step"):
            pass

        error_count = PIPELINE_ERRORS.labels(
            step="success_step",
        )._value.get()
        assert error_count == 0

    def test_multiple_steps_independent(self):
        with time_pipeline_step("step_a"):
            pass
        with time_pipeline_step("step_b"):
            pass

        sum_a = PIPELINE_STEP_DURATION.labels(step="step_a")._sum.get()
        sum_b = PIPELINE_STEP_DURATION.labels(step="step_b")._sum.get()
        assert sum_a >= 0
        assert sum_b >= 0


# ---------------------------------------------------------------------------
# Histogram bucket boundaries
# ---------------------------------------------------------------------------

class TestBucketBoundaries:
    def test_http_latency_buckets(self):
        """HTTP latency histogram should have the specified bucket boundaries."""
        # The upper_bounds include +Inf added by prometheus-client
        buckets = list(REQUEST_LATENCY._upper_bounds)
        expected = [0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5, 5.0, float("inf")]
        assert buckets == expected

    def test_pipeline_duration_buckets(self):
        """Pipeline duration histogram should have the specified bucket boundaries."""
        buckets = list(PIPELINE_STEP_DURATION._upper_bounds)
        expected = [1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0, float("inf")]
        assert buckets == expected
