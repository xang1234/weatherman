"""Observability bootstrap — tracing + structured logging + metrics for Weatherman services."""

from weatherman.observability.tracing import get_tracer, setup_tracing, shutdown_tracing
from weatherman.observability.logging import (
    setup_logging, reset_logging,
    bind_context, unbind_context, clear_context,
)
from weatherman.observability.metrics import (
    AIS_INGEST_TO_VISIBLE_SECONDS,
    AIS_TILE_BYTES,
    AIS_TILE_FEATURES,
    DATA_LAST_PUBLISH,
    EDR_QUERY_DURATION,
    PIPELINE_RUNS,
    PrometheusMiddleware,
    metrics_endpoint,
    time_pipeline_step,
)

__all__ = [
    "get_tracer", "setup_tracing", "shutdown_tracing",
    "setup_logging", "reset_logging",
    "bind_context", "unbind_context", "clear_context",
    "DATA_LAST_PUBLISH", "PIPELINE_RUNS",
    "EDR_QUERY_DURATION",
    "AIS_TILE_BYTES", "AIS_TILE_FEATURES", "AIS_INGEST_TO_VISIBLE_SECONDS",
    "PrometheusMiddleware", "metrics_endpoint", "time_pipeline_step",
]
