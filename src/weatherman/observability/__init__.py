"""Observability bootstrap — tracing + structured logging + metrics for Weatherman services."""

from weatherman.observability.tracing import get_tracer, setup_tracing, shutdown_tracing
from weatherman.observability.logging import (
    setup_logging, reset_logging,
    bind_context, unbind_context, clear_context,
)
from weatherman.observability.metrics import (
    PrometheusMiddleware,
    metrics_endpoint,
    time_pipeline_step,
)

__all__ = [
    "get_tracer", "setup_tracing", "shutdown_tracing",
    "setup_logging", "reset_logging",
    "bind_context", "unbind_context", "clear_context",
    "PrometheusMiddleware", "metrics_endpoint", "time_pipeline_step",
]
