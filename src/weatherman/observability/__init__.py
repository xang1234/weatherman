"""Observability bootstrap — OpenTelemetry tracing for Weatherman services."""

from weatherman.observability.tracing import get_tracer, setup_tracing, shutdown_tracing

__all__ = ["get_tracer", "setup_tracing", "shutdown_tracing"]
