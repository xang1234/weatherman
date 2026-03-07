"""Observability bootstrap — OpenTelemetry tracing for Weatherman services."""

from weatherman.observability.tracing import setup_tracing, get_tracer

__all__ = ["setup_tracing", "get_tracer"]
