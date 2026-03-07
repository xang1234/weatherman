"""Structured JSON logging with correlation fields.

Configures structlog to wrap Python's stdlib logging so that all existing
``logging.getLogger(__name__)`` calls automatically emit structured JSON
with correlation fields (run_id, request_id, tenant_id) and OTel trace
context (trace_id, span_id).

Usage::

    from weatherman.observability.logging import setup_logging, bind_context

    setup_logging()                       # call once at startup
    bind_context(run_id="20260306T00Z")   # bind correlation fields

After setup, all stdlib loggers produce JSON like::

    {
      "timestamp": "2026-03-06T01:00:00Z",
      "level": "info",
      "event": "Publishing run",
      "service": "weatherman",
      "run_id": "20260306T00Z",
      "trace_id": "abc123...",
      "span_id": "def456...",
      "logger": "weatherman.storage.publish"
    }
"""

from __future__ import annotations

import logging
import os
from typing import Any

import structlog
from structlog.types import EventDict

# Re-export for convenience
bind_context = structlog.contextvars.bind_contextvars
unbind_context = structlog.contextvars.unbind_contextvars
clear_context = structlog.contextvars.clear_contextvars

_LOG_LEVELS = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "critical": logging.CRITICAL,
}

_configured = False


def _add_otel_context(
    logger: Any, method: str, event_dict: EventDict
) -> EventDict:
    """Inject OTel trace_id and span_id from the current span context."""
    try:
        from opentelemetry import trace

        span = trace.get_current_span()
        ctx = span.get_span_context()
        if ctx and ctx.trace_id:
            event_dict["trace_id"] = format(ctx.trace_id, "032x")
            event_dict["span_id"] = format(ctx.span_id, "016x")
    except ImportError:
        pass
    return event_dict


def _add_service_name(
    service_name: str,
) -> structlog.types.Processor:
    """Return a processor that adds the service name to every log entry."""

    def processor(
        logger: Any, method: str, event_dict: EventDict
    ) -> EventDict:
        event_dict["service"] = service_name
        return event_dict

    return processor


def setup_logging(
    service_name: str = "weatherman",
    *,
    log_level: str | None = None,
    json_output: bool = True,
) -> None:
    """Configure structured logging for all services.

    Idempotent — subsequent calls are no-ops.

    Parameters
    ----------
    service_name:
        Identifies the service in log entries (``service`` field).
    log_level:
        Log level string (debug/info/warning/error/critical).
        Falls back to ``LOG_LEVEL`` env var, then ``info``.
    json_output:
        If True (default), render logs as JSON. If False, use a
        human-readable console format (useful for local development).
    """
    global _configured
    if _configured:
        return
    _configured = True

    if log_level is None:
        log_level = os.environ.get("LOG_LEVEL", "info")
    level = _LOG_LEVELS.get(log_level.lower(), logging.INFO)

    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        _add_service_name(service_name),
        _add_otel_context,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
    ]

    if json_output:
        renderer: structlog.types.Processor = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer()

    # Configure structlog itself (for direct structlog.get_logger() usage)
    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # Configure stdlib logging to route through structlog's formatter.
    # This makes existing `logging.getLogger(__name__).info(...)` calls
    # produce structured output.
    formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
        foreign_pre_chain=shared_processors,
    )

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(level)


def reset_logging() -> None:
    """Reset logging configuration. For test teardown only."""
    global _configured
    _configured = False
    structlog.reset_defaults()
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.WARNING)
