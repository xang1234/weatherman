"""Tests for structured JSON logging with correlation fields."""

import json
import logging
from unittest.mock import patch

import pytest
import structlog

from weatherman.observability.logging import (
    bind_context,
    clear_context,
    reset_logging,
    setup_logging,
    unbind_context,
)


@pytest.fixture(autouse=True)
def _clean_logging():
    """Reset logging state before and after each test."""
    reset_logging()
    clear_context()
    yield
    reset_logging()
    clear_context()


def _capture_log(logger_name: str, level: str, message: str, **kwargs) -> dict:
    """Emit a log line and capture the JSON output."""
    import io

    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    # Grab the formatter from the root logger (set up by setup_logging)
    root = logging.getLogger()
    if root.handlers:
        handler.setFormatter(root.handlers[0].formatter)
    root.addHandler(handler)
    try:
        logger = logging.getLogger(logger_name)
        getattr(logger, level)(message, **kwargs)
        handler.flush()
        return json.loads(buf.getvalue())
    finally:
        root.removeHandler(handler)


class TestSetupLogging:
    def test_idempotent(self):
        setup_logging()
        handlers_after_first = len(logging.getLogger().handlers)
        setup_logging()  # second call is no-op
        assert len(logging.getLogger().handlers) == handlers_after_first

    def test_root_logger_has_handler(self):
        setup_logging()
        root = logging.getLogger()
        assert len(root.handlers) == 1

    def test_default_level_is_info(self):
        setup_logging()
        root = logging.getLogger()
        assert root.level == logging.INFO

    def test_level_from_env(self):
        with patch.dict("os.environ", {"LOG_LEVEL": "DEBUG"}):
            setup_logging()
        assert logging.getLogger().level == logging.DEBUG

    def test_explicit_level_overrides_env(self):
        with patch.dict("os.environ", {"LOG_LEVEL": "DEBUG"}):
            setup_logging(log_level="error")
        assert logging.getLogger().level == logging.ERROR

    def test_case_insensitive_level(self):
        setup_logging(log_level="WARNING")
        assert logging.getLogger().level == logging.WARNING


class TestJsonOutput:
    def test_output_is_valid_json(self):
        setup_logging()
        entry = _capture_log("test.module", "info", "hello")
        assert isinstance(entry, dict)

    def test_has_timestamp(self):
        setup_logging()
        entry = _capture_log("test.module", "info", "hello")
        assert "timestamp" in entry

    def test_has_level(self):
        setup_logging()
        entry = _capture_log("test.module", "info", "hello")
        assert entry["level"] == "info"

    def test_has_event(self):
        setup_logging()
        entry = _capture_log("test.module", "info", "Publishing run")
        assert entry["event"] == "Publishing run"

    def test_has_service_name(self):
        setup_logging(service_name="ingest-worker")
        entry = _capture_log("test.module", "info", "hello")
        assert entry["service"] == "ingest-worker"

    def test_has_logger_name(self):
        setup_logging()
        entry = _capture_log("weatherman.storage.publish", "info", "hello")
        assert entry["logger"] == "weatherman.storage.publish"


class TestCorrelationFields:
    def test_bind_run_id(self):
        setup_logging()
        bind_context(run_id="20260306T00Z")
        entry = _capture_log("test", "info", "processing")
        assert entry["run_id"] == "20260306T00Z"

    def test_bind_request_id(self):
        setup_logging()
        bind_context(request_id="req-abc-123")
        entry = _capture_log("test", "info", "handling request")
        assert entry["request_id"] == "req-abc-123"

    def test_bind_tenant_id(self):
        setup_logging()
        bind_context(tenant_id="tenant-42")
        entry = _capture_log("test", "info", "tenant op")
        assert entry["tenant_id"] == "tenant-42"

    def test_bind_multiple_fields(self):
        setup_logging()
        bind_context(run_id="20260306T00Z", request_id="req-1", tenant_id="t1")
        entry = _capture_log("test", "info", "combined")
        assert entry["run_id"] == "20260306T00Z"
        assert entry["request_id"] == "req-1"
        assert entry["tenant_id"] == "t1"

    def test_unbind_removes_field(self):
        setup_logging()
        bind_context(run_id="20260306T00Z", request_id="req-1")
        unbind_context("request_id")
        entry = _capture_log("test", "info", "after unbind")
        assert entry["run_id"] == "20260306T00Z"
        assert "request_id" not in entry

    def test_clear_context_removes_all(self):
        setup_logging()
        bind_context(run_id="20260306T00Z", tenant_id="t1")
        clear_context()
        entry = _capture_log("test", "info", "after clear")
        assert "run_id" not in entry
        assert "tenant_id" not in entry


class TestOtelTraceContext:
    def test_trace_id_and_span_id_injected(self):
        """When an OTel span is active, trace_id and span_id appear in logs."""
        setup_logging()
        from opentelemetry import trace
        from opentelemetry.sdk.trace import TracerProvider

        provider = TracerProvider()
        tracer = provider.get_tracer("test")
        with tracer.start_as_current_span("test-span"):
            entry = _capture_log("test", "info", "inside span")
        provider.shutdown()

        assert "trace_id" in entry
        assert "span_id" in entry
        assert len(entry["trace_id"]) == 32  # 128-bit hex
        assert len(entry["span_id"]) == 16  # 64-bit hex

    def test_no_span_no_trace_fields(self):
        """Without an active span, trace_id/span_id are absent."""
        setup_logging()
        entry = _capture_log("test", "info", "no span")
        assert "trace_id" not in entry


class TestExceptionLogging:
    def test_exception_info_included(self):
        setup_logging()
        try:
            raise ValueError("test error")
        except ValueError:
            entry = _capture_log("test", "error", "oh no", exc_info=True)
        assert "ValueError: test error" in entry.get("exception", "")


class TestConsoleOutput:
    def test_console_mode_does_not_crash(self):
        """Console renderer should work without errors."""
        setup_logging(json_output=False)
        logger = logging.getLogger("test.console")
        # Just verify it doesn't raise
        logger.info("hello from console mode")


class TestResetLogging:
    def test_reset_allows_reconfigure(self):
        setup_logging(service_name="first")
        entry1 = _capture_log("test", "info", "first")
        assert entry1["service"] == "first"

        reset_logging()
        setup_logging(service_name="second")
        entry2 = _capture_log("test", "info", "second")
        assert entry2["service"] == "second"
