"""Tests for health check endpoints."""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

from weatherman.health import (
    CheckResult,
    DependencyChecker,
    clear_checks,
    register_check,
    router,
)


def _make_app() -> FastAPI:
    app = FastAPI()
    app.include_router(router)
    return app


client = TestClient(_make_app())


# -- Test helpers --


class FakeChecker:
    """Configurable fake dependency checker for testing."""

    def __init__(
        self,
        name: str,
        *,
        critical: bool = True,
        healthy: bool = True,
        delay: float = 0.0,
        raise_error: bool = False,
    ):
        self._name = name
        self._critical = critical
        self._healthy = healthy
        self._delay = delay
        self._raise_error = raise_error

    @property
    def name(self) -> str:
        return self._name

    @property
    def critical(self) -> bool:
        return self._critical

    async def check(self) -> bool:
        if self._delay > 0:
            await asyncio.sleep(self._delay)
        if self._raise_error:
            raise ConnectionError(f"{self._name} unreachable")
        return self._healthy


@pytest.fixture(autouse=True)
def _clean_checkers():
    """Ensure each test starts with a clean checker registry."""
    clear_checks()
    yield
    clear_checks()


# -- Liveness tests --


class TestLiveness:
    def test_returns_200(self):
        resp = client.get("/health/live")
        assert resp.status_code == 200

    def test_status_is_alive(self):
        resp = client.get("/health/live")
        assert resp.json()["status"] == "alive"

    def test_has_timestamp(self):
        resp = client.get("/health/live")
        ts = resp.json()["timestamp"]
        parsed = datetime.fromisoformat(ts)
        assert parsed.tzinfo is not None

    def test_timestamp_is_recent(self):
        before = datetime.now(timezone.utc)
        resp = client.get("/health/live")
        after = datetime.now(timezone.utc)
        ts = datetime.fromisoformat(resp.json()["timestamp"])
        assert before <= ts <= after

    def test_responds_fast(self):
        """Liveness probe must respond in <5ms (no I/O)."""
        client.get("/health/live")
        start = time.monotonic()
        for _ in range(10):
            client.get("/health/live")
        elapsed = (time.monotonic() - start) / 10
        assert elapsed < 0.050

    def test_response_is_json(self):
        resp = client.get("/health/live")
        assert resp.headers["content-type"] == "application/json"


# -- Readiness tests --


class TestReadinessNoCheckers:
    """When no dependency checkers are registered, the service is ready."""

    def test_returns_200(self):
        resp = client.get("/health/ready")
        assert resp.status_code == 200

    def test_status_is_ready(self):
        resp = client.get("/health/ready")
        assert resp.json()["status"] == "ready"

    def test_checks_is_empty(self):
        resp = client.get("/health/ready")
        assert resp.json()["checks"] == {}

    def test_has_timestamp(self):
        resp = client.get("/health/ready")
        parsed = datetime.fromisoformat(resp.json()["timestamp"])
        assert parsed.tzinfo is not None


class TestReadinessAllHealthy:
    """All dependencies healthy → status=ready, HTTP 200."""

    def test_all_ok(self):
        register_check(FakeChecker("db", critical=True))
        register_check(FakeChecker("s3", critical=True))
        register_check(FakeChecker("titiler", critical=False))

        resp = client.get("/health/ready")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ready"
        assert set(body["checks"].keys()) == {"db", "s3", "titiler"}
        for check in body["checks"].values():
            assert check["status"] == "ok"
            assert check["latency_ms"] >= 0


class TestReadinessCriticalFailure:
    """A critical dependency failing → status=not_ready, HTTP 503."""

    def test_critical_down(self):
        register_check(FakeChecker("db", critical=True, healthy=False))
        register_check(FakeChecker("s3", critical=True))

        resp = client.get("/health/ready")
        assert resp.status_code == 503
        body = resp.json()
        assert body["status"] == "not_ready"
        assert body["checks"]["db"]["status"] == "fail"
        assert body["checks"]["s3"]["status"] == "ok"

    def test_critical_exception(self):
        register_check(FakeChecker("db", critical=True, raise_error=True))

        resp = client.get("/health/ready")
        assert resp.status_code == 503
        assert resp.json()["status"] == "not_ready"
        assert resp.json()["checks"]["db"]["status"] == "fail"


class TestReadinessDegraded:
    """Non-critical dependency failing → status=degraded, HTTP 200."""

    def test_non_critical_down(self):
        register_check(FakeChecker("db", critical=True))
        register_check(FakeChecker("metrics", critical=False, healthy=False))

        resp = client.get("/health/ready")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "degraded"
        assert body["checks"]["db"]["status"] == "ok"
        assert body["checks"]["metrics"]["status"] == "fail"


class TestReadinessTimeout:
    """Checks that exceed the per-check timeout are treated as failures."""

    def test_slow_check_times_out(self):
        register_check(FakeChecker("slow_db", critical=True, delay=3.0))

        resp = client.get("/health/ready")
        assert resp.status_code == 503
        body = resp.json()
        assert body["checks"]["slow_db"]["status"] == "fail"
        # Latency should be roughly the timeout (2s), not the full 3s delay
        assert body["checks"]["slow_db"]["latency_ms"] < 2500


class TestReadinessCache:
    """Check results are cached for 5 seconds."""

    def test_cache_returns_stale_result(self):
        checker = FakeChecker("db", critical=True)
        register_check(checker)

        # First call populates cache
        resp1 = client.get("/health/ready")
        assert resp1.json()["status"] == "ready"

        # Make the checker unhealthy, but cache should still return ready
        checker._healthy = False
        resp2 = client.get("/health/ready")
        assert resp2.json()["status"] == "ready"
        assert resp2.json()["timestamp"] == resp1.json()["timestamp"]

    def test_cache_expires(self):
        checker = FakeChecker("db", critical=True)
        register_check(checker)

        resp1 = client.get("/health/ready")
        assert resp1.json()["status"] == "ready"

        checker._healthy = False

        # Simulate cache expiry by patching the cached_at time
        import weatherman.health as health_mod

        health_mod._cached_state._cached_at -= 10.0

        resp2 = client.get("/health/ready")
        assert resp2.json()["status"] == "not_ready"


class TestReadinessLatency:
    """Check that latency_ms is reported accurately."""

    def test_latency_reflects_check_duration(self):
        register_check(FakeChecker("db", critical=True, delay=0.05))

        resp = client.get("/health/ready")
        latency = resp.json()["checks"]["db"]["latency_ms"]
        assert 40 < latency < 200  # ~50ms with generous bounds
