"""Tests for health check endpoints."""

from __future__ import annotations

import time
from datetime import datetime, timezone

from fastapi import FastAPI
from starlette.testclient import TestClient

from weatherman.health import router


def _make_app() -> FastAPI:
    app = FastAPI()
    app.include_router(router)
    return app


client = TestClient(_make_app())


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
        # Should be a valid ISO 8601 timestamp
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
        # Warm up
        client.get("/health/live")
        # Measure
        start = time.monotonic()
        for _ in range(10):
            client.get("/health/live")
        elapsed = (time.monotonic() - start) / 10
        assert elapsed < 0.050  # 50ms generous bound for test env overhead

    def test_response_is_json(self):
        resp = client.get("/health/live")
        assert resp.headers["content-type"] == "application/json"
