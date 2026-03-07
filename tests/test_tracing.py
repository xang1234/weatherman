"""Tests for weatherman.observability.tracing."""

from __future__ import annotations

import random

import pytest
from opentelemetry.sdk.trace.sampling import Decision

from weatherman.observability.tracing import RouteAwareSampler, _extract_path, _is_low_rate

# TraceIdRatioBased compares trace_id against rate * 2^63, so we need
# realistic large trace IDs (not sequential 0..N) for probabilistic tests.
_rng = random.Random(42)
_TRACE_IDS = [_rng.getrandbits(128) for _ in range(2000)]


class TestRouteAwareSampler:
    """Verify route-aware sampling decisions."""

    def setup_method(self) -> None:
        self.sampler = RouteAwareSampler(low_rate=0.01)

    def test_tile_path_uses_low_rate(self) -> None:
        """Tile requests should mostly be dropped at 1% sampling."""
        attrs = {"url.path": "/tiles/gfs/latest/temperature/0/5/10/12.png"}
        decisions = [
            self.sampler.should_sample(None, tid, "GET", attributes=attrs).decision
            for tid in _TRACE_IDS
        ]
        drop_count = sum(1 for d in decisions if d == Decision.DROP)
        # With 1% sampling over 2000 IDs, expect ~1980 drops. Allow wide margin.
        assert drop_count > 1800, f"Expected mostly drops for tiles, got {drop_count}/{len(_TRACE_IDS)}"

    def test_non_tile_path_always_sampled(self) -> None:
        """Non-tile requests should always be sampled."""
        attrs = {"url.path": "/api/v1/runs"}
        decisions = [
            self.sampler.should_sample(None, tid, "GET", attributes=attrs).decision
            for tid in _TRACE_IDS[:100]
        ]
        assert all(d == Decision.RECORD_AND_SAMPLE for d in decisions)

    def test_no_path_always_sampled(self) -> None:
        """When no path attribute is present, sample everything."""
        decisions = [
            self.sampler.should_sample(None, tid, "GET", attributes=None).decision
            for tid in _TRACE_IDS[:100]
        ]
        assert all(d == Decision.RECORD_AND_SAMPLE for d in decisions)

    def test_legacy_http_target_attribute(self) -> None:
        """Should also recognise the legacy http.target attribute."""
        attrs = {"http.target": "/tiles/gfs/latest/wind/0/3/4/5.png"}
        decisions = [
            self.sampler.should_sample(None, tid, "GET", attributes=attrs).decision
            for tid in _TRACE_IDS
        ]
        drop_count = sum(1 for d in decisions if d == Decision.DROP)
        assert drop_count > 1800

    def test_description(self) -> None:
        desc = self.sampler.get_description()
        assert "RouteAwareSampler" in desc


class TestHelpers:
    def test_extract_path_url_path(self) -> None:
        assert _extract_path({"url.path": "/foo"}) == "/foo"

    def test_extract_path_http_target(self) -> None:
        assert _extract_path({"http.target": "/bar"}) == "/bar"

    def test_extract_path_prefers_url_path(self) -> None:
        assert _extract_path({"url.path": "/a", "http.target": "/b"}) == "/a"

    def test_extract_path_none(self) -> None:
        assert _extract_path(None) is None
        assert _extract_path({}) is None

    def test_is_low_rate_tiles(self) -> None:
        assert _is_low_rate("/tiles/gfs/latest/temp/0/1/2/3.png") is True

    def test_is_low_rate_other(self) -> None:
        assert _is_low_rate("/api/runs") is False

    def test_is_low_rate_none(self) -> None:
        assert _is_low_rate(None) is False


class TestGetTracer:
    def test_returns_tracer(self) -> None:
        from weatherman.observability.tracing import get_tracer

        tracer = get_tracer("test-service")
        assert tracer is not None
