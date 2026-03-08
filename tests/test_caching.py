"""Tests for shared HTTP caching utilities and ETag support on catalog/manifest endpoints."""

from __future__ import annotations

import json

import pytest
from starlette.testclient import TestClient

from weatherman.app import create_app
from weatherman.caching import CACHE_IMMUTABLE, CACHE_REVALIDATE, compute_content_etag, etag_matches
from weatherman.storage.catalog import RunCatalog
from weatherman.storage.manifest import LayerConfig, UIManifest, ValueRange
from weatherman.storage.paths import RunID, StorageLayout


# ---------------------------------------------------------------------------
# Unit tests: caching utilities
# ---------------------------------------------------------------------------


class TestComputeContentEtag:
    def test_deterministic(self):
        data = b'{"hello": "world"}'
        assert compute_content_etag(data) == compute_content_etag(data)

    def test_different_content_different_etag(self):
        assert compute_content_etag(b"aaa") != compute_content_etag(b"bbb")

    def test_quoted_format(self):
        etag = compute_content_etag(b"test")
        assert etag.startswith('"')
        assert etag.endswith('"')
        # 16 hex chars inside quotes
        assert len(etag) == 18


class TestEtagMatches:
    def test_exact_match(self):
        assert etag_matches('"abc123"', '"abc123"')

    def test_no_match(self):
        assert not etag_matches('"abc123"', '"def456"')

    def test_wildcard(self):
        assert etag_matches("*", '"anything"')

    def test_multi_value(self):
        assert etag_matches('"aaa", "bbb", "ccc"', '"bbb"')

    def test_weak_etag_stripped(self):
        assert etag_matches('W/"abc123"', '"abc123"')

    def test_no_match_multi_value(self):
        assert not etag_matches('"aaa", "bbb"', '"ccc"')


# ---------------------------------------------------------------------------
# Integration tests: catalog and manifest ETag support
# ---------------------------------------------------------------------------

MODEL = "gfs"
RUN_ID = "20260306T00Z"


@pytest.fixture()
def data_dir(tmp_path):
    """Populate tmp_path with a catalog and manifest for model 'gfs'."""
    layout = StorageLayout(MODEL)

    # Create catalog
    catalog = RunCatalog.new(MODEL)
    catalog.publish_run(RunID(RUN_ID), layout=layout)
    catalog_path = tmp_path / layout.catalog_path
    catalog_path.parent.mkdir(parents=True, exist_ok=True)
    catalog.save(catalog_path)

    # Create manifest
    manifest = UIManifest(
        model=MODEL,
        run_id=RUN_ID,
        cycle_time="2026-03-06T00:00:00+00:00",
        published_at="2026-03-06T01:46:00+00:00",
        resolution_km=25.0,
        layers=[
            LayerConfig(
                id="wind_speed",
                display_name="Wind Speed",
                unit="m/s",
                palette_name="viridis",
                value_range=ValueRange(min=0.0, max=50.0),
            ),
        ],
        forecast_hours=[0, 3, 6],
        tile_url_template="/tiles/{model}/{run_id}/{layer}/{forecast_hour}/{z}/{x}/{y}.png",
    )
    manifest_path = tmp_path / layout.manifest_path(RunID(RUN_ID))
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(manifest.to_json())

    return tmp_path


@pytest.fixture()
def client(data_dir):
    app = create_app(data_dir=str(data_dir), titiler_base_url="http://localhost:9999")
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c


class TestCatalogETag:
    def test_returns_etag_header(self, client):
        resp = client.get(f"/api/catalog/{MODEL}")
        assert resp.status_code == 200
        assert "etag" in resp.headers

    def test_returns_cache_control_revalidate(self, client):
        resp = client.get(f"/api/catalog/{MODEL}")
        assert resp.headers["cache-control"] == CACHE_REVALIDATE

    def test_304_on_matching_etag(self, client):
        resp1 = client.get(f"/api/catalog/{MODEL}")
        etag = resp1.headers["etag"]

        resp2 = client.get(
            f"/api/catalog/{MODEL}",
            headers={"If-None-Match": etag},
        )
        assert resp2.status_code == 304
        assert resp2.headers["etag"] == etag
        assert resp2.headers["cache-control"] == CACHE_REVALIDATE

    def test_200_on_mismatched_etag(self, client):
        resp = client.get(
            f"/api/catalog/{MODEL}",
            headers={"If-None-Match": '"stale-etag"'},
        )
        assert resp.status_code == 200

    def test_etag_deterministic(self, client):
        """Same content → same ETag across requests."""
        etag1 = client.get(f"/api/catalog/{MODEL}").headers["etag"]
        etag2 = client.get(f"/api/catalog/{MODEL}").headers["etag"]
        assert etag1 == etag2


class TestManifestETag:
    def test_returns_etag_header(self, client):
        resp = client.get(f"/api/manifest/{MODEL}/{RUN_ID}")
        assert resp.status_code == 200
        assert "etag" in resp.headers

    def test_returns_cache_control_immutable(self, client):
        resp = client.get(f"/api/manifest/{MODEL}/{RUN_ID}")
        assert resp.headers["cache-control"] == CACHE_IMMUTABLE

    def test_304_on_matching_etag(self, client):
        resp1 = client.get(f"/api/manifest/{MODEL}/{RUN_ID}")
        etag = resp1.headers["etag"]

        resp2 = client.get(
            f"/api/manifest/{MODEL}/{RUN_ID}",
            headers={"If-None-Match": etag},
        )
        assert resp2.status_code == 304
        assert resp2.headers["etag"] == etag
        assert resp2.headers["cache-control"] == CACHE_IMMUTABLE

    def test_200_on_mismatched_etag(self, client):
        resp = client.get(
            f"/api/manifest/{MODEL}/{RUN_ID}",
            headers={"If-None-Match": '"stale-etag"'},
        )
        assert resp.status_code == 200

    def test_304_with_weak_etag(self, client):
        """Weak ETags (W/\"...\") should match per RFC 9110 §8.8.3."""
        resp1 = client.get(f"/api/manifest/{MODEL}/{RUN_ID}")
        etag = resp1.headers["etag"]

        resp2 = client.get(
            f"/api/manifest/{MODEL}/{RUN_ID}",
            headers={"If-None-Match": f"W/{etag}"},
        )
        assert resp2.status_code == 304

    def test_304_with_multi_value_header(self, client):
        """If-None-Match with multiple ETags should match if any matches."""
        resp1 = client.get(f"/api/manifest/{MODEL}/{RUN_ID}")
        etag = resp1.headers["etag"]

        resp2 = client.get(
            f"/api/manifest/{MODEL}/{RUN_ID}",
            headers={"If-None-Match": f'"old-etag", {etag}, "other"'},
        )
        assert resp2.status_code == 304
