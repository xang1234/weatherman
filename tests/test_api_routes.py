"""Tests for catalog & manifest API routes (wx-sap).

Verifies GET /api/catalog/{model} and GET /api/manifest/{model}/{run_id}
against the storage layer, including happy paths and error cases.
"""

from __future__ import annotations

import json

import pytest
from starlette.testclient import TestClient

from weatherman.app import create_app
from weatherman.storage.catalog import RunCatalog, RunStatus
from weatherman.storage.manifest import UIManifest
from weatherman.storage.paths import RunID, StorageLayout


# ---------------------------------------------------------------------------
# Test data builders
# ---------------------------------------------------------------------------

def _make_catalog_json(model: str = "gfs") -> str:
    catalog = RunCatalog.new(model)
    layout = StorageLayout(model)
    catalog.publish_run(RunID("20260306T00Z"), layout=layout)
    return catalog.to_json()


def _make_manifest_json(model: str = "gfs", run_id: str = "20260306T00Z") -> str:
    manifest = UIManifest(
        model=model,
        run_id=run_id,
        cycle_time="2026-03-06T00:00:00+00:00",
        published_at="2026-03-06T01:46:00+00:00",
        resolution_km=25.0,
        layers=[{
            "id": "wind_speed",
            "display_name": "Wind Speed",
            "unit": "m/s",
            "palette_name": "viridis",
            "value_range": {"min": 0.0, "max": 50.0},
        }],
        forecast_hours=[0, 3, 6],
        tile_url_template="/tiles/{model}/{run_id}/{layer}/{forecast_hour}/{z}/{x}/{y}.png",
    )
    return manifest.to_json()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def data_dir(tmp_path):
    """Populate tmp_path with a test catalog and manifest for model 'gfs'."""
    model = "gfs"
    run_id = "20260306T00Z"
    layout = StorageLayout(model)

    # Write catalog
    catalog_path = tmp_path / layout.catalog_path
    catalog_path.parent.mkdir(parents=True, exist_ok=True)
    catalog_path.write_text(_make_catalog_json(model))

    # Write manifest
    manifest_path = tmp_path / layout.manifest_path(RunID(run_id))
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(_make_manifest_json(model, run_id))

    return tmp_path


@pytest.fixture()
def client(data_dir):
    app = create_app(data_dir=str(data_dir), titiler_base_url="http://localhost:9999")
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c


# ---------------------------------------------------------------------------
# GET /api/catalog/{model}
# ---------------------------------------------------------------------------

class TestCatalogEndpoint:
    def test_returns_catalog(self, client):
        resp = client.get("/api/catalog/gfs")
        assert resp.status_code == 200
        body = resp.json()
        assert body["model"] == "gfs"
        assert body["schema_version"] == 1
        assert body["current_run_id"] == "20260306T00Z"
        assert len(body["runs"]) == 1
        assert body["runs"][0]["status"] == "published"

    def test_catalog_not_found(self, client):
        resp = client.get("/api/catalog/nonexistent_model")
        assert resp.status_code == 404
        assert "No catalog found" in resp.json()["detail"]

    def test_catalog_invalid_model_name(self, client):
        resp = client.get("/api/catalog/INVALID")
        assert resp.status_code == 400

    def test_catalog_response_shape(self, client):
        """Verify the response matches what the frontend expects."""
        resp = client.get("/api/catalog/gfs")
        body = resp.json()
        assert set(body.keys()) == {
            "schema_version", "model", "current_run_id", "runs",
        }
        run = body["runs"][0]
        assert "run_id" in run
        assert "status" in run
        assert "published_at" in run


# ---------------------------------------------------------------------------
# GET /api/manifest/{model}/{run_id}
# ---------------------------------------------------------------------------

class TestManifestEndpoint:
    def test_returns_manifest(self, client):
        resp = client.get("/api/manifest/gfs/20260306T00Z")
        assert resp.status_code == 200
        body = resp.json()
        assert body["schema_version"] == 1
        assert body["model"] == "gfs"
        assert body["run_id"] == "20260306T00Z"
        assert body["resolution_km"] == 25.0
        assert len(body["layers"]) == 1
        assert body["forecast_hours"] == [0, 3, 6]
        assert "{model}" in body["tile_url_template"]

    def test_manifest_not_found(self, client):
        resp = client.get("/api/manifest/gfs/20260307T00Z")
        assert resp.status_code == 404
        assert "No manifest found" in resp.json()["detail"]

    def test_manifest_invalid_run_id(self, client):
        resp = client.get("/api/manifest/gfs/bad-id")
        assert resp.status_code == 400

    def test_manifest_invalid_model(self, client):
        resp = client.get("/api/manifest/INVALID/20260306T00Z")
        assert resp.status_code == 400

    def test_manifest_layer_shape(self, client):
        """Verify layer config matches what the frontend expects."""
        resp = client.get("/api/manifest/gfs/20260306T00Z")
        layer = resp.json()["layers"][0]
        assert set(layer.keys()) == {
            "id", "display_name", "unit", "palette_name", "value_range",
        }
        assert set(layer["value_range"].keys()) == {"min", "max"}
