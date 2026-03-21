"""Tests for the EDR trajectory endpoint."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest
import zarr
from fastapi.testclient import TestClient

from weatherman.app import create_app
from weatherman.storage.catalog import RunCatalog
from weatherman.storage.paths import RunID, StorageLayout


@pytest.fixture
def zarr_store(tmp_path: Path) -> Path:
    """Create a minimal Zarr store for trajectory testing."""
    layout = StorageLayout("gfs")
    run_id = RunID("20260321T00Z")
    zarr_path = tmp_path / layout.zarr_path(run_id)
    zarr_path.parent.mkdir(parents=True, exist_ok=True)

    root = zarr.open_group(str(zarr_path), mode="w")

    # Small 5×5 grid for fast tests
    lats = np.array([2.0, 1.0, 0.0, -1.0, -2.0], dtype=np.float32)
    lons = np.array([-2.0, -1.0, 0.0, 1.0, 2.0], dtype=np.float32)
    times = np.array([0, 3, 6], dtype=np.int32)

    root.create_array("lat", data=lats)
    root.create_array("lon", data=lons)
    root.create_array("time", data=times)

    # Temperature: constant 300.0 everywhere
    tmp = root.create_array(
        "tmp_2m", shape=(3, 5, 5), dtype="float32", fill_value=np.nan,
    )
    tmp[:] = 300.0
    tmp.attrs["long_name"] = "Temperature"
    tmp.attrs["units"] = "K"

    return tmp_path


@pytest.fixture
def catalog(tmp_path: Path) -> Path:
    """Create a catalog with one published run."""
    layout = StorageLayout("gfs")
    run_id = RunID("20260321T00Z")
    catalog = RunCatalog.new("gfs")
    catalog.publish_run(run_id, layout=layout, processing_version="test")
    catalog_path = tmp_path / layout.catalog_path
    catalog_path.parent.mkdir(parents=True, exist_ok=True)
    catalog.save(catalog_path)
    return tmp_path


@pytest.fixture
def client(zarr_store: Path, catalog: Path) -> TestClient:
    """Create a test client with the Zarr store and catalog."""
    # Both fixtures write to tmp_path; merge by using zarr_store as data_dir
    # and copying catalog into it
    layout = StorageLayout("gfs")
    catalog_src = catalog / layout.catalog_path
    catalog_dst = zarr_store / layout.catalog_path
    catalog_dst.parent.mkdir(parents=True, exist_ok=True)
    catalog_dst.write_bytes(catalog_src.read_bytes())

    app = create_app(data_dir=str(zarr_store), titiler_base_url="http://localhost:9999")
    with TestClient(app) as c:
        yield c


def test_trajectory_returns_coverage(client: TestClient) -> None:
    """POST trajectory returns CoverageJSON-like response with correct shape."""
    resp = client.post(
        "/v1/edr/collections/gfs/instances/20260321T00Z/trajectory",
        json={
            "type": "LineString",
            "coordinates": [[-1.0, -1.0], [1.0, 1.0]],
            "num_samples": 5,
        },
    )
    assert resp.status_code == 200
    data = resp.json()

    assert data["type"] == "Coverage"
    assert "tmp_2m" in data["ranges"]
    assert data["ranges"]["tmp_2m"]["shape"] == [5, 3]

    # All values should be ~300.0 (constant grid)
    values = data["ranges"]["tmp_2m"]["values"]
    assert len(values) == 5
    assert len(values[0]) == 3
    assert values[0][0] == pytest.approx(300.0, abs=1.0)


def test_trajectory_route_distances(client: TestClient) -> None:
    """Route metadata includes distances and total."""
    resp = client.post(
        "/v1/edr/collections/gfs/instances/20260321T00Z/trajectory",
        json={
            "type": "LineString",
            "coordinates": [[-1.0, 0.0], [1.0, 0.0]],
            "num_samples": 3,
        },
    )
    data = resp.json()
    route = data["route"]

    assert route["distances_nm"][0] == 0.0
    assert route["total_nm"] > 100  # ~120 nm
    assert len(route["distances_nm"]) == 3


def test_trajectory_with_speed(client: TestClient) -> None:
    """Speed parameter adds ETA hours to response."""
    resp = client.post(
        "/v1/edr/collections/gfs/instances/20260321T00Z/trajectory",
        json={
            "type": "LineString",
            "coordinates": [[-1.0, 0.0], [1.0, 0.0]],
            "num_samples": 3, "speed_knots": 12.0,
        },
    )
    data = resp.json()
    assert "eta_hours" in data["route"]
    assert data["route"]["eta_hours"][0] == 0.0
    assert data["route"]["speed_knots"] == 12.0


def test_trajectory_etag_caching(client: TestClient) -> None:
    """Second request with matching ETag returns 304."""
    body = {
        "type": "LineString",
        "coordinates": [[-1.0, 0.0], [1.0, 0.0]],
        "num_samples": 3,
    }
    resp1 = client.post(
        "/v1/edr/collections/gfs/instances/20260321T00Z/trajectory",
        json=body,
    )
    etag = resp1.headers["etag"]

    resp2 = client.post(
        "/v1/edr/collections/gfs/instances/20260321T00Z/trajectory",
        json=body,
        headers={"If-None-Match": etag},
    )
    assert resp2.status_code == 304


def test_trajectory_latest_resolves(client: TestClient) -> None:
    """'latest' run_id resolves to the current published run."""
    resp = client.post(
        "/v1/edr/collections/gfs/instances/latest/trajectory",
        json={
            "type": "LineString",
            "coordinates": [[-1.0, 0.0], [1.0, 0.0]],
            "num_samples": 3,
        },
    )
    assert resp.status_code == 200


def test_trajectory_invalid_type(client: TestClient) -> None:
    resp = client.post(
        "/v1/edr/collections/gfs/instances/20260321T00Z/trajectory",
        json={"type": "Point", "coordinates": [[0, 0], [1, 1]]},
    )
    assert resp.status_code == 400


def test_trajectory_too_many_samples(client: TestClient) -> None:
    resp = client.post(
        "/v1/edr/collections/gfs/instances/20260321T00Z/trajectory",
        json={
            "type": "LineString",
            "coordinates": [[-1, 0], [1, 0]],
            "num_samples": 500,
        },
    )
    assert resp.status_code == 422
