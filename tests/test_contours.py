"""Tests for isobar contouring and the /api/contours endpoint (#23)."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest
import zarr
from fastapi import FastAPI
from fastapi.testclient import TestClient

from weatherman.edr.contours import _accepts_gzip, _isobars_json, router
from weatherman.edr.position import init_edr_service, shutdown_edr_service
from weatherman.processing.contours import INTERVAL_HPA, isobars_geojson
from weatherman.storage.catalog import RunCatalog
from weatherman.storage.paths import RunID, StorageLayout

# 1° global grid, lat descending, lon -180..179 like the Zarr store.
LAT = np.arange(90.0, -90.5, -1.0)
LON = np.arange(-180.0, 180.0, 1.0)


def _pressure_pa(*blobs: tuple[float, float, float]) -> np.ndarray:
    """1012 hPa background plus Gaussian blobs (lon, lat, hPa), in Pa."""
    lat2d, lon2d = np.meshgrid(LAT, LON, indexing="ij")
    hpa = np.full(lat2d.shape, 1012.0)
    for lon0, lat0, amp in blobs:
        dlon = (lon2d - lon0 + 180.0) % 360.0 - 180.0
        hpa += amp * np.exp(-(dlon**2 + (lat2d - lat0) ** 2) / (2 * 8.0**2))
    return hpa * 100.0


def _features(geojson: dict, kind: str) -> list[dict]:
    return [f for f in geojson["features"] if f["properties"]["kind"] == kind]


def test_centres_where_the_highs_and_lows_are():
    geojson = isobars_geojson(_pressure_pa((30, 45, -30), (-60, -30, 25)), LAT, LON)

    (low,) = _features(geojson, "low")
    (high,) = _features(geojson, "high")
    assert low["geometry"]["coordinates"] == pytest.approx([30, 45], abs=2)
    assert high["geometry"]["coordinates"] == pytest.approx([-60, -30], abs=2)
    assert low["properties"]["hpa"] < 1000 < 1020 < high["properties"]["hpa"]


def test_isobars_every_interval():
    geojson = isobars_geojson(_pressure_pa((30, 45, -30)), LAT, LON)
    levels = {f["properties"]["hpa"] for f in _features(geojson, "isobar")}

    assert levels, "no isobars"
    assert all(level % INTERVAL_HPA == 0 for level in levels)
    assert max(levels) - min(levels) >= 4 * INTERVAL_HPA


def test_isobars_meet_across_the_antimeridian():
    """A low on the date line: the pieces either side end at the same latitudes."""
    geojson = isobars_geojson(_pressure_pa((180, 0, -30)), LAT, LON)

    seam_crossings = 0
    for level in {f["properties"]["hpa"] for f in _features(geojson, "isobar")}:
        ends = [
            point
            for f in _features(geojson, "isobar")
            if f["properties"]["hpa"] == level
            for point in (f["geometry"]["coordinates"][0], f["geometry"]["coordinates"][-1])
        ]
        east = sorted(round(lat) for lon, lat in ends if lon >= 179.5)
        west = sorted(round(lat) for lon, lat in ends if lon <= -179.5)
        assert east == west, f"{level} hPa: ends at 180° {east} vs -180° {west}"
        seam_crossings += len(east)
    assert seam_crossings > 0, "no isobar reached the antimeridian"


@pytest.fixture()
def client(tmp_path: Path):
    model, run_id = "gfs", RunID("20260306T00Z")
    root = zarr.open_group(str(tmp_path / StorageLayout(model).zarr_path(run_id)), mode="w")
    root.create_array("lat", data=LAT)
    root.create_array("lon", data=LON)
    root.create_array("time", data=np.array([0, 3], dtype=np.int32))
    field = _pressure_pa((30, 45, -30))
    root.create_array("prmsl", data=np.stack([field, field]).astype(np.float32))

    catalog = RunCatalog.new(model)
    catalog.publish_run(run_id, layout=StorageLayout(model))
    shutdown_edr_service()
    init_edr_service(lambda m: catalog, lambda p: zarr.open_group(str(tmp_path / p), mode="r"))
    _isobars_json.cache_clear()
    app = FastAPI()
    app.include_router(router)
    yield TestClient(app)
    shutdown_edr_service()
    _isobars_json.cache_clear()


def test_endpoint_returns_isobars(client: TestClient):
    response = client.get("/api/contours/gfs/20260306T00Z/prmsl/3", headers={"Accept-Encoding": "identity"})

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/geo+json"
    assert "immutable" in response.headers["cache-control"]
    assert _features(response.json(), "low")


def test_endpoint_gzips_when_accepted(client: TestClient):
    response = client.get("/api/contours/gfs/latest/prmsl/0", headers={"Accept-Encoding": "gzip"})

    assert response.status_code == 200
    assert response.headers["content-encoding"] == "gzip"
    # TestClient decodes gzip itself, so the body is the GeoJSON either way.
    assert response.json()["type"] == "FeatureCollection"
    # "latest" moves on with each new run, so it must not be cached for long.
    assert "immutable" not in response.headers["cache-control"]


def test_endpoint_404s(client: TestClient):
    assert client.get("/api/contours/gfs/20260306T00Z/prmsl/9").status_code == 404
    assert client.get("/api/contours/gfs/20991231T00Z/prmsl/0").status_code == 404


@pytest.mark.parametrize(("header", "expected"), [
    ("gzip", True),
    ("gzip, deflate, br", True),
    ("br;q=1.0, gzip;q=0.8", True),
    ("*", True),
    (None, False),
    ("identity", False),
    ("gzip;q=0", False),
    ("*;q=1, gzip;q=0", False),
    ("gzip;q=0.0, *", False),
])
def test_accepts_gzip(header, expected):
    assert _accepts_gzip(header) is expected
