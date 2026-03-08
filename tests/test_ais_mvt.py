"""Tests for AIS MVT tile generation and router."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import mapbox_vector_tile as mvt
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from weatherman.ais.ingest import load_day
from weatherman.ais.mvt import (
    DEFAULT_EXTENT,
    MAX_ZOOM,
    MIN_ZOOM,
    generate_tile,
    tile_bounds,
)
from weatherman.ais.router import (
    AISTileService,
    init_ais_tile_service,
    router,
    shutdown_ais_tile_service,
)
from weatherman.ais.snapshot import build_snapshot
from tests.conftest_ais import (
    ROW_BULK_CARRIER,
    ROW_GRAIN_STAR,
    _write_test_parquet,
    ais_con,
    ais_db,
    parquet_dir,
)

SNAPSHOT_DATE = date(2025, 12, 25)
TENANT = "default"


# -- Fixtures --


@pytest.fixture()
def snapshot_con(
    parquet_dir: Path, ais_con: duckdb.DuckDBPyConnection
) -> duckdb.DuckDBPyConnection:
    """Connection with loaded data and built snapshot."""
    load_day(
        f"{parquet_dir}/movement_date=2025-12-25/*",
        load_date=SNAPSHOT_DATE,
        tenant_id=TENANT,
        con=ais_con,
    )
    build_snapshot(snapshot_date=SNAPSHOT_DATE, tenant_id=TENANT, con=ais_con)
    return ais_con


# -- tile_bounds tests --


class TestTileBounds:
    def test_z0_covers_world(self):
        west, south, east, north = tile_bounds(0, 0, 0)
        assert west == pytest.approx(-180.0)
        assert east == pytest.approx(180.0)
        assert north == pytest.approx(85.05, abs=0.1)
        assert south == pytest.approx(-85.05, abs=0.1)

    def test_z1_quadrants(self):
        """z1 has 4 tiles; top-left (0,0) covers western/northern hemisphere."""
        w, s, e, n = tile_bounds(1, 0, 0)
        assert w == pytest.approx(-180.0)
        assert e == pytest.approx(0.0)
        assert n > 0  # northern hemisphere

    def test_higher_zoom_smaller_bounds(self):
        w0, s0, e0, n0 = tile_bounds(0, 0, 0)
        w4, s4, e4, n4 = tile_bounds(4, 8, 5)
        # Higher zoom tiles are smaller
        assert (e4 - w4) < (e0 - w0)
        assert (n4 - s4) < (n0 - s0)


# -- generate_tile tests --


class TestGenerateTile:
    def test_z0_tile_contains_all_vessels(self, snapshot_con):
        """Zoom 0 has one tile covering the whole world — all vessels appear."""
        tile_bytes = generate_tile(
            con=snapshot_con,
            snapshot_date=SNAPSHOT_DATE,
            tenant_id=TENANT,
            z=0,
            x=0,
            y=0,
        )
        assert len(tile_bytes) > 0
        decoded = mvt.decode(tile_bytes)
        assert "vessels" in decoded
        features = decoded["vessels"]["features"]
        assert len(features) == 2

    def test_tile_properties_present(self, snapshot_con):
        """Tile features include the required rendering properties."""
        tile_bytes = generate_tile(
            con=snapshot_con,
            snapshot_date=SNAPSHOT_DATE,
            tenant_id=TENANT,
            z=0,
            x=0,
            y=0,
        )
        decoded = mvt.decode(tile_bytes)
        features = decoded["vessels"]["features"]

        # Find the bulk carrier by MMSI
        bulk = next(f for f in features if f["properties"]["mmsi"] == 211234567)
        props = bulk["properties"]

        assert props["vessel_name"] == "MV BULK CARRIER"
        assert props["shiptype"] == "Cargo"
        assert props["vessel_class"] == "Capesize"
        assert props["sog"] == pytest.approx(12.5)
        assert props["heading"] == pytest.approx(245.0)
        assert props["dwt"] == 180000
        assert props["destination"] == "SINGAPORE"
        assert props["destinationtidied"] == "Singapore"
        assert "eta" in props  # ISO string

    def test_empty_tile_returns_empty_bytes(self, snapshot_con):
        """A tile with no vessels should return empty bytes."""
        # z=4, x=0, y=0 is far north/west — no vessels there in our test data
        tile_bytes = generate_tile(
            con=snapshot_con,
            snapshot_date=SNAPSHOT_DATE,
            tenant_id=TENANT,
            z=4,
            x=0,
            y=0,
        )
        assert tile_bytes == b""

    def test_tile_for_wrong_date_is_empty(self, snapshot_con):
        """No data for a different date."""
        tile_bytes = generate_tile(
            con=snapshot_con,
            snapshot_date=date(2025, 1, 1),
            tenant_id=TENANT,
            z=0,
            x=0,
            y=0,
        )
        assert tile_bytes == b""

    def test_tile_for_wrong_tenant_is_empty(self, snapshot_con):
        """Tenant isolation — other tenants see no data."""
        tile_bytes = generate_tile(
            con=snapshot_con,
            snapshot_date=SNAPSHOT_DATE,
            tenant_id="other-tenant",
            z=0,
            x=0,
            y=0,
        )
        assert tile_bytes == b""

    def test_higher_zoom_isolates_vessels(self, snapshot_con):
        """At higher zoom, vessels in different locations appear in different tiles.

        Bulk carrier: lat=1.35, lon=103.8 (Singapore)
        Grain star: lat=51.5, lon=-0.1 (London)
        At z=4 they should be in different tiles.
        """
        # Singapore area at z4: x=13, y=7 (roughly)
        # London area at z4: x=7, y=5 (roughly)
        # Find which z4 tile contains each vessel
        singapore_found = False
        london_found = False

        for x in range(16):
            for y in range(16):
                tile_bytes = generate_tile(
                    con=snapshot_con,
                    snapshot_date=SNAPSHOT_DATE,
                    tenant_id=TENANT,
                    z=4,
                    x=x,
                    y=y,
                )
                if not tile_bytes:
                    continue
                decoded = mvt.decode(tile_bytes)
                features = decoded["vessels"]["features"]
                mmsis = {f["properties"]["mmsi"] for f in features}
                if 211234567 in mmsis:
                    singapore_found = True
                    assert 311234567 not in mmsis, "Vessels should be in different tiles at z4"
                if 311234567 in mmsis:
                    london_found = True
                    assert 211234567 not in mmsis, "Vessels should be in different tiles at z4"

        assert singapore_found, "Bulk carrier (Singapore) not found in any z4 tile"
        assert london_found, "Grain Star (London) not found in any z4 tile"

    def test_null_properties_omitted(self, snapshot_con):
        """Null string properties should be omitted to save tile size."""
        tile_bytes = generate_tile(
            con=snapshot_con,
            snapshot_date=SNAPSHOT_DATE,
            tenant_id=TENANT,
            z=0,
            x=0,
            y=0,
        )
        decoded = mvt.decode(tile_bytes)
        for feat in decoded["vessels"]["features"]:
            for key, val in feat["properties"].items():
                # No None values should appear
                assert val is not None, f"Property '{key}' is None"


# -- Router / endpoint tests --


@pytest.fixture()
def snapshot_db_path(tmp_path: Path, parquet_dir: Path) -> str:
    """Create a DuckDB file with snapshot data for router tests."""
    from weatherman.ais.db import AISDatabase

    db_path = str(tmp_path / "ais_test.duckdb")
    db = AISDatabase(db_path)
    con = db.connect()
    load_day(
        f"{parquet_dir}/movement_date=2025-12-25/*",
        load_date=SNAPSHOT_DATE,
        tenant_id=TENANT,
        con=con,
    )
    build_snapshot(snapshot_date=SNAPSHOT_DATE, tenant_id=TENANT, con=con)
    db.close()
    return db_path


@pytest.fixture()
def app(snapshot_db_path: str):
    """FastAPI app with the AIS tile router."""
    import weatherman.ais.router as mod

    mod._service = None
    app = FastAPI()
    init_ais_tile_service(snapshot_db_path)
    app.include_router(router)
    yield app
    shutdown_ais_tile_service()


@pytest.fixture()
def client(app):
    return TestClient(app)


class TestAISTileEndpoint:
    def test_tile_returns_mvt(self, client):
        resp = client.get(f"/ais/tiles/{SNAPSHOT_DATE}/0/0/0.pbf")
        assert resp.status_code == 200
        assert resp.headers["content-type"] == "application/vnd.mapbox-vector-tile"
        decoded = mvt.decode(resp.content)
        assert "vessels" in decoded
        assert len(decoded["vessels"]["features"]) == 2

    def test_tile_has_immutable_cache(self, client):
        resp = client.get(f"/ais/tiles/{SNAPSHOT_DATE}/0/0/0.pbf")
        assert resp.status_code == 200
        assert resp.headers["cache-control"] == "public, max-age=31536000, immutable"

    def test_empty_tile_returns_204(self, client):
        resp = client.get(f"/ais/tiles/{SNAPSHOT_DATE}/4/0/0.pbf")
        assert resp.status_code == 204
        assert resp.headers["cache-control"] == "public, max-age=31536000, immutable"

    def test_tile_out_of_range(self, client):
        """x=2 is invalid at z=0 (max is 0)."""
        resp = client.get(f"/ais/tiles/{SNAPSHOT_DATE}/0/2/0.pbf")
        assert resp.status_code == 400
        assert "out of range" in resp.json()["detail"]

    def test_tile_zoom_too_high(self, client):
        """z=13 exceeds MAX_ZOOM=12."""
        resp = client.get(f"/ais/tiles/{SNAPSHOT_DATE}/13/0/0.pbf")
        assert resp.status_code == 422  # FastAPI validation (Path(le=12))

    def test_tile_negative_zoom(self, client):
        resp = client.get(f"/ais/tiles/{SNAPSHOT_DATE}/-1/0/0.pbf")
        # Negative zoom may match differently, but should fail validation
        assert resp.status_code in (404, 422)


class TestAISTileJsonEndpoint:
    def test_tilejson_response(self, client):
        resp = client.get(f"/ais/tiles/{SNAPSHOT_DATE}/tilejson.json")
        assert resp.status_code == 200
        data = resp.json()
        assert data["tilejson"] == "3.0.0"
        assert data["name"] == "ais-vessels"
        assert data["minzoom"] == MIN_ZOOM
        assert data["maxzoom"] == MAX_ZOOM
        assert len(data["tiles"]) == 1
        assert "{z}" in data["tiles"][0]
        assert ".pbf" in data["tiles"][0]

    def test_tilejson_has_immutable_cache(self, client):
        resp = client.get(f"/ais/tiles/{SNAPSHOT_DATE}/tilejson.json")
        assert resp.headers["cache-control"] == "public, max-age=31536000, immutable"

    def test_tilejson_has_vector_layers(self, client):
        resp = client.get(f"/ais/tiles/{SNAPSHOT_DATE}/tilejson.json")
        data = resp.json()
        assert "vector_layers" in data
        layers = data["vector_layers"]
        assert len(layers) == 1
        assert layers[0]["id"] == "vessels"
        assert "mmsi" in layers[0]["fields"]
        assert "shiptype" in layers[0]["fields"]

    def test_tilejson_has_absolute_url(self, client):
        resp = client.get(f"/ais/tiles/{SNAPSHOT_DATE}/tilejson.json")
        data = resp.json()
        assert data["tiles"][0].startswith("http")


class TestAISTileService:
    def test_double_init_raises(self, snapshot_db_path):
        import weatherman.ais.router as mod

        mod._service = None
        init_ais_tile_service(snapshot_db_path)
        with pytest.raises(RuntimeError, match="already initialized"):
            init_ais_tile_service(snapshot_db_path)
        mod._service = None
