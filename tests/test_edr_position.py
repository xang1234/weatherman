"""Tests for the OGC API – EDR position endpoint."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest
import zarr

from weatherman.edr.position import (
    EDRService,
    _build_coverage_json,
    parse_datetime_filter,
    parse_wkt_point,
)
from weatherman.storage.catalog import RunCatalog
from weatherman.storage.paths import RunID, StorageLayout
from weatherman.storage.zarr_schema import GFS_SCHEMA


# ---------------------------------------------------------------------------
# WKT POINT parsing
# ---------------------------------------------------------------------------

class TestParseWktPoint:
    def test_basic(self):
        lon, lat = parse_wkt_point("POINT(10.5 20.3)")
        assert lon == pytest.approx(10.5)
        assert lat == pytest.approx(20.3)

    def test_negative_coords(self):
        lon, lat = parse_wkt_point("POINT(-122.4 37.8)")
        assert lon == pytest.approx(-122.4)
        assert lat == pytest.approx(37.8)

    def test_whitespace_tolerance(self):
        lon, lat = parse_wkt_point("  POINT (  -10  20  )  ")
        assert lon == pytest.approx(-10.0)
        assert lat == pytest.approx(20.0)

    def test_case_insensitive(self):
        lon, lat = parse_wkt_point("point(5 10)")
        assert lon == pytest.approx(5.0)

    def test_invalid_format_raises(self):
        with pytest.raises(ValueError, match="Invalid WKT POINT"):
            parse_wkt_point("LINESTRING(0 0, 1 1)")

    def test_empty_string_raises(self):
        with pytest.raises(ValueError, match="Invalid WKT POINT"):
            parse_wkt_point("")

    def test_missing_lat_raises(self):
        with pytest.raises(ValueError, match="Invalid WKT POINT"):
            parse_wkt_point("POINT(10)")


# ---------------------------------------------------------------------------
# Datetime filter parsing
# ---------------------------------------------------------------------------

class TestParseDatetimeFilter:
    @pytest.fixture()
    def hours(self):
        return np.array([0, 3, 6, 9, 12, 24, 48, 72, 120], dtype=np.int32)

    def test_none_returns_all(self, hours):
        mask = parse_datetime_filter(None, hours)
        assert mask.all()

    def test_dotdot_returns_all(self, hours):
        mask = parse_datetime_filter("..", hours)
        assert mask.all()

    def test_single_hour(self, hours):
        mask = parse_datetime_filter("6", hours)
        assert mask.sum() == 1
        assert hours[mask][0] == 6

    def test_range_slash(self, hours):
        mask = parse_datetime_filter("0/12", hours)
        expected = hours[mask]
        np.testing.assert_array_equal(expected, [0, 3, 6, 9, 12])

    def test_range_dotdot(self, hours):
        mask = parse_datetime_filter("6..48", hours)
        expected = hours[mask]
        np.testing.assert_array_equal(expected, [6, 9, 12, 24, 48])

    def test_invalid_raises(self, hours):
        with pytest.raises(ValueError, match="Invalid datetime"):
            parse_datetime_filter("abc", hours)

    def test_no_match(self, hours):
        mask = parse_datetime_filter("99", hours)
        assert mask.sum() == 0


# ---------------------------------------------------------------------------
# CoverageJSON building
# ---------------------------------------------------------------------------

class TestBuildCoverageJson:
    def test_structure(self):
        result = _build_coverage_json(
            lon=10.0,
            lat=20.0,
            forecast_hours=[0, 3, 6],
            parameters={"tmp_2m": [300.1, 301.2, 302.3]},
            variable_metadata={"tmp_2m": {"long_name": "Temperature at 2m", "units": "K"}},
        )

        assert result["type"] == "Coverage"
        assert result["domain"]["domainType"] == "PointSeries"
        assert result["domain"]["axes"]["x"]["values"] == [10.0]
        assert result["domain"]["axes"]["y"]["values"] == [20.0]
        assert result["domain"]["axes"]["t"]["values"] == [0, 3, 6]
        assert result["parameters"]["tmp_2m"]["unit"]["symbol"] == "K"
        assert result["ranges"]["tmp_2m"]["values"] == [300.1, 301.2, 302.3]
        assert result["ranges"]["tmp_2m"]["shape"] == [3]


# ---------------------------------------------------------------------------
# EDRService integration tests (with real Zarr on disk)
# ---------------------------------------------------------------------------

def _create_test_zarr(tmp_path: Path, model: str, run_id: RunID) -> Path:
    """Create a minimal Zarr store with known data for testing."""
    layout = StorageLayout(model)
    zarr_rel_path = layout.zarr_path(run_id)
    zarr_abs_path = tmp_path / zarr_rel_path

    lat = np.array([90.0, 45.0, 0.0, -45.0, -90.0], dtype=np.float32)
    lon = np.linspace(-180, 135, 8, dtype=np.float32)  # 8 points, step=45
    time = np.array([0, 3, 6], dtype=np.int32)

    root = zarr.open_group(str(zarr_abs_path), mode="w")

    root.create_array("lat", data=lat, dimension_names=("lat",),
                       attributes={"long_name": "latitude", "units": "degrees_north"})
    root.create_array("lon", data=lon, dimension_names=("lon",),
                       attributes={"long_name": "longitude", "units": "degrees_east"})
    root.create_array("time", data=time, dimension_names=("time",),
                       attributes={"long_name": "forecast hour", "units": "hours"})

    # Temperature: simple gradient (lat + lon + time*0.1)
    lat_grid, lon_grid = np.meshgrid(lat, lon, indexing="ij")
    data = np.zeros((3, 5, 8), dtype=np.float32)
    for t_idx, t_val in enumerate(time):
        data[t_idx] = lat_grid + lon_grid + t_val * 0.1

    root.create_array(
        "tmp_2m", data=data, dimension_names=("time", "lat", "lon"),
        attributes={"long_name": "Temperature at 2m", "units": "K"},
    )

    # Wind: constant 5.0 m/s everywhere
    root.create_array(
        "ugrd_10m", data=np.full((3, 5, 8), 5.0, dtype=np.float32),
        dimension_names=("time", "lat", "lon"),
        attributes={"long_name": "U-wind at 10m", "units": "m/s"},
    )

    return zarr_abs_path


@pytest.fixture()
def edr_setup(tmp_path):
    """Set up EDRService with a test Zarr store and catalog."""
    model = "gfs"
    run_id = RunID("20260306T00Z")
    _create_test_zarr(tmp_path, model, run_id)

    catalog = RunCatalog.new(model)
    catalog.publish_run(run_id, layout=StorageLayout(model))

    def catalog_loader(m: str) -> RunCatalog:
        return catalog

    def zarr_opener(zarr_path: str) -> zarr.Group:
        return zarr.open_group(str(tmp_path / zarr_path), mode="r")

    svc = EDRService(catalog_loader, zarr_opener)
    return svc, model, run_id


class TestEDRService:
    def test_resolve_latest(self, edr_setup):
        svc, model, run_id = edr_setup
        resolved = svc.resolve_run_id(model, "latest")
        assert resolved == run_id

    def test_resolve_explicit_run_id(self, edr_setup):
        svc, model, run_id = edr_setup
        resolved = svc.resolve_run_id(model, "20260306T00Z")
        assert resolved == run_id

    def test_position_all_params_all_times(self, edr_setup):
        svc, model, run_id = edr_setup
        result = svc.query_position(
            model=model,
            run_id=run_id,
            lon=0.0,
            lat=45.0,
            parameter_names=None,
            datetime_filter=None,
        )

        assert result["type"] == "Coverage"
        assert result["domain"]["domainType"] == "PointSeries"
        assert result["domain"]["axes"]["t"]["values"] == [0, 3, 6]
        assert "tmp_2m" in result["ranges"]
        assert "ugrd_10m" in result["ranges"]
        assert len(result["ranges"]["tmp_2m"]["values"]) == 3

    def test_position_single_param(self, edr_setup):
        svc, model, run_id = edr_setup
        result = svc.query_position(
            model=model,
            run_id=run_id,
            lon=0.0,
            lat=0.0,
            parameter_names=["ugrd_10m"],
            datetime_filter=None,
        )

        assert list(result["ranges"].keys()) == ["ugrd_10m"]
        # Wind is constant 5.0 everywhere
        for val in result["ranges"]["ugrd_10m"]["values"]:
            assert val == pytest.approx(5.0)

    def test_position_datetime_filter(self, edr_setup):
        svc, model, run_id = edr_setup
        result = svc.query_position(
            model=model,
            run_id=run_id,
            lon=0.0,
            lat=0.0,
            parameter_names=["tmp_2m"],
            datetime_filter="0/3",
        )

        assert result["domain"]["axes"]["t"]["values"] == [0, 3]
        assert len(result["ranges"]["tmp_2m"]["values"]) == 2

    def test_position_single_forecast_hour(self, edr_setup):
        svc, model, run_id = edr_setup
        result = svc.query_position(
            model=model,
            run_id=run_id,
            lon=0.0,
            lat=0.0,
            parameter_names=["tmp_2m"],
            datetime_filter="6",
        )

        assert result["domain"]["axes"]["t"]["values"] == [6]
        assert len(result["ranges"]["tmp_2m"]["values"]) == 1

    def test_position_exact_grid_point_value(self, edr_setup):
        """Verify interpolated value matches expected formula."""
        svc, model, run_id = edr_setup
        # At (lat=45, lon=0, t=0): value = 45 + 0 + 0*0.1 = 45.0
        result = svc.query_position(
            model=model,
            run_id=run_id,
            lon=0.0,
            lat=45.0,
            parameter_names=["tmp_2m"],
            datetime_filter="0",
        )

        val = result["ranges"]["tmp_2m"]["values"][0]
        assert val == pytest.approx(45.0, abs=0.01)

    def test_position_antimeridian_point(self, edr_setup):
        """Query near the anti-meridian should work via wraparound interpolation."""
        svc, model, run_id = edr_setup
        # Our grid has lon from -180 to 135 in steps of 45.
        # Query at lon=179 (near +180, between 135 and -180 wrapped)
        result = svc.query_position(
            model=model,
            run_id=run_id,
            lon=179.0,
            lat=0.0,
            parameter_names=["ugrd_10m"],
            datetime_filter="0",
        )

        # Wind is constant 5.0, so interpolation should return ~5.0
        val = result["ranges"]["ugrd_10m"]["values"][0]
        assert val == pytest.approx(5.0)

    def test_unknown_parameter_raises(self, edr_setup):
        svc, model, run_id = edr_setup
        with pytest.raises(Exception, match="Unknown parameters"):
            svc.query_position(
                model=model,
                run_id=run_id,
                lon=0.0,
                lat=0.0,
                parameter_names=["nonexistent"],
                datetime_filter=None,
            )

    def test_latitude_out_of_range_raises(self, edr_setup):
        svc, model, run_id = edr_setup
        with pytest.raises(Exception, match="Latitude"):
            svc.query_position(
                model=model,
                run_id=run_id,
                lon=0.0,
                lat=95.0,
                parameter_names=None,
                datetime_filter=None,
            )


# ---------------------------------------------------------------------------
# FastAPI route integration test
# ---------------------------------------------------------------------------

class TestEDRRoute:
    @pytest.fixture()
    def client(self, tmp_path):
        """Create a test client with EDR service wired up."""
        from fastapi.testclient import TestClient

        from weatherman.edr.position import (
            init_edr_service,
            router,
            shutdown_edr_service,
        )

        model = "gfs"
        run_id = RunID("20260306T00Z")
        _create_test_zarr(tmp_path, model, run_id)

        catalog = RunCatalog.new(model)
        catalog.publish_run(run_id, layout=StorageLayout(model))

        def catalog_loader(m: str) -> RunCatalog:
            return catalog

        def zarr_opener(zarr_path: str) -> zarr.Group:
            return zarr.open_group(str(tmp_path / zarr_path), mode="r")

        from fastapi import FastAPI

        app = FastAPI()
        app.include_router(router)
        shutdown_edr_service()  # clean up any leaked state from prior tests
        init_edr_service(catalog_loader, zarr_opener)

        yield TestClient(app)

        shutdown_edr_service()

    def test_get_position(self, client):
        resp = client.get(
            "/v1/edr/collections/gfs/instances/latest/position",
            params={
                "coords": "POINT(0 45)",
                "parameter-name": "tmp_2m",
                "datetime": "0",
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["type"] == "Coverage"
        assert data["domain"]["axes"]["x"]["values"] == [0.0]
        assert data["domain"]["axes"]["y"]["values"] == [45.0]

    def test_get_position_all_defaults(self, client):
        resp = client.get(
            "/v1/edr/collections/gfs/instances/latest/position",
            params={"coords": "POINT(0 0)"},
        )
        assert resp.status_code == 200
        data = resp.json()
        # All 3 time steps, all variables
        assert len(data["domain"]["axes"]["t"]["values"]) == 3
        assert "tmp_2m" in data["ranges"]
        assert "ugrd_10m" in data["ranges"]

    def test_invalid_coords(self, client):
        resp = client.get(
            "/v1/edr/collections/gfs/instances/latest/position",
            params={"coords": "not a point"},
        )
        assert resp.status_code == 400
        assert "Invalid WKT POINT" in resp.json()["detail"]

    def test_invalid_parameter(self, client):
        resp = client.get(
            "/v1/edr/collections/gfs/instances/latest/position",
            params={
                "coords": "POINT(0 0)",
                "parameter-name": "fake_var",
            },
        )
        assert resp.status_code == 400
        assert "Unknown parameters" in resp.json()["detail"]

    def test_explicit_run_id(self, client):
        resp = client.get(
            "/v1/edr/collections/gfs/instances/20260306T00Z/position",
            params={"coords": "POINT(0 0)", "parameter-name": "ugrd_10m"},
        )
        assert resp.status_code == 200
