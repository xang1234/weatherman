"""Tests for STAC Item generation with processing provenance."""

from datetime import datetime, timedelta, timezone

import pystac
import pytest

from weatherman.storage.paths import RunID, StorageLayout
from weatherman.storage.stac import (
    COGAsset,
    QCResult,
    RunProvenance,
    StacItemConfig,
    build_stac_item,
    build_stac_item_json,
    PROCESSING_EXTENSION,
)


RUN = RunID("20260306T00Z")
MODEL = "gfs"


def _make_provenance(**overrides) -> RunProvenance:
    defaults = dict(
        source_uris=[
            "s3://noaa-gfs-bdp-pds/gfs.20260306/00/atmos/gfs.t00z.pgrb2.0p25.f000",
            "s3://noaa-gfs-bdp-pds/gfs.20260306/00/atmos/gfs.t00z.pgrb2.0p25.f003",
        ],
        processing_version="1.2.0",
        herbie_version="2024.8.0",
        qc_results=[
            QCResult(name="range_check", passed=True),
            QCResult(name="nan_fraction", passed=True, message="0.1% NaN"),
            QCResult(name="spatial_coverage", passed=False, message="Missing tiles in Arctic"),
        ],
        ingestion_started_at=datetime(2026, 3, 6, 1, 0, 0, tzinfo=timezone.utc),
        processing_completed_at=datetime(2026, 3, 6, 1, 45, 0, tzinfo=timezone.utc),
        published_at=datetime(2026, 3, 6, 1, 46, 0, tzinfo=timezone.utc),
    )
    defaults.update(overrides)
    return RunProvenance(**defaults)


def _make_cog_assets() -> list[COGAsset]:
    return [
        COGAsset(layer="wind_speed", forecast_hour=0, href="models/gfs/runs/20260306T00Z/cogs/wind_speed/000.tif"),
        COGAsset(layer="wind_speed", forecast_hour=3, href="models/gfs/runs/20260306T00Z/cogs/wind_speed/003.tif"),
        COGAsset(layer="temperature", forecast_hour=0, href="models/gfs/runs/20260306T00Z/cogs/temperature/000.tif"),
    ]


def _make_config(**overrides) -> StacItemConfig:
    defaults = dict(
        model=MODEL,
        run_id=RUN,
        provenance=_make_provenance(),
        cog_assets=_make_cog_assets(),
    )
    defaults.update(overrides)
    return StacItemConfig(**defaults)


@pytest.fixture
def layout():
    return StorageLayout(MODEL)


@pytest.fixture
def item(layout):
    return build_stac_item(_make_config(), layout)


class TestStacItemBasics:
    def test_item_id(self, item):
        assert item.id == "gfs-20260306T00Z"

    def test_item_is_valid_stac(self, item):
        """The generated item should be a valid pystac Item."""
        assert isinstance(item, pystac.Item)
        d = item.to_dict()
        assert d["type"] == "Feature"
        assert "geometry" in d
        assert "bbox" in d
        assert "properties" in d

    def test_bbox_default_global(self, item):
        assert item.bbox == [-180.0, -90.0, 180.0, 90.0]

    def test_custom_bbox(self, layout):
        config = _make_config(bbox=[10.0, 20.0, 30.0, 40.0])
        item = build_stac_item(config, layout)
        assert item.bbox == [10.0, 20.0, 30.0, 40.0]

    def test_geometry_defaults_to_bbox_polygon(self, item):
        geom = item.geometry
        assert geom["type"] == "Polygon"
        coords = geom["coordinates"][0]
        assert len(coords) == 5  # closed ring
        assert coords[0] == coords[-1]

    def test_custom_geometry(self, layout):
        custom_geom = {"type": "Point", "coordinates": [0, 0]}
        config = _make_config(geometry=custom_geom)
        item = build_stac_item(config, layout)
        assert item.geometry == custom_geom

    def test_datetime_is_run_time(self, item):
        assert item.datetime == datetime(2026, 3, 6, 0, 0, 0, tzinfo=timezone.utc)


class TestTemporalExtent:
    def test_start_datetime(self, item):
        assert item.properties["start_datetime"] == "2026-03-06T00:00:00+00:00"

    def test_end_datetime_default_384h(self, item):
        expected = datetime(2026, 3, 6, 0, 0, 0, tzinfo=timezone.utc) + timedelta(hours=384)
        assert item.properties["end_datetime"] == expected.isoformat()

    def test_custom_forecast_hours(self, layout):
        config = _make_config(forecast_hours=(0, 120))
        item = build_stac_item(config, layout)
        expected = datetime(2026, 3, 6, 0, 0, 0, tzinfo=timezone.utc) + timedelta(hours=120)
        assert item.properties["end_datetime"] == expected.isoformat()


class TestProcessingExtension:
    def test_processing_extension_declared(self, item):
        assert PROCESSING_EXTENSION in item.stac_extensions

    def test_processing_version(self, item):
        assert item.properties["processing:version"] == "1.2.0"

    def test_processing_software(self, item):
        sw = item.properties["processing:software"]
        assert sw["weatherman"] == "1.2.0"
        assert sw["herbie"] == "2024.8.0"

    def test_source_uris(self, item):
        uris = item.properties["processing:source_uris"]
        assert len(uris) == 2
        assert all("gfs.t00z.pgrb2" in u for u in uris)

    def test_qc_results(self, item):
        qc = item.properties["processing:qc_results"]
        assert qc["range_check"]["passed"] is True
        assert qc["nan_fraction"]["passed"] is True
        assert qc["nan_fraction"]["message"] == "0.1% NaN"
        assert qc["spatial_coverage"]["passed"] is False

    def test_ingestion_started_at(self, item):
        assert item.properties["processing:ingestion_started_at"] == "2026-03-06T01:00:00+00:00"

    def test_processing_completed_at(self, item):
        assert item.properties["processing:processing_completed_at"] == "2026-03-06T01:45:00+00:00"


class TestTimestampsExtension:
    def test_timestamps_extension_declared(self, item):
        ts_uri = "https://stac-extensions.github.io/timestamps/v1.1.0/schema.json"
        assert ts_uri in item.stac_extensions

    def test_published_timestamp(self, item):
        assert item.properties["published"] == "2026-03-06T01:46:00Z"

    def test_published_none_omitted(self, layout):
        prov = _make_provenance(published_at=None)
        config = _make_config(provenance=prov)
        item = build_stac_item(config, layout)
        assert item.properties.get("published") is None


class TestAssets:
    def test_cog_assets_present(self, item):
        assert "wind_speed_f000" in item.assets
        assert "wind_speed_f003" in item.assets
        assert "temperature_f000" in item.assets

    def test_cog_asset_media_type(self, item):
        asset = item.assets["wind_speed_f000"]
        assert asset.media_type == pystac.MediaType.COG

    def test_cog_asset_roles(self, item):
        asset = item.assets["wind_speed_f000"]
        assert "data" in asset.roles

    def test_cog_asset_href(self, item):
        asset = item.assets["wind_speed_f000"]
        assert asset.href == "models/gfs/runs/20260306T00Z/cogs/wind_speed/000.tif"

    def test_cog_asset_extra_fields(self, item):
        asset = item.assets["wind_speed_f000"]
        assert asset.extra_fields["weatherman:layer"] == "wind_speed"
        assert asset.extra_fields["weatherman:forecast_hour"] == 0

    def test_zarr_asset_present(self, item):
        assert "zarr" in item.assets
        zarr = item.assets["zarr"]
        assert zarr.media_type == "application/x-zarr"
        assert "source" in zarr.roles
        assert zarr.href == "models/gfs/runs/20260306T00Z/zarr/20260306T00Z.zarr"

    def test_no_cog_assets_still_has_zarr(self, layout):
        config = _make_config(cog_assets=[])
        item = build_stac_item(config, layout)
        assert "zarr" in item.assets
        assert len(item.assets) == 1


class TestJsonSerialization:
    def test_build_stac_item_json_returns_dict(self, layout):
        config = _make_config()
        d = build_stac_item_json(config, layout)
        assert isinstance(d, dict)
        assert d["type"] == "Feature"
        assert d["id"] == "gfs-20260306T00Z"

    def test_json_roundtrip(self, layout):
        config = _make_config()
        d = build_stac_item_json(config, layout)
        # Should be able to recreate a pystac Item from the dict
        restored = pystac.Item.from_dict(d)
        assert restored.id == "gfs-20260306T00Z"
        assert restored.properties["processing:version"] == "1.2.0"


class TestExtraProperties:
    def test_extra_properties_included(self, layout):
        config = _make_config(extra_properties={"constellation": "NOAA"})
        item = build_stac_item(config, layout)
        assert item.properties["constellation"] == "NOAA"
