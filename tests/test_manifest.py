"""Tests for UI manifest generation and schema versioning."""

import json
from datetime import datetime, timezone

import pytest

from weatherman.storage.manifest import (
    SCHEMA_VERSION,
    LayerConfig,
    ManifestConfig,
    UIManifest,
    ValueRange,
    build_manifest,
    build_manifest_json,
)
from weatherman.storage.paths import RunID


RUN = RunID("20260306T00Z")
MODEL = "gfs"


def _wind_layer():
    return LayerConfig(
        id="wind_speed",
        display_name="Wind Speed",
        unit="m/s",
        palette_name="viridis",
        value_range=ValueRange(min=0.0, max=50.0),
    )


def _temp_layer():
    return LayerConfig(
        id="temperature",
        display_name="Temperature",
        unit="K",
        palette_name="coolwarm",
        value_range=ValueRange(min=220.0, max=320.0),
    )


def _make_config(**overrides) -> ManifestConfig:
    defaults = dict(
        model=MODEL,
        run_id=RUN,
        published_at=datetime(2026, 3, 6, 1, 46, 0, tzinfo=timezone.utc),
        resolution_km=25.0,
        layers=[_wind_layer(), _temp_layer()],
        forecast_hours=[6, 0, 3, 12, 9],
    )
    defaults.update(overrides)
    return ManifestConfig(**defaults)


@pytest.fixture
def manifest():
    return build_manifest(_make_config())


class TestManifestBasics:
    def test_schema_version(self, manifest):
        assert manifest.schema_version == SCHEMA_VERSION

    def test_model(self, manifest):
        assert manifest.model == "gfs"

    def test_run_id(self, manifest):
        assert manifest.run_id == "20260306T00Z"

    def test_cycle_time(self, manifest):
        assert manifest.cycle_time == "2026-03-06T00:00:00+00:00"

    def test_published_at(self, manifest):
        assert manifest.published_at == "2026-03-06T01:46:00+00:00"

    def test_published_at_none(self):
        config = _make_config(published_at=None)
        m = build_manifest(config)
        assert m.published_at is None

    def test_resolution_km(self, manifest):
        assert manifest.resolution_km == 25.0


class TestLayers:
    def test_layer_count(self, manifest):
        assert len(manifest.layers) == 2

    def test_layer_ids(self, manifest):
        ids = [l.id for l in manifest.layers]
        assert ids == ["wind_speed", "temperature"]

    def test_layer_display_name(self, manifest):
        assert manifest.layers[0].display_name == "Wind Speed"

    def test_layer_unit(self, manifest):
        assert manifest.layers[0].unit == "m/s"

    def test_layer_palette(self, manifest):
        assert manifest.layers[0].palette_name == "viridis"

    def test_layer_value_range(self, manifest):
        vr = manifest.layers[0].value_range
        assert vr.min == 0.0
        assert vr.max == 50.0

    def test_empty_layers(self):
        config = _make_config(layers=[])
        m = build_manifest(config)
        assert m.layers == []


class TestForecastHours:
    def test_forecast_hours_sorted(self, manifest):
        assert manifest.forecast_hours == [0, 3, 6, 9, 12]

    def test_already_sorted_input(self):
        config = _make_config(forecast_hours=[0, 3, 6])
        m = build_manifest(config)
        assert m.forecast_hours == [0, 3, 6]

    def test_empty_forecast_hours(self):
        config = _make_config(forecast_hours=[])
        m = build_manifest(config)
        assert m.forecast_hours == []


class TestTileUrlTemplate:
    def test_default_template(self, manifest):
        assert manifest.tile_url_template == (
            "/tiles/{model}/{run_id}/{layer}/{forecast_hour}/{z}/{x}/{y}.png"
        )

    def test_custom_template(self):
        tpl = "https://tiles.example.com/{model}/{run_id}/{layer}/{z}/{x}/{y}"
        config = _make_config(tile_url_template=tpl)
        m = build_manifest(config)
        assert m.tile_url_template == tpl


class TestSerialization:
    def test_to_dict_has_schema_version_first(self, manifest):
        d = manifest.to_dict()
        keys = list(d.keys())
        assert keys[0] == "schema_version"

    def test_to_dict_layers_are_dicts(self, manifest):
        d = manifest.to_dict()
        assert isinstance(d["layers"][0], dict)
        assert d["layers"][0]["id"] == "wind_speed"

    def test_to_json_is_valid_json(self, manifest):
        raw = manifest.to_json()
        d = json.loads(raw)
        assert d["schema_version"] == SCHEMA_VERSION

    def test_to_json_compact(self, manifest):
        raw = manifest.to_json(indent=None)
        assert "\n" not in raw

    def test_json_roundtrip(self, manifest):
        raw = manifest.to_json()
        restored = UIManifest.from_json(raw)
        assert restored.model == manifest.model
        assert restored.run_id == manifest.run_id
        assert restored.layers[0].id == "wind_speed"
        assert restored.layers[0].value_range.min == 0.0

    def test_dict_roundtrip(self, manifest):
        d = manifest.to_dict()
        restored = UIManifest.from_dict(d)
        assert restored == manifest

    def test_build_manifest_json_returns_string(self):
        config = _make_config()
        raw = build_manifest_json(config)
        assert isinstance(raw, str)
        d = json.loads(raw)
        assert d["model"] == "gfs"


class TestSchemaVersioning:
    def test_from_dict_rejects_wrong_version(self):
        d = {"schema_version": 999, "model": "gfs"}
        with pytest.raises(ValueError, match="Unsupported manifest schema version 999"):
            UIManifest.from_dict(d)

    def test_from_dict_rejects_missing_version(self):
        d = {"model": "gfs"}
        with pytest.raises(ValueError, match="Unsupported manifest schema version None"):
            UIManifest.from_dict(d)

    def test_from_json_rejects_wrong_version(self):
        raw = json.dumps({"schema_version": 0, "model": "gfs"})
        with pytest.raises(ValueError, match="Unsupported manifest schema version"):
            UIManifest.from_json(raw)


class TestManifestSize:
    def test_manifest_under_10kb(self):
        """Manifest should stay small for fast frontend loading."""
        layers = [
            LayerConfig(
                id=f"layer_{i}",
                display_name=f"Layer {i}",
                unit="m/s",
                palette_name="viridis",
                value_range=ValueRange(min=0.0, max=100.0),
            )
            for i in range(20)
        ]
        config = _make_config(
            layers=layers,
            forecast_hours=list(range(0, 385, 3)),  # 129 forecast hours
        )
        raw = build_manifest_json(config)
        assert len(raw.encode()) < 10_000
