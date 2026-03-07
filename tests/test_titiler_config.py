"""Tests for TiTiler deployment configuration and colormaps."""

from __future__ import annotations

import json

from weatherman.storage.config import StorageConfig
from weatherman.tiling.colormaps import (
    COLORMAPS,
    PRECIPITATION,
    TEMPERATURE,
    WIND_SPEED,
    WeatherColormap,
    _interpolate_colors,
    get_colormap,
)
from weatherman.tiling.compose import generate_compose_dict
from weatherman.tiling.config import TiTilerConfig


# -- TiTilerConfig tests --


class TestTiTilerConfig:
    def test_env_vars_with_custom_endpoint(self):
        storage = StorageConfig(
            bucket="wx-data",
            endpoint_url="http://minio:9000",
            region="us-east-1",
        )
        cfg = TiTilerConfig(storage=storage)
        env = cfg.env_vars()

        assert env["AWS_S3_ENDPOINT"] == "http://minio:9000"
        assert env["AWS_VIRTUAL_HOSTING"] == "FALSE"
        assert env["AWS_S3_FORCE_PATH_STYLE"] == "true"
        assert env["AWS_DEFAULT_REGION"] == "us-east-1"

    def test_env_vars_without_custom_endpoint(self):
        """Real AWS S3 — no endpoint override, no path-style flags."""
        storage = StorageConfig(bucket="wx-data")
        cfg = TiTilerConfig(storage=storage)
        env = cfg.env_vars()

        assert "AWS_S3_ENDPOINT" not in env
        assert "AWS_VIRTUAL_HOSTING" not in env
        assert "AWS_S3_FORCE_PATH_STYLE" not in env

    def test_cors_origins(self):
        storage = StorageConfig(bucket="wx-data")
        cfg = TiTilerConfig(
            storage=storage,
            cors_origins=["http://localhost:5173", "https://weatherman.example.com"],
        )
        env = cfg.env_vars()
        assert (
            env["TITILER_API_CORS_ORIGINS"]
            == "http://localhost:5173,https://weatherman.example.com"
        )

    def test_worker_count_in_env(self):
        storage = StorageConfig(bucket="wx-data")
        cfg = TiTilerConfig(storage=storage, workers=8)
        env = cfg.env_vars()
        assert env["WEB_CONCURRENCY"] == "8"

    def test_memory_limit(self):
        storage = StorageConfig(bucket="wx-data")
        cfg = TiTilerConfig(storage=storage, workers=4)
        # 4 workers * 512MB + 256MB overhead = 2304MB
        assert cfg.memory_limit_mb() == 2304

    def test_gdal_tuning_defaults(self):
        storage = StorageConfig(bucket="wx-data")
        cfg = TiTilerConfig(storage=storage)
        env = cfg.env_vars()

        assert env["GDAL_CACHEMAX"] == "200"
        assert env["GDAL_DISABLE_READDIR_ON_OPEN"] == "EMPTY_DIR"
        assert env["VSI_CACHE"] == "TRUE"
        assert env["GDAL_HTTP_MULTIPLEX"] == "YES"
        assert env["GDAL_HTTP_VERSION"] == "2"

    def test_from_storage_factory(self):
        storage = StorageConfig(bucket="wx-data", endpoint_url="http://minio:9000")
        cfg = TiTilerConfig.from_storage(
            storage, workers=2, cors_origins=["http://localhost:3000"]
        )
        assert cfg.workers == 2
        assert cfg.cors_origins == ["http://localhost:3000"]
        assert cfg.storage is storage

    def test_gdal_merge_ranges_disabled(self):
        storage = StorageConfig(bucket="b")
        cfg = TiTilerConfig(storage=storage, gdal_http_merge_consecutive_ranges=False)
        env = cfg.env_vars()
        assert env["GDAL_HTTP_MERGE_CONSECUTIVE_RANGES"] == "NO"

    def test_vsi_cache_size_bytes(self):
        storage = StorageConfig(bucket="b")
        cfg = TiTilerConfig(storage=storage, cpl_vsil_curl_cache_size_mb=100)
        env = cfg.env_vars()
        assert env["CPL_VSIL_CURL_CACHE_SIZE"] == str(100 * 1024 * 1024)
        assert env["VSI_CACHE_SIZE"] == str(100 * 1024 * 1024)


# -- Colormap tests --


class TestColormaps:
    def test_temperature_colormap_has_256_entries(self):
        assert len(TEMPERATURE.colormap) == 256

    def test_wind_speed_colormap_has_256_entries(self):
        assert len(WIND_SPEED.colormap) == 256

    def test_precipitation_colormap_has_256_entries(self):
        assert len(PRECIPITATION.colormap) == 256

    def test_colormap_values_are_valid_rgba(self):
        for cmap in COLORMAPS.values():
            for key, (r, g, b, a) in cmap.colormap.items():
                assert 0 <= key <= 255
                assert 0 <= r <= 255
                assert 0 <= g <= 255
                assert 0 <= b <= 255
                assert a == 255

    def test_to_json_is_valid(self):
        raw = TEMPERATURE.to_json()
        parsed = json.loads(raw)
        assert len(parsed) == 256
        assert parsed["0"] == list(TEMPERATURE.colormap[0])

    def test_rescale_range(self):
        assert TEMPERATURE.rescale_range() == "220.0,330.0"
        assert WIND_SPEED.rescale_range() == "0.0,50.0"

    def test_get_colormap_existing(self):
        assert get_colormap("temperature") is TEMPERATURE
        assert get_colormap("wind_speed") is WIND_SPEED
        assert get_colormap("precipitation") is PRECIPITATION

    def test_get_colormap_missing(self):
        try:
            get_colormap("nonexistent")
            assert False, "Expected KeyError"
        except KeyError:
            pass

    def test_interpolation_endpoints(self):
        """First and last entries should match the stop colors."""
        stops = [(0.0, (10, 20, 30)), (1.0, (200, 210, 220))]
        cmap = _interpolate_colors(stops, steps=256)
        assert cmap[0] == (10, 20, 30, 255)
        assert cmap[255] == (200, 210, 220, 255)

    def test_interpolation_midpoint(self):
        stops = [(0.0, (0, 0, 0)), (1.0, (254, 254, 254))]
        cmap = _interpolate_colors(stops, steps=256)
        # Midpoint (~127-128) should be roughly half
        r, g, b, a = cmap[128]
        assert 126 <= r <= 130


# -- Compose generation tests --


class TestComposeGeneration:
    def test_generate_compose_dict_structure(self):
        storage = StorageConfig(bucket="wx-data", endpoint_url="http://minio:9000")
        cfg = TiTilerConfig(storage=storage, port=8080)
        compose = generate_compose_dict(cfg)

        assert "services" in compose
        assert "titiler" in compose["services"]
        svc = compose["services"]["titiler"]
        assert svc["image"] == cfg.image
        assert svc["container_name"] == "weatherman-titiler"
        assert "8080:8080" in svc["ports"]

    def test_compose_has_healthcheck(self):
        storage = StorageConfig(bucket="wx-data")
        cfg = TiTilerConfig(storage=storage, port=8080)
        compose = generate_compose_dict(cfg)
        svc = compose["services"]["titiler"]
        assert "healthcheck" in svc
        assert "curl" in svc["healthcheck"]["test"]
        # Verify the URL uses /api, not a non-existent /healthz
        assert "http://localhost:8080/api" in svc["healthcheck"]["test"]

    def test_compose_has_memory_limits(self):
        storage = StorageConfig(bucket="wx-data")
        cfg = TiTilerConfig(storage=storage, workers=4)
        compose = generate_compose_dict(cfg)
        limits = compose["services"]["titiler"]["deploy"]["resources"]["limits"]
        assert limits["memory"] == "2304M"

    def test_compose_environment_includes_s3(self):
        storage = StorageConfig(bucket="wx-data", endpoint_url="http://minio:9000")
        cfg = TiTilerConfig(storage=storage)
        compose = generate_compose_dict(cfg)
        env = compose["services"]["titiler"]["environment"]
        assert env["AWS_S3_ENDPOINT"] == "http://minio:9000"

    def test_compose_environment_has_credential_placeholders(self):
        storage = StorageConfig(bucket="wx-data")
        cfg = TiTilerConfig(storage=storage)
        compose = generate_compose_dict(cfg)
        env = compose["services"]["titiler"]["environment"]
        assert env["AWS_ACCESS_KEY_ID"] == "${AWS_ACCESS_KEY_ID}"
        assert env["AWS_SECRET_ACCESS_KEY"] == "${AWS_SECRET_ACCESS_KEY}"
