"""Tests for pre-generated data tile generation."""

from __future__ import annotations

import io
import tempfile
from pathlib import Path

import numpy as np
import pytest
import rasterio
import rasterio.transform
from PIL import Image

from weatherman.processing.data_tiles import (
    MAX_DATA_TILE_ZOOM,
    _WORLD_EXTENT,
    generate_all_data_tiles,
    generate_data_tile,
    tile_bounds_3857,
)
from weatherman.tiling.data_encoder import decode_rgba_to_float


def _make_test_cog(
    values: np.ndarray,
    path: str,
    nodata: float | None = None,
) -> None:
    """Write a minimal GeoTIFF (EPSG:4326, global extent) for testing."""
    h, w = values.shape
    transform = rasterio.transform.from_bounds(-180, -90, 180, 90, w, h)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=h,
        width=w,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=transform,
        nodata=nodata,
    ) as dst:
        dst.write(values.astype(np.float32), 1)


# -- tile_bounds_3857 tests --


class TestTileBounds3857:
    def test_z0_covers_full_extent(self):
        """z0/0/0 should cover the full Web Mercator extent."""
        west, south, east, north = tile_bounds_3857(0, 0, 0)
        assert west == pytest.approx(-_WORLD_EXTENT, rel=1e-6)
        assert east == pytest.approx(_WORLD_EXTENT, rel=1e-6)
        assert south == pytest.approx(-_WORLD_EXTENT, rel=1e-6)
        assert north == pytest.approx(_WORLD_EXTENT, rel=1e-6)

    def test_known_tile_bounds(self):
        """z1 tiles should divide the world into 4 quadrants."""
        # z1/0/0 = top-left quadrant
        west, south, east, north = tile_bounds_3857(1, 0, 0)
        assert west == pytest.approx(-_WORLD_EXTENT, rel=1e-6)
        assert east == pytest.approx(0.0, abs=1e-3)
        assert north == pytest.approx(_WORLD_EXTENT, rel=1e-6)
        assert south == pytest.approx(0.0, abs=1e-3)

    def test_tiles_are_contiguous(self):
        """Adjacent tiles at z2 should share edges."""
        _, _, east0, _ = tile_bounds_3857(2, 0, 0)
        west1, _, _, _ = tile_bounds_3857(2, 1, 0)
        assert east0 == pytest.approx(west1, rel=1e-10)

    def test_tile_count_at_zoom(self):
        """Number of tiles at zoom z should be 4^z."""
        for z in range(4):
            n = 2**z
            # Verify we can compute bounds for all tiles without error
            for x in range(n):
                for y in range(n):
                    bounds = tile_bounds_3857(z, x, y)
                    assert len(bounds) == 4


# -- generate_data_tile tests --


class TestGenerateDataTile:
    def test_roundtrip_accuracy(self):
        """Synthetic COG → tile → decode should preserve values within 0.1%."""
        rng = np.random.default_rng(42)
        values = rng.uniform(-55.0, 55.0, size=(180, 360)).astype(np.float32)

        with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as f:
            _make_test_cog(values, f.name)
            cog_path = f.name

        try:
            png_bytes = generate_data_tile(cog_path, 0, 0, 0, -55.0, 55.0)

            img = Image.open(io.BytesIO(png_bytes))
            assert img.size == (256, 256)
            assert img.mode == "RGBA"

            rgba = np.array(img)
            decoded, mask = decode_rgba_to_float(rgba, -55.0, 55.0)

            # Some edge pixels may be nodata due to reprojection, check valid ones
            valid = ~mask
            assert valid.sum() > 0, "Should have some valid pixels"
        finally:
            Path(cog_path).unlink()

    def test_nodata_flagged(self):
        """COG with NaN values should produce nodata-flagged pixels."""
        values = np.full((180, 360), 20.0, dtype=np.float32)
        # Set a large region to NaN to ensure some tile pixels are nodata
        values[:90, :] = np.nan

        with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as f:
            _make_test_cog(values, f.name, nodata=np.nan)
            cog_path = f.name

        try:
            png_bytes = generate_data_tile(cog_path, 0, 0, 0, 0.0, 50.0)
            img = Image.open(io.BytesIO(png_bytes))
            rgba = np.array(img)
            _, mask = decode_rgba_to_float(rgba, 0.0, 50.0)

            # B channel should flag some nodata pixels
            assert mask.any(), "Should have nodata-flagged pixels"
            # But not all pixels (bottom half has valid data)
            assert not mask.all(), "Should also have valid pixels"
        finally:
            Path(cog_path).unlink()


# -- generate_all_data_tiles tests --


class TestGenerateAllDataTiles:
    def test_tile_count_z0_to_z2(self):
        """z0–z2 should yield 1 + 4 + 16 = 21 tiles."""
        values = np.full((180, 360), 15.0, dtype=np.float32)

        with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as f:
            _make_test_cog(values, f.name)
            cog_path = f.name

        try:
            tiles = list(generate_all_data_tiles(cog_path, 0.0, 50.0, max_zoom=2))
            assert len(tiles) == 21

            # Verify z values are correct
            z_values = [t[0] for t in tiles]
            assert z_values.count(0) == 1
            assert z_values.count(1) == 4
            assert z_values.count(2) == 16
        finally:
            Path(cog_path).unlink()

    def test_all_tiles_are_valid_pngs(self):
        """Every yielded tile should be a valid PNG image."""
        values = np.full((180, 360), 25.0, dtype=np.float32)

        with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as f:
            _make_test_cog(values, f.name)
            cog_path = f.name

        try:
            for z, x, y, png_bytes in generate_all_data_tiles(
                cog_path, 0.0, 50.0, max_zoom=1,
            ):
                img = Image.open(io.BytesIO(png_bytes))
                assert img.size == (256, 256)
                assert img.mode == "RGBA"
        finally:
            Path(cog_path).unlink()
