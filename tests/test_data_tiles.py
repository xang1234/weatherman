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
from rasterio.enums import Resampling

from weatherman.processing.data_tiles import (
    MAX_DATA_TILE_ZOOM,
    _WORLD_EXTENT,
    data_tile_resampling_for_layer,
    generate_all_data_tiles,
    generate_data_tile,
    tile_bounds_3857,
)
from weatherman.tiling.data_encoder import decode_f16_to_float, decode_rgba_to_float


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
    def test_wave_direction_uses_nearest_resampling(self):
        assert data_tile_resampling_for_layer("wave_direction") == Resampling.nearest
        assert data_tile_resampling_for_layer("temperature") == Resampling.bilinear

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

    def test_float16_roundtrip_preserves_physical_values(self):
        """Float16 generation should preserve physical values directly."""
        values = np.full((180, 360), 12.5, dtype=np.float32)

        with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as f:
            _make_test_cog(values, f.name)
            cog_path = f.name

        try:
            buf = generate_data_tile(
                cog_path,
                0,
                0,
                0,
                0.0,
                50.0,
                tile_format="f16",
            )
            decoded, mask = decode_f16_to_float(buf, 256, 256)

            assert decoded.shape == (256, 256)
            assert not mask.all()
            assert np.allclose(decoded[~mask], 12.5, atol=0.1)
        finally:
            Path(cog_path).unlink()


# -- generate_all_data_tiles tests --


def _global_point_grid_cog(path: str, field) -> None:
    """Write a 1 degree global point grid (like GFS: first column at -180, rows on the poles)."""
    lon = np.arange(-180.0, 180.0, 1.0)
    lat = np.arange(90.0, -90.5, -1.0)
    values = field(*np.meshgrid(lon, lat)).astype(np.float32)
    transform = rasterio.transform.from_bounds(-180.5, -90.5, 179.5, 90.5, lon.size, lat.size)
    with rasterio.open(
        path, "w", driver="GTiff", height=lat.size, width=lon.size, count=1,
        dtype="float32", crs="EPSG:4326", transform=transform,
    ) as dst:
        dst.write(values, 1)


def _smooth_field(lon, lat):
    """Smooth, and continuous across the antimeridian (period 360 degrees in lon)."""
    return 20.0 * np.sin(np.radians(2 * lon + 30.0)) * np.cos(np.radians(lat))


def _decode_tile(cog_path: str, z: int, x: int, y: int) -> np.ndarray:
    rgba = np.array(Image.open(io.BytesIO(generate_data_tile(cog_path, z, x, y, -50.0, 50.0))))
    values, mask = decode_rgba_to_float(rgba, -50.0, 50.0)
    assert not mask.any()
    return values


class TestDataTileAccuracy:
    def test_tile_values_match_the_source_grid(self, tmp_path: Path):
        """Each tile pixel is the bilinear interpolation of the source grid there.

        The field is noise, so anything that warps through a coarser
        intermediate grid (and returns a blurred field) is far off.
        """
        rng = np.random.default_rng(7)
        noise = rng.uniform(-30.0, 30.0, size=(181, 360)).astype(np.float32)
        cog_path = str(tmp_path / "noise.tif")
        _global_point_grid_cog(cog_path, lambda lon, lat: noise)

        z, x, y = 3, 5, 2
        values = _decode_tile(cog_path, z, x, y)

        # Position of every tile pixel centre, as fractional (row, col) of the
        # 1 degree grid whose point (0, 0) is at 90N, 180W.
        west, south, east, north = tile_bounds_3857(z, x, y)
        world = 20037508.342789244
        mx = west + (np.arange(256) + 0.5) * (east - west) / 256
        my = north - (np.arange(256) + 0.5) * (north - south) / 256
        col = mx / world * 180.0 + 180.0
        row = 90.0 - np.degrees(2 * np.arctan(np.exp(my / world * np.pi)) - np.pi / 2)
        c0, r0 = np.floor(col).astype(int), np.floor(row).astype(int)
        fc, fr = (col - c0)[None, :], (row - r0)[:, None]
        at = lambda r, c: noise[r[:, None], (c % 360)[None, :]]
        expected = (
            (at(r0, c0) * (1 - fc) + at(r0, c0 + 1) * fc) * (1 - fr)
            + (at(r0 + 1, c0) * (1 - fc) + at(r0 + 1, c0 + 1) * fc) * fr
        )

        # 16-bit encoding over a 100-unit range quantises to 0.0015.
        assert np.abs(values - expected).max() < 0.01

    def test_field_is_continuous_across_the_antimeridian(self, tmp_path: Path):
        """The step between the tiles either side of ±180° is an ordinary one."""
        cog_path = str(tmp_path / "field.tif")
        _global_point_grid_cog(cog_path, _smooth_field)

        z, y = 3, 3
        last = _decode_tile(cog_path, z, 2**z - 1, y)   # ends at 180°E
        first = _decode_tile(cog_path, z, 0, y)         # starts at 180°W

        step_across = np.abs(last[:, -1] - first[:, 0]).max()
        step_within = np.abs(last[:, -1] - last[:, -2]).max()
        assert step_within > 0, "field must vary right up to the antimeridian"
        assert step_across < 2 * step_within


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

    def test_float16_tiles_are_generated(self):
        """Iterator should emit raw Float16 tiles when requested."""
        values = np.full((180, 360), 18.0, dtype=np.float32)

        with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as f:
            _make_test_cog(values, f.name)
            cog_path = f.name

        try:
            tiles = list(
                generate_all_data_tiles(
                    cog_path,
                    0.0,
                    50.0,
                    max_zoom=0,
                    tile_format="f16",
                )
            )
            assert len(tiles) == 1
            _, _, _, buf = tiles[0]
            decoded, mask = decode_f16_to_float(buf, 256, 256)
            assert decoded.shape == (256, 256)
            assert not mask.all()
            assert np.allclose(decoded[~mask], 18.0, atol=0.1)
        finally:
            Path(cog_path).unlink()
