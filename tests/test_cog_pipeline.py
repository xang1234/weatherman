"""Tests for GRIB2 → COG conversion pipeline."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest
import rasterio
from rasterio.enums import Resampling
from rasterio.transform import from_bounds

from weatherman.processing.cog import (
    COG_PROFILE,
    DEFAULT_OVERVIEW_LEVELS,
    EXTENDED_OVERVIEW_LEVELS,
    GFS_025_HEIGHT,
    GFS_025_WIDTH,
    GFS_GLOBAL_BOUNDS,
    TARGET_CRS,
    COGResult,
    OverviewConfig,
    grib2_to_cog,
    validate_cog,
    wind_speed_to_cog,
)


@pytest.fixture
def grib2_file(tmp_path: Path) -> Path:
    """Create a synthetic GRIB2-like GeoTIFF for testing.

    We can't easily create real GRIB2 files in tests, so we create
    a GeoTIFF with EPSG:4326 CRS on the GFS 0.25° grid. The COG
    pipeline reads band 1 regardless of driver.
    """
    path = tmp_path / "input.grib2"
    transform = from_bounds(*GFS_GLOBAL_BOUNDS, GFS_025_WIDTH, GFS_025_HEIGHT)
    data = np.random.default_rng(42).standard_normal(
        (GFS_025_HEIGHT, GFS_025_WIDTH)
    ).astype(np.float32)

    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        dtype="float32",
        count=1,
        width=GFS_025_WIDTH,
        height=GFS_025_HEIGHT,
        crs="EPSG:4326",
        transform=transform,
    ) as dst:
        dst.write(data, 1)

    return path


@pytest.fixture
def small_grib2(tmp_path: Path) -> Path:
    """Create a small test raster for fast tests."""
    path = tmp_path / "small.grib2"
    width, height = 72, 37  # ~5° resolution
    transform = from_bounds(*GFS_GLOBAL_BOUNDS, width, height)
    data = np.arange(width * height, dtype=np.float32).reshape(height, width)

    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        dtype="float32",
        count=1,
        width=width,
        height=height,
        crs="EPSG:4326",
        transform=transform,
    ) as dst:
        dst.write(data, 1)

    return path


# ---------------------------------------------------------------------------
# grib2_to_cog
# ---------------------------------------------------------------------------


class TestGrib2ToCog:
    def test_creates_cog_file(self, small_grib2: Path, tmp_path: Path):
        output = tmp_path / "cogs" / "temperature" / "000.tif"
        result = grib2_to_cog(small_grib2, output)

        assert output.exists()
        assert isinstance(result, COGResult)
        assert result.output_path == output
        assert result.size_bytes > 0

    def test_output_is_epsg_4326(self, small_grib2: Path, tmp_path: Path):
        output = tmp_path / "out.tif"
        grib2_to_cog(small_grib2, output)

        with rasterio.open(output) as ds:
            assert ds.crs.to_epsg() == 4326

    def test_output_is_tiled(self, grib2_file: Path, tmp_path: Path):
        """Use full-res grid so tiles are smaller than the image."""
        output = tmp_path / "out.tif"
        grib2_to_cog(grib2_file, output)

        with rasterio.open(output) as ds:
            block_h, block_w = ds.block_shapes[0]
            assert block_h == 256
            assert block_w == 256

    def test_output_has_overviews(self, small_grib2: Path, tmp_path: Path):
        output = tmp_path / "out.tif"
        grib2_to_cog(small_grib2, output)

        with rasterio.open(output) as ds:
            overviews = ds.overviews(1)
            assert len(overviews) > 0

    def test_output_uses_deflate_compression(
        self, small_grib2: Path, tmp_path: Path
    ):
        output = tmp_path / "out.tif"
        grib2_to_cog(small_grib2, output)

        with rasterio.open(output) as ds:
            assert ds.compression.name.lower() == "deflate"

    def test_output_dtype_is_float32(
        self, small_grib2: Path, tmp_path: Path
    ):
        output = tmp_path / "out.tif"
        grib2_to_cog(small_grib2, output)

        with rasterio.open(output) as ds:
            assert ds.dtypes[0] == "float32"

    def test_data_values_preserved(self, small_grib2: Path, tmp_path: Path):
        output = tmp_path / "out.tif"
        grib2_to_cog(small_grib2, output)

        with rasterio.open(small_grib2) as src:
            expected = src.read(1)
        with rasterio.open(output) as dst:
            actual = dst.read(1)

        np.testing.assert_array_almost_equal(actual, expected, decimal=5)

    def test_custom_overview_levels(
        self, grib2_file: Path, tmp_path: Path
    ):
        """Use full-res grid so GDAL doesn't skip small overview levels."""
        output = tmp_path / "out.tif"
        levels = [2, 4]
        grib2_to_cog(grib2_file, output, overview_levels=levels)

        with rasterio.open(output) as ds:
            overviews = ds.overviews(1)
            assert overviews == levels

    def test_result_metadata(self, small_grib2: Path, tmp_path: Path):
        output = tmp_path / "out.tif"
        result = grib2_to_cog(small_grib2, output)

        assert result.input_path == small_grib2
        assert result.crs == TARGET_CRS
        assert result.overview_levels == DEFAULT_OVERVIEW_LEVELS
        assert result.width == 72
        assert result.height == 37

    def test_creates_parent_directories(self, small_grib2: Path, tmp_path: Path):
        output = tmp_path / "deep" / "nested" / "path" / "out.tif"
        grib2_to_cog(small_grib2, output)
        assert output.exists()

    def test_missing_input_raises(self, tmp_path: Path):
        missing = tmp_path / "nonexistent.grib2"
        output = tmp_path / "out.tif"
        with pytest.raises(FileNotFoundError, match="GRIB2 file not found"):
            grib2_to_cog(missing, output)

    def test_full_resolution_grid(self, grib2_file: Path, tmp_path: Path):
        """Test with full GFS 0.25° resolution (1440x721)."""
        output = tmp_path / "full.tif"
        result = grib2_to_cog(grib2_file, output)

        assert result.width == GFS_025_WIDTH
        assert result.height == GFS_025_HEIGHT

        with rasterio.open(output) as ds:
            assert ds.overviews(1) == DEFAULT_OVERVIEW_LEVELS

    def test_input_without_crs_gets_gfs_transform(self, tmp_path: Path):
        """GRIB2 files may lack CRS metadata — pipeline applies GFS transform."""
        path = tmp_path / "no_crs.grib2"
        width, height = 72, 37
        data = np.zeros((height, width), dtype=np.float32)

        with rasterio.open(
            path,
            "w",
            driver="GTiff",
            dtype="float32",
            count=1,
            width=width,
            height=height,
        ) as dst:
            dst.write(data, 1)

        output = tmp_path / "out.tif"
        result = grib2_to_cog(path, output)
        assert result.crs == TARGET_CRS

        with rasterio.open(output) as ds:
            assert ds.crs.to_epsg() == 4326
            # Verify the transform covers the global extent
            bounds = ds.bounds
            assert bounds.left == pytest.approx(-180.0)
            assert bounds.bottom == pytest.approx(-90.0)
            assert bounds.right == pytest.approx(180.0)
            assert bounds.top == pytest.approx(90.0)


# ---------------------------------------------------------------------------
# validate_cog
# ---------------------------------------------------------------------------


class TestValidateCog:
    def test_valid_cog_passes(self, small_grib2: Path, tmp_path: Path):
        output = tmp_path / "valid.tif"
        grib2_to_cog(small_grib2, output)
        assert validate_cog(output) is True

    def test_non_tiled_tiff_fails(self, tmp_path: Path):
        path = tmp_path / "stripped.tif"
        data = np.zeros((37, 72), dtype=np.float32)
        transform = from_bounds(*GFS_GLOBAL_BOUNDS, 72, 37)

        with rasterio.open(
            path,
            "w",
            driver="GTiff",
            dtype="float32",
            count=1,
            width=72,
            height=37,
            crs="EPSG:4326",
            transform=transform,
        ) as dst:
            dst.write(data, 1)

        assert validate_cog(path) is False

    def test_missing_file_fails(self, tmp_path: Path):
        assert validate_cog(tmp_path / "missing.tif") is False

    def test_valid_cog_with_extended_overviews(self, tmp_path: Path):
        """COG with extended overview levels (including 32x) should pass."""
        path = tmp_path / "input.grib2"
        width, height = GFS_025_WIDTH, GFS_025_HEIGHT
        transform = from_bounds(*GFS_GLOBAL_BOUNDS, width, height)
        data = np.random.default_rng(99).standard_normal(
            (height, width)
        ).astype(np.float32)

        with rasterio.open(
            path, "w", driver="GTiff", dtype="float32", count=1,
            width=width, height=height, crs="EPSG:4326", transform=transform,
        ) as dst:
            dst.write(data, 1)

        output = tmp_path / "extended.tif"
        config = OverviewConfig.for_continuous(extended=True)
        grib2_to_cog(path, output, overview_levels=config.levels, resampling=config.resampling)
        assert validate_cog(output) is True

    def test_no_overviews_fails(self, tmp_path: Path):
        path = tmp_path / "no_ovr.tif"
        data = np.zeros((37, 72), dtype=np.float32)
        transform = from_bounds(*GFS_GLOBAL_BOUNDS, 72, 37)

        with rasterio.open(
            path,
            "w",
            driver="GTiff",
            dtype="float32",
            count=1,
            width=72,
            height=37,
            crs="EPSG:4326",
            transform=transform,
            tiled=True,
            blockxsize=256,
            blockysize=256,
        ) as dst:
            dst.write(data, 1)

        assert validate_cog(path) is False


# ---------------------------------------------------------------------------
# Wind speed helpers
# ---------------------------------------------------------------------------


def _make_wind_component(path: Path, data: np.ndarray, width: int, height: int) -> Path:
    """Write a synthetic GRIB2-like wind component file."""
    transform = from_bounds(*GFS_GLOBAL_BOUNDS, width, height)
    with rasterio.open(
        path, "w", driver="GTiff", dtype="float32", count=1,
        width=width, height=height, crs="EPSG:4326", transform=transform,
    ) as dst:
        dst.write(data, 1)
    return path


@pytest.fixture
def wind_components(tmp_path: Path) -> tuple[Path, Path]:
    """Create matching UGRD and VGRD files (small grid)."""
    rng = np.random.default_rng(42)
    width, height = 72, 37
    u_data = rng.standard_normal((height, width)).astype(np.float32)
    v_data = rng.standard_normal((height, width)).astype(np.float32)
    ugrd = _make_wind_component(tmp_path / "ugrd.grib2", u_data, width, height)
    vgrd = _make_wind_component(tmp_path / "vgrd.grib2", v_data, width, height)
    return ugrd, vgrd


@pytest.fixture
def wind_components_fullres(tmp_path: Path) -> tuple[Path, Path]:
    """Create matching UGRD and VGRD files at full GFS 0.25° resolution."""
    rng = np.random.default_rng(7)
    u_data = rng.standard_normal((GFS_025_HEIGHT, GFS_025_WIDTH)).astype(np.float32)
    v_data = rng.standard_normal((GFS_025_HEIGHT, GFS_025_WIDTH)).astype(np.float32)
    ugrd = _make_wind_component(tmp_path / "ugrd.grib2", u_data, GFS_025_WIDTH, GFS_025_HEIGHT)
    vgrd = _make_wind_component(tmp_path / "vgrd.grib2", v_data, GFS_025_WIDTH, GFS_025_HEIGHT)
    return ugrd, vgrd


# ---------------------------------------------------------------------------
# wind_speed_to_cog
# ---------------------------------------------------------------------------


class TestWindSpeedToCog:
    def test_creates_wind_speed_cog(
        self, wind_components: tuple[Path, Path], tmp_path: Path
    ):
        ugrd, vgrd = wind_components
        output = tmp_path / "cogs" / "wind_speed" / "000.tif"
        result = wind_speed_to_cog(ugrd, vgrd, output)

        assert output.exists()
        assert isinstance(result, COGResult)
        assert result.output_path == output
        assert result.size_bytes > 0

    def test_wind_speed_values_correct(
        self, wind_components: tuple[Path, Path], tmp_path: Path
    ):
        """Verify sqrt(u² + v²) computation."""
        ugrd, vgrd = wind_components
        output = tmp_path / "out.tif"
        wind_speed_to_cog(ugrd, vgrd, output)

        with rasterio.open(ugrd) as u_src:
            u = u_src.read(1)
        with rasterio.open(vgrd) as v_src:
            v = v_src.read(1)
        expected = np.sqrt(u**2 + v**2)

        with rasterio.open(output) as ds:
            actual = ds.read(1)

        np.testing.assert_array_almost_equal(actual, expected, decimal=5)

    def test_wind_speed_non_negative(
        self, wind_components: tuple[Path, Path], tmp_path: Path
    ):
        ugrd, vgrd = wind_components
        output = tmp_path / "out.tif"
        wind_speed_to_cog(ugrd, vgrd, output)

        with rasterio.open(output) as ds:
            data = ds.read(1)
        assert np.all(data >= 0)

    def test_output_is_valid_cog(
        self, wind_components: tuple[Path, Path], tmp_path: Path
    ):
        ugrd, vgrd = wind_components
        output = tmp_path / "out.tif"
        wind_speed_to_cog(ugrd, vgrd, output)
        assert validate_cog(output) is True

    def test_output_metadata(
        self, wind_components: tuple[Path, Path], tmp_path: Path
    ):
        ugrd, vgrd = wind_components
        output = tmp_path / "out.tif"
        result = wind_speed_to_cog(ugrd, vgrd, output)

        assert result.input_path == ugrd
        assert result.crs == TARGET_CRS
        assert result.width == 72
        assert result.height == 37

    def test_missing_ugrd_raises(self, tmp_path: Path):
        vgrd = _make_wind_component(
            tmp_path / "vgrd.grib2",
            np.zeros((37, 72), dtype=np.float32), 72, 37,
        )
        with pytest.raises(FileNotFoundError, match="UGRD"):
            wind_speed_to_cog(tmp_path / "missing.grib2", vgrd, tmp_path / "out.tif")

    def test_missing_vgrd_raises(self, tmp_path: Path):
        ugrd = _make_wind_component(
            tmp_path / "ugrd.grib2",
            np.zeros((37, 72), dtype=np.float32), 72, 37,
        )
        with pytest.raises(FileNotFoundError, match="VGRD"):
            wind_speed_to_cog(ugrd, tmp_path / "missing.grib2", tmp_path / "out.tif")

    def test_mismatched_dimensions_raises(self, tmp_path: Path):
        ugrd = _make_wind_component(
            tmp_path / "ugrd.grib2",
            np.zeros((37, 72), dtype=np.float32), 72, 37,
        )
        vgrd = _make_wind_component(
            tmp_path / "vgrd.grib2",
            np.zeros((18, 36), dtype=np.float32), 36, 18,
        )
        with pytest.raises(ValueError, match="dimension mismatch"):
            wind_speed_to_cog(ugrd, vgrd, tmp_path / "out.tif")

    def test_full_resolution(
        self, wind_components_fullres: tuple[Path, Path], tmp_path: Path
    ):
        ugrd, vgrd = wind_components_fullres
        output = tmp_path / "full.tif"
        result = wind_speed_to_cog(ugrd, vgrd, output)

        assert result.width == GFS_025_WIDTH
        assert result.height == GFS_025_HEIGHT
        assert validate_cog(output) is True

    def test_custom_overview_config(
        self, wind_components: tuple[Path, Path], tmp_path: Path
    ):
        ugrd, vgrd = wind_components
        output = tmp_path / "out.tif"
        config = OverviewConfig.for_continuous(extended=True)
        result = wind_speed_to_cog(ugrd, vgrd, output, overview_config=config)
        assert result.overview_levels == EXTENDED_OVERVIEW_LEVELS

    def test_without_crs_gets_gfs_transform(self, tmp_path: Path):
        """GRIB2 files without CRS should still produce EPSG:4326 COGs."""
        width, height = 72, 37
        data = np.zeros((height, width), dtype=np.float32)
        for name in ("ugrd.grib2", "vgrd.grib2"):
            with rasterio.open(
                tmp_path / name, "w", driver="GTiff", dtype="float32",
                count=1, width=width, height=height,
            ) as dst:
                dst.write(data, 1)

        output = tmp_path / "out.tif"
        result = wind_speed_to_cog(
            tmp_path / "ugrd.grib2", tmp_path / "vgrd.grib2", output
        )
        assert result.crs == TARGET_CRS
        with rasterio.open(output) as ds:
            assert ds.crs.to_epsg() == 4326


# ---------------------------------------------------------------------------
# OverviewConfig
# ---------------------------------------------------------------------------


class TestOverviewConfig:
    def test_continuous_default(self):
        config = OverviewConfig.for_continuous()
        assert config.levels == DEFAULT_OVERVIEW_LEVELS
        assert config.resampling == Resampling.average

    def test_continuous_extended(self):
        config = OverviewConfig.for_continuous(extended=True)
        assert config.levels == EXTENDED_OVERVIEW_LEVELS
        assert 32 in config.levels
        assert config.resampling == Resampling.average

    def test_categorical_default(self):
        config = OverviewConfig.for_categorical()
        assert config.levels == DEFAULT_OVERVIEW_LEVELS
        assert config.resampling == Resampling.nearest

    def test_categorical_extended(self):
        config = OverviewConfig.for_categorical(extended=True)
        assert config.levels == EXTENDED_OVERVIEW_LEVELS
        assert config.resampling == Resampling.nearest

    def test_frozen(self):
        config = OverviewConfig.for_continuous()
        with pytest.raises(AttributeError):
            config.levels = [2, 4]

    def test_levels_not_aliased_to_module_constant(self):
        """Mutating config.levels must not corrupt the module-level list."""
        config = OverviewConfig.for_continuous()
        original = list(DEFAULT_OVERVIEW_LEVELS)
        config.levels.append(999)
        assert DEFAULT_OVERVIEW_LEVELS == original

    def test_extended_levels_are_superset(self):
        """Extended levels include all default levels plus 32x."""
        for level in DEFAULT_OVERVIEW_LEVELS:
            assert level in EXTENDED_OVERVIEW_LEVELS
        assert EXTENDED_OVERVIEW_LEVELS[-1] == 32

    def test_overview_size_overhead(self, tmp_path: Path):
        """Verify that extended overviews add minimal file size overhead.

        This is the core benchmark for wx-ir5.3.3: comparing file sizes
        with default vs extended overview levels on a full-res GFS grid.
        """
        width, height = GFS_025_WIDTH, GFS_025_HEIGHT
        transform = from_bounds(*GFS_GLOBAL_BOUNDS, width, height)
        rng = np.random.default_rng(42)
        data = rng.standard_normal((height, width)).astype(np.float32)

        path = tmp_path / "input.grib2"
        with rasterio.open(
            path, "w", driver="GTiff", dtype="float32", count=1,
            width=width, height=height, crs="EPSG:4326", transform=transform,
        ) as dst:
            dst.write(data, 1)

        # Generate COG with default overviews [2,4,8,16]
        default_out = tmp_path / "default.tif"
        grib2_to_cog(path, default_out, overview_levels=DEFAULT_OVERVIEW_LEVELS)
        default_size = default_out.stat().st_size

        # Generate COG with extended overviews [2,4,8,16,32]
        extended_out = tmp_path / "extended.tif"
        grib2_to_cog(path, extended_out, overview_levels=EXTENDED_OVERVIEW_LEVELS)
        extended_size = extended_out.stat().st_size

        # Extended should be only marginally larger (< 5% overhead)
        overhead = (extended_size - default_size) / default_size
        assert overhead < 0.05, f"Extended overview overhead too large: {overhead:.1%}"

        # Extended COG should still be valid
        assert validate_cog(extended_out) is True
