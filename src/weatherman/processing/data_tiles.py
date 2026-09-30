"""Pre-generate data tiles from COGs for the WebGL hot path.

Supports the same two encodings as the live tile service:

- RGBA PNG: normalized values packed into bytes for broad compatibility.
- Float16 binary: physical values stored directly for high-fidelity GPU input.

These tiles are stored alongside COGs and served as static reads,
eliminating the TiTiler roundtrip for the hottest WebGL data requests.

Web Mercator math reference: OGC TMS / Slippy Map convention.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

import numpy as np
import rasterio
from affine import Affine
from rasterio.enums import Resampling
from rasterio.io import DatasetReader, MemoryFile
from rasterio.transform import from_bounds
from rasterio.vrt import WarpedVRT

from weatherman.tiling.data_encoder import (
    encode_float_to_f16,
    encode_float_to_rgba,
    rgba_to_png_bytes,
)

MAX_DATA_TILE_ZOOM = 5

# Full extent of EPSG:3857 in meters
_WORLD_EXTENT = 20037508.342789244

_NEAREST_DATA_TILE_LAYERS = frozenset({"wave_direction"})

# Source columns copied across the antimeridian onto each side of a global
# grid, so resampling at ±180° interpolates across it instead of stopping at
# the raster's edge (which shows as a seam along the date line).
_WRAP_COLUMNS = 4


def _encode_data_tile(
    data: np.ndarray,
    value_min: float,
    value_max: float,
    nodata: float | None,
    tile_format: str,
) -> bytes:
    if tile_format == "png":
        rgba = encode_float_to_rgba(data, value_min, value_max, nodata=nodata)
        return rgba_to_png_bytes(rgba)
    if tile_format == "f16":
        return encode_float_to_f16(data, nodata=nodata)
    raise ValueError(
        f"Unsupported data tile format '{tile_format}' (expected 'png' or 'f16')"
    )


def tile_bounds_3857(z: int, x: int, y: int) -> tuple[float, float, float, float]:
    """Convert z/x/y tile coordinates to EPSG:3857 meter bounds.

    Returns (west, south, east, north) in Web Mercator meters.
    """
    n_tiles = 2**z
    tile_size = 2 * _WORLD_EXTENT / n_tiles

    west = -_WORLD_EXTENT + x * tile_size
    east = west + tile_size

    # Y axis is inverted: y=0 is the top (north)
    north = _WORLD_EXTENT - y * tile_size
    south = north - tile_size

    return (west, south, east, north)


def data_tile_resampling_for_layer(layer: str) -> Resampling:
    """Return the raster resampling strategy for a layer's data tiles."""
    if layer in _NEAREST_DATA_TILE_LAYERS:
        return Resampling.nearest
    return Resampling.bilinear


@contextmanager
def _open_for_tiling(cog_path: str) -> Iterator[DatasetReader]:
    """Open a COG as the source for tile warping.

    A lat/lon grid that spans the full 360° is returned with
    ``_WRAP_COLUMNS`` extra columns on each side, copied from the opposite
    edge. Anything else is returned as is.
    """
    with rasterio.open(cog_path) as src:
        spans_globe = (
            src.crs is not None
            and src.crs.is_geographic
            and abs(src.width * src.res[0] - 360.0) < 1e-6
        )
        if not spans_globe:
            yield src
            return

        data = src.read(1)
        wrapped = np.concatenate(
            [data[:, -_WRAP_COLUMNS:], data, data[:, :_WRAP_COLUMNS]], axis=1,
        )
        profile = {
            "driver": "GTiff",
            "height": src.height,
            "width": wrapped.shape[1],
            "count": 1,
            "dtype": wrapped.dtype,
            "crs": src.crs,
            "transform": src.transform * Affine.translation(-_WRAP_COLUMNS, 0),
            "nodata": src.nodata,
        }

    with MemoryFile() as memfile:
        with memfile.open(**profile) as dst:
            dst.write(wrapped, 1)
        with memfile.open() as padded:
            yield padded


def _warp_tile(
    src: DatasetReader,
    z: int,
    x: int,
    y: int,
    tile_size: int,
    resampling: Resampling,
) -> tuple[np.ndarray, float | None]:
    """Warp the source straight onto one tile's pixel grid.

    The grid is given explicitly. Left to itself, WarpedVRT picks one grid for
    the whole dataset, and for a pole-to-pole source (unbounded in Web
    Mercator) that grid is a few hundred km per pixel and stops short of the
    antimeridian — tiles read out of it are blurred and wrong at the edge.

    Returns (values, nodata).
    """
    transform = from_bounds(*tile_bounds_3857(z, x, y), tile_size, tile_size)
    with WarpedVRT(
        src,
        crs="EPSG:3857",
        transform=transform,
        width=tile_size,
        height=tile_size,
        resampling=resampling,
    ) as vrt:
        return vrt.read(1).astype(np.float32), vrt.nodata


def generate_data_tile(
    cog_path: str,
    z: int,
    x: int,
    y: int,
    value_min: float,
    value_max: float,
    tile_size: int = 256,
    resampling: Resampling = Resampling.bilinear,
    tile_format: str = "png",
) -> bytes:
    """Generate a single pre-generated data tile from a COG.

    Warps the COG onto the tile's EPSG:3857 pixel grid and encodes the
    result in the requested output format.
    """
    with _open_for_tiling(cog_path) as src:
        data, nodata = _warp_tile(src, z, x, y, tile_size, resampling)
        return _encode_data_tile(data, value_min, value_max, nodata, tile_format)


def generate_all_data_tiles(
    cog_path: str,
    value_min: float,
    value_max: float,
    max_zoom: int = MAX_DATA_TILE_ZOOM,
    tile_size: int = 256,
    resampling: Resampling = Resampling.bilinear,
    tile_format: str = "png",
) -> Iterator[tuple[int, int, int, bytes]]:
    """Generate pre-generated data tiles for z0 through max_zoom from a COG.

    Opens the COG once and yields (z, x, y, tile_bytes) for every tile in
    the zoom range.
    """
    with _open_for_tiling(cog_path) as src:
        for z in range(max_zoom + 1):
            n_tiles = 2**z
            for x in range(n_tiles):
                for y in range(n_tiles):
                    data, nodata = _warp_tile(src, z, x, y, tile_size, resampling)
                    yield (
                        z,
                        x,
                        y,
                        _encode_data_tile(
                            data, value_min, value_max, nodata, tile_format,
                        ),
                    )
