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

import numpy as np
import rasterio
from rasterio.enums import Resampling
from rasterio.vrt import WarpedVRT
from rasterio.windows import from_bounds

from weatherman.tiling.data_encoder import (
    encode_float_to_f16,
    encode_float_to_rgba,
    rgba_to_png_bytes,
)

MAX_DATA_TILE_ZOOM = 5

# Full extent of EPSG:3857 in meters
_WORLD_EXTENT = 20037508.342789244

_NEAREST_DATA_TILE_LAYERS = frozenset({"wave_direction"})


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

    Opens the COG, warps to EPSG:3857 via WarpedVRT (GDAL auto-selects
    COG overviews for efficiency), reads the tile window, and encodes
    to the requested output format.
    """
    with rasterio.open(cog_path) as src:
        with WarpedVRT(src, crs="EPSG:3857", resampling=resampling) as vrt:
            bounds = tile_bounds_3857(z, x, y)
            window = from_bounds(*bounds, transform=vrt.transform)
            data = vrt.read(
                1,
                window=window,
                out_shape=(tile_size, tile_size),
                resampling=resampling,
            ).astype(np.float32)

            return _encode_data_tile(
                data, value_min, value_max, vrt.nodata, tile_format,
            )


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

    Opens the COG once via WarpedVRT and yields (z, x, y, tile_bytes)
    for every tile in the zoom range. GDAL handles overview selection
    automatically based on the requested resolution.
    """
    with rasterio.open(cog_path) as src:
        with WarpedVRT(src, crs="EPSG:3857", resampling=resampling) as vrt:
            nodata = vrt.nodata
            for z in range(max_zoom + 1):
                n_tiles = 2**z
                for x in range(n_tiles):
                    for y in range(n_tiles):
                        bounds = tile_bounds_3857(z, x, y)
                        window = from_bounds(*bounds, transform=vrt.transform)
                        data = vrt.read(
                            1,
                            window=window,
                            out_shape=(tile_size, tile_size),
                            resampling=resampling,
                        ).astype(np.float32)

                        yield (
                            z,
                            x,
                            y,
                            _encode_data_tile(
                                data, value_min, value_max, nodata, tile_format,
                            ),
                        )
