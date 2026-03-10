"""Encode float32 weather values into RGBA PNGs for GPU-side decoding.

The encoding packs a normalized float into 16-bit unsigned integers spread
across R (low byte) and G (high byte) channels, with B used as a nodata
flag and A fixed to 0xFF to prevent PNG pre-multiplication artifacts.

Decoding on the GPU (GLSL):
    float value = (pixel.r + pixel.g * 256.0) / 65535.0;
    float physical = mix(vmin, vmax, value);
    bool nodata = pixel.b > 0.5;
"""

from __future__ import annotations

import io

import numpy as np
from numpy.typing import NDArray


def encode_float_to_rgba(
    data: NDArray[np.floating],
    value_min: float,
    value_max: float,
    nodata: float | None = None,
) -> NDArray[np.uint8]:
    """Encode a 2D float array into a (H, W, 4) RGBA uint8 array.

    Args:
        data: 2D array of float32 weather values.
        value_min: Physical minimum (maps to 0).
        value_max: Physical maximum (maps to 65535).
        nodata: Sentinel value for missing data. NaN is always treated as nodata.

    Returns:
        (H, W, 4) uint8 array with channels [R_low, G_high, B_flag, A=0xFF].
    """
    h, w = data.shape[:2]
    rgba = np.zeros((h, w, 4), dtype=np.uint8)

    # Build nodata mask: NaN always, plus explicit sentinel
    mask = np.isnan(data)
    if nodata is not None and not np.isnan(nodata):
        mask |= data == nodata

    # Normalize to [0, 1], clamp to valid range
    value_range = value_max - value_min
    if value_range == 0:
        normalized = np.zeros_like(data)
    else:
        normalized = (data - value_min) / value_range
    normalized = np.clip(normalized, 0.0, 1.0)

    # Zero out NaN before conversion to avoid RuntimeWarning
    normalized = np.where(mask, 0.0, normalized)

    # Scale to 16-bit unsigned integer
    encoded = (normalized * 65535.0).astype(np.uint16)

    # R = low byte, G = high byte
    rgba[:, :, 0] = (encoded & 0xFF).astype(np.uint8)
    rgba[:, :, 1] = ((encoded >> 8) & 0xFF).astype(np.uint8)

    # B = nodata flag (0x00 valid, 0xFF nodata)
    rgba[:, :, 2] = np.where(mask, 0xFF, 0x00).astype(np.uint8)

    # A = 0xFF always
    rgba[:, :, 3] = 0xFF

    # Zero out R,G for nodata pixels (clean encoding)
    rgba[mask, 0] = 0
    rgba[mask, 1] = 0

    return rgba


def rgba_to_png_bytes(rgba: NDArray[np.uint8]) -> bytes:
    """Encode an RGBA array to PNG bytes using Pillow.

    Args:
        rgba: (H, W, 4) uint8 array.

    Returns:
        PNG file contents as bytes.
    """
    from PIL import Image

    img = Image.fromarray(rgba, mode="RGBA")
    buf = io.BytesIO()
    img.save(buf, format="PNG", compress_level=1)  # fast compression
    return buf.getvalue()


def decode_rgba_to_float(
    rgba: NDArray[np.uint8],
    value_min: float,
    value_max: float,
) -> tuple[NDArray[np.floating], NDArray[np.bool_]]:
    """Decode an RGBA-encoded tile back to float values (for testing).

    Args:
        rgba: (H, W, 4) uint8 array from encode_float_to_rgba.
        value_min: Physical minimum used during encoding.
        value_max: Physical maximum used during encoding.

    Returns:
        Tuple of (values, nodata_mask) where values is float32 and
        nodata_mask is boolean.
    """
    r = rgba[:, :, 0].astype(np.uint16)
    g = rgba[:, :, 1].astype(np.uint16)
    b = rgba[:, :, 2]

    encoded = r | (g << 8)
    normalized = encoded.astype(np.float32) / 65535.0
    values = normalized * (value_max - value_min) + value_min

    nodata_mask = b == 0xFF
    return values, nodata_mask
