"""Isobars and H/L centres from a mean-sea-level pressure field.

Turns a global pressure grid into GeoJSON: smoothed contour lines every few
hPa, plus points at the high- and low-pressure centres. The field is smoothed
and thinned first — isobars are a synoptic picture, and contouring the raw
0.25° grid gives wiggly lines and tens of thousands of vertices.
"""

from __future__ import annotations

import math
from typing import Any

import contourpy
import numpy as np
from shapely.geometry import LineString

# Contour interval, as on most synoptic charts.
INTERVAL_HPA = 4.0
# Smoothing (in grid cells of the input) before contouring.
_SMOOTH_SIGMA = 3.0
# Contour on every n-th cell of the smoothed field.
_THIN = 2
# Vertex tolerance (degrees) after contouring.
_SIMPLIFY_DEG = 0.08
# Half-width (degrees) of the box a pressure centre must dominate.
_CENTRE_RADIUS_DEG = 7.0
# How far (hPa) a centre must stand out from its box's mean.
_CENTRE_PROMINENCE_HPA = 3.0
# Centres nearer the poles than this are skipped (Mercator stretches them).
_CENTRE_MAX_LAT = 70.0


def isobars_geojson(
    pressure_pa: np.ndarray,
    lat: np.ndarray,
    lon: np.ndarray,
) -> dict[str, Any]:
    """GeoJSON FeatureCollection of isobars and pressure centres.

    Args:
        pressure_pa: 2-D (lat, lon) mean-sea-level pressure in Pa, covering
            the globe with ``lon`` in -180..180 (the last column wraps to the
            first).
        lat, lon: 1-D coordinates of the grid.

    Lines have ``{"kind": "isobar", "hpa": 1012}``; points have
    ``{"kind": "high" | "low", "hpa": 1031}``.
    """
    hpa = _smooth(np.asarray(pressure_pa, dtype=np.float64) / 100.0, _SMOOTH_SIGMA)
    thinned = hpa[::_THIN, ::_THIN]
    lat_c = np.asarray(lat, dtype=np.float64)[::_THIN]
    lon_t = np.asarray(lon, dtype=np.float64)[::_THIN]
    # Repeat the first column one turn east so lines close across ±180°.
    field = np.concatenate([thinned, thinned[:, :1]], axis=1)
    lon_c = np.append(lon_t, lon_t[0] + 360.0)

    features: list[dict[str, Any]] = []
    generator = contourpy.contour_generator(lon_c, lat_c, field, line_type="Separate")
    first = math.ceil(np.nanmin(field) / INTERVAL_HPA) * INTERVAL_HPA
    for level in np.arange(first, np.nanmax(field), INTERVAL_HPA):
        for line in generator.lines(level):
            if len(line) < 3:
                continue
            simplified = LineString(line).simplify(_SIMPLIFY_DEG)
            coords = [[round(x, 2), round(y, 2)] for x, y in simplified.coords]
            features.append({
                "type": "Feature",
                "geometry": {"type": "LineString", "coordinates": coords},
                "properties": {"kind": "isobar", "hpa": int(level)},
            })

    step = float(abs(lat_c[1] - lat_c[0]))
    features.extend(_centres(thinned, lat_c, lon_t, step))
    return {"type": "FeatureCollection", "features": features}


def _centres(hpa: np.ndarray, lat: np.ndarray, lon: np.ndarray, step: float) -> list[dict[str, Any]]:
    """Points at local pressure extremes that stand out from their surroundings."""
    radius = max(1, round(_CENTRE_RADIUS_DEG / step))
    # Pad with wrap in longitude and edge values in latitude, then scan
    # separable windows: exact for a box and cheap.
    padded = np.pad(hpa, ((radius, radius), (0, 0)), mode="edge")
    padded = np.pad(padded, ((0, 0), (radius, radius)), mode="wrap")
    box_max = _box(padded, radius, np.max)
    box_min = _box(padded, radius, np.min)
    box_mean = _box(padded, radius, np.mean)

    candidates: list[tuple[float, int, int, str]] = []
    for kind, extreme, sign in (("high", box_max, 1.0), ("low", box_min, -1.0)):
        rows, cols = np.nonzero(
            (hpa == extreme)
            & (sign * (hpa - box_mean) > _CENTRE_PROMINENCE_HPA)
            & (np.abs(lat)[:, None] <= _CENTRE_MAX_LAT)
        )
        for r, c in zip(rows, cols):
            candidates.append((sign * (hpa[r, c] - box_mean[r, c]), int(r), int(c), kind))

    # Keep the most prominent of any centres closer than the radius (a flat
    # extreme can tie over several cells).
    kept: list[tuple[int, int, str]] = []
    for _, r, c, kind in sorted(candidates, reverse=True):
        if all(abs(r - kr) > radius or _lon_cells(c, kc, hpa.shape[1]) > radius for kr, kc, _ in kept):
            kept.append((r, c, kind))

    return [
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [round(float(lon[c]), 2), round(float(lat[r]), 2)]},
            "properties": {"kind": kind, "hpa": int(round(hpa[r, c]))},
        }
        for r, c, kind in kept
    ]


def _lon_cells(a: int, b: int, width: int) -> int:
    """Distance in columns between two longitudes, around the globe."""
    d = abs(a - b) % width
    return min(d, width - d)


def _box(padded: np.ndarray, radius: int, reduce: Any) -> np.ndarray:
    """Reduce over a (2r+1)² box around each cell of the unpadded grid."""
    window = 2 * radius + 1
    rows = np.lib.stride_tricks.sliding_window_view(padded, window, axis=0)
    rows = reduce(rows, axis=-1)
    cols = np.lib.stride_tricks.sliding_window_view(rows, window, axis=1)
    return reduce(cols, axis=-1)


def _smooth(field: np.ndarray, sigma: float) -> np.ndarray:
    """Separable Gaussian blur; wraps in longitude, clamps in latitude."""
    radius = int(math.ceil(3 * sigma))
    x = np.arange(-radius, radius + 1)
    kernel = np.exp(-0.5 * (x / sigma) ** 2)
    kernel /= kernel.sum()
    padded = np.pad(field, ((radius, radius), (0, 0)), mode="edge")
    padded = np.pad(padded, ((0, 0), (radius, radius)), mode="wrap")
    out = np.apply_along_axis(lambda v: np.convolve(v, kernel, mode="valid"), 0, padded)
    return np.apply_along_axis(lambda v: np.convolve(v, kernel, mode="valid"), 1, out)
