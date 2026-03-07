"""Custom colormaps for weather layer rendering.

TiTiler accepts colormaps as JSON mappings of pixel value -> [R, G, B, A].
We define colormaps for our core weather layers and provide utilities to
serialize them for TiTiler's colormap query parameter.

Reference ranges:
  - Temperature (2m): 220K to 330K (-53C to +57C)
  - Wind speed (10m): 0 to 50 m/s
  - Precipitation rate: 0 to 50 mm/hr
"""

from __future__ import annotations

import json
from dataclasses import dataclass

# Type alias: maps integer pixel values (0-255) to RGBA tuples
ColormapDict = dict[int, tuple[int, int, int, int]]


@dataclass(frozen=True)
class WeatherColormap:
    """A named colormap for a weather layer with value range metadata."""

    name: str
    unit: str
    value_min: float
    value_max: float
    colormap: ColormapDict

    def to_json(self) -> str:
        """Serialize the colormap for TiTiler's colormap query parameter."""
        return json.dumps(
            {str(k): list(v) for k, v in self.colormap.items()},
            separators=(",", ":"),
        )

    def rescale_range(self) -> str:
        """Return the rescale parameter string for TiTiler: 'min,max'."""
        return f"{self.value_min},{self.value_max}"


def _interpolate_colors(
    stops: list[tuple[float, tuple[int, int, int]]],
    steps: int = 256,
) -> ColormapDict:
    """Linearly interpolate between color stops to build a 256-entry colormap.

    Args:
        stops: List of (position, (R, G, B)) where position is 0.0-1.0.
        steps: Number of entries in the output (default 256).

    Returns:
        ColormapDict mapping 0..steps-1 to RGBA tuples.
    """
    cmap: ColormapDict = {}
    for i in range(steps):
        t = i / (steps - 1)
        # Find surrounding stops
        lower_idx = 0
        for j in range(len(stops) - 1):
            if stops[j + 1][0] >= t:
                lower_idx = j
                break
        else:
            lower_idx = len(stops) - 2

        t0, c0 = stops[lower_idx]
        t1, c1 = stops[lower_idx + 1]
        if t1 == t0:
            frac = 0.0
        else:
            frac = (t - t0) / (t1 - t0)
        frac = max(0.0, min(1.0, frac))

        r = int(c0[0] + (c1[0] - c0[0]) * frac)
        g = int(c0[1] + (c1[1] - c0[1]) * frac)
        b = int(c0[2] + (c1[2] - c0[2]) * frac)
        cmap[i] = (r, g, b, 255)
    return cmap


# -- Colormap definitions --

# Temperature: blue-white-red diverging (220K cold blue -> 275K white -> 330K hot red)
TEMPERATURE_STOPS: list[tuple[float, tuple[int, int, int]]] = [
    (0.0, (5, 48, 97)),       # deep blue (220K / -53C)
    (0.2, (33, 102, 172)),    # medium blue
    (0.4, (146, 197, 222)),   # light blue
    (0.5, (247, 247, 247)),   # near-white (275K / 2C)
    (0.6, (244, 165, 130)),   # light red
    (0.8, (214, 96, 77)),     # medium red
    (1.0, (178, 24, 43)),     # deep red (330K / 57C)
]

TEMPERATURE = WeatherColormap(
    name="temperature",
    unit="K",
    value_min=220.0,
    value_max=330.0,
    colormap=_interpolate_colors(TEMPERATURE_STOPS),
)

# Wind speed: sequential blue-to-purple (calm -> hurricane force)
WIND_SPEED_STOPS: list[tuple[float, tuple[int, int, int]]] = [
    (0.0, (240, 249, 232)),   # very light green (0 m/s, calm)
    (0.2, (186, 228, 188)),   # light green
    (0.4, (123, 204, 196)),   # teal
    (0.6, (67, 162, 202)),    # blue
    (0.8, (8, 104, 172)),     # dark blue
    (1.0, (80, 2, 113)),      # deep purple (50 m/s, hurricane)
]

WIND_SPEED = WeatherColormap(
    name="wind_speed",
    unit="m/s",
    value_min=0.0,
    value_max=50.0,
    colormap=_interpolate_colors(WIND_SPEED_STOPS),
)

# Precipitation rate: white-to-blue sequential
PRECIPITATION_STOPS: list[tuple[float, tuple[int, int, int]]] = [
    (0.0, (255, 255, 255)),   # white (no precip)
    (0.15, (199, 233, 192)),  # very light green
    (0.3, (120, 198, 168)),   # green
    (0.5, (65, 171, 93)),     # medium green
    (0.7, (35, 132, 67)),     # dark green
    (0.85, (0, 90, 50)),      # very dark green
    (1.0, (0, 50, 30)),       # near-black green (50 mm/hr)
]

PRECIPITATION = WeatherColormap(
    name="precipitation",
    unit="mm/hr",
    value_min=0.0,
    value_max=50.0,
    colormap=_interpolate_colors(PRECIPITATION_STOPS),
)


# Registry for lookup by layer name
COLORMAPS: dict[str, WeatherColormap] = {
    "temperature": TEMPERATURE,
    "wind_speed": WIND_SPEED,
    "precipitation": PRECIPITATION,
}


def get_colormap(layer_name: str) -> WeatherColormap:
    """Look up a colormap by layer name.

    Raises:
        KeyError: If no colormap is registered for the given layer.
    """
    return COLORMAPS[layer_name]
