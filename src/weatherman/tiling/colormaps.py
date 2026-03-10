"""Custom colormaps for weather layer rendering.

TiTiler accepts colormaps as JSON mappings of pixel value -> [R, G, B, A].
We define colormaps for our core weather layers and provide utilities to
serialize them for TiTiler's colormap query parameter.

Reference ranges:
  - Temperature (2m): -55°C to +55°C (GRIB2/GDAL delivers Celsius)
  - Wind speed (10m): 0 to 50 m/s
  - Precipitation (accumulated): 0 to 250 kg/m² (≡ mm)
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
    stops: tuple[tuple[float, tuple[int, int, int]], ...] = ()

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

# Temperature: Windy-style multi-hue spectral (-55°C violet -> 0°C green -> +55°C magenta)
TEMPERATURE_STOPS: list[tuple[float, tuple[int, int, int]]] = [
    (0.00, (45, 0, 75)),       # deep violet (-55°C)
    (0.10, (60, 10, 150)),     # purple (-44°C)
    (0.18, (30, 50, 200)),     # blue (-35°C)
    (0.27, (0, 90, 230)),      # bright blue (-25°C)
    (0.36, (0, 180, 210)),     # vivid cyan (-15°C)
    (0.45, (0, 200, 80)),      # bright green (-5°C)
    (0.50, (80, 220, 20)),     # yellow-green (0°C)
    (0.55, (220, 220, 0)),     # pure yellow (5°C)
    (0.64, (255, 180, 0)),     # vivid orange (15°C)
    (0.73, (255, 100, 0)),     # bright orange (25°C)
    (0.82, (230, 30, 15)),     # vivid red (35°C)
    (0.91, (180, 0, 0)),       # deep red (45°C)
    (1.00, (130, 0, 50)),      # magenta (55°C)
]

TEMPERATURE = WeatherColormap(
    name="temperature",
    unit="°C",
    value_min=-55.0,
    value_max=55.0,
    colormap=_interpolate_colors(TEMPERATURE_STOPS),
    stops=tuple(TEMPERATURE_STOPS),
)

# Wind speed: spectral blue→red by 35 kt (18 m/s), then purple (hurricane)
WIND_SPEED_STOPS: list[tuple[float, tuple[int, int, int]]] = [
    (0.00, (30, 50, 200)),    # blue (0 m/s, calm)
    (0.07, (0, 90, 230)),     # bright blue (~3.5 m/s)
    (0.12, (0, 180, 210)),    # vivid cyan (~6 m/s)
    (0.18, (0, 200, 80)),     # bright green (~9 m/s)
    (0.23, (80, 220, 20)),    # yellow-green (~11.5 m/s)
    (0.27, (220, 220, 0)),    # pure yellow (~13.5 m/s)
    (0.31, (255, 180, 0)),    # vivid orange (~15.5 m/s)
    (0.34, (255, 100, 0)),    # bright orange (~17 m/s)
    (0.36, (230, 30, 15)),    # vivid red (~18 m/s / 35 kt)
    (0.50, (180, 0, 0)),      # deep red (~25 m/s)
    (0.70, (130, 0, 80)),     # red-purple (~35 m/s)
    (0.85, (90, 0, 140)),     # purple (~42.5 m/s)
    (1.00, (60, 0, 160)),     # deep purple (50 m/s, hurricane)
]

WIND_SPEED = WeatherColormap(
    name="wind_speed",
    unit="kt",
    value_min=0.0,
    value_max=50.0,  # TiTiler rescale stays in m/s (raw GFS units)
    colormap=_interpolate_colors(WIND_SPEED_STOPS),
    stops=tuple(WIND_SPEED_STOPS),
)

# Precipitation: white-to-green sequential (total accumulated, kg/m² ≡ mm)
PRECIPITATION_STOPS: list[tuple[float, tuple[int, int, int]]] = [
    (0.0, (255, 255, 255)),   # white (no precip)
    (0.15, (199, 233, 192)),  # very light green
    (0.3, (120, 198, 168)),   # green
    (0.5, (65, 171, 93)),     # medium green
    (0.7, (35, 132, 67)),     # dark green
    (0.85, (0, 90, 50)),      # very dark green
    (1.0, (0, 50, 30)),       # near-black green (250 kg/m²)
]

PRECIPITATION = WeatherColormap(
    name="precipitation",
    unit="kg/m²",
    value_min=0.0,
    value_max=250.0,
    colormap=_interpolate_colors(PRECIPITATION_STOPS),
    stops=tuple(PRECIPITATION_STOPS),
)


# Pressure: purple-blue-green-yellow-orange-red (low→high)
PRESSURE_STOPS: list[tuple[float, tuple[int, int, int]]] = [
    (0.00, (60, 0, 160)),     # deep purple (920 hPa, deep low)
    (0.15, (30, 50, 200)),    # blue (940 hPa)
    (0.30, (0, 150, 200)),    # cyan (960 hPa)
    (0.45, (0, 200, 80)),     # green (980 hPa)
    (0.57, (180, 220, 40)),   # yellow-green (1000 hPa)
    (0.64, (220, 220, 0)),    # yellow (1010 hPa, standard)
    (0.75, (255, 180, 0)),    # orange (1025 hPa)
    (0.85, (255, 100, 0)),    # red-orange (1040 hPa)
    (1.00, (180, 0, 0)),      # red (1060 hPa, strong high)
]

PRESSURE = WeatherColormap(
    name="pressure",
    unit="Pa",
    value_min=92000.0,
    value_max=106000.0,
    colormap=_interpolate_colors(PRESSURE_STOPS),
    stops=tuple(PRESSURE_STOPS),
)

# Cloud cover: white-to-dark-gray sequential
CLOUD_COVER_STOPS: list[tuple[float, tuple[int, int, int]]] = [
    (0.00, (240, 248, 255)),  # near-white (clear sky)
    (0.25, (200, 210, 220)),  # light gray
    (0.50, (160, 170, 180)),  # mid gray
    (0.75, (110, 120, 130)),  # dark gray
    (1.00, (60, 65, 75)),     # very dark gray (overcast)
]

CLOUD_COVER = WeatherColormap(
    name="cloud_cover",
    unit="%",
    value_min=0.0,
    value_max=100.0,
    colormap=_interpolate_colors(CLOUD_COVER_STOPS),
    stops=tuple(CLOUD_COVER_STOPS),
)

# Wave height: blue-cyan-green-yellow-orange-red-purple
WAVE_HEIGHT_STOPS: list[tuple[float, tuple[int, int, int]]] = [
    (0.00, (30, 50, 200)),    # blue (calm, 0 m)
    (0.10, (0, 130, 220)),    # bright blue (~1.5 m)
    (0.20, (0, 190, 180)),    # cyan (~3 m)
    (0.33, (0, 200, 80)),     # green (~5 m)
    (0.47, (220, 220, 0)),    # yellow (~7 m)
    (0.60, (255, 160, 0)),    # orange (~9 m)
    (0.73, (230, 30, 15)),    # red (~11 m)
    (0.87, (180, 0, 0)),      # deep red (~13 m)
    (1.00, (130, 0, 80)),     # purple (15 m, extreme)
]

WAVE_HEIGHT = WeatherColormap(
    name="wave_height",
    unit="m",
    value_min=0.0,
    value_max=15.0,
    colormap=_interpolate_colors(WAVE_HEIGHT_STOPS),
    stops=tuple(WAVE_HEIGHT_STOPS),
)

# Wave period: blue-to-purple sequential (short→long swell)
WAVE_PERIOD_STOPS: list[tuple[float, tuple[int, int, int]]] = [
    (0.00, (30, 50, 200)),    # blue (short period, 0 s)
    (0.15, (0, 160, 210)),    # cyan (~3.75 s)
    (0.30, (0, 200, 80)),     # green (~7.5 s)
    (0.50, (220, 220, 0)),    # yellow (~12.5 s)
    (0.70, (255, 140, 0)),    # orange (~17.5 s)
    (0.85, (230, 30, 15)),    # red (~21.25 s)
    (1.00, (130, 0, 80)),     # purple (25 s, long swell)
]

WAVE_PERIOD = WeatherColormap(
    name="wave_period",
    unit="s",
    value_min=0.0,
    value_max=25.0,
    colormap=_interpolate_colors(WAVE_PERIOD_STOPS),
    stops=tuple(WAVE_PERIOD_STOPS),
)

# Wave direction: cyclic HSL-inspired hue wheel (wraps 0°→360°→0°)
WAVE_DIRECTION_STOPS: list[tuple[float, tuple[int, int, int]]] = [
    (0.000, (230, 30, 15)),   # red (N, 0°)
    (0.125, (255, 160, 0)),   # orange (NE, 45°)
    (0.250, (220, 220, 0)),   # yellow (E, 90°)
    (0.375, (0, 200, 80)),    # green (SE, 135°)
    (0.500, (0, 160, 210)),   # cyan (S, 180°)
    (0.625, (30, 50, 200)),   # blue (SW, 225°)
    (0.750, (100, 0, 180)),   # purple (W, 270°)
    (0.875, (180, 0, 100)),   # magenta (NW, 315°)
    (1.000, (230, 30, 15)),   # red (N, 360° = 0°, wraps)
]

WAVE_DIRECTION = WeatherColormap(
    name="wave_direction",
    unit="degree",
    value_min=0.0,
    value_max=360.0,
    colormap=_interpolate_colors(WAVE_DIRECTION_STOPS),
    stops=tuple(WAVE_DIRECTION_STOPS),
)


# Wind U/V components (Cartesian): for data-encoded tiles only.
# These use the same value range but have no visible colormap —
# the shader reconstructs speed from U,V for color ramp lookup.
WIND_U = WeatherColormap(
    name="wind_u",
    unit="m/s",
    value_min=-50.0,
    value_max=50.0,
    colormap=_interpolate_colors(WIND_SPEED_STOPS),
    stops=tuple(WIND_SPEED_STOPS),
)

WIND_V = WeatherColormap(
    name="wind_v",
    unit="m/s",
    value_min=-50.0,
    value_max=50.0,
    colormap=_interpolate_colors(WIND_SPEED_STOPS),
    stops=tuple(WIND_SPEED_STOPS),
)


# Registry for lookup by layer name
COLORMAPS: dict[str, WeatherColormap] = {
    "temperature": TEMPERATURE,
    "wind_speed": WIND_SPEED,
    "wind_u": WIND_U,
    "wind_v": WIND_V,
    "precipitation": PRECIPITATION,
    "pressure": PRESSURE,
    "cloud_cover": CLOUD_COVER,
    "wave_height": WAVE_HEIGHT,
    "wave_period": WAVE_PERIOD,
    "wave_direction": WAVE_DIRECTION,
}


def get_colormap(layer_name: str) -> WeatherColormap:
    """Look up a colormap by layer name.

    Raises:
        KeyError: If no colormap is registered for the given layer.
    """
    return COLORMAPS[layer_name]


def get_value_range(layer_name: str) -> tuple[float, float]:
    """Return (value_min, value_max) for a layer.

    Used by the data tile encoder to normalize float values into [0, 1].

    Raises:
        KeyError: If no colormap is registered for the given layer.
    """
    cmap = COLORMAPS[layer_name]
    return (cmap.value_min, cmap.value_max)


def export_color_ramps() -> dict[str, dict]:
    """Export all color ramp definitions as JSON-serializable dicts.

    Returns a mapping of layer name to ramp metadata including color stops,
    value range, and unit. Used by the frontend to build GPU color ramp
    textures without hardcoding color definitions.
    """
    result: dict[str, dict] = {}
    for name, cmap in COLORMAPS.items():
        result[name] = {
            "name": cmap.name,
            "unit": cmap.unit,
            "valueMin": cmap.value_min,
            "valueMax": cmap.value_max,
            "stops": [
                {"position": pos, "color": list(rgb)}
                for pos, rgb in cmap.stops
            ],
        }
    return result
