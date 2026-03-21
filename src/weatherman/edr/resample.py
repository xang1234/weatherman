"""Geodesic LineString resampling for EDR trajectory queries.

Given a sequence of waypoints, produces N equidistant sample points
along the great-circle path.  Each sample carries its cumulative
distance from the route start in nautical miles.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

# Earth radius in nautical miles (WGS-84 mean radius)
_EARTH_NM = 3440.065


@dataclass(frozen=True)
class SamplePoint:
    """A resampled point along a route."""

    lon: float
    lat: float
    distance_nm: float  # cumulative from start


def _haversine_nm(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    """Great-circle distance between two points in nautical miles."""
    rlat1, rlat2 = math.radians(lat1), math.radians(lat2)
    dlat = rlat2 - rlat1
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2) ** 2
    return 2 * _EARTH_NM * math.asin(min(math.sqrt(min(a, 1.0)), 1.0))


def _interpolate_gc(
    lon1: float, lat1: float, lon2: float, lat2: float, fraction: float,
) -> tuple[float, float]:
    """Interpolate along a great-circle arc at the given fraction (0–1)."""
    if fraction <= 0:
        return lon1, lat1
    if fraction >= 1:
        return lon2, lat2

    rlat1, rlon1 = math.radians(lat1), math.radians(lon1)
    rlat2, rlon2 = math.radians(lat2), math.radians(lon2)

    a = (
        math.sin((rlat2 - rlat1) / 2) ** 2
        + math.cos(rlat1) * math.cos(rlat2) * math.sin((rlon2 - rlon1) / 2) ** 2
    )
    d = 2 * math.asin(min(math.sqrt(min(a, 1.0)), 1.0))

    if d < 1e-12:
        return lon1, lat1

    a = math.sin((1 - fraction) * d) / math.sin(d)
    b = math.sin(fraction * d) / math.sin(d)

    x = a * math.cos(rlat1) * math.cos(rlon1) + b * math.cos(rlat2) * math.cos(rlon2)
    y = a * math.cos(rlat1) * math.sin(rlon1) + b * math.cos(rlat2) * math.sin(rlon2)
    z = a * math.sin(rlat1) + b * math.sin(rlat2)

    lat = math.degrees(math.atan2(z, math.sqrt(x ** 2 + y ** 2)))
    lon = math.degrees(math.atan2(y, x))
    return lon, lat


def resample_linestring(
    coords: list[tuple[float, float]],
    num_samples: int = 40,
) -> list[SamplePoint]:
    """Resample a LineString into equidistant points along the route.

    Args:
        coords: List of (lon, lat) waypoints.
        num_samples: Number of output sample points (including start and end).

    Returns:
        List of SamplePoint with equidistant spacing along the route.

    Raises:
        ValueError: If fewer than 2 waypoints or num_samples < 2.
    """
    if len(coords) < 2:
        raise ValueError("LineString must have at least 2 coordinates")
    if num_samples < 2:
        raise ValueError("num_samples must be at least 2")

    # Compute cumulative distances along segments
    seg_distances: list[float] = []
    for i in range(len(coords) - 1):
        d = _haversine_nm(coords[i][0], coords[i][1], coords[i + 1][0], coords[i + 1][1])
        seg_distances.append(d)

    total_distance = sum(seg_distances)
    if total_distance < 1e-9:
        return [SamplePoint(lon=coords[0][0], lat=coords[0][1], distance_nm=0.0)] * num_samples

    cumulative = [0.0]
    for d in seg_distances:
        cumulative.append(cumulative[-1] + d)

    # Generate equidistant target distances
    step = total_distance / (num_samples - 1)
    samples: list[SamplePoint] = []
    seg_idx = 0

    for i in range(num_samples):
        target_dist = i * step
        # Clamp to total distance for the last point
        if i == num_samples - 1:
            target_dist = total_distance

        # Advance to the segment containing target_dist
        while seg_idx < len(seg_distances) - 1 and cumulative[seg_idx + 1] < target_dist:
            seg_idx += 1

        seg_start = cumulative[seg_idx]
        seg_len = seg_distances[seg_idx]
        frac = (target_dist - seg_start) / seg_len if seg_len > 1e-9 else 0.0

        lon, lat = _interpolate_gc(
            coords[seg_idx][0], coords[seg_idx][1],
            coords[seg_idx + 1][0], coords[seg_idx + 1][1],
            frac,
        )
        samples.append(SamplePoint(lon=lon, lat=lat, distance_nm=target_dist))

    return samples
