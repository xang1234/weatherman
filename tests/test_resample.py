"""Tests for geodesic LineString resampling."""

from __future__ import annotations

import pytest

from weatherman.edr.resample import SamplePoint, _haversine_nm, resample_linestring


def test_haversine_known_distance() -> None:
    """London to Paris is approximately 188 nm."""
    d = _haversine_nm(-0.1278, 51.5074, 2.3522, 48.8566)
    assert 185 < d < 195


def test_haversine_same_point() -> None:
    d = _haversine_nm(0.0, 0.0, 0.0, 0.0)
    assert d == pytest.approx(0.0, abs=1e-6)


def test_resample_two_points() -> None:
    """Resampling a 2-point line produces equidistant samples."""
    coords = [(0.0, 0.0), (10.0, 0.0)]
    samples = resample_linestring(coords, num_samples=5)

    assert len(samples) == 5
    assert samples[0].distance_nm == pytest.approx(0.0, abs=0.1)
    assert samples[0].lon == pytest.approx(0.0, abs=0.01)
    assert samples[-1].lon == pytest.approx(10.0, abs=0.01)

    # Check equidistant spacing
    total = samples[-1].distance_nm
    step = total / 4
    for i, sp in enumerate(samples):
        assert sp.distance_nm == pytest.approx(i * step, abs=0.5)


def test_resample_three_waypoints() -> None:
    """Multi-segment route is resampled along the full path."""
    coords = [(0.0, 0.0), (5.0, 0.0), (10.0, 0.0)]
    samples = resample_linestring(coords, num_samples=5)

    assert len(samples) == 5
    assert samples[0].lon == pytest.approx(0.0, abs=0.01)
    assert samples[-1].lon == pytest.approx(10.0, abs=0.01)
    # Middle sample should be near 5° (equator, symmetric)
    assert samples[2].lon == pytest.approx(5.0, abs=0.1)


def test_resample_preserves_endpoints() -> None:
    coords = [(-10.0, 30.0), (20.0, 50.0)]
    samples = resample_linestring(coords, num_samples=10)

    assert samples[0].lon == pytest.approx(-10.0, abs=0.01)
    assert samples[0].lat == pytest.approx(30.0, abs=0.01)
    assert samples[-1].lon == pytest.approx(20.0, abs=0.01)
    assert samples[-1].lat == pytest.approx(50.0, abs=0.01)


def test_resample_too_few_points_raises() -> None:
    with pytest.raises(ValueError, match="at least 2"):
        resample_linestring([(0.0, 0.0)], num_samples=5)


def test_resample_too_few_samples_raises() -> None:
    with pytest.raises(ValueError, match="at least 2"):
        resample_linestring([(0.0, 0.0), (1.0, 1.0)], num_samples=1)
