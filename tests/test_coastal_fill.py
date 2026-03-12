"""Tests for coastal fill (nearest-neighbor NaN dilation)."""

from __future__ import annotations

import numpy as np
import pytest

from weatherman.processing.coastal_fill import coastal_fill


class TestCoastalFill:
    def test_single_iteration_fills_immediate_neighbors(self):
        """One iteration fills NaN cells adjacent to valid data."""
        data = np.full((5, 5), np.nan, dtype=np.float32)
        data[2, 2] = 10.0  # single valid cell in center

        result = coastal_fill(data, iterations=1)

        # Center preserved
        assert result[2, 2] == 10.0
        # 8 immediate neighbors should be filled (all see the center)
        for di in (-1, 0, 1):
            for dj in (-1, 0, 1):
                if di == 0 and dj == 0:
                    continue
                assert result[2 + di, 2 + dj] == pytest.approx(10.0)
        # Corners (2 cells away) still NaN after 1 iteration
        assert np.isnan(result[0, 0])

    def test_multi_iteration_fills_wider_border(self):
        """3 iterations fill a 3-cell-wide NaN border."""
        data = np.full((9, 9), np.nan, dtype=np.float32)
        data[3:6, 3:6] = 5.0  # 3×3 valid block in center

        result = coastal_fill(data, iterations=3)

        # All cells should be filled (3 iterations from a 3×3 block
        # in a 9×9 grid — corners are 3 diagonal steps away).
        assert not np.isnan(result).any()

    def test_all_valid_is_noop(self):
        """All-valid array returns identical values."""
        data = np.arange(20, dtype=np.float32).reshape(4, 5)
        result = coastal_fill(data, iterations=3)
        np.testing.assert_array_equal(result, data)

    def test_all_nan_stays_nan(self):
        """All-NaN array stays all-NaN (no valid neighbors to propagate)."""
        data = np.full((4, 4), np.nan, dtype=np.float32)
        result = coastal_fill(data, iterations=3)
        assert np.isnan(result).all()

    def test_valid_cells_never_modified(self):
        """Original valid values must be exactly preserved."""
        rng = np.random.default_rng(42)
        data = rng.standard_normal((10, 10)).astype(np.float32)
        # Punch some NaN holes
        data[0:3, 0:3] = np.nan
        data[7:10, 7:10] = np.nan
        original_valid = ~np.isnan(data)
        original_values = data[original_valid].copy()

        result = coastal_fill(data, iterations=5)

        np.testing.assert_array_equal(result[original_valid], original_values)

    def test_zero_iterations_returns_copy(self):
        """Zero iterations returns an unmodified copy."""
        data = np.full((3, 3), np.nan, dtype=np.float32)
        data[1, 1] = 7.0
        result = coastal_fill(data, iterations=0)
        assert result[1, 1] == 7.0
        assert np.isnan(result[0, 0])

    def test_fill_averages_multiple_neighbors(self):
        """Fill value is the average of valid neighbors, not just nearest."""
        data = np.full((3, 3), np.nan, dtype=np.float32)
        data[0, 1] = 4.0
        data[1, 0] = 8.0
        # Cell (1,1) has two valid neighbors: 4.0 and 8.0
        result = coastal_fill(data, iterations=1)
        assert result[1, 1] == pytest.approx(6.0)

    def test_returns_new_array(self):
        """Result is a new array — original is not mutated."""
        data = np.full((3, 3), np.nan, dtype=np.float32)
        data[1, 1] = 1.0
        result = coastal_fill(data, iterations=1)
        assert np.isnan(data[0, 0])  # original unchanged
        assert not np.isnan(result[0, 0])  # result filled
