"""Iterative nearest-neighbor dilation for ocean data coastal fill.

GFS wave data has NaN over land, but the ~28km grid spacing means the
data edge is 1-3 cells inland from the actual vector coastline.  This
module fills those NaN cells by iteratively averaging valid neighbors,
extending wave data far enough to overlap with the basemap coastline
polygon so there's no visible gap.

Pure numpy implementation — no scipy dependency required.
"""

from __future__ import annotations

import numpy as np


def coastal_fill(data: np.ndarray, iterations: int = 3) -> np.ndarray:
    """Dilate valid data into NaN cells via nearest-neighbor averaging.

    Each iteration: for every NaN cell with at least one valid neighbor
    in the 3×3 window, replace with the average of valid neighbors.

    Args:
        data: 2D float32 array with NaN for missing values.
        iterations: Number of dilation passes (default 3 ≈ 84km at 28km grid).

    Returns:
        New array with NaN cells near data edges filled.  Original valid
        cells are never modified.
    """
    result = data.copy()

    for _ in range(iterations):
        nan_mask = np.isnan(result)
        if not nan_mask.any():
            break

        # Replace NaN with 0 for neighbor summation, then compute
        # neighbor sums and counts using padded slicing (3×3 window).
        filled = np.where(nan_mask, 0.0, result)
        valid = (~nan_mask).astype(np.float32)

        # Pad with zeros so edge cells still get correct neighbor counts.
        fp = np.pad(filled, 1, mode="constant", constant_values=0.0)
        vp = np.pad(valid, 1, mode="constant", constant_values=0.0)

        # Sum over all 8 neighbors + center (we'll subtract center later
        # if needed, but since center is NaN→0 for the cells we care about,
        # including it is harmless and saves 8 additions vs explicit offsets).
        neighbor_sum = np.zeros_like(result)
        neighbor_cnt = np.zeros_like(result)
        for di in (-1, 0, 1):
            for dj in (-1, 0, 1):
                if di == 0 and dj == 0:
                    continue
                neighbor_sum += fp[1 + di : fp.shape[0] - 1 + di,
                                   1 + dj : fp.shape[1] - 1 + dj]
                neighbor_cnt += vp[1 + di : vp.shape[0] - 1 + di,
                                   1 + dj : vp.shape[1] - 1 + dj]

        # Fill NaN cells that have at least one valid neighbor.
        fill_mask = nan_mask & (neighbor_cnt > 0)
        result[fill_mask] = neighbor_sum[fill_mask] / neighbor_cnt[fill_mask]

    return result
