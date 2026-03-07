"""Exponential backoff with jitter for ingestion retries.

Backoff formula: min(max_delay, base_delay * 2^attempt) ± jitter
Default: base=1s, max=300s, jitter=±50%.
"""

from __future__ import annotations

import random
from dataclasses import dataclass


@dataclass(frozen=True)
class RetryPolicy:
    """Configuration for retry behavior."""

    max_attempts: int = 5
    base_delay_s: float = 1.0
    max_delay_s: float = 300.0
    jitter_factor: float = 0.5  # ±50%

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError(f"max_attempts must be >= 1, got {self.max_attempts}")
        if self.base_delay_s <= 0:
            raise ValueError(f"base_delay_s must be > 0, got {self.base_delay_s}")
        if self.max_delay_s < self.base_delay_s:
            raise ValueError("max_delay_s must be >= base_delay_s")
        if not 0 <= self.jitter_factor <= 1:
            raise ValueError(f"jitter_factor must be in [0, 1], got {self.jitter_factor}")

    def delay_for_attempt(self, attempt: int) -> float:
        """Calculate the delay in seconds for a given attempt number (0-based).

        Returns the clamped exponential delay with random jitter applied.
        """
        raw = min(self.max_delay_s, self.base_delay_s * (2 ** attempt))
        jitter_range = raw * self.jitter_factor
        return max(0, raw + random.uniform(-jitter_range, jitter_range))

    def should_retry(self, attempt: int) -> bool:
        """Whether another attempt should be made (attempt is 0-based)."""
        return attempt < self.max_attempts - 1
