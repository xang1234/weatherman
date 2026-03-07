"""Storage path construction for the maritime weather platform.

Canonical S3 layout:

    s3://<bucket>/models/<model>/
      runs/<run_id>/                      # published, immutable
        zarr/<run_id>.zarr/               # canonical dataset
        cogs/<layer>/<forecast_hour>.tif  # map-optimized rasters
        vectors/                          # cyclones, zones
        stac/item.json                    # includes provenance
        ui/manifest.json
      staging/<run_id>/                   # temporary until publish
      catalog.json                        # run catalog index (append-only)

Run ID format: YYYYMMDDThhZ (e.g. 20260306T00Z)
  - Lexicographic ordering == chronological ordering
  - Deterministic from model cycle time (no UUIDs)
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import total_ordering

_RUN_ID_RE = re.compile(r"^\d{8}T\d{2}Z$")
_LAYER_RE = re.compile(r"^[a-z][a-z0-9_]+$")


def _validate_layer(layer: str) -> None:
    if not _LAYER_RE.match(layer):
        raise ValueError(
            f"Layer name must be lowercase alphanumeric + underscores, got '{layer}'"
        )


@total_ordering
@dataclass(frozen=True)
class RunID:
    """A validated, immutable model run identifier.

    Format: YYYYMMDDThhZ where hh is the cycle hour (00, 06, 12, 18 for GFS).
    """

    value: str

    def __post_init__(self) -> None:
        if not _RUN_ID_RE.match(self.value):
            raise ValueError(
                f"Invalid run ID '{self.value}'. "
                f"Expected format YYYYMMDDThhZ (e.g. 20260306T00Z)"
            )
        # Validate the date and hour are semantically valid
        try:
            datetime.strptime(self.value, "%Y%m%dT%HZ")
        except ValueError:
            raise ValueError(
                f"Invalid run ID '{self.value}': date or hour is out of range"
            )

    @classmethod
    def from_cycle(cls, date: datetime, cycle_hour: int) -> RunID:
        """Create a RunID from a date and cycle hour."""
        if not 0 <= cycle_hour <= 23:
            raise ValueError(f"Cycle hour must be 0-23, got {cycle_hour}")
        return cls(f"{date.strftime('%Y%m%d')}T{cycle_hour:02d}Z")

    @classmethod
    def from_datetime(cls, dt: datetime) -> RunID:
        """Create a RunID from a datetime (uses the hour as cycle hour)."""
        return cls.from_cycle(dt, dt.hour)

    @property
    def date_str(self) -> str:
        return self.value[:8]

    @property
    def cycle_hour(self) -> int:
        return int(self.value[9:11])

    @property
    def as_datetime(self) -> datetime:
        return datetime.strptime(self.value, "%Y%m%dT%HZ").replace(
            tzinfo=timezone.utc
        )

    def __str__(self) -> str:
        return self.value

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, RunID):
            return NotImplemented
        return self.value < other.value


class StorageLayout:
    """Constructs canonical storage paths for the platform.

    All paths are relative to the bucket root. Callers join with the
    bucket/endpoint prefix appropriate to their storage backend.
    """

    _MODEL_RE = re.compile(r"^[a-z][a-z0-9_]+$")

    def __init__(self, model: str) -> None:
        if not self._MODEL_RE.match(model):
            raise ValueError(
                f"Model name must be lowercase alphanumeric + underscores "
                f"(e.g. 'gfs', 'gfs_wave', 'icon_global'), got '{model}'"
            )
        self._model = model

    @property
    def model(self) -> str:
        return self._model

    # -- Model-level paths --

    @property
    def model_prefix(self) -> str:
        """Root prefix for this model: models/<model>/"""
        return f"models/{self._model}"

    @property
    def catalog_path(self) -> str:
        """Path to the run catalog index: models/<model>/catalog.json"""
        return f"{self.model_prefix}/catalog.json"

    # -- Published run paths (immutable after publish) --

    def run_prefix(self, run_id: RunID) -> str:
        """Published run root: models/<model>/runs/<run_id>/"""
        return f"{self.model_prefix}/runs/{run_id}"

    def zarr_path(self, run_id: RunID) -> str:
        """Canonical Zarr store: models/<model>/runs/<run_id>/zarr/<run_id>.zarr/"""
        return f"{self.run_prefix(run_id)}/zarr/{run_id}.zarr"

    def cog_path(self, run_id: RunID, layer: str, forecast_hour: int) -> str:
        """COG asset: models/<model>/runs/<run_id>/cogs/<layer>/<fhour>.tif"""
        _validate_layer(layer)
        return f"{self.run_prefix(run_id)}/cogs/{layer}/{forecast_hour:03d}.tif"

    def vectors_prefix(self, run_id: RunID) -> str:
        """Vector data directory: models/<model>/runs/<run_id>/vectors/"""
        return f"{self.run_prefix(run_id)}/vectors"

    def stac_item_path(self, run_id: RunID) -> str:
        """STAC item: models/<model>/runs/<run_id>/stac/item.json"""
        return f"{self.run_prefix(run_id)}/stac/item.json"

    def manifest_path(self, run_id: RunID) -> str:
        """UI manifest: models/<model>/runs/<run_id>/ui/manifest.json"""
        return f"{self.run_prefix(run_id)}/ui/manifest.json"

    # -- Staging paths (temporary, never visible to consumers) --

    def staging_prefix(self, run_id: RunID) -> str:
        """Staging root: models/<model>/staging/<run_id>/"""
        return f"{self.model_prefix}/staging/{run_id}"

    def staging_zarr_path(self, run_id: RunID) -> str:
        """Staging Zarr: models/<model>/staging/<run_id>/zarr/<run_id>.zarr/"""
        return f"{self.staging_prefix(run_id)}/zarr/{run_id}.zarr"

    def staging_cog_path(
        self, run_id: RunID, layer: str, forecast_hour: int
    ) -> str:
        """Staging COG: models/<model>/staging/<run_id>/cogs/<layer>/<fhour>.tif"""
        _validate_layer(layer)
        return f"{self.staging_prefix(run_id)}/cogs/{layer}/{forecast_hour:03d}.tif"

    def staging_stac_item_path(self, run_id: RunID) -> str:
        """Staging STAC item."""
        return f"{self.staging_prefix(run_id)}/stac/item.json"

    def staging_manifest_path(self, run_id: RunID) -> str:
        """Staging UI manifest."""
        return f"{self.staging_prefix(run_id)}/ui/manifest.json"
