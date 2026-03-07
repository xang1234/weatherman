"""UI manifest generation for weather model runs.

The manifest is the single file the frontend reads to configure all layer
controls, time sliders, and tile URL construction. It is intentionally
minimal and schema-versioned so frontend and backend can evolve independently.

Schema layout (v1):

    {
      "schema_version": 1,
      "model": "gfs",
      "run_id": "20260306T00Z",
      "cycle_time": "2026-03-06T00:00:00+00:00",
      "published_at": "2026-03-06T01:46:00+00:00",
      "resolution_km": 25.0,
      "layers": [
        {
          "id": "wind_speed",
          "display_name": "Wind Speed",
          "unit": "m/s",
          "palette_name": "viridis",
          "value_range": { "min": 0.0, "max": 50.0 }
        }
      ],
      "forecast_hours": [0, 3, 6, ...],
      "tile_url_template": "/tiles/{model}/{run_id}/{layer}/{forecast_hour}/{z}/{x}/{y}.png"
    }
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

from weatherman.storage.paths import RunID

SCHEMA_VERSION = 1


@dataclass(frozen=True)
class ValueRange:
    """Min/max value range for a layer's colormap scaling."""

    min: float
    max: float


@dataclass(frozen=True)
class LayerConfig:
    """Frontend configuration for a single weather layer."""

    id: str
    display_name: str
    unit: str
    palette_name: str
    value_range: ValueRange


@dataclass(frozen=True)
class UIManifest:
    """Schema-versioned manifest describing a published model run for the frontend.

    Attributes:
        schema_version: Always SCHEMA_VERSION; enables frontend compatibility checks.
        model: Model identifier (e.g. "gfs").
        run_id: Run identifier string (e.g. "20260306T00Z").
        cycle_time: ISO 8601 datetime of the model cycle.
        published_at: ISO 8601 datetime when the run was published (None if unpublished).
        resolution_km: Approximate grid resolution in kilometers.
        layers: Available layer configurations.
        forecast_hours: Sorted list of available forecast hours.
        tile_url_template: URL template with placeholders for tile requests.
    """

    model: str
    run_id: str
    cycle_time: str
    published_at: str | None
    resolution_km: float
    layers: list[LayerConfig]
    forecast_hours: list[int]
    tile_url_template: str
    schema_version: int = SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a JSON-compatible dict."""
        d = asdict(self)
        # Ensure schema_version is first for readability when inspecting JSON
        return {"schema_version": d.pop("schema_version"), **d}

    def to_json(self, indent: int | None = 2) -> str:
        """Serialize to a JSON string."""
        return json.dumps(self.to_dict(), indent=indent)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> UIManifest:
        """Deserialize from a dict (e.g. parsed JSON)."""
        version = data.get("schema_version")
        if version != SCHEMA_VERSION:
            raise ValueError(
                f"Unsupported manifest schema version {version}, "
                f"expected {SCHEMA_VERSION}"
            )
        layers = [
            LayerConfig(
                id=l["id"],
                display_name=l["display_name"],
                unit=l["unit"],
                palette_name=l["palette_name"],
                value_range=ValueRange(**l["value_range"]),
            )
            for l in data["layers"]
        ]
        return cls(
            model=data["model"],
            run_id=data["run_id"],
            cycle_time=data["cycle_time"],
            published_at=data.get("published_at"),
            resolution_km=data["resolution_km"],
            layers=layers,
            forecast_hours=data["forecast_hours"],
            tile_url_template=data["tile_url_template"],
            schema_version=version,
        )

    @classmethod
    def from_json(cls, raw: str) -> UIManifest:
        """Deserialize from a JSON string."""
        return cls.from_dict(json.loads(raw))


@dataclass
class ManifestConfig:
    """Input configuration for building a UIManifest.

    Attributes:
        model: Model identifier.
        run_id: The model run identifier.
        published_at: When the run was published (None if not yet).
        resolution_km: Approximate grid resolution in km.
        layers: Layer definitions for the frontend.
        forecast_hours: Available forecast hours (will be sorted).
        tile_url_template: URL template with placeholders.
    """

    model: str
    run_id: RunID
    published_at: datetime | None
    resolution_km: float
    layers: list[LayerConfig]
    forecast_hours: list[int]
    tile_url_template: str = (
        "/tiles/{model}/{run_id}/{layer}/{forecast_hour}/{z}/{x}/{y}.png"
    )


def build_manifest(config: ManifestConfig) -> UIManifest:
    """Build a UIManifest from the given configuration.

    Args:
        config: Input parameters describing the model run and its layers.

    Returns:
        A UIManifest ready for serialization.
    """
    return UIManifest(
        model=config.model,
        run_id=config.run_id.value,
        cycle_time=config.run_id.as_datetime.isoformat(),
        published_at=config.published_at.isoformat() if config.published_at else None,
        resolution_km=config.resolution_km,
        layers=config.layers,
        forecast_hours=sorted(config.forecast_hours),
        tile_url_template=config.tile_url_template,
    )


def build_manifest_json(config: ManifestConfig) -> str:
    """Build a UIManifest and return it as a JSON string."""
    return build_manifest(config).to_json()
