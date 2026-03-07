"""Config-driven layer definition system.

Loads ``layers.yaml`` and provides a single registry that all pipeline stages
reference.  Adding a new weather layer requires only a YAML entry — no code
changes (design principle #5).

The config defines two concepts:

  **Variables** — source data extracted from GRIB2 into the canonical Zarr
  store.  Each variable has a GRIB2 search key, units, and vertical level.

  **Layers** — display layers rendered as COG tiles and shown in the frontend.
  A layer either maps 1:1 to a variable, or is *derived* from multiple source
  variables via a computation method (e.g. wind speed from U/V components).

Usage::

    from weatherman.layers import get_layer_registry

    registry = get_layer_registry()
    var_defs  = registry.variable_defs()       # -> dict[str, VariableDef]
    grib_keys = registry.grib2_search_patterns() # -> dict[str, str]
    layers    = registry.layer_configs()        # -> list[LayerConfig]
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

from weatherman.storage.manifest import LayerConfig, ValueRange
from weatherman.storage.zarr_schema import VariableDef

logger = logging.getLogger(__name__)

_CONFIG_PATH = Path(__file__).parent / "layers.yaml"


# ---------------------------------------------------------------------------
# Config data classes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class VariableConfig:
    """Parsed variable entry from the YAML config."""

    name: str
    long_name: str
    units: str
    grib2_key: str
    level: str | None = None


@dataclass(frozen=True)
class DerivedSpec:
    """Specification for a derived layer computation."""

    method: str
    sources: tuple[str, ...]


@dataclass(frozen=True)
class LayerEntry:
    """Parsed layer entry from the YAML config."""

    id: str
    display_name: str
    units: str
    palette: str
    value_min: float
    value_max: float
    variable: str | None = None
    derived: DerivedSpec | None = None


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


class LayerRegistry:
    """Typed registry built from the layers YAML config.

    Provides accessor methods that produce the data structures expected
    by each pipeline stage, eliminating duplicated definitions.
    """

    def __init__(
        self,
        variables: dict[str, VariableConfig],
        layers: dict[str, LayerEntry],
    ) -> None:
        self._variables = variables
        self._layers = layers

    # -- Zarr schema helpers -------------------------------------------------

    def variable_defs(self) -> dict[str, VariableDef]:
        """Build ``VariableDef`` objects for the Zarr schema."""
        return {
            name: VariableDef(
                name=name,
                long_name=vc.long_name,
                units=vc.units,
                grib_key=vc.grib2_key,
                level=vc.level,
            )
            for name, vc in self._variables.items()
        }

    # -- GRIB2 downloader helpers -------------------------------------------

    def grib2_search_patterns(self) -> dict[str, str]:
        """Build ``{variable_name: herbie_search_pattern}`` for the downloader."""
        return {name: vc.grib2_key for name, vc in self._variables.items()}

    # -- UI manifest helpers ------------------------------------------------

    def layer_configs(self) -> list[LayerConfig]:
        """Build ``LayerConfig`` objects for the UI manifest."""
        return [
            LayerConfig(
                id=entry.id,
                display_name=entry.display_name,
                unit=entry.units,
                palette_name=entry.palette,
                value_range=ValueRange(min=entry.value_min, max=entry.value_max),
            )
            for entry in self._layers.values()
        ]

    # -- COG pipeline helpers -----------------------------------------------

    def source_variable(self, layer_id: str) -> str | None:
        """Return the source variable name for a simple (non-derived) layer."""
        entry = self._layers.get(layer_id)
        if entry is None:
            return None
        return entry.variable

    def derived_layers(self) -> dict[str, DerivedSpec]:
        """Return all derived layer specs, keyed by layer ID."""
        return {
            layer_id: entry.derived
            for layer_id, entry in self._layers.items()
            if entry.derived is not None
        }

    # -- Lookup helpers -----------------------------------------------------

    def get_variable(self, name: str) -> VariableConfig | None:
        return self._variables.get(name)

    def get_layer(self, layer_id: str) -> LayerEntry | None:
        return self._layers.get(layer_id)

    @property
    def variable_names(self) -> list[str]:
        return list(self._variables)

    @property
    def layer_ids(self) -> list[str]:
        return list(self._layers)


# ---------------------------------------------------------------------------
# YAML parsing
# ---------------------------------------------------------------------------


def _parse_variable(name: str, raw: dict[str, Any]) -> VariableConfig:
    return VariableConfig(
        name=name,
        long_name=raw["long_name"],
        units=raw["units"],
        grib2_key=raw["grib2_key"],
        level=raw.get("level"),
    )


def _parse_layer(layer_id: str, raw: dict[str, Any]) -> LayerEntry:
    vrange = raw["value_range"]
    derived = None
    if "derived" in raw:
        d = raw["derived"]
        derived = DerivedSpec(method=d["method"], sources=tuple(d["sources"]))

    return LayerEntry(
        id=layer_id,
        display_name=raw["display_name"],
        units=raw["units"],
        palette=raw["palette"],
        value_min=float(vrange[0]),
        value_max=float(vrange[1]),
        variable=raw.get("variable"),
        derived=derived,
    )


def load_registry(config_path: Path | str) -> LayerRegistry:
    """Load a ``LayerRegistry`` from a YAML config file.

    Validates that:
      - Every simple layer references a defined variable
      - Every derived layer's sources reference defined variables
      - No layer has both ``variable`` and ``derived``

    Raises:
        FileNotFoundError: If the config file does not exist.
        ValueError: If validation fails.
    """
    config_path = Path(config_path)
    with config_path.open() as f:
        raw = yaml.safe_load(f)

    variables: dict[str, VariableConfig] = {}
    for name, v in raw.get("variables", {}).items():
        variables[name] = _parse_variable(name, v)

    layers: dict[str, LayerEntry] = {}
    for layer_id, l_raw in raw.get("layers", {}).items():
        entry = _parse_layer(layer_id, l_raw)

        # Validate: must have exactly one of variable or derived
        if entry.variable and entry.derived:
            raise ValueError(
                f"Layer '{layer_id}' has both 'variable' and 'derived' — pick one"
            )
        if not entry.variable and not entry.derived:
            raise ValueError(
                f"Layer '{layer_id}' must specify either 'variable' or 'derived'"
            )

        # Validate references
        if entry.variable and entry.variable not in variables:
            raise ValueError(
                f"Layer '{layer_id}' references unknown variable '{entry.variable}'"
            )
        if entry.derived:
            for src in entry.derived.sources:
                if src not in variables:
                    raise ValueError(
                        f"Layer '{layer_id}' derived source '{src}' "
                        f"not found in variables"
                    )

        layers[layer_id] = entry

    logger.info(
        "Loaded layer config: %d variables, %d layers from %s",
        len(variables),
        len(layers),
        config_path,
    )

    return LayerRegistry(variables, layers)


@lru_cache(maxsize=1)
def get_layer_registry() -> LayerRegistry:
    """Return the default LayerRegistry (singleton, loaded from ``layers.yaml``)."""
    return load_registry(_CONFIG_PATH)
