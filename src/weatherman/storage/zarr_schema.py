"""Zarr dataset schema definition for the canonical weather store.

Each model run produces a single Zarr store containing all forecast
variables on a regular lat/lon grid.  This module defines:

  - Dimensions and coordinate arrays
  - Per-variable metadata (dtype, units, fill value, vertical level)
  - Chunking strategy tuned for HTTP range-request access patterns
  - Compression codec (Blosc/Zstd with byte-shuffle)

See ADR: docs/adr/001-zarr-dataset-schema.md
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import numpy as np


# ---------------------------------------------------------------------------
# Grid specification
# ---------------------------------------------------------------------------

class GridResolution(Enum):
    """Supported source grid resolutions."""

    GFS_025 = "0.25"  # GFS 0.25-degree global grid

    @property
    def lat_count(self) -> int:
        return _GRID_SPECS[self]["lat_count"]

    @property
    def lon_count(self) -> int:
        return _GRID_SPECS[self]["lon_count"]

    @property
    def step(self) -> float:
        return _GRID_SPECS[self]["step"]


_GRID_SPECS: dict[GridResolution, dict[str, Any]] = {
    GridResolution.GFS_025: {
        "step": 0.25,
        "lat_count": 721,   # -90 to 90 inclusive
        "lon_count": 1440,  # -180 to 179.75 (half-open)
    },
}


def make_lat_array(grid: GridResolution) -> np.ndarray:
    """Generate latitude coordinate array (north-to-south, descending)."""
    spec = _GRID_SPECS[grid]
    return np.linspace(90.0, -90.0, spec["lat_count"], dtype=np.float32)


def make_lon_array(grid: GridResolution) -> np.ndarray:
    """Generate longitude coordinate array in [-180, 180) convention."""
    spec = _GRID_SPECS[grid]
    # arange is semantically correct for half-open intervals with uniform step.
    # Generate in float64 to avoid step-accumulation errors, then downcast.
    return np.arange(-180.0, 180.0, spec["step"], dtype=np.float64).astype(np.float32)


# ---------------------------------------------------------------------------
# Compression
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class CompressionCodec:
    """Blosc codec configuration for Zarr chunks.

    Blosc wraps an inner compressor (zstd) with a byte-shuffle filter
    that regroups bytes across array elements before compression.
    This dramatically improves ratio on float32 weather grids because
    nearby grid cells have similar byte patterns.
    """

    cname: str = "zstd"
    clevel: int = 3          # 1-9; 3 is a good speed/ratio balance
    shuffle: int = 1         # 0=none, 1=byte-shuffle, 2=bit-shuffle
    blocksize: int = 0       # 0 = auto

    def to_numcodecs(self) -> Any:
        """Create a numcodecs Blosc instance."""
        import numcodecs
        return numcodecs.Blosc(
            cname=self.cname,
            clevel=self.clevel,
            shuffle=self.shuffle,
            blocksize=self.blocksize,
        )


DEFAULT_COMPRESSOR = CompressionCodec()


# ---------------------------------------------------------------------------
# Chunking
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ChunkSpec:
    """Chunk sizes for a 3-D (time, lat, lon) variable.

    Design rationale (see ADR 001):
      time=1   — consumers access single forecast hours (tiles, EDR)
      lat/lon=512 — ~6 chunks cover a GFS 0.25 deg grid; each chunk is
                     ~1 MB uncompressed (512*512*4 bytes), yielding
                     sub-second range requests on typical cloud storage.
    """

    time: int = 1
    lat: int = 512
    lon: int = 512

    def as_tuple(self) -> tuple[int, int, int]:
        return (self.time, self.lat, self.lon)


DEFAULT_CHUNKS = ChunkSpec()


# ---------------------------------------------------------------------------
# Variable definitions
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class VariableDef:
    """Metadata for a single forecast variable in the Zarr store."""

    name: str
    long_name: str
    units: str
    grib_key: str                    # Herbie search pattern
    dtype: str = "float32"
    fill_value: float = np.nan
    level: str | None = None         # e.g. "2 m above ground"
    chunks: ChunkSpec = field(default_factory=ChunkSpec)
    compressor: CompressionCodec = field(default_factory=CompressionCodec)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, VariableDef):
            return NotImplemented
        # NaN-safe: math.isnan handles the NaN != NaN problem
        for f in self.__dataclass_fields__:
            a, b = getattr(self, f), getattr(other, f)
            if isinstance(a, float) and isinstance(b, float):
                if math.isnan(a) and math.isnan(b):
                    continue
            if a != b:
                return False
        return True

    def __hash__(self) -> int:
        # Replace NaN with a sentinel for consistent hashing
        fv = 0 if math.isnan(self.fill_value) else self.fill_value
        return hash((self.name, self.long_name, self.units, self.grib_key,
                      self.dtype, fv, self.level, self.chunks, self.compressor))

    @property
    def dims(self) -> tuple[str, ...]:
        return ("time", "lat", "lon")


# Phase 1 variables — surface-level fields for maritime weather display
PHASE1_VARIABLES: dict[str, VariableDef] = {
    "tmp_2m": VariableDef(
        name="tmp_2m",
        long_name="Temperature at 2m above ground",
        units="K",
        grib_key=":TMP:2 m above ground:",
        level="2 m above ground",
    ),
    "ugrd_10m": VariableDef(
        name="ugrd_10m",
        long_name="U-component of wind at 10m above ground",
        units="m/s",
        grib_key=":UGRD:10 m above ground:",
        level="10 m above ground",
    ),
    "vgrd_10m": VariableDef(
        name="vgrd_10m",
        long_name="V-component of wind at 10m above ground",
        units="m/s",
        grib_key=":VGRD:10 m above ground:",
        level="10 m above ground",
    ),
    "apcp_sfc": VariableDef(
        name="apcp_sfc",
        long_name="Total precipitation",
        units="kg/m^2",
        grib_key=":APCP:surface:",
        level="surface",
    ),
    "prmsl": VariableDef(
        name="prmsl",
        long_name="Pressure reduced to mean sea level",
        units="Pa",
        grib_key=":PRMSL:mean sea level:",
        level="mean sea level",
    ),
    "tcdc_atm": VariableDef(
        name="tcdc_atm",
        long_name="Total cloud cover",
        units="%",
        grib_key=":TCDC:entire atmosphere:(?!.*ave)",
        level="entire atmosphere",
    ),
}


# ---------------------------------------------------------------------------
# Dataset schema (ties it all together)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ZarrSchema:
    """Complete schema for one model run's Zarr store.

    Coordinates:
        time (forecast_hour) — int32, hours since run init time
        lat  — float32, [-90, 90] north-to-south
        lon  — float32, [-180, 180) half-open

    Data variables:
        Each variable is (time, lat, lon) float32 with Blosc/Zstd
        compression and the specified chunking.
    """

    grid: GridResolution
    forecast_hours: tuple[int, ...]
    variables: dict[str, VariableDef]
    global_attrs: dict[str, str] = field(default_factory=dict)

    @property
    def lat_array(self) -> np.ndarray:
        return make_lat_array(self.grid)

    @property
    def lon_array(self) -> np.ndarray:
        return make_lon_array(self.grid)

    @property
    def time_array(self) -> np.ndarray:
        return np.array(self.forecast_hours, dtype=np.int32)

    @property
    def shape(self) -> tuple[int, int, int]:
        return (len(self.forecast_hours), self.grid.lat_count, self.grid.lon_count)


# The default schema used by the GRIB2-to-Zarr pipeline
GFS_SCHEMA = ZarrSchema(
    grid=GridResolution.GFS_025,
    forecast_hours=tuple(range(0, 121, 3)),  # 0..120 by 3h = 41 steps
    variables=PHASE1_VARIABLES,
    global_attrs={
        "Conventions": "CF-1.8",
        "source": "NCEP GFS 0.25 degree",
        "institution": "NOAA/NWS/NCEP",
        "history": "Generated by weatherman ingest pipeline",
    },
)
