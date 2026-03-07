"""GRIB2 to Zarr conversion pipeline.

Reads downloaded GRIB2 files (one per variable/forecast-hour), extracts
the data values, normalizes the longitude convention from [0, 360) to
[-180, 180), and writes into a pre-allocated Zarr store shaped by the
ZarrSchema.

Pipeline steps:
    1. init_zarr_store()   — create empty Zarr with coordinates + data arrays
    2. ingest_grib2_file() — read one GRIB2, normalize, write into the store
    3. finalize_store()    — consolidate metadata for fast remote open

Usage:
    store_path = init_zarr_store(schema, "/tmp/staging/20260306T00Z.zarr")
    for var, fhour, grib_path in downloaded_files:
        ingest_grib2_file(store_path, schema, var, fhour, grib_path)
    finalize_store(store_path)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import xarray as xr
import zarr
from zarr.codecs import BloscCodec, BytesCodec

from weatherman.processing.geo import (
    GridProvenance,
    normalize_grid,
    validate_geographic_crs,
)
from weatherman.storage.zarr_schema import VariableDef, ZarrSchema

logger = logging.getLogger(__name__)


def _blosc_codec(var_def: VariableDef) -> BloscCodec:
    """Build a Zarr v3 BloscCodec from schema compression settings."""
    c = var_def.compressor
    shuffle_map = {0: "noshuffle", 1: "shuffle", 2: "bitshuffle"}
    return BloscCodec(
        cname=c.cname,
        clevel=c.clevel,
        shuffle=shuffle_map[c.shuffle],
    )


# ---------------------------------------------------------------------------
# Store initialization
# ---------------------------------------------------------------------------

def init_zarr_store(schema: ZarrSchema, store_path: str | Path) -> Path:
    """Create an empty Zarr store with coordinate arrays and data variables.

    The store is pre-allocated: data variable arrays are created at full
    shape with NaN fill, so individual forecast hours can be written
    independently and in any order.

    Args:
        schema: The dataset schema defining dimensions, variables, and codecs.
        store_path: Local filesystem path for the Zarr store.

    Returns:
        The resolved store path.
    """
    store_path = Path(store_path)
    store_path.parent.mkdir(parents=True, exist_ok=True)

    root = zarr.open_group(str(store_path), mode="w")

    # Write global attributes
    root.attrs.update(schema.global_attrs)

    # -- Coordinate arrays (small, written once, no chunking needed) --
    # Zarr v3 forbids data + dtype together; data's dtype is inferred.
    root.create_array(
        "time",
        data=schema.time_array,
        dimension_names=("time",),
        attributes={
            "long_name": "forecast hour",
            "units": "hours since model init time",
        },
    )

    root.create_array(
        "lat",
        data=schema.lat_array,
        dimension_names=("lat",),
        attributes={
            "long_name": "latitude",
            "units": "degrees_north",
            "standard_name": "latitude",
        },
    )

    root.create_array(
        "lon",
        data=schema.lon_array,
        dimension_names=("lon",),
        attributes={
            "long_name": "longitude",
            "units": "degrees_east",
            "standard_name": "longitude",
        },
    )

    # -- Data variables (pre-allocated, filled incrementally) --
    n_time, n_lat, n_lon = schema.shape

    for var_def in schema.variables.values():
        root.create_array(
            var_def.name,
            shape=(n_time, n_lat, n_lon),
            chunks=var_def.chunks.as_tuple(),
            dtype=var_def.dtype,
            fill_value=float("nan"),
            compressors=_blosc_codec(var_def),
            dimension_names=var_def.dims,
            attributes={
                "long_name": var_def.long_name,
                "units": var_def.units,
                **({"level": var_def.level} if var_def.level else {}),
            },
        )

    logger.info(
        "Initialized Zarr store at %s: %d variables, shape %s",
        store_path,
        len(schema.variables),
        schema.shape,
    )

    return store_path


# ---------------------------------------------------------------------------
# GRIB2 ingestion (one file at a time)
# ---------------------------------------------------------------------------

def ingest_grib2_file(
    store_path: str | Path,
    schema: ZarrSchema,
    variable_name: str,
    forecast_hour: int,
    grib2_path: str | Path,
) -> None:
    """Read one GRIB2 file and write its data into the Zarr store.

    Opens the GRIB2 via cfgrib, extracts the data values, normalizes
    the longitude convention, and writes into the pre-allocated array
    at the correct time index.

    Note: This function is NOT safe for concurrent calls on the same
    store path.  Zarr directory stores have no built-in locking, so
    parallel writes must be coordinated externally.

    Args:
        store_path: Path to the initialized Zarr store.
        schema: Dataset schema (used for time index lookup and validation).
        variable_name: Schema variable name (e.g. "tmp_2m").
        forecast_hour: Forecast hour this file represents.
        grib2_path: Path to the GRIB2 file.

    Raises:
        ValueError: If the variable or forecast hour is not in the schema.
        FileNotFoundError: If the GRIB2 file doesn't exist.
    """
    grib2_path = Path(grib2_path)
    if not grib2_path.exists():
        raise FileNotFoundError(f"GRIB2 file not found: {grib2_path}")

    if variable_name not in schema.variables:
        raise ValueError(
            f"Variable '{variable_name}' not in schema. "
            f"Available: {list(schema.variables.keys())}"
        )

    if forecast_hour not in schema.forecast_hours:
        raise ValueError(
            f"Forecast hour {forecast_hour} not in schema. "
            f"Available: {list(schema.forecast_hours)}"
        )

    time_idx = schema.forecast_hours.index(forecast_hour)

    # Read GRIB2 via cfgrib (xarray handles eccodes/cfgrib backend)
    with xr.open_dataset(str(grib2_path), engine="cfgrib") as ds:
        # cfgrib names the data variable by its GRIB shortName (e.g. "t2m", "10u")
        # — we just need the first (and usually only) data variable.
        data_vars = list(ds.data_vars)
        if not data_vars:
            raise ValueError(f"No data variables found in {grib2_path}")

        grib_var = data_vars[0]
        data = ds[grib_var].values.astype(np.float32)

        # Extract source coordinates
        lon_dim = "longitude" if "longitude" in ds.coords else "lon"
        lat_dim = "latitude" if "latitude" in ds.coords else "lat"
        src_lon = ds.coords[lon_dim].values
        src_lat = ds.coords[lat_dim].values

        # Extract CRS info for provenance
        crs_wkt = ds.attrs.get("GRIB_gridType", None)
        # cfgrib doesn't expose EPSG directly; GFS is always geographic
        source_crs = validate_geographic_crs(
            crs_wkt=crs_wkt if isinstance(crs_wkt, str) else None,
            epsg=None,
        )

    # Normalize longitude and latitude conventions, capture provenance
    result = normalize_grid(
        data, src_lat, src_lon,
        source_crs=source_crs,
        grid_resolution=schema.grid.step,
    )
    data = result.data

    # Validate shape against schema
    expected_lat = schema.grid.lat_count
    expected_lon = schema.grid.lon_count
    if data.shape != (expected_lat, expected_lon):
        raise ValueError(
            f"Shape mismatch for {variable_name} fxx={forecast_hour}: "
            f"got {data.shape}, expected ({expected_lat}, {expected_lon})"
        )

    # Write into pre-allocated Zarr array and record provenance
    root = zarr.open_group(str(store_path), mode="r+")
    root[variable_name][time_idx, :, :] = data

    # Write provenance on first ingest (all variables share the same grid)
    if "provenance:source_crs" not in root.attrs:
        root.attrs.update(result.provenance.to_attrs())

    logger.debug(
        "Ingested %s fxx=%03d into %s [time_idx=%d]",
        variable_name,
        forecast_hour,
        store_path,
        time_idx,
    )


# ---------------------------------------------------------------------------
# Finalization
# ---------------------------------------------------------------------------

def finalize_store(store_path: str | Path) -> None:
    """Consolidate Zarr metadata for fast remote open.

    After all forecast hours have been written, consolidate the metadata
    so that opening the store over HTTP requires only a single request
    for the metadata (instead of one per array).
    """
    zarr.consolidate_metadata(str(store_path))
    logger.info("Consolidated metadata for %s", store_path)


# ---------------------------------------------------------------------------
# Full pipeline orchestration
# ---------------------------------------------------------------------------

@dataclass
class ConversionResult:
    """Result of a full GRIB2 → Zarr conversion."""

    store_path: Path
    variables_written: int = 0
    hours_written: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return len(self.errors) == 0


def convert_grib2_to_zarr(
    schema: ZarrSchema,
    grib2_dir: Path,
    store_path: str | Path,
) -> ConversionResult:
    """Convert a directory of downloaded GRIB2 files into a Zarr store.

    Expects the GRIB2 directory layout produced by the GFS downloader:
        <grib2_dir>/grib2/<variable_name>/f<NNN>.grib2

    Steps:
        1. Initialize an empty Zarr store from the schema
        2. Ingest each GRIB2 file into the store
        3. Consolidate metadata

    Partial failures are captured in the result rather than aborting.

    Args:
        schema: The Zarr dataset schema.
        grib2_dir: Run-level staging directory (e.g. staging/<run_id>/).
            This is the directory passed as ``staging_dir`` to
            ``download_variable`` — NOT the parent staging root.
        store_path: Output path for the Zarr store.

    Returns:
        ConversionResult with counts and any errors.
    """
    store_path = Path(store_path)
    result = ConversionResult(store_path=store_path)

    # Step 1: Initialize store
    init_zarr_store(schema, store_path)

    # Step 2: Ingest each variable/forecast-hour pair
    hours_seen: set[int] = set()

    for var_name, var_def in schema.variables.items():
        for fhour in schema.forecast_hours:
            grib_file = grib2_dir / "grib2" / var_name / f"f{fhour:03d}.grib2"
            if not grib_file.exists():
                msg = f"Missing GRIB2: {var_name} fxx={fhour:03d} ({grib_file})"
                logger.warning(msg)
                result.errors.append(msg)
                continue

            try:
                ingest_grib2_file(store_path, schema, var_name, fhour, grib_file)
                result.variables_written += 1
                hours_seen.add(fhour)
            except Exception as exc:
                msg = f"{var_name} fxx={fhour:03d}: {exc}"
                logger.warning("Ingest failed: %s", msg)
                result.errors.append(msg)

    result.hours_written = len(hours_seen)

    # Step 3: Finalize
    finalize_store(store_path)

    logger.info(
        "GRIB2→Zarr conversion complete: %d vars written across %d hours, "
        "%d errors, store=%s",
        result.variables_written,
        result.hours_written,
        len(result.errors),
        store_path,
    )

    return result
