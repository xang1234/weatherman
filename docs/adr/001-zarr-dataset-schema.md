# ADR-001: Zarr Dataset Schema

**Status:** Accepted
**Date:** 2026-03-07
**Deciders:** David Ten
**Implementation:** `src/weatherman/storage/zarr_schema.py`

## Context

Each GFS model run produces ~40 forecast hours of gridded weather data.
We need a canonical storage format that supports:

1. **Map tile serving** — spatial subset of a single forecast hour
2. **EDR point/trajectory queries** — time-series at a location
3. **Weather routing** — bulk reads of wind/wave fields across a time range
4. **Cloud-native access** — HTTP range requests on S3-compatible stores

## Decision

### Dimensions

| Dimension | Type | Description |
|-----------|------|-------------|
| `time` | int32 | Forecast hour offset from run init (0, 3, 6, ..., 120) |
| `lat` | float32 | Latitude, -90 to 90, north-to-south (descending) |
| `lon` | float32 | Longitude, -180 to 179.75, half-open `[-180, 180)` |

The `level` dimension is omitted for Phase 1 (surface-only fields).
When pressure-level variables are added (Phase 2+), they will use a
separate `level` coordinate or be stored as distinct named variables
(e.g., `wind_850hpa`) to avoid sparse 4-D arrays.

### Coordinate conventions

- **Longitude: `[-180, 180)`** — matches GeoJSON/MapLibre conventions and
  avoids the anti-meridian seam issues that `[0, 360)` causes in web maps.
  GFS native `[0, 360)` is remapped during ingest.
- **Latitude: north-to-south** — matches image/tile coordinate order (row 0
  = north) for simpler COG export and tile rendering.

### Chunking strategy

```
time=1, lat=512, lon=512
```

**Rationale:**

- `time=1`: Tile rendering and EDR queries access individual forecast
  hours. Chunking by 1 avoids pulling unnecessary timesteps.
- `lat=512, lon=512`: For GFS 0.25 deg (1440×721 grid), this produces
  ~6 spatial chunks per timestep. Each chunk is ~1 MB uncompressed
  (512 × 512 × 4 bytes = 1,048,576 bytes). After Blosc/Zstd
  compression, chunks are typically 200-400 KB — well within a single
  HTTP range request round-trip.

**Trade-offs considered:**

| Chunk size | Chunks/timestep | Uncompressed size | Pros | Cons |
|------------|-----------------|-------------------|------|------|
| 128×128 | ~45 | 64 KB | Fine-grained reads | Too many requests for full-field |
| 256×256 | ~12 | 256 KB | Good for small AOI | More requests for global views |
| **512×512** | **~6** | **1 MB** | **Balanced** | **Slightly large for tiny AOI** |
| 1024×1024 | ~2 | 4 MB | Minimal requests | Over-fetch for regional queries |

512×512 was chosen as the starting point. The `ChunkSpec` dataclass
makes it easy to tune per-variable or per-model if benchmarks show a
different optimum.

### Compression

**Codec:** Blosc with Zstd inner compressor, byte-shuffle, level 3.

- Blosc's byte-shuffle reorders float32 bytes so that all MSBs are
  grouped, then all second bytes, etc. Nearby grid cells have similar
  values, so this creates long runs of similar bytes → excellent
  compression.
- Zstd level 3 provides ~3-5× compression on weather floats with
  minimal CPU overhead (~500 MB/s decode on modern hardware).
- Level 3 is the sweet spot: levels 1-2 save little, levels 5+ add
  significant encode time for marginal ratio improvement.

### Data variables (Phase 1)

| Variable | Long name | Units | Level | GRIB key |
|----------|-----------|-------|-------|----------|
| `tmp_2m` | Temperature at 2m | K | 2 m above ground | `:TMP:2 m above ground:` |
| `ugrd_10m` | U-wind at 10m | m/s | 10 m above ground | `:UGRD:10 m above ground:` |
| `vgrd_10m` | V-wind at 10m | m/s | 10 m above ground | `:VGRD:10 m above ground:` |

All variables are float32 with NaN fill value.

### Global attributes

CF-1.8 conventions with source/institution/history metadata for
provenance tracking.

### Estimated storage

Per GFS run (41 forecast hours, 3 variables):
- Uncompressed: 41 × 3 × 1440 × 721 × 4 bytes = ~508 MB
- Compressed (~4× ratio): ~127 MB

## Consequences

- The GRIB2-to-Zarr pipeline reads this schema to create correctly
  shaped, chunked, and compressed stores.
- TiTiler/tile serving reads single `(time=1, lat, lon)` slices —
  aligned with chunk boundaries.
- EDR point queries read a thin `(time=all, lat=1, lon=1)` column —
  crosses all time chunks but each is small.
- Adding new variables requires only a new `VariableDef` entry; no
  schema migration needed.
- If benchmarks show 512×512 is suboptimal, `ChunkSpec` can be tuned
  per-variable without changing the rest of the pipeline.
