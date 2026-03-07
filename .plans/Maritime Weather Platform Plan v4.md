# Maritime Weather & Dry Bulk AIS Platform
**Windy.com–style Visualization + Maritime Decision Support — Technical Architecture & Implementation Plan**

**Version:** 4.0  
**Date:** March 2026  
**Classification:** Internal / Engineering

> v4.0 incorporates 12 architecture, product, and process revisions on top of v3.0. Key changes: weather routing optimization engine, run catalog index with retention, client-side tile prefetching, SSE push for run events, formal graceful degradation, DuckDB as AIS store, offline/vessel-side deployment, AIS-derived ETA and port congestion, health/readiness probes, observability from day one, first-class provenance tracking, and multi-tenant workspace isolation.

---

## 0. At-a-glance

| Property | Value |
|---|---|
| Backend | Python (FastAPI), optional **pygeoapi** for standards-heavy endpoints |
| Orchestration | Kubernetes + **Argo Workflows** (DAG pipelines), optional Celery for fine-grained tasks |
| Raster storage | **Zarr** (canonical n-D run dataset) + **COGs** (map-optimized) |
| Tile serving | **TiTiler** (dynamic tiles from COG/Zarr), optional static tile origin for pre-renders |
| Vector serving | OGC API Features and/or Vector Tiles (MVT), with caching/CDN |
| AIS storage | **DuckDB** (embedded, with spatial extension) reading daily Parquet natively |
| Frontend | React + TypeScript + **MapLibre GL JS** + WebGL custom layers + optional **deck.gl** |
| Deployment | On-prem / hybrid / vessel-side lite; S3-compatible object storage (e.g., MinIO) |
| Weather models (start) | GFS + Wave model(s) (e.g., WW3/GFS-Wave); extensible to ICON/ECMWF |
| AI forecast models (optional) | NOAA AIGFS/AIGEFS/HGEFS family |
| AIS | Daily Parquet ingest; **dry bulk only**; snapshot + time-aware queries/tiles |
| Weather routing | Isochrone-based route optimization engine backed by Zarr sampling |
| Key standards | **OGC API – Tiles**, **OGC API – Features**, **OGC API – EDR**, **STAC** |

---

## 1. Revision history (v3 → v4)

v4.0 adds these changes as first-class design elements:

1. **Weather routing optimization engine** — isochrone solver for optimal route computation, not just passive weather-along-route sampling.
2. **Run catalog index + retention policy** — replaces single `latest.json` pointer with append-only run index and automated garbage collection.
3. **Client-side tile prefetching** — Web Worker–based predictive prefetch for timeline animation and pan.
4. **SSE push channel for run events** — server-sent events notify clients of new published runs, eliminating polling.
5. **Formal graceful degradation hierarchy** — explicit fallback paths per subsystem for partial-failure scenarios.
6. **DuckDB as primary AIS store** — commit to DuckDB with spatial extension; drop PostGIS/ClickHouse as AIS options.
7. **Offline/vessel-side deployment mode** — downloadable voyage packages (PMTiles + cached EDR) for ships with limited connectivity.
8. **AIS-derived ETA prediction + port congestion** — computed products from AIS positions and weather forecasts.
9. **Health check and Kubernetes probe strategy** — `/health/live` and `/health/ready` per service with dependency-aware readiness.
10. **Observability from Phase 1** — baseline metrics/logging from day one; AIS moved earlier in phasing.
11. **First-class provenance tracking** — machine-readable lineage on every artifact via STAC processing extension.
12. **Multi-tenant workspace isolation** — tenant-scoped data, routes, alerts, and AIS access from the architecture level.

All 26 upgrades from v3.0 remain in effect. Where v4 changes conflict with v3 text, v4 takes precedence.

---

## 2. Executive summary

We are building a maritime weather visualization and decision-support platform comparable to Windy.com, designed for **enterprise-grade reliability**, **low-latency tile delivery**, and **maritime operations** (weather routing, vessel ETA, port congestion, alerts).

The architecture uses **cloud-native rasters**:

- **Zarr** is the canonical run dataset (fast point/trajectory sampling, derived products, routing engine input).
- **Cloud Optimized GeoTIFF (COG)** is the map-optimized raster artifact.
- **TiTiler** serves tiles dynamically from COG/Zarr with aggressive caching.
- **STAC** catalogs each run, its assets, and full provenance.
- OGC APIs (Tiles/Features/EDR) provide interoperable geospatial access patterns.
- A **weather routing engine** turns forecasts into actionable optimal routes.
- **DuckDB** powers AIS analytics and derived products (ETA, congestion).

Deployable **on-prem**, **hybrid** (CDN + cloud ingest), or **vessel-side lite** (offline voyage packages).

### 2.1 Service-level objectives (SLOs)

| SLO | Target |
|---|---|
| Data freshness | Priority model runs published within 25 min of upstream availability |
| Tile latency | p95 < 250 ms (cache hit), p95 < 900 ms (cache miss) |
| EDR latency | p95 < 1.5 s (`position`), p95 < 4 s (`trajectory`/`corridor`) |
| Routing latency | p95 < 8 s for single-route optimization (global, 14-day horizon) |
| Availability | 99.9% monthly for tile/API endpoints |
| Pipeline success | ≥ 98% successful run publications per month |
| Degraded-mode coverage | Stale data served within 60 s of primary path failure |

Error budget burn triggers incident review and temporary feature freeze for the affected subsystem.

---

## 3. Design principles

1. **Run-centric truth** — everything is organized around an immutable model run (`model + cycle + run_time`). Once published, it never changes.
2. **Atomic publishing** — users never see partial runs. A run transitions staged → validated → published via a single metadata flip.
3. **Cloud-native rasters, not tile explosions** — dynamic tiling + caching, not pre-rendering global pyramids for all timesteps.
4. **Standards where they help** — OGC API – Tiles/Features/EDR and STAC reduce bespoke API surface.
5. **Config-driven layers** — adding a layer = config + extraction rules + optional derived computation.
6. **Observability from day one** — baseline metrics, structured logs, and request tracing from the first deployment.
7. **Idempotent, replay-safe workflows** — every run operation is safe to retry with deterministic outputs.
8. **Graceful degradation over hard failure** — every subsystem has a defined fallback path; stale data beats no data.
9. **Provenance is auditable** — every published artifact carries machine-readable lineage.
10. **Tenant isolation by default** — data models, APIs, and caching support workspace scoping from inception.

---

## 4. High-level architecture

### 4.1 Core services

| Service | Responsibility | Notes |
|---|---|---|
| **Control Plane API** | Run states, publish orchestration, config/policy, tenant management | Stateless; backed by transactional metadata DB |
| **Data Plane Services** | Tiles, EDR, feature serving, routing | Independent autoscaling from control plane |
| **Run Orchestrator** | Executes ingestion → processing → QC → publish workflows | Argo Workflows DAGs; manages run states |
| **Ingestion Service** | Downloads model data from best source; stages raw artifacts | Event-driven triggers + polling fallback; Herbie-assisted |
| **Processing Service** | GRIB2 → canonical Zarr → COGs → derived products | Dask-enabled for parallelism |
| **Catalog Service** | Run catalog index + STAC items/collections + provenance | FastAPI; manages retention + GC |
| **Tile Service** | Dynamic tiles from COG/Zarr | TiTiler; supports OGC Tiles API |
| **Feature Service** | Maritime zones, cyclones, AIS snapshots as features/tiles | OGC API Features and/or MVT endpoints |
| **EDR Service** | Point, area, trajectory/corridor sampling from Zarr | OGC API – EDR patterns |
| **Routing Service** | Isochrone-based weather route optimization | Reads from Zarr; produces optimized waypoint sequences |
| **AIS Service** | Daily Parquet ingest, DuckDB queries, ETA, congestion | Embedded DuckDB with spatial extension |
| **Event Bus** | SSE push for run publications, alert activations, AIS refreshes | FastAPI SSE endpoint |
| **Frontend** | Map + animation + routing UX + vessel context | MapLibre + WebGL + optional deck.gl; prefetch manager |
| **Observability Stack** | Traces/metrics/logs from all services | OpenTelemetry → Prometheus/Grafana |

### 4.2 Data flow (run lifecycle)

```text
[Trigger: SNS / schedule]
        |
        v
[Ingest GRIB2 → staging] ──→ [QC: completeness + sanity]
        |                            |
        v                            v
[Build Zarr canonical run] ──→ [Build COG assets]
        |                            |
        v                            v
[Build derived products]      [Build STAC + provenance + UI manifest]
  (risk indices, routing grids)      |
        |                            |
        +────────→ [ATOMIC PUBLISH] ←──────+
                    (update run catalog index,
                     flip latest pointer)
                          |
                          v
                   [SSE broadcast: new run event]
                          |
                          v
                   [Tiles + APIs + Routing + UI]
```

### 4.3 Control plane / data plane split

The control plane (run orchestration, catalog writes, config, tenant management) and data plane (tile serving, EDR, routing, AIS queries) scale independently. A surge in tile traffic does not affect pipeline publishing; a stuck pipeline does not affect read availability.

Implementation: separate Kubernetes Deployments with independent HPA policies. The metadata DB is shared but accessed through connection pooling with separate pools per plane.

---

## 5. Storage & versioning

### 5.1 Object storage layout (S3-compatible)

```text
s3://<bucket>/models/<model>/
  runs/<run_id>/                      # published, immutable
    zarr/<run_id>.zarr/               # canonical dataset
    cogs/<layer>/<forecast_hour>.tif  # map-optimized rasters
    vectors/                          # cyclones, zones
    stac/item.json                    # includes provenance
    ui/manifest.json
  staging/<run_id>/                   # temporary until publish
  catalog.json                        # run catalog index (append-only)
```

**Run ID format:** `YYYYMMDDThhZ` (e.g., `20260306T00Z`) + optional model variant.

### 5.2 Run catalog index (replaces single latest.json)

Each model has a `catalog.json` that is an append-only index of published runs:

```json
{
  "model": "gfs",
  "schema_version": 1,
  "current_run_id": "20260306T00Z",
  "runs": [
    {
      "run_id": "20260306T00Z",
      "published_at": "2026-03-06T01:23:00Z",
      "status": "published",
      "stac_item": "runs/20260306T00Z/stac/item.json",
      "manifest": "runs/20260306T00Z/ui/manifest.json",
      "processing_version": "v2.3.1"
    },
    {
      "run_id": "20260305T12Z",
      "published_at": "2026-03-05T13:45:00Z",
      "status": "superseded",
      "stac_item": "runs/20260305T12Z/stac/item.json",
      "manifest": "runs/20260305T12Z/ui/manifest.json",
      "processing_version": "v2.3.1"
    }
  ]
}
```

The catalog index is the source of truth for which run is current. The metadata DB backs it transactionally; the JSON file in S3 is a read-optimized projection.

**Retention policy:** configurable per model (default: keep last 7 days / 28 runs). A scheduled Argo CronWorkflow runs garbage collection: marks old runs as `expired`, deletes their S3 prefixes, and compacts the catalog index.

### 5.3 Atomic publish mechanism

- Track run lifecycle in transactional states: `discovered → ingesting → staged → validated → published → superseded → expired`.
- Enforce idempotency key: `model + run_id + processing_version`.
- Acquire per-model publish lock before catalog updates.
- Produce all artifacts in `staging/<run_id>/`.
- Validate (QC gate).
- Publish: move artifacts to `runs/<run_id>/`, append to catalog index, update `current_run_id` — all in one transaction against the metadata DB, then write the S3 catalog projection.
- **Never rewrite published assets.** Immutable caching and trivial rollback.
- On publish success: emit SSE event to connected clients.

### 5.4 Data contracts and schema evolution

- Version JSON schemas for `ui/manifest.json`, catalog index, run registry records, and AIS normalized rows.
- Minor schema versions: backward compatible. Breaking changes: major version bump.
- Validate STAC Items/Collections and OGC payload contracts in CI.
- Emit `schema_version` in manifests and custom API responses.

---

## 6. Weather model sources & ingestion

### 6.1 Primary sources (prefer cloud mirrors)

Use NOAA's cloud dissemination program:
- NOAA Open Data Dissemination (NODD) provides NOAA datasets on commercial clouds.
- AWS Open Data Registry provides NOAA model buckets and **SNS topics for new data notifications**.

### 6.2 Event-driven ingestion

When using AWS-hosted NOAA datasets:
- Subscribe an SQS queue to the dataset's SNS topic (e.g., NewGFSObject).
- Trigger an Argo workflow when a new run is complete (or when required key files arrive).

Fallback: poll official endpoints (e.g., NOMADS) on schedule.

### 6.3 Ingestion abstraction: Herbie

Use **Herbie** inside ingestion jobs to:
- Select best source (cloud mirror vs fallback).
- Handle paths, retries, and consistent model/hour selection.
- Simplify adding models over time.

### 6.4 Models roadmap

| Category | Model | Use |
|---|---|---|
| Baseline global | GFS + wave model (WW3/GFS-Wave) | Free, global |
| Premium global | ECMWF HRES (licensed) | Higher resolution |
| Global alt | ICON (DWD) | Additional comparison |
| AI (optional) | AIGFS / AIGEFS / HGEFS | Optional "AI forecast" layers (labeled, not presented as truth) |

### 6.5 Ingestion reliability controls

- Route all ingestion notifications through a durable queue with DLQ.
- Exponential backoff + jitter retry with bounded attempts.
- Persist failed event payload + failure class for replay and postmortem.
- Provide `replay_ingest_events` tooling for targeted backfills/recovery.

---

## 7. Canonical data model (Zarr) + map assets (COGs)

### 7.1 Why Zarr + COG

- **Zarr**: chunked, compressed n-D arrays for scalable subset access. Used for point/trajectory sampling, derived analytics, and routing engine input.
- **COG**: raster format organized for HTTP range requests; ideal for dynamic tiling.

### 7.2 Zarr dataset design (per run)

Dimensions:
- `time` (forecast valid times or forecast hours)
- `lat`, `lon` (regular grid or standardized target grid)
- `level` (optional for 3D fields)

Chunking (tune with benchmarks):
- Chunk primarily by `time` and spatial blocks (e.g., 256×256 or 512×512 lat/lon)

Geospatial normalization:
- Canonical longitude convention: `[-180, 180)`.
- Split route segments crossing anti-meridian before interpolation/sampling.
- Record source CRS/grid metadata and regridding recipe in provenance.

### 7.3 COG asset strategy

Per layer: export one COG per `(forecast_hour, variable)` or a small set partitioned by time blocks. Create internal overviews for fast low-zoom reads.

### 7.4 Derived products

Computed from Zarr, published as additional COGs/features:
- Wind speed/direction from U/V
- Risk indices (Section 11.2)
- Threshold masks (danger zones)
- Model differences and anomalies
- Uncertainty fields (ensemble spread) when ensembles available
- Routing cost grids (Section 11.1)

---

## 8. Processing pipeline (Argo DAG)

### 8.1 Workflow stages

1. **Acquire** — download/stage GRIB2
2. **Validate raw** — checksums, expected files/hours
3. **Extract variables**
4. **Build Zarr**
5. **QC gate** — completeness + sanity (must pass before publish)
6. **Build COGs**
7. **Build derived products** — risk indices, routing cost grids
8. **Optional pre-render tiles** — low zoom + hot AOIs only
9. **Build STAC item + provenance metadata + UI manifest**
10. **Publish** — atomic catalog update + SSE broadcast

### 8.2 Quality gate

Must pass before publish:
- **Completeness**: all required layers and forecast hours exist.
- **Sanity**: min/max within expected physical bounds; detect all-zeros; nodata masks correct.
- **Geometry**: grid extent, CRS assumptions, lon wrapping.
- **Global edge checks**: anti-meridian continuity, polar clipping, coastline mask alignment.
- **Provenance**: record model run time, source URIs, processing version hash, QC results.

On QC failure: run transitions to `failed` state; previous valid run remains current; alert fires.

### 8.3 Selective pre-rendering policy

Pre-render only:
- Very low zoom levels (fast global previews)
- Hot AOIs (major shipping lanes, port corridors)
- Presentation tiles for specific palettes

Everything else: dynamic via TiTiler.

---

## 9. Tile serving & APIs

### 9.1 Raster tiles (dynamic)

**Primary:** TiTiler serves tiles from COG/Zarr with caching.
- Supports PNG/JPEG and **NumpyTile** for high-fidelity data tiles.
- Supports OGC Tiles API / WMTS patterns.
- Supports MosaicJSON for multi-asset composition.

### 9.2 Standards-first API surface

**OGC API – Tiles:** raster tiles for weather overlays; vector tiles for AIS/zones/cyclones.

**OGC API – Features:** ports/anchorages/berths, cyclone tracks/cones, AIS vessel snapshots.

**OGC API – EDR:**
- `position` — point forecast time-series
- `area` — spatial subset extraction
- `trajectory` — weather along route sampling
- `corridor` — weather near route sampling

**API governance:** prefix custom endpoints with `/v1`, `/v2`. Publish deprecation windows. All endpoints are tenant-scoped (see Section 17).

### 9.3 SSE push channel

Endpoint: `GET /events/stream` (FastAPI SSE, per-tenant filtered).

Events emitted:
- `run.published` — new run ID, model, manifest URL
- `ais.refreshed` — new AIS date available
- `alert.activated` / `alert.expired` — official weather alert changes
- `system.maintenance` — planned maintenance windows

Client subscribes on load; auto-reconnects on disconnect. Eliminates manifest polling.

### 9.4 STAC catalog

Each run is a STAC Item with assets: Zarr store, COGs per layer/time, derived products, vector layers, UI manifest. STAC processing extension carries full provenance (Section 15.3). The frontend consumes a simplified manifest derived from STAC.

---

## 10. Wind/vector field tiles (particle animation)

### 10.1 Compatibility mode (8-bit U/V packed PNG)

Existing approach for broad browser compatibility and fast decode.

### 10.2 High-fidelity modes

Support at least one:
- **NumpyTile** — U/V tiles as arrays (TiTiler-supported) for precise values.
- **16-bit PNG** where feasible.
- **Binary tiles** (Float16/Float32 compressed) via API endpoints.

### 10.3 Time interpolation

Between adjacent timesteps:
- Scalars: linear interpolation.
- Vectors (U/V): linear interpolation in vector space.
- Particles: advect using interpolated vector field.

The client-side prefetch manager (Section 16.2) ensures the next timesteps are already loaded before the interpolation window expires.

---

## 11. Maritime features

### 11.1 Weather routing optimization engine

This is the core differentiator that transforms the platform from visualization into decision support.

**Architecture:**
- A dedicated **Routing Service** (FastAPI) exposes `POST /v1/routes/optimize`.
- Input: departure point + time, arrival point, vessel speed profile (speed vs fuel curve or fixed speed), constraint thresholds (max wave height, max wind speed, max combined sea height).
- Engine: isochrone-based solver. At each time step, expand wavefront of reachable positions; prune positions that violate constraints; select optimal path minimizing transit time (or fuel proxy).
- Data source: reads forecast fields (wind, waves, currents when available) directly from the current published Zarr store via xarray/zarr.
- Output: optimized waypoint sequence with per-leg ETA, weather conditions, risk flags; total transit time and comparison vs great-circle route.

**API contract:**

```json
POST /v1/routes/optimize
{
  "departure": { "lat": 35.0, "lon": 139.0, "time": "2026-03-07T00:00Z" },
  "arrival": { "lat": 37.8, "lon": -122.4 },
  "vessel": {
    "speed_knots": 13.5,
    "max_wave_height_m": 5.0,
    "max_wind_speed_kts": 40
  },
  "model": "gfs",
  "options": { "time_step_hours": 6, "optimization": "min_time" }
}

Response:
{
  "route_id": "uuid",
  "waypoints": [
    { "lat": 35.0, "lon": 139.0, "eta": "2026-03-07T00:00Z", "wind_kts": 12, "wave_m": 1.2, "risk": "low" },
    ...
  ],
  "total_transit_hours": 238,
  "great_circle_hours": 252,
  "savings_hours": 14,
  "model_run": "20260306T00Z",
  "warnings": ["Segment 3-4: wave height within 80% of threshold"]
}
```

**Isochrone solver implementation notes:**
- Grid resolution: match the weather model grid (0.25° for GFS).
- Time step: configurable, default 6 hours (matching GFS forecast hour interval).
- Pruning: discard positions that another position dominates (farther along the route, reached at same or earlier time).
- Anti-meridian: split isochrone expansion at ±180° and reconnect.
- Caching: cache routing cost grids (derived from Zarr) per run as numpy arrays; invalidate on new run publish.

The routing service is stateless (all state in the request + Zarr). Scale horizontally.

### 11.2 Route weather sampling (passive)

Separate from optimization. User-drawn routes are sampled via EDR trajectory/corridor:
- Timeline charts (wind/wave/pressure along track)
- Risk hotspots along route
- Printable summary for ops

### 11.3 Vessel-aware risk indices

Computed from Zarr, published as COG layers + EDR queries:
- Wave height thresholds
- Wave period/steepness proxies
- Comfort indices (heuristic)
- Port approach risk windows

### 11.4 Alerts and warnings

Integrate official alerts (starting with NWS-style APIs):
- Active alerts by region
- Historical alerts (limited window)
- Render as polygons/lines with severity metadata
- Push alert activation/expiration via SSE

Alert coverage varies by region; support multiple providers with clear attribution.

---

## 12. AIS layer (daily Parquet, dry bulk only)

### 12.1 Data constraints

- Input: **Parquet files** ingested **once per day**.
- Coverage: **dry bulk vessels only**.
- This is a daily snapshot/near-real-time layer, not live streaming.

### 12.2 AIS storage: DuckDB with spatial extension

**DuckDB** is the sole AIS analytical store. Rationale: reads Parquet natively (zero ETL), supports spatial queries via `spatial` extension, runs embedded (no separate server), handles columnar analytics efficiently at daily-batch dry-bulk volumes.

Setup:
- DuckDB database file per deployment (or MotherDuck for hosted).
- Install `spatial` extension for ST_Within, ST_DWithin, bbox filtering.
- Load daily Parquet directly: `CREATE TABLE ais_YYYYMMDD AS SELECT * FROM read_parquet('path')`.

Schema (normalized):
```sql
CREATE TABLE ais_positions (
  mmsi BIGINT,
  imo BIGINT,
  vessel_name VARCHAR,
  timestamp TIMESTAMP,
  lat DOUBLE,
  lon DOUBLE,
  sog DOUBLE,       -- speed over ground (knots)
  cog DOUBLE,       -- course over ground (degrees)
  heading DOUBLE,
  destination VARCHAR,
  draught DOUBLE,
  vessel_type_code INT,
  date DATE,         -- partition key
  tenant_id VARCHAR  -- workspace scoping
);
```

### 12.3 AIS ingestion pipeline

1. Daily Parquet arrival → staged in object storage.
2. Normalize schema, filter to dry bulk vessel type codes, assign tenant_id.
3. Load into DuckDB.
4. Produce serving products:
   - **Snapshot table**: latest position per vessel per day (for map display).
   - **Track table**: time-ordered points per vessel (for history).
5. Generate **vector tiles (MVT)** for AIS snapshot (daily). Cache as immutable for that AIS date.
6. Emit SSE `ais.refreshed` event.

### 12.4 AIS-derived products

#### 12.4.1 ETA prediction

For each vessel with a known destination port:
1. Compute great-circle distance from current position to destination port coordinates (reference geometry lookup).
2. Compute base ETA from distance / current SOG.
3. Apply weather adjustment: sample forecast wind and wave along projected route; apply speed reduction factor for adverse conditions (configurable lookup table: wave_height → speed_penalty_pct).
4. Output: `estimated_arrival` timestamp per vessel, with confidence band (weather uncertainty).

Store in DuckDB; expose via `GET /v1/ais/vessels/{mmsi}/eta` and as a field on vessel feature responses.

#### 12.4.2 Port congestion indicators

Define reference anchorage polygons for major dry bulk ports (static config, GeoJSON). For each port/day:
1. Count vessels within anchorage polygon (DuckDB spatial query).
2. Compute median dwell time (vessels present on consecutive days).
3. Classify: `low` / `moderate` / `high` / `severe` based on configurable thresholds.

Expose via `GET /v1/ports/{port_id}/congestion` and as a map overlay (port marker color-coded by congestion level).

### 12.5 AIS serving strategy

**Map load path:** pre-generated MVT vector tiles for daily snapshot. Cached as immutable per AIS date.

**Interactive queries:**
- `/v1/ais/vessels/{mmsi}` — details + recent track + ETA
- `/v1/ais/vessels?bbox=...&date=...` — bbox query
- `/v1/ports/{port_id}/congestion` — congestion indicator

**Tenant scoping:** all AIS queries filter by `tenant_id`. Vector tiles are generated per-tenant if AIS data licensing differs; otherwise shared tiles with tenant-level access check.

### 12.6 Frontend rendering

- deck.gl (IconLayer/ScatterplotLayer) when vessel density is high.
- Click vessel → details panel: MMSI/IMO, SOG/COG, timestamp, destination, ETA (weather-adjusted), track playback.
- Port markers color-coded by congestion level; click for congestion detail.

---

## 13. Multi-model mosaics, blending, and uncertainty

### 13.1 Model mosaics / blending

- Spatial blending: regional high-res near coasts + global offshore.
- Seam handling and weighting strategies (config-driven).
- MosaicJSON + TiTiler virtual mosaics.

### 13.2 Uncertainty visualization

When ensembles available:
- Spread fields (e.g., wind speed spread)
- Probability-of-threshold layers (e.g., P(wave_height > X))
- Ensemble plume in point/route charts

---

## 14. Caching and performance strategy

### 14.1 Versioned URLs + immutable caching

- Published runs have versioned, immutable paths.
- Tiles and immutable assets: long TTL + `Cache-Control: immutable`.
- Catalog index and manifests: ETags for cheap validation (`If-None-Match`).

### 14.2 CDN and edge caching (hybrid)

- CDN in front of tile origin for hybrid/global deployments.
- Versioned URLs are the primary cache invalidation mechanism (no purge workflows).

### 14.3 Hot AOI pre-render + server-side cache warming

- Pre-render low zoom and shipping lane AOIs.
- Server-side cache warmers populate CDN/edge for major operational regions on new run publish.

### 14.4 Client-side tile prefetching

The frontend includes a **prefetch manager** running in a Web Worker:

**Timeline prefetch:** when animation is playing forward, prefetch tiles for the next 2–3 timesteps at the current viewport bounds. When paused and scrubbing, prefetch ±1 timestep from the scrub position.

**Pan prefetch:** on pan gesture, compute pan velocity vector; prefetch tiles in the direction of movement at the current timestep, one tile ring beyond the visible viewport.

**Implementation:**
- Web Worker maintains a priority queue of fetch requests.
- Priority: `current_viewport_current_time` > `current_viewport_next_time` > `adjacent_viewport_current_time`.
- Uses `AbortController` to cancel stale requests when viewport/time changes.
- Respects a configurable max concurrent fetches (default: 6) to avoid saturating the connection.
- Tiles are stored in a bounded in-memory LRU cache (max ~200 MB, configurable).
- The worker communicates with the main thread via `postMessage`; main thread requests tiles from worker cache before falling back to network.

This ensures time interpolation (Section 10.3) always has data ahead of the playback cursor.

---

## 15. Observability, operations & provenance

### 15.1 OpenTelemetry (from Phase 1)

Instrument from day one:
- Ingestion workflow steps (duration, success/fail)
- Processing time per variable/hour
- Tile request latency and cache hit ratio
- EDR query latency and payload size
- Routing computation time
- AIS query latency

Correlate by: `run_id`, `layer`, `forecast_hour`, `request_id`, `tenant_id`.

### 15.2 Monitoring baseline

Prometheus + Grafana dashboards:
- Data freshness (time since latest published run)
- Workflow success rate
- Tile throughput, p95 latency
- Routing p95 latency
- AIS query latency
- Object storage growth, cache storage
- Error budget burn rate

Alerting triggers:
- Pipeline failures
- Stale data (no new run by expected time window)
- Storage > 85% / 90%
- SLO budget burn > 50% in rolling window

### 15.3 Provenance tracking

Every published artifact carries machine-readable lineage via the **STAC processing extension**:

```json
{
  "stac_extensions": ["https://stac-extensions.github.io/processing/v1.2.0/schema.json"],
  "properties": {
    "processing:lineage": "GFS run 20260306T00Z ingested from s3://noaa-gfs-bdp-pds/...",
    "processing:software": { "pipeline": "v2.3.1", "herbie": "2024.12" },
    "processing:level": "L2",
    "custom:source_uris": ["s3://noaa-gfs-bdp-pds/gfs.20260306/00/atmos/gfs.t00z.pgrb2.0p25.f000", "..."],
    "custom:qc_results": { "completeness": "pass", "sanity": "pass", "geometry": "pass" },
    "custom:ingested_at": "2026-03-06T00:48:00Z",
    "custom:published_at": "2026-03-06T01:02:00Z"
  }
}
```

Expose `GET /v1/provenance/{model}/{run_id}` — returns full dependency graph for any run.

This supports audit queries: "what data was the platform showing for location X at time T?"

### 15.4 Health checks and Kubernetes probes

Every service exposes:

**`GET /health/live`** — process is running, not deadlocked. Returns 200 if main event loop responds. Kubernetes liveness probe (check every 10s, fail after 3).

**`GET /health/ready`** — service can serve useful traffic. Readiness conditions per service:

| Service | Ready when |
|---|---|
| Tile Service | TiTiler initialized AND at least one published run reachable in object store |
| EDR Service | Zarr store for current run reachable |
| Routing Service | Zarr store reachable AND routing cost grid loaded |
| AIS Service | DuckDB loaded with at least one day's data |
| Catalog Service | Metadata DB reachable |
| Control Plane | Metadata DB reachable |
| Event Bus (SSE) | Process running (always ready if live) |

Kubernetes readiness probe (check every 5s, fail after 2). Pods not ready are removed from Service endpoints — no traffic routed to uninitialized pods.

**Degraded readiness:** the Tile Service reports ready even if it can only serve pre-rendered/cached tiles (not dynamic). The `X-Degraded: true` response header signals degraded mode to clients.

---

## 16. Graceful degradation hierarchy

Each subsystem has a defined fallback path. The system prefers stale or reduced data over outage.

| Failure | Primary path | Fallback | User signal |
|---|---|---|---|
| TiTiler down/overloaded | Dynamic tiles from COG/Zarr | Serve pre-rendered static tiles (low zoom + hot AOIs) | "Limited zoom levels available" banner |
| Current run Zarr unreachable | EDR reads from current run | Serve cached EDR results from previous requests; degrade routing to last-known cost grid | Stale data timestamp shown |
| Latest run fails QC | Publish new run | Keep previous valid run as current; do not publish failed run | "Data age: X hours" indicator |
| Object store partially unreachable | Full asset access | Serve whatever tiles/layers are cached; hide unavailable layers | Affected layers grayed out in selector |
| AIS DuckDB unreachable | Live DuckDB queries | Serve last-generated MVT tiles (static) | "AIS data as of [date]" label |
| Processing pipeline backed up | Normal pipeline cadence | Continue serving previous run; alert ops | Data age indicator |
| CDN/edge down | CDN-fronted tiles | Client falls back to origin URL | Possible increased latency |

**Implementation:** the Tile Service and EDR Service each maintain a fallback configuration loaded at startup. The `/health/ready` endpoint reports degraded-but-serving states. The frontend checks the `X-Degraded` header and adjusts UI indicators.

---

## 17. Security, access control & multi-tenancy

### 17.1 Authentication

- OIDC/JWT for enterprise deployments.
- API keys for programmatic access, scoped per tenant.

### 17.2 Tenant / workspace isolation

The platform supports multiple tenants (customers, teams) from the architecture level.

**Tenant model:**
- Every API request carries a `tenant_id` (from JWT claim or API key scope).
- Data isolation: AIS data, saved routes, alert configurations, and custom layers are tenant-scoped.
- Weather data (model runs, tiles, EDR) is shared across tenants (it's public forecast data).
- AIS data may be tenant-scoped if licensing restricts vessel coverage per customer.

**Implementation:**
- Metadata DB: `tenant_id` column on tenant-scoped tables (routes, alerts_config, ais_access_grants).
- AIS DuckDB: `tenant_id` filter on all queries.
- AIS vector tiles: if tenants share the same AIS data, generate shared tiles with access check at the tile endpoint. If tenants have different AIS datasets, generate separate tile sets per tenant.
- API middleware: extract `tenant_id` from auth token, inject into all query paths.
- Caching: tenant-scoped data includes `tenant_id` in cache key. Shared weather tiles do not.

### 17.3 Role-based access control

Roles are per-tenant:
- **viewer** — read tiles, EDR, AIS, routes
- **operator** — all viewer permissions + create/edit routes, configure alerts
- **admin** — all operator permissions + manage tenant users, configure AIS access, manage retention policy

### 17.4 Rate limiting and audit

- Rate limiting at API gateway, per tenant + per user.
- Audit logging for: admin operations, route optimizations, data exports, AIS detail queries.

---

## 18. Frontend application (MapLibre + WebGL)

### 18.1 Rendering stack

- MapLibre GL JS for base map + layer composition.
- Custom WebGL layer(s) for wind particles: GPU advection, trail fading via ping-pong framebuffers.
- Raster overlays via raster sources (dynamic tiles).
- Vector overlays via MVT or GeoJSON (zones/cyclones/alerts/AIS).
- Optional deck.gl for heavy vessel rendering.
- **Prefetch manager** (Web Worker) as described in Section 14.4.

### 18.2 UX features

- Layer selector (weather + maritime + AIS-derived)
- Timeline with play/pause/speed + smooth interpolation
- Model selector + multi-model comparison view
- Point forecast panel (click map → time-series chart)
- **Route planner**: draw route manually OR request optimized route from routing service; view weather along track, risk hotspots, ETA
- Alert toggles and legend
- Vessel filter (dry bulk subtypes) + vessel details panel (SOG, COG, destination, weather-adjusted ETA, track playback)
- **Port congestion overlay**: color-coded port markers with click-for-detail
- **Data age indicator**: always visible, shows time since latest published run; amber/red when stale
- **Degraded mode banners**: shown when `X-Degraded` header detected

### 18.3 Static basemap + overlays via PMTiles

Serve basemap and static vector overlays as PMTiles. Frontend reads PMTiles directly via MapLibre protocol integration. Also used for offline/vessel-side packages.

---

## 19. Offline / vessel-side deployment

### 19.1 Concept

Ships with limited/expensive satellite bandwidth cannot rely on full platform access. The platform generates **voyage packages** that a vessel downloads once (or receives via compressed satellite update) and runs locally.

### 19.2 Voyage package contents

A voyage package is a self-contained bundle for a specific route corridor and time window:

```text
voyage-package-<route_id>/
  tiles/                          # PMTiles file covering route corridor ± buffer
    weather.pmtiles                # raster tiles for relevant layers
    basemap.pmtiles                # basemap for corridor region
  route/
    optimized_route.json           # pre-computed routing result
    trajectory_forecast.json       # EDR trajectory sample along route
    corridor_forecast.json         # EDR corridor sample (risk bands)
  ais/
    snapshot.geojson               # AIS snapshot for corridor region
    port_congestion.json           # congestion data for destination + waypoint ports
  manifest.json                    # metadata, model run info, generation time, expiry
```

Total size target: < 50 MB for a transoceanic voyage (achievable with corridor-scoped tiles at limited zoom range).

### 19.3 Generation

API endpoint: `POST /v1/voyage-packages/generate`
- Input: route (waypoints or route_id from optimizer), buffer radius (nm), zoom range, layer selection.
- Server-side pipeline: extract corridor-scoped tiles from COGs → write PMTiles; extract EDR samples; snapshot AIS for corridor; package as tar.gz or zip.
- Return download URL (time-limited signed URL).

### 19.4 Vessel-side viewer

A minimal offline viewer (static HTML + MapLibre + JS) that reads the voyage package from local filesystem:
- PMTiles served via MapLibre PMTiles protocol (no server needed).
- Route and forecast data rendered from static JSON.
- No internet required after initial download.

This can be packaged as an Electron app or served from a lightweight local HTTP server (Python `http.server` level).

---

## 20. Deployment patterns

### 20.1 Single-node (dev/staging)

- Docker Compose
- Local filesystem storage or MinIO
- DuckDB embedded (default)
- All services in one host

### 20.2 Production (on-prem or hybrid)

- Kubernetes
- Argo Workflows
- S3-compatible object storage
- DuckDB embedded per AIS service pod (or MotherDuck for shared access)
- Horizontal scaling for: tile service, API/EDR service, routing service, processing workers (CPU-bound)
- SSE endpoint behind load balancer with sticky sessions or replicated fan-out

### 20.3 Vessel-side lite

- Static file server (nginx or Python) serving a voyage package
- No Kubernetes, no object store, no pipeline
- Standalone offline viewer application

---

## 21. Phased implementation plan

### Phase 1 — Foundations + observability baseline (Weeks 1–4)

**Goal:** display one weather layer via TiTiler from COGs, with run catalog, atomic publish, and baseline observability from day one.

Deliverables:
- Object storage layout + run catalog index (with retention stub)
- Ingest GFS (polling fallback + cloud mirror option)
- Build COGs for 1–2 layers (e.g., temperature, wind speed)
- TiTiler serving XYZ and/or OGC Tiles
- Minimal STAC item with provenance fields + UI manifest
- Frontend: MapLibre base map + raster overlay + legend + data age indicator
- **OpenTelemetry instrumentation**: structured logging, request latency histograms, pipeline duration metrics
- **Prometheus + Grafana**: basic dashboard (tile latency, pipeline status, data freshness)
- **Health endpoints**: `/health/live` and `/health/ready` on all services
- Tenant-id middleware (single tenant initially, but plumbed through)

Acceptance:
- New run appears only after publish flip
- Tiles load smoothly at multiple zooms
- Grafana dashboard shows tile latency and pipeline status
- Health probes functional in Kubernetes

---

### Phase 2 — Canonical Zarr + EDR + AIS ingest (Weeks 5–8)

**Goal:** Zarr-backed point forecasts, multi-layer support, and initial AIS data visible on map.

Deliverables:
- Build canonical Zarr per run
- EDR `position` endpoint backed by Zarr
- More layers (precip, pressure, cloud, waves)
- Caching headers (immutable tiles, ETag manifests)
- Initial QC checks
- **AIS daily Parquet ingest into DuckDB** (normalize, filter dry bulk)
- **AIS snapshot MVT generation** (daily)
- **Frontend: vessel dots on map** (basic click → MMSI, position, timestamp)
- SSE endpoint (emit `run.published` and `ais.refreshed` events)
- Frontend subscribes to SSE for auto-refresh

Acceptance:
- Click-on-map shows point time-series quickly
- No partial runs visible
- Dry bulk vessel dots appear on map from daily Parquet
- New runs auto-appear in UI without page refresh

---

### Phase 3 — Wind particles + time interpolation + AIS details (Weeks 9–12)

**Goal:** Windy-style animation with smooth interpolation; AIS track playback and vessel details.

Deliverables:
- Wind U/V tile path: 8-bit compatibility + one high-fidelity option (NumpyTile or binary)
- GPU particle engine
- Time interpolation (scalar + vector)
- **Client-side prefetch manager** (Web Worker) for timeline + pan prefetch
- Performance tuning (WebWorkers, adaptive particle count)
- **AIS vessel detail panel**: track playback, SOG/COG, destination
- **AIS bbox query endpoint**

Acceptance:
- Smooth animation at 60fps on mid-range laptop
- Transition between timesteps without jumps
- Prefetch eliminates stutter during normal-speed playback
- Vessel click shows track + metadata

---

### Phase 4 — Production hardening + routing engine (Weeks 13–18)

**Goal:** operational reliability, full observability, weather routing.

Deliverables:
- Argo DAG workflows for end-to-end runs
- QC gate that blocks publishing on failures
- Full OpenTelemetry distributed tracing (ingest → tile → UI)
- Alerting rules (SLO burn rate, stale data, pipeline failures)
- Auth (OIDC/JWT), rate limits, audit logging
- Backfill/reprocess tooling
- Run catalog retention + garbage collection CronWorkflow
- **Graceful degradation paths** implemented per Section 16
- **Weather routing service**: isochrone solver, `POST /v1/routes/optimize`
- **Frontend route planner**: draw or optimize route, view weather along track

Acceptance:
- Pipeline failures visible and actionable
- Rollbacks are a metadata flip
- Degraded mode works (kill TiTiler → pre-rendered tiles served, banner shown)
- Route optimization returns result < 8s p95
- User can plan route, see weather along track, identify risk windows

---

### Phase 5 — AIS derived products + maritime decision tools (Weeks 19–24)

**Goal:** AIS-derived intelligence and full maritime decision support.

Deliverables:
- **ETA prediction** (weather-adjusted) per vessel
- **Port congestion indicators** (anchorage polygon counts + dwell time)
- Risk indices + threshold alert layers
- Official alerts ingestion + visualization + SSE push
- Route weather sampling (passive EDR trajectory/corridor) + printable summary
- Frontend: port congestion overlay, ETA in vessel panel, alert toggles
- Multi-tenant workspace isolation (if >1 tenant expected)

Acceptance:
- Vessel details show weather-adjusted ETA
- Port markers show congestion level; click shows detail
- Alerts render as polygons with severity
- Different tenants see only their scoped data

---

### Phase 6 — Advanced modeling + offline mode (Weeks 25–32)

**Goal:** multi-model support, uncertainty, and vessel-side deployment.

Deliverables:
- Multi-model blending and/or mosaics (MosaicJSON)
- Ensemble uncertainty visualization (spread, probability-of-threshold, plumes)
- Optional AI forecast model layers
- **Voyage package generation** (`POST /v1/voyage-packages/generate`)
- **Offline vessel-side viewer** (static HTML + MapLibre + PMTiles)
- Schema versioning enforcement in CI
- Full provenance query endpoint

Acceptance:
- Multi-model comparisons understandable in UI
- Voyage package < 50 MB for transoceanic route
- Offline viewer works with no network
- Provenance endpoint returns full audit trail for any run

---

## 22. Risk register

| Risk | Impact | Likelihood | Mitigation |
|---|---|---|---|
| Tile explosion if pre-rendering too much | High | Medium | Dynamic tiling first; pre-render only low zoom/AOI |
| Partial run visibility | High | Medium | Atomic publish gating + immutable assets |
| Data corruption / silent bad ingest | High | Medium | QC gate: completeness + sanity + provenance |
| Object-store small-file pressure (Zarr) | Medium | Medium | Tune chunking; consolidated metadata; fewer/larger chunks |
| AIS daily cadence limits real-time expectations | Medium | High | UI clearly labels AIS timestamp; ETA adds predictive value |
| Multi-model blending seams / bias | Medium | Medium | Configurable blending zones; QA visual tests |
| Browser performance variance | Medium | Medium | Adaptive particle counts; fallback scalar-only mode |
| Routing solver performance at scale | Medium | Medium | Cache routing cost grids; bound isochrone expansion; timeout + degrade |
| DuckDB single-writer limitation | Low | Medium | AIS writes are daily batch (one writer); reads are concurrent (safe) |
| Voyage package staleness | Medium | Medium | Include expiry in manifest; viewer shows warning if expired |
| Tenant data leakage | High | Low | tenant_id filter in middleware; integration tests verify isolation |
| S3 eventual consistency on catalog writes | Medium | Low | Metadata DB is source of truth; S3 catalog is read projection with ETag |

---

## 23. References

- OGC API – Tiles: https://ogcapi.ogc.org/tiles/
- OGC API – Features: https://www.ogc.org/standards/ogcapi-features/
- OGC API – EDR: https://docs.ogc.org/is/19-086r6/19-086r6.html
- STAC spec: https://stacspec.org/en/
- STAC processing extension: https://github.com/stac-extensions/processing
- TiTiler: https://developmentseed.org/titiler/
- Cloud Optimized GeoTIFF: https://cogeo.org/in-depth.html
- Zarr / xarray: https://tutorial.xarray.dev/intermediate/intro-to-zarr.html
- NOAA NODD: https://www.noaa.gov/information-technology/open-data-dissemination
- NOAA GFS AWS + SNS: https://github.com/awslabs/open-data-registry/blob/main/datasets/noaa-gfs-bdp-pds.yaml
- NOAA EAGLE / GraphCastGFS: https://registry.opendata.aws/noaa-nws-graphcastgfs-pds/
- Argo Workflows: https://argoproj.github.io/workflows/
- OpenTelemetry: https://opentelemetry.io/docs/
- HTTP immutable caching: https://datatracker.ietf.org/doc/html/rfc8246
- PMTiles: https://github.com/protomaps/PMTiles
- MapLibre PMTiles: https://maplibre.org/maplibre-gl-js/docs/examples/pmtiles-source-and-protocol/
- NWS API (alerts): https://www.weather.gov/documentation/services-web-api
- DuckDB spatial extension: https://duckdb.org/docs/extensions/spatial.html
- Isochrone weather routing (background): Hagiwara (1989), "Weather Routing of Sail-Assisted Motor Vessels"

---

*End of document.*
