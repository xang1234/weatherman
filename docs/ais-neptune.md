# Neptune AIS Operations

This project can ingest AIS data through the Neptune bridge while preserving the
existing DuckDB-backed `/ais/*` API contract.

## Backends

- `legacy_parquet`: existing raw-Parquet refresh flow
- `neptune`: Neptune archival bridge into `ais_positions`

`scripts/refresh_ais.py` now reads `AIS_BACKEND`, `AIS_DB_PATH`, and
`AIS_TENANT_ID` from the environment by default.

## Environment

Core archival settings:

- `AIS_BACKEND`: `legacy_parquet` or `neptune`
- `AIS_DB_PATH`: DuckDB path for `ais_positions` / `ais_snapshot`
- `AIS_TENANT_ID`: tenant stamp written during refresh
- `WEATHERMAN_EVENT_JOURNAL_PATH`: shared JSONL journal used to replay
  `ais.refreshed` events across processes
- `NEPTUNE_STORE_ROOT`: canonical Neptune store root
- `NEPTUNE_SOURCES`: comma-separated archival sources, for example `noaa`
- `NEPTUNE_MERGE`: Neptune merge mode, for example `best`
- `NEPTUNE_BBOX`: optional `west,south,east,north`
- `NEPTUNE_MMSI`: optional comma-separated MMSI allowlist
- `NEPTUNE_RAW_POLICY`: Neptune raw retention policy
- `NEPTUNE_OVERWRITE`: force Neptune re-download on archival refresh
- `NEPTUNE_API_KEYS_JSON`: optional JSON map of per-source API keys

Live settings:

- `NEPTUNE_LIVE_SOURCE`: streaming source, usually `aisstream`
- `NEPTUNE_LIVE_LANDING_DIR`: landing zone for live Parquet shards
- `NEPTUNE_LIVE_BBOX`: optional live bbox
- `NEPTUNE_LIVE_MMSI`: optional live MMSI allowlist
- `NEPTUNE_LIVE_MAX_MESSAGES`: bounded batch size for test runs
- `NEPTUNE_LIVE_CLEANUP`: remove landing shards after promotion
- `NEPTUNE_LIVE_FLUSH_INTERVAL`: promotion cadence in seconds
- `NEPTUNE_LIVE_API_KEY`: live source API key

## Archival refresh

Refresh a day from Neptune into the existing DuckDB schema:

```bash
AIS_BACKEND=neptune \
AIS_DB_PATH=.data/ais.duckdb \
WEATHERMAN_EVENT_JOURNAL_PATH=.data/events/sse-events.jsonl \
NEPTUNE_STORE_ROOT=.data/neptune \
uv run python scripts/refresh_ais.py 2026-03-08
```

Legacy refresh remains available:

```bash
uv run python scripts/refresh_ais.py '.data/ais/movement_date=2026-03-08/*' 2026-03-08
```

## Live ingest

Run the Neptune live bridge as a long-lived process:

```bash
AIS_DB_PATH=.data/ais.duckdb \
WEATHERMAN_EVENT_JOURNAL_PATH=.data/events/sse-events.jsonl \
NEPTUNE_STORE_ROOT=.data/neptune \
NEPTUNE_LIVE_LANDING_DIR=.data/neptune-live \
NEPTUNE_LIVE_SOURCE=aisstream \
NEPTUNE_LIVE_API_KEY=... \
uv run python scripts/stream_ais_neptune.py
```

The live bridge promotes landing shards on every flush interval and refreshes
the affected UTC day into DuckDB. `ais.refreshed` is appended to the shared
event journal so API workers can replay it over SSE even when the ingester runs
as a separate process.

## Compose and dev shell

- `docker compose` reads `.env.example` keys through `.env`
- `COMPOSE_PROFILES=ais-live docker compose up` starts the optional
  `ais-neptune-live` service
- `NEPTUNE_LIVE_ENABLE=true ./scripts/dev.sh` starts the live ingester in the
  local dev shell alongside the backend

## Validation checklist

After enabling Neptune:

1. Run one archival refresh for a known day.
2. Compare `/ais/bbox`, `/ais/tracks/{mmsi}`, and tile output against the
   legacy dataset for a sample of vessels.
   Example:

   ```bash
   uv run python scripts/compare_ais_parity.py \
     --legacy-db-path .data/legacy-ais.duckdb \
     --neptune-db-path .data/ais.duckdb \
     --snapshot-date 2026-03-08 \
     --bbox 100,0,110,10 \
     --track 211234567,2026-03-07,2026-03-08 \
     --tile 4,14,9
   ```
3. Check `SELECT MAX("date") FROM ais_snapshot` in the DuckDB file.
4. For live mode, confirm new landing shards appear under
   `NEPTUNE_LIVE_LANDING_DIR` and canonical partitions under `NEPTUNE_STORE_ROOT`.
5. Confirm refresh latency by watching the DuckDB snapshot advance within one
   flush interval.

## Cross-process SSE

Separate Neptune ingesters and API workers must point
`WEATHERMAN_EVENT_JOURNAL_PATH` at the same shared filesystem path. The backend
tails that journal during SSE streaming and replays any new `ais.refreshed`
entries to connected clients. If pods do not share the same journal file, live
DuckDB refreshes still work but frontend auto-refresh will lag until clients
poll `/ais/tiles/latest`.
