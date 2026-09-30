#!/usr/bin/env bash
# Quick local dev startup — no Docker required.
# Starts TiTiler, the backend, and the Vite dev server (hot reload) against
# the data in .data/. If there is no weather data yet, fetches a small sample
# first (latest GFS cycle, a few forecast hours).
#
# Usage:  ./scripts/dev.sh
#
# Optional environment:
#   SAMPLE_HOURS=0,3,6        forecast hours to fetch when seeding (default 0,3,6)
#   WEATHERMAN_DATA_DIR=path  use a data directory other than .data/
#   VITE_BASEMAP_URL=url      basemap PMTiles URL (default: newest Protomaps daily build)
#   NODE_BIN=path             directory holding a Node >= 20 binary

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# ── Pre-flight ───────────────────────────────────────────────────────
DATA_DIR="${WEATHERMAN_DATA_DIR:-$ROOT/.data}"
SAMPLE_HOURS="${SAMPLE_HOURS:-0,3,6}"
NODE_BIN="${NODE_BIN:-/Users/admin/.nvm/versions/node/v22.18.0/bin}"

if [ ! -x frontend/node_modules/.bin/vite ]; then
  echo "ERROR: frontend dependencies are not installed. Run:"
  echo "  (cd frontend && PATH=\"$NODE_BIN:/usr/bin:/bin\" npm install)"
  exit 1
fi

# ── Sample data ──────────────────────────────────────────────────────
# PNG tiles only: Float16 tiles triple the disk use and the frontend does not
# read them unless VITE_USE_FLOAT16_TILES=true.
if [ ! -f "$DATA_DIR/models/gfs/catalog.json" ]; then
  echo "No weather data in $DATA_DIR — fetching a sample from NOAA"
  echo "(latest GFS cycle, hours $SAMPLE_HOURS; about 1 min and 0.6 GB per hour) ..."
  uv run python scripts/run_pipeline.py \
    --data-dir "$DATA_DIR" --hours "$SAMPLE_HOURS" --max-runs 1 --tile-formats png
fi

# ── Basemap ──────────────────────────────────────────────────────────
# Protomaps daily builds need no API key but expire after about a week, so
# pick the newest one that exists rather than pinning a date. This overrides
# VITE_BASEMAP_URL from frontend/.env; set it in your shell to choose another.
if [ -z "${VITE_BASEMAP_URL:-}" ]; then
  for days_ago in 1 2 3 4 5 6; do
    day="$(date -v-"${days_ago}"d +%Y%m%d 2>/dev/null || date -d "$days_ago days ago" +%Y%m%d)"
    if curl -sfI --max-time 5 "https://build.protomaps.com/$day.pmtiles" >/dev/null; then
      # /basemap is proxied to build.protomaps.com by the Vite dev server.
      export VITE_BASEMAP_URL="/basemap/$day.pmtiles"
      break
    fi
  done
fi

# ── Cleanup on exit ──────────────────────────────────────────────────
PIDS=()

cleanup() {
  echo ""
  echo "Shutting down..."
  for pid in "${PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null
  echo "Done."
}

trap cleanup EXIT INT TERM

# ── Environment ──────────────────────────────────────────────────────
export WEATHERMAN_DATA_DIR="$DATA_DIR"
export TITILER_COG_ROOT="$DATA_DIR"
export TITILER_BASE_URL="http://localhost:8080"
export AIS_DB_PATH="${AIS_DB_PATH:-$DATA_DIR/ais.duckdb}"
export AIS_BACKEND="${AIS_BACKEND:-legacy_parquet}"
export AIS_TENANT_ID="${AIS_TENANT_ID:-default}"
export WEATHERMAN_EVENT_JOURNAL_PATH="${WEATHERMAN_EVENT_JOURNAL_PATH:-$DATA_DIR/events/sse-events.jsonl}"
export NEPTUNE_STORE_ROOT="${NEPTUNE_STORE_ROOT:-$DATA_DIR/neptune}"
export NEPTUNE_SOURCES="${NEPTUNE_SOURCES:-noaa}"
export NEPTUNE_MERGE="${NEPTUNE_MERGE:-best}"
export NEPTUNE_RAW_POLICY="${NEPTUNE_RAW_POLICY:-metadata}"
export NEPTUNE_OVERWRITE="${NEPTUNE_OVERWRITE:-false}"
export NEPTUNE_LIVE_SOURCE="${NEPTUNE_LIVE_SOURCE:-aisstream}"
export NEPTUNE_LIVE_LANDING_DIR="${NEPTUNE_LIVE_LANDING_DIR:-$DATA_DIR/neptune-live}"
export NEPTUNE_LIVE_CLEANUP="${NEPTUNE_LIVE_CLEANUP:-false}"
export NEPTUNE_LIVE_FLUSH_INTERVAL="${NEPTUNE_LIVE_FLUSH_INTERVAL:-60}"
export NEPTUNE_LIVE_ENABLE="${NEPTUNE_LIVE_ENABLE:-false}"
export CORS_ORIGINS="http://localhost:5173"
export OTEL_SDK_DISABLED="true"

# ── Start services ───────────────────────────────────────────────────
echo "Starting TiTiler on :8080 ..."
uv run python scripts/run_titiler.py --port 8080 &
PIDS+=($!)

echo "Starting backend on :8000 ..."
uv run python -m weatherman &
PIDS+=($!)

if [ "$NEPTUNE_LIVE_ENABLE" = "true" ]; then
  echo "Starting Neptune live AIS ingest ..."
  uv run python scripts/stream_ais_neptune.py &
  PIDS+=($!)
fi

# ── Wait for services ────────────────────────────────────────────────
wait_for() {
  local name=$1 url=$2
  echo "Waiting for $name ..."
  for i in $(seq 1 30); do
    if curl -sf "$url" >/dev/null 2>&1; then
      echo "$name ready."
      return
    fi
    [ "$i" -eq 30 ] && echo "WARNING: $name did not become ready in 30s."
    sleep 1
  done
}

wait_for "TiTiler" "http://localhost:8080/api"
wait_for "Backend" "http://localhost:8000/health/live"

# Empty VITE_API_BASE_URL overrides frontend/.env so API calls are same-origin
# and go through the Vite proxy — works on whichever port Vite ends up on.
echo "Starting frontend (Vite dev server) ..."
# --host 127.0.0.1 so Vite sees a port already taken on IPv4 and moves on.
(cd frontend && PATH="$NODE_BIN:/usr/bin:/bin" VITE_API_BASE_URL="" exec ./node_modules/.bin/vite --host 127.0.0.1) &
PIDS+=($!)

# ── Ready ────────────────────────────────────────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Frontend:  the \"Local:\" URL Vite prints below (5173 unless taken)"
echo "  Backend:   http://localhost:8000"
echo "  TiTiler:   http://localhost:8080"
echo "  Basemap:   ${VITE_BASEMAP_URL:-from frontend/.env}  (/basemap = build.protomaps.com)"
if [ "$NEPTUNE_LIVE_ENABLE" = "true" ]; then
  echo "  AIS Live:  enabled (shared DuckDB at $AIS_DB_PATH)"
fi
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Press Ctrl+C to stop all services"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

wait
