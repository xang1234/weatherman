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
#   VITE_BASEMAP_URL=url      basemap PMTiles URL (default: the extract below)
#   NODE_BIN=path             directory holding the node binary to run Vite with
#                             (default: the first Node on PATH, else nvm, that
#                             Vite supports: ^20.19 or >= 22.12)

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# ── Pre-flight ───────────────────────────────────────────────────────
DATA_DIR="${WEATHERMAN_DATA_DIR:-$ROOT/.data}"
SAMPLE_HOURS="${SAMPLE_HOURS:-0,3,6}"
# Vite 7 needs Node ^20.19 or >= 22.12. The node on PATH may be older (some
# machines ship v14), so fall back to an nvm install (#73).
node_ok() {
  [ -x "$1/node" ] && "$1/node" -e '
    const [major, minor] = process.versions.node.split(".").map(Number)
    process.exit(major > 22 || (major === 22 && minor >= 12) || (major === 20 && minor >= 19) ? 0 : 1)
  ' 2>/dev/null
}
if [ -z "${NODE_BIN:-}" ]; then
  path_node="$(command -v node 2>/dev/null || true)"
  for candidate in ${path_node:+"$(dirname "$path_node")"} "$HOME"/.nvm/versions/node/*/bin; do
    if node_ok "$candidate"; then NODE_BIN="$candidate"; break; fi
  done
fi
if [ -z "${NODE_BIN:-}" ] || ! node_ok "$NODE_BIN"; then
  echo "ERROR: Vite needs Node ^20.19 or >= 22.12, and none was found on PATH or"
  echo "in ~/.nvm. Install one, or set NODE_BIN to the directory holding it."
  exit 1
fi

if [ ! -x frontend/node_modules/.bin/vite ]; then
  echo "ERROR: frontend dependencies are not installed. Run:"
  echo "  (cd frontend && PATH=\"$NODE_BIN:\$PATH\" npm install)"
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
# A low-zoom extract of the Protomaps planet, served by the backend at
# /basemap (#70). Protomaps' own builds can't be read from most origins (CORS)
# and get pruned, so the app keeps its own copy. Fetched once, about 190 MB.
BASEMAP="$DATA_DIR/basemap/basemap.pmtiles"
if [ -z "${VITE_BASEMAP_URL:-}" ] && [ ! -f "$BASEMAP" ]; then
  echo "No basemap in $DATA_DIR — fetching a zoom 0-7 extract of the Protomaps planet (about 190 MB) ..."
  uv run python scripts/fetch_basemap.py --out "$BASEMAP" \
    || echo "WARNING: basemap fetch failed; weather will show without coastlines. Retry with: uv run python scripts/fetch_basemap.py"
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
(cd frontend && PATH="$NODE_BIN:$PATH" VITE_API_BASE_URL="" exec ./node_modules/.bin/vite --host 127.0.0.1) &
PIDS+=($!)

# ── Ready ────────────────────────────────────────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Frontend:  the \"Local:\" URL Vite prints below (5173 unless taken)"
echo "  Backend:   http://localhost:8000"
echo "  TiTiler:   http://localhost:8080"
echo "  Basemap:   ${VITE_BASEMAP_URL:-/basemap/basemap.pmtiles (served from $BASEMAP)}"
echo "  Node:      $("$NODE_BIN/node" --version) from $NODE_BIN"
if [ "$NEPTUNE_LIVE_ENABLE" = "true" ]; then
  echo "  AIS Live:  enabled (shared DuckDB at $AIS_DB_PATH)"
fi
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Press Ctrl+C to stop all services"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

wait
