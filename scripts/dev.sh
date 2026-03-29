#!/usr/bin/env bash
# Quick local dev startup — no Docker required.
# Starts TiTiler, backend, and frontend using existing data in .data/
#
# Usage:  bash scripts/dev.sh
#         ./scripts/dev.sh          (after chmod +x)

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# ── Pre-flight ───────────────────────────────────────────────────────
DATA_DIR="$ROOT/.data"

if [ ! -f "$DATA_DIR/models/gfs/catalog.json" ]; then
  echo "ERROR: No data found at $DATA_DIR/models/gfs/catalog.json"
  echo "Run the pipeline at least once first:"
  echo "  uv run python scripts/run_pipeline.py --data-dir .data"
  exit 1
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

NODE_BIN="/Users/admin/.nvm/versions/node/v22.18.0/bin"

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

echo "Starting frontend on :5173 ..."
(cd frontend && PATH="$NODE_BIN:/usr/bin:/bin" exec ./node_modules/.bin/vite) &
PIDS+=($!)

# ── Ready ────────────────────────────────────────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Frontend:  http://localhost:5173"
echo "  Backend:   http://localhost:8000"
echo "  TiTiler:   http://localhost:8080"
if [ "$NEPTUNE_LIVE_ENABLE" = "true" ]; then
  echo "  AIS Live:  enabled (shared DuckDB at $AIS_DB_PATH)"
fi
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Press Ctrl+C to stop all services"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

wait
