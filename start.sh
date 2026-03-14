#!/usr/bin/env bash
# Prometheus Integrated Stack — Apathis (info) + Prometheus (trading) + Frontend
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
APATHIS_ROOT="/home/feanor/coding/apathis"
APATHIS_VENV="$APATHIS_ROOT/.venv"
PROM_VENV="$ROOT/.venv"

APATHIS_PORT=8100
BACKEND_PORT=8000
FRONTEND_PORT=5173

cleanup() {
  echo ""
  echo "Shutting down..."
  kill "$APATHIS_PID" "$BACKEND_PID" "$FRONTEND_PID" 2>/dev/null || true
  wait "$APATHIS_PID" "$BACKEND_PID" "$FRONTEND_PID" 2>/dev/null || true
  echo "Done."
}
trap cleanup EXIT INT TERM

# ── Load environment ─────────────────────────────────────
if [[ -f "$ROOT/.env" ]]; then
  set -a
  source "$ROOT/.env"
  set +a
  echo "Loaded .env"
fi

# ── Apathis API (info-layer, private mode) ───────────────
echo "Starting Apathis API on :$APATHIS_PORT (private mode)..."
APATHIS_MODE=private "$APATHIS_VENV/bin/uvicorn" apathis.api.app:app \
  --host 0.0.0.0 \
  --port "$APATHIS_PORT" \
  --app-dir "$APATHIS_ROOT" &
APATHIS_PID=$!

# ── Prometheus trading API ───────────────────────────────
echo "Starting Prometheus trading API on :$BACKEND_PORT..."
"$PROM_VENV/bin/uvicorn" prometheus.monitoring.app:app \
  --host 0.0.0.0 \
  --port "$BACKEND_PORT" \
  --app-dir "$ROOT" &
BACKEND_PID=$!

# ── Frontend ─────────────────────────────────────────────
echo "Starting frontend on :$FRONTEND_PORT..."
cd "$ROOT/prometheus_web"
npx vite --port "$FRONTEND_PORT" < /dev/null &
FRONTEND_PID=$!
cd "$ROOT"

# ── Wait for services ────────────────────────────────────
sleep 3
echo ""
echo "════════════════════════════════════════════════════════"
echo "  Apathis API:   http://localhost:$APATHIS_PORT/api/docs"
echo "  Prometheus API: http://localhost:$BACKEND_PORT/api/docs"
echo "  Frontend:       http://localhost:$FRONTEND_PORT"
echo "  Press Ctrl+C to stop all services"
echo "════════════════════════════════════════════════════════"
echo ""

wait
