#!/usr/bin/env bash
# Prometheus Integrated Stack — Apathis (info) + Prometheus (trading) + Frontend
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
APATHIS_ROOT="/home/feanor/coding/apathis"
APATHIS_VENV="$APATHIS_ROOT/.venv"
PROM_VENV="$ROOT/.venv"

APATHIS_PORT=8100
BACKEND_PORT=8200
FRONTEND_PORT=5173
APATHIS_FRONTEND_PORT=5174

DAEMON_PID=""
APATHIS_FE_PID=""

cleanup() {
  echo ""
  echo "Shutting down..."
  kill "$APATHIS_PID" "$BACKEND_PID" "$FRONTEND_PID" ${APATHIS_FE_PID:+"$APATHIS_FE_PID"} ${DAEMON_PID:+"$DAEMON_PID"} 2>/dev/null || true
  wait "$APATHIS_PID" "$BACKEND_PID" "$FRONTEND_PID" ${APATHIS_FE_PID:+"$APATHIS_FE_PID"} ${DAEMON_PID:+"$DAEMON_PID"} 2>/dev/null || true
  echo "Done."
}
trap cleanup EXIT INT TERM

# ── Activate Apathis venv (shared base for all Python) ───
source "$APATHIS_VENV/bin/activate"
echo "Activated venv: $APATHIS_VENV"

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

# ── Prometheus Frontend ───────────────────────────────────
echo "Starting Prometheus frontend on :$FRONTEND_PORT..."
cd "$ROOT/prometheus_web"
npx vite --port "$FRONTEND_PORT" < /dev/null &
FRONTEND_PID=$!
cd "$ROOT"

# ── Apathis Frontend ─────────────────────────────────────
echo "Starting Apathis frontend on :$APATHIS_FRONTEND_PORT..."
cd "$APATHIS_ROOT/apathis_web"
npx vite --port "$APATHIS_FRONTEND_PORT" < /dev/null &
APATHIS_FE_PID=$!
cd "$ROOT"

# ── Market-aware daemon (daily pipeline orchestrator) ────
if [[ "${NO_DAEMON:-}" != "1" ]]; then
  echo "Starting market-aware daemon (US_EQ + KRONOS + INTEL, options=paper)..."
  "$PROM_VENV/bin/python" -m prometheus.orchestration.market_aware_daemon \
    --market US_EQ \
    --market KRONOS \
    --market INTEL \
    --options-mode paper \
    --poll-interval-seconds 60 &
  DAEMON_PID=$!
else
  echo "Daemon disabled (NO_DAEMON=1)"
fi

# ── Optional catch-up: run today's pipeline ───────────────
# Disabled by default to avoid forcing run-state transitions during startup.
if [[ "${RUN_CATCHUP:-0}" == "1" ]]; then
  echo "Running catch-up pipeline for $(date +%Y-%m-%d)..."
  "$PROM_VENV/bin/python" -m prometheus.scripts.run.run_daily_pipeline \
      --date "$(date +%Y-%m-%d)" &
else
  echo "Skipping catch-up pipeline (set RUN_CATCHUP=1 to enable)"
fi

# ── Wait for services ────────────────────────────────────
sleep 3
echo ""
echo "════════════════════════════════════════════════════════"
echo "  Apathis API:      http://localhost:$APATHIS_PORT/api/docs"
echo "  Prometheus API:   http://localhost:$BACKEND_PORT/api/docs"
echo "  Prometheus UI:    http://localhost:$FRONTEND_PORT"
echo "  Apathis UI:       http://localhost:$APATHIS_FRONTEND_PORT"
[[ -n "$DAEMON_PID" ]] && echo "  Daemon:         PID $DAEMON_PID (US_EQ,KRONOS,INTEL, options=paper, 60s poll)"
echo "  Press Ctrl+C to stop all services"
echo "════════════════════════════════════════════════════════"
echo ""

wait
