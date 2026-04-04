#!/usr/bin/env bash
# Prometheus + Apathis + Cassandra — Development Frontend Servers
#
# Backends run via systemd:
#   apathis-api         → :8100  (geopolitical intelligence)
#   cassandra           → :8200  (prediction market module)
#   prometheus-web      → :8000  (trading C2 backend)
#   prometheus-daemon   → orchestration (no port)
#
# This script only starts the Vite dev servers for hot-reload.
# For production: run ./deploy.sh instead (builds + copies static files).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
APATHIS_ROOT="/home/feanor/coding/apathis"

FRONTEND_PORT=5173
APATHIS_FRONTEND_PORT=5174

cleanup() {
  echo ""
  echo "Shutting down frontend dev servers..."
  kill "$FRONTEND_PID" "$APATHIS_FE_PID" 2>/dev/null || true
  wait "$FRONTEND_PID" "$APATHIS_FE_PID" 2>/dev/null || true
  echo "Done."
}
trap cleanup EXIT INT TERM

# ── Check backends are running ──────────────────────────
echo "Checking backend services..."
for svc in apathis-api cassandra prometheus-web prometheus-daemon; do
  if systemctl is-active --quiet "$svc" 2>/dev/null; then
    echo "  ✓ $svc is running"
  else
    echo "  ✗ $svc is NOT running — start with: sudo systemctl start $svc"
  fi
done

# ── Prometheus Frontend ──────────────────────────────────
echo "Starting Prometheus frontend on :$FRONTEND_PORT..."
cd "$ROOT/prometheus_web"
npx vite --port "$FRONTEND_PORT" < /dev/null &
FRONTEND_PID=$!
cd "$ROOT"

# ── Apathis Frontend ────────────────────────────────────
echo "Starting Apathis frontend on :$APATHIS_FRONTEND_PORT..."
cd "$APATHIS_ROOT/apathis_web"
npx vite --port "$APATHIS_FRONTEND_PORT" < /dev/null &
APATHIS_FE_PID=$!
cd "$ROOT"

sleep 3
echo ""
echo "════════════════════════════════════════════════════════"
echo "  Backends (systemd):"
echo "  Apathis API:    http://localhost:8100"
echo "  Cassandra API:  http://localhost:8200  (prediction markets)"
echo "  Prometheus API: http://localhost:8000"
echo ""
echo "  Frontends (dev mode — hot reload):"
echo "  Prometheus UI:  http://localhost:$FRONTEND_PORT"
echo "  Apathis UI:     http://localhost:$APATHIS_FRONTEND_PORT"
echo "    └ Polymarket:  http://localhost:$APATHIS_FRONTEND_PORT/app/polymarket"
echo ""
echo "  Press Ctrl+C to stop frontend dev servers"
echo "════════════════════════════════════════════════════════"
echo ""

wait
