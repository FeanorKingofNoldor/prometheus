#!/usr/bin/env bash
# Prometheus + Apatheon — Production Build & Deploy
#
# Builds both frontends, copies to static dirs, restarts backends.
# After this, backends serve the built frontends directly — no Vite needed.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
APATHEON_ROOT="/home/feanor/coding/apatheon"

echo "═══════════════════════════════════════════════════"
echo "  Production Build & Deploy"
echo "═══════════════════════════════════════════════════"

# ── Build Apatheon frontend ───────────────────────────────
echo ""
echo "Building Apatheon frontend..."
cd "$APATHEON_ROOT/apatheon_web"
npm run build
rm -rf "$APATHEON_ROOT/static"
cp -r dist "$APATHEON_ROOT/static"
# Copy to /opt/ for nginx (nginx can't traverse /home/feanor/)
sudo rm -rf /opt/apatheon/static
sudo cp -r dist /opt/apatheon/static
echo "  ✓ Apatheon frontend built → /opt/apatheon/static/"

# ── Build Prometheus frontend ────────────────────────────
echo ""
echo "Building Prometheus frontend..."
cd "$ROOT/prometheus_web"
npm run build
rm -rf "$ROOT/static"
cp -r dist "$ROOT/static"
# Copy to /opt/ for nginx
sudo mkdir -p /opt/prometheus
sudo rm -rf /opt/prometheus/static
sudo cp -r dist /opt/prometheus/static
echo "  ✓ Prometheus frontend built → /opt/prometheus/static/"

# ── Deploy nginx configs ─────────────────────────────────
echo ""
echo "Deploying nginx configs..."
sudo cp "$ROOT/deploy/nginx/prometheus.conf" /etc/nginx/conf.d/prometheus.conf
sudo cp "$ROOT/deploy/nginx/apatheon.conf" /etc/nginx/conf.d/apatheon.conf
sudo nginx -t && sudo systemctl reload nginx
echo "  ✓ Nginx configs deployed"

# ── Restart backends to pick up static files ─────────────
echo ""
echo "Restarting backend services..."
sudo systemctl restart apatheon-api prometheus-api prometheus-daemon
sleep 5

# ── Verify ───────────────────────────────────────────────
echo ""
echo "Verifying..."
for svc in apatheon-api prometheus-api prometheus-daemon; do
  if systemctl is-active --quiet "$svc"; then
    echo "  ✓ $svc is running"
  else
    echo "  ✗ $svc FAILED — check: journalctl -u $svc -n 20"
  fi
done

echo ""
echo "═══════════════════════════════════════════════════"
echo "  Production deploy complete!"
echo ""
echo "  Apatheon:    http://localhost:8100  (API + frontend)"
echo "  Prometheus: http://localhost:8200  (API + frontend)"
echo ""
echo "  No Vite dev servers needed."
echo "  Logs: journalctl -u apatheon-api -f"
echo "═══════════════════════════════════════════════════"
