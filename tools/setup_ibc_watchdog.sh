#!/usr/bin/env bash
# IBC live-gateway watchdog + pinned 07:30 daily restart.
#
# Problem: IBC can freeze on a credential-rejection modal when 2FA
# pushes are missed. systemd thinks bash is alive (because it is) and
# never restarts. AutoRestartTime never fires from a frozen state.
# Result: no fresh login, no daily 2FA push, silent failure for days.
#
# Fix:
#   1. Pin IBC AutoRestartTime to 07:30 (was 07:00).
#   2. systemd OnCalendar timer fires at 07:35 daily as a hard backup
#      `systemctl restart` — covers the case where IBC's internal
#      restart deadlocked.
#   3. Watchdog timer (every 15 min) probes localhost:4001. If the
#      port is closed, restart the service. Catches deadlocked GUI
#      states that don't show as "service failed".
#   4. Drop-in adds StartLimitIntervalSec=600 StartLimitBurst=3 so
#      a credential-rejection storm can't spiral past 3 restarts /
#      10 min — protects against IBKR locking the account.
#
# Run once with sudo. Idempotent — safe to re-run.
set -euo pipefail

CONFIG=/opt/ibc/config.live.ini
WATCHDOG_BIN=/usr/local/bin/ibc-watchdog.sh
LIVE_PORT=4001
LIVE_SERVICE=ibc-gateway-live.service

# ── 1. Patch IBC config: AutoRestartTime → 07:30 ──────────────
if [[ -f "$CONFIG" ]]; then
  cp "$CONFIG" "${CONFIG}.bak.$(date +%Y%m%d)"
  sed -i 's/^AutoRestartTime=.*/AutoRestartTime=07:30/' "$CONFIG"
  echo "[config] AutoRestartTime → 07:30"
else
  echo "ERR: $CONFIG missing" >&2; exit 1
fi

# ── 2. Watchdog script ────────────────────────────────────────
cat > "$WATCHDOG_BIN" <<'EOF'
#!/usr/bin/env bash
# Probe localhost:4001 (IBKR live gateway). If down for two consecutive
# checks, restart the unit. Two-strike rule avoids restarting during the
# normal 30-second login startup window.
set -euo pipefail

PORT=4001
SERVICE=ibc-gateway-live.service
STATE_FILE=/run/ibc-watchdog-live.miss

if nc -z -w 3 localhost "$PORT" 2>/dev/null; then
  rm -f "$STATE_FILE"
  exit 0
fi

# Port is dead. Increment miss counter.
MISSES=0
[[ -f "$STATE_FILE" ]] && MISSES=$(<"$STATE_FILE")
MISSES=$((MISSES + 1))
echo "$MISSES" > "$STATE_FILE"

if [[ $MISSES -lt 2 ]]; then
  echo "[watchdog] port $PORT down (miss $MISSES/2); waiting one more cycle"
  exit 0
fi

echo "[watchdog] port $PORT down for 2 consecutive checks; restarting $SERVICE"
rm -f "$STATE_FILE"
systemctl restart "$SERVICE"
EOF
chmod 755 "$WATCHDOG_BIN"
echo "[watchdog] installed $WATCHDOG_BIN"

# ── 3. Watchdog systemd unit + timer (every 15 min) ───────────
cat > /etc/systemd/system/ibc-watchdog-live.service <<EOF
[Unit]
Description=IBC live gateway watchdog (probe :$LIVE_PORT, restart if dead)
After=$LIVE_SERVICE

[Service]
Type=oneshot
ExecStart=$WATCHDOG_BIN
EOF

cat > /etc/systemd/system/ibc-watchdog-live.timer <<EOF
[Unit]
Description=Run IBC live watchdog every 15 minutes

[Timer]
OnBootSec=5min
OnUnitActiveSec=15min
AccuracySec=30s
Unit=ibc-watchdog-live.service

[Install]
WantedBy=timers.target
EOF

# ── 4. Hard daily restart at 07:35 (5 min after IBC's own) ────
cat > /etc/systemd/system/ibc-restart-live.service <<EOF
[Unit]
Description=Force-restart IBC live gateway (backup for IBC's internal AutoRestartTime)

[Service]
Type=oneshot
ExecStart=/usr/bin/systemctl restart $LIVE_SERVICE
EOF

cat > /etc/systemd/system/ibc-restart-live.timer <<EOF
[Unit]
Description=Daily IBC live gateway restart at 07:35

[Timer]
OnCalendar=*-*-* 07:35:00
AccuracySec=10s
Persistent=true
Unit=ibc-restart-live.service

[Install]
WantedBy=timers.target
EOF

# ── 5. Drop-in: rate-limit restarts ───────────────────────────
mkdir -p /etc/systemd/system/${LIVE_SERVICE}.d
cat > /etc/systemd/system/${LIVE_SERVICE}.d/restart-burst.conf <<EOF
[Unit]
StartLimitIntervalSec=600
StartLimitBurst=3

[Service]
# If IBC ever wedges past 25h (e.g. frozen modal that doesn't drop the
# port), force-kill and let Restart=always bring us back fresh.
RuntimeMaxSec=90000
EOF

# ── 6. Reload + enable + start timers ─────────────────────────
systemctl daemon-reload
systemctl enable --now ibc-watchdog-live.timer
systemctl enable --now ibc-restart-live.timer

echo
echo "── Setup complete ──"
echo "Timers active:"
systemctl list-timers ibc-watchdog-live.timer ibc-restart-live.timer --no-pager
echo
echo "Next: restart the live service NOW to clear the stuck modal."
echo "   sudo systemctl restart $LIVE_SERVICE"
echo "Then watch the log and approve the 2FA push on your phone:"
echo "   sudo journalctl -u $LIVE_SERVICE -f"
