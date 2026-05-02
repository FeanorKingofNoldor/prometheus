# Prometheus Daemon Recovery Runbook

This is the on-call playbook when the production trading daemon misbehaves.
Each scenario lists symptoms, diagnosis steps, and the safest recovery
action. Run commands as the `feanor` user unless noted.

## 0. First diagnostic

```bash
systemctl status prometheus-daemon
journalctl -u prometheus-daemon --since "1 hour ago" -n 200
curl -s http://127.0.0.1:8200/health | jq .
```

The `/health` endpoint returns 503 with structured `checks` when any
dependency is down (runtime DB, historical DB, stale pipeline). Read the
JSON body before doing anything destructive.

---

## 1. Daemon dead / refusing to start

**Symptoms**: `systemctl status` shows failed, restart loop, or "exited
with code 2".

**Diagnose**:
- `journalctl -u prometheus-daemon -n 100` — look for the last error
- Code 2 → IBKR credential preflight failed; check the
  `IBKR_PAPER_*` env vars in `/etc/sysconfig/prometheus-daemon`
- ImportError → likely a partial deploy; verify
  `/home/feanor/coding/apatheon/.venv/lib/...` matches `git log`

**Fix**:
1. Set the missing env var or revert the bad deploy
2. `systemctl daemon-reload && systemctl restart prometheus-daemon`
3. `curl http://127.0.0.1:8200/health` — must return 200

---

## 2. Pipeline stuck mid-phase ("RUNNING" forever)

**Symptoms**: `engine_runs` row sitting in `SIGNALS_DONE` /
`UNIVERSES_DONE` etc. for hours; daemon log goes quiet on that run.

**Diagnose**:
```sql
SELECT run_id, as_of_date, region, phase, updated_at
FROM engine_runs
WHERE phase NOT IN ('COMPLETED', 'FAILED')
ORDER BY updated_at;
```
Cross-reference `job_executions` for the same as_of_date — any
`RUNNING` rows indicate either an active job or an orphaned thread.

**Fix**:
- **If <24h old**: do nothing — the daemon's zombie reaper will finalise
  it as FAILED at the next morning catch-up window.
- **If >24h old and blocking today's run**: force-finalise:
```sql
UPDATE engine_runs SET phase = 'FAILED',
       error = jsonb_build_object('manual_finalisation', NOW()::text)
 WHERE run_id = '<the run_id>';
```
- **If a job_execution is RUNNING but no thread exists**:
```sql
UPDATE job_executions SET status = 'FAILED',
       error_message = 'manual cleanup — thread orphaned'
 WHERE execution_id = '<the execution_id>';
```

---

## 3. Options orders not flowing to broker

**Symptoms**: `run_options_for_run` log lines say `LOG_ONLY` or
`submitted=0`; IBKR shows no orders.

**Diagnose**:
- Live mode requires `PROMETHEUS_OPTIONS_SUBMIT_LIVE=1` in the env file.
  If not set, the daemon logs "LIVE mode but PROMETHEUS_OPTIONS_SUBMIT_LIVE
  not set — logging N directive(s) without submitting".
- Paper mode submits unconditionally but requires IB Gateway logged in
  on port 4002 (paper) or 4001 (live).
- `pgrep -af "ib.*gateway\|java.*ibgateway"` — confirm gateway is up.

**Fix**:
1. Start IB Gateway and log in (manually, the daemon does NOT log you in)
2. If you intentionally want live submission, set the env flag and
   restart the daemon
3. Trigger the options job to retry by faking a phase rewind:
```sql
UPDATE engine_runs SET phase = 'EXECUTION_DONE'
 WHERE as_of_date = CURRENT_DATE AND region = 'US_EQ';
```

---

## 4. Drawdown circuit breaker tripped

**Symptoms**: Every order rejected with "drawdown circuit breaker tripped:
... dd=11.43% > max_drawdown_pct=10.00%".

**Diagnose**:
```sql
SELECT MAX(equity), MIN(equity) FROM portfolio_equity_history
 WHERE as_of_date >= CURRENT_DATE - 252;
```
Compare against current account NAV from IBKR.

**Fix**:
- **Believed correct**: do nothing — the breaker is doing its job.
  Recovery happens automatically when equity climbs back above
  `peak * (1 - max_drawdown_pct)`.
- **False alarm** (e.g. peak in `portfolio_equity_history` is a stale
  test row): clean the history table or temporarily raise the threshold
  via `EXEC_RISK_MAX_DRAWDOWN_PCT=0.20` in the env file, restart daemon,
  then revert once the bad peak is overwritten.

---

## 5. Stale prices preventing pipeline advance

**Symptoms**: `ingest_prices` repeatedly logs "price data stale: latest
trade_date=YYYY-MM-DD vs expected_as_of=YYYY-MM-DD (lag=Nd > 1d)".

**Diagnose**: EODHD ingest is failing silently for fresh dates.
```sql
SELECT trade_date, COUNT(*) FROM prices_daily
 WHERE trade_date >= CURRENT_DATE - 5
 GROUP BY trade_date ORDER BY trade_date;
```

**Fix**:
1. Check EODHD API key/quota — `curl https://eodhd.com/api/...`
2. Manually run today's ingestion:
```bash
cd /home/feanor/coding/apatheon
.venv/bin/python -m apatheon.data_ingestion.daily_orchestrator US_EQ
```
3. If EODHD is down, override staleness threshold (will run on yesterday's
   prices — accept the risk explicitly):
   - Currently the threshold is hardcoded to 1d in `daily_orchestrator.py`;
     change `max_lag_days` parameter and re-deploy if persistent.

---

## 6. Log file growing out of control

**Symptoms**: `/home/feanor/coding/prometheus/apatheon.log` over 100MB.

**Diagnose**: log rotation should kick in at `APATHEON_LOG_MAX_BYTES`
(default 100MB). Verify via:
```bash
ls -lh /home/feanor/coding/prometheus/apatheon.log*
```

**Fix**:
- If only `apatheon.log` exists (no `.1`, `.2` etc.), the daemon was
  running on the pre-rotation build. Restart picks up the rotating
  handler.
- If rotation is working but logs grow too fast, tune via env:
```
APATHEON_LOG_MAX_BYTES=52428800
APATHEON_LOG_BACKUP_COUNT=10
```

---

## 7. Cassandra / PgBouncer down

**Symptoms**: `/health` reports `runtime_db.ok=false` or
`historical_db.ok=false`; daemon logs `DatabaseError: Failed to acquire
... connection`.

**Fix**:
```bash
systemctl status pgbouncer postgresql
systemctl restart pgbouncer  # safe; stateless
```
Daemon will retry connections on next cycle (60s).

---

## 8. Graceful shutdown for maintenance

```bash
sudo systemctl stop prometheus-daemon  # sends SIGTERM
```

The daemon now:
1. Catches SIGTERM, sets `_shutdown_event`
2. Wakes from any sleep within 1s
3. Finalises in-flight `job_executions` as FAILED with reason
   "daemon shutdown while job was running"
4. Exits cleanly

If it hangs > 90s, systemd sends SIGKILL. After SIGKILL, expect zombie
runs the next morning's reaper will clean up.

---

## 9. Total wipe & restart (last resort)

Only after the above fail. **Drops all in-flight state for today**:

```bash
sudo systemctl stop prometheus-daemon
psql -h 127.0.0.1 -p 6432 prometheus_runtime -c "
  UPDATE engine_runs SET phase = 'FAILED'
   WHERE as_of_date = CURRENT_DATE
     AND phase NOT IN ('COMPLETED', 'FAILED');"
psql -h 127.0.0.1 -p 6432 prometheus_runtime -c "
  UPDATE job_executions SET status = 'FAILED'
   WHERE as_of_date = CURRENT_DATE
     AND status IN ('RUNNING', 'PENDING');"
sudo systemctl start prometheus-daemon
```

---

## Environment variables quick reference

| Var | Default | Purpose |
|-----|---------|---------|
| `APATHEON_LOG_MAX_BYTES` | 104857600 | Log file rotation size |
| `APATHEON_LOG_BACKUP_COUNT` | 5 | Number of rotated files kept |
| `PROMETHEUS_LOCAL_TZ` | Europe/Berlin | Local zone for scheduler decisions |
| `PROMETHEUS_OPTIONS_SUBMIT_LIVE` | unset | Required for live mode order submission |
| `PROMETHEUS_OPTIONS_MAX_FAILURE_PCT` | 50 | Threshold for hard-failing options job |
| `PROMETHEUS_RETRY_MAX_DELAY_SECONDS` | 3600 | Cap on exponential retry backoff |
| `PROMETHEUS_CATCHUP_BUDGET_SECONDS` | 1200 | Wall-clock budget for morning catch-up |
| `EXEC_RISK_ENABLED` | true | Master switch for execution risk wrapper |
| `EXEC_RISK_MAX_DRAWDOWN_PCT` | 0.0 (disabled) | Drawdown circuit breaker |
| `EXEC_RISK_MAX_SECTOR_CONCENTRATION_PCT` | 0.0 (disabled) | Per-sector exposure cap |
| `IBKR_PAPER_USERNAME` | unset | Required at boot if `--options-mode=paper` |
| `IBKR_LIVE_USERNAME` | unset | Required at boot if `--options-mode=live` |
