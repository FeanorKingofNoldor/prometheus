# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Configured external APIs

**Check this list BEFORE recommending a paid service — Max already pays for most worth paying for.** Repo-side env file: `.env` (gitignored). Live daemon: `/etc/sysconfig/prometheus-daemon`. See `[[api_keys_inventory]]` memory for the full cross-repo inventory.

| Key | Use |
|---|---|
| `EODHD_API_KEY` | EOD prices, fundamentals, exchange holidays, earnings calendar |
| `EIA_API_KEY` | US Energy Information Administration: petroleum/natgas inventories AND release schedule (Wed petroleum 10:30 ET, Thu storage 10:30 ET) |
| `FRED_API_KEY` | St Louis Fed FRED — macro series, BLS series metadata for release dates |
| `TRADING_ECONOMICS_API_KEY` (in apatheon repo `.env`) | **Comprehensive economic calendar: FOMC, CPI, NFP, OPEC, USDA WASDE, global central banks. Use this for the canonical event calendar.** |
| `AISSTREAM_API_KEY` | Vessel AIS (chokepoint behavioral signal) |
| `ANTHROPIC_API_KEY` | Claude API for super-iris desk |
| `IBKR_PAPER_*` / `IBKR_LIVE_*` | Broker — paper `:4002` active (account `DUN807925`, username `xubtmn245` — switched 2026-07-04 from `DUN188994` because the second account already carries the multi-market trading permissions), live `:4001` intentionally off (see `[[paper_only_until_prometheus_trusted]]`) |
| `STRIPE_*` | Billing (SaaS) |

## Commands

```bash
# Install dev dependencies
pip install -e .[dev]

# Run all tests
pytest

# Run a single test
pytest tests/test_execution_api_persistence.py::test_apply_execution_plan_paper_persists_statuses_and_filters_batch_fills -v

# Lint
ruff check .
ruff check --fix .

# Type check
mypy

# Start full stack (Apatheon API on :8100, Prometheus API on :8200, frontends on :5173/:5174)
./start.sh

# Start with catch-up pipeline
RUN_CATCHUP=1 ./start.sh

# Backend only (no daemon)
NO_DAEMON=1 ./start.sh
```

## Apatheon (Info Layer)

Apatheon (`../apatheon` — Python package name unchanged) is the intelligence layer that Prometheus depends on. It must be running at port 8100 before Prometheus starts (`start.sh` handles this). Run it with `APATHEON_MODE=private` (or legacy `APATHEON_MODE=private`) to disable auth/rate-limiting for internal use.

Apatheon provides:
- **Regime detection** — market regime (crisis/expansion/contraction) consumed by Prometheus engines
- **Stability (STAB) scores** — market stability signals
- **Fragility scores** — entity/sector fragility
- **Nation risk & intel** — geopolitical signals feeding the assessment engine
- **Market data** — prices, returns, volatility, fundamentals, macro series via `prometheus_historical` DB
- **LLM chat** — Iris integration via `/api/chat`

Both projects share the same two PostgreSQL databases (`prometheus_historical`, `prometheus_runtime`) accessed through PgBouncer on port 6432. Apatheon owns the schema and ingestion; Prometheus reads from it. Apatheon has its own CLAUDE.md at `../apatheon/CLAUDE.md`.

## Architecture

Prometheus is a multi-market quantitative trading system layered on top of the `apatheon` sibling package (the intelligence/info layer). It consists of:

### Pipeline State Machine
The daily pipeline progresses through phases tracked in the `engine_runs` DB table:
`WAITING_FOR_DATA → DATA_READY → SIGNALS_DONE → UNIVERSES_DONE → BOOKS_DONE → EXECUTION_DONE → OPTIONS_DONE → COMPLETED`

Each phase maps to tasks in `prometheus/pipeline/tasks.py`. State transitions live in `prometheus/pipeline/state.py`.

### Market-Aware Daemon (`prometheus/orchestration/`)
The daemon (`market_aware_daemon.py`) polls every 60s, detects market state (PRE_OPEN, OPEN, POST_CLOSE, etc.), and executes DAG-defined jobs in dependency order with retry logic. Separate DAGs exist for `US_EQ`, `IRIS`, and `INTEL` markets.

### Engine Facades
Each engine has an `api.py` (public interface) and `storage.py` (persistence):
- **AssessmentEngine** (`prometheus/assessment/`) — scores instruments (alpha, conviction, risk)
- **UniverseEngine** (`prometheus/universe/`) — filters instruments based on constraints
- **PortfolioEngine** (`prometheus/portfolio/`) — constructs target positions from universe
- **RiskEngine** (`prometheus/risk/`) — applies position/exposure constraints

### Execution Layer (`prometheus/execution/`)
Uses a **broker factory pattern**: `BrokerInterface` (abstract) → implementations:
- `IBKRClientImpl` — live Interactive Brokers trading
- `PaperBroker` / `BacktestBroker` — simulation modes
- `RiskBroker` — risk-filtered wrapper

### Derivatives Sleeves (`prometheus/derivatives/`)
Successor to the seventeen per-strategy classes in `execution/options_strategy.py`.
Four explicit sleeves with fixed NAV budgets (35% total when all cut over):
- **HEDGE** (10% NAV) — always-on downside protection
- **INCOME** (15% NAV) — short-premium when vol is rich
- **CONVEX** (5% NAV) — Apatheon-signal-driven asymmetric bets
- **COMMODITY** (5% NAV) — long-debit FOP calls on commodity intel

Each sleeve owns a tuple of `TemplateConfig` entries (e.g. `hedge.sector_put_spread`,
`income.spy_iron_condor`, `convex.convergence_straddle`). The runner
(`run_sleeve`) walks every template, evaluates its trigger against signals,
calls `select_contract` / `select_spread` (which goes chain → liquidity filter
→ live IV → delta-by-IV → pick), and emits `SleeveDirective` rows that survive
greeks-headroom + margin checks. Two persistence tables back this:
`options_positions` (mutable state) and `derivatives_shadow_decisions` (per-template-per-day log).

**Operational state (updated 2026-07-03):** shadow mode is configured via
`PROMETHEUS_DERIVATIVES_SHADOW`; per-sleeve cutover flags
(`PROMETHEUS_DERIVATIVES_{HEDGE,INCOME,CONVEX}_CUTOVER`) are unset, so the
legacy strategies still drive execution while the new pipeline shadow-logs
every day's would-have-done. Cutover decisions wait on ≥10 days of shadow
data per sleeve. NOTE: the shadow-day counter starts 2026-07 — a SQL
reserved-word bug (unquoted `right` column) crashed the derivatives job
daily from 2026-05-21 until the 2026-07-03 rehabilitation, so no earlier
shadow data exists. `run_derivatives_daily` returns `errors` (fatal,
pre-submission — retry-safe) separately from `warnings` (post-submission —
never retried), and option orders carry deterministic orderRefs with a
pre-submission open-order check.

The full template list lives in `prometheus/derivatives/sleeves.py`; the
adapter that runs the harness against synthetic data is in
`prometheus/derivatives/backtest.py`.

### Meta-Orchestrator (`prometheus/meta/`)
Generates decision proposals, logs them to `engine_decisions`, and tracks realized outcomes vs. decision-time expectations at multiple time horizons (1d, 5d, etc.).
Approved+applied config proposals now actually take effect: `run_books_for_run`
overlays `strategies.active_strategy_config_id` → `strategy_configs` onto the
YAML-derived `PortfolioConfig` via `prometheus/meta/config_resolver.py`
(whitelisted keys only; approval stays human via the intelligence API).

### Signal & sizing conventions (post-2026-07 rehabilitation)
- **All signal math runs on `adjusted_close`** (see `prometheus/pricing_utils.py`);
  raw `close` is only for trade prices/price filters. Momentum uses a
  126d window with a 21d skip-month; cross-sectional standardization is
  median/MAD.
- **Budget stack**: five multiplicative layers — regime, fragility,
  Tier-1 SOP, forward indicators (each floored individually at its own
  min), then the sector allocator (unfloored). The combined product is
  deliberately unfloored: in a real crisis the equity book can go to 0.
  All five multipliers land in `target_portfolios` metadata daily.
- **Conviction lifecycle** is fully armed: prices/stress/positions
  providers are wired in `run_books_for_run`, exits write tombstones
  (`position_convictions.exited_at`), stale states expire after 45 days,
  and exits taper instead of cliff-liquidating.
- **`target_portfolios` holds exactly ONE row per (strategy, date)** —
  the post-sector-allocator final target (pre-allocator weights are in
  metadata).
- **Fill telemetry**: execution submits post-close; overnight fills are
  captured by the morning `reconcile_fills` daemon job (IBKR
  `reqExecutions` matched by orderRef). Never trust `fills` before the
  morning reconcile has run for that session.
- **Calibration timeline** (paper run restarted 2026-07): the 5d-horizon
  assessment scorecard becomes statistically meaningful after ~60
  trading sessions (~late Sep 2026); portfolio-level verdicts (Sharpe,
  drift, hedge effectiveness) need ~100 independent outcomes (~5
  months). Metrics before those dates carry `INSUFFICIENT_DATA` /
  `reliable=false` flags — do not trust earlier numbers.

### Multi-market orchestration (24/7, built 2026-07)
The daemon runs **per-market lanes**: each market's jobs stay strictly
serialized while different markets (and IRIS/INTEL) run concurrently;
one global IBKR token serializes gateway-touching jobs (fixed client ids
per job type). Rollover is a deferred per-lane DAG swap; morning
catch-up is per market and non-blocking. Non-US market DAGs omit
options/snapshot/reconcile/geo (account-global, US_EQ DAG only — see
`MarketDagProfile` in `orchestration/dag.py`). Global signals (sector
health, forward indicators, Tier-1) compute once in the US signals run;
other regions read the persisted values (staleness-bounded) — Asia
trading on last-US-close risk state is intentional (news-lag thesis).
Sizing: `account_equity × book.capital_fraction × weight / price_usd`
with FX conversion (`prometheus/execution/fx.py`, `fx_rates_daily`,
LSE pence handled); a book only sees its own market's positions.
FX settlement: "convert once, fixed local pots" — the `fx_sweep` job
(US DAG, client_id 15) zeroes negative non-USD balances >$2k via
IDEALPRO; positive local balances are the books' working capital and
are never swept back; KRW is trade-linked (never converted by us).

**Enable-all-markets runbook**: 1) IBKR Client Portal → Trading
Permissions → Stocks: enable UK, Germany, France, Netherlands, Belgium,
Switzerland, Spain, Finland, Hong Kong, Korea, Australia. 2) With the
paper gateway up:
`python -m prometheus.scripts.show.qualify_market_contracts --market X_EQ --collect-lots`
for each of UK/EU/HK/KR/AU_EQ (collects HK board lots; must exit 0 —
a failure means fix the mapping or retire the name). 3)
`python -m prometheus.scripts.maintenance.enable_multimarket` — applies
the capital fractions (US .50 / EU .15 / UK .10 / HK .10 / KR .075 /
AU .075) to books.yaml and validates Σ=1.0; idempotent; while US-only,
US stays at the 1.0 default. 4) root: remove the three `--market` lines
from /etc/systemd/system/prometheus-daemon.service (**CLI flags OVERRIDE
the env var**), set `PROMETHEUS_ACTIVE_MARKETS=US_EQ,UK_EQ,EU_EQ,HK_EQ,KR_EQ,AU_EQ,IRIS,INTEL`
in /etc/sysconfig/prometheus-daemon, then
`systemctl daemon-reload && systemctl restart prometheus-daemon apatheon-api`.
5) Verify: one engine_runs row per region per day, no FAILED jobs.
**Regulatory (EU retail at IBKR Ireland)**: individual stocks are fine
everywhere; US-domiciled ETFs (SPY/QQQ/XL*) are PRIIPs-blocked on the
LIVE entity even though paper accepts them — use UCITS twins
(CSPX/SXR8, CNDX, SXL*) or SPX/XSP options; Korea: KRW is trade-linked
settlement only, KEPCO/KOGAS untradeable, ±30% daily limits; HK orders
must respect board lots. Japan blocked (no Tokyo on the EODHD plan).

### Backtest Infrastructure (`prometheus/backtest/`)
`BacktestRunner.run_sleeve()` iterates daily via `TimeMachine`, calls a `target_positions_fn()` callback per date, and persists results to `backtest_runs`, `backtest_trades`, `backtest_daily_equity`.
**Namespace rule**: backtests share tables with production and are isolated
only by id — every backtest id must carry a backtest prefix (`BT_`, `LAMBDA_`,
…), enforced by `prometheus/backtest/naming.py`. Live ids
(`US_CORE_LONG_EQ`, `US_EQ_LONG_V12`, `CORE_EQ_US`, `IBKR_PAPER`) are always
rejected. Historical CAGR/Sharpe claims produced before 2026-07 ran on
split-unadjusted prices without the production overlay stack — treat them
as unvalidated until re-run.

### FastAPI Monitoring Backend (`prometheus/monitoring/`)
REST API at port 8200. Docs at `/api/docs`. Endpoint groups:
- `/api/status` — system overview, DAG status, regime, fragility, assessment, universe, portfolio
- `/api/control` — trigger backtests, synthetic datasets, DAGs, config changes
- `/api/logs` — daily logs and trading reports
- `/api/iris` — chat with meta-orchestrator
- `/api/meta` — engine configs and performance metrics

### Database
PostgreSQL via PgBouncer (port 6432). Migrations managed by Alembic (`migrations/`). Check migration status with `pytest tests/test_show_alembic_status.py` or `python -m prometheus.scripts.show.show_alembic_status`.

### Frontend (`prometheus_web/`)
React/Vite app (port 5173). Build: `npm --prefix prometheus_web ci && npm --prefix prometheus_web run build`.

## Key Dependencies
- `apatheon` — sibling private package, must be running on port 8100
- `ib_insync` — Interactive Brokers API client
- `sqlalchemy` + `alembic` — ORM and migrations
- `fastapi` + `uvicorn` — REST API
- `ruff` — linter (line length 120, Python 3.11, ignores E501)
- `mypy` — type checker (only checks files listed in `pyproject.toml`)
