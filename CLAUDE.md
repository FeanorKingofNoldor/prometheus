# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

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

# Start full stack (Apathis API on :8100, Prometheus API on :8200, frontends on :5173/:5174)
./start.sh

# Start with catch-up pipeline
RUN_CATCHUP=1 ./start.sh

# Backend only (no daemon)
NO_DAEMON=1 ./start.sh
```

## Apathis (Info Layer)

Apathis (`../apathis`) is the intelligence layer that Prometheus depends on. It must be running at port 8100 before Prometheus starts (`start.sh` handles this). Run it with `APATHIS_MODE=private` (disables auth/rate-limiting for internal use).

Apathis provides:
- **Regime detection** — market regime (crisis/expansion/contraction) consumed by Prometheus engines
- **Stability (STAB) scores** — market stability signals
- **Fragility scores** — entity/sector fragility
- **Nation risk & intel** — geopolitical signals feeding the assessment engine
- **Market data** — prices, returns, volatility, fundamentals, macro series via `prometheus_historical` DB
- **LLM chat** — Kronos integration via `/api/chat`

Both projects share the same two PostgreSQL databases (`prometheus_historical`, `prometheus_runtime`) accessed through PgBouncer on port 6432. Apathis owns the schema and ingestion; Prometheus reads from it. Apathis has its own CLAUDE.md at `../apathis/CLAUDE.md`.

## Architecture

Prometheus is a multi-market quantitative trading system layered on top of the `apathis` sibling package (the intelligence/info layer). It consists of:

### Pipeline State Machine
The daily pipeline progresses through phases tracked in the `engine_runs` DB table:
`WAITING_FOR_DATA → DATA_READY → SIGNALS_DONE → UNIVERSES_DONE → BOOKS_DONE → EXECUTION_DONE → OPTIONS_DONE → COMPLETED`

Each phase maps to tasks in `prometheus/pipeline/tasks.py`. State transitions live in `prometheus/pipeline/state.py`.

### Market-Aware Daemon (`prometheus/orchestration/`)
The daemon (`market_aware_daemon.py`) polls every 60s, detects market state (PRE_OPEN, OPEN, POST_CLOSE, etc.), and executes DAG-defined jobs in dependency order with retry logic. Separate DAGs exist for `US_EQ`, `KRONOS`, and `INTEL` markets.

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

### Meta-Orchestrator (`prometheus/meta/`)
Generates decision proposals, logs them to `engine_decisions`, and tracks realized outcomes vs. decision-time expectations at multiple time horizons (1d, 5d, etc.).

### Backtest Infrastructure (`prometheus/backtest/`)
`BacktestRunner.run_sleeve()` iterates daily via `TimeMachine`, calls a `target_positions_fn()` callback per date, and persists results to `backtest_runs`, `backtest_trades`, `backtest_daily_equity`.

### FastAPI Monitoring Backend (`prometheus/monitoring/`)
REST API at port 8200. Docs at `/api/docs`. Endpoint groups:
- `/api/status` — system overview, DAG status, regime, fragility, assessment, universe, portfolio
- `/api/control` — trigger backtests, synthetic datasets, DAGs, config changes
- `/api/logs` — daily logs and trading reports
- `/api/kronos` — chat with meta-orchestrator
- `/api/meta` — engine configs and performance metrics

### Database
PostgreSQL via PgBouncer (port 6432). Migrations managed by Alembic (`migrations/`). Check migration status with `pytest tests/test_show_alembic_status.py` or `python -m prometheus.scripts.show.show_alembic_status`.

### Frontend (`prometheus_web/`)
React/Vite app (port 5173). Build: `npm --prefix prometheus_web ci && npm --prefix prometheus_web run build`.

## Key Dependencies
- `apathis` — sibling private package, must be running on port 8100
- `ib_insync` — Interactive Brokers API client
- `sqlalchemy` + `alembic` — ORM and migrations
- `fastapi` + `uvicorn` — REST API
- `ruff` — linter (line length 120, Python 3.11, ignores E501)
- `mypy` — type checker (only checks files listed in `pyproject.toml`)
