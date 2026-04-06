# Prometheus

Algorithmic trading system for US equities with options overlay. Runs a daily pipeline that scores instruments, constructs portfolios, and executes trades via Interactive Brokers. Backtested at **23% CAGR, 1.2 Sharpe, -25% MaxDD** over 29 years (1997-2025), beating the S&P 500 in 25 of 29 years.

Built on top of [Apathis](../apathis) (the intelligence/info layer) which provides regime detection, stability scoring, entity graph, and geopolitical signals.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Daily Pipeline                            │
│                                                             │
│  Ingest Prices → Assessment → Universe → Portfolio → Exec   │
│       ↓              ↓           ↓          ↓         ↓     │
│   EODHD API     Momentum +    Top-K     Conviction   IBKR   │
│   + FRED        STAB + Embed  Ranking   Lifecycle    Paper   │
│                                                             │
│  ──── Overlay Layers ────                                   │
│  Forward Indicators (13 signals → budget adjustment)        │
│  Tier 1 Monitor (39 G-SIBs/CBs → SOP constraints)         │
│  Sector Allocator (SHI → kill/reduce sick sectors)          │
│  Options Overlay (7 strategies, regime-gated)               │
│  Iris Monitor (trade journal → weekly reports)            │
└─────────────────────────────────────────────────────────────┘
```

### Pipeline Phases
The daily pipeline progresses through phases tracked in `engine_runs`:

`WAITING_FOR_DATA → DATA_READY → SIGNALS_DONE → UNIVERSES_DONE → BOOKS_DONE → EXECUTION_DONE → OPTIONS_DONE → COMPLETED`

### Key Engines

| Engine | What it does | Key file |
|--------|-------------|----------|
| **Assessment** | Scores instruments: momentum + STAB fragility + embedding outlier + guidance breadth | `prometheus/assessment/model_basic.py` |
| **Universe** | Filters 660 instruments to top-K by score, with sector caps | `prometheus/universe/engine.py` |
| **Portfolio** | Builds target weights with conviction lifecycle (half-weight entry, 3-day build) | `prometheus/portfolio/model_conviction.py` |
| **Forward Indicators** | 13 leading signals (credit, rates, internals, macro) → budget multiplier | `apathis/regime/forward_indicators.py` |
| **Tier 1 Monitor** | 39 systemic entities (G-SIBs, central banks) → SOP constraints | `apathis/stability/tier1_monitor.py` |
| **Options** | 7 strategies (iron butterfly, VIX tail hedge, bull call spread, etc.) | `prometheus/execution/options_strategy.py` |
| **Crisis Alpha** | Offensive SPY puts when 5+ sectors deteriorate | `prometheus/sector/crisis_alpha.py` |
| **Daemon Log Viewer** | Live-tailing daemon logs in the frontend dashboard | `prometheus/monitoring/` |

### Production Config

**K20_CP1.0_CONV**: 20 names, equal-weight, conviction enabled, lambda scoring weight 10.

| Metric | Equity Only | Equity + Options |
|--------|------------|-----------------|
| CAGR | 17.6% | 23.0% |
| Sharpe | 1.031 | 1.196 |
| MaxDD | -46.9% | -24.6% |

## Project Structure

```
prometheus/
├── assessment/          # Instrument scoring (momentum, STAB, embeddings)
├── backtest/            # Backtest runner, options backtester, C++ bridge
├── books/               # Book registry, sleeve specs
├── decisions/           # Decision tracking + outcome evaluation
├── execution/           # Broker interface, IBKR client, options strategies
├── meta/                # Feedback loop, trade monitor, trade journal
├── monitoring/          # FastAPI REST API (port 8200)
├── opportunity/         # Lambda/GBT opportunity model
├── orchestration/       # Market-aware daemon, DAG executor, daily orchestrator
├── pipeline/            # Pipeline tasks, state machine, embedding generation
├── portfolio/           # Portfolio construction, conviction model
├── risk/                # Risk constraints
├── sector/              # Sector health (SHI), crisis alpha
├── scripts/             # CLI tools: backtest campaigns, backfills, utilities
├── synthetic/           # Synthetic market reality generation
├── universe/            # Universe selection engine
prometheus_web/          # React frontend (port 5173)
configs/                 # YAML configs: books, policy, universe, portfolio
migrations/              # Alembic database migrations
tests/                   # 222 unit tests
docs/                    # Dev log, analysis docs
```

## Quick Start

### Prerequisites
- Python 3.14 with venv
- PostgreSQL with PgBouncer (port 6432)
- Apathis running on port 8100
- IBKR Gateway (port 4002 for paper trading)

### Setup
```bash
# Backend
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]

# Frontend
npm --prefix prometheus_web ci
npm --prefix prometheus_web run build

# Database
alembic upgrade head

# Run tests
pytest

# Start full stack
./start.sh
```

### Running the Daemon
```bash
# Via systemd (production)
sudo systemctl start prometheus-daemon

# Manual (development)
python -m prometheus.orchestration.market_aware_daemon \
    --market US_EQ --market IRIS --market INTEL \
    --options-mode paper --poll-interval-seconds 60 \
    --morning-catchup-hour 9
```

### Running Backtests
```bash
# C++ engine (fast: 29 years in 14 seconds)
PYTHONPATH=../prometheus_v2/cpp/build python scripts/compare_engine_variants.py

# Python engine (with all features)
python -m prometheus.scripts.run.run_daily_pipeline --date 2024-12-31 --execute --paper

# Options overlay
python -m prometheus.scripts.run.run_options_backtest \
    --start 1997-01-02 --end 2025-12-31 \
    --equity-backtest results/equity_nav_k20_conv.json \
    --use-db --load-equity-universe
```

## Database

Two PostgreSQL databases via PgBouncer (port 6432):

- **prometheus_historical**: prices_daily, returns_daily, volatility_daily, nation_macro_indicators, numeric_window_embeddings
- **prometheus_runtime**: engine_runs, orders, fills, positions_snapshots, engine_decisions, decision_outcomes, backtest_runs, corporate_guidance, forward_indicator_snapshots, trade_journal

## API

FastAPI on port 8200. Docs at `http://localhost:8200/api/docs`.

| Group | Endpoints |
|-------|-----------|
| Status | `/api/status/overview`, `/api/status/regime`, `/api/status/sector_health` |
| Portfolio | `/api/status/portfolio`, `/api/status/portfolio_equity` |
| Execution | `/api/status/execution`, `/api/status/orders` |
| Meta | `/api/meta/feedback`, `/api/meta/weekly_report`, `/api/meta/trade_journal` |
| Control | `/api/control/schedule_dag`, `/api/control/sync_data` |
| Logs | `/api/logs/daemon` (live tailing) |
| Iris | `/api/iris/chat` |

## Tech Stack

- **Backend**: Python 3.14, FastAPI, psycopg2, ib_insync
- **Frontend**: React 19, Vite, TailwindCSS, TanStack Query, Recharts
- **Database**: PostgreSQL 18, PgBouncer
- **C++ Engine**: prom2_cpp (pybind11, C++20) for fast backtesting
- **Broker**: Interactive Brokers via IB Gateway
- **LLM**: Ollama (local, for Iris chat + EDGAR extraction)
