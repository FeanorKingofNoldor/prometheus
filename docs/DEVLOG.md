# Prometheus Development Log

## 2026-04-05 — Operations Dashboard, Daemon Reliability, Production Deployment

### Daemon Reliability
- Morning catchup date mismatch fix: separate temporary DAG for catchup date
- Timeout fixes: intel_flash_check 120s→300s, intel_daily_sitrep 1800s→5400s
- Catchup busy loop fix: sleep between iterations, max_iterations=60
- _check_timeouts stale variable: fixed wrong job reference
- Descriptive error messages for compute_returns/volatility failures
- Critical: apply_risk_constraints called with wrong kwargs in tasks.py

### Operations Dashboard
- New `/api/ops/overview` + `/api/ops/day/{date}` endpoints
- Frontend page at `/operations` with service status, 14-day history, day drill-down
- Sidebar entry with Server icon

### Production Deployment
- nginx configs: split routing (info-layer→:8100, trading→:8200)
- deploy.sh: builds frontends, copies to /opt/, deploys nginx, restarts services
- start.sh: added prometheus-api to health check
- Static IP: 88.116.16.140:8443

### Code Quality
- 4 audit rounds, 15+ fixes: lint errors, unused imports, TS type issues, cursor leaks

## 2026-04-01 — Pipeline Audit, Ollama Streaming, Daemon Log Viewer

### Pipeline Audit (38 Issues)
- **5 critical**: IBKR credentials removed from source, `timedelta` import fix, `decision_outcomes` UNIQUE constraint, log returns division-by-zero guard, volatility window off-by-one
- **12 high**: confidence now uses `adjusted_score`, scorecard O(N^2) replaced with batch SQL, max drawdown capped at -100%, FAILED state recovery, iron condor regime exit priority, COALESCE NULL fix
- **14 medium, 7 low**: assorted robustness and correctness fixes across the pipeline

### Ollama Streaming Timeout
- Replaced blocking `stream: False` with token-flow monitoring to avoid timeouts on long LLM calls

### DDG News Entity Mapping
- Topics from DuckDuckGo news now map to real graph entities instead of raw strings

### Daemon Log Viewer
- New frontend tab with live log tailing for the production daemon

### GDELT Rate Limiting
- Added 1.5s delay between GDELT queries to avoid HTTP 429 errors

### Conflict Analyst
- Re-enabled with 2 focused tools (was disabled due to hallucination issues)

### Runtime Data Cleanup
- Cleared runtime data for first real test week; March 30 reconstructed from source data

### Config Changes
- Covered call coverage ratio increased from 20% to 50%
- Kronos scorecard timeout increased to 3600s

---

## 2026-03-27 — Grid Search, Tier 1 Monitor, EDGAR Guidance, Kronos Monitor

### Production Config Optimization
- **83-config grid search** via C++ engine (17 min for 29 years × 83 configs)
- Swept: max_names (10-30), concentration_power (1.0-4.0), max_weight (6-20%), lambda_weight (0-20), universe_size, hysteresis
- Winner: **K20_CP1.0_CONV** (20 names, equal-weight, conviction enabled, lambda 10)
- Deployed to production: CAGR 17.6% (was 14.9%), Sharpe 1.031 (was 0.960)
- With options overlay: **23.0% CAGR, 1.20 Sharpe, -24.6% MaxDD**
- Beat S&P 500 in 25/29 years (86%)

### Options Strategy Sizing
- Iron butterfly: 20% NAV × 6 positions (120% exposure) → 5% × 2 (10%)
- Iron condor: 4% × 5 → 3% × 3
- Bull call spread: 12 positions → 6
- Wheel: 6% × 5 → 4% × 3
- Backtest validated: still adds +6% CAGR with realistic sizing

### Tier 1 Systemic Risk Monitor
- 21 G-SIBs (FSB bucket 1-4), 10 central banks, 8 market infrastructure
- **TBTF adjustment**: JPM/GS/Citi (bucket 3-4) get stability bonus; UBS/Barclays (bucket 1-2) get penalty
- Pre-scripted SOPs: differentiated by bank tier
- 8 G-SIB price histories backfilled from EODHD
- Graph expanded: 484 → 605 nodes, 2114 → 2289 edges

### SEC EDGAR Corporate Guidance
- EFTS full-text search finds filings mentioning guidance keywords
- LLM extracts direction (raised/maintained/lowered/withdrawn) with confidence
- Sector guidance breadth signal wired into assessment model (max 5% penalty)
- Smart scheduling: weekly off-season, daily during earnings season
- Feeds into entity graph + living profiles for guidance changes

### Kronos Trade Monitor
- Trade journal: per-trade decisions with full context + return backfill
- Weekly report: NAV, positions, sector P&L, anomaly detection
- Frontend page at `/monitor` in Prometheus dashboard
- Meta feedback: hit rate, assessment accuracy, risk override tracking

### Morning Catch-up
- Daemon checks at 09:00 local if overnight pipeline missed
- Forces POST_CLOSE cycle if yesterday's run didn't complete
- `--morning-catchup-hour 9` in systemd service

---

## 2026-03-26 — Conviction Model, Forward Indicators, Backtest Speedup

### Conviction Model
- Backtested 6 variants: conviction wins (Sharpe 1.28→1.36, MaxDD -14.7%→-13.9%)
- Fixed _last_members propagation bug in ConvictionPortfolioModel
- Deployed to production V12/K25 sleeve

### Forward-Looking Regime Indicators
- 13 indicators across 4 domains: credit (HY-IG spread, HY OAS, HY RoC), rates (2-10Y curve, real yields, 10Y nominal), internals (VIX, VIX term structure), macro (claims, Sahm rule, consumer sentiment, Fed balance sheet)
- All from existing FRED data (71 series in DB)
- Wired into portfolio budget adjustment (GREEN=1.0, YELLOW=0.95, ORANGE=0.80, RED=0.50)
- API endpoint + Regime page frontend panel
- DB persistence for historical tracking

### Numeric Embedding Integration
- Daily numeric window embeddings (384-dim, deterministic, no external models)
- Cross-sectional cosine distance from universe mean
- Top 10% outlier instruments get mild penalty (max 3%)
- Text/news embeddings evaluated and **rejected** — noise, not signal

### Backtest Performance
- Assessment batch pricing: 633 queries/day → 1 query (5x speedup)
- STAB transition matrix cached at module level (78s → 0s on repeat)
- C++ engine rebuilt for Python 3.14 (29 years in 14 seconds)
- Purged 411M synthetic price rows (86GB → 792MB)

### Hedge ETF Removal
- SH.US (volatility decay from daily rebalancing) and VIXY.US (contango bleed) removed
- Downside protection now options-only: crisis alpha SPY puts, VIX tail hedge calls

### Meta Learning Feedback Loop
- Tracks decision outcomes at multiple horizons
- Portfolio hit rate, assessment spread, risk override rate
- API endpoint: `/api/meta/feedback`

---

## 2026-03-25 — Apatheon v2, Entity Graph, Intelligence Engine

### Reactive Entity Graph
- In-memory typed property graph with BFS propagation
- 452 nodes (167 sovereigns, 172 persons, 24 conflicts, 38 resources, 15 orgs, 7 chokepoints, 12 trade routes, 17 deployments)
- 2020 edges with weighted relationships
- 8 propagation rules (chokepoint disruption, nation risk contagion, conflict escalation, resource disruption, sanctions cascade, leadership change, lobby influence)
- PostgreSQL persistence for deltas and property overrides

### Perspective-Taking Agents
- 3-round adversarial reasoning: entity options → adversary response → synthesis
- JSON structured output for reliable parsing
- Dynamic system prompts with entity context, tools, constraints, goals

### Scenario Tree Engine
- Branching probability trees from trigger events
- JSON-based parsing with regex fallback
- Expanded entity recognition (10 chokepoints, 15 organizations)

### Ingestion Pipeline
- GDELT news sweep (15 min), DuckDuckGo breaking news (15 min)
- RSS multi-perspective feeds (hourly), ACLED conflict events (hourly)
- EODHD prices (daily), FRED macro (daily)
- Wikidata leader refresh (weekly), Wikipedia cabinet refresh (weekly)
- EDGAR corporate guidance (daily during earnings, weekly otherwise)

### Living Entity Profiles
- Update IS the report: `process_entity_update()` mutates graph + generates narrative
- Prediction ledger: extracts predictions, tracks outcomes, computes accuracy
- Report continuity: each new SITREP sees previous brief's key findings

### Frontend
- NetworkGraph SVG component with zoom/pan, hover detail bar
- Entity Graph page, Perspective Analysis, Scenario Trees
- Nation Profiles with Intelligence tab (graph connections, living profile, perspective, scenarios)
- All 44 navigation links verified

---

## 2026-03-24 — Initial Stack, Pipeline Fixes, Test Suite

### Stack Migration
- Migrated from prometheus_v2 to new prometheus + apatheon stack
- Systemd service configured with SELinux-compatible paths
- PgBouncer on port 6432, PostgreSQL with historical + runtime DBs

### Pipeline Fixes
- ib.sleep() for fill recording (was blocking event loop)
- RiskCheckingBroker wrapping in pipeline execution
- EODHD retry mechanism (12 × 10min = 2h window)
- Stale price guard (10-day max)
- Assessment model 85% data tolerance

### Test Suite
- 218 tests across 8 modules (assessment, pipeline, risk, execution, storage, data quality, monitoring)
- All passing with 3 xfail (later fixed to 222 passed, 1 xfail)

### Sector Health Index
- 6-signal composite per sector
- Added 5-day fast momentum (triggers 1-2 weeks earlier in crashes)
- Crisis alpha strategy: SPY puts on broad sector deterioration
