# Prometheus C2 Backend APIs

This directory contains the FastAPI backend that serves all APIs for the Prometheus C2 UI (Godot client).

## Architecture

The backend is organized into several API modules:

- **`api.py`** - Monitoring/status endpoints (system overview, pipeline status, regime, stability, fragility, portfolio, risk)
- **`visualization_api.py`** - ANT_HILL 3D visualization data (scenes, traces, DB tables, embedding spaces)
- **`control_api.py`** - Write operations (backtests, synthetic datasets, DAG scheduling, config changes)
- **`meta_api.py`** - Kronos Chat, Geo data, engine configs and performance
- **`app.py`** - FastAPI application entry point that wires everything together

## Quick Start

### 1. Install Dependencies

```bash
# From project root
pip install -e .
```

### 2. Run the Server

```bash
# Development mode with auto-reload
uvicorn prometheus.monitoring.app:app --reload --host 0.0.0.0 --port 8000
```

### 3. Access API Documentation

Open your browser to:
- **Swagger UI**: http://localhost:8000/api/docs
- **ReDoc**: http://localhost:8000/api/redoc

## API Endpoints

### Monitoring/Status (`/api/status`)
- `GET /api/status/overview` - Global system KPIs
- `GET /api/status/pipeline?market_id=US_EQ` - Per-market DAG status
- `GET /api/status/regime?region=US` - Regime history
- `GET /api/status/stability?region=US` - Stability metrics
- `GET /api/status/fragility` - Fragility entities table
- `GET /api/status/fragility/{entity_id}` - Entity details
- `GET /api/status/assessment?strategy_id=MAIN` - Assessment output
- `GET /api/status/universe?strategy_id=MAIN` - Universe membership
- `GET /api/status/portfolio?portfolio_id=MAIN` - Portfolio positions
- `GET /api/status/portfolio_risk?portfolio_id=MAIN` - Portfolio risk metrics

### Visualization (`/api`)
- `GET /api/scenes` - List available ANT_HILL scenes
- `GET /api/scene/{view_id}` - Scene graph for 3D rendering
- `GET /api/traces` - List execution traces
- `GET /api/traces/{trace_id}` - Trace events
- `GET /api/db/runtime/{table}` - Runtime DB snapshot
- `GET /api/db/historical/{table}` - Historical DB snapshot
- `GET /api/embedding_space/{space_id}` - Embedding vectors

### Control (`/api/control`)
- `POST /api/control/run_backtest` - Submit backtest job
- `POST /api/control/create_synthetic_dataset` - Create synthetic data
- `POST /api/control/schedule_dag` - Trigger DAG execution
- `POST /api/control/apply_config_change` - Apply config change
- `GET /api/control/jobs/{job_id}` - Query job status

### Kronos Chat (`/api/kronos`)
- `POST /api/kronos/chat` - Chat with Kronos meta-orchestrator

### Geo (`/api/geo`)
- `GET /api/geo/countries` - Country-level status for map
- `GET /api/geo/country/{country_code}` - Country details

### Meta (`/api/meta`)
- `GET /api/meta/configs` - Engine configurations
- `GET /api/meta/performance?engine_name=regime` - Engine performance

## Current State

All endpoints currently return **mock/template data** to enable UI development. They will be progressively wired to:
- Real engine implementations (regime, stability, fragility, etc.)
- Runtime and historical databases
- DAG orchestrator
- Job execution framework

## Testing

Test individual endpoints with curl:

```bash
# System overview
curl http://localhost:8000/api/status/overview

# Scene list
curl http://localhost:8000/api/scenes

# Submit backtest
curl -X POST http://localhost:8000/api/control/run_backtest \
  -H "Content-Type: application/json" \
  -d '{
    "strategy_id": "MAIN",
    "start_date": "2024-01-01",
    "end_date": "2024-11-28",
    "market_ids": ["US_EQ"]
  }'

# Kronos chat
curl -X POST http://localhost:8000/api/kronos/chat \
  -H "Content-Type: application/json" \
  -d '{
    "question": "Should I run a backtest?"
  }'
```

## Next Steps

1. **Phase 2**: Bootstrap Godot C2 project structure
2. **Phase 3**: Build UI shell and panel framework in Godot
3. **Phase 4**: Wire Godot panels to these APIs
4. **Phase 5**: Implement control plane in UI
5. **Later**: Replace mock data with real engine/DB integration
