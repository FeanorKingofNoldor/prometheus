# Prometheus
Prometheus is the trading and orchestration layer that consumes Apathis intelligence outputs and executes multi-market pipeline workflows.

## Components
- Backend services and orchestration daemon under `prometheus/`
- Web UI under `prometheus_web/`
- Alembic migrations under `migrations/`

## Development quick start
```bash
pip install -e .[dev]
npm --prefix prometheus_web ci
npm --prefix prometheus_web run build
```
