#!/usr/bin/env bash
set -u

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
project_dir="$(cd "${script_dir}/.." && pwd)"

"${script_dir}/daily_workflow_template.sh" \
  --project "${project_dir}" \
  --name "prometheus-daily-gates" \
  --note "Compile, lint, type, test, frontend build, and operational validators." \
  --step "python -m compileall -q \"${project_dir}/prometheus\"" \
  --step "python -m ruff check \"${project_dir}/prometheus/scripts/show/show_alembic_status.py\" \"${project_dir}/prometheus/scripts/validate/validate_layer0.py\" \"${project_dir}/prometheus/scripts/validate/validate_hedge_etf_data.py\" \"${project_dir}/tests/test_smoke.py\" \"${project_dir}/tests/test_show_alembic_status.py\"" \
  --step "python -m mypy --ignore-missing-imports --follow-imports skip \"${project_dir}/prometheus/scripts/show/show_alembic_status.py\" \"${project_dir}/prometheus/scripts/validate/validate_layer0.py\" \"${project_dir}/prometheus/scripts/validate/validate_hedge_etf_data.py\" \"${project_dir}/tests/test_smoke.py\" \"${project_dir}/tests/test_show_alembic_status.py\"" \
  --step "python -m pytest -q \"${project_dir}/tests\"" \
  --step "npm --prefix \"${project_dir}/prometheus_web\" ci --legacy-peer-deps" \
  --step "npm --prefix \"${project_dir}/prometheus_web\" run build" \
  --step "python -m prometheus.scripts.show.show_alembic_status" \
  --step "python -m prometheus.scripts.validate.validate_layer0" \
  --step "python -m prometheus.scripts.validate.validate_hedge_etf_data --book-id US_EQ_HEDGE_ETF"
