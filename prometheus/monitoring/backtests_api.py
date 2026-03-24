"""Prometheus v2 – Backtests API.

Read-side endpoints for retrieving backtest runs and chart-friendly
timeseries (equity curve + per-day diagnostics).

These endpoints are intended for the C2 UI and avoid generic "SELECT *"
access patterns.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from apathis.core.database import get_db_manager
from fastapi import APIRouter, HTTPException, Path, Query
from pydantic import BaseModel, Field

router = APIRouter(prefix="/api/backtests", tags=["backtests"])


class BacktestRunRow(BaseModel):
    run_id: str
    strategy_id: str
    start_date: date
    end_date: date
    universe_id: Optional[str] = None

    # Optional human-readable label stored in backtest_runs.config_json.run_name
    run_name: Optional[str] = None

    config_json: Dict[str, Any] = Field(default_factory=dict)
    metrics_json: Optional[Dict[str, Any]] = None

    created_at: datetime


class DailyEquityRow(BaseModel):
    date: date
    equity_curve_value: float
    drawdown: Optional[float] = None
    exposure_metrics_json: Optional[Dict[str, Any]] = None


def _parse_jsonb(value: object) -> Dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            return None
        return parsed if isinstance(parsed, dict) else None
    return None


@router.get("/runs", response_model=List[BacktestRunRow])
async def list_backtest_runs(
    strategy_id: Optional[str] = Query(None, description="Filter by strategy_id"),
    book_id: Optional[str] = Query(None, description="Filter by config_json.base_prefix (allocator book_id)"),
    start_date: Optional[date] = Query(None, description="Filter by start_date (exact match)"),
    end_date: Optional[date] = Query(None, description="Filter by end_date (exact match)"),
    limit: int = Query(50, description="Row limit"),
) -> List[BacktestRunRow]:
    limit_eff = max(1, min(int(limit), 500))

    conditions: list[str] = []
    params: list[Any] = []

    if strategy_id is not None and str(strategy_id).strip() != "":
        conditions.append("strategy_id = %s")
        params.append(str(strategy_id))

    if book_id is not None and str(book_id).strip() != "":
        # Allocator runs persist cfg.base_prefix in config_json.base_prefix.
        conditions.append("config_json ->> 'base_prefix' = %s")
        params.append(str(book_id))

    if start_date is not None:
        conditions.append("start_date = %s")
        params.append(start_date)

    if end_date is not None:
        conditions.append("end_date = %s")
        params.append(end_date)

    where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""

    sql = (
        "SELECT run_id, strategy_id, start_date, end_date, universe_id, config_json, metrics_json, created_at "
        "FROM backtest_runs" + where_clause + " ORDER BY created_at DESC LIMIT %s"
    )
    params.append(limit_eff)

    db = get_db_manager()
    out: List[BacktestRunRow] = []

    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()
        finally:
            cur.close()

    for run_id, strat, start_d, end_d, univ, cfg_json, met_json, created_at in rows:
        cfg = _parse_jsonb(cfg_json) or {}

        run_name: Optional[str] = None
        rn = cfg.get("run_name")
        if isinstance(rn, str):
            rns = rn.strip()
            if rns:
                run_name = rns

        out.append(
            BacktestRunRow(
                run_id=str(run_id),
                strategy_id=str(strat),
                start_date=start_d,
                end_date=end_d,
                universe_id=str(univ) if univ is not None else None,
                run_name=run_name,
                config_json=cfg,
                metrics_json=_parse_jsonb(met_json),
                created_at=created_at,
            )
        )

    return out


@router.get("/runs/{run_id}/daily_equity", response_model=List[DailyEquityRow])
async def get_backtest_daily_equity(
    run_id: str = Path(..., description="Backtest run_id"),
    start_date: Optional[date] = Query(None, description="Optional start date filter"),
    end_date: Optional[date] = Query(None, description="Optional end date filter"),
) -> List[DailyEquityRow]:
    conditions: list[str] = ["run_id = %s"]
    params: list[Any] = [str(run_id)]

    if start_date is not None:
        conditions.append("date >= %s")
        params.append(start_date)
    if end_date is not None:
        conditions.append("date <= %s")
        params.append(end_date)

    where_clause = " WHERE " + " AND ".join(conditions)

    sql = (
        "SELECT date, equity_curve_value, drawdown, exposure_metrics_json "
        "FROM backtest_daily_equity" + where_clause + " ORDER BY date ASC"
    )

    db = get_db_manager()

    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()
        finally:
            cur.close()

    if not rows:
        raise HTTPException(status_code=404, detail=f"No backtest_daily_equity rows found for run_id={run_id!r}")

    out: List[DailyEquityRow] = []
    for d, eq, dd, exposure in rows:
        out.append(
            DailyEquityRow(
                date=d,
                equity_curve_value=float(eq or 0.0),
                drawdown=float(dd) if dd is not None else None,
                exposure_metrics_json=_parse_jsonb(exposure),
            )
        )

    return out
