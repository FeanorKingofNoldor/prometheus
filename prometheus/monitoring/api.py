"""Prometheus v2 – Monitoring Status API.

This module provides REST endpoints for the Prometheus C2 UI to query
system status, engine states, and real-time pipeline information.

Currently returns mock/template data to enable UI development. Will be
progressively wired to real engines and runtime DB as they mature.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, Optional

from apathis.core.database import get_db_manager
from apathis.core.logging import get_logger
from apathis.core.market_state import MarketState, get_market_state, get_next_state_transition
from apathis.core.markets import MARKETS_BY_REGION
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from prometheus.orchestration.dag import build_market_dag

router = APIRouter(prefix="/api/status", tags=["monitoring"])
logger = get_logger(__name__)


# ============================================================================
# Response Models
# ============================================================================


class SystemOverview(BaseModel):
    """Global system KPIs and alerts."""

    timestamp: datetime = Field(default_factory=datetime.now)
    pnl_today: float = 0.0
    pnl_mtd: float = 0.0
    pnl_ytd: float = 0.0
    max_drawdown: float = 0.0
    net_exposure: float = 0.0
    gross_exposure: float = 0.0
    leverage: float = 0.0
    global_stability_index: float = 0.85
    regimes: List[Dict[str, Any]] = Field(default_factory=list)
    alerts: List[Dict[str, Any]] = Field(default_factory=list)


class PipelineStatus(BaseModel):
    """Per-market pipeline and DAG status."""

    market_id: str
    as_of_date: Optional[date] = None
    dag_id: Optional[str] = None

    market_state: str = "UNKNOWN"
    next_transition_state: Optional[str] = None
    next_transition_time: Optional[datetime] = None

    jobs: List[Dict[str, Any]] = Field(default_factory=list)


class RegimeStatus(BaseModel):
    """Regime history and current state."""

    region: str
    as_of_date: Optional[date] = None
    current_regime: str = "STABLE_EXPANSION"
    confidence: float = 0.82
    history: List[Dict[str, Any]] = Field(default_factory=list)


class StabilityStatus(BaseModel):
    """Stability metrics over time."""

    region: str
    as_of_date: Optional[date] = None
    current_index: float = 0.85
    liquidity_component: float = 0.88
    volatility_component: float = 0.82
    contagion_component: float = 0.85
    history: List[Dict[str, Any]] = Field(default_factory=list)


class FragilityStatus(BaseModel):
    """Fragility entities table."""

    region: str
    entity_type: str
    as_of_date: Optional[date] = None
    entities: List[Dict[str, Any]] = Field(default_factory=list)


class FragilityDetail(BaseModel):
    """Detailed fragility info for a single entity."""

    entity_id: str
    entity_type: str
    soft_target_score: float
    fragility_alpha: float
    fragility_class: str
    history: List[Dict[str, Any]] = Field(default_factory=list)
    scenarios: List[Dict[str, Any]] = Field(default_factory=list)
    positions: List[Dict[str, Any]] = Field(default_factory=list)


class AssessmentStatus(BaseModel):
    """Assessment engine output for a strategy."""

    strategy_id: str
    as_of_date: Optional[date] = None
    instruments: List[Dict[str, Any]] = Field(default_factory=list)


class UniverseStatus(BaseModel):
    """Universe membership and scores."""

    strategy_id: str
    as_of_date: Optional[date] = None
    candidates: List[Dict[str, Any]] = Field(default_factory=list)


class PortfolioSummary(BaseModel):
    """Summary of a single portfolio for the portfolio picker."""

    portfolio_id: str
    mode: str = "BACKTEST"
    latest_date: Optional[date] = None
    num_positions: int = 0
    total_market_value: float = 0.0
    net_exposure: Optional[float] = None
    gross_exposure: Optional[float] = None


class PortfolioListResponse(BaseModel):
    """All available portfolios."""

    portfolios: List[PortfolioSummary] = Field(default_factory=list)


class PortfolioStatus(BaseModel):
    """Current portfolio state and P&L."""

    portfolio_id: str
    mode: str = "BACKTEST"
    as_of_date: Optional[date] = None
    net_liquidation_value: float = 0.0
    total_cash: float = 0.0
    positions: List[Dict[str, Any]] = Field(default_factory=list)
    pnl: Dict[str, float] = Field(default_factory=dict)
    exposures: Dict[str, Any] = Field(default_factory=dict)


class PortfolioRiskStatus(BaseModel):
    """Portfolio risk metrics and scenarios."""

    portfolio_id: str
    as_of_date: Optional[date] = None
    volatility: float = 0.0
    var_95: float = 0.0
    expected_shortfall: float = 0.0
    max_drawdown: float = 0.0
    scenarios: List[Dict[str, Any]] = Field(default_factory=list)


class ExecutionStatus(BaseModel):
    """Recent execution activity for a portfolio.

    This model intentionally uses untyped dict payloads for orders/fills/
    positions because the database schemas evolve frequently during
    research. The UI only requires a small, stable subset of keys.
    """

    portfolio_id: str
    mode: Optional[str] = None
    orders: List[Dict[str, Any]] = Field(default_factory=list)
    fills: List[Dict[str, Any]] = Field(default_factory=list)
    positions: List[Dict[str, Any]] = Field(default_factory=list)


class ExecutionDecisionResponse(BaseModel):
    """Recent EXECUTION engine decision (order generation + constraints)."""

    decision_id: str
    market_id: Optional[str] = None
    as_of_date: date
    created_at: Optional[str] = None

    portfolio_id: Optional[str] = None
    portfolio_decision_id: Optional[str] = None

    order_count: int = 0
    orders_preview: List[Dict[str, Any]] = Field(default_factory=list)

    plan_summary: Dict[str, Any] = Field(default_factory=dict)
    execution_policy: Dict[str, Any] = Field(default_factory=dict)


class RiskActionRow(BaseModel):
    """Single risk-action entry taken during risk constraints application."""

    created_at: datetime
    instrument_id: Optional[str] = None
    decision_id: Optional[str] = None
    action_type: str
    original_weight: Optional[float] = None
    adjusted_weight: Optional[float] = None
    reason: Optional[str] = None


class RiskActionsStatus(BaseModel):
    """Recent risk actions taken for a strategy."""

    strategy_id: str
    actions: List[RiskActionRow] = Field(default_factory=list)


# ============================================================================
# Endpoints
# ============================================================================


@router.get("/overview", response_model=SystemOverview)
async def get_status_overview() -> SystemOverview:
    logger.debug("[api/overview] Fetching system overview")
    """Return global system KPIs and current state.

    P&L figures are derived from NLV changes in ``positions_snapshots``
    for the primary PAPER/LIVE portfolios.  Exposure metrics come from
    ``portfolio_risk_reports``.  Global stability from ``stability_vectors``.
    """

    db_manager = get_db_manager()

    # Aggregate exposure metrics from the latest portfolio_risk_reports
    # snapshot (if any).
    net_exposure = 0.0
    gross_exposure = 0.0
    leverage = 0.0

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT MAX(as_of_date) FROM portfolio_risk_reports")
            row = cursor.fetchone()
            latest_date = row[0] if row is not None else None
            if latest_date is not None:
                cursor.execute(
                    """
                    SELECT AVG(net_exposure), AVG(gross_exposure), AVG(leverage)
                    FROM portfolio_risk_reports
                    WHERE as_of_date = %s
                    """,
                    (latest_date,),
                )
                exp_row = cursor.fetchone()
                if exp_row is not None:
                    net_exposure = float(exp_row[0] or 0.0)
                    gross_exposure = float(exp_row[1] or 0.0)
                    leverage = float(exp_row[2] or 0.0)
        finally:
            cursor.close()

    # Global stability index: 1 - normalised mean overall_score from
    # stability_vectors (0..100) for the most recent date.
    global_stability_index = 0.0
    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT MAX(as_of_date) FROM stability_vectors")
            row = cursor.fetchone()
            stab_date = row[0] if row is not None else None
            if stab_date is not None:
                cursor.execute(
                    """
                    SELECT AVG(overall_score)
                    FROM stability_vectors
                    WHERE as_of_date = %s
                    """,
                    (stab_date,),
                )
                avg_row = cursor.fetchone()
                if avg_row is not None and avg_row[0] is not None:
                    mean_score = float(avg_row[0])
                    global_stability_index = max(0.0, min(1.0, 1.0 - mean_score / 100.0))
        finally:
            cursor.close()

    # Latest regime snapshot per core region.
    regimes: List[Dict[str, Any]] = []
    regions = ("US", "EU", "ASIA")
    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            for region in regions:
                cursor.execute(
                    """
                    SELECT regime_label, confidence
                    FROM regimes
                    WHERE region = %s
                    ORDER BY as_of_date DESC
                    LIMIT 1
                    """,
                    (region,),
                )
                row = cursor.fetchone()
                if row is None:
                    continue
                regimes.append(
                    {
                        "region": region,
                        "regime_label": str(row[0]),
                        "confidence": float(row[1] or 0.0),
                    }
                )
        finally:
            cursor.close()

    # P&L: NLV changes for the primary paper/live portfolio.
    # Uses positions_snapshots; picks the latest timestamp per day,
    # then computes day-over-day, month-over-month, and year-over-day diffs.
    pnl_today = 0.0
    pnl_mtd = 0.0
    pnl_ytd = 0.0
    today = date.today()
    month_start = today.replace(day=1)
    year_start = today.replace(month=1, day=1)

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                WITH latest_per_day AS (
                    SELECT DISTINCT ON (as_of_date)
                        as_of_date,
                        SUM(market_value) OVER (
                            PARTITION BY as_of_date, timestamp
                        ) AS nlv
                    FROM positions_snapshots
                    WHERE mode IN ('PAPER', 'LIVE')
                      AND portfolio_id = 'IBKR_PAPER'
                    ORDER BY as_of_date, timestamp DESC
                )
                SELECT
                    as_of_date,
                    nlv
                FROM latest_per_day
                WHERE as_of_date >= %s
                ORDER BY as_of_date
                """,
                (year_start,),
            )
            nlv_by_date: Dict[date, float] = {
                row[0]: float(row[1] or 0.0) for row in cursor.fetchall()
            }
        finally:
            cursor.close()

    nlv_today = nlv_by_date.get(today, 0.0)
    max_drawdown = 0.0

    # Detect capital-flow days (deposits/withdrawals) where NLV jumps
    # by more than 15% in a single day.  These are not market returns
    # and must be excluded from P&L and drawdown calculations.
    _FLOW_THRESHOLD = 0.15
    sorted_dates = sorted(nlv_by_date)
    flow_dates: set = set()
    for i in range(1, len(sorted_dates)):
        prev_nlv = nlv_by_date[sorted_dates[i - 1]]
        curr_nlv = nlv_by_date[sorted_dates[i]]
        if prev_nlv > 0 and abs(curr_nlv - prev_nlv) / prev_nlv > _FLOW_THRESHOLD:
            flow_dates.add(sorted_dates[i])

    if nlv_today and nlv_by_date:
        # today vs previous trading day (skip if today is a flow day)
        prev_dates = [d for d in sorted_dates if d < today]
        if prev_dates and today not in flow_dates:
            pnl_today = nlv_today - nlv_by_date[prev_dates[-1]]

        # month-to-date: sum of non-flow daily diffs since month start
        mtd_dates = [d for d in sorted_dates if d >= month_start]
        for i, d in enumerate(mtd_dates):
            if d in flow_dates:
                continue
            idx = sorted_dates.index(d)
            if idx > 0:
                pnl_mtd += nlv_by_date[d] - nlv_by_date[sorted_dates[idx - 1]]

        # year-to-date: sum of non-flow daily diffs since year start
        for i, d in enumerate(sorted_dates):
            if d in flow_dates:
                continue
            if i > 0:
                pnl_ytd += nlv_by_date[d] - nlv_by_date[sorted_dates[i - 1]]

        # Max drawdown: largest peak-to-trough decline, ignoring flow days.
        # Build a flow-adjusted NLV series by accumulating only market returns.
        clean_nlvs: list = []
        for i, d in enumerate(sorted_dates):
            if i == 0:
                clean_nlvs.append(nlv_by_date[d])
            elif d in flow_dates:
                # Carry forward previous clean NLV (rebase after flow)
                clean_nlvs.append(clean_nlvs[-1])
            else:
                prev_raw = nlv_by_date[sorted_dates[i - 1]]
                curr_raw = nlv_by_date[d]
                if prev_raw > 0:
                    daily_ret = (curr_raw - prev_raw) / prev_raw
                    clean_nlvs.append(clean_nlvs[-1] * (1.0 + daily_ret))
                else:
                    clean_nlvs.append(clean_nlvs[-1])

        if len(clean_nlvs) >= 2:
            peak = clean_nlvs[0]
            for v in clean_nlvs:
                if v > peak:
                    peak = v
                dd = (peak - v) / peak
                if dd > max_drawdown:
                    max_drawdown = dd

    return SystemOverview(
        pnl_today=pnl_today,
        pnl_mtd=pnl_mtd,
        pnl_ytd=pnl_ytd,
        max_drawdown=max_drawdown,
        net_exposure=net_exposure,
        gross_exposure=gross_exposure,
        leverage=leverage,
        global_stability_index=global_stability_index,
        regimes=regimes,
        alerts=[],  # Detailed alerting will be added in a later iteration.
    )


@router.get("/pipeline", response_model=PipelineStatus)
async def get_pipeline_status(
    market_id: str = Query(..., description="Market identifier (e.g. US_EQ)"),
    as_of_date: Optional[date] = Query(
        None,
        description="DAG date. Defaults to today; matches MarketAwareDaemon default.",
    ),
) -> PipelineStatus:
    """Return per-market pipeline and DAG job status.

    Used by the Live System panel to show current job states and upcoming
    scheduled runs.

    Implementation notes:
    - The logical schedule comes from the code-defined DAG in
      `prometheus/orchestration/dag.py`.
    - Actual executions are read from `runtime_db.job_executions` when the
      orchestration daemon is running.
    """

    def _dt_to_iso_z(dt: Optional[datetime]) -> Optional[str]:
        if dt is None:
            return None
        # Runtime DB stores naive timestamps; treat them as UTC.
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

    def _table_exists(table_name: str) -> bool:
        db = get_db_manager()
        with db.get_runtime_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.tables
                        WHERE table_schema = 'public' AND table_name = %s
                    )
                    """,
                    (table_name,),
                )
                (exists,) = cur.fetchone()
            finally:
                cur.close()
        return bool(exists)

    def _next_time_for_state(market: str, desired: MarketState, now_utc: datetime) -> Optional[datetime]:
        try:
            cur_state = get_market_state(market, now_utc)
        except Exception:
            return None

        if cur_state == desired:
            return now_utc

        t = now_utc
        for _ in range(8):
            try:
                next_state, when = get_next_state_transition(market, t)
            except Exception:
                return None
            if next_state == desired:
                return when
            t = when
        return None

    # Snapshot time for state transitions.
    now = datetime.now(timezone.utc)

    try:
        current_state = get_market_state(market_id, now)
        next_state, next_time = get_next_state_transition(market_id, now)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Unknown or unsupported market_id={market_id!r}: {exc}")

    eff_date = as_of_date or date.today()

    # Build the schedule DAG for this market/date.
    try:
        dag = build_market_dag(str(market_id), eff_date)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Failed to build DAG for market_id={market_id!r} as_of_date={eff_date}: {exc}")

    # NOTE: MarketAwareDaemon persists job_executions with dag_id = f"{market_id}_{as_of_date}".
    dag_id = f"{market_id}_{eff_date.isoformat()}"

    exec_by_job_id: Dict[str, Dict[str, Any]] = {}

    if _table_exists("job_executions"):
        db = get_db_manager()
        with db.get_runtime_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    SELECT DISTINCT ON (job_id)
                        execution_id,
                        job_id,
                        job_type,
                        status,
                        started_at,
                        completed_at,
                        attempt_number,
                        error_message,
                        created_at,
                        updated_at
                    FROM job_executions
                    WHERE dag_id = %s
                    ORDER BY job_id, created_at DESC
                    """,
                    (dag_id,),
                )
                rows = cur.fetchall()
            finally:
                cur.close()

        for (
            execution_id,
            job_id_db,
            job_type_db,
            status_db,
            started_at,
            completed_at,
            attempt_number,
            error_message,
            created_at,
            updated_at,
        ) in rows:
            exec_by_job_id[str(job_id_db)] = {
                "execution_id": str(execution_id),
                "job_id": str(job_id_db),
                "job_type": str(job_type_db),
                "status": str(status_db),
                "started_at": started_at,
                "completed_at": completed_at,
                "attempt_number": int(attempt_number or 0),
                "error_message": str(error_message) if error_message is not None else None,
                "created_at": created_at,
                "updated_at": updated_at,
            }

    # Build job list in a stable, priority-first order.
    jobs_sorted = sorted(
        dag.jobs.values(),
        key=lambda j: (int(j.priority.value), str(j.job_type)),
    )

    jobs_out: List[Dict[str, Any]] = []

    for job in jobs_sorted:
        rec = exec_by_job_id.get(str(job.job_id))

        status = rec.get("status") if rec else "NOT_STARTED"
        started_at = rec.get("started_at") if rec else None
        completed_at = rec.get("completed_at") if rec else None

        latency_ms = None
        if isinstance(started_at, datetime) and isinstance(completed_at, datetime):
            latency_ms = int(max(0.0, (completed_at - started_at).total_seconds()) * 1000.0)

        next_run = None
        if job.required_state is not None:
            when = _next_time_for_state(str(market_id), job.required_state, now)
            next_run = _dt_to_iso_z(when)

        jobs_out.append(
            {
                "job_name": str(job.job_type),
                "job_id": str(job.job_id),
                "required_state": job.required_state.value if job.required_state is not None else None,
                "dependencies": list(job.dependencies),
                "priority": int(job.priority.value),
                "timeout_seconds": int(job.timeout_seconds),
                # Execution record
                "last_run_status": status,
                "last_run_time": _dt_to_iso_z(completed_at or started_at),
                "latency_ms": latency_ms,
                "slo_ms": int(job.timeout_seconds) * 1000,
                "next_run": next_run,
                "attempt_number": rec.get("attempt_number") if rec else None,
                "error_message": rec.get("error_message") if rec else None,
            }
        )

    return PipelineStatus(
        market_id=str(market_id),
        as_of_date=eff_date,
        dag_id=dag_id,
        market_state=current_state.value,
        next_transition_state=next_state.value,
        next_transition_time=next_time,
        jobs=jobs_out,
    )


@router.get("/pipelines", response_model=List[PipelineStatus])
async def get_pipelines_status(
    market_ids: Optional[str] = Query(
        None,
        description="Comma-separated market identifiers. Defaults to all known markets.",
    ),
    as_of_date: Optional[date] = Query(
        None,
        description="DAG date. Defaults to today; matches MarketAwareDaemon default.",
    ),
) -> List[PipelineStatus]:
    """Return pipeline status for multiple markets.

    This is a convenience endpoint for the Portfolio Dashboard to show
    upcoming runs across markets without making N separate HTTP requests.

    Errors for individual markets are logged and skipped.
    """

    if market_ids is not None and str(market_ids).strip() != "":
        ids = [m.strip() for m in str(market_ids).split(",") if m.strip()]
    else:
        # Flatten the region->markets map into a unique market list.
        ids_set = set()
        for ms in MARKETS_BY_REGION.values():
            for m in ms:
                ids_set.add(str(m))
        ids = sorted(ids_set)

    out: List[PipelineStatus] = []
    for mid in ids:
        try:
            out.append(await get_pipeline_status(market_id=str(mid), as_of_date=as_of_date))
        except HTTPException as exc:
            logger.warning("Skipping pipeline status for market_id=%s: %s", mid, exc.detail)
            continue
        except Exception:
            logger.exception("Skipping pipeline status for market_id=%s due to unexpected error", mid)
            continue

    return out


@router.get("/regime", response_model=RegimeStatus)
async def get_regime_status(
    region: str = Query("US", description="Region identifier"),
    as_of_date: Optional[date] = Query(None, description="As-of date for historical view"),
) -> RegimeStatus:
    """Return regime state and history for a region from the ``regimes`` table."""

    db_manager = get_db_manager()
    region_norm = region.upper()

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            if as_of_date is None:
                cursor.execute(
                    "SELECT MAX(as_of_date) FROM regimes WHERE region = %s",
                    (region_norm,),
                )
                row = cursor.fetchone()
                latest_date = row[0] if row is not None else None
                if latest_date is None:
                    return RegimeStatus(region=region_norm, as_of_date=None, current_regime="UNKNOWN", confidence=0.0, history=[])
                end_date = latest_date
            else:
                end_date = as_of_date

            start_date = end_date - timedelta(days=90)

            cursor.execute(
                """
                SELECT as_of_date, regime_label, confidence
                FROM regimes
                WHERE region = %s
                  AND as_of_date BETWEEN %s AND %s
                ORDER BY as_of_date ASC
                """,
                (region_norm, start_date, end_date),
            )
            rows = cursor.fetchall()
        finally:
            cursor.close()

    if not rows:
        return RegimeStatus(
            region=region_norm,
            as_of_date=end_date,
            current_regime="UNKNOWN",
            confidence=0.0,
            history=[],
        )

    history: List[Dict[str, Any]] = []
    for as_of_db, label_db, conf_db in rows:
        history.append(
            {
                "date": as_of_db.isoformat(),
                "regime": str(label_db),
                "confidence": float(conf_db or 0.0),
            }
        )

    last_date, last_label, last_conf = rows[-1]

    return RegimeStatus(
        region=region_norm,
        as_of_date=end_date,
        current_regime=str(last_label),
        confidence=float(last_conf or 0.0),
        history=history,
    )


@router.get("/stability", response_model=StabilityStatus)
async def get_stability_status(
    region: str = Query("US", description="Region identifier"),
    as_of_date: Optional[date] = Query(None, description="As-of date for historical view"),
) -> StabilityStatus:
    """Return stability metrics and history for a region.

    The current implementation aggregates ``stability_vectors`` for
    ``entity_type='INSTRUMENT'`` over instruments whose ``market_id``
    maps to the requested region (via ``MARKETS_BY_REGION``). Metrics are
    based on the mean ``overall_score`` and component scores from the
    basic STAB model.
    """

    db_manager = get_db_manager()
    region_norm = region.upper()

    markets = MARKETS_BY_REGION.get(region_norm)

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            if as_of_date is None:
                cursor.execute("SELECT MAX(as_of_date) FROM stability_vectors")
                row = cursor.fetchone()
                latest_date = row[0] if row is not None else None
                if latest_date is None:
                    return StabilityStatus(
                        region=region_norm,
                        as_of_date=None,
                        current_index=0.0,
                        liquidity_component=0.0,
                        volatility_component=0.0,
                        contagion_component=0.0,
                        history=[],
                    )
                end_date = latest_date
            else:
                end_date = as_of_date

            start_date = end_date - timedelta(days=90)

            if markets:
                cursor.execute(
                    """
                    SELECT sv.as_of_date,
                           sv.overall_score,
                           sv.vector_components
                    FROM stability_vectors AS sv
                    JOIN instruments AS i ON i.instrument_id = sv.entity_id
                    WHERE sv.entity_type = 'INSTRUMENT'
                      AND i.market_id = ANY(%s)
                      AND sv.as_of_date BETWEEN %s AND %s
                    """,
                    (list(markets), start_date, end_date),
                )
            else:
                cursor.execute(
                    """
                    SELECT as_of_date,
                           overall_score,
                           vector_components
                    FROM stability_vectors
                    WHERE entity_type = 'INSTRUMENT'
                      AND as_of_date BETWEEN %s AND %s
                    """,
                    (start_date, end_date),
                )

            rows = cursor.fetchall()
        finally:
            cursor.close()

    if not rows:
        return StabilityStatus(
            region=region_norm,
            as_of_date=end_date,
            current_index=0.0,
            liquidity_component=0.0,
            volatility_component=0.0,
            contagion_component=0.0,
            history=[],
        )

    # Aggregate by date.
    by_date: Dict[date, Dict[str, Any]] = {}
    for as_of_db, overall_score, components in rows:
        bucket = by_date.setdefault(
            as_of_db,
            {
                "overall_sum": 0.0,
                "count": 0,
                "vol_sum": 0.0,
                "dd_sum": 0.0,
            },
        )
        bucket["overall_sum"] += float(overall_score or 0.0)
        bucket["count"] += 1
        comp = components or {}
        try:
            bucket["vol_sum"] += float(comp.get("vol_score", 0.0) or 0.0)
        except Exception:
            pass
        try:
            bucket["dd_sum"] += float(comp.get("dd_score", 0.0) or 0.0)
        except Exception:
            pass

    # Build time series sorted by date.
    dates_sorted = sorted(by_date.keys())
    history: List[Dict[str, Any]] = []
    current_index = 0.0
    liquidity_component = 0.0
    volatility_component = 0.0
    contagion_component = 0.0

    for d in dates_sorted:
        bucket = by_date[d]
        count = max(bucket["count"], 1)
        mean_overall = bucket["overall_sum"] / count
        mean_vol = bucket["vol_sum"] / count
        mean_dd = bucket["dd_sum"] / count

        idx = max(0.0, min(1.0, 1.0 - mean_overall / 100.0))
        vol_comp = max(0.0, min(1.0, 1.0 - mean_vol / 100.0))
        dd_comp = max(0.0, min(1.0, 1.0 - mean_dd / 100.0))

        history.append(
            {
                "date": d.isoformat(),
                "index": idx,
                "liquidity": 1.0,  # Placeholder until a dedicated liquidity metric exists.
                "volatility": vol_comp,
                "contagion": dd_comp,
            }
        )

        if d == dates_sorted[-1]:
            current_index = idx
            liquidity_component = 1.0
            volatility_component = vol_comp
            contagion_component = dd_comp

    return StabilityStatus(
        region=region_norm,
        as_of_date=end_date,
        current_index=current_index,
        liquidity_component=liquidity_component,
        volatility_component=volatility_component,
        contagion_component=contagion_component,
        history=history,
    )


@router.get("/fragility", response_model=FragilityStatus)
async def get_fragility_status(
    region: str = Query("GLOBAL", description="Region filter"),
    entity_type: str = Query("ANY", description="Entity type filter"),
    as_of_date: Optional[date] = Query(None, description="As-of date"),
) -> FragilityStatus:
    """Return fragility entities table derived from ``fragility_measures``.

    For this iteration we focus on instrument-level fragility
    (``entity_type='INSTRUMENT'``). Region filters are applied by mapping
    regions to market_ids via ``MARKETS_BY_REGION`` and joining to the
    ``instruments`` table.
    """

    db_manager = get_db_manager()
    region_norm = region.upper()

    # We currently expose only instrument-level fragility. Other
    # higher-level entity types (COMPANY, SOVEREIGN, etc.) can be added
    # later by aggregating over issuers/sectors.
    _ = entity_type  # kept for API compatibility; unused for now.

    markets = None
    if region_norm != "GLOBAL":
        markets = MARKETS_BY_REGION.get(region_norm)

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            if as_of_date is None:
                cursor.execute(
                    "SELECT MAX(as_of_date) FROM fragility_measures WHERE entity_type = 'INSTRUMENT'",
                )
                row = cursor.fetchone()
                eff_date = row[0] if row is not None else None
                if eff_date is None:
                    return FragilityStatus(
                        region=region_norm,
                        entity_type="INSTRUMENT",
                        as_of_date=None,
                        entities=[],
                    )
            else:
                # Use the most recent fragility snapshot up to the
                # requested as_of_date.
                cursor.execute(
                    """
                    SELECT MAX(as_of_date)
                    FROM fragility_measures
                    WHERE entity_type = 'INSTRUMENT'
                      AND as_of_date <= %s
                    """,
                    (as_of_date,),
                )
                row = cursor.fetchone()
                eff_date = row[0] if row is not None else None
                if eff_date is None:
                    return FragilityStatus(
                        region=region_norm,
                        entity_type="INSTRUMENT",
                        as_of_date=as_of_date,
                        entities=[],
                    )

            # Get previous snapshot date for trend computation
            cursor.execute(
                """
                SELECT DISTINCT as_of_date
                FROM fragility_measures
                WHERE entity_type = 'INSTRUMENT' AND as_of_date < %s
                ORDER BY as_of_date DESC
                LIMIT 1
                """,
                (eff_date,),
            )
            prev_row = cursor.fetchone()
            prev_date = prev_row[0] if prev_row else None

            prev_scores: Dict[str, float] = {}
            if prev_date is not None:
                cursor.execute(
                    "SELECT entity_id, fragility_score FROM fragility_measures WHERE entity_type = 'INSTRUMENT' AND as_of_date = %s",
                    (prev_date,),
                )
                for eid, fscore in cursor.fetchall():
                    try:
                        prev_scores[str(eid)] = float(fscore) if fscore is not None else 0.0
                    except Exception:
                        pass

            if markets:
                cursor.execute(
                    """
                    SELECT fm.entity_id,
                           fm.fragility_score,
                           fm.metadata,
                           st.soft_target_score,
                           st.soft_target_class,
                           iss.name
                    FROM fragility_measures AS fm
                    JOIN instruments AS i
                      ON i.instrument_id = fm.entity_id
                    JOIN issuers AS iss
                      ON iss.issuer_id = i.issuer_id
                    LEFT JOIN soft_target_classes AS st
                      ON st.entity_type = fm.entity_type
                     AND st.entity_id = fm.entity_id
                     AND st.as_of_date = fm.as_of_date
                    WHERE fm.entity_type = 'INSTRUMENT'
                      AND fm.as_of_date = %s
                      AND i.market_id = ANY(%s)
                    ORDER BY fm.fragility_score DESC
                    LIMIT 200
                    """,
                    (eff_date, list(markets)),
                )
            else:
                cursor.execute(
                    """
                    SELECT fm.entity_id,
                           fm.fragility_score,
                           fm.metadata,
                           st.soft_target_score,
                           st.soft_target_class,
                           iss.name
                    FROM fragility_measures AS fm
                    JOIN instruments AS i
                      ON i.instrument_id = fm.entity_id
                    JOIN issuers AS iss
                      ON iss.issuer_id = i.issuer_id
                    LEFT JOIN soft_target_classes AS st
                      ON st.entity_type = fm.entity_type
                     AND st.entity_id = fm.entity_id
                     AND st.as_of_date = fm.as_of_date
                    WHERE fm.entity_type = 'INSTRUMENT'
                      AND fm.as_of_date = %s
                    ORDER BY fm.fragility_score DESC
                    LIMIT 200
                    """,
                    (eff_date,),
                )

            rows = cursor.fetchall()
        finally:
            cursor.close()

    entities: List[Dict[str, Any]] = []
    for inst_id, frag_score, metadata, soft_score, soft_class, issuer_name in rows:
        meta = metadata or {}
        class_str = soft_class or meta.get("class_label") or "NONE"
        try:
            frag_val = float(frag_score or 0.0)
        except Exception:
            frag_val = 0.0
        try:
            soft_val = float(soft_score or 0.0) / 100.0 if soft_score is not None else 0.0
        except Exception:
            soft_val = 0.0

        # Compute trend from previous snapshot
        prev = prev_scores.get(str(inst_id))
        if prev is not None:
            diff = frag_val - prev
            if abs(diff) < 0.005:
                trend_str = "stable"
            elif diff > 0:
                trend_str = "degrading"
            else:
                trend_str = "improving"
        else:
            trend_str = "—"

        # Classify fragility level
        if frag_val >= 0.25:
            frag_class = "fragile"
        elif frag_val >= 0.15:
            frag_class = "watch"
        elif frag_val > 0:
            frag_class = "antifragile"
        else:
            frag_class = str(class_str).lower()

        entities.append(
            {
                "entity_id": str(inst_id),
                "entity_type": "INSTRUMENT",
                "name": str(issuer_name) if issuer_name else str(inst_id),
                "score": frag_val,
                "classification": frag_class,
                "trend": trend_str,
                "soft_target_score": soft_val,
                "fragility_alpha": frag_val,
                "fragility_class": str(class_str),
            }
        )

    return FragilityStatus(
        region=region_norm,
        entity_type="INSTRUMENT",
        as_of_date=eff_date,
        entities=entities,
    )


@router.get("/fragility/{entity_id}", response_model=FragilityDetail)
async def get_fragility_detail(
    entity_id: str,
    as_of_date: Optional[date] = Query(None, description="As-of date"),
) -> FragilityDetail:
    """Return detailed fragility info for a specific instrument entity."""

    db_manager = get_db_manager()
    inst_id = str(entity_id)

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            if as_of_date is None:
                cursor.execute(
                    """
                    SELECT as_of_date, fragility_score, metadata
                    FROM fragility_measures
                    WHERE entity_type = 'INSTRUMENT' AND entity_id = %s
                    ORDER BY as_of_date ASC
                    """,
                    (inst_id,),
                )
            else:
                cursor.execute(
                    """
                    SELECT as_of_date, fragility_score, metadata
                    FROM fragility_measures
                    WHERE entity_type = 'INSTRUMENT' AND entity_id = %s
                      AND as_of_date <= %s
                    ORDER BY as_of_date ASC
                    """,
                    (inst_id, as_of_date),
                )
            rows = cursor.fetchall()
        finally:
            cursor.close()

    if not rows:
        return FragilityDetail(
            entity_id=inst_id,
            entity_type="INSTRUMENT",
            soft_target_score=0.0,
            fragility_alpha=0.0,
            fragility_class="NONE",
            history=[],
            scenarios=[],
            positions=[],
        )

    history: List[Dict[str, Any]] = []
    last_score = 0.0
    last_class = "NONE"

    for as_of_db, frag_score, metadata in rows:
        meta = metadata or {}
        class_str = meta.get("class_label") or "NONE"
        try:
            score_val = float(frag_score or 0.0)
        except Exception:
            score_val = 0.0
        history.append(
            {
                "date": as_of_db.isoformat(),
                "score": score_val,
                "alpha": score_val,
                "class": str(class_str),
            }
        )
        last_score = score_val
        last_class = str(class_str)

    # Scenario-level losses are stored in scenario_losses metadata; we
    # expose them as a lightweight table here.
    last_metadata = rows[-1][2] or {}
    scenario_losses = (last_metadata or {}).get("scenario_losses") or None
    if scenario_losses is None:
        scenario_losses = (last_metadata or {}).get("components", {}).get("scenario_losses", {})
    scenarios: List[Dict[str, Any]] = []
    if isinstance(scenario_losses, Mapping):
        for scen_id, loss in scenario_losses.items():
            try:
                loss_val = float(loss)
            except Exception:
                continue
            scenarios.append({"scenario": str(scen_id), "pnl": -loss_val})

    # Positions: surface simple target weights from book_targets, if any.
    positions: List[Dict[str, Any]] = []
    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT book_id, as_of_date, target_weight
                FROM book_targets
                WHERE entity_type = 'INSTRUMENT' AND entity_id = %s
                ORDER BY as_of_date DESC
                LIMIT 5
                """,
                (inst_id,),
            )
            pos_rows = cursor.fetchall()
        finally:
            cursor.close()

    for book_id, as_of_db, weight in pos_rows:
        try:
            w = float(weight or 0.0)
        except Exception:
            w = 0.0
        positions.append(
            {
                "portfolio_id": str(book_id),
                "position": w,
                "market_value": w,  # Targets are expressed in NAV terms.
            }
        )

    return FragilityDetail(
        entity_id=inst_id,
        entity_type="INSTRUMENT",
        soft_target_score=0.0,  # Soft-target detail can be added later.
        fragility_alpha=last_score,
        fragility_class=last_class,
        history=history,
        scenarios=scenarios,
        positions=positions,
    )


@router.get("/assessment", response_model=AssessmentStatus)
async def get_assessment_status(
    strategy_id: str = Query(..., description="Strategy identifier"),
    as_of_date: Optional[date] = Query(None, description="As-of date"),
) -> AssessmentStatus:
    """Return assessment output for a strategy from ``instrument_scores``."""

    db_manager = get_db_manager()
    strat_id = str(strategy_id)

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            if as_of_date is None:
                cursor.execute(
                    "SELECT MAX(as_of_date) FROM instrument_scores WHERE strategy_id = %s",
                    (strat_id,),
                )
                row = cursor.fetchone()
                eff_date = row[0] if row is not None else None
                if eff_date is None:
                    return AssessmentStatus(strategy_id=strat_id, as_of_date=None, instruments=[])
            else:
                eff_date = as_of_date

            cursor.execute(
                """
                SELECT instrument_id, expected_return, horizon_days, confidence, alpha_components
                FROM instrument_scores
                WHERE strategy_id = %s AND as_of_date = %s
                ORDER BY expected_return DESC
                LIMIT 200
                """,
                (strat_id, eff_date),
            )
            rows = cursor.fetchall()
        finally:
            cursor.close()

    instruments: List[Dict[str, Any]] = []
    for inst_id, exp_ret, horizon_days, conf, alpha_components in rows:
        alpha = alpha_components or {}
        if not isinstance(alpha, Mapping):
            alpha = {}
        instruments.append(
            {
                "instrument_id": str(inst_id),
                "expected_return": float(exp_ret or 0.0),
                "horizon_days": int(horizon_days or 0),
                "confidence": float(conf or 0.0),
                "alpha_breakdown": alpha,
            }
        )

    return AssessmentStatus(
        strategy_id=strat_id,
        as_of_date=eff_date,
        instruments=instruments,
    )


@router.get("/universe", response_model=UniverseStatus)
async def get_universe_status(
    strategy_id: str = Query(..., description="Strategy identifier"),
    as_of_date: Optional[date] = Query(None, description="As-of date"),
) -> UniverseStatus:
    """Return universe membership and scores from ``universe_members``.

    The mapping from ``strategy_id`` to ``universe_id`` currently follows
    the core long equity convention used in the engine pipeline, where
    ``US_CORE_LONG_EQ`` maps to ``CORE_EQ_US``.
    """

    db_manager = get_db_manager()
    strat_id = str(strategy_id)

    # Derive region and universe_id from strategy_id; fall back to a
    # simple uppercase mapping if the expected pattern is not present.
    parts = strat_id.upper().split("_", 1)
    region_code = parts[0] if parts else strat_id.upper()
    universe_id = f"CORE_EQ_{region_code}"

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            if as_of_date is None:
                cursor.execute(
                    "SELECT MAX(as_of_date) FROM universe_members WHERE universe_id = %s",
                    (universe_id,),
                )
                row = cursor.fetchone()
                eff_date = row[0] if row is not None else None
                if eff_date is None:
                    return UniverseStatus(strategy_id=strat_id, as_of_date=None, candidates=[])
            else:
                eff_date = as_of_date

            cursor.execute(
                """
                SELECT entity_id, included, score, reasons
                FROM universe_members
                WHERE universe_id = %s
                  AND as_of_date = %s
                  AND entity_type = 'INSTRUMENT'
                ORDER BY included DESC, score DESC, entity_id ASC
                LIMIT 500
                """,
                (universe_id, eff_date),
            )
            rows = cursor.fetchall()
        finally:
            cursor.close()

    candidates: List[Dict[str, Any]] = []
    for entity_id_db, included, score_db, reasons_db in rows:
        reasons = reasons_db or {}
        if not isinstance(reasons, Mapping):
            reasons = {}
        try:
            avg_vol = float(reasons.get("avg_volume_63d", 0.0) or 0.0)
        except Exception:
            avg_vol = 0.0
        try:
            soft_score = float(reasons.get("soft_target_score", 0.0) or 0.0)
        except Exception:
            soft_score = 0.0

        # Simple, bounded proxies for liquidity/quality.
        liquidity_score = max(0.0, min(1.0, avg_vol / 1_000_000.0))
        quality_score = max(0.0, min(1.0, 1.0 - soft_score / 100.0))

        candidates.append(
            {
                "instrument_id": str(entity_id_db),
                "in_universe": bool(included),
                "liquidity_score": liquidity_score,
                "quality_score": quality_score,
            }
        )

    return UniverseStatus(
        strategy_id=strat_id,
        as_of_date=eff_date,
        candidates=candidates,
    )


@router.get("/portfolios", response_model=PortfolioListResponse)
async def list_portfolios() -> PortfolioListResponse:
    """List all available portfolios with summary metadata."""

    db_manager = get_db_manager()
    summaries: List[PortfolioSummary] = []

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("""
                WITH latest AS (
                    SELECT portfolio_id,
                           MAX(timestamp) AS max_ts
                    FROM positions_snapshots
                    GROUP BY portfolio_id
                ),
                snap AS (
                    SELECT ps.portfolio_id,
                           ps.mode,
                           ps.as_of_date,
                           COUNT(*) AS num_pos,
                           SUM(ps.market_value) AS total_mv
                    FROM positions_snapshots ps
                    JOIN latest l ON l.portfolio_id = ps.portfolio_id AND l.max_ts = ps.timestamp
                    GROUP BY ps.portfolio_id, ps.mode, ps.as_of_date
                )
                SELECT s.portfolio_id, s.mode, s.as_of_date, s.num_pos, s.total_mv,
                       r.risk_metrics->'net_exposure' AS net_exp,
                       r.risk_metrics->'gross_exposure' AS gross_exp
                FROM snap s
                LEFT JOIN LATERAL (
                    SELECT risk_metrics FROM portfolio_risk_reports
                    WHERE portfolio_id = s.portfolio_id
                    ORDER BY as_of_date DESC LIMIT 1
                ) r ON TRUE
                ORDER BY s.total_mv DESC NULLS LAST
            """)
            for pid, mode, aod, npos, tmv, net_exp, gross_exp in cursor.fetchall():
                summaries.append(PortfolioSummary(
                    portfolio_id=pid,
                    mode=str(mode or "BACKTEST"),
                    latest_date=aod,
                    num_positions=int(npos or 0),
                    total_market_value=float(tmv or 0.0),
                    net_exposure=float(net_exp) if net_exp is not None else None,
                    gross_exposure=float(gross_exp) if gross_exp is not None else None,
                ))
        finally:
            cursor.close()

    logger.info("[api/portfolios] Returning %d portfolios", len(summaries))
    return PortfolioListResponse(portfolios=summaries)


@router.get("/portfolio", response_model=PortfolioStatus)
async def get_portfolio_status(
    portfolio_id: str = Query(..., description="Portfolio identifier"),
    as_of_date: Optional[date] = Query(None, description="As-of date"),
) -> PortfolioStatus:
    """Return portfolio targets and basic exposures for a portfolio_id.

    Positions are derived from ``target_portfolios`` weights (NAV-based
    targets). Exposures are taken from the latest corresponding
    ``portfolio_risk_reports`` row.
    """
    logger.info("[api/portfolio] Querying portfolio_id=%s as_of_date=%s", portfolio_id, as_of_date)

    db_manager = get_db_manager()
    port_id = str(portfolio_id)

    # Determine effective as_of_date: try portfolio_risk_reports first,
    # fall back to positions_snapshots (for live/paper portfolios that
    # may not have risk reports yet).
    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            eff_date = as_of_date
            if eff_date is None:
                cursor.execute(
                    "SELECT MAX(as_of_date) FROM portfolio_risk_reports WHERE portfolio_id = %s",
                    (port_id,),
                )
                row = cursor.fetchone()
                eff_date = row[0] if row is not None else None

            # If no risk reports, try positions_snapshots for the date.
            if eff_date is None:
                cursor.execute(
                    "SELECT MAX(as_of_date) FROM positions_snapshots WHERE portfolio_id = %s",
                    (port_id,),
                )
                row = cursor.fetchone()
                eff_date = row[0] if row is not None else None

            if eff_date is None:
                logger.warning("[api/portfolio] No data found for portfolio_id=%s in risk_reports or snapshots", port_id)
                return PortfolioStatus(portfolio_id=port_id, as_of_date=None, positions=[], pnl={}, exposures={})

            # Load target weights from target_portfolios.
            target_positions = None
            cursor.execute(
                """
                SELECT target_positions
                FROM target_portfolios
                WHERE portfolio_id = %s AND as_of_date = %s
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (port_id, eff_date),
            )
            row = cursor.fetchone()
            if row is not None:
                target_positions = row[0]

            # Load risk report row for exposures.
            risk_row = None
            cursor.execute(
                """
                SELECT risk_metrics, exposures_by_sector, exposures_by_factor, metadata
                FROM portfolio_risk_reports
                WHERE portfolio_id = %s AND as_of_date = %s
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (port_id, eff_date),
            )
            risk_row = cursor.fetchone()
        finally:
            cursor.close()

    # Positions from target weights (NAV=1.0 convention).
    positions: List[Dict[str, Any]] = []
    weights_payload: Mapping[str, Any] | None = None
    if isinstance(target_positions, Mapping):
        weights_payload = target_positions.get("weights")  # type: ignore[index]
    if isinstance(weights_payload, Mapping):
        for inst_id, w in weights_payload.items():
            try:
                weight = float(w or 0.0)
            except Exception:
                continue
            positions.append(
                {
                    "instrument_id": str(inst_id),
                    "quantity": 0.0,
                    "market_value": weight,
                    "weight": weight,
                }
            )

    # Exposures from portfolio_risk_reports.
    exposures: Dict[str, Any] = {}
    if risk_row is not None:
        risk_metrics_db, by_sector, by_factor, metadata = risk_row
        by_sector = by_sector or {}
        if not isinstance(by_sector, Mapping):
            by_sector = {}
        by_factor = by_factor or {}
        if not isinstance(by_factor, Mapping):
            by_factor = {}
        meta = metadata or {}
        if not isinstance(meta, Mapping):
            meta = {}

        exposures["by_sector"] = by_sector
        exposures["by_factor"] = by_factor

        frag_weights = meta.get("fragility_weight_by_class", {})
        if isinstance(frag_weights, Mapping):
            exposures["by_fragility_class"] = frag_weights

    # Compute NLV from positions_snapshots (actual dollar values).
    nlv = 0.0
    total_cash = 0.0
    mode = "BACKTEST"
    snap_positions: List[Dict[str, Any]] = []
    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT MAX(timestamp) FROM positions_snapshots WHERE portfolio_id = %s",
                (port_id,),
            )
            row_ts = cursor.fetchone()
            snap_ts = row_ts[0] if row_ts else None
            if snap_ts is not None:
                cursor.execute(
                    """
                    SELECT instrument_id, quantity, avg_cost, market_value,
                           unrealized_pnl, mode
                    FROM positions_snapshots
                    WHERE portfolio_id = %s AND timestamp = %s
                    ORDER BY market_value DESC
                    """,
                    (port_id, snap_ts),
                )
                for inst_id_s, qty_s, ac_s, mv_s, upnl_s, mode_s in cursor.fetchall():
                    mv = float(mv_s or 0.0)
                    nlv += mv
                    mode = str(mode_s or "BACKTEST")
                    snap_positions.append({
                        "instrument_id": str(inst_id_s),
                        "quantity": float(qty_s or 0.0),
                        "avg_cost": float(ac_s or 0.0),
                        "market_value": mv,
                        "unrealized_pnl": float(upnl_s or 0.0),
                        "weight": 0.0,  # filled below
                        "side": "LONG" if float(qty_s or 0) >= 0 else "SHORT",
                    })
                # Assign weights from NLV
                if nlv > 0:
                    for p in snap_positions:
                        p["weight"] = p["market_value"] / nlv
        finally:
            cursor.close()

    # If we got real dollar positions from snapshots, prefer those over NAV weights.
    if snap_positions:
        positions = snap_positions

    # Extract cash/NLV from risk_metrics if available (e.g. from IBKR sync).
    if risk_row is not None:
        rm = risk_row[0] or {}
        if isinstance(rm, Mapping):
            if "net_liquidation" in rm:
                nlv = float(rm["net_liquidation"])
            if "total_cash" in rm:
                total_cash = float(rm["total_cash"])

    # P&L: NLV diffs vs yesterday / month-start / year-start for this portfolio.
    pnl: Dict[str, float] = {"today": 0.0, "mtd": 0.0, "ytd": 0.0}
    _today = date.today()
    _month_start = _today.replace(day=1)
    _year_start = _today.replace(month=1, day=1)
    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                WITH latest_per_day AS (
                    SELECT DISTINCT ON (as_of_date)
                        as_of_date,
                        SUM(market_value) OVER (
                            PARTITION BY as_of_date, timestamp
                        ) AS nlv
                    FROM positions_snapshots
                    WHERE portfolio_id = %s
                    ORDER BY as_of_date, timestamp DESC
                )
                SELECT as_of_date, nlv
                FROM latest_per_day
                WHERE as_of_date >= %s
                ORDER BY as_of_date
                """,
                (port_id, _year_start),
            )
            _nlv_by_date: Dict[date, float] = {
                r[0]: float(r[1] or 0.0) for r in cursor.fetchall()
            }
        finally:
            cursor.close()

    _nlv_today = _nlv_by_date.get(_today, nlv)
    if _nlv_today and _nlv_by_date:
        _prev = [d for d in sorted(_nlv_by_date) if d < _today]
        if _prev:
            pnl["today"] = _nlv_today - _nlv_by_date[_prev[-1]]
        _mtd_ref = [d for d in sorted(_nlv_by_date) if d <= _month_start]
        if _mtd_ref:
            pnl["mtd"] = _nlv_today - _nlv_by_date[_mtd_ref[-1]]
        _ytd_ref = [d for d in sorted(_nlv_by_date) if d <= _year_start]
        if _ytd_ref:
            pnl["ytd"] = _nlv_today - _nlv_by_date[_ytd_ref[-1]]
        else:
            _earliest = sorted(_nlv_by_date)[0]
            pnl["ytd"] = _nlv_today - _nlv_by_date[_earliest]

    logger.info(
        "[api/portfolio] Returning portfolio_id=%s as_of=%s positions=%d nlv=%.2f mode=%s",
        port_id, eff_date, len(positions), nlv, mode,
    )
    return PortfolioStatus(
        portfolio_id=port_id,
        mode=mode,
        as_of_date=eff_date,
        net_liquidation_value=nlv,
        total_cash=total_cash,
        positions=positions,
        pnl=pnl,
        exposures=exposures,
    )


@router.get("/portfolio_risk", response_model=PortfolioRiskStatus)
async def get_portfolio_risk_status(
    portfolio_id: str = Query(..., description="Portfolio identifier"),
    as_of_date: Optional[date] = Query(None, description="As-of date"),
) -> PortfolioRiskStatus:
    """Return portfolio risk metrics from ``portfolio_risk_reports``."""

    db_manager = get_db_manager()
    port_id = str(portfolio_id)

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            if as_of_date is None:
                cursor.execute(
                    "SELECT MAX(as_of_date) FROM portfolio_risk_reports WHERE portfolio_id = %s",
                    (port_id,),
                )
                row = cursor.fetchone()
                eff_date = row[0] if row is not None else None
                if eff_date is None:
                    return PortfolioRiskStatus(
                        portfolio_id=port_id,
                        as_of_date=None,
                        volatility=0.0,
                        var_95=0.0,
                        expected_shortfall=0.0,
                        max_drawdown=0.0,
                        scenarios=[],
                    )
            else:
                eff_date = as_of_date

            cursor.execute(
                """
                SELECT risk_metrics, scenario_pnl
                FROM portfolio_risk_reports
                WHERE portfolio_id = %s AND as_of_date = %s
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (port_id, eff_date),
            )
            row = cursor.fetchone()
        finally:
            cursor.close()

    if row is None:
        return PortfolioRiskStatus(
            portfolio_id=port_id,
            as_of_date=eff_date,
            volatility=0.0,
            var_95=0.0,
            expected_shortfall=0.0,
            max_drawdown=0.0,
            scenarios=[],
        )

    risk_metrics_db, scenario_pnl_db = row
    rm = risk_metrics_db or {}
    if not isinstance(rm, Mapping):
        rm = {}
    scenario_pnl = scenario_pnl_db or {}
    if not isinstance(scenario_pnl, Mapping):
        scenario_pnl = {}

    volatility = float(rm.get("expected_volatility", 0.0) or 0.0)

    # Prefer scenario-based VaR/ES metrics when available, looking for
    # keys that contain "scenario_var_95" / "scenario_es_95".
    var_95 = 0.0
    es_95 = 0.0
    for key, value in rm.items():
        if "scenario_var_95" in str(key) and var_95 == 0.0:
            try:
                var_95 = float(value or 0.0)
            except Exception:
                continue
        if "scenario_es_95" in str(key) and es_95 == 0.0:
            try:
                es_95 = float(value or 0.0)
            except Exception:
                continue

    max_drawdown = float(rm.get("max_drawdown", 0.0) or 0.0)

    scenarios: List[Dict[str, Any]] = []
    for key, value in scenario_pnl.items():
        try:
            pnl_val = float(value or 0.0)
        except Exception:
            continue
        scenarios.append({"scenario": str(key), "pnl": pnl_val})

    return PortfolioRiskStatus(
        portfolio_id=port_id,
        as_of_date=eff_date,
        volatility=volatility,
        var_95=var_95,
        expected_shortfall=es_95,
        max_drawdown=max_drawdown,
        scenarios=scenarios,
    )


@router.get("/execution", response_model=ExecutionStatus)
async def get_execution_status(
    portfolio_id: str = Query(..., description="Portfolio identifier"),
    mode: Optional[str] = Query(
        None,
        description="Optional execution mode filter (LIVE/PAPER/BACKTEST)",
    ),
    limit_orders: int = Query(50, ge=1, le=500),
    limit_fills: int = Query(50, ge=1, le=500),
) -> ExecutionStatus:
    logger.info("[api/execution] Querying portfolio_id=%s mode=%s", portfolio_id, mode)
    """Return recent execution activity for a portfolio.

    Orders are read from the ``orders`` table using ``portfolio_id`` and
    optional ``mode``. Fills are joined to orders via ``order_id`` so
    that the same filters can be applied. Positions are taken from the
    most recent ``positions_snapshots`` timestamp for the portfolio.
    """

    db_manager = get_db_manager()
    port_id = str(portfolio_id)
    mode_norm = mode.upper() if mode is not None else None

    # IBKR_PAPER positions are synced under "IBKR_PAPER" but the execution
    # pipeline stores orders/fills under the allocator portfolio_id
    # (e.g. "US_EQ_ALLOCATOR").  Include both so the execution page shows
    # real trading activity.
    _IBKR_ALIAS = {
        "IBKR_PAPER": ["IBKR_PAPER", "US_EQ_ALLOCATOR"],
        "IBKR_LIVE": ["IBKR_LIVE", "US_EQ_ALLOCATOR"],
    }
    port_ids = _IBKR_ALIAS.get(port_id, [port_id])

    orders: List[Dict[str, Any]] = []
    fills: List[Dict[str, Any]] = []
    positions: List[Dict[str, Any]] = []

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            # Orders
            placeholders = ",".join(["%s"] * len(port_ids))
            where_clauses = [f"portfolio_id IN ({placeholders})"]
            params: list[object] = list(port_ids)
            if mode_norm is not None:
                where_clauses.append("mode = %s")
                params.append(mode_norm)
            where_sql = " WHERE " + " AND ".join(where_clauses)
            sql_orders = (
                "SELECT order_id, timestamp, instrument_id, side, order_type, "
                "quantity, status, mode, decision_id "
                "FROM orders" + where_sql + " ORDER BY timestamp DESC LIMIT %s"
            )
            params.append(limit_orders)
            cursor.execute(sql_orders, tuple(params))
            order_rows = cursor.fetchall()
            for (
                order_id,
                ts,
                instrument_id,
                side,
                order_type,
                quantity,
                status,
                mode_db,
                decision_id,
            ) in order_rows:
                orders.append(
                    {
                        "order_id": str(order_id),
                        "timestamp": ts,
                        "instrument_id": str(instrument_id),
                        "side": str(side),
                        "order_type": str(order_type),
                        "quantity": float(quantity or 0.0),
                        "status": str(status),
                        "mode": str(mode_db),
                        "decision_id": str(decision_id) if decision_id is not None else None,
                    }
                )

            # Fills (join to orders to filter by portfolio_id)
            where_clauses_f: List[str] = ["o.portfolio_id = %s"]
            params_f: list[object] = [port_id]
            if mode_norm is not None:
                where_clauses_f.append("f.mode = %s")
                params_f.append(mode_norm)
            where_sql_f = " WHERE " + " AND ".join(where_clauses_f)
            sql_fills = (
                "SELECT f.fill_id, f.timestamp, f.instrument_id, f.side, "
                "f.quantity, f.price, f.commission, f.order_id, f.mode, o.decision_id "
                "FROM fills f JOIN orders o ON o.order_id = f.order_id" +
                where_sql_f + " ORDER BY f.timestamp DESC LIMIT %s"
            )
            params_f.append(limit_fills)
            cursor.execute(sql_fills, tuple(params_f))
            fill_rows = cursor.fetchall()
            for (
                fill_id,
                ts_f,
                inst_id_f,
                side_f,
                qty_f,
                price_f,
                comm_f,
                order_id_f,
                mode_f,
                decision_id_f,
            ) in fill_rows:
                fills.append(
                    {
                        "fill_id": str(fill_id),
                        "timestamp": ts_f,
                        "instrument_id": str(inst_id_f),
                        "side": str(side_f),
                        "quantity": float(qty_f or 0.0),
                        "price": float(price_f or 0.0),
                        "commission": float(comm_f or 0.0),
                        "order_id": str(order_id_f),
                        "mode": str(mode_f),
                        "decision_id": str(decision_id_f) if decision_id_f is not None else None,
                    }
                )

            # Positions: latest snapshot timestamp for portfolio.
            cursor.execute(
                """
                SELECT MAX(timestamp) FROM positions_snapshots
                WHERE portfolio_id = %s
                """,
                (port_id,),
            )
            row_ts = cursor.fetchone()
            latest_ts = row_ts[0] if row_ts is not None else None
            if latest_ts is not None:
                cursor.execute(
                    """
                    SELECT instrument_id, quantity, avg_cost, market_value,
                           unrealized_pnl, mode
                    FROM positions_snapshots
                    WHERE portfolio_id = %s AND timestamp = %s
                    ORDER BY instrument_id
                    """,
                    (port_id, latest_ts),
                )
                pos_rows = cursor.fetchall()
                for (
                    inst_id_p,
                    qty_p,
                    avg_cost_p,
                    mv_p,
                    upnl_p,
                    mode_p,
                ) in pos_rows:
                    positions.append(
                        {
                            "instrument_id": str(inst_id_p),
                            "quantity": float(qty_p or 0.0),
                            "avg_cost": float(avg_cost_p or 0.0),
                            "market_value": float(mv_p or 0.0),
                            "unrealized_pnl": float(upnl_p or 0.0),
                            "mode": str(mode_p),
                        }
                    )
        finally:
            cursor.close()

    logger.info(
        "[api/execution] Returning portfolio_id=%s orders=%d fills=%d positions=%d",
        port_id, len(orders), len(fills), len(positions),
    )
    if not orders and not fills and not positions:
        logger.warning(
            "[api/execution] All empty for portfolio_id=%s — check that sync has persisted data to orders/fills/positions_snapshots tables",
            port_id,
        )
    return ExecutionStatus(
        portfolio_id=port_id,
        mode=mode_norm,
        orders=orders,
        fills=fills,
        positions=positions,
    )


@router.get("/execution/decisions", response_model=List[ExecutionDecisionResponse])
async def get_execution_decisions(
    portfolio_id: str = Query(..., description="Portfolio identifier"),
    as_of_date: Optional[date] = Query(None, description="Optional as_of_date filter"),
    limit: int = Query(50, ge=1, le=500),
) -> List[ExecutionDecisionResponse]:
    """Return recent EXECUTION decisions for a portfolio.

    Execution decisions are recorded in `engine_decisions` by
    `DecisionTracker.record_execution_decision`. We filter by
    `engine_name='EXECUTION'` and `strategy_id=<portfolio_id>`.
    """

    db = get_db_manager()
    port_id = str(portfolio_id)

    # Same IBKR alias mapping as get_execution_status
    _IBKR_ALIAS = {
        "IBKR_PAPER": ["IBKR_PAPER", "US_EQ_ALLOCATOR"],
        "IBKR_LIVE": ["IBKR_LIVE", "US_EQ_ALLOCATOR"],
    }
    port_ids = _IBKR_ALIAS.get(port_id, [port_id])
    placeholders = ",".join(["%s"] * len(port_ids))

    where_clauses = ["engine_name = 'EXECUTION'", f"strategy_id IN ({placeholders})"]
    params: list[object] = list(port_ids)

    if as_of_date is not None:
        where_clauses.append("as_of_date = %s")
        params.append(as_of_date)

    where_sql = " WHERE " + " AND ".join(where_clauses)

    sql = (
        "SELECT decision_id, market_id, as_of_date, input_refs, output_refs, created_at "
        "FROM engine_decisions" + where_sql + " ORDER BY as_of_date DESC, created_at DESC LIMIT %s"
    )
    params.append(int(limit))

    with db.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, tuple(params))
            rows = cursor.fetchall()
        finally:
            cursor.close()

    out: List[ExecutionDecisionResponse] = []

    for decision_id, market_id_db, as_of_date_db, input_refs_db, output_refs_db, created_at in rows:
        in_refs = input_refs_db if isinstance(input_refs_db, Mapping) else {}
        out_refs = output_refs_db if isinstance(output_refs_db, Mapping) else {}

        out.append(
            ExecutionDecisionResponse(
                decision_id=str(decision_id),
                market_id=str(market_id_db) if market_id_db is not None else None,
                as_of_date=as_of_date_db,
                created_at=created_at.isoformat() if hasattr(created_at, "isoformat") else None,
                portfolio_id=str(in_refs.get("portfolio_id")) if in_refs.get("portfolio_id") is not None else None,
                portfolio_decision_id=(
                    str(in_refs.get("portfolio_decision_id"))
                    if in_refs.get("portfolio_decision_id") is not None
                    else None
                ),
                order_count=int(out_refs.get("order_count") or 0),
                orders_preview=list(out_refs.get("orders") or []),
                plan_summary=dict(out_refs.get("plan_summary") or {}),
                execution_policy=dict(out_refs.get("execution_policy") or {}),
            )
        )

    return out


@router.get("/risk_actions", response_model=RiskActionsStatus)
async def get_risk_actions_status(
    strategy_id: str = Query(..., description="Strategy identifier"),
    limit: int = Query(50, ge=1, le=500, description="Maximum number of rows"),
) -> RiskActionsStatus:
    """Return recent ``risk_actions`` rows for a strategy.

    This endpoint mirrors the behaviour of the ``show_risk_actions`` CLI
    but returns structured JSON for the UI. It is primarily useful for
    inspecting how the Risk Management Service (and, in future, any
    execution-time risk wrappers) modified proposed positions.
    """

    db_manager = get_db_manager()
    strat_id = str(strategy_id)

    sql = """
        SELECT created_at, instrument_id, decision_id, action_type, details_json
        FROM risk_actions
        WHERE strategy_id = %s
        ORDER BY created_at DESC
        LIMIT %s
    """

    actions: List[RiskActionRow] = []
    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (strat_id, limit))
            rows = cursor.fetchall()
        finally:
            cursor.close()

    for created_at, instrument_id, decision_id, action_type, details in rows:
        details = details or {}
        if not isinstance(details, Mapping):
            details = {}
        orig = details.get("original_weight")
        adj = details.get("adjusted_weight")
        reason = details.get("reason")
        try:
            orig_f = float(orig) if orig is not None else None
        except Exception:
            orig_f = None
        try:
            adj_f = float(adj) if adj is not None else None
        except Exception:
            adj_f = None
        actions.append(
            RiskActionRow(
                created_at=created_at,
                instrument_id=str(instrument_id) if instrument_id is not None else None,
                decision_id=str(decision_id) if decision_id is not None else None,
                action_type=str(action_type),
                original_weight=orig_f,
                adjusted_weight=adj_f,
                reason=str(reason) if reason is not None else None,
            )
        )

    return RiskActionsStatus(strategy_id=strat_id, actions=actions)


# ── Portfolio Equity History ──────────────────────────────────────────


class EquityPoint(BaseModel):
    date: str
    portfolio: Optional[float] = None
    benchmark: Optional[float] = None


@router.get("/portfolio_equity")
async def get_portfolio_equity(
    portfolio_id: str = Query(..., description="Portfolio identifier"),
    benchmark: str = Query("SPY.US", description="Benchmark instrument_id (EODHD symbol in prices_daily)"),
) -> List[EquityPoint]:
    """Return daily portfolio equity and an optional benchmark series.

    Portfolio equity is derived from ``positions_snapshots`` by summing
    ``market_value`` per ``as_of_date``.  The benchmark is read from
    ``prices_daily`` (adjusted_close) and normalised so the first
    overlapping date matches the portfolio value.
    """
    db = get_db_manager()
    port_id = str(portfolio_id)

    # 1) Portfolio daily equity from positions_snapshots
    with db.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT as_of_date, SUM(market_value) AS total_mv
                FROM (
                    SELECT DISTINCT ON (as_of_date, instrument_id)
                           as_of_date, instrument_id, market_value
                    FROM positions_snapshots
                    WHERE portfolio_id = %s
                      AND as_of_date IS NOT NULL
                    ORDER BY as_of_date, instrument_id, timestamp DESC
                ) latest
                GROUP BY as_of_date
                ORDER BY as_of_date ASC
                """,
                (port_id,),
            )
            port_rows = cursor.fetchall()
        finally:
            cursor.close()

    if not port_rows:
        return []

    raw_by_date: Dict[str, float] = {}
    for d, mv in port_rows:
        raw_by_date[str(d)] = float(mv or 0.0)

    # 1b) Build flow-adjusted equity series.
    # Detect capital-flow days (deposits/withdrawals) where NLV jumps >15%
    # and rebase so the chart shows investment performance, not cash movements.
    _FLOW_THRESHOLD = 0.15
    sorted_raw = sorted(raw_by_date.keys())
    port_by_date: Dict[str, float] = {}

    if sorted_raw:
        # Start the adjusted series at the first raw NLV
        adj_equity = raw_by_date[sorted_raw[0]]
        port_by_date[sorted_raw[0]] = adj_equity

        for i in range(1, len(sorted_raw)):
            d = sorted_raw[i]
            prev_d = sorted_raw[i - 1]
            prev_raw = raw_by_date[prev_d]
            curr_raw = raw_by_date[d]

            if prev_raw > 0:
                daily_ret = (curr_raw - prev_raw) / prev_raw
                if abs(daily_ret) > _FLOW_THRESHOLD:
                    # Capital flow — carry adjusted equity flat (rebase)
                    port_by_date[d] = adj_equity
                else:
                    adj_equity *= (1.0 + daily_ret)
                    port_by_date[d] = round(adj_equity, 2)
            else:
                port_by_date[d] = adj_equity

    # 2) Benchmark prices from prices_daily (historical_db)
    bench_by_date: Dict[str, float] = {}
    bench_id = str(benchmark).strip()
    if bench_id:
        try:
            with db.get_historical_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(
                        """
                        SELECT trade_date, adjusted_close
                        FROM prices_daily
                        WHERE instrument_id = %s
                          AND adjusted_close IS NOT NULL
                        ORDER BY trade_date ASC
                        """,
                        (bench_id,),
                    )
                    for td, ac in cursor.fetchall():
                        bench_by_date[str(td)] = float(ac)
                finally:
                    cursor.close()
        except Exception as exc:
            logger.warning("[api/portfolio_equity] benchmark query failed: %s", exc)

    # 3) Normalise benchmark to portfolio value on first overlapping date
    all_dates = sorted(port_by_date.keys())
    if bench_by_date:
        first_port_val = None
        first_bench_val = None
        for d in all_dates:
            if d in bench_by_date:
                first_port_val = port_by_date[d]
                first_bench_val = bench_by_date[d]
                break
        if first_port_val and first_bench_val:
            scale = first_port_val / first_bench_val
            bench_by_date = {d: v * scale for d, v in bench_by_date.items()}

    # 4) Merge into response
    # Include all portfolio dates, plus any benchmark dates in range.
    # Forward-fill benchmark gaps (weekends, holidays, not-yet-ingested days).
    min_d, max_d = all_dates[0], all_dates[-1]
    merged_dates = sorted(set(all_dates) | {d for d in bench_by_date if min_d <= d <= max_d})

    out: List[EquityPoint] = []
    last_bench: float | None = None
    for d in merged_dates:
        bench_val = bench_by_date.get(d)
        if bench_val is not None:
            last_bench = bench_val
        out.append(EquityPoint(
            date=d,
            portfolio=port_by_date.get(d),
            benchmark=bench_val if bench_val is not None else last_bench,
        ))

    logger.info("[api/portfolio_equity] portfolio_id=%s benchmark=%s points=%d", port_id, bench_id, len(out))
    return out


# ── Position P&L History ─────────────────────────────────────────────


@router.get("/position_pnl_history")
async def get_position_pnl_history(
    portfolio_id: str = Query(..., description="Portfolio identifier"),
) -> List[Dict[str, Any]]:
    """Return per-position unrealized P&L over time.

    Each row has ``date`` plus one key per instrument (e.g. ``AAPL.US``) with
    its unrealized_pnl for that date.  This is designed for a multi-line chart.
    """
    db = get_db_manager()
    port_id = str(portfolio_id)

    with db.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT as_of_date, instrument_id, unrealized_pnl, market_value
                FROM (
                    SELECT DISTINCT ON (as_of_date, instrument_id)
                           as_of_date, instrument_id, unrealized_pnl, market_value
                    FROM positions_snapshots
                    WHERE portfolio_id = %s
                      AND as_of_date IS NOT NULL
                    ORDER BY as_of_date, instrument_id, timestamp DESC
                ) latest
                ORDER BY as_of_date, instrument_id
                """,
                (port_id,),
            )
            rows = cursor.fetchall()
        finally:
            cursor.close()

    if not rows:
        return []

    # Pivot: {date -> {instrument -> pnl}}
    from collections import OrderedDict
    by_date: OrderedDict[str, Dict[str, float]] = OrderedDict()
    instruments: set = set()
    for d, inst, pnl, mv in rows:
        ds = str(d)
        instruments.add(str(inst))
        if ds not in by_date:
            by_date[ds] = {}
        by_date[ds][str(inst)] = float(pnl or 0.0)

    out: List[Dict[str, Any]] = []
    for d, vals in by_date.items():
        row: Dict[str, Any] = {"date": d}
        for inst in sorted(instruments):
            row[inst] = vals.get(inst, None)
        out.append(row)

    logger.info("[api/position_pnl_history] portfolio_id=%s instruments=%d points=%d", port_id, len(instruments), len(out))
    return out


# ── Computed Portfolio Risk ──────────────────────────────────────────


class PositionRiskDetail(BaseModel):
    instrument_id: str
    weight: float
    vol_20d: Optional[float] = None
    vol_60d: Optional[float] = None
    beta: Optional[float] = None
    fragility_score: Optional[float] = None
    last_risk_action: Optional[str] = None


class ComputedPortfolioRisk(BaseModel):
    portfolio_id: str
    as_of_date: Optional[str] = None
    # Portfolio-level
    portfolio_vol_20d: Optional[float] = None
    portfolio_vol_60d: Optional[float] = None
    var_95: Optional[float] = None
    expected_shortfall: Optional[float] = None
    max_drawdown: Optional[float] = None
    hhi: Optional[float] = None
    regime: Optional[str] = None
    regime_confidence: Optional[float] = None
    # Per-position
    positions: List[PositionRiskDetail] = Field(default_factory=list)


@router.get("/portfolio_risk_computed")
async def get_portfolio_risk_computed(
    portfolio_id: str = Query(..., description="Portfolio identifier"),
    lookback: int = Query(252, description="Trading days of price history for risk calc"),
) -> ComputedPortfolioRisk:
    """Compute real risk metrics from daily prices and current positions.

    Uses historical daily returns to calculate volatility, VaR, expected
    shortfall, and beta.  Pulls fragility scores and regime from runtime DB.
    """
    import numpy as np

    db = get_db_manager()
    port_id = str(portfolio_id)

    # 1) Get positions from the latest snapshot timestamp (current holdings).
    # Using the most recent timestamp ensures we only see positions that
    # are actually held now, not closed positions from earlier snapshots.
    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT MAX(timestamp) FROM positions_snapshots WHERE portfolio_id = %s",
                (port_id,),
            )
            snap_ts = (cur.fetchone() or (None,))[0]

            if snap_ts is None:
                cur.close()
                return ComputedPortfolioRisk(portfolio_id=port_id)

            cur.execute(
                """
                SELECT instrument_id, quantity, avg_cost, market_value,
                       CASE WHEN SUM(market_value) OVER () > 0
                            THEN market_value / SUM(market_value) OVER ()
                            ELSE 0 END AS weight
                FROM positions_snapshots
                WHERE portfolio_id = %s AND timestamp = %s
                ORDER BY market_value DESC
                """,
                (port_id, snap_ts),
            )
            pos_rows = cur.fetchall()
        finally:
            cur.close()

    if not pos_rows:
        return ComputedPortfolioRisk(portfolio_id=port_id)

    instruments = [r[0] for r in pos_rows]
    weights = np.array([float(r[4]) for r in pos_rows])
    nlv = sum(float(r[3] or 0) for r in pos_rows)

    # 2) Fetch daily adjusted_close for each instrument (last N trading days)
    returns_by_inst: Dict[str, np.ndarray] = {}
    with db.get_historical_connection() as conn:
        cur = conn.cursor()
        try:
            for inst in instruments:
                cur.execute(
                    """
                    SELECT adjusted_close FROM (
                        SELECT trade_date, adjusted_close
                        FROM prices_daily
                        WHERE instrument_id = %s AND adjusted_close IS NOT NULL
                        ORDER BY trade_date DESC
                        LIMIT %s
                    ) sub ORDER BY trade_date ASC
                    """,
                    (inst, lookback + 1),
                )
                prices = np.array([float(r[0]) for r in cur.fetchall()])
                if len(prices) > 1:
                    rets = np.diff(np.log(prices))  # log returns
                    returns_by_inst[inst] = rets

            # Also get SPY for beta calculation
            cur.execute(
                """
                SELECT adjusted_close FROM (
                    SELECT trade_date, adjusted_close
                    FROM prices_daily
                    WHERE instrument_id = 'SPY.US' AND adjusted_close IS NOT NULL
                    ORDER BY trade_date DESC
                    LIMIT %s
                ) sub ORDER BY trade_date ASC
                """,
                (lookback + 1,),
            )
            spy_prices = np.array([float(r[0]) for r in cur.fetchall()])
            spy_rets = np.diff(np.log(spy_prices)) if len(spy_prices) > 1 else None
        finally:
            cur.close()

    # 3) Compute per-position metrics
    TRADING_DAYS = 252
    pos_details: List[PositionRiskDetail] = []
    inst_vols_20 = {}

    for i, inst in enumerate(instruments):
        rets = returns_by_inst.get(inst)
        v20 = v60 = beta_val = None
        if rets is not None and len(rets) >= 20:
            v20 = float(np.std(rets[-20:]) * np.sqrt(TRADING_DAYS))
            inst_vols_20[inst] = v20
        if rets is not None and len(rets) >= 60:
            v60 = float(np.std(rets[-60:]) * np.sqrt(TRADING_DAYS))
        if rets is not None and spy_rets is not None:
            n = min(len(rets), len(spy_rets))
            if n >= 20:
                r_i = rets[-n:]
                r_m = spy_rets[-n:]
                cov = np.cov(r_i, r_m)
                if cov[1, 1] > 0:
                    beta_val = float(cov[0, 1] / cov[1, 1])

        pos_details.append(PositionRiskDetail(
            instrument_id=inst,
            weight=float(weights[i]),
            vol_20d=v20,
            vol_60d=v60,
            beta=beta_val,
        ))

    # 4) Portfolio-level vol using correlation matrix
    # Only include instruments that have sufficient price history (skip
    # options and other derivatives without prices_daily data).
    priced_idx = [i for i, inst in enumerate(instruments) if inst in returns_by_inst]
    priced_rets = {inst: returns_by_inst[inst] for inst in instruments if inst in returns_by_inst}
    common_len = min((len(r) for r in priced_rets.values()), default=0)
    port_vol_20 = port_vol_60 = var_95 = es_95 = max_dd = None

    if common_len >= 20 and priced_rets:
        priced_instruments = [instruments[i] for i in priced_idx]
        priced_weights = weights[priced_idx]
        # Renormalize weights to sum to 1 over priced instruments
        w_sum = priced_weights.sum()
        if w_sum > 0:
            priced_weights = priced_weights / w_sum

        ret_matrix = np.column_stack([
            priced_rets[inst][-common_len:] for inst in priced_instruments
        ])

        # Portfolio daily returns (weighted)
        port_daily = ret_matrix @ priced_weights

        # 20d and 60d portfolio vol
        port_vol_20 = float(np.std(port_daily[-20:]) * np.sqrt(TRADING_DAYS))
        if common_len >= 60:
            port_vol_60 = float(np.std(port_daily[-60:]) * np.sqrt(TRADING_DAYS))

        # Historical VaR 95% (dollar terms, 1-day)
        var_pct = float(np.percentile(port_daily, 5))
        var_95 = round(var_pct * nlv, 2) if nlv else None

        # Expected Shortfall (CVaR) – mean of returns below VaR
        tail = port_daily[port_daily <= var_pct]
        if len(tail) > 0:
            es_95 = round(float(np.mean(tail)) * nlv, 2) if nlv else None

        # Max drawdown from cumulative returns
        cum = np.cumsum(port_daily)
        running_max = np.maximum.accumulate(cum)
        dd = cum - running_max
        max_dd = float(np.min(dd)) if len(dd) > 0 else None

    # 5) HHI (Herfindahl-Hirschman Index) — use only priced (equity) positions
    if priced_idx:
        equity_weights = weights[priced_idx]
        ew_sum = equity_weights.sum()
        if ew_sum > 0:
            equity_weights_norm = equity_weights / ew_sum
            hhi = float(np.sum(equity_weights_norm ** 2))
        else:
            hhi = None
    else:
        hhi = float(np.sum(weights ** 2)) if len(weights) > 0 else None

    # 6) Fragility scores for held instruments
    frag_map: Dict[str, float] = {}
    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            placeholders = ",".join(["%s"] * len(instruments))
            cur.execute(
                f"""
                SELECT DISTINCT ON (entity_id) entity_id, fragility_score
                FROM fragility_measures
                WHERE entity_id IN ({placeholders})
                ORDER BY entity_id, as_of_date DESC
                """,
                instruments,
            )
            for eid, fscore in cur.fetchall():
                frag_map[str(eid)] = float(fscore) if fscore is not None else None
        finally:
            cur.close()

    # 7) Latest risk actions per instrument
    action_map: Dict[str, str] = {}
    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                SELECT DISTINCT ON (instrument_id) instrument_id, action_type
                FROM risk_actions
                WHERE instrument_id IN ({placeholders})
                ORDER BY instrument_id, created_at DESC
                """,
                instruments,
            )
            for iid, atype in cur.fetchall():
                action_map[str(iid)] = str(atype)
        finally:
            cur.close()

    # 8) Regime
    regime_label = regime_conf = None
    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute("SELECT regime_label, confidence FROM regimes ORDER BY as_of_date DESC LIMIT 1")
            row = cur.fetchone()
            if row:
                regime_label = str(row[0])
                regime_conf = float(row[1]) if row[1] is not None else None
        finally:
            cur.close()

    # Merge fragility + risk action into position details
    for pd in pos_details:
        pd.fragility_score = frag_map.get(pd.instrument_id)
        pd.last_risk_action = action_map.get(pd.instrument_id)

    today_str = str(date.today())
    logger.info(
        "[api/portfolio_risk_computed] portfolio_id=%s instruments=%d port_vol_20d=%s var_95=%s",
        port_id, len(instruments), port_vol_20, var_95,
    )

    return ComputedPortfolioRisk(
        portfolio_id=port_id,
        as_of_date=today_str,
        portfolio_vol_20d=port_vol_20,
        portfolio_vol_60d=port_vol_60,
        var_95=var_95,
        expected_shortfall=es_95,
        max_drawdown=max_dd,
        hhi=hhi,
        regime=regime_label,
        regime_confidence=regime_conf,
        positions=pos_details,
    )


# ============================================================================
# Market Overview – VIX, Momentum, Breadth, Fear & Greed
# ============================================================================


class MarketOverview(BaseModel):
    """Comprehensive market indicators computed from historical prices."""

    as_of_date: Optional[str] = None

    # VIX
    vix_current: Optional[float] = None
    vix_ma20: Optional[float] = None
    vix_percentile_1y: Optional[float] = None
    vix_history: List[Dict[str, Any]] = Field(default_factory=list)

    # SPY
    spy_current: Optional[float] = None
    spy_ma50: Optional[float] = None
    spy_ma200: Optional[float] = None
    spy_pct_from_high: Optional[float] = None
    spy_history: List[Dict[str, Any]] = Field(default_factory=list)

    # Market breadth
    breadth_above_50d: Optional[float] = None
    breadth_above_200d: Optional[float] = None
    breadth_total: int = 0

    # Credit spread proxy (HYG)
    hyg_current: Optional[float] = None
    hyg_ma200: Optional[float] = None
    hyg_relative_strength: Optional[float] = None

    # Fear & Greed composite
    fear_greed_score: Optional[float] = None
    fear_greed_label: Optional[str] = None
    fg_vix_component: Optional[float] = None
    fg_momentum_component: Optional[float] = None
    fg_breadth_component: Optional[float] = None
    fg_credit_component: Optional[float] = None

    # Regime transitions (all-time, for timeline)
    regime_transitions: List[Dict[str, Any]] = Field(default_factory=list)


def _fg_label(score: float) -> str:
    """Map Fear & Greed score (0–100) to human label."""
    if score <= 20:
        return "Extreme Fear"
    if score <= 40:
        return "Fear"
    if score <= 60:
        return "Neutral"
    if score <= 80:
        return "Greed"
    return "Extreme Greed"


def _fetch_closes(cursor, instrument_id: str, limit: int) -> list[tuple]:
    """Fetch (trade_date, close) rows ordered by date descending."""
    cursor.execute(
        """
        SELECT trade_date, close
        FROM prices_daily
        WHERE instrument_id = %s
        ORDER BY trade_date DESC
        LIMIT %s
        """,
        (instrument_id, limit),
    )
    return cursor.fetchall()


@router.get("/market_overview", response_model=MarketOverview)
async def get_market_overview() -> MarketOverview:
    """Return market indicators: VIX, SPY momentum, breadth, credit, Fear & Greed.

    All data is computed from ``prometheus_historical.prices_daily``.
    Market breadth uses S&P-constituent instruments joined from the runtime DB.
    """

    import numpy as np

    db = get_db_manager()

    # ------------------------------------------------------------------
    # 1) VIX
    # ------------------------------------------------------------------
    vix_current = vix_ma20 = vix_pct_1y = None
    vix_history: List[Dict[str, Any]] = []

    with db.get_historical_connection() as conn:
        cur = conn.cursor()
        try:
            rows = _fetch_closes(cur, "VIX.INDX", 260)  # ~1 year
        finally:
            cur.close()

    if rows:
        # rows are DESC; reverse for chronological
        rows_asc = list(reversed(rows))
        closes = [float(r[1]) for r in rows_asc]

        vix_current = closes[-1]
        if len(closes) >= 20:
            vix_ma20 = float(np.mean(closes[-20:]))
        # 1-year percentile rank (what % of days had VIX below current)
        vix_pct_1y = float(np.sum(np.array(closes) < vix_current) / len(closes) * 100)

        # Last 90 days for chart
        for dt, cl in rows_asc[-90:]:
            vix_history.append({"date": dt.isoformat(), "close": round(float(cl), 2)})

    # ------------------------------------------------------------------
    # 2) SPY
    # ------------------------------------------------------------------
    spy_current = spy_ma50 = spy_ma200 = spy_pct_high = None
    spy_history: List[Dict[str, Any]] = []

    with db.get_historical_connection() as conn:
        cur = conn.cursor()
        try:
            rows = _fetch_closes(cur, "SPY.US", 260)
        finally:
            cur.close()

    if rows:
        rows_asc = list(reversed(rows))
        closes = [float(r[1]) for r in rows_asc]

        spy_current = closes[-1]
        if len(closes) >= 50:
            spy_ma50 = float(np.mean(closes[-50:]))
        if len(closes) >= 200:
            spy_ma200 = float(np.mean(closes[-200:]))

        high_52w = max(closes)
        spy_pct_high = round((spy_current / high_52w - 1) * 100, 2) if high_52w else None

        for dt, cl in rows_asc[-90:]:
            spy_history.append({"date": dt.isoformat(), "close": round(float(cl), 2)})

    # ------------------------------------------------------------------
    # 3) HYG (credit spread proxy)
    # ------------------------------------------------------------------
    hyg_current = hyg_ma200 = hyg_rs = None

    with db.get_historical_connection() as conn:
        cur = conn.cursor()
        try:
            rows = _fetch_closes(cur, "HYG.US", 210)
        finally:
            cur.close()

    if rows:
        rows_asc = list(reversed(rows))
        closes = [float(r[1]) for r in rows_asc]

        hyg_current = closes[-1]
        if len(closes) >= 200:
            hyg_ma200 = float(np.mean(closes[-200:]))
            hyg_rs = round(hyg_current / hyg_ma200, 4) if hyg_ma200 else None

    # ------------------------------------------------------------------
    # 4) Market breadth – % of S&P instruments above 50d / 200d MA
    # ------------------------------------------------------------------
    breadth_50 = breadth_200 = None
    breadth_total = 0

    # Get S&P instrument IDs from runtime DB
    sp_instruments: list[str] = []
    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT i.instrument_id
                FROM instruments i
                JOIN issuers iss ON iss.issuer_id = i.issuer_id
                WHERE iss.issuer_type = 'COMPANY'
                  AND i.asset_class = 'EQUITY'
                  AND i.status = 'ACTIVE'
                """
            )
            sp_instruments = [str(r[0]) for r in cur.fetchall()]
        finally:
            cur.close()

    if sp_instruments:
        above_50 = 0
        above_200 = 0
        counted = 0

        with db.get_historical_connection() as conn:
            cur = conn.cursor()
            try:
                # Batch query: latest 200 closes per instrument, aggregate
                # For performance, use a single query with window functions
                placeholders = ",".join(["%s"] * len(sp_instruments))
                cur.execute(
                    f"""
                    WITH ranked AS (
                        SELECT instrument_id, close,
                               ROW_NUMBER() OVER (
                                   PARTITION BY instrument_id
                                   ORDER BY trade_date DESC
                               ) AS rn
                        FROM prices_daily
                        WHERE instrument_id IN ({placeholders})
                          AND trade_date >= CURRENT_DATE - INTERVAL '300 days'
                    )
                    SELECT instrument_id,
                           -- latest close
                           MAX(CASE WHEN rn = 1 THEN close END) AS last_close,
                           -- 50d MA
                           AVG(CASE WHEN rn <= 50 THEN close END) AS ma50,
                           -- 200d MA
                           AVG(CASE WHEN rn <= 200 THEN close END) AS ma200,
                           COUNT(*) AS n_rows
                    FROM ranked
                    WHERE rn <= 200
                    GROUP BY instrument_id
                    HAVING COUNT(*) >= 50
                    """,
                    sp_instruments,
                )
                for _iid, last_close, ma50, ma200, n_rows in cur.fetchall():
                    if last_close is None:
                        continue
                    counted += 1
                    lc = float(last_close)
                    if ma50 is not None and lc > float(ma50):
                        above_50 += 1
                    if ma200 is not None and n_rows >= 200 and lc > float(ma200):
                        above_200 += 1
            finally:
                cur.close()

        breadth_total = counted
        if counted > 0:
            breadth_50 = round(above_50 / counted * 100, 1)
            breadth_200 = round(above_200 / counted * 100, 1)

    # ------------------------------------------------------------------
    # 5) Fear & Greed composite (0–100, higher = greedier)
    # ------------------------------------------------------------------
    components: list[float] = []
    fg_vix = fg_mom = fg_brd = fg_crd = None

    # VIX component: invert percentile (low VIX = greed)
    if vix_pct_1y is not None:
        fg_vix = round(100 - vix_pct_1y, 1)
        components.append(fg_vix)

    # Momentum component: SPY distance from 125d MA
    if spy_current is not None and len(spy_history) >= 1:
        with db.get_historical_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    SELECT AVG(close) FROM (
                        SELECT close FROM prices_daily
                        WHERE instrument_id = 'SPY.US'
                        ORDER BY trade_date DESC LIMIT 125
                    ) sub
                    """
                )
                row = cur.fetchone()
                ma125 = float(row[0]) if row and row[0] else None
            finally:
                cur.close()

        if ma125 and ma125 > 0:
            pct_above = (spy_current / ma125 - 1) * 100
            # Scale: -5% = 0, 0% = 50, +5% = 100
            fg_mom = round(max(0, min(100, 50 + pct_above * 10)), 1)
            components.append(fg_mom)

    # Breadth component: scale % above 200d MA
    if breadth_200 is not None:
        fg_brd = round(breadth_200, 1)  # Already 0–100
        components.append(fg_brd)

    # Credit component: HYG relative strength
    if hyg_rs is not None:
        # RS 0.95 = fear (0), RS 1.00 = neutral (50), RS 1.05 = greed (100)
        fg_crd = round(max(0, min(100, (hyg_rs - 0.95) / 0.10 * 100)), 1)
        components.append(fg_crd)

    fg_score = round(float(np.mean(components)), 1) if components else None
    fg_lbl = _fg_label(fg_score) if fg_score is not None else None

    # ------------------------------------------------------------------
    # 6) Regime transitions (from authoritative regime_transitions table)
    # ------------------------------------------------------------------
    transitions: List[Dict[str, Any]] = []
    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT as_of_date, from_regime_label, to_regime_label
                FROM regime_transitions
                WHERE region = 'US'
                ORDER BY as_of_date DESC
                LIMIT 30
                """
            )
            for aod, prev, cur_lbl in cur.fetchall():
                transitions.append({
                    "date": aod.isoformat(),
                    "from": str(prev),
                    "to": str(cur_lbl),
                })
        finally:
            cur.close()

    as_of = spy_history[-1]["date"] if spy_history else None

    logger.info(
        "[api/market_overview] vix=%.1f spy=%.1f breadth_200=%.1f%% fg=%.1f (%s)",
        vix_current or 0, spy_current or 0, breadth_200 or 0,
        fg_score or 0, fg_lbl or "N/A",
    )

    return MarketOverview(
        as_of_date=as_of,
        vix_current=vix_current,
        vix_ma20=round(vix_ma20, 2) if vix_ma20 else None,
        vix_percentile_1y=round(vix_pct_1y, 1) if vix_pct_1y is not None else None,
        vix_history=vix_history,
        spy_current=round(spy_current, 2) if spy_current else None,
        spy_ma50=round(spy_ma50, 2) if spy_ma50 else None,
        spy_ma200=round(spy_ma200, 2) if spy_ma200 else None,
        spy_pct_from_high=spy_pct_high,
        spy_history=spy_history,
        breadth_above_50d=breadth_50,
        breadth_above_200d=breadth_200,
        breadth_total=breadth_total,
        hyg_current=round(hyg_current, 2) if hyg_current else None,
        hyg_ma200=round(hyg_ma200, 2) if hyg_ma200 else None,
        hyg_relative_strength=hyg_rs,
        fear_greed_score=fg_score,
        fear_greed_label=fg_lbl,
        fg_vix_component=fg_vix,
        fg_momentum_component=fg_mom,
        fg_breadth_component=fg_brd,
        fg_credit_component=fg_crd,
        regime_transitions=transitions,
    )


# ============================================================================
# Documentation
# ============================================================================


_PROMETHEUS_DOCS: Dict[str, Dict[str, str]] = {
    "overview": {
        "title": "System Overview",
        "content": """# System Overview

**Version:** 2.0 · March 2026
**Status:** Live paper-trading via IBKR · 17 options strategies · 3 market pipelines
**Mode:** PAPER (IBKR Gateway port 4001)

---

## What is Prometheus v2?

Prometheus v2 is a quantitative trading system that combines multi-asset regime detection, per-entity fragility analysis, and a regime-adaptive options overlay to manage a portfolio of US equities and derivatives.

The system operates on a **daily cadence**, triggered by market close events. Three regional pipelines (US, EU, ASIA) run independently — EU and ASIA generate cross-market intelligence signals while all capital is concentrated in US equities and options.

### Core Philosophy

- **Intelligence without capital commitment** — EU/ASIA pipelines run for signal generation (cross-market contagion, macro stress, breadth) but capital trades only in US_EQ.
- **Regime-adaptive everything** — The regime label flows into strategy allocation budgets, sector put spread thresholds, options strategy activation, and portfolio risk limits.
- **Defined-risk derivatives** — All 17 options strategies use spreads with defined max loss. No naked short options. Position lifecycle manager enforces profit targets and expiry rolls.
- **Offensive + defensive** — Options layer includes both hedging strategies (protective put, collar, VIX tail) and profit-seeking strategies (bear put spread, sector decline, bull call spread, momentum call).

---

## System Architecture

The complete end-to-end system: data ingestion → representation → decision engines → execution → monitoring.

```mermaid
graph TB
    subgraph EXT["External Data Sources"]
        IBKR["IBKR Gateway\\n(paper 4001 · live 7496)"]
        YAHOO["Market Data\\n(Yahoo Finance)"]
        FRED["FRED API\\n(STLFSI4, rates, claims)"]
        NEWS["News & Filings"]
    end

    subgraph INGEST["Data Ingestion"]
        ING_PRICE["Price Ingestion\\n(OHLCV, adj close)"]
        ING_FACTOR["Factor Ingestion\\n(sector, style)"]
        ING_MACRO["Macro Ingestion\\n(FRED, VIX, breadth)"]
        ING_BROKER["Broker Sync\\n(positions, fills)"]
    end

    subgraph STORAGE["PostgreSQL"]
        subgraph HIST["historical_db"]
            PRICES["prices_daily\\nreturns_daily\\nvolatility_daily\\nfactors_daily"]
            TEXT["news_articles\\nfilings\\nmacro_events"]
            EMB["text_embeddings\\nnumeric_window_embeddings\\njoint_embeddings"]
        end
        subgraph RUNTIME["runtime_db"]
            ENTITIES["markets · issuers\\ninstruments · portfolios"]
            ENGINE_OUT["regimes · stability_vectors\\nfragility_measures\\nsoft_target_classes\\ninstrument_scores"]
            SECTOR_TBL["sector_health_daily\\n(score, raw_composite,\\nsignals JSONB)"]
            UNIV_TBL["universe_members\\n(entity_id, score,\\nreasons JSONB)"]
            RUNS_TBL["engine_runs\\n(phase state machine)"]
            EXEC_TBL["orders · fills\\npositions_snapshots\\ntarget_portfolios"]
            DECISIONS["engine_decisions\\nexecuted_actions\\ndecision_outcomes"]
        end
    end

    subgraph REPR["Representation Layer"]
        TEXT_ENC["Text Encoders\\n(news, profiles, macro)"]
        NUM_ENC["Numeric Window Encoders\\n(price to embeddings)"]
        JOINT_ENC["Joint Multi-Entity Encoder\\n(text + numeric fusion)"]
        PROF_SVC["Profile Service\\n(issuer snapshots)"]
    end

    subgraph ENGINES["Core Decision Engines"]
        REGIME["Regime Engine\\nCARRY · NEUTRAL · RISK_OFF · CRISIS"]
        STAB["Stability & Soft-Target\\nSTABLE · TARGETABLE · WATCH · FRAGILE"]
        FRAG["Fragility Alpha\\nfragility_score per entity"]
        SHI["Sector Health Engine\\nSHI score (6 signals)\\ntrend · momentum · volatility\\ndrawdown · breadth · macro_stress"]
        ASSESS["Assessment Engine\\ninstrument_scores"]
        UNIV["Universe Engine\\nCORE_EQ members + lambda scores"]
        PORT["Portfolio & Risk\\ntarget weights, risk reports"]
        LAMBDA["Lambda Opportunity Density\\ncluster dispersion models"]
    end

    subgraph EXEC["Execution Layer"]
        EQUITY_EXEC["Equity Execution\\n(compute deltas to orders)"]
        subgraph OPTIONS["Options & Derivatives"]
            ALLOC["Strategy Allocator\\n(regime to category budgets)"]
            OSM["Options Strategy Manager\\n(17 strategies)"]
            LIFECYCLE["Position Lifecycle\\n(roll, close, profit-take)"]
        end
        subgraph BROKERS["Broker Implementations"]
            LIVE_B["LiveBroker"]
            PAPER_B["PaperBroker"]
            BT_B["BacktestBroker"]
        end
    end

    subgraph ORCH["Orchestration"]
        DAEMON["Market-Aware Daemon\\n(systemd service)"]
        CAL["TradingCalendar\\n(market states per region)"]
        DAG["DAG Orchestrator\\n(job graph per market)"]
    end

    subgraph MON["Monitoring & UI"]
        API["FastAPI Server\\n(REST endpoints)"]
        NGINX["nginx reverse proxy\\n(:8443 HTTPS)"]
        GUI["React GUI\\n(Bloomberg-style C2)"]
        KRONOS["Kronos Meta-Orchestrator\\n(analytics, LLM chat)"]
    end

    subgraph BT["Backtesting"]
        TIME_M["TimeMachine\\n(no-lookahead data)"]
        MKT_SIM["MarketSimulator\\n(fills, slippage)"]
        CPP["C++ Backtest Core\\n(prom2_cpp fast path)"]
    end

    YAHOO --> ING_PRICE
    FRED --> ING_MACRO
    NEWS --> TEXT
    IBKR --> ING_BROKER

    ING_PRICE --> PRICES
    ING_FACTOR --> PRICES
    ING_MACRO --> TEXT
    ING_BROKER --> EXEC_TBL

    PRICES --> NUM_ENC
    TEXT --> TEXT_ENC
    ENTITIES --> PROF_SVC

    TEXT_ENC --> JOINT_ENC
    NUM_ENC --> JOINT_ENC
    PROF_SVC --> JOINT_ENC
    JOINT_ENC --> REGIME
    JOINT_ENC --> STAB
    JOINT_ENC --> ASSESS

    PRICES --> REGIME
    PRICES --> SHI
    FRED --> SHI
    REGIME --> STAB
    REGIME --> ASSESS
    STAB --> FRAG
    STAB --> ASSESS
    FRAG --> ASSESS
    SHI --> PORT
    SHI --> OPTIONS
    ASSESS --> UNIV
    LAMBDA --> UNIV
    UNIV --> PORT

    REGIME --> ENGINE_OUT
    STAB --> ENGINE_OUT
    FRAG --> ENGINE_OUT
    ASSESS --> ENGINE_OUT
    SHI --> SECTOR_TBL
    UNIV --> UNIV_TBL
    PORT --> EXEC_TBL

    PORT --> EQUITY_EXEC
    REGIME --> ALLOC
    ALLOC --> OSM
    OSM --> LIFECYCLE
    EQUITY_EXEC --> BROKERS
    LIFECYCLE --> BROKERS
    LIVE_B --> IBKR
    PAPER_B --> IBKR

    BT_B --> TIME_M
    BT_B --> MKT_SIM
    TIME_M --> PRICES
    PRICES --> CPP

    CAL --> DAEMON
    DAEMON --> DAG
    DAG --> INGEST
    DAG --> REPR
    DAG --> ENGINES
    DAG --> EXEC
    DAG --> RUNS_TBL

    ENGINE_OUT --> API
    SECTOR_TBL --> API
    EXEC_TBL --> API
    DECISIONS --> API
    API --> NGINX
    NGINX --> GUI
    API --> KRONOS

    REGIME -.-> DECISIONS
    STAB -.-> DECISIONS
    PORT -.-> DECISIONS
    EQUITY_EXEC -.-> EXEC_TBL
    OSM -.-> EXEC_TBL

    classDef ext fill:#2d3748,stroke:#4a5568,color:#e2e8f0
    classDef store fill:#1a365d,stroke:#2c5282,color:#e2e8f0
    classDef engine fill:#742a2a,stroke:#c53030,color:#e2e8f0
    classDef exec fill:#234e52,stroke:#2c7a7b,color:#e2e8f0
    classDef orch fill:#44337a,stroke:#6b46c1,color:#e2e8f0
    classDef mon fill:#553c00,stroke:#d69e2e,color:#e2e8f0

    class IBKR,YAHOO,FRED,NEWS ext
    class PRICES,TEXT,EMB,ENTITIES,ENGINE_OUT,SECTOR_TBL,UNIV_TBL,RUNS_TBL,EXEC_TBL,DECISIONS store
    class REGIME,STAB,FRAG,SHI,ASSESS,UNIV,PORT,LAMBDA engine
    class EQUITY_EXEC,ALLOC,OSM,LIFECYCLE,LIVE_B,PAPER_B,BT_B exec
    class DAEMON,CAL,DAG orch
    class API,NGINX,GUI,KRONOS mon
```

---

## Data Flow Summary

1. **Ingestion** — Yahoo Finance prices, FRED macro data, and IBKR broker state are pulled daily after market close.
2. **Representation** — Raw data is encoded into numeric window embeddings (price patterns) and text embeddings (news/macro), then fused via a joint encoder.
3. **Engines** — Six decision engines process the representations: Regime (market state), Stability (per-entity classification), Fragility (stress scores), Sector Health (11 sectors × 6 signals), Assessment (alpha scores), and Universe (member selection).
4. **Execution** — The portfolio optimizer generates target weights; the equity executor computes deltas; the options layer deploys 17 regime-adaptive derivative strategies.
5. **Monitoring** — FastAPI serves all engine outputs to a React GUI dashboard. Kronos meta-orchestrator provides analytics and LLM chat interface.
""",
    },
    "pipeline": {
        "title": "Daily Pipeline & Orchestration",
        "content": """# Daily Pipeline & Orchestration

The system runs a full pipeline daily for each market region, orchestrated by the Market-Aware Daemon and DAG framework.

---

## Pipeline DAG

Each market (US_EQ, EU_EQ, ASIA_EQ) runs this pipeline daily, triggered by market state transitions detected by the daemon.

```mermaid
flowchart LR
    subgraph TRIGGER["Market State Trigger"]
        CAL["TradingCalendar\\ndetects POST_CLOSE"]
        DAEMON["Daemon creates\\nEngineRun row"]
    end

    subgraph PHASE1["Phase 1: Data"]
        ING_P["ingest_prices"]
        ING_F["ingest_factors"]
        COMP_R["compute_returns"]
        COMP_V["compute_volatility"]
        BUILD_W["build_numeric_windows"]
    end

    subgraph PHASE2["Phase 2: Signals"]
        UPD_PROF["update_profiles"]
        RUN_SIG["run_signals\\n(regime, STAB, fragility,\\nsector health, assessment)"]
    end

    subgraph PHASE3["Phase 3: Portfolio"]
        RUN_UNIV["run_universes\\n(CORE_EQ member selection,\\nlambda score ranking)"]
        RUN_BOOKS["run_books\\n(portfolio optimization,\\ntarget weights)"]
    end

    subgraph PHASE4["Phase 4: Execution"]
        RUN_EXEC["run_execution\\n(equity deltas to IBKR orders)"]
        RUN_OPT["run_options\\n(17 derivative strategies,\\nregime-adaptive allocation)"]
    end

    subgraph STATES["Run Phase States"]
        S1["WAITING_FOR_DATA"]
        S2["DATA_READY"]
        S3["SIGNALS_DONE"]
        S4["UNIVERSES_DONE"]
        S5["BOOKS_DONE"]
        S6["EXECUTION_DONE"]
        S7["OPTIONS_DONE"]
        S8["COMPLETED"]
    end

    CAL --> DAEMON
    DAEMON --> ING_P
    DAEMON --> ING_F

    ING_P --> COMP_R
    ING_P --> COMP_V
    ING_F -.-> COMP_R
    COMP_R --> BUILD_W
    COMP_V --> BUILD_W

    BUILD_W --> UPD_PROF
    UPD_PROF --> RUN_SIG
    BUILD_W --> RUN_SIG

    RUN_SIG --> RUN_UNIV
    RUN_UNIV --> RUN_BOOKS

    RUN_BOOKS --> RUN_EXEC
    RUN_EXEC --> RUN_OPT

    S1 --> S2
    S2 --> S3
    S3 --> S4
    S4 --> S5
    S5 --> S6
    S6 --> S7
    S7 --> S8

    classDef trigger fill:#44337a,stroke:#6b46c1,color:#e2e8f0
    classDef data fill:#1a365d,stroke:#2c5282,color:#e2e8f0
    classDef signal fill:#742a2a,stroke:#c53030,color:#e2e8f0
    classDef port fill:#234e52,stroke:#2c7a7b,color:#e2e8f0
    classDef exec fill:#553c00,stroke:#d69e2e,color:#e2e8f0
    classDef state fill:#1a202c,stroke:#4a5568,color:#a0aec0

    class CAL,DAEMON trigger
    class ING_P,ING_F,COMP_R,COMP_V,BUILD_W data
    class UPD_PROF,RUN_SIG signal
    class RUN_UNIV,RUN_BOOKS port
    class RUN_EXEC,RUN_OPT exec
    class S1,S2,S3,S4,S5,S6,S7,S8 state
```

---

## Market-Aware Daemon

The daemon (`prometheus/orchestration/market_aware_daemon.py`) is the production orchestrator running as a systemd service. It monitors multiple markets in a **follow-the-sun** pattern.

### How It Works

1. **Market State Detection** — The `TradingCalendar` determines the current state of each market: `PRE_MARKET`, `OPEN`, `POST_CLOSE`, `CLOSED`, `HOLIDAY`.
2. **DAG Construction** — When a market transitions to `POST_CLOSE`, the daemon builds a DAG of jobs for that market and date using `build_market_dag()`.
3. **Dependency Resolution** — The DAG framework resolves which jobs can run based on: completed dependencies, market state requirements, and retry limits.
4. **Job Execution** — Jobs run with retry logic (3 attempts, 5-minute delay, exponential backoff) and timeout monitoring (1 hour default).
5. **State Tracking** — All executions are persisted in the `job_executions` table for monitoring and debugging.

### Design Properties

- **Idempotent** — Jobs can be safely re-run without side effects.
- **Resilient** — Graceful failure handling with exponential backoff. Per-market DAGs execute independently so one region's failure doesn't block others.
- **Observable** — All executions tracked in the database with status, timing, and error details.
- **Non-blocking** — Per-market DAGs execute independently.

---

## DAG Framework

The DAG framework (`prometheus/orchestration/dag.py`) defines the job dependency graph for each market.

### Job Metadata

Each job carries:
- **job_type** — Logical type (e.g., `ingest_prices`, `compute_returns`, `run_regime`)
- **market_id** — Which market this job belongs to
- **required_state** — Market state needed to run (e.g., `POST_CLOSE`)
- **dependencies** — List of job_ids that must complete first
- **run_phase** — Maps to the `EngineRun` phase state machine
- **priority** — CRITICAL (tier 1), STANDARD (tier 2), OPTIONAL (tier 3)
- **timeout** — Maximum execution time (default 1 hour)

### Pipeline Phases

**Phase 1: Data** — Price ingestion, factor ingestion, returns computation, volatility computation, numeric window building. All jobs require `POST_CLOSE` state.

**Phase 2: Signals** — Profile updates and signal computation (regime, stability, fragility, sector health, assessment). Depends on Phase 1 completion.

**Phase 3: Portfolio** — Universe selection (CORE_EQ members via lambda scoring) and portfolio optimization (target weights). Depends on Phase 2.

**Phase 4: Execution** — Equity order generation (delta computation) and options strategy evaluation (17 strategies with regime-adaptive allocation). Depends on Phase 3.

---

## Engine Run State Machine

Each pipeline execution is tracked as an `EngineRun` row with a phase state machine:

```
WAITING_FOR_DATA → DATA_READY → SIGNALS_DONE → UNIVERSES_DONE → BOOKS_DONE → EXECUTION_DONE → OPTIONS_DONE → COMPLETED
```

Any phase can transition to `FAILED` if errors exceed retry limits. The GUI displays the current phase for each market's daily run on the Pipeline Status panel.

---

## Follow-the-Sun Schedule

Markets process in order of close time:

1. **ASIA_EQ** — Closes ~02:00 UTC, pipeline runs ~02:15 UTC
2. **EU_EQ** — Closes ~16:30 UTC, pipeline runs ~16:45 UTC
3. **US_EQ** — Closes ~21:00 UTC, pipeline runs ~21:15 UTC

EU and ASIA pipelines generate cross-market contagion signals, macro stress readings, and breadth data that feed into the US pipeline's regime and sector health engines.
""",
    },
    "engines": {
        "title": "Decision Engines",
        "content": """# Decision Engines

Six core engines process market data and generate the signals that drive portfolio construction and execution.

---

## Engine Chain

```mermaid
flowchart LR
    PRICES["Price Data\\nReturns\\nVolatility"] --> REGIME
    MACRO["FRED Macro\\nVIX · Rates\\nCredit · Claims"] --> REGIME
    EMBED["Joint Embeddings\\n(numeric + text)"] --> REGIME

    REGIME["Regime Engine\\nCARRY · NEUTRAL\\nRISK_OFF · CRISIS\\nRECOVERY"] --> STAB
    REGIME --> ASSESS

    EMBED --> STAB
    STAB["Stability Engine\\nSTABLE · TARGETABLE\\nWATCH · FRAGILE"] --> FRAG
    STAB --> ASSESS

    FRAG["Fragility Alpha\\nPer-entity\\nfragility score"] --> ASSESS

    PRICES --> SHI
    MACRO --> SHI
    SHI["Sector Health\\n11 sectors × 6 signals\\nSHI score 0-1"]

    ASSESS["Assessment Engine\\nInstrument alpha scores\\nPer-instrument per-horizon"] --> UNIV

    LAMBDA["Lambda Density\\nCluster dispersion\\nOpportunity density"] --> UNIV

    UNIV["Universe Engine\\nCORE_EQ member selection\\nLambda score ranking"] --> PORT

    SHI --> PORT
    PORT["Portfolio & Risk\\nTarget weights\\nRisk budgets\\nSector limits"]

    classDef input fill:#1a365d,stroke:#2c5282,color:#e2e8f0
    classDef engine fill:#742a2a,stroke:#c53030,color:#e2e8f0
    classDef output fill:#234e52,stroke:#2c7a7b,color:#e2e8f0

    class PRICES,MACRO,EMBED,LAMBDA input
    class REGIME,STAB,FRAG,SHI,ASSESS,UNIV engine
    class PORT output
```

---

## Regime Engine

**Module:** `prometheus/regime/engine.py`
**Output:** `RegimeState` per region per date → persisted to `regimes` table

The Regime Engine classifies the current market environment into one of five states:

- **CARRY / RISK_ON** — Low vol, supportive macro, risk assets outperforming
- **NEUTRAL** — Mixed signals, no strong directional bias
- **RECOVERY** — Improving sentiment after stress period
- **RISK_OFF** — Elevated vol, widening spreads, defensive positioning
- **CRISIS** — Extreme stress, funding disruption, flight to safety

### How It Works

1. **Joint embeddings** are computed from numeric windows (63 trading days of cross-asset returns, vol, correlations, factor returns) fused with text embeddings (macro news, policy statements).
2. **Regime prototypes** are discovered offline via clustering (GMM/HDBSCAN) on historical embeddings, then labeled using domain knowledge.
3. **Online classification** assigns the current embedding to the nearest prototype with a confidence score.
4. **Transition guards** prevent noisy regime flips: a **5 trading day minimum hold** suppresses transitions, unless the new regime is CRISIS (which always punches through immediately).
5. **Non-trading days** carry forward the previous regime.

### Downstream Consumers

The regime label flows into: strategy allocator budgets, sector put spread thresholds, options strategy activation maps, portfolio risk limits, and STAB/Assessment engines.

---

## Stability & Soft-Target Engine

**Module:** `prometheus/stability/engine.py`
**Output:** `StabilityVector` + `SoftTargetClass` per entity → persisted to `stability_vectors`, `soft_target_classes`

Classifies each entity (stock, ETF) into stability tiers:

- **STABLE** — Consistent behavior, low regime sensitivity
- **TARGETABLE** — Attractive risk/reward, suitable for active positioning
- **WATCH** — Elevated uncertainty, reduced position sizing
- **FRAGILE** — High regime sensitivity, avoid or hedge

Uses: regime embedding, entity-specific price patterns, factor exposures, and sector membership.

---

## Fragility Alpha

**Module:** `prometheus/stability/fragility.py`
**Output:** `fragility_score` per entity (0 = robust, 1 = extremely fragile) → persisted to `fragility_measures`

Computes a per-entity fragility score that captures how vulnerable a stock is to adverse market moves. Inputs include:

- Stability classification from STAB engine
- Scenario loss estimates under stress conditions
- Drawdown behavior relative to sector and market
- Earnings/event sensitivity

Fragility scores drive:
- **Bear put spread** target selection (FRAG >= 0.50 required)
- **Futures overlay** hedge sizing (aggregate FRAG triggers)
- Position sizing adjustments in the portfolio optimizer

---

## Sector Health Index (SHI)

**Module:** `prometheus/sector/health.py`
**Output:** SHI score ∈ [0, 1] per sector per date + signal breakdown → persisted to `sector_health_daily`

Computes health scores for all **11 GICS sectors** using 6 signals:

### The 6 Signals

1. **Trend** — ETF price / SMA200. Above 1.0 = healthy uptrend.
2. **Momentum** — Blended 1-month / 3-month / 6-month returns.
3. **Volatility** — 21-day realized vol percentile within trailing 252 days. High percentile = stressed.
4. **Drawdown** — Current drawdown from 252-day high. Deep drawdown = unhealthy.
5. **Breadth** — Fraction of sector constituents with positive 21-day returns.
6. **Macro Stress** — Sector-specific sensitivity to macro indicators.

### Macro Stress Profiles

Each sector has unique sensitivity weights to 5 macro indicators:

- **Real Estate** — rate_stress 0.90, credit_stress 0.80 (REITs highly rate-sensitive)
- **Financial Services** — credit_stress 0.90, financial_stress 0.80 (loan losses, counterparty)
- **Utilities** — rate_stress 0.80 (bond-proxy, high debt)
- **Consumer Cyclical** — labor_stress 0.70 (consumer spending)
- **Technology** — financial_stress 0.70 (risk-on/off sensitivity)
- **Consumer Defensive** — labor_stress 0.30 (low sensitivity, staples)
- **Energy** — financial_stress 0.40, credit_stress 0.40 (capital-intensive)
- **Healthcare** — financial_stress 0.30 (defensive)
- **Industrials** — labor_stress 0.60 (cyclical, capex-driven)
- **Communication Services** — financial_stress 0.60, rate_stress 0.40
- **Basic Materials** — financial_stress 0.50

Macro indicators: credit spreads (HY OAS), real yields (DFII10), financial stress (STLFSI2), yield curve (10Y-2Y), initial claims (ICSA).

### Scoring

Signals 1-5 are price-technical; signal 6 is fundamental/macro. All mapped to [-1, +1], then blended with weights: 0.15 each for signals 1-5, 0.25 for macro stress. Raw composite rescaled to [0, 1].

### Sector ETF Map

XLK (Technology) · XLF (Financial Services) · XLV (Healthcare) · XLI (Industrials) · XLY (Consumer Cyclical) · XLP (Consumer Defensive) · XLE (Energy) · XLU (Utilities) · XLRE (Real Estate) · XLC (Communication Services) · XLB (Basic Materials)

### SHI Action Zones

- **SHI > 0.55** — Healthy: maintain or increase exposure
- **SHI 0.40-0.55** — Caution: reduce sizing, sector put spread hedging activates
- **SHI 0.25-0.40** — Reduce: significant exposure reduction, defensive hedging active
- **SHI < 0.25** — Kill: allocator liquidates sector exposure entirely

---

## Assessment Engine

**Module:** `prometheus/assessment/engine.py`
**Output:** `instrument_scores` per instrument per horizon → persisted to `instrument_scores`

Combines regime context, stability classification, fragility scores, and joint embeddings to produce alpha scores per instrument across multiple time horizons. These scores feed into universe selection and portfolio optimization.

---

## Universe Engine

**Module:** `prometheus/universe/engine.py`
**Output:** `universe_members` with lambda scores → persisted to `universe_members`

Selects which instruments belong to the tradeable universe (CORE_EQ). Uses:
- Lambda (λ) opportunity density scores from cluster dispersion models
- Assessment engine alpha scores
- Liquidity and market cap filters

The universe is recomputed daily. Members receive a lambda score in the reasons JSONB field, which downstream strategies use for conviction-based sizing.

---

## Portfolio & Risk

**Module:** `prometheus/portfolio/optimizer.py`
**Output:** `target_portfolios` with optimized weights → persisted to `target_portfolios`

Takes universe members, their scores, SHI sector limits, and risk constraints to produce target portfolio weights via mean-variance optimization with:
- Sector exposure limits (informed by SHI)
- Position concentration limits
- Turnover constraints
- Transaction cost estimates
""",
    },
    "options": {
        "title": "Options & Derivatives Layer",
        "content": """# Options & Derivatives Layer

The regime-adaptive options overlay: 17 strategies across 5 categories, dynamically allocated based on market regime. All strategies use defined-risk structures (spreads) — no naked short options.

---

## Strategy Flow

```mermaid
graph TB
    subgraph SIGNALS["Signal Inputs"]
        REG["Market Regime\\n(RISK_ON · NEUTRAL · RISK_OFF · CRISIS)"]
        VIX["VIX Level\\n+ VIX3M Contango"]
        FRAG["Market Fragility\\n(aggregate score)"]
        SHI["Sector Health Index\\n(11 sectors x 6 signals)"]
        LAM["Lambda Scores\\n(per-stock conviction)"]
        STAB["STAB Scores\\n(per-stock stability)"]
        FRAG_S["Frag Scores\\n(per-stock fragility)"]
        EQPR["Equity & ETF Prices"]
    end

    ALLOC["Strategy Allocator\\nRegime to Category Budgets\\nDIRECTIONAL · INCOME · HEDGE\\nVOLATILITY · FUTURES\\nPortfolio Greeks Limits\\ndelta · gamma · theta · vega"]

    REG --> ALLOC
    VIX --> ALLOC
    FRAG --> ALLOC

    subgraph CAT_HEDGE["HEDGE (4 strategies)"]
        S_PP["Protective Put"]
        S_COLLAR["Collar"]
        S_SECTOR_PS["Sector Put Spread\\n(regime-adaptive thresholds)"]
        S_VIX["VIX Tail Hedge"]
    end

    subgraph CAT_INCOME["INCOME (5 strategies)"]
        S_CC["Covered Call"]
        S_SP["Short Put (CSP)"]
        S_IC["Iron Condor"]
        S_IB["Iron Butterfly"]
        S_WHEEL["Wheel"]
    end

    subgraph CAT_DIR["DIRECTIONAL (5 strategies)"]
        S_BCS["Bull Call Spread"]
        S_BPS["Bear Put Spread\\n(offensive bearish)"]
        S_SD["Sector Decline\\n(offensive sector shorts)"]
        S_MOM["Momentum Call"]
        S_LEAPS["LEAPS"]
    end

    subgraph CAT_VOL["VOLATILITY (2 strategies)"]
        S_SS["Straddle / Strangle"]
        S_CAL["Calendar Spread"]
    end

    subgraph CAT_FUT["FUTURES (1 strategy)"]
        S_FO["Futures Overlay"]
        S_FOPT["Futures Options"]
    end

    ALLOC --> CAT_HEDGE
    ALLOC --> CAT_INCOME
    ALLOC --> CAT_DIR
    ALLOC --> CAT_VOL
    ALLOC --> CAT_FUT

    SHI --> S_SECTOR_PS
    SHI --> S_SD
    LAM --> S_BCS
    LAM --> S_BPS
    LAM --> S_SP
    LAM --> S_LEAPS
    LAM --> S_WHEEL
    STAB --> S_BCS
    STAB --> S_BPS
    FRAG_S --> S_BPS
    VIX --> S_CC
    VIX --> S_VIX
    EQPR --> CAT_INCOME
    EQPR --> CAT_DIR

    LCM["Position Lifecycle Manager\\nRoll near expiry (< 14 DTE)\\nProfit target close (60%)\\nStop-loss management\\nRegime-flip exits"]

    CAT_HEDGE --> LCM
    CAT_INCOME --> LCM
    CAT_DIR --> LCM
    CAT_VOL --> LCM
    CAT_FUT --> LCM

    IBKR["IBKR Gateway\\n(paper / live)"]
    LCM --> IBKR

    classDef input fill:#1a365d,stroke:#2c5282,color:#e2e8f0
    classDef alloc fill:#44337a,stroke:#6b46c1,color:#e2e8f0
    classDef hedge fill:#742a2a,stroke:#c53030,color:#e2e8f0
    classDef income fill:#234e52,stroke:#2c7a7b,color:#e2e8f0
    classDef dir fill:#553c00,stroke:#d69e2e,color:#e2e8f0
    classDef vol fill:#2a4365,stroke:#3182ce,color:#e2e8f0
    classDef fut fill:#22543d,stroke:#38a169,color:#e2e8f0
    classDef lifecycle fill:#4a1d96,stroke:#7c3aed,color:#e2e8f0

    class REG,VIX,FRAG,SHI,LAM,STAB,FRAG_S,EQPR input
    class ALLOC alloc
    class S_PP,S_COLLAR,S_SECTOR_PS,S_VIX hedge
    class S_CC,S_SP,S_IC,S_IB,S_WHEEL income
    class S_BCS,S_BPS,S_SD,S_MOM,S_LEAPS dir
    class S_SS,S_CAL vol
    class S_FO,S_FOPT fut
    class LCM lifecycle
```

---

## Strategy Allocator

The `StrategyAllocator` (`prometheus/execution/strategy_allocator.py`) decides which strategies run and how much capital each receives.

### Capital Budget Templates (% of 15% NAV derivatives budget)

**RISK_ON:** DIRECTIONAL 35% · INCOME 40% · HEDGE 10% · VOLATILITY 5% · FUTURES 10%

**NEUTRAL:** DIRECTIONAL 5% · INCOME 50% · HEDGE 15% · VOLATILITY 15% · FUTURES 15%

**RECOVERY:** DIRECTIONAL 0% · INCOME 15% · HEDGE 45% · VOLATILITY 15% · FUTURES 25%

**RISK_OFF:** DIRECTIONAL 0% · INCOME 0% · HEDGE 50% · VOLATILITY 0% · FUTURES 50%

**CRISIS:** DIRECTIONAL 0% · INCOME 0% · HEDGE 60% · VOLATILITY 0% · FUTURES 40%

### Portfolio-Level Greeks Limits

- Max |delta| as % of NAV: **20%**
- Max portfolio gamma: **50,000**
- Min daily theta: **-$10,000**
- Max portfolio vega: **100,000**

### Always-On Strategies

`vix_tail_hedge` runs regardless of regime — 0.5% of NAV annualized (~3% annualized) on OTM VIX calls as catastrophe insurance.

---

## Regime → Strategy Activation

**RISK_ON:** bull_call_spread, momentum_call, leaps, covered_call, short_put, wheel, iron_condor, iron_butterfly, vix_tail_hedge, sector_put_spread

**NEUTRAL:** covered_call, short_put, iron_condor, iron_butterfly, calendar_spread, wheel, vix_tail_hedge, sector_put_spread, sector_decline

**RECOVERY:** collar, protective_put, covered_call, short_put, straddle_strangle, vix_tail_hedge, futures_overlay, sector_put_spread

**RISK_OFF:** protective_put, sector_put_spread, vix_tail_hedge, collar, futures_overlay, futures_option, bear_put_spread, sector_decline

**CRISIS:** protective_put, vix_tail_hedge, futures_overlay, futures_option, sector_put_spread, bear_put_spread, sector_decline

---

## All 17 Strategies — Detail

### HEDGE Category

**Protective Put** — Buy SPY puts when MHI < 0.40. OTM 5%, 45-90 DTE, 2% NAV budget. Rolls at 14 DTE. Closes when MHI recovers.

**Collar** — Buy protective put + sell covered call on large equity positions. Put delta 0.25, call delta 0.25. Min position $25K. 45-90 DTE. Rolls at 14 DTE.

**Sector Put Spread** — Defensive hedge on sector ETFs when SHI drops into "reduce" zone (SHI < 0.40, above kill threshold 0.25). 7% wide spreads. Regime-adaptive: threshold widens +0.10 in CRISIS/RISK_OFF for earlier activation. Max 1% NAV per sector.

**VIX Tail Hedge** — Always-on OTM VIX calls. Strike = VIX + 60% (far OTM, cheap tail insurance). 0.5% NAV per roll, 45-90 DTE. Rolls at 14 DTE.

### INCOME Category

**Covered Call** — Sell ~0.20 delta calls on largest equity positions. VIX >= 16 for entry (enough premium). 30-45 DTE, coverage ratio 20%, profit target 80%.

**Short Put (CSP)** — Sell 0.25 delta cash-secured puts on high-conviction names (lambda >= 0.60, STAB >= 0.50). Max 5% buying power per underlying, 10 concurrent positions. Profit target 50%.

**Iron Condor** — Sell OTM put + call spreads on SPY in calm markets (VIX < 22, FRAG < 0.45). ~7% OTM short strikes, $5 wings. 30-45 DTE, profit target 50%.

**Iron Butterfly** — ATM straddle + OTM wings on SPY. Very low vol only (VIX < 16). Higher credit but narrower profit zone. Max 2 positions.

**Wheel** — CSP → assignment → covered call cycle. Lambda >= 0.60, STAB >= 0.55. CSP at 0.28 delta, CC at 0.30 delta. 6% NAV per position, max 5 positions.

### DIRECTIONAL Category

**Bull Call Spread** — Long slightly-ITM call + short OTM call on high-conviction names. Lambda >= 0.65, STAB >= 0.50, RISK_ON only. 7% wide, 3% NAV risk per trade, max 9 positions. Profit target 60%.

**Bear Put Spread** — Long ATM put + short OTM put on fundamentally weak names. Lambda <= 0.35, STAB <= 0.40, FRAG >= 0.50. CRISIS/RISK_OFF only. 7% wide, 2% NAV risk, max 6 positions.

**Sector Decline** — Offensive put spreads on weak sector ETFs. SHI < 0.45, min 3/6 negative signals. Conviction-scaled sizing (more negative signals = larger position). Active in NEUTRAL too for early entry. 1.5% base NAV per trade.

**Momentum Call** — SPY ATM call spreads in confirmed bull markets. RISK_ON + VIX < 20 + positive 63d momentum. Addresses bull-year drag where vol-harvesting underperforms directional rips.

**LEAPS** — Deep ITM calls (0.70-0.80 delta, 6-12 months) as stock replacement. Frees capital while maintaining upside. Lambda >= 0.65, min $50K position value. Rolls at 90 DTE.

### VOLATILITY Category

**Straddle/Strangle** — Buy vol when cheap (VIX <= 18, FRAG >= 0.35). 5% OTM strangle legs preferred. 14-30 DTE, profit target 100%, max loss 50%.

**Calendar Spread** — Front-month short + back-month long at same strike. Profits from vol term structure (min 8% contango). Front 25-35 DTE, back 55-90 DTE.

### FUTURES Category

**Futures Overlay** — ES/NQ futures for portfolio beta management. FRAG >= 0.65 → short ES (max 30% hedge). Lambda aggregate >= 0.70 → long ES (max 15% leverage). Multiplier: $50/point.

**Futures Options** — VX call spreads when FRAG low (expect vol expansion) + ES put spreads as cheap downside protection (MHI < 0.45). Defined-risk FOP trades.

---

## Position Lifecycle Manager

Every open option position is managed by the lifecycle system:

- **Roll near expiry** — Positions with < 14 DTE are rolled to the next monthly expiry (except LEAPS which roll at 90 DTE).
- **Profit target close** — Most strategies close at 50-80% of max profit. Bull/bear spreads close at 60%.
- **Stop-loss management** — Iron condors and butterflies close at 14 DTE to avoid gamma risk.
- **Regime-flip exits** — Directional strategies (bull call, momentum call) close if regime flips away from their activation regime. Bear put spreads close if regime turns bullish.
""",
    },
    "database": {
        "title": "Database Schema",
        "content": """# Database Schema

Prometheus v2 uses two PostgreSQL databases on the same local server: **runtime_db** for live engine state and execution, and **historical_db** for market data, embeddings, and text.

---

## Schema Overview

```mermaid
graph LR
    subgraph RUNTIME["runtime_db (~30 tables)"]
        subgraph CORE["Core Entities"]
            M["markets"]
            I["issuers"]
            INS["instruments"]
            P["portfolios"]
            S["strategies"]
        end

        subgraph ENG["Engine Outputs"]
            REG["regimes\\n(label, confidence,\\nembedding)"]
            SV["stability_vectors\\n(per-entity components)"]
            FM["fragility_measures\\n(score, scenario losses)"]
            STC["soft_target_classes\\n(STABLE/TARGETABLE/\\nWATCH/FRAGILE)"]
            IS["instrument_scores\\n(alpha per horizon)"]
            SHD["sector_health_daily\\n(SHI score, raw_composite,\\n6-signal JSONB)"]
            UM["universe_members\\n(entity_id, score,\\nreasons JSONB)"]
            TP["target_portfolios\\n(optimized weights)"]
        end

        subgraph EXECUTION["Execution & Tracking"]
            ER["engine_runs\\n(phase state machine)"]
            JE["job_executions\\n(DAG job tracking)"]
            ORD["orders"]
            FIL["fills"]
            PS["positions_snapshots"]
            ED["engine_decisions"]
            EA["executed_actions"]
            DO["decision_outcomes"]
        end

        subgraph CONFIG["Configuration"]
            EC["engine_configs\\n(versioned per engine)"]
            MOD["models\\n(trained artifacts)"]
        end
    end

    subgraph HISTORICAL["historical_db (~15 tables)"]
        subgraph MKTDATA["Market Data"]
            PD["prices_daily"]
            RD["returns_daily"]
            VD["volatility_daily"]
            FD["factors_daily"]
            IFD["instrument_factors_daily"]
            CP["correlation_panels"]
        end

        subgraph TEXTDATA["Text & Events"]
            NA["news_articles"]
            NL["news_links"]
            FI["filings"]
            ECL["earnings_calls"]
            ME["macro_events"]
        end

        subgraph EMBEDDINGS["Embeddings"]
            TE["text_embeddings"]
            NWE["numeric_window_embeddings"]
            JNE["joint_embeddings"]
        end
    end

    classDef core fill:#234e52,stroke:#2c7a7b,color:#e2e8f0
    classDef engine fill:#742a2a,stroke:#c53030,color:#e2e8f0
    classDef exec fill:#553c00,stroke:#d69e2e,color:#e2e8f0
    classDef config fill:#44337a,stroke:#6b46c1,color:#e2e8f0
    classDef hist fill:#1a365d,stroke:#2c5282,color:#e2e8f0

    class M,I,INS,P,S core
    class REG,SV,FM,STC,IS,SHD,UM,TP engine
    class ER,JE,ORD,FIL,PS,ED,EA,DO exec
    class EC,MOD config
    class PD,RD,VD,FD,IFD,CP,NA,NL,FI,ECL,ME,TE,NWE,JNE hist
```

---

## Runtime DB — Key Tables

### Core Entities

- **markets** — Market definitions (US_EQ, EU_EQ, ASIA_EQ) with trading hours, timezone, calendar
- **issuers** — Companies/entities with classification data (sector, industry, market cap tier)
- **instruments** — Tradeable securities (stocks, ETFs, options, futures) linked to issuers
- **portfolios** — Portfolio definitions with strategy assignment
- **strategies** — Strategy metadata and configuration references

### Engine Outputs

- **regimes** — One row per (region, date). Stores regime label (CARRY/NEUTRAL/RISK_OFF/CRISIS/RECOVERY), confidence score, embedding vector, and metadata JSONB. Transition suppression info recorded in metadata.

- **stability_vectors** — Per-entity stability component scores. Used by STAB engine to classify into soft-target tiers.

- **fragility_measures** — Per-entity fragility score (0-1) plus scenario loss estimates under stress conditions. Drives bear put spread targeting and futures overlay sizing.

- **soft_target_classes** — Per-entity classification: STABLE, TARGETABLE, WATCH, FRAGILE. Derived from stability vectors + regime context.

- **instrument_scores** — Alpha scores per instrument per time horizon. Output of Assessment Engine, consumed by Universe and Portfolio engines.

- **sector_health_daily** — Per-sector per-date: SHI score ∈ [0,1], raw_composite, and a signals JSONB column containing all 6 signal values (trend, momentum, volatility, drawdown, breadth, macro_stress). Powers the GUI sector health panel and options hedging decisions.

- **universe_members** — Which instruments are in the tradeable universe (CORE_EQ) on each date. Includes entity_id, composite score, and reasons JSONB with lambda scores and filter results.

- **target_portfolios** — Optimized portfolio weights per strategy/date. Output of the portfolio optimizer, consumed by equity execution.

### Execution & Tracking

- **engine_runs** — One row per (market, date) pipeline execution. Tracks phase state machine: WAITING_FOR_DATA → DATA_READY → ... → COMPLETED or FAILED.

- **job_executions** — DAG job-level tracking. Each job has: execution_id, job_type, dag_id, status (PENDING/RUNNING/SUCCESS/FAILED/SKIPPED), attempt number, timing, error details.

- **orders** — Trade orders submitted to broker. Linked to strategy and engine run.

- **fills** — Execution fills received from broker. Linked to orders.

- **positions_snapshots** — Point-in-time portfolio holdings. Snapshotted daily after execution.

- **engine_decisions** — Structured decision records from engines (what was decided and why).

- **executed_actions** — Actions taken based on decisions (trades placed, positions adjusted).

- **decision_outcomes** — Post-hoc evaluation of decision quality (was the trade profitable, did the signal work).

### Configuration

- **engine_configs** — Versioned configuration per engine. Supports A/B testing of engine parameters.
- **models** — Trained model artifacts (embeddings, clustering models) with version tracking.

---

## Historical DB — Key Tables

### Market Data

- **prices_daily** — OHLCV + adjusted close for all instruments. Primary data source for all engines.
- **returns_daily** — Computed daily returns (simple and log) from prices.
- **volatility_daily** — Realized volatility at multiple windows (5d, 10d, 21d, 63d).
- **factors_daily** — Factor returns (value, momentum, carry, quality, size) per date.
- **instrument_factors_daily** — Per-instrument factor exposures.
- **correlation_panels** — Rolling correlation matrices (within and cross asset class).

### Text & Events

- **news_articles** — Financial news with source, publication date, relevance scores.
- **news_links** — Links between news articles and entities/instruments.
- **filings** — SEC/regulatory filings.
- **earnings_calls** — Earnings call transcripts and sentiment.
- **macro_events** — Scheduled macro events (FOMC, CPI, payrolls) with actual/expected values.

### Embeddings

- **text_embeddings** — Dense vector representations of text content.
- **numeric_window_embeddings** — Price/return pattern embeddings from sliding windows.
- **joint_embeddings** — Fused text + numeric embeddings via Joint Encoder. Primary input to Regime and STAB engines.

---

## Schema Management

All schema changes are managed via **Alembic migrations** in `migrations/versions/`. Each migration is numbered sequentially (e.g., `0082_sector_health_daily.py`). Migrations are applied automatically on deployment.
""",
    },
    "infrastructure": {
        "title": "Infrastructure & Operations",
        "content": """# Infrastructure & Operations

Single-server deployment running Fedora Linux with systemd services, PostgreSQL, and IBKR Gateway.

---

## Service Architecture

```mermaid
flowchart TB
    subgraph INTERNET["Internet"]
        BROWSER["Browser\\n(HTTPS :8443)"]
        IBKR_GW["IBKR Gateway\\n(TWS API :4001)"]
        YAHOO_API["Yahoo Finance API"]
        FRED_API["FRED API"]
    end

    subgraph SERVER["Prometheus Server (Fedora Linux)"]
        subgraph SYSTEMD["systemd Services"]
            NGINX["nginx\\nHTTPS reverse proxy\\n:8443 → :8000\\nServes React static build"]
            API_SVC["prometheus-api.service\\nFastAPI / Uvicorn\\n:8000\\nREST API + WebSocket"]
            DAEMON_SVC["prometheus-daemon.service\\nMarket-Aware Daemon\\nDAG orchestration\\nFollow-the-sun scheduling"]
        end

        subgraph DATA["Data Layer"]
            PG["PostgreSQL\\nruntime_db + historical_db\\nLocal socket connection"]
        end

        subgraph STATIC["Static Assets"]
            REACT["React Build\\n(/opt/prometheus/prometheus_v2/\\nprometheus_web/dist/)\\nVite + Tailwind + Recharts"]
        end
    end

    BROWSER --> NGINX
    NGINX --> API_SVC
    NGINX --> REACT
    API_SVC --> PG
    DAEMON_SVC --> PG
    DAEMON_SVC --> IBKR_GW
    DAEMON_SVC --> YAHOO_API
    DAEMON_SVC --> FRED_API

    classDef internet fill:#2d3748,stroke:#4a5568,color:#e2e8f0
    classDef service fill:#44337a,stroke:#6b46c1,color:#e2e8f0
    classDef data fill:#1a365d,stroke:#2c5282,color:#e2e8f0

    class BROWSER,IBKR_GW,YAHOO_API,FRED_API internet
    class NGINX,API_SVC,DAEMON_SVC service
    class PG,REACT data
```

---

## systemd Services

### prometheus-api.service

- **Process:** FastAPI application running under Uvicorn
- **Port:** 8000 (HTTP, proxied by nginx)
- **Responsibilities:**
  - REST API for all engine outputs (regime, sectors, pipelines, positions)
  - WebSocket connections for real-time updates
  - Docs endpoint serving architecture markdown files
  - Health check endpoint at `/api/status/health`
- **Restart policy:** Always restart on failure

### prometheus-daemon.service

- **Process:** Market-aware DAG orchestrator
- **Responsibilities:**
  - Monitors market state transitions across US_EQ, EU_EQ, ASIA_EQ
  - Builds and executes daily pipeline DAGs per market
  - Manages job retries, timeouts, and state tracking
  - Triggers data ingestion, engine runs, and execution phases
- **Restart policy:** Always restart on failure

### nginx

- **Port:** 8443 (HTTPS with self-signed cert)
- **Configuration:**
  - Reverse proxy `/api/*` → `localhost:8000`
  - Serve React static build from dist directory
  - SPA fallback: all non-API routes → `index.html`
  - WebSocket upgrade support for live data feeds

---

## Deployment Paths

**Development:** `/home/feanor/coding/prometheus_v2/`

**Production:** `/opt/prometheus/prometheus_v2/`

### Deployment Workflow

1. Develop and test in dev path
2. Build frontend: `cd prometheus_web && npm run build`
3. Copy to production: `sudo cp -r prometheus_web/dist/* /opt/.../prometheus_web/dist/`
4. Copy Python changes: `sudo rsync -a prometheus/ /opt/.../prometheus/`
5. Restart services: `sudo systemctl restart prometheus-api prometheus-daemon`
6. Verify: `./start_production.sh` (health-checks all endpoints)

### start_production.sh

The startup script:
- Checks if services are already running (skips sudo if all up)
- Starts only stopped services
- Health-checks API (30s timeout) and HTTPS (10s timeout)
- Verifies data endpoints (regime, sectors, pipelines)
- Prints final "Prometheus v2 is live" banner

---

## Modes of Operation

### LIVE
- **Broker:** LiveBroker → IBKR live gateway (port 7496)
- **Data:** Real-time market data
- **Purpose:** Production trading with real capital
- **Status:** Not yet activated

### PAPER (Current Mode)
- **Broker:** PaperBroker → IBKR paper gateway (port 4001)
- **Data:** Real-time market data
- **Purpose:** Live paper trading for system validation
- **Status:** Active since March 2026

### BACKTEST_PY
- **Broker:** BacktestBroker + TimeMachine + MarketSimulator
- **Data:** Historical data with no-lookahead guarantee
- **Purpose:** Strategy validation and parameter tuning
- **Fills:** Simulated with configurable slippage models

### BACKTEST_CPP
- **Broker:** prom2_cpp C++ backend
- **Data:** In-memory historical data
- **Purpose:** Fast research runs and parameter sweeps
- **Performance:** ~100x faster than Python backtester for numerical-heavy workloads

---

## Monitoring & Observability

### GUI Dashboard (React)

Bloomberg-style command center with panels for:
- **Regime & Market** — Current regime, regime history, market health indicators, sector health bars
- **Portfolio** — Holdings, P&L, sector exposure, greeks
- **Pipeline Status** — Engine run phases per market, job execution status
- **Options** — Open option positions, strategy allocations, P&L by strategy
- **Docs** — This architecture documentation with interactive mermaid diagrams

### API Endpoints

All engine outputs available via REST at `/api/status/*`:
- `/api/status/regime` — Current and historical regime data
- `/api/status/sectors` — Sector health scores and signal breakdown
- `/api/status/pipeline` — Engine run status per market
- `/api/status/positions` — Current portfolio holdings
- `/api/status/docs/{key}` — Architecture documentation pages

### Logging

Structured logging via `prometheus.core.logging` with:
- Per-module loggers
- Log levels: DEBUG, INFO, WARNING, ERROR
- Rotation and retention policies via systemd journal

---

## External Dependencies

- **IBKR Gateway** — TWS/IB Gateway for order execution and position sync
- **Yahoo Finance** — Daily OHLCV price data for all instruments
- **FRED API** — Macro indicators: STLFSI2 (financial stress), DGS2/DGS10 (yields), DFII10 (real yields), ICSA (initial claims), BAMLH0A0HYM2 (HY OAS)
- **PostgreSQL** — Local database server (two databases)
- **Node.js/npm** — Frontend build toolchain (Vite, React, Tailwind)
- **Python 3.12** — Backend runtime
""",
    },
}


@router.get("/docs")
async def list_docs() -> List[Dict[str, str]]:
    """List available documentation pages."""
    return [{"key": k, "title": v["title"]} for k, v in _PROMETHEUS_DOCS.items()]


@router.get("/docs/{key}")
async def get_doc(key: str) -> Dict[str, str]:
    """Return a single documentation page."""
    doc = _PROMETHEUS_DOCS.get(key)
    if not doc:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Doc '{key}' not found")
    return doc
