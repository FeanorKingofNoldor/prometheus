"""Prometheus v2 – Monitoring Status API.

This module provides REST endpoints for the Prometheus C2 UI to query
system status, engine states, and real-time pipeline information.

Currently returns mock/template data to enable UI development. Will be
progressively wired to real engines and runtime DB as they mature.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Mapping

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from apathis.core.database import get_db_manager
from apathis.core.logging import get_logger
from apathis.core.market_state import MarketState, get_market_state, get_next_state_transition
from apathis.core.markets import MARKETS_BY_REGION
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

    This implementation derives aggregate exposure metrics from the
    latest ``portfolio_risk_reports`` rows and a simple global stability
    index from the most recent ``stability_vectors`` snapshot. P&L
    fields remain placeholders until a dedicated P&L aggregation path is
    implemented.
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

    return SystemOverview(
        pnl_today=0.0,
        pnl_mtd=0.0,
        pnl_ytd=0.0,
        max_drawdown=0.0,
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
        except Exception as exc:
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

    # P&L aggregation is not yet implemented in the engine; return zeros
    # for now.
    pnl = {"today": 0.0, "mtd": 0.0, "ytd": 0.0}

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

    orders: List[Dict[str, Any]] = []
    fills: List[Dict[str, Any]] = []
    positions: List[Dict[str, Any]] = []

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            # Orders
            where_clauses = ["portfolio_id = %s"]
            params: list[object] = [port_id]
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

    where_clauses = ["engine_name = 'EXECUTION'", "strategy_id = %s"]
    params: list[object] = [port_id]

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

    port_by_date: Dict[str, float] = {}
    for d, mv in port_rows:
        port_by_date[str(d)] = float(mv or 0.0)

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
    # Include all portfolio dates, plus any benchmark dates in range
    min_d, max_d = all_dates[0], all_dates[-1]
    merged_dates = sorted(set(all_dates) | {d for d in bench_by_date if min_d <= d <= max_d})

    out: List[EquityPoint] = []
    for d in merged_dates:
        out.append(EquityPoint(
            date=d,
            portfolio=port_by_date.get(d),
            benchmark=bench_by_date.get(d),
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

    # 1) Get latest positions (instrument + weight + market_value)
    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT instrument_id, quantity, avg_cost, market_value,
                       CASE WHEN SUM(market_value) OVER () > 0
                            THEN market_value / SUM(market_value) OVER ()
                            ELSE 0 END AS weight
                FROM (
                    SELECT DISTINCT ON (instrument_id)
                           instrument_id, quantity, avg_cost, market_value
                    FROM positions_snapshots
                    WHERE portfolio_id = %s AND as_of_date IS NOT NULL
                    ORDER BY instrument_id, as_of_date DESC, timestamp DESC
                ) latest
                """,
                (port_id,),
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
    # Build aligned return matrix
    common_len = min((len(r) for r in returns_by_inst.values()), default=0)
    port_vol_20 = port_vol_60 = var_95 = es_95 = max_dd = None

    if common_len >= 20 and len(returns_by_inst) == len(instruments):
        ret_matrix = np.column_stack([
            returns_by_inst[inst][-common_len:] for inst in instruments
        ])

        # Portfolio daily returns (weighted)
        port_daily = ret_matrix @ weights

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

    # 5) HHI (Herfindahl-Hirschman Index)
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
