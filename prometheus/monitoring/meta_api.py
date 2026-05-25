"""Prometheus v2 – Meta APIs (Iris Chat + Geo).

This module provides:
- Iris Chat API for LLM-powered meta-orchestration
- Geo API for world map visualization with country-level data
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

from apatheon.core.config import get_config
from apatheon.core.database import get_db_manager
from apatheon.core.logging import get_logger
from fastapi import APIRouter, Body, HTTPException, Path, Query
from pydantic import BaseModel, Field

from prometheus.assessment.model_basic import BasicAssessmentModel
from prometheus.books.registry import (
    AllocatorSleeveSpec,
    HedgeEtfSleeveSpec,
    LongEquitySleeveSpec,
    load_book_registry,
)
from prometheus.execution.policy import load_execution_policy_artifact
from prometheus.meta.policy import MetaPolicySelection, load_meta_policy_artifact
from prometheus.pipeline.tasks import (
    _load_daily_portfolio_risk_config,
    _load_daily_universe_lambda_config,
)

logger = get_logger(__name__)


iris_router = APIRouter(prefix="/api/iris", tags=["iris"])
geo_router = APIRouter(prefix="/api/geo", tags=["geo"])
meta_router = APIRouter(prefix="/api/meta", tags=["meta"])


# ============================================================================
# Iris Chat Models
# ============================================================================


class IrisRequest(BaseModel):
    """Request to Iris chat interface."""

    question: str
    context: Dict[str, Any] = Field(default_factory=dict)


class IrisProposal(BaseModel):
    """Action proposal from Iris."""

    proposal_id: str
    action_type: str  # backtest, config_change, synthetic_dataset
    description: str
    parameters: Dict[str, Any] = Field(default_factory=dict)
    risk_level: str = "LOW"


class IrisResponse(BaseModel):
    """Response from Iris chat."""

    answer: str
    proposals: List[IrisProposal] = Field(default_factory=list)
    sources: List[str] = Field(default_factory=list)


# ============================================================================
# Geo Models
# ============================================================================


class CountryStatus(BaseModel):
    """Country-level status for world map."""

    country_code: str
    country_name: str
    stability_index: float
    fragility_risk: str  # LOW, MODERATE, HIGH
    exposure: float = 0.0
    num_positions: int = 0


class CountryDetail(BaseModel):
    """Detailed country information."""

    country_code: str
    country_name: str
    stability_index: float
    fragility_risk: str
    regime: Optional[str] = None
    exposures: Dict[str, float] = Field(default_factory=dict)
    top_positions: List[Dict[str, Any]] = Field(default_factory=list)


# ============================================================================
# Meta Config Models
# ============================================================================


class EngineConfig(BaseModel):
    """Engine configuration snapshot."""

    engine_name: str
    config_version: str
    parameters: Dict[str, Any] = Field(default_factory=dict)
    last_updated: str


class ConfigRow(BaseModel):
    """Single editable config row for the Settings Configuration panel."""

    key: str
    value: str
    section: str
    editable: bool = False


class EnginePerformance(BaseModel):
    """Engine performance metrics."""

    engine_name: str
    period: str
    metrics: Dict[str, float] = Field(default_factory=dict)
    by_regime: Dict[str, Dict[str, float]] = Field(default_factory=dict)


class EngineParameterItem(BaseModel):
    """Single engine parameter with current value and rationale."""

    key: str
    value: Any = None
    source: str
    detrimental_reason: str


class EngineParameterGroup(BaseModel):
    """Parameter group for one engine."""

    engine_id: str
    engine_label: str
    parameters: List[EngineParameterItem] = Field(default_factory=list)


class EngineParametersResponse(BaseModel):
    """All settings-page engine parameter groups."""

    generated_at: str
    engines: List[EngineParameterGroup] = Field(default_factory=list)


class MetaPolicySelectionModel(BaseModel):
    """Book+sleeve selection in a meta policy artifact."""

    book_id: str
    sleeve_id: Optional[str] = None


class MetaPolicyArtifactResponse(BaseModel):
    """Meta policy artifact for a single market."""

    market_id: str
    version: Optional[str] = None
    updated_at: Optional[str] = None
    updated_by: Optional[str] = None

    default: MetaPolicySelectionModel
    situations: Dict[str, MetaPolicySelectionModel] = Field(default_factory=dict)


class MetaPolicyDecisionResponse(BaseModel):
    """Recent META_POLICY_V1 decision record."""

    decision_id: str
    run_id: Optional[str] = None
    market_id: str
    as_of_date: date

    selected_book_id: Optional[str] = None
    selected_sleeve_id: Optional[str] = None

    market_situation: Optional[str] = None
    policy_version: Optional[str] = None
    created_at: Optional[str] = None


# ============================================================================
# Iris Endpoints
# ============================================================================


@iris_router.post("/chat", response_model=IrisResponse)
def iris_chat(request: IrisRequest = Body(...)) -> IrisResponse:
    """Interact with Iris meta-orchestrator.

    Iris can explain system behavior, propose experiments, and analyze
    engine performance. It cannot directly execute changes - all actions
    require explicit approval via the Control API.
    """
    from prometheus.monitoring.iris_service import iris_chat as _iris_chat

    history = request.context.get("history", []) if request.context else []

    try:
        result = _iris_chat(question=request.question, history=history)
        return IrisResponse(
            answer=result["answer"],
            proposals=[IrisProposal(**p) for p in result.get("proposals", [])],
            sources=result.get("sources", []),
        )
    except Exception as exc:
        logger.exception("[iris] Chat failed: %s", exc)
        return IrisResponse(
            answer=f"Iris encountered an error: {exc}. Check LLM configuration in Settings.",
            proposals=[],
            sources=[],
        )


# ============================================================================
# LLM Configuration Endpoints
# ============================================================================


class LLMConfigRequest(BaseModel):
    """Request to configure the LLM provider."""

    provider: str  # "ollama" | "openai"
    model: Optional[str] = None
    api_key: Optional[str] = None
    base_url: Optional[str] = None


@iris_router.get("/llm/config")
async def get_llm_config() -> Dict[str, Any]:
    """Return current LLM configuration (no secrets)."""
    from apatheon.llm.gateway import get_llm_info
    return get_llm_info()


@iris_router.post("/llm/config")
async def set_llm_config(request: LLMConfigRequest = Body(...)) -> Dict[str, Any]:
    """Reconfigure the LLM provider at runtime."""
    from apatheon.llm.gateway import configure_llm

    try:
        health = configure_llm(
            provider=request.provider,
            model=request.model,
            api_key=request.api_key,
            base_url=request.base_url,
        )
        return {"status": "ok", "health": health}
    except Exception as exc:
        logger.exception("[iris] LLM config failed: %s", exc)
        raise HTTPException(status_code=400, detail=str(exc))


@iris_router.get("/llm/health")
async def llm_health() -> Dict[str, Any]:
    """Run a health check on the current LLM provider."""
    from apatheon.llm.gateway import get_llm

    try:
        provider = get_llm()
        return provider.health_check()
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


# ============================================================================
# Geo Endpoints
# ============================================================================


@geo_router.get("/countries", response_model=List[CountryStatus])
async def get_countries(
    as_of_date: Optional[date] = Query(None, description="As-of date filter")
) -> List[CountryStatus]:
    """Return country-level status for world map visualization."""
    return [
        CountryStatus(
            country_code="US",
            country_name="United States",
            stability_index=0.85,
            fragility_risk="LOW",
            exposure=0.58,
            num_positions=125,
        ),
        CountryStatus(
            country_code="GB",
            country_name="United Kingdom",
            stability_index=0.78,
            fragility_risk="MODERATE",
            exposure=0.12,
            num_positions=22,
        ),
        CountryStatus(
            country_code="DE",
            country_name="Germany",
            stability_index=0.82,
            fragility_risk="LOW",
            exposure=0.08,
            num_positions=18,
        ),
        CountryStatus(
            country_code="JP",
            country_name="Japan",
            stability_index=0.88,
            fragility_risk="LOW",
            exposure=0.15,
            num_positions=35,
        ),
        CountryStatus(
            country_code="CN",
            country_name="China",
            stability_index=0.72,
            fragility_risk="HIGH",
            exposure=0.02,
            num_positions=4,
        ),
    ]


@geo_router.get("/country/{country_code}", response_model=CountryDetail)
async def get_country_detail(
    country_code: str = Path(..., description="ISO country code"),
    as_of_date: Optional[date] = Query(None, description="As-of date"),
) -> CountryDetail:
    """Return detailed country information."""
    return CountryDetail(
        country_code=country_code,
        country_name="United States" if country_code == "US" else country_code,
        stability_index=0.85,
        fragility_risk="LOW",
        regime="STABLE_EXPANSION",
        exposures={
            "equity": 0.52,
            "fixed_income": 0.04,
            "fx": 0.02,
        },
        top_positions=[
            {
                "instrument_id": "AAPL",
                "weight": 0.185,
                "market_value": 925000.0,
            },
            {
                "instrument_id": "MSFT",
                "weight": 0.230,
                "market_value": 1152000.0,
            },
        ],
    )


# ============================================================================
# Meta Config Endpoints
# ============================================================================


@meta_router.get("/configs", response_model=List[ConfigRow])
async def get_configs() -> List[ConfigRow]:
    """Return current engine configurations as editable config rows.

    Values are fetched from live config sources (YAML/env loaders).
    The ``section``, ``key``, ``value``, ``editable`` format matches
    the Settings page ConfigRow interface.
    """
    region = "US"
    market_id = "US_EQ"

    daily_universe_cfg = _load_daily_universe_lambda_config(region)
    daily_portfolio_cfg = _load_daily_portfolio_risk_config(region)
    exec_policy_artifact = load_execution_policy_artifact()
    exec_policy = exec_policy_artifact.policy
    exec_risk = get_config().execution_risk
    meta_policy_artifact = load_meta_policy_artifact()

    def _risk_val(v: float) -> str:
        return "unconstrained" if v == 0.0 else str(v)

    rows: List[ConfigRow] = [
        # Universe Engine
        ConfigRow(section="Universe", key=f"{region}.lambda_score_weight", value=str(daily_universe_cfg.score_weight), editable=True),
        ConfigRow(section="Universe", key=f"{region}.lambda_experiment_id", value=str(daily_universe_cfg.experiment_id or ""), editable=False),
        ConfigRow(section="Universe", key=f"{region}.lambda_predictions_csv", value=str(daily_universe_cfg.predictions_csv or ""), editable=False),
        # Portfolio Engine
        ConfigRow(section="Portfolio", key=f"{region}.hazard_profile", value=str(daily_portfolio_cfg.hazard_profile), editable=False),
        ConfigRow(section="Portfolio", key=f"{region}.meta_budget_enabled", value=str(daily_portfolio_cfg.meta_budget_enabled), editable=True),
        ConfigRow(section="Portfolio", key=f"{region}.meta_budget_alpha", value=str(daily_portfolio_cfg.meta_budget_alpha), editable=True),
        ConfigRow(section="Portfolio", key=f"{region}.meta_budget_min", value=str(daily_portfolio_cfg.meta_budget_min), editable=True),
        # Execution Engine
        ConfigRow(section="Execution", key="policy.turnover.one_way_limit", value=str(exec_policy.turnover.one_way_limit), editable=True),
        ConfigRow(section="Execution", key="policy.no_trade_band_bps", value=str(exec_policy.no_trade_band_bps), editable=True),
        ConfigRow(section="Execution", key="policy.cash_buffer_weight", value=str(exec_policy.cash_buffer_weight), editable=True),
        # Execution Risk
        ConfigRow(section="Execution Risk", key="risk.max_order_notional", value=_risk_val(exec_risk.max_order_notional), editable=True),
        ConfigRow(section="Execution Risk", key="risk.max_position_notional", value=_risk_val(exec_risk.max_position_notional), editable=True),
        ConfigRow(section="Execution Risk", key="risk.max_leverage", value=_risk_val(exec_risk.max_leverage), editable=True),
    ]

    policy = meta_policy_artifact.policies.get(market_id)
    if policy is not None:
        rows.extend([
            ConfigRow(section="Meta Policy", key=f"{market_id}.default.book_id", value=str(policy.default.book_id), editable=False),
            ConfigRow(section="Meta Policy", key=f"{market_id}.default.sleeve_id", value=str(policy.default.sleeve_id or ""), editable=False),
        ])

    return rows


@meta_router.get("/weekly_report")
async def get_weekly_report() -> Dict[str, Any]:
    """Iris weekly trade monitoring report."""
    from prometheus.meta.trade_monitor import compute_weekly_report, format_weekly_report

    db = get_db_manager()
    report = compute_weekly_report(db, date.today())
    text = format_weekly_report(report)

    return {
        "period_start": report.period_start.isoformat(),
        "period_end": report.period_end.isoformat(),
        "nav": report.current_nav,
        "n_positions": report.n_positions,
        "n_entries": report.n_entries,
        "n_exits": report.n_exits,
        "turnover_pct": report.turnover_pct,
        "regime": report.regime_label,
        "forward_signal": report.forward_signal,
        "portfolio_hit_rate": report.portfolio_hit_rate,
        "anomalies": report.anomalies,
        "top_winners": [
            {"instrument_id": p.instrument_id, "pnl_pct": round(p.pnl_pct, 4),
             "pnl": round(p.unrealized_pnl, 2), "sector": p.sector}
            for p in report.top_winners
        ],
        "top_losers": [
            {"instrument_id": p.instrument_id, "pnl_pct": round(p.pnl_pct, 4),
             "pnl": round(p.unrealized_pnl, 2), "sector": p.sector}
            for p in report.top_losers
        ],
        "sector_pnl": {k: round(v, 2) for k, v in report.sector_pnl.items()},
        "formatted_report": text,
    }


@meta_router.get("/trade_journal")
async def get_trade_journal(
    lookback_days: int = Query(63, ge=7, le=252),
) -> Dict[str, Any]:
    """Trade journal analysis: systematic patterns in trade outcomes."""
    from prometheus.meta.trade_journal import compute_journal_analysis

    db = get_db_manager()
    return compute_journal_analysis(db, lookback_days=lookback_days)


@meta_router.get("/feedback")
async def get_meta_feedback(
    lookback_days: int = Query(63, ge=7, le=252),
) -> Dict[str, Any]:
    """Meta learning feedback: how are decisions performing vs expectations?"""
    from prometheus.meta.feedback import compute_feedback_report

    db = get_db_manager()
    report = compute_feedback_report(db, date.today(), lookback_days=lookback_days)

    return {
        "as_of_date": report.as_of_date.isoformat(),
        "portfolio_hit_rate": report.portfolio_hit_rate,
        "assessment_accuracy": report.assessment_accuracy,
        "risk_override_pct": report.risk_override_pct,
        "avg_decision_return": report.avg_decision_return,
        "insights": [
            {
                "category": i.category,
                "severity": i.severity,
                "message": i.message,
                "metric_name": i.metric_name,
                "metric_value": round(i.metric_value, 4),
                "benchmark": round(i.benchmark, 4),
                "deviation": round(i.deviation, 4),
            }
            for i in report.insights
        ],
    }


@meta_router.get("/performance")
async def get_performance(
    engine_name: str = Query("regime", description="Engine name (unused, kept for backward compat)"),
    period: str = Query("30d", description="Period (unused, kept for backward compat)"),
) -> Dict[str, Any]:
    """Return flat performance metrics from the latest backtest run and live portfolio.

    The Settings page iterates ``Object.entries(response)`` and renders
    each key as a KPI card, so the response must be a flat dict of
    scalar values (not nested ``metrics``/``by_regime`` dicts).
    """
    from apatheon.core.database import get_db_manager

    db = get_db_manager()
    out: Dict[str, Any] = {}

    # 1) Latest backtest run metrics
    try:
        with db.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    SELECT metrics_json, strategy_id, start_date, end_date
                    FROM backtest_runs
                    WHERE metrics_json IS NOT NULL
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                )
                row = cursor.fetchone()
            finally:
                cursor.close()

        if row is not None:
            metrics_raw, strat, bt_start, bt_end = row
            metrics = metrics_raw if isinstance(metrics_raw, dict) else {}
            out["backtest_sharpe"] = round(float(metrics.get("annualised_sharpe", 0.0)), 3)
            out["backtest_return"] = round(float(metrics.get("cumulative_return", 0.0)), 4)
            out["backtest_max_dd"] = round(float(metrics.get("max_drawdown", 0.0)), 4)
            out["backtest_win_rate"] = round(float(metrics.get("win_rate", 0.0)), 4)
            out["backtest_period"] = f"{bt_start} → {bt_end}"
    except Exception:
        logger.exception("[meta/performance] backtest metrics query failed")

    # 2) Live portfolio Sharpe from NLV series (positions_snapshots)
    try:
        with db.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    WITH snaps AS (
                        SELECT as_of_date, timestamp, SUM(market_value) AS nlv
                        FROM positions_snapshots
                        WHERE portfolio_id = 'IBKR_PAPER'
                        GROUP BY as_of_date, timestamp
                    ),
                    latest_per_day AS (
                        SELECT DISTINCT ON (as_of_date)
                               as_of_date, nlv
                        FROM snaps
                        WHERE nlv > 0
                        ORDER BY as_of_date, timestamp DESC
                    )
                    SELECT as_of_date, nlv
                    FROM latest_per_day
                    ORDER BY as_of_date
                    """,
                )
                nlv_rows = cursor.fetchall()
            finally:
                cursor.close()

        if len(nlv_rows) >= 2:
            import math

            nlvs = [float(r[1]) for r in nlv_rows]
            # Filter out capital-flow days (deposits/withdrawals) where
            # NLV jumps >15% in a single day — not market returns.
            flow_threshold = 0.15
            daily_returns = []
            for i in range(1, len(nlvs)):
                if nlvs[i - 1] > 0:
                    ret = (nlvs[i] - nlvs[i - 1]) / nlvs[i - 1]
                    if abs(ret) <= flow_threshold:
                        daily_returns.append(ret)

            if daily_returns:
                n = len(daily_returns)
                mean_r = sum(daily_returns) / n
                var_r = sum((r - mean_r) ** 2 for r in daily_returns) / max(n - 1, 1)
                vol = math.sqrt(var_r) if var_r > 0 else 0.0
                ann_vol = vol * math.sqrt(252)
                ann_sharpe = (mean_r * 252) / ann_vol if ann_vol > 0 else 0.0

                out["live_sharpe"] = round(ann_sharpe, 3)
                out["live_ann_vol"] = round(ann_vol, 4)
                out["live_days"] = n
    except Exception:
        logger.exception("[meta/performance] live portfolio metrics query failed")

    if not out:
        out["status"] = "no data — run a backtest or sync IBKR positions"

    return out


@meta_router.get("/engine_parameters", response_model=EngineParametersResponse)
async def get_engine_parameters() -> EngineParametersResponse:
    """Return current high-impact ("detrimental when mis-set") engine params.

    Values are fetched from live config sources (YAML/env loaders), not
    hardcoded constants in this endpoint.
    """
    import dataclasses

    assessment_defaults = {
        f.name: f.default
        for f in dataclasses.fields(BasicAssessmentModel)
        if f.default is not dataclasses.MISSING
    }

    region = "US"
    market_id = "US_EQ"

    daily_universe_cfg = _load_daily_universe_lambda_config(region)
    daily_portfolio_cfg = _load_daily_portfolio_risk_config(region)
    exec_policy_artifact = load_execution_policy_artifact()
    exec_policy = exec_policy_artifact.policy
    exec_risk = get_config().execution_risk
    meta_policy_artifact = load_meta_policy_artifact()
    book_registry = load_book_registry()

    policy = meta_policy_artifact.policies.get(market_id)

    meta_default_book_id: str | None = None
    meta_default_sleeve_id: str | None = None
    meta_default_max_names: Any = None
    meta_default_per_name_cap: Any = None
    meta_default_fragility_threshold: Any = None
    if policy is not None:
        meta_default_book_id = policy.default.book_id
        meta_default_sleeve_id = policy.default.sleeve_id
        book = book_registry.get(meta_default_book_id) if meta_default_book_id else None
        if book is not None:
            resolved_sleeve_id = book.resolve_sleeve_id(meta_default_sleeve_id)
            sleeve = book.sleeves.get(resolved_sleeve_id) if resolved_sleeve_id else None
            if isinstance(sleeve, LongEquitySleeveSpec):
                meta_default_max_names = sleeve.portfolio_max_names
                meta_default_per_name_cap = sleeve.portfolio_per_instrument_max_weight
            elif isinstance(sleeve, AllocatorSleeveSpec):
                meta_default_max_names = sleeve.portfolio_max_names
                meta_default_per_name_cap = sleeve.portfolio_per_instrument_max_weight
                meta_default_fragility_threshold = sleeve.fragility_threshold
            elif isinstance(sleeve, HedgeEtfSleeveSpec):
                meta_default_fragility_threshold = sleeve.fragility_threshold

    engines: List[EngineParameterGroup] = [
        EngineParameterGroup(
            engine_id="REGIME_ENGINE",
            engine_label="Regime Engine",
            parameters=[
                EngineParameterItem(
                    key=f"{region}.hazard_profile",
                    value=daily_portfolio_cfg.hazard_profile,
                    source="configs/portfolio/core_long_eq_daily.yaml",
                    detrimental_reason="Wrong profile can misclassify risk regimes and flip downstream routing.",
                ),
            ],
        ),
        EngineParameterGroup(
            engine_id="ASSESSMENT_ENGINE",
            engine_label="Assessment Engine",
            parameters=[
                EngineParameterItem(
                    key="momentum_window_days",
                    value=assessment_defaults.get("momentum_window_days", 126),
                    source="BasicAssessmentModel default",
                    detrimental_reason="Too short puts model in short-term reversal territory (negative IC); too long is slow to adapt.",
                ),
                EngineParameterItem(
                    key="momentum_ref",
                    value=assessment_defaults.get("momentum_ref", 0.20),
                    source="BasicAssessmentModel default",
                    detrimental_reason="Sets normalisation scale; wrong ref compresses or inflates all scores uniformly.",
                ),
                EngineParameterItem(
                    key="fragility_penalty_weight",
                    value=assessment_defaults.get("fragility_penalty_weight", 0.15),
                    source="BasicAssessmentModel default",
                    detrimental_reason="Too high clips all scores toward -1 dominating momentum; too low ignores STAB fragility signal.",
                ),
                EngineParameterItem(
                    key="strong_buy_threshold",
                    value=assessment_defaults.get("strong_buy_threshold", 0.03),
                    source="BasicAssessmentModel default",
                    detrimental_reason="Sets the STRONG_BUY adjusted-score boundary; misaligned threshold distorts signal-label distribution.",
                ),
                EngineParameterItem(
                    key="sell_threshold",
                    value=assessment_defaults.get("sell_threshold", 0.01),
                    source="BasicAssessmentModel default",
                    detrimental_reason="Too tight generates excessive SELL labels on noise; too loose delays de-risking signals.",
                ),
                EngineParameterItem(
                    key="max_workers",
                    value=assessment_defaults.get("max_workers", 1),
                    source="BasicAssessmentModel default",
                    detrimental_reason="1 = single-threaded; too high can starve other pipeline tasks on shared workers.",
                ),
            ],
        ),
        EngineParameterGroup(
            engine_id="UNIVERSE_ENGINE",
            engine_label="Universe Engine",
            parameters=[
                EngineParameterItem(
                    key=f"{region}.lambda_predictions_csv",
                    value=daily_universe_cfg.predictions_csv,
                    source="configs/universe/core_long_eq_daily.yaml",
                    detrimental_reason="Bad path disables lambda enrichment and can degrade selection quality.",
                ),
                EngineParameterItem(
                    key=f"{region}.lambda_experiment_id",
                    value=daily_universe_cfg.experiment_id,
                    source="configs/universe/core_long_eq_daily.yaml",
                    detrimental_reason="Mismatched experiment picks wrong score set for inclusion ranking.",
                ),
                EngineParameterItem(
                    key=f"{region}.lambda_score_weight",
                    value=daily_universe_cfg.score_weight,
                    source="configs/universe/core_long_eq_daily.yaml",
                    detrimental_reason="Overweight can force unstable name selection; underweight can mute signal.",
                ),
            ],
        ),
        EngineParameterGroup(
            engine_id="PORTFOLIO_ENGINE",
            engine_label="Portfolio Engine",
            parameters=[
                EngineParameterItem(
                    key=f"{region}.scenario_risk_set_id",
                    value=daily_portfolio_cfg.scenario_risk_set_id,
                    source="configs/portfolio/core_long_eq_daily.yaml",
                    detrimental_reason="Wrong scenario set distorts scenario P&L and risk gating.",
                ),
                EngineParameterItem(
                    key=f"{region}.stab_scenario_set_id",
                    value=daily_portfolio_cfg.stab_scenario_set_id,
                    source="configs/portfolio/core_long_eq_daily.yaml",
                    detrimental_reason="Incorrect STAB scenario map can hide state-change risk.",
                ),
                EngineParameterItem(
                    key=f"{region}.meta_budget_enabled",
                    value=daily_portfolio_cfg.meta_budget_enabled,
                    source="configs/portfolio/core_long_eq_daily.yaml",
                    detrimental_reason="Disabled budget gating can overexpose risk during unstable periods.",
                ),
                EngineParameterItem(
                    key=f"{region}.meta_budget_alpha",
                    value=daily_portfolio_cfg.meta_budget_alpha,
                    source="configs/portfolio/core_long_eq_daily.yaml",
                    detrimental_reason="Too high/low alpha overreacts or underreacts to regime risk.",
                ),
                EngineParameterItem(
                    key=f"{region}.meta_budget_min",
                    value=daily_portfolio_cfg.meta_budget_min,
                    source="configs/portfolio/core_long_eq_daily.yaml",
                    detrimental_reason="Too low can starve exposure; too high can suppress de-risking.",
                ),
            ],
        ),
        EngineParameterGroup(
            engine_id="EXECUTION_ENGINE",
            engine_label="Execution Engine",
            parameters=[
                EngineParameterItem(
                    key="policy.turnover.one_way_limit",
                    value=exec_policy.turnover.one_way_limit,
                    source="configs/execution/policy.yaml",
                    detrimental_reason="Too loose increases churn/slippage; too tight blocks required repositioning.",
                ),
                EngineParameterItem(
                    key="policy.no_trade_band_bps",
                    value=exec_policy.no_trade_band_bps,
                    source="configs/execution/policy.yaml",
                    detrimental_reason="Too low overtrades micro-noise; too high delays meaningful rebalance.",
                ),
                EngineParameterItem(
                    key="policy.cash_buffer_weight",
                    value=exec_policy.cash_buffer_weight,
                    source="configs/execution/policy.yaml",
                    detrimental_reason="Too low risks cash failures; too high leaves persistent under-investment.",
                ),
                EngineParameterItem(
                    key="risk.max_order_notional",
                    value="unconstrained" if exec_risk.max_order_notional == 0.0 else exec_risk.max_order_notional,
                    source="env: EXEC_RISK_MAX_ORDER_NOTIONAL",
                    detrimental_reason="Mis-set cap can block valid orders or allow oversized tickets. 0 = unconstrained.",
                ),
                EngineParameterItem(
                    key="risk.max_position_notional",
                    value="unconstrained" if exec_risk.max_position_notional == 0.0 else exec_risk.max_position_notional,
                    source="env: EXEC_RISK_MAX_POSITION_NOTIONAL",
                    detrimental_reason="Incorrect cap can force concentration drift or reject desired hedges. 0 = unconstrained.",
                ),
                EngineParameterItem(
                    key="risk.max_leverage",
                    value="unconstrained" if exec_risk.max_leverage == 0.0 else exec_risk.max_leverage,
                    source="env: EXEC_RISK_MAX_LEVERAGE",
                    detrimental_reason="Wrong leverage limit can either over-risk or unnecessarily constrain execution. 0 = unconstrained.",
                ),
            ],
        ),
        EngineParameterGroup(
            engine_id="META_POLICY_V1",
            engine_label="Meta Policy Engine",
            parameters=[
                EngineParameterItem(
                    key=f"{market_id}.default.book_id",
                    value=meta_default_book_id,
                    source="configs/meta/policy.yaml",
                    detrimental_reason="Wrong default book routes all situations into an unintended strategy stack.",
                ),
                EngineParameterItem(
                    key=f"{market_id}.default.sleeve_id",
                    value=meta_default_sleeve_id,
                    source="configs/meta/policy.yaml",
                    detrimental_reason="Wrong sleeve can alter concentration and turnover profile materially.",
                ),
                EngineParameterItem(
                    key="default_sleeve.portfolio_max_names",
                    value=meta_default_max_names,
                    source="configs/meta/books.yaml",
                    detrimental_reason="Too many names dilutes signal; too few increases concentration and variance.",
                ),
                EngineParameterItem(
                    key="default_sleeve.portfolio_per_instrument_max_weight",
                    value=meta_default_per_name_cap,
                    source="configs/meta/books.yaml",
                    detrimental_reason="Incorrect cap can create concentration risk or block intended positioning.",
                ),
                EngineParameterItem(
                    key="default_sleeve.fragility_threshold",
                    value=meta_default_fragility_threshold,
                    source="configs/meta/books.yaml",
                    detrimental_reason="Threshold drift can delay or over-trigger defensive hedge allocation.",
                ),
            ],
        ),
    ]

    return EngineParametersResponse(
        generated_at=datetime.now(timezone.utc).isoformat(),
        engines=engines,
    )


# ============================================================================
# Meta Policy (Book/Sleeve Routing) Endpoints
# ============================================================================


def _sel_model(sel: MetaPolicySelection) -> MetaPolicySelectionModel:
    return MetaPolicySelectionModel(book_id=sel.book_id, sleeve_id=sel.sleeve_id)


@meta_router.get("/policy/decisions", response_model=List[MetaPolicyDecisionResponse])
async def get_meta_policy_decisions(
    market_id: str = Query(..., description="Market identifier (e.g. US_EQ)"),
    limit: int = Query(50, description="Row limit"),
) -> List[MetaPolicyDecisionResponse]:
    """Return recent META_POLICY_V1 engine decisions (book/sleeve routing)."""

    limit_eff = max(1, min(int(limit), 500))
    market_id_eff = str(market_id).upper()

    db = get_db_manager()

    sql = """
        SELECT decision_id,
               run_id,
               market_id,
               as_of_date,
               config_id,
               input_refs,
               output_refs,
               created_at
        FROM engine_decisions
        WHERE engine_name = 'META_POLICY_V1'
          AND market_id = %s
        ORDER BY as_of_date DESC, created_at DESC
        LIMIT %s
    """

    with db.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (market_id_eff, limit_eff))
            rows = cursor.fetchall()
        finally:
            cursor.close()

    out: List[MetaPolicyDecisionResponse] = []

    for decision_id, run_id, mkt, as_of_date, config_id, input_refs, output_refs, created_at in rows:
        in_refs = input_refs or {}
        out_refs = output_refs or {}

        selected_book_id = out_refs.get("selected_book_id")
        selected_sleeve_id = out_refs.get("selected_sleeve_id")
        if selected_sleeve_id is None and isinstance(config_id, str):
            selected_sleeve_id = config_id

        out.append(
            MetaPolicyDecisionResponse(
                decision_id=str(decision_id),
                run_id=str(run_id) if run_id is not None else None,
                market_id=str(mkt),
                as_of_date=as_of_date,
                selected_book_id=str(selected_book_id) if isinstance(selected_book_id, str) else None,
                selected_sleeve_id=str(selected_sleeve_id) if isinstance(selected_sleeve_id, str) else None,
                market_situation=str(in_refs.get("market_situation")) if in_refs.get("market_situation") is not None else None,
                policy_version=str(in_refs.get("policy_version")) if in_refs.get("policy_version") is not None else None,
                created_at=created_at.isoformat() if hasattr(created_at, "isoformat") else None,
            )
        )

    return out


# ============================================================================
# Notifications inbox (migration 0100)
# ============================================================================


class NotificationItem(BaseModel):
    """A single notification row from the inbox."""

    notification_id: int
    created_at: datetime
    as_of_date: Optional[date]
    kind: str
    severity: str
    title: str
    body: Optional[str]
    source_table: Optional[str]
    source_id: Optional[str]
    link_path: Optional[str]
    read_at: Optional[datetime]
    dismissed_at: Optional[datetime]
    metadata: Dict[str, Any] = Field(default_factory=dict)


class NotificationListResponse(BaseModel):
    """Paginated list of notifications."""

    items: List[NotificationItem]
    unread_count: int
    total: int


def _row_to_notification(row: tuple) -> NotificationItem:
    (
        notification_id, created_at, as_of_date, kind, severity, title, body,
        source_table, source_id, link_path, read_at, dismissed_at, metadata_json,
    ) = row
    return NotificationItem(
        notification_id=notification_id,
        created_at=created_at,
        as_of_date=as_of_date,
        kind=kind,
        severity=severity,
        title=title,
        body=body,
        source_table=source_table,
        source_id=source_id,
        link_path=link_path,
        read_at=read_at,
        dismissed_at=dismissed_at,
        metadata=metadata_json or {},
    )


@meta_router.get("/notifications", response_model=NotificationListResponse)
async def list_notifications(
    unread_only: bool = Query(False, description="Return only unread+undismissed."),
    include_dismissed: bool = Query(False, description="Include dismissed notifications."),
    kind: Optional[str] = Query(None, description="Filter by notification kind."),
    severity: Optional[str] = Query(None, description="Filter by severity (info|warning|critical)."),
    limit: int = Query(100, ge=1, le=500),
) -> NotificationListResponse:
    """Notifications inbox for the alerting loop (migration 0100)."""
    db = get_db_manager()

    where: List[str] = []
    params: List[Any] = []
    if unread_only:
        where.append("read_at IS NULL AND dismissed_at IS NULL")
    elif not include_dismissed:
        where.append("dismissed_at IS NULL")
    if kind:
        where.append("kind = %s")
        params.append(kind)
    if severity:
        where.append("severity = %s")
        params.append(severity)

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                SELECT notification_id, created_at, as_of_date, kind, severity,
                       title, body, source_table, source_id, link_path,
                       read_at, dismissed_at, metadata_json
                FROM notifications
                {where_sql}
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (*params, limit),
            )
            rows = cur.fetchall()

            cur.execute(
                "SELECT COUNT(*) FROM notifications "
                "WHERE read_at IS NULL AND dismissed_at IS NULL"
            )
            unread_row = cur.fetchone()
            unread_count = int(unread_row[0]) if unread_row else 0

            cur.execute("SELECT COUNT(*) FROM notifications WHERE dismissed_at IS NULL")
            total_row = cur.fetchone()
            total = int(total_row[0]) if total_row else 0
        finally:
            cur.close()

    return NotificationListResponse(
        items=[_row_to_notification(r) for r in rows],
        unread_count=unread_count,
        total=total,
    )


@meta_router.post("/notifications/{notification_id}/read", response_model=NotificationItem)
async def mark_notification_read(
    notification_id: int = Path(..., description="Notification ID."),
) -> NotificationItem:
    """Mark a single notification as read."""
    from prometheus.meta.notifications import mark_read

    db = get_db_manager()
    mark_read(db, notification_id)  # idempotent; returns False if already read

    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT notification_id, created_at, as_of_date, kind, severity,
                       title, body, source_table, source_id, link_path,
                       read_at, dismissed_at, metadata_json
                FROM notifications WHERE notification_id = %s
                """,
                (notification_id,),
            )
            row = cur.fetchone()
        finally:
            cur.close()
    if row is None:
        raise HTTPException(status_code=404, detail=f"notification {notification_id} not found")
    return _row_to_notification(row)


@meta_router.post("/notifications/{notification_id}/dismiss", response_model=NotificationItem)
async def dismiss_notification(
    notification_id: int = Path(..., description="Notification ID."),
) -> NotificationItem:
    """Dismiss a single notification (hides it from the default inbox view)."""
    from prometheus.meta.notifications import dismiss

    db = get_db_manager()
    dismiss(db, notification_id)  # idempotent

    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT notification_id, created_at, as_of_date, kind, severity,
                       title, body, source_table, source_id, link_path,
                       read_at, dismissed_at, metadata_json
                FROM notifications WHERE notification_id = %s
                """,
                (notification_id,),
            )
            row = cur.fetchone()
        finally:
            cur.close()
    if row is None:
        raise HTTPException(status_code=404, detail=f"notification {notification_id} not found")
    return _row_to_notification(row)


# ============================================================================
# Persisted meta history (migration 0099)
# ============================================================================


class FeedbackInsightRow(BaseModel):
    """A persisted row from meta_feedback_insights."""

    insight_id: int
    as_of_date: date
    category: str
    severity: str
    message: str
    metric_name: Optional[str]
    metric_value: Optional[float]
    benchmark: Optional[float]
    deviation: Optional[float]
    lookback_days: Optional[int]
    created_at: datetime


class FeedbackInsightsResponse(BaseModel):
    items: List[FeedbackInsightRow]
    distinct_dates: int


@meta_router.get("/feedback_insights", response_model=FeedbackInsightsResponse)
async def list_feedback_insights(
    days: int = Query(30, ge=1, le=180, description="Lookback days from latest."),
    severity: Optional[str] = Query(None, description="Filter (info|warning|critical)."),
    limit: int = Query(200, ge=1, le=1000),
) -> FeedbackInsightsResponse:
    """Historical feedback insights — persisted daily by the autopilot loop."""
    db = get_db_manager()

    where: List[str] = ["as_of_date >= CURRENT_DATE - (%s || ' days')::interval"]
    params: List[Any] = [int(days)]
    if severity:
        where.append("severity = %s")
        params.append(severity)
    where_sql = "WHERE " + " AND ".join(where)

    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                SELECT insight_id, as_of_date, category, severity, message,
                       metric_name, metric_value, benchmark, deviation,
                       lookback_days, created_at
                FROM meta_feedback_insights
                {where_sql}
                ORDER BY as_of_date DESC, severity DESC, insight_id DESC
                LIMIT %s
                """,
                (*params, limit),
            )
            rows = cur.fetchall()
            cur.execute(
                f"""
                SELECT COUNT(DISTINCT as_of_date) FROM meta_feedback_insights
                {where_sql}
                """,
                params,
            )
            n_dates_row = cur.fetchone()
        finally:
            cur.close()

    n_dates = int(n_dates_row[0]) if n_dates_row else 0
    items = [
        FeedbackInsightRow(
            insight_id=r[0],
            as_of_date=r[1],
            category=r[2],
            severity=r[3],
            message=r[4],
            metric_name=r[5],
            metric_value=r[6],
            benchmark=r[7],
            deviation=r[8],
            lookback_days=r[9],
            created_at=r[10],
        )
        for r in rows
    ]
    return FeedbackInsightsResponse(items=items, distinct_dates=n_dates)


class WeeklyReportRow(BaseModel):
    """A persisted row from weekly_reports."""

    report_id: int
    week_start: date
    week_end: date
    strategy_id: Optional[str]
    period_return: Optional[float]
    period_sharpe: Optional[float]
    period_max_drawdown: Optional[float]
    n_trades: int
    n_winners: int
    n_losers: int
    has_markdown: bool
    created_at: datetime


class WeeklyReportsResponse(BaseModel):
    items: List[WeeklyReportRow]


@meta_router.get("/weekly_reports", response_model=WeeklyReportsResponse)
async def list_weekly_reports(
    strategy_id: Optional[str] = Query(None, description="Filter by strategy (NULL means portfolio-wide)."),
    limit: int = Query(26, ge=1, le=104, description="Max number of weeks."),
) -> WeeklyReportsResponse:
    """Historical weekly rollups — persisted Mondays by the autopilot loop."""
    db = get_db_manager()

    where: List[str] = []
    params: List[Any] = []
    if strategy_id:
        if strategy_id.upper() == "NULL":
            where.append("strategy_id IS NULL")
        else:
            where.append("strategy_id = %s")
            params.append(strategy_id)
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                SELECT report_id, week_start, week_end, strategy_id,
                       period_return, period_sharpe, period_max_drawdown,
                       n_trades, n_winners, n_losers,
                       (markdown IS NOT NULL) AS has_markdown,
                       created_at
                FROM weekly_reports
                {where_sql}
                ORDER BY week_end DESC, strategy_id NULLS FIRST
                LIMIT %s
                """,
                (*params, limit),
            )
            rows = cur.fetchall()
        finally:
            cur.close()

    items = [
        WeeklyReportRow(
            report_id=r[0],
            week_start=r[1],
            week_end=r[2],
            strategy_id=r[3],
            period_return=r[4],
            period_sharpe=r[5],
            period_max_drawdown=r[6],
            n_trades=r[7] or 0,
            n_winners=r[8] or 0,
            n_losers=r[9] or 0,
            has_markdown=bool(r[10]),
            created_at=r[11],
        )
        for r in rows
    ]
    return WeeklyReportsResponse(items=items)


@meta_router.get("/weekly_reports/{report_id}")
async def get_weekly_report_detail(
    report_id: int = Path(..., description="Persisted weekly report ID."),
) -> Dict[str, Any]:
    """Full weekly report (incl. markdown + JSON payload) by ID."""
    db = get_db_manager()
    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT report_id, week_start, week_end, strategy_id,
                       period_return, period_sharpe, period_max_drawdown,
                       n_trades, n_winners, n_losers, report_json, markdown,
                       created_at
                FROM weekly_reports
                WHERE report_id = %s
                """,
                (int(report_id),),
            )
            row = cur.fetchone()
        finally:
            cur.close()
    if row is None:
        raise HTTPException(status_code=404, detail=f"weekly report {report_id} not found")
    return {
        "report_id": row[0],
        "week_start": row[1].isoformat() if row[1] else None,
        "week_end": row[2].isoformat() if row[2] else None,
        "strategy_id": row[3],
        "period_return": row[4],
        "period_sharpe": row[5],
        "period_max_drawdown": row[6],
        "n_trades": row[7] or 0,
        "n_winners": row[8] or 0,
        "n_losers": row[9] or 0,
        "report_json": row[10] or {},
        "markdown": row[11],
        "created_at": row[12].isoformat() if row[12] else None,
    }


# ============================================================================
# Diagnostic reports + signal validations history (migration 0099)
# ============================================================================


class DiagnosticReportRow(BaseModel):
    """A persisted row from meta_diagnostic_reports."""

    report_id: int
    as_of_date: date
    strategy_id: str
    has_underperformers: bool
    has_high_risk: bool
    num_runs_analysed: int
    created_at: datetime


class DiagnosticReportsResponse(BaseModel):
    items: List[DiagnosticReportRow]


@meta_router.get("/diagnostic_reports", response_model=DiagnosticReportsResponse)
async def list_diagnostic_reports(
    strategy_id: Optional[str] = Query(None, description="Filter by strategy."),
    days: int = Query(30, ge=1, le=180),
    only_with_findings: bool = Query(False, description="Only reports with underperformers or high risk."),
    limit: int = Query(200, ge=1, le=1000),
) -> DiagnosticReportsResponse:
    """Historical diagnostic reports — persisted daily by the autopilot loop."""
    db = get_db_manager()

    where: List[str] = ["as_of_date >= CURRENT_DATE - (%s || ' days')::interval"]
    params: List[Any] = [int(days)]
    if strategy_id:
        where.append("strategy_id = %s")
        params.append(strategy_id)
    if only_with_findings:
        where.append("(has_underperformers OR has_high_risk)")
    where_sql = "WHERE " + " AND ".join(where)

    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                SELECT report_id, as_of_date, strategy_id,
                       has_underperformers, has_high_risk,
                       num_runs_analysed, created_at
                FROM meta_diagnostic_reports
                {where_sql}
                ORDER BY as_of_date DESC, strategy_id
                LIMIT %s
                """,
                (*params, limit),
            )
            rows = cur.fetchall()
        finally:
            cur.close()

    items = [
        DiagnosticReportRow(
            report_id=r[0],
            as_of_date=r[1],
            strategy_id=r[2],
            has_underperformers=bool(r[3]),
            has_high_risk=bool(r[4]),
            num_runs_analysed=int(r[5] or 0),
            created_at=r[6],
        )
        for r in rows
    ]
    return DiagnosticReportsResponse(items=items)


@meta_router.get("/diagnostic_reports/{report_id}")
async def get_diagnostic_report_detail(
    report_id: int = Path(..., description="Persisted diagnostic report ID."),
) -> Dict[str, Any]:
    """Full diagnostic report JSON (incl. underperformer list, risk flags) by ID."""
    db = get_db_manager()
    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT report_id, as_of_date, strategy_id,
                       has_underperformers, has_high_risk,
                       num_runs_analysed, report_json, created_at
                FROM meta_diagnostic_reports
                WHERE report_id = %s
                """,
                (int(report_id),),
            )
            row = cur.fetchone()
        finally:
            cur.close()
    if row is None:
        raise HTTPException(status_code=404, detail=f"diagnostic report {report_id} not found")
    return {
        "report_id": row[0],
        "as_of_date": row[1].isoformat() if row[1] else None,
        "strategy_id": row[2],
        "has_underperformers": bool(row[3]),
        "has_high_risk": bool(row[4]),
        "num_runs_analysed": int(row[5] or 0),
        "report_json": row[6] or {},
        "created_at": row[7].isoformat() if row[7] else None,
    }


class SignalValidationRow(BaseModel):
    """A persisted row from meta_signal_validations."""

    validation_id: int
    as_of_date: date
    signal_name: str
    verdict: str
    metric_value: Optional[float]
    threshold: Optional[float]
    sample_size: Optional[int]
    lookback_days: Optional[int]
    details: Dict[str, Any]
    created_at: datetime


class SignalValidationsResponse(BaseModel):
    items: List[SignalValidationRow]
    distinct_signals: int


@meta_router.get("/signal_validations", response_model=SignalValidationsResponse)
async def list_signal_validations(
    signal_name: Optional[str] = Query(None, description="Filter by signal name."),
    verdict: Optional[str] = Query(None, description="Filter by verdict (e.g. PASS, FAIL, DEGRADED)."),
    days: int = Query(30, ge=1, le=180),
    limit: int = Query(200, ge=1, le=1000),
) -> SignalValidationsResponse:
    """Historical signal validations — persisted daily by the autopilot loop."""
    db = get_db_manager()

    where: List[str] = ["as_of_date >= CURRENT_DATE - (%s || ' days')::interval"]
    params: List[Any] = [int(days)]
    if signal_name:
        where.append("signal_name = %s")
        params.append(signal_name)
    if verdict:
        where.append("verdict = %s")
        params.append(verdict)
    where_sql = "WHERE " + " AND ".join(where)

    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                SELECT validation_id, as_of_date, signal_name, verdict,
                       metric_value, threshold, sample_size, lookback_days,
                       details_json, created_at
                FROM meta_signal_validations
                {where_sql}
                ORDER BY as_of_date DESC, signal_name
                LIMIT %s
                """,
                (*params, limit),
            )
            rows = cur.fetchall()
            cur.execute(
                f"""
                SELECT COUNT(DISTINCT signal_name) FROM meta_signal_validations
                {where_sql}
                """,
                params,
            )
            distinct_row = cur.fetchone()
        finally:
            cur.close()

    n_signals = int(distinct_row[0]) if distinct_row else 0
    items = [
        SignalValidationRow(
            validation_id=r[0],
            as_of_date=r[1],
            signal_name=r[2],
            verdict=r[3],
            metric_value=r[4],
            threshold=r[5],
            sample_size=r[6],
            lookback_days=r[7],
            details=r[8] or {},
            created_at=r[9],
        )
        for r in rows
    ]
    return SignalValidationsResponse(items=items, distinct_signals=n_signals)


# ============================================================================
# Backtest-vs-live drift (migration 0101)
# ============================================================================


class DriftRow(BaseModel):
    """One row from the backtest_live_drift table."""

    drift_id: int
    as_of_date: date
    strategy_id: str
    horizon_days: int
    n_live_outcomes: int
    backtest_run_id: Optional[str]
    live_sharpe: Optional[float]
    backtest_sharpe: Optional[float]
    sharpe_delta: Optional[float]
    live_return: Optional[float]
    backtest_return: Optional[float]
    return_delta: Optional[float]
    live_max_drawdown: Optional[float]
    backtest_max_drawdown: Optional[float]
    max_drawdown_delta: Optional[float]
    severity: str
    notes: Optional[str]
    created_at: datetime


class DriftListResponse(BaseModel):
    items: List[DriftRow]
    latest_as_of_date: Optional[date]


@meta_router.get("/drift", response_model=DriftListResponse)
async def list_drift(
    strategy_id: Optional[str] = Query(None, description="Filter by strategy."),
    horizon_days: Optional[int] = Query(None, description="Filter by horizon (days)."),
    latest_only: bool = Query(True, description="Only the most recent as_of_date per strategy/horizon."),
    limit: int = Query(200, ge=1, le=1000),
) -> DriftListResponse:
    """Backtest-vs-live drift rows for the Drift Monitor page."""
    db = get_db_manager()

    where: List[str] = []
    params: List[Any] = []
    if strategy_id:
        where.append("strategy_id = %s")
        params.append(strategy_id)
    if horizon_days:
        where.append("horizon_days = %s")
        params.append(int(horizon_days))
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    if latest_only:
        sql = f"""
            WITH ranked AS (
              SELECT
                drift_id, as_of_date, strategy_id, horizon_days,
                n_live_outcomes, backtest_run_id,
                live_sharpe, backtest_sharpe, sharpe_delta,
                live_return, backtest_return, return_delta,
                live_max_drawdown, backtest_max_drawdown, max_drawdown_delta,
                severity, notes, created_at,
                ROW_NUMBER() OVER (
                  PARTITION BY strategy_id, horizon_days
                  ORDER BY as_of_date DESC
                ) AS rn
              FROM backtest_live_drift
              {where_sql}
            )
            SELECT
              drift_id, as_of_date, strategy_id, horizon_days,
              n_live_outcomes, backtest_run_id,
              live_sharpe, backtest_sharpe, sharpe_delta,
              live_return, backtest_return, return_delta,
              live_max_drawdown, backtest_max_drawdown, max_drawdown_delta,
              severity, notes, created_at
            FROM ranked
            WHERE rn = 1
            ORDER BY strategy_id, horizon_days
            LIMIT %s
        """
    else:
        sql = f"""
            SELECT
              drift_id, as_of_date, strategy_id, horizon_days,
              n_live_outcomes, backtest_run_id,
              live_sharpe, backtest_sharpe, sharpe_delta,
              live_return, backtest_return, return_delta,
              live_max_drawdown, backtest_max_drawdown, max_drawdown_delta,
              severity, notes, created_at
            FROM backtest_live_drift
            {where_sql}
            ORDER BY as_of_date DESC, strategy_id, horizon_days
            LIMIT %s
        """

    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, (*params, limit))
            rows = cur.fetchall()
            cur.execute("SELECT MAX(as_of_date) FROM backtest_live_drift")
            latest_row = cur.fetchone()
            latest = latest_row[0] if latest_row else None
        finally:
            cur.close()

    items = [
        DriftRow(
            drift_id=r[0],
            as_of_date=r[1],
            strategy_id=r[2],
            horizon_days=r[3],
            n_live_outcomes=r[4] or 0,
            backtest_run_id=r[5],
            live_sharpe=r[6],
            backtest_sharpe=r[7],
            sharpe_delta=r[8],
            live_return=r[9],
            backtest_return=r[10],
            return_delta=r[11],
            live_max_drawdown=r[12],
            backtest_max_drawdown=r[13],
            max_drawdown_delta=r[14],
            severity=r[15] or "info",
            notes=r[16],
            created_at=r[17],
        )
        for r in rows
    ]
    return DriftListResponse(items=items, latest_as_of_date=latest)


@meta_router.post("/notifications/mark_all_read")
async def mark_all_notifications_read() -> Dict[str, int]:
    """Mark every unread+undismissed notification as read."""
    db = get_db_manager()
    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                "UPDATE notifications SET read_at = NOW() "
                "WHERE read_at IS NULL AND dismissed_at IS NULL"
            )
            updated = cur.rowcount or 0
            conn.commit()
        finally:
            cur.close()
    return {"updated": int(updated)}


@meta_router.get("/policy/{market_id}", response_model=MetaPolicyArtifactResponse)
async def get_meta_policy(market_id: str = Path(..., description="Market identifier (e.g. US_EQ)")) -> MetaPolicyArtifactResponse:
    """Return the current meta policy artifact for a market."""

    artifact = load_meta_policy_artifact()
    policy = artifact.policies.get(str(market_id).upper())
    if policy is None:
        raise HTTPException(status_code=404, detail=f"No meta policy found for market_id={market_id!r}")

    situations: Dict[str, MetaPolicySelectionModel] = {}
    for sit, sel in policy.by_situation.items():
        situations[str(sit.value)] = _sel_model(sel)

    return MetaPolicyArtifactResponse(
        market_id=policy.market_id,
        version=artifact.version,
        updated_at=artifact.updated_at,
        updated_by=artifact.updated_by,
        default=_sel_model(policy.default),
        situations=situations,
    )
