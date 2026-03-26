"""Prometheus v2 – Meta APIs (Kronos Chat + Geo).

This module provides:
- Kronos Chat API for LLM-powered meta-orchestration
- Geo API for world map visualization with country-level data
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

from apathis.core.database import get_db_manager
from apathis.core.logging import get_logger
from fastapi import APIRouter, Body, HTTPException, Path, Query
from pydantic import BaseModel, Field

from apathis.core.config import get_config
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


kronos_router = APIRouter(prefix="/api/kronos", tags=["kronos"])
geo_router = APIRouter(prefix="/api/geo", tags=["geo"])
meta_router = APIRouter(prefix="/api/meta", tags=["meta"])


# ============================================================================
# Kronos Chat Models
# ============================================================================


class KronosRequest(BaseModel):
    """Request to Kronos chat interface."""

    question: str
    context: Dict[str, Any] = Field(default_factory=dict)


class KronosProposal(BaseModel):
    """Action proposal from Kronos."""

    proposal_id: str
    action_type: str  # backtest, config_change, synthetic_dataset
    description: str
    parameters: Dict[str, Any] = Field(default_factory=dict)
    risk_level: str = "LOW"


class KronosResponse(BaseModel):
    """Response from Kronos chat."""

    answer: str
    proposals: List[KronosProposal] = Field(default_factory=list)
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
# Kronos Endpoints
# ============================================================================


@kronos_router.post("/chat", response_model=KronosResponse)
def kronos_chat(request: KronosRequest = Body(...)) -> KronosResponse:
    """Interact with Kronos meta-orchestrator.

    Kronos can explain system behavior, propose experiments, and analyze
    engine performance. It cannot directly execute changes - all actions
    require explicit approval via the Control API.
    """
    from prometheus.monitoring.kronos_service import kronos_chat as _kronos_chat

    history = request.context.get("history", []) if request.context else []

    try:
        result = _kronos_chat(question=request.question, history=history)
        return KronosResponse(
            answer=result["answer"],
            proposals=[KronosProposal(**p) for p in result.get("proposals", [])],
            sources=result.get("sources", []),
        )
    except Exception as exc:
        logger.exception("[kronos] Chat failed: %s", exc)
        return KronosResponse(
            answer=f"Kronos encountered an error: {exc}. Check LLM configuration in Settings.",
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


@kronos_router.get("/llm/config")
async def get_llm_config() -> Dict[str, Any]:
    """Return current LLM configuration (no secrets)."""
    from apathis.llm.gateway import get_llm_info
    return get_llm_info()


@kronos_router.post("/llm/config")
async def set_llm_config(request: LLMConfigRequest = Body(...)) -> Dict[str, Any]:
    """Reconfigure the LLM provider at runtime."""
    from apathis.llm.gateway import configure_llm

    try:
        health = configure_llm(
            provider=request.provider,
            model=request.model,
            api_key=request.api_key,
            base_url=request.base_url,
        )
        return {"status": "ok", "health": health}
    except Exception as exc:
        logger.exception("[kronos] LLM config failed: %s", exc)
        raise HTTPException(status_code=400, detail=str(exc))


@kronos_router.get("/llm/health")
async def llm_health() -> Dict[str, Any]:
    """Run a health check on the current LLM provider."""
    from apathis.llm.gateway import get_llm

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
    from apathis.core.database import get_db_manager

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
