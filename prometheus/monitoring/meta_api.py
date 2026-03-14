"""Prometheus v2 – Meta APIs (Kronos Chat + Geo).

This module provides:
- Kronos Chat API for LLM-powered meta-orchestration
- Geo API for world map visualization with country-level data
"""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, HTTPException, Path, Query
from pydantic import BaseModel, Field

from apathis.core.database import get_db_manager
from apathis.core.logging import get_logger
from prometheus.meta.policy import MetaPolicySelection, load_meta_policy_artifact

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


class EnginePerformance(BaseModel):
    """Engine performance metrics."""

    engine_name: str
    period: str
    metrics: Dict[str, float] = Field(default_factory=dict)
    by_regime: Dict[str, Dict[str, float]] = Field(default_factory=dict)


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


@meta_router.get("/configs", response_model=List[EngineConfig])
async def get_configs() -> List[EngineConfig]:
    """Return current engine configurations."""
    return [
        EngineConfig(
            engine_name="regime",
            config_version="v2.1.0",
            parameters={
                "lookback_days": 60,
                "confidence_threshold": 0.75,
                "transition_smoothing": 0.85,
            },
            last_updated="2024-11-15T10:00:00Z",
        ),
        EngineConfig(
            engine_name="stability",
            config_version="v2.0.5",
            parameters={
                "liquidity_weight": 0.35,
                "volatility_weight": 0.35,
                "contagion_weight": 0.30,
            },
            last_updated="2024-11-01T12:00:00Z",
        ),
        EngineConfig(
            engine_name="fragility",
            config_version="v1.8.2",
            parameters={
                "alpha_threshold": 0.075,
                "min_score": 0.65,
                "lookback_days": 21,
            },
            last_updated="2024-10-28T14:00:00Z",
        ),
    ]


@meta_router.get("/performance", response_model=EnginePerformance)
async def get_performance(
    engine_name: str = Query(..., description="Engine name"),
    period: str = Query("30d", description="Period (e.g. 30d, 90d, 1y)"),
) -> EnginePerformance:
    """Return engine performance metrics."""
    return EnginePerformance(
        engine_name=engine_name,
        period=period,
        metrics={
            "accuracy": 0.78,
            "sharpe": 1.42,
            "hit_rate": 0.65,
            "avg_latency_ms": 850,
        },
        by_regime={
            "STABLE_EXPANSION": {
                "accuracy": 0.82,
                "sharpe": 1.68,
                "hit_rate": 0.70,
            },
            "GROWTH_WITH_VOLATILITY": {
                "accuracy": 0.72,
                "sharpe": 1.12,
                "hit_rate": 0.58,
            },
        },
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
