"""Prometheus v2 – Pipeline phase tasks.

This module contains the concrete phase tasks used by the engine run
state machine. Each task operates on a single ``EngineRun`` and
advances it through the phases by invoking existing engines
(Regime/Profiles/STAB/Universe/Books).

The design goal is to keep each phase function **idempotent** and
stateless beyond the database. Re-running a phase for the same
(as_of_date, region) should either be a no-op or simply overwrite
previous results with the same values.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import yaml
from psycopg2.extras import Json

from apathis.core.config import get_config
from apathis.core.database import DatabaseManager, get_db_manager
from apathis.core.ids import generate_uuid
from apathis.core.logging import get_logger
from apathis.core.time import TradingCalendar
from apathis.core.markets import MARKETS_BY_REGION, infer_region_from_market_id
from apathis.data.reader import DataReader
from apathis.profiles import (
    ProfileService,
    ProfileStorage,
    ProfileFeatureBuilder,
    RoutedProfileFeatureBuilder,
    SovereignProfileFeatureBuilder,
    BasicProfileEmbedder,
)
from apathis.stability import (
    StabilityEngine,
    StabilityStorage,
    BasicPriceStabilityModel,
    StabilityStateChangeForecaster,
)
from apathis.regime import RegimeEngine, RegimeStorage, MarketProxyRegimeModel
from apathis.regime.state_change import RegimeStateChangeForecaster
from prometheus.universe import (
    UniverseEngine,
    UniverseMember,
    UniverseStorage,
    BasicUniverseModel,
)
from prometheus.universe.config import UniverseConfig
from prometheus.assessment import AssessmentEngine
from prometheus.portfolio import (
    PortfolioConfig,
    PortfolioEngine,
    PortfolioStorage,
    TargetPortfolio,
    BasicLongOnlyPortfolioModel,
)
from prometheus.assessment.model_basic import BasicAssessmentModel
from prometheus.assessment.storage import InstrumentScoreStorage
from apathis.fragility import (
    BasicFragilityAlphaModel,
    FragilityAlphaEngine,
    FragilityStorage,
)
from apathis.fragility.model_market import MarketFragilityModel
from apathis.fragility.overlay import compute_fragility_overlay, overlay_config_from_sleeve_spec
from prometheus.pipeline.state import EngineRun, RunPhase, update_phase
from prometheus.meta import MetaStorage, MetaOrchestrator, EngineDecision
from prometheus.meta.market_situation import MarketSituationService
from prometheus.meta.policy import load_meta_policy_artifact
from prometheus.books import (
    AllocatorSleeveSpec,
    BookKind,
    BookSpec,
    HedgeEtfSleeveSpec,
    LongEquitySleeveSpec,
    load_book_registry,
)
from prometheus.decisions import DecisionTracker
from prometheus.backtest import SleeveRunSummary, run_backtest_campaign
from prometheus.backtest.catalog import build_core_long_sleeves
from prometheus.opportunity.lambda_provider import CsvLambdaClusterScoreProvider
from prometheus.risk import apply_risk_constraints
from apathis.sector.health import compute_sector_health, SectorHealthResult
from prometheus.sector.allocator import SectorAllocator, SectorAllocatorConfig, AllocationDecision
from apathis.sector.mapper import SectorMapper
from prometheus.scripts.backfill.backfill_portfolio_stab_scenario_metrics import (
    backfill_portfolio_stab_scenario_metrics_for_range,
)
from prometheus.scripts.backfill.backfill_backtest_stab_scenario_metrics import (
    summarise_backtest_stab_scenario_metrics,
)


logger = get_logger(__name__)

# Project root used for locating config files (e.g. configs/universe, configs/portfolio).
PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass
class DailyUniverseLambdaConfig:
    """Configuration for lambda-aware daily universes.

    This small config surface allows enabling lambda-based opportunity
    scores inside :func:`run_universes_for_run` without altering the
    public API. When ``predictions_csv`` is ``None`` or
    ``score_weight`` is zero, lambda integration is effectively
    disabled.
    """

    predictions_csv: str | None = None
    experiment_id: str | None = None
    score_column: str = "lambda_hat"
    score_weight: float = 0.0


@dataclass
class DailyPortfolioRiskConfig:
    """Configuration for scenario-aware daily portfolios and Meta budgets.

    When ``scenario_risk_set_id`` is provided, the daily
    :func:`run_books_for_run` phase will enable inline scenario P&L for
    the core long-only equity book via ``PortfolioConfig``.

    When ``meta_budget_enabled`` is True, the BOOKS phase will compute a
    **Meta budget multiplier** (a capital allocation scalar in [0,1]) and
    apply it when persisting portfolio targets.

    """

    scenario_risk_set_id: str | None = None
    stab_scenario_set_id: str | None = None
    stab_joint_model_id: str = "joint-stab-fragility-v1"

    # Meta budget allocation (capital/cash lives in Meta; books consume the budget).
    meta_budget_enabled: bool = False
    meta_budget_alpha: float = 1.0
    meta_budget_min: float = 0.35
    meta_budget_horizon_steps: int = 21
    meta_budget_region: str | None = None

    # Optional hazard profile used by the market-proxy regime detector.
    hazard_profile: str | None = None

def _fragility_budget_multiplier(score: float) -> float:
    """Step-function overlay using market fragility.

    Mirrors the evaluated "step" strategy:
    - <0.3  -> 1.0 (full exposure)
    - 0.3-0.5 -> 0.5 (half exposure)
    - >=0.5 -> 0.0 (go to cash)
    """
    if score < 0.3:
        return 1.0
    if score < 0.5:
        return 0.5
    return 0.0


def _load_daily_universe_lambda_config(region: str) -> DailyUniverseLambdaConfig:
    """Load lambda config for CORE_EQ_<REGION> universes from YAML.

    The expected schema is ``configs/universe/core_long_eq_daily.yaml``::

        core_long_eq:
          US:
            lambda_predictions_csv: "data/lambda_predictions_US_EQ.csv"
            lambda_experiment_id: "US_EQ_GL_POLY2_V0"
            lambda_score_column: "lambda_hat"
            lambda_score_weight: 10.0

    All keys are optional; missing files or malformed content result in
    a default config with lambda disabled.
    """

    cfg_path = PROJECT_ROOT / "configs" / "universe" / "core_long_eq_daily.yaml"
    if not cfg_path.exists():
        return DailyUniverseLambdaConfig()

    try:
        raw: Any = yaml.safe_load(cfg_path.read_text())
    except Exception:  # pragma: no cover - defensive
        logger.exception(
            "Failed to load daily universe lambda config from %s; disabling lambda for region=%s",
            cfg_path,
            region,
        )
        return DailyUniverseLambdaConfig()

    if not isinstance(raw, dict):
        logger.warning(
            "Daily universe lambda config at %s is not a mapping; disabling lambda for region=%s",
            cfg_path,
            region,
        )
        return DailyUniverseLambdaConfig()

    core_cfg = raw.get("core_long_eq")
    if not isinstance(core_cfg, dict):
        return DailyUniverseLambdaConfig()

    region_cfg = core_cfg.get(region.upper()) or {}
    if not isinstance(region_cfg, dict):
        return DailyUniverseLambdaConfig()

    predictions_csv_raw = region_cfg.get("lambda_predictions_csv")
    experiment_id_raw = region_cfg.get("lambda_experiment_id")
    score_column_raw = region_cfg.get("lambda_score_column", "lambda_hat")
    score_weight_raw = region_cfg.get("lambda_score_weight", 0.0)

    predictions_csv = (
        str(predictions_csv_raw) if isinstance(predictions_csv_raw, str) else None
    )
    experiment_id = str(experiment_id_raw) if isinstance(experiment_id_raw, str) else None
    score_column = str(score_column_raw) if isinstance(score_column_raw, str) else "lambda_hat"
    try:
        score_weight = float(score_weight_raw)
    except (TypeError, ValueError):  # pragma: no cover - defensive
        score_weight = 0.0

    return DailyUniverseLambdaConfig(
        predictions_csv=predictions_csv,
        experiment_id=experiment_id,
        score_column=score_column,
        score_weight=score_weight,
    )


def _load_daily_portfolio_risk_config(region: str) -> DailyPortfolioRiskConfig:
    """Load scenario risk config for <REGION>_CORE_LONG_EQ portfolios.

    The expected schema is ``configs/portfolio/core_long_eq_daily.yaml``::

        core_long_eq:
          US:
            scenario_risk_set_id: "US_EQ_HIST_20D_2020ON"

    Missing files or malformed content result in scenario risk being
    disabled for the given region.
    """

    cfg_path = PROJECT_ROOT / "configs" / "portfolio" / "core_long_eq_daily.yaml"
    if not cfg_path.exists():
        return DailyPortfolioRiskConfig()

    try:
        raw: Any = yaml.safe_load(cfg_path.read_text())
    except Exception:  # pragma: no cover - defensive
        logger.exception(
            "Failed to load daily portfolio risk config from %s; disabling scenario risk for region=%s",
            cfg_path,
            region,
        )
        return DailyPortfolioRiskConfig()

    if not isinstance(raw, dict):
        logger.warning(
            "Daily portfolio risk config at %s is not a mapping; disabling scenario risk for region=%s",
            cfg_path,
            region,
        )
        return DailyPortfolioRiskConfig()

    core_cfg = raw.get("core_long_eq")
    if not isinstance(core_cfg, dict):
        return DailyPortfolioRiskConfig()

    region_cfg = core_cfg.get(region.upper()) or {}
    if not isinstance(region_cfg, dict):
        return DailyPortfolioRiskConfig()

    scenario_set_raw = region_cfg.get("scenario_risk_set_id")
    stab_scenario_set_raw = region_cfg.get("stab_scenario_set_id")
    stab_joint_model_raw = region_cfg.get("stab_joint_model_id", "joint-stab-fragility-v1")

    # New Meta budget settings.
    meta_budget_enabled_raw = region_cfg.get("meta_budget_enabled")
    meta_budget_alpha_raw = region_cfg.get("meta_budget_alpha", 1.0)
    meta_budget_min_raw = region_cfg.get("meta_budget_min", 0.35)
    meta_budget_horizon_raw = region_cfg.get("meta_budget_horizon_steps", 21)
    meta_budget_region_raw = region_cfg.get("meta_budget_region")
    hazard_profile_raw = region_cfg.get("hazard_profile")


    scenario_set_id = str(scenario_set_raw) if isinstance(scenario_set_raw, str) else None
    stab_scenario_set_id = (
        str(stab_scenario_set_raw) if isinstance(stab_scenario_set_raw, str) else None
    )
    stab_joint_model_id = (
        str(stab_joint_model_raw)
        if isinstance(stab_joint_model_raw, str)
        else "joint-stab-fragility-v1"
    )

    def _parse_bool(raw_val: Any, *, default: bool = False) -> bool:
        if isinstance(raw_val, bool):
            return raw_val
        if isinstance(raw_val, str):
            return raw_val.strip().lower() in {"1", "true", "yes", "on"}
        return bool(default)

    meta_budget_enabled = _parse_bool(meta_budget_enabled_raw)

    try:
        meta_budget_alpha = float(meta_budget_alpha_raw)
    except (TypeError, ValueError):
        meta_budget_alpha = 1.0

    try:
        meta_budget_min = float(meta_budget_min_raw)
    except (TypeError, ValueError):
        meta_budget_min = 0.35

    try:
        meta_budget_horizon_steps = int(meta_budget_horizon_raw)
    except (TypeError, ValueError):
        meta_budget_horizon_steps = 21

    meta_budget_region = str(meta_budget_region_raw) if isinstance(meta_budget_region_raw, str) else None

    hazard_profile = str(hazard_profile_raw) if isinstance(hazard_profile_raw, str) else None

    return DailyPortfolioRiskConfig(
        scenario_risk_set_id=scenario_set_id,
        stab_scenario_set_id=stab_scenario_set_id,
        stab_joint_model_id=stab_joint_model_id,
        meta_budget_enabled=meta_budget_enabled,
        meta_budget_alpha=meta_budget_alpha,
        meta_budget_min=meta_budget_min,
        meta_budget_horizon_steps=meta_budget_horizon_steps,
        meta_budget_region=meta_budget_region,
        hazard_profile=hazard_profile,
    )




def _get_region_instruments(
    db_manager: DatabaseManager,
    region: str,
) -> List[Tuple[str, str, str]]:
    """Return list of (instrument_id, issuer_id, market_id) for region.

    Instruments are filtered by ``market_id`` and ``status = 'ACTIVE'``.
    If the region is unknown or no mapping is found, an empty list is
    returned.
    """

    markets = MARKETS_BY_REGION.get(region.upper())
    if not markets:
        logger.warning("No market mapping for region %s; skipping", region)
        return []

    sql = """
        SELECT instrument_id, issuer_id, market_id
        FROM instruments
        WHERE market_id = ANY(%s)
          AND status = 'ACTIVE'
    """

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (list(markets),))
            rows = cursor.fetchall()
        finally:
            cursor.close()

    return [(inst_id, issuer_id, market_id) for inst_id, issuer_id, market_id in rows]


def run_signals_for_run(db_manager: DatabaseManager, run: EngineRun) -> EngineRun:
    """Compute Regime + Profiles + STAB signals for the run's date/region."""

    logger.info(
        "run_signals_for_run: run_id=%s as_of_date=%s region=%s",
        run.run_id,
        run.as_of_date,
        run.region,
    )

    # Transition from WAITING_FOR_DATA to DATA_READY if needed
    if run.phase == RunPhase.WAITING_FOR_DATA:
        run = update_phase(db_manager, run.run_id, RunPhase.DATA_READY)

    # ------------------------------------------------------------------
    # Regime detection (provider-only)
    # ------------------------------------------------------------------

    try:
        risk_cfg = _load_daily_portfolio_risk_config(run.region)
        regime_storage = RegimeStorage(db_manager=db_manager)
        regime_model = MarketProxyRegimeModel(
            db_manager=db_manager,
            profile_name=risk_cfg.hazard_profile,
        )
        regime_engine = RegimeEngine(model=regime_model, storage=regime_storage)
        regime_engine.get_regime(as_of_date=run.as_of_date, region=run.region.upper())
    except Exception:  # pragma: no cover - defensive
        logger.exception(
            "run_signals_for_run: regime detection failed for run_id=%s as_of=%s region=%s",
            run.run_id,
            run.as_of_date,
            run.region,
        )

    # ------------------------------------------------------------------
    # Market Fragility (US_EQ only for now)
    # ------------------------------------------------------------------

    try:
        markets = MARKETS_BY_REGION.get(run.region.upper())
        if markets:
            for market_id in markets:
                fragility_storage = FragilityStorage(db_manager=db_manager)
                market_fragility_model = MarketFragilityModel(db_manager=db_manager)
                
                # Check if already computed for this date/market
                existing = fragility_storage.get_latest_measure(
                    "MARKET",
                    market_id,
                    as_of_date=run.as_of_date,
                )
                if existing is None or existing.as_of_date != run.as_of_date:
                    # Need to compute
                    fragility_engine = FragilityAlphaEngine(
                        model=BasicFragilityAlphaModel(
                            db_manager=db_manager,
                            stability_storage=StabilityStorage(db_manager=db_manager),
                            scenario_set_id=None,
                        ),
                        storage=fragility_storage,
                        market_model=market_fragility_model,
                    )
                    fragility_engine.score_and_save(run.as_of_date, "MARKET", market_id)
                    logger.info(
                        "Market fragility computed: market=%s as_of=%s",
                        market_id,
                        run.as_of_date,
                    )
                else:
                    logger.debug(
                        "Market fragility already exists: market=%s as_of=%s",
                        market_id,
                        run.as_of_date,
                    )
    except Exception:  # pragma: no cover - defensive
        logger.exception(
            "run_signals_for_run: market fragility failed for run_id=%s as_of=%s region=%s",
            run.run_id,
            run.as_of_date,
            run.region,
        )

    instruments = _get_region_instruments(db_manager, run.region)
    if not instruments:
        logger.info("No instruments found for region %s; marking SIGNALS_DONE", run.region)
        return update_phase(db_manager, run.run_id, RunPhase.SIGNALS_DONE)

    calendar = TradingCalendar()
    reader = DataReader(db_manager=db_manager)

    # Filter candidates to instruments that have a close price on as_of_date.
    # This removes stale/delisted instruments that are still marked ACTIVE
    # in the runtime instruments table.
    candidate_total = len(instruments)
    instrument_ids_all = [row[0] for row in instruments]
    prices_today = reader.read_prices_close(
        instrument_ids_all,
        run.as_of_date,
        run.as_of_date,
    )
    tradable_today: set[str] = set()
    if not prices_today.empty:
        tradable_today = {
            str(inst_id)
            for inst_id, close in zip(
                prices_today["instrument_id"].astype(str),
                prices_today["close"].astype(float),
            )
            if float(close) > 0.0
        }

    instruments = [row for row in instruments if row[0] in tradable_today]
    if not instruments:
        logger.info(
            "No tradable instruments with prices on %s for region %s; marking SIGNALS_DONE",
            run.as_of_date,
            run.region,
        )
        return update_phase(db_manager, run.run_id, RunPhase.SIGNALS_DONE)

    tradable_count = len(instruments)

    instrument_ids = [row[0] for row in instruments]
    instrument_to_issuer: Dict[str, str] = {row[0]: row[1] for row in instruments}

    # STAB – prefer precomputed soft_target_classes and only compute missing states.
    stab_storage = StabilityStorage(db_manager=db_manager)

    try:
        have_state = stab_storage.get_entities_with_soft_target_state(
            entity_type="INSTRUMENT",
            entity_ids=instrument_ids,
            as_of_date=run.as_of_date,
        )
    except Exception:  # pragma: no cover - defensive
        logger.exception(
            "Failed to check STAB coverage for run_id=%s as_of_date=%s; computing STAB for all instruments",
            run.run_id,
            run.as_of_date,
        )
        have_state = set()

    missing_stab_ids = [inst_id for inst_id in instrument_ids if inst_id not in have_state]

    logger.info(
        "STAB coverage: as_of=%s present=%d/%d missing=%d",
        run.as_of_date,
        len(have_state),
        len(instrument_ids),
        len(missing_stab_ids),
    )

    if missing_stab_ids:
        # Profiles are only required when computing STAB with profile integration.
        issuer_ids_needed = sorted(
            {
                issuer_id
                for inst_id in missing_stab_ids
                if (issuer_id := instrument_to_issuer.get(inst_id)) is not None
            }
        )

        profile_storage = ProfileStorage(db_manager=db_manager)

        company_builder = ProfileFeatureBuilder(
            db_manager=db_manager,
            data_reader=reader,
            calendar=calendar,
        )
        sovereign_builder = SovereignProfileFeatureBuilder(db_manager=db_manager)
        feature_builder = RoutedProfileFeatureBuilder(
            db_manager=db_manager,
            builders_by_issuer_type={
                "COMPANY": company_builder,
                "SOVEREIGN": sovereign_builder,
            },
            default_builder=company_builder,
        )

        embedder = BasicProfileEmbedder(embedding_dim=16)
        profile_service = ProfileService(
            storage=profile_storage,
            feature_builder=feature_builder,
            embedder=embedder,
        )

        for issuer_id in issuer_ids_needed:
            try:
                profile_service.get_snapshot(issuer_id, run.as_of_date)
            except Exception as exc:  # pragma: no cover - defensive
                logger.exception(
                    "Failed to build profile snapshot issuer_id=%s as_of=%s: %s",
                    issuer_id,
                    run.as_of_date,
                    exc,
                )

        def _instrument_to_issuer(instrument_id: str) -> str | None:
            return instrument_to_issuer.get(instrument_id)

        stab_model = BasicPriceStabilityModel(
            data_reader=reader,
            calendar=calendar,
            window_days=63,
            profile_service=profile_service,
            instrument_to_issuer=_instrument_to_issuer,
        )
        stab_engine = StabilityEngine(model=stab_model, storage=stab_storage)

        for instrument_id in missing_stab_ids:
            try:
                stab_engine.score_entity(run.as_of_date, "INSTRUMENT", instrument_id)
            except ValueError as exc:
                # Insufficient history or data issues: log and continue.
                logger.warning(
                    "Skipping STAB score for instrument %s on %s: %s",
                    instrument_id,
                    run.as_of_date,
                    exc,
                )
            except Exception as exc:  # pragma: no cover - defensive
                logger.exception(
                    "Unexpected error scoring STAB for instrument %s on %s: %s",
                    instrument_id,
                    run.as_of_date,
                    exc,
                )

    # Fragility Alpha – combine STAB soft-target state and optional
    # scenario-based losses into scalar fragility scores. For the daily
    # engine runs we start with a configuration that only uses the latest
    # STAB state (scenario_set_id=None); more advanced scenario integration
    # is handled by dedicated research/CLI workflows.
    fragility_storage = FragilityStorage(db_manager=db_manager)
    fragility_model = BasicFragilityAlphaModel(
        db_manager=db_manager,
        stability_storage=stab_storage,
        scenario_set_id=None,
    )
    fragility_engine = FragilityAlphaEngine(
        model=fragility_model,
        storage=fragility_storage,
    )

    for instrument_id in instrument_ids:
        try:
            fragility_engine.score_and_save(
                run.as_of_date,
                "INSTRUMENT",
                instrument_id,
            )
        except Exception as exc:  # pragma: no cover - defensive
            # Fragility is an overlay on top of STAB; failures here should
            # not block the rest of the signals pipeline.
            logger.exception(
                "Unexpected error scoring Fragility Alpha for instrument %s on %s: %s",
                instrument_id,
                run.as_of_date,
                exc,
            )

    # Assessment – basic price/STAB-based model.
    try:
        markets = MARKETS_BY_REGION.get(run.region.upper())
        market_id = markets[0] if markets else run.region.upper()

        assessment_storage = InstrumentScoreStorage(db_manager=db_manager)
        assessment_model = BasicAssessmentModel(
            data_reader=reader,
            calendar=calendar,
            stability_storage=stab_storage,
        )
        assessment_engine = AssessmentEngine(
            model=assessment_model,
            storage=assessment_storage,
            model_id="assessment-basic-v1",
        )

        # For now we use a simple default strategy identifier tied to the
        # region; this can be replaced by a proper strategies table lookup
        # in a later iteration.
        strategy_id = f"{run.region.upper()}_CORE_LONG_EQ"

        scores = assessment_engine.score_universe(
            strategy_id=strategy_id,
            market_id=market_id,
            instrument_ids=instrument_ids,
            as_of_date=run.as_of_date,
            horizon_days=21,
        )
        
        # Record assessment decision
        try:
            tracker = DecisionTracker(db_manager=db_manager)
            instrument_scores = {inst_id: score.score for inst_id, score in scores.items()}
            
            decision_id = tracker.record_assessment_decision(
                strategy_id=strategy_id,
                market_id=market_id,
                as_of_date=run.as_of_date,
                universe_id=f"CORE_EQ_{run.region.upper()}",
                instrument_scores=instrument_scores,
                model_id="assessment-basic-v1",
                horizon_days=21,
                run_id=run.run_id,
                reasoning={
                    "model_type": "BasicAssessmentModel",
                    "uses_stab": True,
                    "uses_price_momentum": True,
                    "candidate_total": int(candidate_total),
                    "tradable_today": int(tradable_count),
                    "filtered_no_price_today": int(candidate_total - tradable_count),
                    "insufficient_history_count": int(
                        sum(
                            1
                            for s in scores.values()
                            if isinstance(getattr(s, "metadata", None), dict)
                            and (s.metadata or {}).get("insufficient_history") is True
                        )
                    ),
                },
            )
            logger.info(
                "Recorded assessment decision: decision_id=%s strategy_id=%s instruments=%d",
                decision_id,
                strategy_id,
                len(instrument_scores),
            )
        except Exception:  # pragma: no cover - defensive
            logger.exception(
                "run_signals_for_run: failed to record assessment decision for run_id=%s",
                run.run_id,
            )
    except Exception as exc:  # pragma: no cover - defensive
        logger.exception(
            "run_signals_for_run: Assessment Engine failed for run_id=%s: %s",
            run.run_id,
            exc,
        )

    # ------------------------------------------------------------------
    # Sector Health – compute per-sector SHI and persist to DB
    # ------------------------------------------------------------------

    try:
        from datetime import timedelta

        shi_start = run.as_of_date - timedelta(days=400)  # ~252 trading days lookback for SMA200
        shi_result = compute_sector_health(
            start=shi_start,
            end=run.as_of_date,
            db_manager=db_manager,
            load_breadth=True,
        )

        _persist_sector_health_daily(db_manager, run.as_of_date, shi_result)
        logger.info(
            "Sector health computed and persisted: as_of=%s sectors=%d",
            run.as_of_date,
            len(shi_result.scores),
        )
    except Exception:  # pragma: no cover - defensive
        logger.exception(
            "run_signals_for_run: sector health computation failed for run_id=%s as_of=%s",
            run.run_id,
            run.as_of_date,
        )

    # ------------------------------------------------------------------
    # Lambda Daily – compute fresh lambda_hat predictions for today
    # ------------------------------------------------------------------

    try:
        from prometheus.opportunity.lambda_daily import run_daily_lambda

        markets = MARKETS_BY_REGION.get(run.region.upper())
        market_id = markets[0] if markets else run.region.upper()

        lambda_result = run_daily_lambda(
            db_manager=db_manager,
            as_of_date=run.as_of_date,
            market_id=market_id,
        )
        if lambda_result.success:
            logger.info(
                "Lambda daily: as_of=%s market=%s clusters=%d predictions=%d csv=%s",
                run.as_of_date,
                market_id,
                lambda_result.n_clusters,
                lambda_result.n_predictions,
                lambda_result.predictions_csv,
            )
        else:
            logger.warning(
                "Lambda daily skipped: as_of=%s market=%s reason=%s",
                run.as_of_date,
                market_id,
                lambda_result.error,
            )
    except Exception:  # pragma: no cover - non-fatal
        logger.exception(
            "run_signals_for_run: lambda daily step failed for run_id=%s as_of=%s",
            run.run_id,
            run.as_of_date,
        )

    return update_phase(db_manager, run.run_id, RunPhase.SIGNALS_DONE)


def _persist_sector_health_daily(
    db_manager: DatabaseManager,
    as_of_date: date,
    result: SectorHealthResult,
) -> None:
    """Persist latest-day sector health scores to sector_health_daily.

    Uses INSERT ... ON CONFLICT for idempotency.
    """
    sql = """
        INSERT INTO sector_health_daily (
            sector_name, as_of_date, score, raw_composite, signals, created_at
        )
        VALUES (%s, %s, %s, %s, %s, NOW())
        ON CONFLICT (sector_name, as_of_date)
        DO UPDATE SET
            score = EXCLUDED.score,
            raw_composite = EXCLUDED.raw_composite,
            signals = EXCLUDED.signals,
            created_at = EXCLUDED.created_at
    """

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            for sector_name, score_dict in result.scores.items():
                score = score_dict.get(as_of_date)
                if score is None:
                    continue

                raw = None
                if sector_name in result.raw_composites:
                    raw = result.raw_composites[sector_name].get(as_of_date)

                signals_breakdown: dict[str, float] = {}
                if sector_name in result.signals:
                    for sig_name, sig_dict in result.signals[sector_name].items():
                        val = sig_dict.get(as_of_date)
                        if val is not None:
                            signals_breakdown[sig_name] = float(val)

                cursor.execute(
                    sql,
                    (
                        sector_name,
                        as_of_date,
                        float(score),
                        float(raw) if raw is not None else None,
                        Json(signals_breakdown) if signals_breakdown else None,
                    ),
                )
            conn.commit()
        finally:
            cursor.close()


def _load_sector_health_for_date(
    db_manager: DatabaseManager,
    as_of_date: date,
) -> Dict[str, float]:
    """Load sector health scores for a date from sector_health_daily.

    Returns mapping of sector_name → SHI score ∈ [0, 1].
    """
    sql = """
        SELECT sector_name, score
        FROM sector_health_daily
        WHERE as_of_date = %s
    """

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (as_of_date,))
            rows = cursor.fetchall()
        finally:
            cursor.close()

    return {str(row[0]): float(row[1]) for row in rows}


def run_universes_for_run(db_manager: DatabaseManager, run: EngineRun) -> EngineRun:
    """Build universes for the run's date/region and persist members.

    Currently constructs a single long-friendly equity universe
    ``CORE_EQ_<REGION>`` using :class:`BasicUniverseModel`.
    """

    logger.info(
        "run_universes_for_run: run_id=%s as_of_date=%s region=%s",
        run.run_id,
        run.as_of_date,
        run.region,
    )

    markets = MARKETS_BY_REGION.get(run.region.upper())
    if not markets:
        logger.warning("No market mapping for region %s; marking UNIVERSES_DONE", run.region)
        return update_phase(db_manager, run.run_id, RunPhase.UNIVERSES_DONE)

    calendar = TradingCalendar()
    reader = DataReader(db_manager=db_manager)

    # Profiles and STAB storage reused to configure the universe model.
    profile_storage = ProfileStorage(db_manager=db_manager)

    company_builder = ProfileFeatureBuilder(
        db_manager=db_manager,
        data_reader=reader,
        calendar=calendar,
    )
    sovereign_builder = SovereignProfileFeatureBuilder(db_manager=db_manager)
    feature_builder = RoutedProfileFeatureBuilder(
        db_manager=db_manager,
        builders_by_issuer_type={
            "COMPANY": company_builder,
            "SOVEREIGN": sovereign_builder,
        },
        default_builder=company_builder,
    )

    embedder = BasicProfileEmbedder(embedding_dim=16)
    profile_service = ProfileService(
        storage=profile_storage,
        feature_builder=feature_builder,
        embedder=embedder,
    )

    stab_storage = StabilityStorage(db_manager=db_manager)

    universe_storage = UniverseStorage(db_manager=db_manager)

    # STAB state-change forecaster for per-instrument fragility risk
    # integration in universes.
    stab_forecaster = StabilityStateChangeForecaster(storage=stab_storage)

    # Regime state-change forecaster for region-level regime risk. We keep
    # the regime risk alpha at its UniverseConfig default (0.0) for now so
    # that enabling regime-aware universes is an explicit configuration
    # decision rather than a behavioural surprise.
    regime_storage = RegimeStorage(db_manager=db_manager)
    regime_forecaster = RegimeStateChangeForecaster(storage=regime_storage)

    # For this iteration we construct a simple UniverseConfig in-memory
    # rather than loading it from engine_configs. The parameters are
    # conservative defaults for a long-only core equity universe.
    strategy_id = f"{run.region.upper()}_CORE_LONG_EQ"
    universe_config = UniverseConfig(
        strategy_id=strategy_id,
        markets=list(markets),
        max_universe_size=200,
        min_liquidity_adv=100_000.0,
        min_price=1.0,
        sector_max_names=0,
        universe_model_id="basic-equity-v1",
        # Start with regime risk disabled (alpha=0.0) so that turning it on
        # is an explicit config change once regime history is populated.
        regime_region=run.region.upper(),
        regime_risk_alpha=0.0,
        regime_risk_horizon_steps=1,
        stability_risk_alpha=0.5,
        stability_risk_horizon_steps=1,
    )

    # Optional lambda-aware universe configuration driven by YAML. When a
    # predictions CSV and non-zero score_weight are provided for the
    # region, we construct a CsvLambdaClusterScoreProvider and wire it
    # into BasicUniverseModel.
    lambda_cfg = _load_daily_universe_lambda_config(run.region)
    lambda_provider: object | None = None
    lambda_score_weight = 0.0
    if lambda_cfg.predictions_csv is not None and lambda_cfg.score_weight != 0.0:
        lambda_csv_path = Path(lambda_cfg.predictions_csv)
        if not lambda_csv_path.is_absolute():
            lambda_csv_path = PROJECT_ROOT / lambda_csv_path
        try:
            lambda_provider = CsvLambdaClusterScoreProvider(
                csv_path=lambda_csv_path,
                experiment_id=lambda_cfg.experiment_id,
                score_column=lambda_cfg.score_column,
            )
            lambda_score_weight = float(lambda_cfg.score_weight)
        except Exception as exc:  # pragma: no cover - defensive
            # In daily engine runs we treat lambda provider initialisation
            # failures as a non-fatal condition and simply disable lambda
            # integration for the region instead of surfacing a full
            # stack trace on every run.
            logger.warning(
                "run_universes_for_run: disabling lambda integration for region=%s due to error "
                "initialising CsvLambdaClusterScoreProvider from %s: %s",
                run.region,
                lambda_csv_path,
                exc,
            )
            lambda_provider = None
            lambda_score_weight = 0.0

    # TODO(v1-regime): once regime history is populated for the run
    # region, instantiate a RegimeStateChangeForecaster and pass it into
    # BasicUniverseModel via ``regime_forecaster``, ``regime_region``, and
    # a non-zero ``regime_risk_alpha`` to make universes explicitly
    # regime/state-aware.
    universe_model = BasicUniverseModel(
        db_manager=db_manager,
        calendar=calendar,
        data_reader=reader,
        profile_service=profile_service,
        stability_storage=stab_storage,
        market_ids=tuple(universe_config.markets),
        min_avg_volume=universe_config.min_liquidity_adv,
        max_universe_size=universe_config.max_universe_size,
        sector_max_names=universe_config.sector_max_names,
        min_price=universe_config.min_price,
        hard_exclusion_list=tuple(universe_config.hard_exclusion_list),
        issuer_exclusion_list=tuple(universe_config.issuer_exclusion_list),
        # Align Assessment strategy id with the one used in the signals
        # phase so that universe ranking can incorporate Assessment
        # scores when available.
        use_assessment_scores=True,
        assessment_strategy_id=strategy_id,
        assessment_horizon_days=21,
        # Regime risk integration remains disabled until
        # ``universe_config.regime_risk_alpha`` is set to a non-zero value
        # in configuration. The forecaster is wired so that enabling
        # regime-aware universes is a config-only change.
        regime_forecaster=regime_forecaster,
        regime_region=universe_config.regime_region or run.region.upper(),
        regime_risk_alpha=universe_config.regime_risk_alpha,
        regime_risk_horizon_steps=universe_config.regime_risk_horizon_steps,
        # STAB state-change risk integration: apply a modest multiplicative
        # penalty based on per-instrument soft-target state-change risk.
        stability_state_change_forecaster=stab_forecaster,
        stability_risk_alpha=universe_config.stability_risk_alpha,
        stability_risk_horizon_steps=universe_config.stability_risk_horizon_steps,
        # Optional lambda opportunity integration.
        lambda_score_provider=lambda_provider,
        lambda_score_weight=lambda_score_weight,
    )
    universe_engine = UniverseEngine(model=universe_model, storage=universe_storage)

    universe_id = f"CORE_EQ_{run.region.upper()}"
    
    # Get candidate instruments before filtering
    candidate_instruments = _get_region_instruments(db_manager, run.region)
    candidate_ids = {row[0] for row in candidate_instruments}
    
    universe_engine.build_and_save(run.as_of_date, universe_id)

    # Record universe decision with excluded instruments and reasons
    try:
        tracker = DecisionTracker(db_manager=db_manager)
        members = universe_storage.get_universe(
            as_of_date=run.as_of_date,
            universe_id=universe_id,
            entity_type="INSTRUMENT",
            included_only=True,
        )
        included_ids = [m.entity_id for m in members]
        excluded_ids = sorted(candidate_ids - set(included_ids))
        
        # Get assessment scores to understand exclusion reasons.
        # instrument_scores is append-only per (strategy, instrument, date), so
        # we take the latest row per instrument_id.
        excluded_scores: Dict[str, float] = {}
        excluded_sample = excluded_ids[:100]  # Limit for performance
        if excluded_sample:
            sql = """
                SELECT DISTINCT ON (instrument_id)
                    instrument_id,
                    score
                FROM instrument_scores
                WHERE strategy_id = %s
                  AND market_id = %s
                  AND as_of_date = %s
                  AND horizon_days = %s
                  AND instrument_id = ANY(%s)
                  AND (metadata->>'model_id') = %s
                ORDER BY instrument_id, created_at DESC
            """
            with db_manager.get_runtime_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(
                        sql,
                        (
                            strategy_id,
                            markets[0] if markets else "UNKNOWN",
                            run.as_of_date,
                            21,
                            excluded_sample,
                            "assessment-basic-v1",
                        ),
                    )
                    for inst_id, score in cursor.fetchall():
                        if score is None:
                            continue
                        excluded_scores[str(inst_id)] = float(score)
                finally:
                    cursor.close()
        
        # Build exclusion reasons based on config and scores
        exclusion_reasons: Dict[str, Any] = {
            "total_candidates": len(candidate_ids),
            "total_excluded": len(excluded_ids),
            "max_universe_size": universe_config.max_universe_size,
            "min_liquidity_adv": universe_config.min_liquidity_adv,
            "min_price": universe_config.min_price,
            "stability_risk_alpha": universe_config.stability_risk_alpha,
            "excluded_with_scores": excluded_scores,
        }
        
        markets_str = ", ".join(markets) if markets else "N/A"
        decision_id = tracker.record_universe_decision(
            strategy_id=strategy_id,
            market_id=markets[0] if markets else "UNKNOWN",
            as_of_date=run.as_of_date,
            universe_id=universe_id,
            included_instruments=included_ids,
            excluded_instruments=excluded_ids,
            run_id=run.run_id,
            inclusion_reasons={
                "lambda_enabled": lambda_score_weight > 0,
                "lambda_weight": lambda_score_weight,
                "max_size": universe_config.max_universe_size,
                "markets": markets_str,
            },
            exclusion_reasons=exclusion_reasons,
        )
        logger.info(
            "Recorded universe decision: decision_id=%s universe_id=%s included=%d excluded=%d",
            decision_id,
            universe_id,
            len(included_ids),
            len(excluded_ids),
        )
    except Exception:  # pragma: no cover - defensive
        logger.exception(
            "run_universes_for_run: failed to record universe decision for run_id=%s",
            run.run_id,
        )

    return update_phase(db_manager, run.run_id, RunPhase.UNIVERSES_DONE)


def run_books_for_run(
    db_manager: DatabaseManager,
    run: EngineRun,
    *,
    apply_risk: bool = True,
) -> EngineRun:
    """Run book-level strategies for the run's date/region.

    This phase is now *meta-routed*: we compute a MarketSituation label
    (from Regime + market fragility), consult the meta policy artifact
    (configs/meta/policy.yaml), and execute the chosen {book_id, sleeve_id}
    from the book registry (configs/meta/books.yaml).

    - LONG_EQUITY books reuse the existing ``CORE_EQ_<REGION>`` universe and
      derive target weights via the PortfolioEngine.
    - HEDGE_ETF books produce rule-based ETF targets (no Assessment/Universe).
    - ALLOCATOR books blend a long-only target with hedge ETF weights into a
      single master target_portfolios row.
    - CASH produces no targets.
    """

    logger.info(
        "run_books_for_run: run_id=%s as_of_date=%s region=%s",
        run.run_id,
        run.as_of_date,
        run.region,
    )

    markets = MARKETS_BY_REGION.get(run.region.upper())
    if not markets:
        logger.warning("No market mapping for region %s; marking BOOKS_DONE", run.region)
        return update_phase(db_manager, run.run_id, RunPhase.BOOKS_DONE)

    # ------------------------------------------------------------------
    # Meta policy selection (book + sleeve)
    # ------------------------------------------------------------------

    market_id = markets[0] if markets else None

    book_registry = load_book_registry()
    policy_artifact = load_meta_policy_artifact()
    policies = policy_artifact.policies

    situation_info = None
    selected_book_id: str | None = None
    selected_sleeve_id: str | None = None

    try:
        if market_id is not None:
            situation_info = MarketSituationService(db_manager=db_manager).get_situation(
                market_id=str(market_id),
                as_of_date=run.as_of_date,
                region=run.region,
            )
            policy = policies.get(str(market_id).upper())
            if policy is not None:
                sel = policy.select(situation_info.situation)
                selected_book_id = sel.book_id
                selected_sleeve_id = sel.sleeve_id
    except Exception:  # pragma: no cover - defensive
        logger.exception(
            "run_books_for_run: meta policy selection failed for run_id=%s as_of=%s region=%s",
            run.run_id,
            run.as_of_date,
            run.region,
        )

    # Resolve selected book+sleeve against the registry; if no selection is
    # available, fall back to the legacy core long equity behaviour.
    book_spec: BookSpec | None = None
    if selected_book_id is not None:
        book_spec = book_registry.get(selected_book_id)

    # Record the meta routing decision (best-effort).
    try:
        if situation_info is not None and market_id is not None:
            storage = MetaStorage(db_manager=db_manager)
            decision_id = generate_uuid()
            storage.save_engine_decision(
                EngineDecision(
                    decision_id=decision_id,
                    engine_name="META_POLICY_V1",
                    run_id=run.run_id,
                    strategy_id=selected_book_id,
                    market_id=str(market_id),
                    as_of_date=run.as_of_date,
                    config_id=selected_sleeve_id,
                    input_refs={
                        "policy_version": policy_artifact.version,
                        "policy_updated_at": policy_artifact.updated_at,
                        "policy_updated_by": policy_artifact.updated_by,
                        "market_situation": situation_info.situation.value,
                        "regime_label": situation_info.regime_label.value if situation_info.regime_label else None,
                        "prev_regime_label": situation_info.prev_regime_label.value if situation_info.prev_regime_label else None,
                        "fragility_score": situation_info.fragility_score,
                        "fragility_class": situation_info.fragility_class,
                    },
                    output_refs={
                        "selected_book_id": selected_book_id,
                        "selected_sleeve_id": selected_sleeve_id,
                    },
                    metadata={"type": "book_sleeve_routing"},
                )
            )
    except Exception:  # pragma: no cover - defensive
        logger.exception(
            "run_books_for_run: failed to record meta policy decision for run_id=%s",
            run.run_id,
        )

    universe_storage = UniverseStorage(db_manager=db_manager)

    # ------------------------------------------------------------------
    # Resolve selected book + sleeve (or fall back to legacy core book)
    # ------------------------------------------------------------------

    # Legacy defaults: keep behaviour identical if the registry/policy is
    # not configured or if the selected book is unknown.
    universe_id = f"CORE_EQ_{run.region.upper()}"
    book_id = f"{run.region.upper()}_CORE_LONG_EQ"

    selected_kind: BookKind = BookKind.LONG_EQUITY
    resolved_sleeve_id: str | None = None
    sleeve: LongEquitySleeveSpec | HedgeEtfSleeveSpec | AllocatorSleeveSpec | None = None

    if book_spec is not None:
        selected_kind = book_spec.kind
        book_id = book_spec.book_id
        resolved_sleeve_id = book_spec.resolve_sleeve_id(selected_sleeve_id)
        if resolved_sleeve_id is not None:
            sleeve = book_spec.sleeves.get(resolved_sleeve_id)

    # CASH book: produce no targets (explicit cash) and finish.
    if selected_kind == BookKind.CASH:
        logger.info(
            "run_books_for_run: selected CASH book for run_id=%s as_of=%s region=%s",
            run.run_id,
            run.as_of_date,
            run.region,
        )
        return update_phase(db_manager, run.run_id, RunPhase.BOOKS_DONE)

    # HEDGE_ETF book: rule-based targets, no Assessment/Universe dependency.
    if selected_kind == BookKind.HEDGE_ETF:
        hedge_sleeve = sleeve if isinstance(sleeve, HedgeEtfSleeveSpec) else None
        if hedge_sleeve is None:
            logger.warning(
                "run_books_for_run: HEDGE_ETF selected but no valid sleeve; falling back to CASH run_id=%s",
                run.run_id,
            )
            return update_phase(db_manager, run.run_id, RunPhase.BOOKS_DONE)

        # Compute hedge allocation (NAV weights) from situation signals.
        alloc = 0.0
        try:
            if situation_info is not None:
                if str(hedge_sleeve.sizing_mode) == "fragility_linear":
                    frag = float(situation_info.fragility_score or 0.0)
                    thr = float(hedge_sleeve.fragility_threshold)
                    if thr < 0.0:
                        thr = 0.0
                    if thr >= 1.0:
                        thr = 0.99
                    raw = (frag - thr) / (1.0 - thr)
                    alloc = max(0.0, min(1.0, raw))
                else:
                    # regime_based sizing uses the situation label.
                    sit = situation_info.situation
                    if sit.value == "CRISIS":
                        alloc = 1.0
                    elif sit.value == "RISK_OFF":
                        alloc = 0.5
                    elif sit.value == "RECOVERY":
                        alloc = 0.25
                    else:
                        alloc = 0.0
        except Exception:  # pragma: no cover - defensive
            alloc = 0.0

        max_alloc = max(0.0, min(1.0, float(hedge_sleeve.max_hedge_allocation)))
        alloc = max(0.0, min(1.0, float(alloc)))
        alloc = max(0.0, min(max_alloc, alloc))

        # Filter to tradable instruments (must have a close price today).
        reader = DataReader(db_manager=db_manager)
        inst_ids = list(hedge_sleeve.instrument_ids)
        prices_today = reader.read_prices_close(inst_ids, run.as_of_date, run.as_of_date)
        tradable: set[str] = set()
        if not prices_today.empty:
            tradable = {
                str(inst_id)
                for inst_id, close in zip(
                    prices_today["instrument_id"].astype(str),
                    prices_today["close"].astype(float),
                )
                if float(close) > 0.0
            }

        inst_ids = [inst for inst in inst_ids if inst in tradable]
        if not inst_ids:
            logger.warning(
                "run_books_for_run: no tradable hedge ETF instruments for sleeve=%s as_of=%s; falling back to CASH",
                hedge_sleeve.sleeve_id,
                run.as_of_date,
            )
            return update_phase(db_manager, run.run_id, RunPhase.BOOKS_DONE)

        requested_alloc = float(alloc)

        weights: dict[str, float] = {}
        if alloc > 0.0:
            w = alloc / float(len(inst_ids))
            weights = {inst: float(w) for inst in inst_ids}

        # Apply risk constraints BEFORE persisting so execution consumes
        # the risk-adjusted weights (binding constraints).
        risk_summary: dict[str, object] = {}
        if apply_risk and weights:
            decisions = [
                {"instrument_id": inst_id, "target_weight": float(weight)}
                for inst_id, weight in weights.items()
            ]
            try:
                adjusted = apply_risk_constraints(
                    decisions,
                    strategy_id=book_id,
                    db_manager=db_manager,
                )
                weights = {
                    str(d["instrument_id"]): float(d.get("target_weight", 0.0))
                    for d in adjusted
                    if d.get("instrument_id") is not None
                }
                num_capped = sum(1 for d in adjusted if d.get("risk_action_type") == "CAPPED")
                num_rejected = sum(1 for d in adjusted if d.get("risk_action_type") == "REJECTED")
                risk_summary = {
                    "risk_constraints_applied": True,
                    "risk_num_capped": int(num_capped),
                    "risk_num_rejected": int(num_rejected),
                }
            except Exception:  # pragma: no cover - defensive
                logger.exception(
                    "run_books_for_run: apply_risk_constraints failed for hedge book_id=%s as_of=%s",
                    book_id,
                    run.as_of_date,
                )
                risk_summary = {"risk_constraints_error": "apply_failed"}

        net_exposure = float(sum(weights.values()))
        gross_exposure = float(sum(abs(w) for w in weights.values()))
        cash_weight = float(max(0.0, 1.0 - net_exposure))

        # Persist hedge targets into book_targets.
        portfolio_storage = PortfolioStorage(db_manager=db_manager)

        hedge_universe_id = f"{book_id}_UNIVERSE"
        members = [
            UniverseMember(
                as_of_date=run.as_of_date,
                universe_id=hedge_universe_id,
                entity_type="INSTRUMENT",
                entity_id=inst,
                included=True,
                score=float(weights.get(inst, 0.0)),
                reasons={
                    "book_kind": "HEDGE_ETF",
                    "sleeve_id": hedge_sleeve.sleeve_id,
                    "leg": "HEDGE",
                },
                # L3 contract: tier must be one of CORE/SATELLITE/EXCLUDED.
                tier="SATELLITE",
            )
            for inst in inst_ids
        ]
        try:
            universe_storage.save_members(members)
        except Exception:  # pragma: no cover - defensive
            logger.exception(
                "run_books_for_run: failed to save hedge universe members for book_id=%s as_of=%s",
                book_id,
                run.as_of_date,
            )

        meta_extra: dict[str, object] = {
            "book_kind": "HEDGE_ETF",
            "sleeve_id": hedge_sleeve.sleeve_id,
            "hedge_allocation_requested": requested_alloc,
            "hedge_allocation": float(net_exposure),
            "cash_weight": float(cash_weight),
        } | risk_summary
        if situation_info is not None:
            meta_extra |= {
                "market_situation": situation_info.situation.value,
                "fragility_score": situation_info.fragility_score,
                "fragility_class": situation_info.fragility_class,
                "regime_label": situation_info.regime_label.value if situation_info.regime_label else None,
            }

        portfolio_storage.save_book_targets(
            portfolio_id=book_id,
            region=run.region,
            as_of_date=run.as_of_date,
            members=members,
            weights=weights,
            metadata_extra=meta_extra,
        )

        # Also persist an aggregated row into target_portfolios so the
        # execution bridge can operate on hedge books the same way as
        # LONG_EQUITY books.
        try:
            meta_budget: dict[str, object] = {
                "budget_mult": 1.0,
                "meta_selected_book_id": selected_book_id,
                "meta_selected_sleeve_id": resolved_sleeve_id,
                "market_situation": situation_info.situation.value if situation_info is not None else None,
            }

            target = TargetPortfolio(
                portfolio_id=book_id,
                as_of_date=run.as_of_date,
                weights=weights,
                expected_return=0.0,
                expected_volatility=0.0,
                risk_metrics={
                    "net_exposure": net_exposure,
                    "gross_exposure": gross_exposure,
                    "cash_weight": cash_weight,
                    "hedge_allocation": float(net_exposure),
                    "hedge_allocation_requested": float(requested_alloc),
                },
                factor_exposures={},
                constraints_status={"rule_based": True},
                metadata={
                    "risk_model_id": "rule-based-hedge-v1",
                    "meta_budget": meta_budget,
                }
                | meta_extra,
            )

            portfolio_storage.save_target_portfolio(strategy_id=book_id, target=target)
        except Exception:  # pragma: no cover - defensive
            logger.exception(
                "run_books_for_run: failed to persist hedge target_portfolios row for book_id=%s as_of=%s",
                book_id,
                run.as_of_date,
            )

        # Record portfolio decision (best-effort).
        try:
            tracker = DecisionTracker(db_manager=db_manager)
            tracker.record_portfolio_decision(
                strategy_id=book_id,
                market_id=str(market_id) if market_id is not None else "UNKNOWN",
                as_of_date=run.as_of_date,
                portfolio_id=book_id,
                target_weights=weights,
                run_id=run.run_id,
                constraints_applied={"book_kind": "HEDGE_ETF"},
                risk_metrics={"cash_weight": cash_weight},
            )
        except Exception:  # pragma: no cover - defensive
            logger.exception(
                "run_books_for_run: failed to record hedge portfolio decision for run_id=%s",
                run.run_id,
            )

        return update_phase(db_manager, run.run_id, RunPhase.BOOKS_DONE)

    # ALLOCATOR book: blend a long equity target with hedge ETF weights.
    if selected_kind == BookKind.ALLOCATOR:
        alloc_sleeve = sleeve if isinstance(sleeve, AllocatorSleeveSpec) else None
        if alloc_sleeve is None:
            logger.warning(
                "run_books_for_run: ALLOCATOR selected but no valid sleeve; falling back to CASH run_id=%s",
                run.run_id,
            )
            return update_phase(db_manager, run.run_id, RunPhase.BOOKS_DONE)

        # Hedge allocation based on fragility (advance) and/or situation.
        #
        # The goal is:
        # - fragility provides *advance* hedging (before the situation flips),
        # - situation provides a floor / override for extreme regimes (e.g. CRISIS).
        hedge_alloc = 0.0
        frag = float(getattr(situation_info, "fragility_score", 0.0) or 0.0) if situation_info is not None else 0.0
        sit = situation_info.situation if situation_info is not None else None

        try:
            sizing_mode = str(getattr(alloc_sleeve, "hedge_sizing_mode", "fragility_linear"))
            if sizing_mode == "fragility_linear":
                thr = float(alloc_sleeve.fragility_threshold)
                thr = max(0.0, min(0.99, thr))
                raw = (frag - thr) / (1.0 - thr)
                hedge_alloc = max(0.0, min(1.0, float(raw)))
            else:
                # regime_based sizing uses config-provided overrides by situation.
                if sit is not None:
                    overrides = getattr(alloc_sleeve, "hedge_allocation_overrides", None) or {}
                    hedge_alloc = float(overrides.get(sit.value, 0.0) or 0.0)
                else:
                    hedge_alloc = 0.0
        except Exception:  # pragma: no cover - defensive
            hedge_alloc = 0.0

        # Situation-based floors/caps/overrides (config-driven).
        try:
            if sit is not None:
                floors = getattr(alloc_sleeve, "hedge_allocation_floors", None) or {}
                caps = getattr(alloc_sleeve, "hedge_allocation_caps", None) or {}
                overrides = getattr(alloc_sleeve, "hedge_allocation_overrides", None) or {}

                floor = floors.get(sit.value)
                if floor is not None:
                    hedge_alloc = max(hedge_alloc, float(floor))

                cap = caps.get(sit.value)
                if cap is not None:
                    hedge_alloc = min(hedge_alloc, float(cap))

                override = overrides.get(sit.value)
                if override is not None:
                    hedge_alloc = float(override)
        except Exception:  # pragma: no cover - defensive
            pass

        hedge_alloc = max(0.0, min(1.0, float(hedge_alloc)))

        max_alloc = max(0.0, min(1.0, float(alloc_sleeve.max_hedge_allocation)))
        hedge_alloc = max(0.0, min(max_alloc, hedge_alloc))

        # Filter hedge instruments to tradable (must have a close price today).
        reader = DataReader(db_manager=db_manager)
        hedge_ids = list(getattr(alloc_sleeve, "hedge_instrument_ids", ()) or ())
        prices_today = reader.read_prices_close(hedge_ids, run.as_of_date, run.as_of_date)
        tradable: set[str] = set()
        if not prices_today.empty:
            tradable = {
                str(inst_id)
                for inst_id, close in zip(
                    prices_today["instrument_id"].astype(str),
                    prices_today["close"].astype(float),
                )
                if float(close) > 0.0
            }
        hedge_ids = [inst for inst in hedge_ids if inst in tradable]

        if hedge_alloc > 0.0 and not hedge_ids:
            logger.warning(
                "run_books_for_run: allocator hedge_alloc>0 but no tradable hedge ETFs; disabling hedge for book_id=%s as_of=%s",
                book_id,
                run.as_of_date,
            )
            hedge_alloc = 0.0

        # Optional fragility overlay applied to the long leg.
        long_overlay_mult = 1.0
        long_overlay_meta: dict[str, object] = {}
        try:
            if bool(getattr(alloc_sleeve, "apply_fragility_overlay", False)) and book_spec is not None:
                overlay_cfg = overlay_config_from_sleeve_spec(alloc_sleeve)
                mult_computed, diag = compute_fragility_overlay(
                    db_manager=db_manager,
                    market_id=str(book_spec.market_id),
                    as_of_date=run.as_of_date,
                    cfg=overlay_cfg,
                )
                long_overlay_mult = float(mult_computed)
                long_overlay_meta = dict(diag)
                long_overlay_meta["fragility_budget_mult_computed"] = float(mult_computed)
                long_overlay_meta["fragility_budget_mult"] = float(mult_computed)
                long_overlay_meta["fragility_overlay_enabled"] = True
        except Exception:  # pragma: no cover - defensive
            long_overlay_mult = 1.0
            long_overlay_meta = {"fragility_budget_error": "overlay_failed"}

        long_alloc = max(0.0, 1.0 - float(hedge_alloc))
        long_alloc_eff = float(long_alloc) * float(long_overlay_mult)

        # Build long-only weights from the core universe.
        universe_id = f"CORE_EQ_{run.region.upper()}"

        portfolio_cfg = PortfolioConfig(
            portfolio_id=book_id,
            strategies=[book_id],
            markets=list(MARKETS_BY_REGION.get(run.region.upper(), ())),
            base_currency="USD",
            risk_model_id="basic-longonly-v1",
            optimizer_type="SIMPLE_LONG_ONLY",
            risk_aversion_lambda=0.0,
            leverage_limit=1.0,
            gross_exposure_limit=1.0,
            per_instrument_max_weight=float(getattr(alloc_sleeve, "portfolio_per_instrument_max_weight", 0.05) or 0.05),
            max_names=getattr(alloc_sleeve, "portfolio_max_names", None),
            hysteresis_buffer=getattr(alloc_sleeve, "portfolio_hysteresis_buffer", None),
            sector_limits={},
            country_limits={},
            factor_limits={},
            fragility_exposure_limit=0.5,
            turnover_limit=0.5,
            cost_model_id="none",
            scenario_risk_scenario_set_ids=[],
        )

        portfolio_storage = PortfolioStorage(db_manager=db_manager)
        model = BasicLongOnlyPortfolioModel(
            universe_storage=universe_storage,
            config=portfolio_cfg,
            universe_id=universe_id,
        )

        long_target = model.build_target_portfolio(book_id, run.as_of_date)
        long_weights = {k: float(v) * float(long_alloc_eff) for k, v in (long_target.weights or {}).items()}

        # Hedge ETF weights (equal-weight).
        hedge_weights: dict[str, float] = {}
        if hedge_alloc > 0.0 and hedge_ids:
            w = float(hedge_alloc) / float(len(hedge_ids))
            hedge_weights = {inst: float(w) for inst in hedge_ids}

        # Merge weights.
        weights: dict[str, float] = dict(long_weights)
        for inst_id, w in hedge_weights.items():
            weights[inst_id] = float(weights.get(inst_id, 0.0)) + float(w)

        # Apply risk constraints (binding) before persistence.
        risk_summary: dict[str, object] = {}
        if apply_risk and weights:
            decisions = [
                {"instrument_id": inst_id, "target_weight": float(weight)}
                for inst_id, weight in weights.items()
            ]
            try:
                adjusted = apply_risk_constraints(
                    decisions,
                    strategy_id=book_id,
                    db_manager=db_manager,
                )
                weights = {
                    str(d["instrument_id"]): float(d.get("target_weight", 0.0))
                    for d in adjusted
                    if d.get("instrument_id") is not None
                }
                num_capped = sum(1 for d in adjusted if d.get("risk_action_type") == "CAPPED")
                num_rejected = sum(1 for d in adjusted if d.get("risk_action_type") == "REJECTED")
                risk_summary = {
                    "risk_constraints_applied": True,
                    "risk_num_capped": int(num_capped),
                    "risk_num_rejected": int(num_rejected),
                }
            except Exception:  # pragma: no cover - defensive
                logger.exception(
                    "run_books_for_run: apply_risk_constraints failed for allocator book_id=%s as_of=%s",
                    book_id,
                    run.as_of_date,
                )
                risk_summary = {"risk_constraints_error": "apply_failed"}

        net_exposure = float(sum(weights.values()))
        gross_exposure = float(sum(abs(w) for w in weights.values()))
        cash_weight = float(max(0.0, 1.0 - net_exposure))

        # Persist combined universe members for transparency.
        master_universe_id = f"{book_id}_UNIVERSE"
        hedge_set = set(hedge_ids)
        members = [
            UniverseMember(
                as_of_date=run.as_of_date,
                universe_id=master_universe_id,
                entity_type="INSTRUMENT",
                entity_id=inst,
                included=True,
                score=float(weights.get(inst, 0.0)),
                reasons={
                    "book_kind": "ALLOCATOR",
                    "sleeve_id": alloc_sleeve.sleeve_id,
                    "leg": "HEDGE" if inst in hedge_set else "LONG",
                },
                # L3 contract: tier must be one of CORE/SATELLITE/EXCLUDED.
                tier="SATELLITE" if inst in hedge_set else "CORE",
            )
            for inst in weights.keys()
        ]
        try:
            universe_storage.save_members(members)
        except Exception:  # pragma: no cover - defensive
            logger.exception(
                "run_books_for_run: failed to save allocator universe members for book_id=%s as_of=%s",
                book_id,
                run.as_of_date,
            )

        meta_extra: dict[str, object] = {
            "book_kind": "ALLOCATOR",
            "sleeve_id": alloc_sleeve.sleeve_id,
            "hedge_allocation": float(hedge_alloc),
            "long_allocation": float(long_alloc_eff),
            "cash_weight": float(cash_weight),
            "fragility_score": float(frag),
            "market_situation": situation_info.situation.value if situation_info is not None else None,
        } | risk_summary | long_overlay_meta

        portfolio_storage.save_book_targets(
            portfolio_id=book_id,
            region=run.region,
            as_of_date=run.as_of_date,
            members=members,
            weights=weights,
            metadata_extra=meta_extra,
        )

        # Persist aggregated target_portfolios row for execution.
        try:
            meta_budget: dict[str, object] = {
                "budget_mult": 1.0,
                "meta_selected_book_id": selected_book_id,
                "meta_selected_sleeve_id": resolved_sleeve_id,
                "market_situation": situation_info.situation.value if situation_info is not None else None,
            }

            target = TargetPortfolio(
                portfolio_id=book_id,
                as_of_date=run.as_of_date,
                weights=weights,
                expected_return=0.0,
                expected_volatility=0.0,
                risk_metrics={
                    "net_exposure": net_exposure,
                    "gross_exposure": gross_exposure,
                    "cash_weight": cash_weight,
                    "hedge_allocation": float(hedge_alloc),
                    "long_allocation": float(long_alloc_eff),
                },
                factor_exposures={},
                constraints_status={"allocator": True},
                metadata={
                    "risk_model_id": "allocator-v1",
                    "meta_budget": meta_budget,
                }
                | meta_extra,
            )

            portfolio_storage.save_target_portfolio(strategy_id=book_id, target=target)

            # Optional: persist a basic risk report for monitoring.
            try:
                report = model.build_risk_report(book_id, run.as_of_date, target=target)
                if report is not None:
                    portfolio_storage.save_portfolio_risk_report(model_id="allocator-v1", report=report)
            except Exception:  # pragma: no cover - defensive
                logger.exception(
                    "run_books_for_run: allocator risk report failed for book_id=%s as_of=%s",
                    book_id,
                    run.as_of_date,
                )
        except Exception:  # pragma: no cover - defensive
            logger.exception(
                "run_books_for_run: failed to persist allocator target_portfolios row for book_id=%s as_of=%s",
                book_id,
                run.as_of_date,
            )

        # Record portfolio decision (best-effort).
        try:
            tracker = DecisionTracker(db_manager=db_manager)
            tracker.record_portfolio_decision(
                strategy_id=book_id,
                market_id=str(market_id) if market_id is not None else "UNKNOWN",
                as_of_date=run.as_of_date,
                portfolio_id=book_id,
                target_weights=weights,
                run_id=run.run_id,
                constraints_applied={"book_kind": "ALLOCATOR"},
                risk_metrics={
                    "cash_weight": cash_weight,
                    "hedge_allocation": float(hedge_alloc),
                    "long_allocation": float(long_alloc_eff),
                },
            )
        except Exception:  # pragma: no cover - defensive
            logger.exception(
                "run_books_for_run: failed to record allocator portfolio decision for run_id=%s",
                run.run_id,
            )

        return update_phase(db_manager, run.run_id, RunPhase.BOOKS_DONE)

    # LONG_EQUITY book: we reuse the existing core long-only equity pipeline.
    long_sleeve = sleeve if isinstance(sleeve, LongEquitySleeveSpec) else None
    if long_sleeve is None:
        # Legacy behaviour (no explicit sleeve): keep current defaults.
        long_sleeve = LongEquitySleeveSpec(
            sleeve_id=resolved_sleeve_id or "LEGACY",
            portfolio_max_names=None,
            portfolio_hysteresis_buffer=None,
            portfolio_per_instrument_max_weight=None,
            apply_fragility_overlay=True,
        )

    # Optional scenario-based risk configuration for the daily core book
    # driven by YAML. When a ``scenario_risk_set_id`` is provided for the
    # region, we enable inline scenario P&L inside the PortfolioEngine.
    risk_cfg = _load_daily_portfolio_risk_config(run.region)
    scenario_set_ids: list[str] = []
    if risk_cfg.scenario_risk_set_id is not None:
        scenario_set_ids = [risk_cfg.scenario_risk_set_id]

    # ------------------------------------------------------------------
    # Meta budget allocation (capital / cash lives here)
    # ------------------------------------------------------------------

    budget_mult: float | None = None
    budget_metadata: dict[str, object] | None = None

    if risk_cfg.meta_budget_enabled:
        region_for_budget = (risk_cfg.meta_budget_region or run.region).upper()
        horizon = int(risk_cfg.meta_budget_horizon_steps)
        alpha = float(risk_cfg.meta_budget_alpha)
        m_min = float(risk_cfg.meta_budget_min)

        if horizon <= 0:
            horizon = 1
        if m_min < 0.0:
            m_min = 0.0
        if m_min > 1.0:
            m_min = 1.0

        try:
            regime_storage = RegimeStorage(db_manager=db_manager)
            regime_forecaster = RegimeStateChangeForecaster(storage=regime_storage)
            risk = regime_forecaster.forecast(
                region=region_for_budget,
                horizon_steps=horizon,
                as_of_date=run.as_of_date,
            )

            risk_score = float(getattr(risk, "risk_score", 0.0) or 0.0) if risk is not None else 0.0
            p_change_any = (
                float(getattr(risk, "p_change_any", 0.0) or 0.0) if risk is not None else None
            )

            mult_raw = 1.0 - alpha * risk_score
            if mult_raw < m_min:
                mult_raw = m_min
            if mult_raw > 1.0:
                mult_raw = 1.0

            budget_mult = float(mult_raw)

            budget_metadata = {
                "region": region_for_budget,
                "horizon_steps": horizon,
                "alpha": alpha,
                "min_budget": m_min,
                "regime_risk_score": risk_score,
                "regime_p_change_any": p_change_any,
            }

            distribution = getattr(risk, "distribution", None) if risk is not None else None
            if distribution:
                budget_metadata["distribution"] = {k.value: float(v) for k, v in distribution.items()}

            logger.info(
                "run_books_for_run: meta budget enabled region=%s as_of=%s mult=%.4f risk_score=%.3f",
                region_for_budget,
                run.as_of_date,
                float(budget_mult),
                float(risk_score),
            )
        except Exception:  # pragma: no cover - defensive
            logger.exception(
                "run_books_for_run: meta budget computation failed for run_id=%s as_of=%s region=%s",
                run.run_id,
                run.as_of_date,
                region_for_budget,
            )
            budget_mult = 1.0
            budget_metadata = {
                "region": region_for_budget,
                "horizon_steps": horizon,
                "alpha": alpha,
                "min_budget": m_min,
                "error": "forecast_failed",
            }

    # ------------------------------------------------------------------
    # Fragility overlay: scale budget by market fragility (config-driven)
    # ------------------------------------------------------------------
    fragility_mult = 1.0
    fragility_meta: dict[str, object] = {}

    overlay_enabled = bool(getattr(long_sleeve, "apply_fragility_overlay", False))

    try:
        # Prefer the selected book's configured market_id; fall back to the
        # region mapping.
        market_id_eff: str | None = None
        if book_spec is not None and isinstance(getattr(book_spec, "market_id", None), str):
            market_id_eff = str(book_spec.market_id).strip().upper() or None

        if market_id_eff is None:
            markets = MARKETS_BY_REGION.get(run.region.upper()) or ()
            market_id_eff = str(markets[0]).upper() if markets else None

        if market_id_eff:
            overlay_cfg = overlay_config_from_sleeve_spec(long_sleeve)
            mult_computed, diag = compute_fragility_overlay(
                db_manager=db_manager,
                market_id=market_id_eff,
                as_of_date=run.as_of_date,
                cfg=overlay_cfg,
            )

            fragility_mult_computed = float(mult_computed)
            fragility_mult = fragility_mult_computed if overlay_enabled else 1.0

            # Persist both computed and applied multipliers for auditability.
            fragility_meta = dict(diag)
            fragility_meta.update(
                {
                    "fragility_overlay_enabled": overlay_enabled,
                    "fragility_budget_mult_computed": fragility_mult_computed,
                    "fragility_budget_mult": float(fragility_mult),
                }
            )
    except Exception:  # pragma: no cover - defensive
        logger.exception(
            "run_books_for_run: fragility overlay computation failed for run_id=%s as_of=%s region=%s",
            run.run_id,
            run.as_of_date,
            run.region,
        )
        fragility_mult = 1.0
        fragility_meta = {"fragility_budget_error": "overlay_failed"}

    # Combine regime/meta budget with fragility overlay (default 1.0 if no meta budget)
    regime_budget_mult = budget_mult if budget_mult is not None else 1.0
    combined_budget_mult = regime_budget_mult * fragility_mult
    budget_mult = combined_budget_mult
    budget_metadata = budget_metadata or {}
    budget_metadata |= {
        "regime_budget_mult": regime_budget_mult,
        "fragility_budget_mult": fragility_mult,
        "book_id": book_id,
        "sleeve_id": getattr(long_sleeve, "sleeve_id", None),
        "meta_selected_book_id": selected_book_id,
        "meta_selected_sleeve_id": resolved_sleeve_id,
        "market_situation": situation_info.situation.value if situation_info is not None else None,
    } | fragility_meta

    # Simple, hard-coded PortfolioConfig for the core long-only equity
    # book in this iteration. This can later be sourced from
    # engine_configs.
    portfolio_config = PortfolioConfig(
        portfolio_id=book_id,
        strategies=[book_id],
        markets=list(MARKETS_BY_REGION.get(run.region.upper(), ())),
        base_currency="USD",
        risk_model_id="basic-longonly-v1",
        optimizer_type="SIMPLE_LONG_ONLY",
        risk_aversion_lambda=0.0,
        leverage_limit=1.0,
        gross_exposure_limit=1.0,
        per_instrument_max_weight=float(getattr(long_sleeve, "portfolio_per_instrument_max_weight", 0.05) or 0.05),
        max_names=getattr(long_sleeve, "portfolio_max_names", None),
        hysteresis_buffer=getattr(long_sleeve, "portfolio_hysteresis_buffer", None),
        sector_limits={},
        country_limits={},
        factor_limits={},
        fragility_exposure_limit=0.5,
        turnover_limit=0.5,
        cost_model_id="none",
        scenario_risk_scenario_set_ids=scenario_set_ids,
    )

    portfolio_storage = PortfolioStorage(db_manager=db_manager)
    base_model = BasicLongOnlyPortfolioModel(
        universe_storage=universe_storage,
        config=portfolio_config,
        universe_id=universe_id,
    )

    # Optionally wrap with conviction-based position lifecycle.
    conviction_enabled = bool(
        getattr(long_sleeve, "conviction_enabled", False)
        or portfolio_config.conviction_enabled
    )
    if conviction_enabled:
        from prometheus.portfolio.conviction import ConvictionConfig, ConvictionStorage
        from prometheus.portfolio.model_conviction import ConvictionPortfolioModel

        conviction_cfg = ConvictionConfig(
            entry_credit=portfolio_config.conviction_entry_credit,
            build_rate=portfolio_config.conviction_build_rate,
            base_decay_rate=portfolio_config.conviction_decay_rate,
            score_cap=portfolio_config.conviction_score_cap,
            sell_threshold=portfolio_config.conviction_sell_threshold,
            hard_stop_pct=portfolio_config.conviction_hard_stop_pct,
            scale_up_days=portfolio_config.conviction_scale_up_days,
            entry_weight_fraction=portfolio_config.conviction_entry_weight_fraction,
        )
        conviction_storage = ConvictionStorage(db_manager=db_manager)
        portfolio_model = ConvictionPortfolioModel(
            inner_model=base_model,
            conviction_config=conviction_cfg,
            conviction_storage=conviction_storage,
            portfolio_id=book_id,
        )
        logger.info(
            "run_books_for_run: conviction enabled for %s (decay=%.1f, stop=%.0f%%)",
            book_id, conviction_cfg.base_decay_rate,
            conviction_cfg.hard_stop_pct * 100,
        )
    else:
        portfolio_model = base_model

    portfolio_engine = PortfolioEngine(
        model=portfolio_model,
        storage=portfolio_storage,
        region=run.region,
    )

    target = portfolio_engine.optimize_and_save(
        book_id,
        run.as_of_date,
        budget_mult=budget_mult,
        budget_metadata=budget_metadata,
        apply_risk=bool(apply_risk),
        risk_strategy_id=book_id,
    )

    if not target.weights:
        logger.info(
            "PortfolioEngine produced empty target for %s on %s; marking BOOKS_DONE",
            book_id,
            run.as_of_date,
        )
        return update_phase(db_manager, run.run_id, RunPhase.BOOKS_DONE)

    # ------------------------------------------------------------------
    # Sector Allocator overlay: adjust weights based on sector health
    # ------------------------------------------------------------------

    sector_alloc_meta: dict[str, object] = {}
    try:
        sector_scores = _load_sector_health_for_date(db_manager, run.as_of_date)
        if sector_scores:
            # Build a minimal SectorHealthResult with just today's scores.
            shi_result = SectorHealthResult(
                scores={s: {run.as_of_date: sc} for s, sc in sector_scores.items()},
            )

            sector_mapper = SectorMapper(db_manager=db_manager)
            sector_mapper.load(as_of_date=run.as_of_date)

            sector_allocator = SectorAllocator(
                config=SectorAllocatorConfig(),
                sector_mapper=sector_mapper,
                sector_health=shi_result,
            )

            alloc_decision = sector_allocator.adjust_weights(
                weights=target.weights,
                as_of_date=run.as_of_date,
            )

            # Replace target weights with sector-adjusted weights.
            target = TargetPortfolio(
                portfolio_id=target.portfolio_id,
                as_of_date=target.as_of_date,
                weights=alloc_decision.adjusted_weights,
                expected_return=target.expected_return,
                expected_volatility=target.expected_volatility,
                risk_metrics=target.risk_metrics,
                factor_exposures=target.factor_exposures,
                constraints_status=target.constraints_status,
                metadata=target.metadata,
            )

            # Re-persist the adjusted target.
            portfolio_storage.save_target_portfolio(strategy_id=book_id, target=target)

            sector_alloc_meta = {
                "sector_stress_level": alloc_decision.stress_level.value,
                "sector_sick": alloc_decision.sick_sectors,
                "sector_weak": alloc_decision.weak_sectors,
                "sector_healthy": alloc_decision.healthy_sectors,
                "sector_equity_multiplier": alloc_decision.equity_multiplier,
                "sector_hedge_allocation": alloc_decision.hedge_allocation,
                "sector_weight_killed": alloc_decision.weight_killed,
                "sector_weight_reduced": alloc_decision.weight_reduced,
            }

            logger.info(
                "run_books_for_run: sector allocator applied for %s on %s: "
                "stress=%s sick=%d weak=%d positions=%d",
                book_id,
                run.as_of_date,
                alloc_decision.stress_level.value,
                len(alloc_decision.sick_sectors),
                len(alloc_decision.weak_sectors),
                len(alloc_decision.adjusted_weights),
            )
        else:
            logger.info(
                "run_books_for_run: no sector health scores for %s; skipping sector allocator",
                run.as_of_date,
            )
    except Exception:  # pragma: no cover - defensive
        logger.exception(
            "run_books_for_run: sector allocator failed for run_id=%s as_of=%s; "
            "proceeding with unadjusted weights",
            run.run_id,
            run.as_of_date,
        )

    # Record portfolio decision
    try:
        tracker = DecisionTracker(db_manager=db_manager)
        
        # Build risk metrics from target attributes
        risk_metrics = {}
        if hasattr(target, "risk_metrics") and target.risk_metrics:
            risk_metrics = target.risk_metrics
        if hasattr(target, "expected_return"):
            risk_metrics["expected_return"] = target.expected_return
        if hasattr(target, "expected_vol"):
            risk_metrics["expected_vol"] = target.expected_vol
        if hasattr(target, "expected_sharpe"):
            risk_metrics["expected_sharpe"] = target.expected_sharpe
        
        # Build constraints metadata
        constraints_applied = {
            "risk_aversion_lambda": portfolio_config.risk_aversion_lambda,
            "leverage_limit": portfolio_config.leverage_limit,
            "gross_exposure_limit": portfolio_config.gross_exposure_limit,
            "per_instrument_max_weight": portfolio_config.per_instrument_max_weight,
            "max_names": portfolio_config.max_names,
            "hysteresis_buffer": portfolio_config.hysteresis_buffer,
            "fragility_exposure_limit": portfolio_config.fragility_exposure_limit,
            "turnover_limit": portfolio_config.turnover_limit,
        }
        if hasattr(target, "constraints_status") and target.constraints_status:
            constraints_applied["status"] = target.constraints_status
        if hasattr(target, "factor_exposures") and target.factor_exposures:
            constraints_applied["factor_exposures"] = target.factor_exposures
        
        decision_id = tracker.record_portfolio_decision(
            strategy_id=book_id,
            market_id=markets[0] if markets else "UNKNOWN",
            as_of_date=run.as_of_date,
            portfolio_id=book_id,
            target_weights=target.weights,
            run_id=run.run_id,
            constraints_applied=constraints_applied,
            risk_metrics=risk_metrics,
        )
        logger.info(
            "Recorded portfolio decision: decision_id=%s portfolio_id=%s positions=%d",
            decision_id,
            book_id,
            len(target.weights),
        )
    except Exception:  # pragma: no cover - defensive
        logger.exception(
            "run_books_for_run: failed to record portfolio decision for run_id=%s",
            run.run_id,
        )

    # Optionally compute STAB-scenario diagnostics for the daily
    # portfolio when configured. This reuses the same helper as the
    # backtest campaign but restricts the range to the current as_of
    # date.
    if risk_cfg.stab_scenario_set_id is not None:
        try:
            backfill_portfolio_stab_scenario_metrics_for_range(
                db_manager=db_manager,
                portfolio_id=book_id,
                scenario_set_id=risk_cfg.stab_scenario_set_id,
                stab_model_id=risk_cfg.stab_joint_model_id,
                start=run.as_of_date,
                end=run.as_of_date,
                limit=None,
            )
        except Exception:  # pragma: no cover - defensive
            logger.exception(
                "run_books_for_run: STAB-scenario backfill failed for portfolio_id=%s as_of=%s",
                book_id,
                run.as_of_date,
            )

    return update_phase(db_manager, run.run_id, RunPhase.BOOKS_DONE)


# ======================================================================
# EXECUTION phase
# ======================================================================


@dataclass
class ExecutionConfig:
    """Configuration for the EXECUTION phase.

    Attributes:
        mode: Execution mode — "dry_run" (log only), "paper" (IBKR paper
            account), or "live" (IBKR live account).
        max_orders: Safety limit — abort if the planner produces more
            orders than this.
        max_single_order_value: Safety limit — skip any single order
            whose notional value exceeds this (USD).
        fill_timeout_sec: Per-order timeout for fill polling.
        portfolio_id: Which portfolio's target weights to execute.
            Defaults to ``{region}_CORE_LONG_EQ``.
    """

    mode: str = "dry_run"           # "dry_run" | "paper" | "live"
    max_orders: int = 50
    max_single_order_value: float = 100_000.0
    fill_timeout_sec: int = 120
    portfolio_id: str | None = None  # auto-derived from region if None


def run_execution_for_run(
    db_manager: DatabaseManager,
    run: EngineRun,
    *,
    execution_config: ExecutionConfig | None = None,
) -> EngineRun:
    """Execute target weights against IBKR (or dry-run).

    This function:
    1. Loads today's target_portfolios weights from DB.
    2. Connects to IBKR (paper by default) to read current positions.
    3. Converts target weights → target share quantities.
    4. Plans orders (delta between current and target).
    5. Submits orders (unless dry_run mode).
    6. Waits for fills with a per-order timeout.
    7. Records execution results (orders, fills) to executed_actions.
    8. Reconciles post-execution positions vs. targets.

    The function is safe by default:
    - ``dry_run`` mode only logs planned orders.
    - Max order count and max single-order value safety checks.
    - All orders are MARKET type.
    """
    if execution_config is None:
        execution_config = ExecutionConfig()

    mode = execution_config.mode
    portfolio_id = execution_config.portfolio_id or f"{run.region.upper()}_CORE_LONG_EQ"

    logger.info(
        "run_execution_for_run: run_id=%s as_of=%s portfolio=%s mode=%s",
        run.run_id,
        run.as_of_date,
        portfolio_id,
        mode,
    )

    # ------------------------------------------------------------------
    # 1. Load target weights from target_portfolios
    # ------------------------------------------------------------------

    target_weights = _load_target_weights(db_manager, portfolio_id, run.as_of_date)
    if not target_weights:
        logger.warning(
            "run_execution_for_run: no target weights for portfolio=%s as_of=%s; skipping",
            portfolio_id,
            run.as_of_date,
        )
        return update_phase(db_manager, run.run_id, RunPhase.EXECUTION_DONE)

    logger.info(
        "run_execution_for_run: loaded %d target positions for %s",
        len(target_weights),
        portfolio_id,
    )

    # ------------------------------------------------------------------
    # 2. Connect to IBKR and get current positions + account state
    # ------------------------------------------------------------------

    from prometheus.execution.broker_interface import Position as BrokerPosition, OrderType
    from prometheus.execution.order_planner import plan_orders
    from prometheus.execution.executed_actions import (
        record_executed_actions_for_fills,
        ExecutedActionContext,
    )

    if mode == "dry_run":
        # Synthesise empty positions for dry-run (no broker connection).
        current_positions: Dict[str, BrokerPosition] = {}
        account_equity = 1_000_000.0  # notional equity for dry-run
        prices: Dict[str, float] = _load_latest_prices(
            db_manager, list(target_weights.keys()), run.as_of_date,
        )
    else:
        from prometheus.execution.ibkr_config import (
            IbkrMode,
            create_connection_config,
            IbkrGatewayType,
        )
        from prometheus.execution.ibkr_client_impl import IbkrClientImpl
        from prometheus.execution.live_broker import LiveBroker

        ibkr_mode = IbkrMode.PAPER if mode == "paper" else IbkrMode.LIVE
        if ibkr_mode == IbkrMode.LIVE:
            logger.warning(
                "run_execution_for_run: LIVE mode enabled for run_id=%s — real money orders!",
                run.run_id,
            )

        conn_config = create_connection_config(
            mode=ibkr_mode,
            gateway_type=IbkrGatewayType.GATEWAY,
            client_id=10,  # dedicated client_id for execution
        )
        client = IbkrClientImpl(config=conn_config)
        broker = LiveBroker(account_id=conn_config.account_id, client=client)

        try:
            client.connect()
        except Exception:
            logger.exception(
                "run_execution_for_run: IBKR connection failed for run_id=%s mode=%s",
                run.run_id,
                mode,
            )
            return update_phase(db_manager, run.run_id, RunPhase.EXECUTION_DONE)

        try:
            current_positions = broker.get_positions()
            account_state = broker.get_account_state()
            account_equity = float(
                account_state.get("NetLiquidation")
                or account_state.get("TotalCashValue")
                or 100_000.0
            )
            prices = _load_latest_prices(
                db_manager, list(target_weights.keys()), run.as_of_date,
            )
        except Exception:
            logger.exception(
                "run_execution_for_run: failed to read IBKR state for run_id=%s",
                run.run_id,
            )
            try:
                client.disconnect()
            except Exception:
                pass
            return update_phase(db_manager, run.run_id, RunPhase.EXECUTION_DONE)

    # ------------------------------------------------------------------
    # 3. Convert target weights → target share quantities
    # ------------------------------------------------------------------

    target_quantities: Dict[str, float] = {}
    for inst_id, weight in target_weights.items():
        price = prices.get(inst_id, 0.0)
        if price > 0:
            target_quantities[inst_id] = round((account_equity * weight) / price)
        else:
            logger.warning(
                "run_execution_for_run: no price for %s; skipping",
                inst_id,
            )

    # ------------------------------------------------------------------
    # 4. Plan orders (with turnover filter, sells-first, limit orders)
    # ------------------------------------------------------------------

    # Use LIMIT orders with a 10 bps buffer for live/paper to reduce
    # market impact.  Dry-run keeps MARKET for simplicity.
    use_limit = mode in ("paper", "live")

    orders = plan_orders(
        current_positions=current_positions,
        target_positions=target_quantities,
        order_type=OrderType.LIMIT if use_limit else OrderType.MARKET,
        prices=prices if use_limit else None,
        sells_first=True,
    )

    logger.info(
        "run_execution_for_run: planned %d orders (equity=%.0f, positions=%d→%d)",
        len(orders),
        account_equity,
        len(current_positions),
        len(target_quantities),
    )

    # Safety check: max order count.
    if len(orders) > execution_config.max_orders:
        logger.error(
            "run_execution_for_run: ABORTING — %d orders exceeds max_orders=%d",
            len(orders),
            execution_config.max_orders,
        )
        return update_phase(db_manager, run.run_id, RunPhase.EXECUTION_DONE)

    # Safety check: max single-order value.
    for order in orders:
        price = prices.get(order.instrument_id, 0.0)
        notional = abs(order.quantity * price)
        if notional > execution_config.max_single_order_value:
            logger.error(
                "run_execution_for_run: ABORTING — order %s notional $%.0f > max $%.0f",
                order.instrument_id,
                notional,
                execution_config.max_single_order_value,
            )
            return update_phase(db_manager, run.run_id, RunPhase.EXECUTION_DONE)

    if not orders:
        logger.info("run_execution_for_run: no orders needed — portfolio is on target")
        return update_phase(db_manager, run.run_id, RunPhase.EXECUTION_DONE)

    # ------------------------------------------------------------------
    # 5 & 6. Submit orders and wait for fills (skip in dry_run)
    # ------------------------------------------------------------------

    if mode == "dry_run":
        for order in orders:
            logger.info(
                "run_execution_for_run [DRY_RUN]: %s %s qty=%.0f %s",
                order.side.value,
                order.instrument_id,
                order.quantity,
                order.order_type.value,
            )
        return update_phase(db_manager, run.run_id, RunPhase.EXECUTION_DONE)

    # Live / paper execution
    import time as _time
    from prometheus.execution.broker_interface import OrderStatus

    submitted_ids: List[str] = []
    for order in orders:
        try:
            broker.submit_order(order)
            submitted_ids.append(order.order_id)
            logger.info(
                "run_execution_for_run: submitted %s %s qty=%.0f",
                order.side.value,
                order.instrument_id,
                order.quantity,
            )
        except Exception:
            logger.exception(
                "run_execution_for_run: failed to submit order for %s",
                order.instrument_id,
            )

    # Poll for fills with timeout.
    deadline = _time.monotonic() + execution_config.fill_timeout_sec
    pending = set(submitted_ids)
    while pending and _time.monotonic() < deadline:
        _time.sleep(2.0)
        for oid in list(pending):
            try:
                status = broker.get_order_status(oid)
                if status in (
                    OrderStatus.FILLED,
                    OrderStatus.CANCELLED,
                    OrderStatus.REJECTED,
                ):
                    pending.discard(oid)
            except Exception:
                pass

    if pending:
        logger.warning(
            "run_execution_for_run: %d orders still pending after timeout",
            len(pending),
        )

    # ------------------------------------------------------------------
    # 7. Record fills to executed_actions
    # ------------------------------------------------------------------

    try:
        fills = broker.get_fills()
        record_executed_actions_for_fills(
            db_manager,
            fills=fills,
            context=ExecutedActionContext(
                run_id=run.run_id,
                portfolio_id=portfolio_id,
                mode=mode.upper(),
            ),
        )
        logger.info(
            "run_execution_for_run: recorded %d fills for run_id=%s",
            len(fills),
            run.run_id,
        )
    except Exception:  # pragma: no cover - defensive
        logger.exception(
            "run_execution_for_run: failed to record fills for run_id=%s",
            run.run_id,
        )

    # ------------------------------------------------------------------
    # 8. Reconcile: log any discrepancies between IBKR and targets
    # ------------------------------------------------------------------

    try:
        broker.sync()
        post_positions = broker.get_positions()
        mismatches = 0
        for inst_id, target_qty in target_quantities.items():
            actual = post_positions.get(inst_id)
            actual_qty = float(actual.quantity) if actual else 0.0
            if abs(actual_qty - target_qty) > 1.0:
                mismatches += 1
                logger.warning(
                    "run_execution_for_run: MISMATCH %s target=%.0f actual=%.0f",
                    inst_id,
                    target_qty,
                    actual_qty,
                )
        if mismatches == 0:
            logger.info("run_execution_for_run: reconciliation OK — all positions match")
        else:
            logger.warning(
                "run_execution_for_run: %d position mismatches detected",
                mismatches,
            )
    except Exception:  # pragma: no cover - defensive
        logger.exception(
            "run_execution_for_run: reconciliation failed for run_id=%s",
            run.run_id,
        )

    # Disconnect
    try:
        client.disconnect()
    except Exception:
        pass

    return update_phase(db_manager, run.run_id, RunPhase.EXECUTION_DONE)


# ======================================================================
# OPTIONS phase
# ======================================================================


# Map regime engine labels → strategy allocator market situations.
_REGIME_TO_SITUATION = {
    "CRISIS": "CRISIS",
    "RISK_OFF": "RISK_OFF",
    "CARRY": "RISK_ON",
    "NEUTRAL": "NEUTRAL",
}


@dataclass
class OptionsExecutionConfig:
    """Configuration for the OPTIONS phase.

    Attributes:
        mode: Execution mode — ``"dry_run"`` (log only), ``"paper"``
            (IBKR paper), or ``"live"`` (IBKR live).
        derivatives_budget_pct: Fraction of account equity allocated to
            derivatives (from v12 grid search).
        max_orders: Safety limit — abort if too many orders produced.
        max_position_count: Cap on simultaneously open option positions.
        strategy_overrides_path: Path to v12 strategy override JSON.
        account_equity_override: Override account equity for dry_run
            (defaults to $1M).
    """

    mode: str = "dry_run"
    derivatives_budget_pct: float = 0.15
    max_orders: int = 30
    max_position_count: int = 100
    strategy_overrides_path: str | None = None
    account_equity_override: float = 1_000_000.0


def _load_strategy_overrides(path: str | None = None) -> Dict[str, Dict]:
    """Load v12 strategy config overrides from JSON."""
    import json

    if path is None:
        cfg_path = PROJECT_ROOT / "configs" / "v12_strategy_overrides.json"
    else:
        cfg_path = Path(path)
    if not cfg_path.exists():
        return {}
    try:
        return json.loads(cfg_path.read_text())
    except Exception:  # pragma: no cover - defensive
        logger.warning("Failed to load strategy overrides from %s", cfg_path)
        return {}


def build_options_signals(
    db_manager: DatabaseManager,
    as_of_date: date,
    run: EngineRun,
    *,
    account_equity: float = 1_000_000.0,
    derivatives_budget_pct: float = 0.15,
) -> Dict[str, Any]:
    """Build the signals dict consumed by options strategies.

    Uses real pipeline data (regime, fragility, sector health, STAB,
    prices) instead of the backtest's synthetic proxies.
    """
    from datetime import timedelta

    reader = DataReader(db_manager=db_manager)
    markets = MARKETS_BY_REGION.get(run.region.upper())
    market_id = markets[0] if markets else run.region.upper()

    # ── Regime / market situation ────────────────────────────────────
    regime_label = "NEUTRAL"
    try:
        regime_storage = RegimeStorage(db_manager=db_manager)
        regime_state = regime_storage.get_latest_regime(run.region.upper(), as_of_date=as_of_date)
        if regime_state is not None:
            regime_label = regime_state.regime_label.value
    except Exception:
        logger.warning("build_options_signals: regime lookup failed; defaulting to NEUTRAL", exc_info=True)

    market_situation = _REGIME_TO_SITUATION.get(regime_label, "NEUTRAL")

    # ── Fragility ────────────────────────────────────────────────────
    frag = 0.20
    try:
        frag_storage = FragilityStorage(db_manager=db_manager)
        measure = frag_storage.get_latest_measure("MARKET", market_id, as_of_date=as_of_date)
        if measure is not None:
            frag = float(measure.fragility_score)
    except Exception:
        logger.warning("build_options_signals: fragility lookup failed; defaulting to 0.20", exc_info=True)

    mhi = round(1.0 - frag, 3)

    # ── VIX ──────────────────────────────────────────────────────────
    vix = 20.0
    vix3m = None
    try:
        vix_prices = _load_latest_prices_historical(db_manager, ["VIX.INDX"], as_of_date)
        if "VIX.INDX" in vix_prices:
            vix = vix_prices["VIX.INDX"]
        vix3m_prices = _load_latest_prices_historical(db_manager, ["VIX3M.INDX"], as_of_date)
        if "VIX3M.INDX" in vix3m_prices:
            vix3m = vix3m_prices["VIX3M.INDX"]
    except Exception:
        logger.warning("build_options_signals: VIX lookup failed; defaulting to 20.0", exc_info=True)

    vix_contango = max(0.0, (20.0 - vix) / 100.0)
    if vix3m is not None and vix > 0:
        vix_contango = round((vix3m - vix) / vix, 4)

    # ── SPY price + momentum ─────────────────────────────────────────
    spy_price = 0.0
    spy_momentum_63d = 0.0
    try:
        spy_prices = _load_latest_prices_historical(db_manager, ["SPY.US"], as_of_date)
        spy_price = spy_prices.get("SPY.US", 0.0)
        if spy_price > 0:
            past_date = as_of_date - timedelta(days=90)
            spy_past = _load_latest_prices_historical(db_manager, ["SPY.US"], past_date)
            past_price = spy_past.get("SPY.US", 0.0)
            if past_price > 0:
                spy_momentum_63d = round(spy_price / past_price - 1.0, 4)
    except Exception:
        logger.warning("build_options_signals: SPY lookup failed", exc_info=True)

    # ── Sector health ────────────────────────────────────────────────
    sector_shi = _load_sector_health_for_date(db_manager, as_of_date)

    # ── STAB scores (latest soft_target_class → stability proxy) ─────
    stab_scores: Dict[str, float] = {}
    try:
        stab_map = {"STABLE": 0.90, "TARGETABLE": 0.65, "WATCH": 0.40, "FRAGILE": 0.15}
        with db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """SELECT DISTINCT ON (entity_id) entity_id, soft_target_class
                       FROM soft_target_classes
                       WHERE entity_type = 'INSTRUMENT'
                         AND as_of_date <= %s AND as_of_date >= %s
                       ORDER BY entity_id, as_of_date DESC""",
                    (as_of_date, as_of_date - timedelta(days=14)),
                )
                for row in cursor.fetchall():
                    inst_id = str(row[0]).replace(".US", "")
                    stab_scores[inst_id] = stab_map.get(str(row[1]), 0.50)
            finally:
                cursor.close()
    except Exception:
        logger.warning("build_options_signals: STAB lookup failed", exc_info=True)

    # ── Lambda scores (from lambda predictions CSV) ──────────────────
    lambda_scores: Dict[str, float] = {}
    try:
        lambda_cfg = _load_daily_universe_lambda_config(run.region.upper())
        if lambda_cfg.predictions_csv:
            provider = CsvLambdaClusterScoreProvider(
                csv_path=Path(lambda_cfg.predictions_csv),
                experiment_id=lambda_cfg.experiment_id or "US_EQ_GL_POLY2_V0",
            )
            # Iterate all cluster keys and collect scores for the date
            for (mid, sec, stc) in provider._date_index:
                score = provider.get_cluster_score(
                    as_of_date=as_of_date,
                    market_id=mid,
                    sector=sec,
                    soft_target_class=stc,
                )
                if score is not None:
                    lambda_scores[f"{sec}_{stc}"] = round(float(score), 3)
    except Exception:
        logger.warning("build_options_signals: lambda lookup failed", exc_info=True)

    lambda_agg = round(
        sum(lambda_scores.values()) / max(len(lambda_scores), 1), 3,
    ) if lambda_scores else 0.5

    # ── Equity prices for universe members ───────────────────────────
    equity_prices: Dict[str, float] = {}
    try:
        sector_etfs = [
            "XLK.US", "XLF.US", "XLE.US", "XLV.US", "XLI.US",
            "XLP.US", "XLY.US", "XLU.US", "XLB.US", "SPY.US",
        ]
        etf_prices = _load_latest_prices_historical(db_manager, sector_etfs, as_of_date)
        for inst_id, price in etf_prices.items():
            symbol = inst_id.replace(".US", "")
            equity_prices[symbol] = price
    except Exception:
        logger.warning("build_options_signals: equity prices lookup failed", exc_info=True)

    # Also ensure VIX is in equity_prices for vix_tail_hedge strategy
    equity_prices["VIX"] = vix

    # ── Log signal snapshot for debugging ──────────────────────────────
    logger.info(
        "build_options_signals: regime=%s frag=%.3f mhi=%.3f vix=%.2f "
        "spy=$%.2f momentum=%.4f vix_contango=%.4f lambda_n=%d "
        "stab_n=%d sectors=%d etf_n=%d",
        regime_label, frag, mhi, vix,
        spy_price, spy_momentum_63d, vix_contango, len(lambda_scores),
        len(stab_scores), len(sector_shi), len(equity_prices),
    )

    # ── Build signals dict (matching backtest format) ────────────────
    return {
        "as_of_date": as_of_date,
        "nav": account_equity,
        "buying_power": account_equity * derivatives_budget_pct,
        "market_state": market_situation,
        "mhi": mhi,
        "frag": frag,
        "vix_level": vix,
        "spy_price": spy_price,
        "spy_momentum_63d": spy_momentum_63d,
        "es_price": spy_price * 10.0 if spy_price > 0 else 0.0,
        "lambda_scores": lambda_scores,
        "lambda_aggregate": lambda_agg,
        "stab_scores": stab_scores,
        "sector_shi": sector_shi,
        "sector_exposures": {
            name: account_equity / max(len(sector_shi), 1)
            for name in sector_shi
        },
        "vix_contango": vix_contango,
        "etf_prices": {
            sym: equity_prices.get(sym, 0.0)
            for sym in ["XLK", "XLF", "XLE", "XLV", "XLI", "XLP", "XLY", "XLU", "XLB"]
        },
        "equity_prices": equity_prices,
        "futures_positions": {},
    }


def run_options_for_run(
    db_manager: DatabaseManager,
    run: EngineRun,
    *,
    options_config: OptionsExecutionConfig | None = None,
) -> EngineRun:
    """Evaluate and execute options strategies for the run's date.

    This mirrors the backtest's daily options loop using live pipeline
    data (regime, fragility, sector health, STAB, lambda) and the v12
    tuned strategy parameters.

    Phases:
    1. Build signals from live pipeline data.
    2. Run ``StrategyAllocator`` with current market situation.
    3. Run ``PositionLifecycleManager`` on existing positions.
    4. Evaluate enabled strategies for new trade directives.
    5. Execute directives (dry_run logs only; paper/live submits to IBKR).
    """
    if options_config is None:
        options_config = OptionsExecutionConfig()

    mode = options_config.mode
    logger.info(
        "run_options_for_run: run_id=%s as_of=%s mode=%s",
        run.run_id, run.as_of_date, mode,
    )

    # ------------------------------------------------------------------
    # 0. Determine account equity
    # ------------------------------------------------------------------

    account_equity = options_config.account_equity_override
    if mode != "dry_run":
        try:
            from prometheus.execution.ibkr_config import (
                IbkrMode, create_connection_config, IbkrGatewayType,
            )
            from prometheus.execution.ibkr_client_impl import IbkrClientImpl
            from prometheus.execution.live_broker import LiveBroker

            ibkr_mode = IbkrMode.PAPER if mode == "paper" else IbkrMode.LIVE
            conn_config = create_connection_config(
                mode=ibkr_mode, gateway_type=IbkrGatewayType.GATEWAY, client_id=11,
            )
            client = IbkrClientImpl(config=conn_config)
            client.connect()
            broker = LiveBroker(account_id=conn_config.account_id, client=client)
            acct = broker.get_account_state()
            account_equity = float(
                acct.get("NetLiquidation") or acct.get("TotalCashValue") or account_equity
            )
        except Exception:
            logger.exception(
                "run_options_for_run: IBKR connection failed; using equity override %.0f",
                account_equity,
            )
            return update_phase(db_manager, run.run_id, RunPhase.OPTIONS_DONE)

    # ------------------------------------------------------------------
    # 1. Build signals from live pipeline data
    # ------------------------------------------------------------------

    signals = build_options_signals(
        db_manager, run.as_of_date, run,
        account_equity=account_equity,
        derivatives_budget_pct=options_config.derivatives_budget_pct,
    )
    market_situation = signals["market_state"]

    logger.info(
        "run_options_for_run: signals built — situation=%s vix=%.1f frag=%.2f "
        "spy=$%.0f stab=%d lambda=%d sectors=%d",
        market_situation,
        signals["vix_level"],
        signals["frag"],
        signals["spy_price"],
        len(signals["stab_scores"]),
        len(signals["lambda_scores"]),
        len(signals["sector_shi"]),
    )

    # ------------------------------------------------------------------
    # 2. Strategy allocator — which strategies run and how much capital
    # ------------------------------------------------------------------

    from prometheus.execution.strategy_allocator import StrategyAllocator

    allocator = StrategyAllocator()
    allocations = allocator.allocate(
        market_situation=market_situation,
        signals=signals,
    )
    enabled = [name for name, alloc in allocations.items() if alloc.enabled]
    disabled = [name for name, alloc in allocations.items() if not alloc.enabled]
    logger.info(
        "run_options_for_run: allocator → %d enabled: %s",
        len(enabled), ", ".join(sorted(enabled)),
    )
    if disabled:
        logger.info(
            "run_options_for_run: allocator → %d disabled: %s",
            len(disabled), ", ".join(sorted(disabled)),
        )

    # ------------------------------------------------------------------
    # 3. Lifecycle manager — check existing option positions
    # ------------------------------------------------------------------

    from prometheus.execution.position_lifecycle import PositionLifecycleManager

    lifecycle = PositionLifecycleManager()
    existing_positions: list = []

    if mode != "dry_run":
        try:
            from prometheus.execution.options_portfolio import OptionsPortfolio
            opt_portfolio = OptionsPortfolio(ib=client)
            opt_portfolio.sync()
            existing_positions = [
                pos.to_dict() for pos in opt_portfolio.get_all_positions()
            ]
        except Exception:
            logger.warning("run_options_for_run: could not sync option positions", exc_info=True)

    lifecycle_directives = lifecycle.evaluate(
        positions=existing_positions,
        signals=signals,
    )
    if lifecycle_directives:
        logger.info(
            "run_options_for_run: lifecycle → %d directives (roll/close/adjust)",
            len(lifecycle_directives),
        )

    # ------------------------------------------------------------------
    # 4. Evaluate enabled strategies for new trade directives
    # ------------------------------------------------------------------

    from prometheus.execution.options_strategy import (
        VixTailHedgeStrategy, IronCondorStrategy, IronButterflyStrategy,
        ShortPutStrategy, FuturesOverlayStrategy, FuturesOptionStrategy,
        BullCallSpreadStrategy, MomentumCallStrategy, LEAPSStrategy,
        WheelStrategy,
        VixTailHedgeConfig, IronCondorConfig, IronButterflyConfig,
        ShortPutConfig, FuturesOverlayConfig, FuturesOptionConfig,
        BullCallSpreadConfig, MomentumCallConfig, LEAPSConfig,
        WheelConfig,
        TradeAction,
    )

    overrides = _load_strategy_overrides(options_config.strategy_overrides_path)
    if overrides:
        logger.info(
            "run_options_for_run: applying v12 overrides for %s",
            ", ".join(sorted(overrides.keys())),
        )

    def _cfg(cls, key=None):
        name = key or cls.__name__
        return cls(**overrides[name]) if name in overrides else cls()

    strategies = [
        VixTailHedgeStrategy(config=_cfg(VixTailHedgeConfig)),
        IronCondorStrategy(config=_cfg(IronCondorConfig)),
        IronButterflyStrategy(config=_cfg(IronButterflyConfig)),
        ShortPutStrategy(config=_cfg(ShortPutConfig)),
        FuturesOverlayStrategy(config=_cfg(FuturesOverlayConfig)),
        FuturesOptionStrategy(config=_cfg(FuturesOptionConfig)),
        BullCallSpreadStrategy(config=_cfg(BullCallSpreadConfig)),
        MomentumCallStrategy(config=_cfg(MomentumCallConfig)),
        LEAPSStrategy(config=_cfg(LEAPSConfig)),
        WheelStrategy(config=_cfg(WheelConfig)),
    ]

    # Apply allocator enable/disable to each strategy.
    for strat in strategies:
        strat_name = strat.name
        if strat_name in allocations:
            alloc = allocations[strat_name]
            if hasattr(strat, "_config") and hasattr(strat._config, "enabled"):
                strat._config.enabled = alloc.enabled
        elif hasattr(strat, "_config") and hasattr(strat._config, "enabled"):
            if strat_name != "vix_tail_hedge":
                strat._config.enabled = False

    # Build minimal portfolio dict for strategy evaluate().
    portfolio: Dict[str, Any] = {}
    for symbol, price in (signals.get("equity_prices", {}) or {}).items():
        if price > 0:
            portfolio[f"{symbol}.US"] = type("Pos", (), {
                "quantity": int(account_equity * 0.02 / max(price, 1)),
                "market_value": account_equity * 0.02,
                "avg_cost": price,
            })()

    existing_options_dicts = existing_positions
    new_directives = []
    for strategy in strategies:
        try:
            directives = strategy.evaluate(portfolio, signals, existing_options_dicts)
            for d in directives:
                if d.action == TradeAction.OPEN:
                    new_directives.append(d)
        except Exception as exc:
            logger.warning(
                "run_options_for_run: strategy %s raised %s: %s",
                strategy.name, type(exc).__name__, exc,
                exc_info=True,
            )

    all_directives = lifecycle_directives + new_directives

    logger.info(
        "run_options_for_run: %d total directives (%d lifecycle + %d new)",
        len(all_directives), len(lifecycle_directives), len(new_directives),
    )

    # Safety check.
    if len(all_directives) > options_config.max_orders:
        logger.error(
            "run_options_for_run: ABORTING — %d directives exceeds max_orders=%d",
            len(all_directives), options_config.max_orders,
        )
        return update_phase(db_manager, run.run_id, RunPhase.OPTIONS_DONE)

    # ------------------------------------------------------------------
    # 5. Execute directives
    # ------------------------------------------------------------------

    if mode == "dry_run":
        for d in all_directives:
            logger.info(
                "run_options_for_run [DRY_RUN]: %s %s %s strike=%.1f qty=%d (%s) — %s",
                d.action.value,
                d.symbol,
                d.right,
                d.strike,
                d.quantity,
                d.strategy,
                d.reason or "",
            )
    else:
        # Live/paper execution via contract discovery + IBKR.
        try:
            from prometheus.execution.contract_discovery import ContractDiscoveryService

            discovery = ContractDiscoveryService(client)
            for d in all_directives:
                try:
                    # Resolve contract via discovery.
                    chain = discovery.discover_option_chain(d.symbol)
                    if chain is None:
                        logger.warning(
                            "run_options_for_run: no chain for %s; skipping", d.symbol,
                        )
                        continue
                    logger.info(
                        "run_options_for_run [%s]: %s %s %s strike=%.1f qty=%d (%s)",
                        mode.upper(),
                        d.action.value, d.symbol, d.right,
                        d.strike, d.quantity, d.strategy,
                    )
                    # TODO: submit order via broker once paper-tested
                except Exception as exc:
                    logger.warning(
                        "run_options_for_run: failed to execute %s %s: %s",
                        d.action.value, d.symbol, exc,
                    )
        except Exception:
            logger.exception("run_options_for_run: options execution failed")
        finally:
            try:
                client.disconnect()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # 6. Record options decision
    # ------------------------------------------------------------------

    try:
        tracker = DecisionTracker(db_manager=db_manager)
        tracker.record_options_decision(
            strategy_id=f"{run.region.upper()}_OPTIONS_OVERLAY",
            market_id=markets[0] if markets else "UNKNOWN",
            as_of_date=run.as_of_date,
            run_id=run.run_id,
            market_situation=market_situation,
            directives_count=len(all_directives),
            enabled_strategies=enabled,
            mode=mode,
        )
    except Exception:  # pragma: no cover - non-fatal
        logger.warning(
            "run_options_for_run: decision recording failed",
            exc_info=True,
        )

    return update_phase(db_manager, run.run_id, RunPhase.OPTIONS_DONE)


def _load_target_weights(
    db_manager: DatabaseManager,
    portfolio_id: str,
    as_of_date: date,
) -> Dict[str, float]:
    """Load target weights from target_portfolios for a portfolio/date.

    The ``target_positions`` column stores a JSONB payload with structure
    ``{"weights": {instrument_id: weight, ...}}``.
    """
    sql = """
        SELECT target_positions
        FROM target_portfolios
        WHERE portfolio_id = %s AND as_of_date = %s
        ORDER BY created_at DESC
        LIMIT 1
    """

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (portfolio_id, as_of_date))
            row = cursor.fetchone()
        finally:
            cursor.close()

    if not row or not row[0]:
        return {}

    raw = row[0]
    if isinstance(raw, str):
        import json
        raw = json.loads(raw)

    # Extract weights dict from the JSONB payload.
    weights = raw.get("weights", raw) if isinstance(raw, dict) else {}
    return {str(k): float(v) for k, v in weights.items() if v}


def _load_latest_prices(
    db_manager: DatabaseManager,
    instrument_ids: List[str],
    as_of_date: date,
) -> Dict[str, float]:
    """Load latest close prices for instruments on or before as_of_date."""
    if not instrument_ids:
        return {}

    sql = """
        SELECT DISTINCT ON (instrument_id)
            instrument_id, close
        FROM prices_daily
        WHERE instrument_id = ANY(%s)
          AND trade_date <= %s
          AND close > 0
        ORDER BY instrument_id, trade_date DESC
    """

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (instrument_ids, as_of_date))
            rows = cursor.fetchall()
        finally:
            cursor.close()

    return {str(row[0]): float(row[1]) for row in rows}


def _load_latest_prices_historical(
    db_manager: DatabaseManager,
    instrument_ids: List[str],
    as_of_date: date,
) -> Dict[str, float]:
    """Load latest close prices from the *historical* database.

    Identical to :func:`_load_latest_prices` but queries the historical
    connection where market data (SPY, VIX, sector ETFs) is stored.
    """
    if not instrument_ids:
        return {}

    sql = """
        SELECT DISTINCT ON (instrument_id)
            instrument_id, close
        FROM prices_daily
        WHERE instrument_id = ANY(%s)
          AND trade_date <= %s
          AND close > 0
        ORDER BY instrument_id, trade_date DESC
    """

    with db_manager.get_historical_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (instrument_ids, as_of_date))
            rows = cursor.fetchall()
        finally:
            cursor.close()

    return {str(row[0]): float(row[1]) for row in rows}


def run_meta_for_strategy(
    db_manager: DatabaseManager,
    strategy_id: str,
    as_of_date: date,
    top_k: int = 3,
) -> str | None:
    """Run Meta-Orchestrator for a strategy and record a decision.

    This helper reads all backtest runs for ``strategy_id`` via
    :class:`MetaOrchestrator`, selects the top-k sleeves based on
    backtest metrics, and inserts a single row into ``engine_decisions``
    capturing the selection.

    Args:
        db_manager: Database manager for the runtime database.
        strategy_id: Logical strategy identifier whose sleeves should be
            evaluated (e.g. "US_CORE_LONG_EQ").
        as_of_date: Date on which the meta decision is being recorded.
        top_k: Number of top sleeves to select.

    Returns:
        The generated ``decision_id`` if a decision was recorded, or
        ``None`` if no sleeves were available for the strategy.
    """

    storage = MetaStorage(db_manager=db_manager)
    orchestrator = MetaOrchestrator(storage=storage)

    evaluations = orchestrator.select_top_sleeves(strategy_id, k=top_k)
    if not evaluations:
        logger.info(
            "run_meta_for_strategy: no evaluated sleeves for strategy_id=%s; skipping decision",
            strategy_id,
        )
        return None

    decision_id = generate_uuid()

    # Derive a market_id from the first selected sleeve; if unavailable,
    # leave as None.
    first_cfg = evaluations[0].sleeve_config
    market_id = getattr(first_cfg, "market_id", None)

    input_refs = {
        "strategy_id": strategy_id,
        "top_k": top_k,
        "candidate_runs": [
            {"run_id": ev.run_id, "sleeve_id": ev.sleeve_config.sleeve_id}
            for ev in evaluations
        ],
    }

    output_refs = {
        "selected_sleeves": [
            {
                "run_id": ev.run_id,
                "sleeve_id": ev.sleeve_config.sleeve_id,
                "metrics": ev.metrics,
            }
            for ev in evaluations
        ],
    }

    decision = EngineDecision(
        decision_id=decision_id,
        engine_name="META_ORCHESTRATOR",
        run_id=None,
        strategy_id=strategy_id,
        market_id=market_id,
        as_of_date=as_of_date,
        config_id=None,
        input_refs=input_refs,
        output_refs=output_refs,
        metadata={"type": "sleeve_selection"},
    )

    storage.save_engine_decision(decision)

    logger.info(
        "run_meta_for_strategy: recorded decision_id=%s for strategy_id=%s top_k=%d",
        decision_id,
        strategy_id,
        top_k,
    )

    return decision_id


def run_backtest_campaign_and_meta_for_strategy(
    db_manager: DatabaseManager,
    strategy_id: str,
    market_id: str,
    start_date: date,
    end_date: date,
    top_k: int = 3,
    initial_cash: float = 1_000_000.0,
    *,
    apply_risk: bool = True,
    assessment_backend: str = "basic",
    assessment_use_joint_context: bool = False,
    assessment_context_model_id: str = "joint-assessment-context-v1",
    assessment_model_id: str | None = None,
    stability_risk_alpha: float | None = None,
    stability_risk_horizon_steps: int | None = None,
    regime_risk_alpha: float | None = None,
    lambda_predictions_csv: str | None = None,
    lambda_experiment_id: str | None = None,
    lambda_score_weight: float | None = None,
    scenario_risk_set_id: str | None = None,
    stab_scenario_set_id: str | None = None,
    stab_joint_model_id: str = "joint-stab-fragility-v1",
) -> tuple[list[SleeveRunSummary], str | None]:
    """Run a sleeve backtest campaign and Meta-Orchestrator for a strategy.

    This helper is a convenience for performing a full offline
    config-space sweep for a single logical strategy:

    1. Construct a small grid of core long-only sleeves for the given
       ``strategy_id`` and ``market_id``.
    2. Run a backtest campaign over ``[start_date, end_date]`` using the
       basic STAB/Assessment/Universe/Portfolio sleeve pipeline.
    3. Invoke :func:`run_meta_for_strategy` to record a Meta-Orchestrator
       decision selecting the top-k sleeves by backtest metrics.

    Returns the list of :class:`SleeveRunSummary` objects produced by the
    campaign together with the ``decision_id`` recorded by the
    Meta-Orchestrator (or ``None`` if no decision was written).
    """

    if end_date < start_date:
        raise ValueError("end_date must be >= start_date")

    # If the caller did not provide explicit lambda/scenario/STAB
    # configuration, default to the same settings used by the daily
    # UNIVERSES/BOOKS pipeline for the inferred region (when available).
    region = infer_region_from_market_id(market_id)

    risk_cfg_defaults: DailyPortfolioRiskConfig | None = None


    if region is not None:
        # Lambda defaults from daily universe config.
        if lambda_predictions_csv is None or lambda_score_weight is None:
            lambda_cfg = _load_daily_universe_lambda_config(region)
            if lambda_predictions_csv is None and lambda_cfg.predictions_csv is not None:
                lambda_predictions_csv = lambda_cfg.predictions_csv
            if lambda_experiment_id is None and lambda_cfg.experiment_id is not None:
                lambda_experiment_id = lambda_cfg.experiment_id
            if (
                lambda_score_weight is None
                and lambda_cfg.score_weight is not None
                and lambda_cfg.score_weight != 0.0
            ):
                lambda_score_weight = float(lambda_cfg.score_weight)

        # Scenario and STAB-scenario defaults from daily portfolio config.
        risk_cfg = _load_daily_portfolio_risk_config(region)
        risk_cfg_defaults = risk_cfg
        if scenario_risk_set_id is None:
            scenario_risk_set_id = risk_cfg.scenario_risk_set_id
        if stab_scenario_set_id is None:
            stab_scenario_set_id = risk_cfg.stab_scenario_set_id
        # If the caller did not override the default STAB joint model,
        # align it with the daily config as well.
        if stab_joint_model_id == "joint-stab-fragility-v1" and risk_cfg.stab_joint_model_id:
            stab_joint_model_id = risk_cfg.stab_joint_model_id


    calendar = TradingCalendar()
    sleeve_configs = build_core_long_sleeves(strategy_id=strategy_id, market_id=market_id)
    if not sleeve_configs:
        logger.info(
            "run_backtest_campaign_and_meta_for_strategy: no sleeve configs for strategy_id=%s market_id=%s",
            strategy_id,
            market_id,
        )
        return [], None

    # Apply assessment configuration to each sleeve in the campaign.
    for cfg in sleeve_configs:
        cfg.assessment_backend = assessment_backend
        cfg.assessment_use_joint_context = assessment_use_joint_context
        cfg.assessment_context_model_id = assessment_context_model_id
        if assessment_model_id is not None:
            cfg.assessment_model_id = assessment_model_id
        # Optional STAB/regime/scenario configuration for the sleeve
        if stability_risk_alpha is not None:
            cfg.stability_risk_alpha = stability_risk_alpha
        if stability_risk_horizon_steps is not None:
            cfg.stability_risk_horizon_steps = stability_risk_horizon_steps
        if regime_risk_alpha is not None:
            cfg.regime_risk_alpha = regime_risk_alpha
        if scenario_risk_set_id is not None:
            cfg.scenario_risk_set_id = scenario_risk_set_id
        if lambda_score_weight is not None:
            cfg.lambda_score_weight = lambda_score_weight

        # Meta budget defaults from daily config (if available).
        if risk_cfg_defaults is not None:
            cfg.meta_budget_enabled = bool(risk_cfg_defaults.meta_budget_enabled)
            cfg.meta_budget_alpha = float(risk_cfg_defaults.meta_budget_alpha)
            cfg.meta_budget_min = float(risk_cfg_defaults.meta_budget_min)
            cfg.meta_budget_horizon_steps = int(risk_cfg_defaults.meta_budget_horizon_steps)
            cfg.meta_budget_region = risk_cfg_defaults.meta_budget_region
            cfg.hazard_profile = risk_cfg_defaults.hazard_profile

    lambda_provider = None
    if lambda_predictions_csv is not None:
        preds_path = Path(lambda_predictions_csv)
        try:
            lambda_provider = CsvLambdaClusterScoreProvider(
                csv_path=preds_path,
                experiment_id=lambda_experiment_id,
                score_column="lambda_hat",
            )
        except Exception as exc:  # pragma: no cover - defensive
            # For backtest campaigns, failure to initialise a lambda
            # provider (e.g. missing experiment_id rows) should not abort
            # the entire campaign; we log a concise warning and proceed
            # without lambda integration.
            logger.warning(
                "run_backtest_campaign_and_meta_for_strategy: disabling lambda integration from %s "
                "due to error: %s",
                preds_path,
                exc,
            )
            lambda_provider = None

    summaries = run_backtest_campaign(
        db_manager=db_manager,
        calendar=calendar,
        market_id=market_id,
        start_date=start_date,
        end_date=end_date,
        sleeve_configs=sleeve_configs,
        initial_cash=initial_cash,
        apply_risk=apply_risk,
        lambda_provider=lambda_provider,
    )

    # Optionally enrich portfolio_risk_reports and backtest_runs with
    # STAB-scenario diagnostics when a scenario set is provided. We only
    # attempt this when a real DatabaseManager instance is in use so that
    # pure wiring tests can pass in lightweight stand-ins.
    if (
        stab_scenario_set_id is not None
        and summaries
        and isinstance(db_manager, DatabaseManager)
    ):
        # Backfill portfolio-level STAB-scenario metrics for each
        # portfolio used in the campaign over the campaign window.
        portfolio_ids = sorted({cfg.portfolio_id for cfg in sleeve_configs})
        for portfolio_id in portfolio_ids:
            backfill_portfolio_stab_scenario_metrics_for_range(
                db_manager=db_manager,
                portfolio_id=portfolio_id,
                scenario_set_id=stab_scenario_set_id,
                stab_model_id=stab_joint_model_id,
                start=start_date,
                end=end_date,
                limit=None,
            )

        # Summarise those STAB-scenario metrics into backtest_runs.metrics_json
        # for each run we just created.
        for summary in summaries:
            summarise_backtest_stab_scenario_metrics(
                db_manager=db_manager,
                strategy_id=None,
                run_id=summary.run_id,
            )

    decision_id = run_meta_for_strategy(
        db_manager=db_manager,
        strategy_id=strategy_id,
        as_of_date=end_date,
        top_k=top_k,
    )

    logger.info(
        "run_backtest_campaign_and_meta_for_strategy: strategy_id=%s market_id=%s runs=%d decision_id=%s",
        strategy_id,
        market_id,
        len(summaries),
        decision_id,
    )

    return summaries, decision_id


def advance_run(db_manager: DatabaseManager, run: EngineRun) -> EngineRun:
    """Advance a run by one phase, executing the appropriate task.

    This function does **not** loop; it performs at most one phase
    transition. Callers (e.g. CLI tools or daemons) can repeatedly call
    :func:`advance_run` until the run reaches COMPLETED/FAILED.
    """

    if run.phase == RunPhase.WAITING_FOR_DATA:
        # External ingestion should flip to DATA_READY once EOD data is
        # available. We treat a call in WAITING_FOR_DATA as a no-op.
        logger.info("advance_run: run %s still WAITING_FOR_DATA", run.run_id)
        return run

    if run.phase == RunPhase.DATA_READY:
        return run_signals_for_run(db_manager, run)

    if run.phase == RunPhase.SIGNALS_DONE:
        return run_universes_for_run(db_manager, run)

    if run.phase == RunPhase.UNIVERSES_DONE:
        run_after_books = run_books_for_run(db_manager, run)
        # Finalise to COMPLETED in a separate transition for clarity.
        if run_after_books.phase == RunPhase.BOOKS_DONE:
            return update_phase(db_manager, run_after_books.run_id, RunPhase.COMPLETED)
        return run_after_books

    if run.phase in {RunPhase.BOOKS_DONE, RunPhase.COMPLETED, RunPhase.FAILED}:
        # Nothing to do; caller can decide whether to drop or inspect.
        logger.info(
            "advance_run: run %s in terminal or post-book phase %s",
            run.run_id,
            run.phase.value,
        )
        return run

    # Defensive default; should not be hit.
    logger.warning("advance_run: run %s in unexpected phase %s", run.run_id, run.phase.value)
    return run
