"""Prometheus v2 – Basic sleeve pipeline for backtesting.

This module provides a thin orchestration layer that wires together the
STAB, Assessment, Universe, and Portfolio & Risk engines in order to
produce target *positions* (instrument quantities) for a single sleeve.

It is intentionally simple and focused on Iteration 1 backtests:

* Uses the existing BasicPriceStabilityModel, BasicAssessmentModel,
  BasicUniverseModel, and BasicLongOnlyPortfolioModel.
* Operates at end-of-day frequency; intraday behaviour is not modelled.
* Converts portfolio weights into share quantities using the simulated
  account equity from :class:`BacktestBroker` and close prices at the
  current ``as_of_date``.

Higher-level orchestration (multiple sleeves, Meta-Orchestrator, regime-
conditioned budgets) can be layered on top of this building block.
"""

from __future__ import annotations

import os
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List

from apathis.core.database import DatabaseManager
from apathis.core.logging import get_logger
from apathis.core.time import TradingCalendar
from apathis.data.reader import DataReader
from apathis.fragility.overlay import FragilityOverlayStepper, overlay_config_from_sleeve_spec
from apathis.fragility.storage import FragilityStorage
from apathis.regime import MarketProxyRegimeModel, RegimeEngine, RegimeStorage
from apathis.regime.state_change import RegimeStateChangeForecaster
from apathis.sector.health import SectorHealthResult, compute_sector_health
from apathis.sector.mapper import SectorMapper
from apathis.stability import (
    BasicPriceStabilityModel,
    StabilityEngine,
    StabilityStateChangeForecaster,
    StabilityStorage,
)

from prometheus.assessment import AssessmentEngine
from prometheus.assessment.model_basic import BasicAssessmentModel
from prometheus.assessment.model_context import ContextAssessmentModel
from prometheus.assessment.storage import InstrumentScoreStorage
from prometheus.backtest.config import SleeveConfig
from prometheus.backtest.runner import TargetPositionsFn
from prometheus.decisions import DecisionTracker
from prometheus.execution.backtest_broker import BacktestBroker
from prometheus.execution.time_machine import TimeMachine
from prometheus.portfolio import (
    BasicLongOnlyPortfolioModel,
    PortfolioConfig,
    PortfolioEngine,
    PortfolioStorage,
)
from prometheus.portfolio.types import TargetPortfolio
from prometheus.sector.allocator import (
    SectorAllocator,
    SectorAllocatorConfig,
)
from prometheus.universe import (
    BasicUniverseModel,
    UniverseEngine,
    UniverseStorage,
)

logger = get_logger(__name__)


def _fragility_budget_multiplier(score: float) -> float:
    """Step-function overlay using market fragility.

    Mirrors the daily pipeline overlay used by defensive sleeves:
    - <0.3     -> 1.0 (full exposure)
    - 0.3-0.5  -> 0.5 (half exposure)
    - >=0.5    -> 0.0 (go to cash)
    """
    if score < 0.3:
        return 1.0
    if score < 0.5:
        return 0.5
    return 0.0


@dataclass
class BasicSleevePipeline:
    """Wire STAB, Assessment, Universe, and Portfolio for a single sleeve.

    The pipeline is configured for a particular :class:`SleeveConfig` and a
    backtest environment consisting of a :class:`DatabaseManager`,
    :class:`TradingCalendar`, and :class:`BacktestBroker`. It exposes a
    small method :meth:`target_positions_for_date` suitable for use as the
    ``target_positions_fn`` argument to :class:`BacktestRunner`.

    The ``apply_risk`` flag controls whether the Risk Management Service is
    invoked to post-process portfolio weights before converting them into
    target positions. This allows risk-on vs risk-off backtests using the
    same sleeve pipeline.
    """

    db_manager: DatabaseManager
    calendar: TradingCalendar
    config: SleeveConfig
    broker: BacktestBroker

    # Core shared infrastructure
    data_reader: DataReader
    time_machine: TimeMachine

    # Engines
    stab_engine: StabilityEngine
    assessment_engine: AssessmentEngine
    universe_engine: UniverseEngine
    portfolio_engine: PortfolioEngine

    # Cached instrument universe for the sleeve's markets. This is purely
    # an optimisation; we recompute STAB/Assessment/Universe per date.
    instrument_ids: List[str]

    # Optional decision tracking (engine_decisions) for explainable backtests.
    decision_tracker: DecisionTracker | None = None

    # Optional BacktestRunner context for joining per-date decisions.
    backtest_run_id: str | None = None
    backtest_decision_id: str | None = None

    # Whether to apply the Risk Management Service (per-name caps, etc.)
    # when turning portfolio weights into target positions.
    apply_risk: bool = True

    # Maximum number of worker threads to use for per-instrument scoring
    # (STAB/Assessment). A value of 1 preserves the original
    # single-threaded behaviour; higher values allow us to better utilise
    # multi-core CPUs when scoring large universes.
    num_workers: int = 8

    # Cached market fragility overlay stepper (optional). This allows us to
    # evaluate EMA+hysteresis overlays efficiently across many dates.
    _fragility_overlay_stepper: FragilityOverlayStepper | None = field(
        default=None,
        init=False,
        repr=False,
    )

    # Pre-computed sector health scores for the full backtest range.
    # Populated by _build_engines_for_sleeve() when apply_sector_allocator
    # is enabled.  Avoids re-loading sector ETF prices on every date.
    _sector_health_result: SectorHealthResult | None = field(
        default=None,
        init=False,
        repr=False,
    )
    _sector_mapper: SectorMapper | None = field(
        default=None,
        init=False,
        repr=False,
    )

    def target_positions_for_date(self, as_of_date: date) -> Dict[str, float]:
        """Return target share quantities for the sleeve on ``as_of_date``.

        The sequence of operations mirrors the design in
        ``backtesting_and_books_pipeline.md`` for a single sleeve:

        1. Ensure STAB soft-target states exist for all candidate
           instruments.
        2. Run Assessment to score those instruments and persist scores
           into ``instrument_scores``.
        3. Build a universe via :class:`UniverseEngine` and
           :class:`BasicUniverseModel`.
        4. Run :class:`PortfolioEngine` with a long-only model to obtain
           a :class:`TargetPortfolio` of weights.
        5. Convert weights into share quantities using the current
           account equity and close prices at ``as_of_date``.
        """

        if not self.instrument_ids:
            return {}

        # Filter to instruments that have a close price on as_of_date.
        # This removes stale/delisted constituents that cannot be traded
        # on the simulated day.
        prices_today = self.data_reader.read_prices_close(
            self.instrument_ids,
            as_of_date,
            as_of_date,
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

        instrument_ids_today = [
            inst_id for inst_id in self.instrument_ids if inst_id in tradable_today
        ]
        if not instrument_ids_today:
            return {}

        # ------------------------------------------------------------------
        # 0) Regime – ensure a region-level regime state exists for as_of_date.
        # ------------------------------------------------------------------

        try:
            region_for_regime = str(self.config.market_id).split("_")[0].upper()
            regime_storage = RegimeStorage(db_manager=self.db_manager)
            regime_model = MarketProxyRegimeModel(
                db_manager=self.db_manager,
                profile_name=getattr(self.config, "hazard_profile", None),
            )
            regime_engine = RegimeEngine(model=regime_model, storage=regime_storage)
            regime_engine.get_regime(as_of_date=as_of_date, region=region_for_regime)
        except Exception:  # pragma: no cover - defensive
            logger.exception(
                "BasicSleevePipeline: regime detection failed for sleeve=%s on %s",
                self.config.sleeve_id,
                as_of_date,
            )

        # 1) STAB – ensure stability / soft-target state exists for each
        # instrument using only history up to as_of_date.
        #
        # We prefer using precomputed rows from ``soft_target_classes``
        # (historical backfill) and only compute missing states.
        try:
            have_state = self.stab_engine.storage.get_entities_with_soft_target_state(
                entity_type="INSTRUMENT",
                entity_ids=instrument_ids_today,
                as_of_date=as_of_date,
            )
        except Exception:  # pragma: no cover - defensive
            logger.exception(
                "BasicSleevePipeline: failed to check STAB coverage for %s; computing for all tradable instruments",
                as_of_date,
            )
            have_state = set()

        missing_stab_ids = [inst_id for inst_id in instrument_ids_today if inst_id not in have_state]

        def _score_stab(inst_id: str) -> None:
            try:
                self.stab_engine.score_entity(as_of_date, "INSTRUMENT", inst_id)
            except ValueError:
                # Instruments with insufficient history are excluded later
                # by the Universe model via a "no_stab_state" reason.
                return

        if missing_stab_ids:
            if self.num_workers > 1 and len(missing_stab_ids) > 1:
                with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
                    list(executor.map(_score_stab, missing_stab_ids))
            else:
                for inst_id in missing_stab_ids:
                    _score_stab(inst_id)

        # 2) Assessment – score instruments for the sleeve's assessment
        # strategy and horizon. Scores are persisted into
        # ``instrument_scores`` via InstrumentScoreStorage.
        scores = {}
        try:
            scores = self.assessment_engine.score_universe(
                strategy_id=self.config.assessment_strategy_id,
                market_id=self.config.market_id,
                instrument_ids=instrument_ids_today,
                as_of_date=as_of_date,
                horizon_days=self.config.assessment_horizon_days,
            )
        except Exception:  # pragma: no cover - defensive
            logger.exception(
                "BasicSleevePipeline: AssessmentEngine.score_universe failed for %s on %s",
                self.config.assessment_strategy_id,
                as_of_date,
            )
            scores = {}

        assessment_decision_id: str | None = None
        if self.decision_tracker is not None and scores:
            try:
                instrument_scores = {
                    str(inst_id): float(score.score) for inst_id, score in scores.items()
                }
                assessment_decision_id = self.decision_tracker.record_assessment_decision(
                    strategy_id=self.config.assessment_strategy_id,
                    market_id=self.config.market_id,
                    as_of_date=as_of_date,
                    universe_id=self.config.universe_id,
                    instrument_scores=instrument_scores,
                    model_id=str(self.assessment_engine.model_id),
                    horizon_days=int(self.config.assessment_horizon_days),
                    run_id=self.backtest_run_id,
                    config_id=self.config.sleeve_id,
                    reasoning={
                        "mode": "BACKTEST",
                        "backend": getattr(self.config, "assessment_backend", "basic"),
                        "use_joint_context": bool(
                            getattr(self.config, "assessment_use_joint_context", False)
                        ),
                        "assessment_context_model_id": getattr(
                            self.config,
                            "assessment_context_model_id",
                            None,
                        ),
                        "candidate_total": int(len(self.instrument_ids)),
                        "tradable_today": int(len(instrument_ids_today)),
                        "filtered_no_price_today": int(
                            len(self.instrument_ids) - len(instrument_ids_today)
                        ),
                        "insufficient_history_count": int(
                            sum(
                                1
                                for s in scores.values()
                                if isinstance(getattr(s, "metadata", None), dict)
                                and (s.metadata or {}).get("insufficient_history") is True
                            )
                        ),
                        "run_id": self.backtest_run_id,
                        "decision_id": self.backtest_decision_id,
                    },
                )
            except Exception:  # pragma: no cover - defensive
                logger.exception(
                    "BasicSleevePipeline: failed to record ASSESSMENT decision for sleeve=%s on %s",
                    self.config.sleeve_id,
                    as_of_date,
                )

        # 3) Universe – build a sleeve-specific universe and persist
        # decisions into ``universe_members``.
        members = self.universe_engine.build_and_save(as_of_date, self.config.universe_id)
        included = [m for m in members if m.included]
        excluded = [m for m in members if not m.included]

        if self.decision_tracker is not None:
            try:
                included_ids = [m.entity_id for m in included]
                excluded_ids = [m.entity_id for m in excluded]

                excluded_reason_counts: dict[str, int] = {}
                counter = Counter()
                for m in excluded:
                    for k, v in (getattr(m, "reasons", None) or {}).items():
                        if v is True:
                            counter[str(k)] += 1
                excluded_reason_counts = {k: int(v) for k, v in counter.items()}

                excluded_with_scores: dict[str, float] = {}
                if scores:
                    for inst_id in excluded_ids:
                        s = scores.get(inst_id)
                        if s is not None:
                            excluded_with_scores[str(inst_id)] = float(s.score)
                    # Keep only the highest-scoring excluded names (helps spot
                    # "good" names filtered out downstream).
                    excluded_with_scores = dict(
                        sorted(excluded_with_scores.items(), key=lambda kv: kv[1], reverse=True)[:100]
                    )

                self.decision_tracker.record_universe_decision(
                    strategy_id=self.config.strategy_id,
                    market_id=self.config.market_id,
                    as_of_date=as_of_date,
                    universe_id=self.config.universe_id,
                    included_instruments=included_ids,
                    excluded_instruments=excluded_ids,
                    run_id=self.backtest_run_id,
                    config_id=self.config.sleeve_id,
                    inclusion_reasons={
                        "mode": "BACKTEST",
                        "run_id": self.backtest_run_id,
                        "decision_id": self.backtest_decision_id,
                        "assessment_decision_id": assessment_decision_id,
                        "assessment_strategy_id": self.config.assessment_strategy_id,
                        "assessment_horizon_days": int(self.config.assessment_horizon_days),
                        "lambda_score_weight": float(getattr(self.config, "lambda_score_weight", 0.0)),
                        "stability_risk_alpha": float(getattr(self.config, "stability_risk_alpha", 0.0)),
                        "regime_risk_alpha": float(getattr(self.config, "regime_risk_alpha", 0.0)),
                    },
                    exclusion_reasons={
                        "excluded_reason_counts": excluded_reason_counts,
                        "excluded_with_scores_top": excluded_with_scores,
                    },
                )
            except Exception:  # pragma: no cover - defensive
                logger.exception(
                    "BasicSleevePipeline: failed to record UNIVERSE decision for sleeve=%s on %s",
                    self.config.sleeve_id,
                    as_of_date,
                )
        if not included:
            logger.info(
                "BasicSleevePipeline: no included universe members for sleeve=%s on %s",
                self.config.sleeve_id,
                as_of_date,
            )
            return {}

        # 4) Portfolio – construct a long-only target portfolio from the
        # universe and persist into book_targets and target_portfolios
        # tables.

        # Optional Meta budget allocation: compute a budget multiplier from
        # regime state-change risk and apply it when persisting targets.
        budget_mult: float | None = None
        budget_metadata: dict[str, object] | None = None

        if bool(getattr(self.config, "meta_budget_enabled", False)):
            region_for_budget = (
                str(getattr(self.config, "meta_budget_region", "") or "")
                or str(self.config.market_id).split("_")[0]
            ).upper()
            horizon = int(getattr(self.config, "meta_budget_horizon_steps", 21) or 21)
            alpha = float(getattr(self.config, "meta_budget_alpha", 1.0) or 1.0)
            m_min = float(getattr(self.config, "meta_budget_min", 0.35) or 0.35)

            if horizon <= 0:
                horizon = 1
            if m_min < 0.0:
                m_min = 0.0
            if m_min > 1.0:
                m_min = 1.0

            try:
                # Reuse the sleeve's regime storage (runtime DB) to avoid
                # any look-ahead; we anchor to as_of_date.
                regime_storage = RegimeStorage(db_manager=self.db_manager)
                forecaster = RegimeStateChangeForecaster(storage=regime_storage)
                risk = forecaster.forecast(
                    region=region_for_budget,
                    horizon_steps=horizon,
                    as_of_date=as_of_date,
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
                    "mode": "BACKTEST",
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
            except Exception:  # pragma: no cover - defensive
                logger.exception(
                    "BasicSleevePipeline: meta budget forecast failed for sleeve=%s on %s",
                    self.config.sleeve_id,
                    as_of_date,
                )
                budget_mult = 1.0
                budget_metadata = {
                    "mode": "BACKTEST",
                    "region": region_for_budget,
                    "horizon_steps": horizon,
                    "alpha": alpha,
                    "min_budget": m_min,
                    "error": "forecast_failed",
                }

        # Optional fragility overlay: config-driven exposure scaler driven
        # by market fragility.
        if bool(getattr(self.config, "apply_fragility_overlay", False)):
            fragility_score: float | None = None
            fragility_class: str | None = None
            fragility_as_of: str | None = None
            try:
                frag_storage = FragilityStorage(db_manager=self.db_manager)
                measure = frag_storage.get_latest_measure(
                    "MARKET",
                    self.config.market_id,
                    as_of_date=as_of_date,
                    inclusive=True,
                )
                if measure is not None:
                    fragility_score = float(getattr(measure, "fragility_score", 0.0) or 0.0)

                    cls = getattr(measure, "class_label", None)
                    if cls is not None:
                        fragility_class = str(getattr(cls, "value", cls))

                    asof = getattr(measure, "as_of_date", None)
                    if isinstance(asof, date):
                        fragility_as_of = asof.isoformat()
                    elif asof is not None:
                        fragility_as_of = str(asof)
            except Exception:  # pragma: no cover - defensive
                logger.exception(
                    "BasicSleevePipeline: fragility overlay lookup failed for sleeve=%s on %s",
                    self.config.sleeve_id,
                    as_of_date,
                )

            frag_mult = 1.0
            overlay_diag: dict[str, object] = {}
            if fragility_score is not None:
                if self._fragility_overlay_stepper is None:
                    overlay_cfg = overlay_config_from_sleeve_spec(self.config)
                    self._fragility_overlay_stepper = FragilityOverlayStepper(cfg=overlay_cfg)
                frag_mult, overlay_diag = self._fragility_overlay_stepper.update(float(fragility_score))

            # Treat missing meta_budget as 1.0 so the overlay can still
            # reduce exposure even when meta_budget_enabled is False.
            regime_mult = float(budget_mult) if budget_mult is not None else 1.0
            combined = float(regime_mult) * float(frag_mult)

            budget_mult = float(combined)
            if budget_metadata is None:
                budget_metadata = {"mode": "BACKTEST"}

            budget_metadata.update(
                {
                    "fragility_overlay_enabled": True,
                    "fragility_score": fragility_score,
                    "fragility_class": fragility_class,
                    "fragility_as_of": fragility_as_of,
                    "fragility_budget_mult": float(frag_mult),
                    "budget_mult_before_fragility": float(regime_mult),
                    "budget_mult_after_fragility": float(combined),
                }
                | overlay_diag
            )

        target = self.portfolio_engine.optimize_and_save(
            self.config.portfolio_id,
            as_of_date,
            budget_mult=budget_mult,
            budget_metadata=budget_metadata,
            apply_risk=bool(self.apply_risk),
            risk_strategy_id=self.config.strategy_id,
        )

        if self.decision_tracker is not None:
            try:
                model = self.portfolio_engine.model
                cfg = getattr(model, "config", None)
                constraints_applied: dict[str, object] = {
                    "mode": "BACKTEST",
                    "apply_risk": bool(self.apply_risk),
                    "universe_id": self.config.universe_id,
                }
                if cfg is not None:
                    constraints_applied.update(
                        {
                            "risk_aversion_lambda": float(getattr(cfg, "risk_aversion_lambda", 0.0)),
                            "leverage_limit": float(getattr(cfg, "leverage_limit", 0.0)),
                            "gross_exposure_limit": float(getattr(cfg, "gross_exposure_limit", 0.0)),
                            "per_instrument_max_weight": float(
                                getattr(cfg, "per_instrument_max_weight", 0.0)
                            ),
                            "portfolio_max_names": int(getattr(cfg, "max_names", 0) or 0),
                            "portfolio_hysteresis_buffer": int(getattr(cfg, "hysteresis_buffer", 0) or 0),
                            "fragility_exposure_limit": float(
                                getattr(cfg, "fragility_exposure_limit", 0.0)
                            ),
                            "turnover_limit": float(getattr(cfg, "turnover_limit", 0.0)),
                        }
                    )

                risk_metrics = {
                    str(k): float(v)
                    for k, v in (getattr(target, "risk_metrics", None) or {}).items()
                    if isinstance(v, (int, float))
                }

                self.decision_tracker.record_portfolio_decision(
                    strategy_id=self.config.strategy_id,
                    market_id=self.config.market_id,
                    as_of_date=as_of_date,
                    portfolio_id=self.config.portfolio_id,
                    target_weights={
                        str(inst_id): float(w) for inst_id, w in (target.weights or {}).items()
                    },
                    assessment_decision_id=assessment_decision_id,
                    constraints_applied=constraints_applied,
                    risk_metrics=risk_metrics,
                    run_id=self.backtest_run_id,
                    config_id=self.config.sleeve_id,
                )
            except Exception:  # pragma: no cover - defensive
                logger.exception(
                    "BasicSleevePipeline: failed to record PORTFOLIO decision for sleeve=%s on %s",
                    self.config.sleeve_id,
                    as_of_date,
                )

        if not target.weights:
            logger.info(
                "BasicSleevePipeline: empty TargetPortfolio for sleeve=%s on %s",
                self.config.sleeve_id,
                as_of_date,
            )
            return {}

        # ------------------------------------------------------------------
        # Sector Allocator overlay (mirrors tasks.py run_books_for_run)
        # ------------------------------------------------------------------
        if (
            bool(getattr(self.config, "apply_sector_allocator", False))
            and self._sector_health_result is not None
            and self._sector_mapper is not None
        ):
            try:
                # Build a single-date SectorHealthResult for the allocator.
                day_scores: dict[str, dict[date, float]] = {}
                for sector_name, score_dict in self._sector_health_result.scores.items():
                    score = score_dict.get(as_of_date)
                    if score is not None:
                        day_scores[sector_name] = {as_of_date: score}

                if day_scores:
                    shi_result = SectorHealthResult(scores=day_scores)
                    alloc_cfg = SectorAllocatorConfig(
                        sector_kill_threshold=float(
                            getattr(self.config, "sector_allocator_kill_threshold", 0.25)
                        ),
                        sector_reduce_threshold=float(
                            getattr(self.config, "sector_allocator_reduce_threshold", 0.40)
                        ),
                    )
                    allocator = SectorAllocator(
                        config=alloc_cfg,
                        sector_mapper=self._sector_mapper,
                        sector_health=shi_result,
                    )
                    alloc_decision = allocator.adjust_weights(
                        weights=target.weights,
                        as_of_date=as_of_date,
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

                    # Re-persist adjusted target (matching live pipeline).
                    self.portfolio_engine._storage.save_target_portfolio(
                        strategy_id=self.config.strategy_id,
                        target=target,
                    )

                    logger.info(
                        "BasicSleevePipeline: sector allocator applied for sleeve=%s on %s: "
                        "stress=%s sick=%d weak=%d positions=%d",
                        self.config.sleeve_id,
                        as_of_date,
                        alloc_decision.stress_level.value,
                        len(alloc_decision.sick_sectors),
                        len(alloc_decision.weak_sectors),
                        len(alloc_decision.adjusted_weights),
                    )
            except Exception:  # pragma: no cover - defensive
                logger.exception(
                    "BasicSleevePipeline: sector allocator failed for sleeve=%s on %s; "
                    "proceeding with unadjusted weights",
                    self.config.sleeve_id,
                    as_of_date,
                )

        # Risk constraints are applied inside PortfolioEngine.optimize_and_save
        # when self.apply_risk is True, so we can directly consume target weights.
        weights_for_positions: Dict[str, float] = {
            str(inst_id): float(weight) for inst_id, weight in target.weights.items()
        }

        # 5) Convert weights into share quantities based on current equity
        # and close prices for as_of_date. We use the BacktestBroker's
        # account state for equity so that the portfolio naturally scales
        # with P&L.
        account_state = self.broker.get_account_state()
        equity = float(account_state.get("equity", 0.0))
        if equity <= 0.0:
            logger.warning(
                "BasicSleevePipeline: non-positive equity %.4f for sleeve=%s on %s; returning zero targets",
                equity,
                self.config.sleeve_id,
                as_of_date,
            )
            return {}

        instrument_ids = list(weights_for_positions.keys())
        prices_df = self.data_reader.read_prices(instrument_ids, as_of_date, as_of_date)
        if prices_df.empty:
            logger.warning(
                "BasicSleevePipeline: no prices for target instruments on %s; returning zero targets",
                as_of_date,
            )
            return {}

        price_map: Dict[str, float] = {}
        for _, row in prices_df.iterrows():
            price_map[str(row["instrument_id"])] = float(row["close"])

        target_positions: Dict[str, float] = {}
        for instrument_id, weight in weights_for_positions.items():
            px = price_map.get(instrument_id)
            if px is None or px <= 0.0:
                continue
            qty = float(weight) * equity / px
            if qty != 0.0:
                target_positions[instrument_id] = qty

        return target_positions

    def exposure_metrics_for_date(self, as_of_date: date) -> Dict[str, float]:
        """Aggregate lambda/state-aware diagnostics for the sleeve on a date.

        This reads from the already-persisted ``universe_members`` for
        ``(universe_id, as_of_date)`` and computes simple cross-sectional
        averages that can be attached to ``backtest_daily_equity``
        ``exposure_metrics_json``. It is intentionally lightweight and
        purely diagnostic; failures return an empty dict.
        """

        try:
            members = self.universe_engine.get_universe(
                as_of_date,
                self.config.universe_id,
                included_only=True,
            )
        except Exception:  # pragma: no cover - defensive
            logger.exception(
                "BasicSleevePipeline: failed to load universe for exposures sleeve=%s on %s",
                self.config.sleeve_id,
                as_of_date,
            )
            return {}

        if not members:
            return {}

        n = float(len(members))
        metrics: Dict[str, float] = {"universe_size": n}

        def _mean_from_reasons(key: str) -> float | None:
            vals: List[float] = []
            for m in members:
                reasons = getattr(m, "reasons", None) or {}
                val = reasons.get(key)
                if isinstance(val, (int, float)):
                    vals.append(float(val))
            if not vals:
                return None
            return float(sum(vals) / len(vals))

        def _coverage_from_reasons(key: str) -> float | None:
            have = 0
            for m in members:
                reasons = getattr(m, "reasons", None) or {}
                val = reasons.get(key)
                if isinstance(val, (int, float)):
                    have += 1
            if have == 0:
                return None
            return float(have) / n

        # Lambda / opportunity-density exposure for the included universe.
        lambda_mean = _mean_from_reasons("lambda_score")
        if lambda_mean is not None:
            metrics["lambda_score_mean"] = lambda_mean
            cov = _coverage_from_reasons("lambda_score")
            if cov is not None:
                metrics["lambda_score_coverage"] = cov

        # STAB state-change risk exposure.
        stab_risk_mean = _mean_from_reasons("stab_risk_score")
        if stab_risk_mean is not None:
            metrics["stab_risk_score_mean"] = stab_risk_mean

        stab_p_worsen_mean = _mean_from_reasons("stab_p_worsen_any")
        if stab_p_worsen_mean is not None:
            metrics["stab_p_worsen_any_mean"] = stab_p_worsen_mean

        # Regime state-change risk is global rather than per-instrument;
        # query the universe model's forecaster once.
        model = self.universe_engine.model
        regime_forecaster = getattr(model, "regime_forecaster", None)
        if regime_forecaster is not None:
            region = getattr(model, "regime_region", "GLOBAL")
            horizon = getattr(model, "regime_risk_horizon_steps", 1)
            try:
                risk = regime_forecaster.forecast(
                    region=region,
                    horizon_steps=horizon,
                    as_of_date=as_of_date,
                )
                risk_score = getattr(risk, "risk_score", None)
                if isinstance(risk_score, (int, float)):
                    metrics["regime_risk_score"] = float(risk_score)
                p_change_any = getattr(risk, "p_change_any", None)
                if isinstance(p_change_any, (int, float)):
                    metrics["regime_p_change_any"] = float(p_change_any)
            except Exception:  # pragma: no cover - defensive
                logger.exception(
                    "BasicSleevePipeline: regime_forecaster.forecast failed for sleeve=%s on %s",
                    self.config.sleeve_id,
                    as_of_date,
                )

        # Regime detector diagnostics (hazard inputs + confidence) from the
        # stored regime state for this date.
        try:
            regime_region = getattr(model, "regime_region", None)
            if not regime_region:
                regime_region = str(self.config.market_id).split("_")[0]
            regime_region = str(regime_region).upper()

            regime_storage = RegimeStorage(db_manager=self.db_manager)
            st = regime_storage.get_latest_regime(
                region=regime_region,
                as_of_date=as_of_date,
                inclusive=True,
            )
            if st is not None:
                conf = getattr(st, "confidence", None)
                if isinstance(conf, (int, float)):
                    metrics["regime_confidence"] = float(conf)

                meta = getattr(st, "metadata", None) or {}
                dr = meta.get("down_risk")
                if isinstance(dr, (int, float)):
                    metrics["down_risk"] = float(dr)
                ur = meta.get("up_risk")
                if isinstance(ur, (int, float)):
                    metrics["up_risk"] = float(ur)
        except Exception:  # pragma: no cover - defensive
            logger.exception(
                "BasicSleevePipeline: failed to load stored regime diagnostics for sleeve=%s on %s",
                self.config.sleeve_id,
                as_of_date,
            )

        return metrics


def _build_engines_for_sleeve(
    db_manager: DatabaseManager,
    calendar: TradingCalendar,
    config: SleeveConfig,
    broker: BacktestBroker,
    *,
    apply_risk: bool = True,
    lambda_provider: object | None = None,
) -> BasicSleevePipeline:
    """Construct a :class:`BasicSleevePipeline` for the given sleeve.

    This helper initialises the shared DataReader and all dependent
    engines using conservative default hyperparameters.
    """

    # Determine the desired level of parallelism for per-instrument
    # scoring from the BACKTEST_NUM_WORKERS environment variable. A value
    # of 1 preserves the original single-threaded behaviour.
    raw_workers = os.getenv("BACKTEST_NUM_WORKERS")
    try:
        num_workers = int(raw_workers) if raw_workers else 1
    except (TypeError, ValueError):
        logger.warning(
            "Invalid BACKTEST_NUM_WORKERS=%r; falling back to single-threaded execution",
            raw_workers,
        )
        num_workers = 1
    if num_workers < 1:
        num_workers = 1

    # Cap worker count based on database connection pool sizes so that we
    # do not exhaust the psycopg2 SimpleConnectionPool when scoring in
    # parallel. Both STAB and Assessment use the same DatabaseManager for
    # historical and runtime DB access, so we take the minimum of the two
    # pool sizes as a conservative upper bound.
    try:
        hist_pool_size = db_manager.config.historical_db.pool_size
        runtime_pool_size = db_manager.config.runtime_db.pool_size
    except Exception:  # pragma: no cover - defensive
        logger.exception(
            "BasicSleevePipeline: failed to read DB pool sizes from config; "
            "falling back to BACKTEST_NUM_WORKERS=%d",
            num_workers,
        )
        # Fall back to treating the current requested worker count as the
        # effective pool size for both DBs so that we do not accidentally
        # reference uninitialised variables below.
        hist_pool_size = num_workers
        runtime_pool_size = num_workers

    max_db_workers = max(1, min(hist_pool_size, runtime_pool_size))

    if num_workers > max_db_workers:
        logger.warning(
            "Capping BACKTEST_NUM_WORKERS from %d to %d based on DB pool sizes (hist=%d, runtime=%d)",
            num_workers,
            max_db_workers,
            hist_pool_size,
            runtime_pool_size,
        )
        num_workers = max_db_workers

    data_reader = DataReader(db_manager=db_manager)

    # STAB infrastructure
    stab_storage = StabilityStorage(db_manager=db_manager)
    stab_model = BasicPriceStabilityModel(
        data_reader=data_reader,
        calendar=calendar,
        window_days=63,
    )
    stab_engine = StabilityEngine(model=stab_model, storage=stab_storage)
    stab_forecaster = StabilityStateChangeForecaster(storage=stab_storage)

    # Regime state-change forecaster for region-level regime risk in the
    # sleeve's universe model.
    regime_storage = RegimeStorage(db_manager=db_manager)
    regime_forecaster = RegimeStateChangeForecaster(storage=regime_storage)

    # Assessment infrastructure – backend is configurable via SleeveConfig.
    assessment_storage = InstrumentScoreStorage(db_manager=db_manager)

    backend = getattr(config, "assessment_backend", "basic")
    use_joint_ctx = getattr(config, "assessment_use_joint_context", False)
    ctx_model_id = getattr(
        config,
        "assessment_context_model_id",
        "joint-assessment-context-v1",
    )
    assessment_model_id = getattr(config, "assessment_model_id", None)
    if assessment_model_id is None:
        # Choose a sensible default based on backend.
        if backend == "basic":
            assessment_model_id = "assessment-basic-v1"
        elif backend == "context":
            assessment_model_id = "assessment-context-v1"
        else:
            assessment_model_id = backend

    if backend == "basic":
        assessment_model = BasicAssessmentModel(
            data_reader=data_reader,
            calendar=calendar,
            stability_storage=stab_storage,
            db_manager=db_manager,
            use_assessment_context=use_joint_ctx,
            assessment_context_model_id=ctx_model_id,
            max_workers=num_workers,
        )
    elif backend == "context":
        assessment_model = ContextAssessmentModel(
            db_manager=db_manager,
            assessment_context_model_id=ctx_model_id,
        )
    else:
        raise ValueError(f"Unknown assessment_backend {backend!r} in SleeveConfig")

    assessment_engine = AssessmentEngine(
        model=assessment_model,
        storage=assessment_storage,
        model_id=assessment_model_id,
    )

    # Universe infrastructure with Assessment integration enabled.
    universe_storage = UniverseStorage(db_manager=db_manager)

    raw_max = getattr(config, "universe_max_size", None)
    try:
        max_universe_size = int(raw_max) if raw_max is not None and int(raw_max) > 0 else None
    except (TypeError, ValueError):
        max_universe_size = None

    raw_sector_cap = getattr(config, "universe_sector_max_names", None)
    try:
        sector_max_names = int(raw_sector_cap) if raw_sector_cap is not None and int(raw_sector_cap) > 0 else None
    except (TypeError, ValueError):
        sector_max_names = None

    universe_model = BasicUniverseModel(
        db_manager=db_manager,
        calendar=calendar,
        data_reader=data_reader,
        profile_service=None,  # profile-aware universes can be added later
        stability_storage=stab_storage,
        market_ids=(config.market_id,),
        min_avg_volume=100_000.0,
        max_soft_target_score=90.0,
        exclude_breakers=True,
        exclude_weak_profile_when_fragile=True,
        max_universe_size=max_universe_size,
        sector_max_names=sector_max_names,
        min_price=0.0,
        # Temporarily hard-exclude a small set of problematic names that
        # lack reliable price data around corporate actions in 2019 so that
        # the backtest harness does not generate unfillable orders for
        # them. This keeps the core sleeve behaviour clean while we defer
        # full corporate-action handling.
        hard_exclusion_list=("RHT.US", "APC.US"),
        issuer_exclusion_list=(),
        window_days=63,
        use_assessment_scores=True,
        assessment_strategy_id=config.assessment_strategy_id,
        assessment_horizon_days=config.assessment_horizon_days,
        assessment_score_weight=50.0,
        # Optional lambda opportunity integration for research/backtests.
        lambda_score_provider=lambda_provider,
        lambda_score_weight=float(getattr(config, "lambda_score_weight", 0.0)),
        lambda_score_weight_selection=getattr(config, "lambda_score_weight_selection", None),
        lambda_score_weight_portfolio=getattr(config, "lambda_score_weight_portfolio", None),
        # STAB state-change risk integration consistent with pipeline
        # universes.
        stability_state_change_forecaster=stab_forecaster,
        stability_risk_alpha=config.stability_risk_alpha,
        stability_risk_horizon_steps=config.stability_risk_horizon_steps,
        # Regime state-change risk integration. As in the pipeline, this is
        # effectively disabled unless ``config.regime_risk_alpha`` is set
        # to a non-zero value.
        regime_forecaster=regime_forecaster,
        regime_region=config.market_id.split("_")[0],
        regime_risk_alpha=config.regime_risk_alpha,
        regime_risk_horizon_steps=1,
    )
    universe_engine = UniverseEngine(model=universe_model, storage=universe_storage)

    # Portfolio & Risk infrastructure – basic long-only model.
    portfolio_storage = PortfolioStorage(db_manager=db_manager)
    per_name_cap = (
        float(config.portfolio_per_instrument_max_weight)
        if getattr(config, "portfolio_per_instrument_max_weight", None) is not None
        else 0.10
    )

    raw_buf = getattr(config, "portfolio_hysteresis_buffer", None)
    try:
        hysteresis_buffer = int(raw_buf) if raw_buf is not None and int(raw_buf) > 0 else None
    except (TypeError, ValueError):
        hysteresis_buffer = None

    portfolio_config = PortfolioConfig(
        portfolio_id=config.portfolio_id,
        strategies=[config.strategy_id],
        markets=[config.market_id],
        base_currency="USD",
        risk_model_id="basic-longonly-v1",
        optimizer_type="SIMPLE_LONG_ONLY",
        risk_aversion_lambda=1.0,
        leverage_limit=1.0,
        gross_exposure_limit=1.0,
        per_instrument_max_weight=per_name_cap,
        max_names=(
            int(config.portfolio_max_names)
            if getattr(config, "portfolio_max_names", None) is not None
            and int(config.portfolio_max_names) > 0
            else None
        ),
        hysteresis_buffer=hysteresis_buffer,
        sector_limits={},
        country_limits={},
        factor_limits={},
        fragility_exposure_limit=1.0,
        turnover_limit=1.0,
        cost_model_id="none",
        # Optional scenario-based risk; if a scenario_risk_set_id is
        # configured on the sleeve, enable inline scenario P&L for this
        # portfolio.
        scenario_risk_scenario_set_ids=[config.scenario_risk_set_id]
        if getattr(config, "scenario_risk_set_id", None)
        else [],
    )
    base_model = BasicLongOnlyPortfolioModel(
        universe_storage=universe_storage,
        config=portfolio_config,
        universe_id=config.universe_id,
        # Provide current holdings so the portfolio model can apply
        # optional hysteresis/top-K turnover control.
        held_ids_provider=lambda _d: set(broker.get_positions().keys()),
    )

    # Optionally wrap with conviction-based position lifecycle (mirrors
    # the live pipeline in tasks.py run_books_for_run).
    if bool(getattr(config, "conviction_enabled", False)):
        from prometheus.portfolio.conviction import ConvictionConfig, ConvictionStorage
        from prometheus.portfolio.model_conviction import ConvictionPortfolioModel

        conviction_cfg = ConvictionConfig(
            entry_credit=float(getattr(config, "conviction_entry_credit", 5.0)),
            build_rate=float(getattr(config, "conviction_build_rate", 1.0)),
            base_decay_rate=float(getattr(config, "conviction_decay_rate", 2.0)),
            score_cap=float(getattr(config, "conviction_score_cap", 20.0)),
            sell_threshold=float(getattr(config, "conviction_sell_threshold", 0.0)),
            hard_stop_pct=float(getattr(config, "conviction_hard_stop_pct", 0.20)),
            scale_up_days=int(getattr(config, "conviction_scale_up_days", 3)),
            entry_weight_fraction=float(getattr(config, "conviction_entry_weight_fraction", 0.50)),
        )
        conviction_storage = ConvictionStorage(db_manager=db_manager)
        portfolio_model = ConvictionPortfolioModel(
            inner_model=base_model,
            conviction_config=conviction_cfg,
            conviction_storage=conviction_storage,
            portfolio_id=config.portfolio_id,
        )
        logger.info(
            "_build_engines_for_sleeve: conviction enabled for %s (decay=%.1f, stop=%.0f%%)",
            config.sleeve_id,
            conviction_cfg.base_decay_rate,
            conviction_cfg.hard_stop_pct * 100,
        )
    else:
        portfolio_model = base_model

    portfolio_engine = PortfolioEngine(
        model=portfolio_model,
        storage=portfolio_storage,
        region="US",  # simple default region for US_EQ; can be extended later
    )

    # Determine the candidate instrument universe once based on the
    # current contents of the instruments table.
    try:
        # BasicUniverseModel._enumerate_instruments is date-aware (uses
        # as-of classifications) and returns tuples:
        # (instrument_id, issuer_id, sector, market_id, sector_source).
        instruments = universe_model._enumerate_instruments(  # type: ignore[attr-defined]
            broker.time_machine.current_date
        )
        # Cache instrument_ids for STAB/Assessment scoring across the backtest
        # horizon.
        instrument_ids = [
            inst_id
            for inst_id, _issuer_id, _sector, _market_id, _sector_source in instruments
        ]
    except Exception:  # pragma: no cover - defensive
        logger.exception(
            "BasicSleevePipeline: failed to enumerate instruments for markets=%s",
            universe_model.market_ids,
        )
        instrument_ids = []

    pipeline = BasicSleevePipeline(
        db_manager=db_manager,
        calendar=calendar,
        config=config,
        broker=broker,
        data_reader=data_reader,
        time_machine=broker.time_machine,
        stab_engine=stab_engine,
        assessment_engine=assessment_engine,
        universe_engine=universe_engine,
        portfolio_engine=portfolio_engine,
        decision_tracker=DecisionTracker(db_manager=db_manager),
        instrument_ids=instrument_ids,
        apply_risk=apply_risk,
        num_workers=num_workers,
    )

    # Pre-compute sector health for the full backtest range so that
    # target_positions_for_date() can look up scores without reloading
    # sector ETF prices on every date.
    if bool(getattr(config, "apply_sector_allocator", False)):
        try:
            from datetime import timedelta

            shi_start = broker.time_machine.start_date - timedelta(days=400)
            shi_end = broker.time_machine.end_date
            shi_result = compute_sector_health(
                start=shi_start,
                end=shi_end,
                db_manager=db_manager,
                load_breadth=True,
            )
            pipeline._sector_health_result = shi_result

            mapper = SectorMapper(db_manager=db_manager)
            mapper.load(as_of_date=broker.time_machine.start_date)
            pipeline._sector_mapper = mapper

            logger.info(
                "_build_engines_for_sleeve: sector allocator enabled for %s "
                "(sectors=%d, dates=%d)",
                config.sleeve_id,
                len(shi_result.scores),
                len(shi_result.dates),
            )
        except Exception:  # pragma: no cover - defensive
            logger.exception(
                "_build_engines_for_sleeve: failed to pre-compute sector health "
                "for sleeve=%s; sector allocator will be disabled",
                config.sleeve_id,
            )

    return pipeline


def build_basic_sleeve_target_fn(
    db_manager: DatabaseManager,
    calendar: TradingCalendar,
    config: SleeveConfig,
    broker: BacktestBroker,
    *,
    apply_risk: bool = True,
    lambda_provider: object | None = None,
) -> TargetPositionsFn:
    """Return a ``target_positions_fn`` suitable for :class:`BacktestRunner`.

    The returned callable closes over a :class:`BasicSleevePipeline` and
    can be passed directly as ``target_positions_fn`` when constructing a
    :class:`BacktestRunner` instance.

    Args:
        apply_risk: If ``True`` (default), invoke the Risk Management
            Service to cap per-name weights before converting them into
            target positions. If ``False``, use raw portfolio weights.
    """

    pipeline = _build_engines_for_sleeve(
        db_manager=db_manager,
        calendar=calendar,
        config=config,
        broker=broker,
        apply_risk=apply_risk,
        lambda_provider=lambda_provider,
    )

    def _fn(as_of_date: date) -> Dict[str, float]:
        return pipeline.target_positions_for_date(as_of_date)

    def _set_run_context(*, run_id: str, decision_id: str | None = None) -> None:
        pipeline.backtest_run_id = str(run_id)
        pipeline.backtest_decision_id = str(decision_id) if decision_id is not None else None

    # Allow BacktestRunner to inject run_id/decision_id so per-date decisions
    # can be joined cheaply.
    setattr(_fn, "set_run_context", _set_run_context)

    return _fn


def build_basic_sleeve_target_and_exposure_fns(
    db_manager: DatabaseManager,
    calendar: TradingCalendar,
    config: SleeveConfig,
    broker: BacktestBroker,
    *,
    apply_risk: bool = True,
    lambda_provider: object | None = None,
) -> tuple[TargetPositionsFn, TargetPositionsFn]:
    """Construct both ``target_positions_fn`` and ``exposure_metrics_fn``.

    This is a thin wrapper around :func:`_build_engines_for_sleeve` that
    exposes :meth:`BasicSleevePipeline.exposure_metrics_for_date` so
    :class:`BacktestRunner` can attach lambda/state-aware diagnostics to
    ``backtest_daily_equity`` rows.
    """

    pipeline = _build_engines_for_sleeve(
        db_manager=db_manager,
        calendar=calendar,
        config=config,
        broker=broker,
        apply_risk=apply_risk,
        lambda_provider=lambda_provider,
    )

    def _target(as_of_date: date) -> Dict[str, float]:
        return pipeline.target_positions_for_date(as_of_date)

    def _exposure(as_of_date: date) -> Dict[str, float]:
        return pipeline.exposure_metrics_for_date(as_of_date)

    def _set_run_context(*, run_id: str, decision_id: str | None = None) -> None:
        pipeline.backtest_run_id = str(run_id)
        pipeline.backtest_decision_id = str(decision_id) if decision_id is not None else None

    # Attach hook so BacktestRunner can inject run_id/decision_id.
    setattr(_target, "set_run_context", _set_run_context)
    setattr(_exposure, "set_run_context", _set_run_context)

    return _target, _exposure
