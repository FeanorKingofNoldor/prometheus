"""Prometheus v2 – Decision tracking service.

This module provides a high-level service for recording decisions from
various engines (Universe, Assessment, Portfolio, Execution). Each decision
is written to the `engine_decisions` table with structured metadata about
inputs, outputs, and reasoning.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List

from apathis.core.database import DatabaseManager
from apathis.core.ids import generate_uuid
from apathis.core.logging import get_logger

from prometheus.meta.storage import MetaStorage
from prometheus.meta.types import EngineDecision

logger = get_logger(__name__)


@dataclass
class DecisionTracker:
    """Service for recording engine decisions with structured metadata.

    Usage:
        tracker = DecisionTracker(db_manager=db)
        decision_id = tracker.record_assessment_decision(
            strategy_id="US_CORE_LONG_EQ",
            market_id="US_EQ",
            as_of_date=date(2024, 12, 15),
            instrument_scores={"AAPL.US": 0.85, "MSFT.US": 0.78},
            reasoning={"model": "basic", "horizon_days": 21}
        )
    """

    db_manager: DatabaseManager

    def __post_init__(self) -> None:
        self._storage = MetaStorage(db_manager=self.db_manager)

    def record_universe_decision(
        self,
        *,
        strategy_id: str,
        market_id: str,
        as_of_date: date,
        universe_id: str,
        included_instruments: List[str],
        excluded_instruments: List[str] | None = None,
        inclusion_reasons: Dict[str, Any] | None = None,
        exclusion_reasons: Dict[str, Any] | None = None,
        run_id: str | None = None,
        config_id: str | None = None,
    ) -> str:
        """Record a universe selection decision.

        Args:
            strategy_id: Strategy making the decision (e.g., "US_CORE_LONG_EQ")
            market_id: Market being analyzed (e.g., "US_EQ")
            as_of_date: Decision date
            universe_id: Generated universe identifier
            included_instruments: List of instrument IDs included in universe
            excluded_instruments: Optional list of excluded instrument IDs
            inclusion_reasons: Optional dict mapping instrument_id to inclusion rationale
            exclusion_reasons: Optional dict mapping instrument_id to exclusion rationale
            run_id: Optional engine run identifier
            config_id: Optional reference to universe config version

        Returns:
            decision_id: UUID of the recorded decision
        """
        decision_id = generate_uuid()

        input_refs = {
            "candidate_instruments": len(included_instruments) + len(excluded_instruments or []),
        }

        output_refs = {
            "universe_id": universe_id,
            "included_count": len(included_instruments),
            "excluded_count": len(excluded_instruments or []),
            "included_instruments": included_instruments[:100],  # Limit size
        }
        if excluded_instruments:
            # Keep a small sample so we can do basic counterfactual analysis
            # later without joining universe_members.
            output_refs["excluded_instruments"] = excluded_instruments[:100]

        metadata: Dict[str, Any] = {}
        if inclusion_reasons is not None:
            metadata["inclusion_reasons"] = inclusion_reasons
        if exclusion_reasons is not None:
            metadata["exclusion_reasons"] = exclusion_reasons

        decision = EngineDecision(
            decision_id=decision_id,
            engine_name="UNIVERSE",
            run_id=run_id,
            strategy_id=strategy_id,
            market_id=market_id,
            as_of_date=as_of_date,
            config_id=config_id,
            input_refs=input_refs,
            output_refs=output_refs,
            metadata=metadata,
        )

        self._storage.save_engine_decision(decision)

        logger.info(
            "Recorded universe decision: decision_id=%s strategy_id=%s universe_id=%s included=%d",
            decision_id,
            strategy_id,
            universe_id,
            len(included_instruments),
        )

        return decision_id

    def record_assessment_decision(
        self,
        *,
        strategy_id: str,
        market_id: str,
        as_of_date: date,
        universe_id: str,
        instrument_scores: Dict[str, float],
        model_id: str | None = None,
        horizon_days: int | None = None,
        reasoning: Dict[str, Any] | None = None,
        run_id: str | None = None,
        config_id: str | None = None,
    ) -> str:
        """Record an assessment scoring decision.

        Args:
            strategy_id: Strategy making the decision
            market_id: Market being analyzed
            as_of_date: Decision date
            universe_id: Universe being assessed
            instrument_scores: Dict mapping instrument_id to assessment score
            model_id: Optional assessment model identifier
            horizon_days: Optional forward-looking horizon
            reasoning: Optional dict with model rationale, features used, etc.
            run_id: Optional engine run identifier
            config_id: Optional reference to assessment config version

        Returns:
            decision_id: UUID of the recorded decision
        """
        decision_id = generate_uuid()

        # Compute score statistics for metadata
        scores = list(instrument_scores.values())
        score_stats = {
            "count": len(scores),
            "mean": sum(scores) / len(scores) if scores else 0.0,
            "min": min(scores) if scores else 0.0,
            "max": max(scores) if scores else 0.0,
        }

        input_refs = {
            "universe_id": universe_id,
            "instrument_count": len(instrument_scores),
        }

        if horizon_days is not None:
            input_refs["horizon_days"] = horizon_days

        output_refs = {
            "instrument_scores": instrument_scores,
            "score_statistics": score_stats,
        }

        metadata: Dict[str, Any] = {}
        if model_id is not None:
            metadata["model_id"] = model_id
        if reasoning is not None:
            metadata["reasoning"] = reasoning

        decision = EngineDecision(
            decision_id=decision_id,
            engine_name="ASSESSMENT",
            run_id=run_id,
            strategy_id=strategy_id,
            market_id=market_id,
            as_of_date=as_of_date,
            config_id=config_id,
            input_refs=input_refs,
            output_refs=output_refs,
            metadata=metadata,
        )

        self._storage.save_engine_decision(decision)

        logger.info(
            "Recorded assessment decision: decision_id=%s strategy_id=%s instruments=%d mean_score=%.3f",
            decision_id,
            strategy_id,
            len(instrument_scores),
            score_stats["mean"],
        )

        return decision_id

    def record_portfolio_decision(
        self,
        *,
        strategy_id: str,
        market_id: str,
        as_of_date: date,
        portfolio_id: str,
        target_weights: Dict[str, float],
        assessment_decision_id: str | None = None,
        constraints_applied: Dict[str, Any] | None = None,
        risk_metrics: Dict[str, float] | None = None,
        run_id: str | None = None,
        config_id: str | None = None,
    ) -> str:
        """Record a portfolio construction decision.

        Args:
            strategy_id: Strategy making the decision
            market_id: Market being analyzed
            as_of_date: Decision date
            portfolio_id: Portfolio identifier
            target_weights: Dict mapping instrument_id to target weight
            assessment_decision_id: Optional link to assessment decision
            constraints_applied: Optional dict describing constraints (max_weight, sector_caps, etc.)
            risk_metrics: Optional dict with expected_return, expected_vol, etc.
            run_id: Optional engine run identifier
            config_id: Optional reference to portfolio config version

        Returns:
            decision_id: UUID of the recorded decision
        """
        decision_id = generate_uuid()

        # Compute weight statistics
        weights = list(target_weights.values())
        weight_stats = {
            "count": len(weights),
            "sum": sum(weights),
            "mean": sum(weights) / len(weights) if weights else 0.0,
            "max": max(weights) if weights else 0.0,
        }

        input_refs = {
            "instrument_count": len(target_weights),
        }

        if assessment_decision_id is not None:
            input_refs["assessment_decision_id"] = assessment_decision_id

        output_refs = {
            "portfolio_id": portfolio_id,
            "target_weights": target_weights,
            "weight_statistics": weight_stats,
        }

        if risk_metrics is not None:
            output_refs["risk_metrics"] = risk_metrics

        metadata: Dict[str, Any] = {}
        if constraints_applied is not None:
            metadata["constraints_applied"] = constraints_applied

        decision = EngineDecision(
            decision_id=decision_id,
            engine_name="PORTFOLIO",
            run_id=run_id,
            strategy_id=strategy_id,
            market_id=market_id,
            as_of_date=as_of_date,
            config_id=config_id,
            input_refs=input_refs,
            output_refs=output_refs,
            metadata=metadata,
        )

        self._storage.save_engine_decision(decision)

        logger.info(
            "Recorded portfolio decision: decision_id=%s strategy_id=%s positions=%d weight_sum=%.3f",
            decision_id,
            strategy_id,
            len(target_weights),
            weight_stats["sum"],
        )

        return decision_id

    def record_execution_decision(
        self,
        *,
        strategy_id: str,
        market_id: str,
        as_of_date: date,
        portfolio_id: str,
        orders_generated: List[Dict[str, Any]],
        portfolio_decision_id: str | None = None,
        current_positions: Dict[str, float] | None = None,
        expected_cost: float | None = None,
        plan_summary: Dict[str, Any] | None = None,
        execution_policy: Dict[str, Any] | None = None,
        run_id: str | None = None,
        config_id: str | None = None,
        metadata: Dict[str, Any] | None = None,
    ) -> str:
        """Record an execution decision (order generation).

        Args:
            strategy_id: Strategy making the decision
            market_id: Market being executed
            as_of_date: Decision date
            portfolio_id: Portfolio being rebalanced
            orders_generated: List of order dicts with side, instrument_id, quantity, etc.
            portfolio_decision_id: Optional link to portfolio decision
            current_positions: Optional dict of current positions before rebalancing
            expected_cost: Optional expected transaction cost estimate
            run_id: Optional engine run identifier
            config_id: Optional reference to execution config version

        Returns:
            decision_id: UUID of the recorded decision
        """
        decision_id = generate_uuid()

        input_refs = {
            "portfolio_id": portfolio_id,
            "order_count": len(orders_generated),
        }

        if portfolio_decision_id is not None:
            input_refs["portfolio_decision_id"] = portfolio_decision_id

        if current_positions is not None:
            input_refs["current_position_count"] = len(current_positions)

        output_refs = {
            "orders": orders_generated[:100],  # Limit size
            "order_count": len(orders_generated),
        }

        if expected_cost is not None:
            output_refs["expected_transaction_cost"] = expected_cost

        if plan_summary is not None:
            output_refs["plan_summary"] = plan_summary

        if execution_policy is not None:
            output_refs["execution_policy"] = execution_policy

        metadata = dict(metadata or {})

        decision = EngineDecision(
            decision_id=decision_id,
            engine_name="EXECUTION",
            run_id=run_id,
            strategy_id=strategy_id,
            market_id=market_id,
            as_of_date=as_of_date,
            config_id=config_id,
            input_refs=input_refs,
            output_refs=output_refs,
            metadata=metadata,
        )

        self._storage.save_engine_decision(decision)

        logger.info(
            "Recorded execution decision: decision_id=%s strategy_id=%s orders=%d",
            decision_id,
            strategy_id,
            len(orders_generated),
        )

        return decision_id

    def record_options_decision(
        self,
        *,
        strategy_id: str,
        market_id: str,
        as_of_date: date,
        orders: List[Dict[str, Any]],
        signals_snapshot: Dict[str, Any] | None = None,
        run_id: str | None = None,
    ) -> str:
        """Record an options/derivatives trade decision.

        Args:
            strategy_id: Strategy group identifier (e.g., "US_OPTIONS")
            market_id: Market being traded (e.g., "US_EQ")
            as_of_date: Decision date
            orders: List of order dicts, each containing:
                - symbol: Underlying symbol (e.g., "VIX")
                - underlying_id: Instrument ID of the underlying (e.g., "VIX.INDX")
                - instrument_id: Option instrument ID (e.g., "VIX_260519_38C.US")
                - right: "C" or "P"
                - expiry: YYYYMMDD string
                - strike: float
                - action: "BUY" or "SELL"
                - quantity: positive int
                - entry_price: limit price (premium)
                - strategy: strategy name (e.g., "vix_tail_hedge")
                - reason: reasoning string
                - trade_action: TradeAction value ("OPEN", "CLOSE", "ROLL", etc.)
            signals_snapshot: Optional dict of market signals at decision time
                (vix_level, nav, mhi, frag, etc.)
            run_id: Optional engine run identifier

        Returns:
            decision_id: UUID of the recorded decision
        """
        decision_id = generate_uuid()

        input_refs: Dict[str, Any] = {
            "order_count": len(orders),
        }
        if signals_snapshot is not None:
            input_refs["signals_snapshot"] = signals_snapshot

        output_refs: Dict[str, Any] = {
            "orders": orders[:100],  # Cap at 100 orders
            "order_count": len(orders),
        }

        # Aggregate summary for quick inspection
        strategies = list({o.get("strategy", "") for o in orders})
        symbols = list({o.get("symbol", "") for o in orders})
        output_refs["strategies"] = strategies
        output_refs["symbols"] = symbols

        decision = EngineDecision(
            decision_id=decision_id,
            engine_name="OPTIONS",
            run_id=run_id,
            strategy_id=strategy_id,
            market_id=market_id,
            as_of_date=as_of_date,
            config_id=None,
            input_refs=input_refs,
            output_refs=output_refs,
            metadata={},
        )

        self._storage.save_engine_decision(decision)

        logger.info(
            "Recorded options decision: decision_id=%s strategy_id=%s orders=%d strategies=%s",
            decision_id,
            strategy_id,
            len(orders),
            strategies,
        )

        return decision_id

