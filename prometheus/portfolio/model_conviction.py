"""Prometheus v2 – Conviction-aware portfolio model.

Wraps :class:`BasicLongOnlyPortfolioModel` (or any ``PortfolioModel``
implementation) and applies conviction-based position lifecycle logic.

The inner model produces the *selection signal* (which instruments
should be in the portfolio and at what weight).  This wrapper then:

1. Determines entries/holds/exits via :class:`ConvictionTracker`.
2. Scales new entries to half weight until confirmed.
3. Keeps positions alive beyond the selection signal while conviction
   remains above threshold.
4. Forces exits when conviction decays to zero or the hard stop fires.
5. Persists conviction state via :class:`ConvictionStorage`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Callable, Dict, List, Optional, Set

from apathis.core.logging import get_logger
from prometheus.portfolio.conviction import (
    ConvictionConfig,
    ConvictionDecision,
    ConvictionStorage,
    ConvictionTracker,
    PositionConviction,
)
from prometheus.portfolio.types import RiskReport, TargetPortfolio
from prometheus.sector.allocator import StressLevel


logger = get_logger(__name__)


# Type aliases for injectable providers.
PricesProvider = Callable[[date], Dict[str, float]]
StressLevelProvider = Callable[[date], StressLevel]


@dataclass
class ConvictionPortfolioModel:
    """Portfolio model that applies conviction-based entry/exit logic.

    This implements the ``PortfolioModel`` protocol expected by
    :class:`PortfolioEngine`.

    Parameters
    ----------
    inner_model
        The underlying model that produces score-based target weights
        (typically :class:`BasicLongOnlyPortfolioModel`).
    conviction_config
        Tuning parameters for the conviction tracker.
    conviction_storage
        Persistence layer for conviction states.  If ``None``, states
        are kept in memory only (useful for backtesting without DB).
    portfolio_id
        Logical portfolio identifier used for conviction state
        persistence.
    prices_provider
        Callable ``(date) -> {instrument_id: price}`` used for
        hard-stop evaluation.  When ``None`` hard stops are disabled.
    stress_level_provider
        Callable ``(date) -> StressLevel`` used to look up the current
        regime.  When ``None`` defaults to ``NORMAL``.
    """

    inner_model: Any  # PortfolioModel — avoid circular import
    conviction_config: ConvictionConfig = field(default_factory=ConvictionConfig)
    conviction_storage: Optional[ConvictionStorage] = None
    portfolio_id: str = ""
    prices_provider: Optional[PricesProvider] = None
    stress_level_provider: Optional[StressLevelProvider] = None

    # In-memory state cache for use when storage is not available
    # (e.g. unit tests or lightweight backtests).
    _state_cache: Dict[str, PositionConviction] = field(
        default_factory=dict, init=False,
    )
    _tracker: ConvictionTracker = field(init=False)

    def __post_init__(self) -> None:
        self._tracker = ConvictionTracker(self.conviction_config)

    # ── PortfolioModel protocol ──────────────────────────────────────

    def build_target_portfolio(
        self,
        portfolio_id: str,
        as_of_date: date,
    ) -> TargetPortfolio:
        """Build a conviction-adjusted target portfolio.

        Steps
        -----
        1. Call the inner model to get score-based weights (selection
           signal).
        2. Load prior conviction states.
        3. Run :meth:`ConvictionTracker.update` to get entries / exits /
           holds.
        4. Modify weights: apply weight fractions for half-size entries,
           keep positions alive beyond the signal via conviction, and
           zero out exits.
        5. Persist updated conviction states.
        6. Return the modified :class:`TargetPortfolio`.
        """
        pid = portfolio_id or self.portfolio_id

        # 1) Inner model: score-based target weights.
        inner_target = self.inner_model.build_target_portfolio(portfolio_id, as_of_date)
        selection_weights = dict(inner_target.weights)
        current_selection: Set[str] = {
            iid for iid, w in selection_weights.items() if w > 0
        }

        # 2) Load prior conviction states.
        prior_states = self._load_states(pid, as_of_date)

        # 3) Get prices and stress level.
        prices: Dict[str, float] = {}
        if self.prices_provider is not None:
            try:
                prices = self.prices_provider(as_of_date)
            except Exception as exc:
                logger.warning("prices_provider failed for %s: %s", as_of_date, exc)

        stress = StressLevel.NORMAL
        if self.stress_level_provider is not None:
            try:
                stress = self.stress_level_provider(as_of_date)
            except Exception as exc:
                logger.warning("stress_level_provider failed for %s: %s", as_of_date, exc)

        # 4) Conviction update.
        decision = self._tracker.update(
            current_selection=current_selection,
            prior_states=prior_states,
            prices=prices,
            as_of_date=as_of_date,
            stress_level=stress,
        )

        # 5) Build adjusted weights.
        adjusted_weights = self._apply_decision(
            selection_weights, decision, prior_states,
        )

        # 6) Persist states.
        self._save_states(pid, decision.position_states, as_of_date)

        # 7) Build metadata.
        n_entries = len(decision.entries)
        n_exits = len(decision.exits)
        n_holds = len(decision.holds)

        conviction_meta = {
            "conviction_entries": n_entries,
            "conviction_exits": n_exits,
            "conviction_holds": n_holds,
            "conviction_exit_reasons": dict(decision.exit_reasons),
        }

        meta = dict(inner_target.metadata)
        meta["conviction"] = conviction_meta

        risk_metrics = dict(inner_target.risk_metrics)
        net_exposure = sum(adjusted_weights.values())
        gross_exposure = sum(abs(w) for w in adjusted_weights.values())
        risk_metrics["net_exposure"] = net_exposure
        risk_metrics["gross_exposure"] = gross_exposure
        risk_metrics["cash_weight"] = max(0.0, 1.0 - net_exposure)
        risk_metrics["num_names"] = float(len(adjusted_weights))

        logger.info(
            "ConvictionPortfolioModel: %s as_of=%s entries=%d exits=%d holds=%d names=%d",
            pid, as_of_date, n_entries, n_exits, n_holds, len(adjusted_weights),
        )

        return TargetPortfolio(
            portfolio_id=inner_target.portfolio_id,
            as_of_date=as_of_date,
            weights=adjusted_weights,
            expected_return=inner_target.expected_return,
            expected_volatility=inner_target.expected_volatility,
            risk_metrics=risk_metrics,
            factor_exposures=inner_target.factor_exposures,
            constraints_status=inner_target.constraints_status,
            metadata=meta,
        )

    def build_risk_report(
        self,
        portfolio_id: str,
        as_of_date: date,
        target: Optional[TargetPortfolio] = None,
    ) -> Optional[RiskReport]:
        """Delegate risk report to the inner model."""
        if hasattr(self.inner_model, "build_risk_report"):
            return self.inner_model.build_risk_report(portfolio_id, as_of_date, target=target)
        return None

    # ── Internal helpers ──────────────────────────────────────────────

    def _apply_decision(
        self,
        selection_weights: Dict[str, float],
        decision: ConvictionDecision,
        prior_states: Dict[str, PositionConviction],
    ) -> Dict[str, float]:
        """Produce final weights from the inner model weights + conviction decision."""
        adjusted: Dict[str, float] = {}

        # New entries: use inner model weight × entry fraction.
        for iid, frac in decision.entries.items():
            base_w = selection_weights.get(iid, 0.0)
            adjusted[iid] = base_w * frac

        # Holds: may be currently selected or held by conviction.
        for iid, frac in decision.holds.items():
            if iid in selection_weights and selection_weights[iid] > 0:
                # Still selected: use current score-based weight × fraction.
                adjusted[iid] = selection_weights[iid] * frac
            else:
                # Not selected but kept alive by conviction.  Use the
                # *last known* weight from the inner model.  Since we
                # don't persist inner weights, we approximate with equal
                # share of the total weight budget.  A better approach
                # would store the last target weight in PositionConviction,
                # but for v1 this is acceptable.
                #
                # Use a conservative estimate: the average weight of
                # currently selected instruments.
                selected_weights = [
                    w for w in selection_weights.values() if w > 0
                ]
                avg_w = (
                    sum(selected_weights) / len(selected_weights)
                    if selected_weights else 0.0
                )
                adjusted[iid] = avg_w * frac

        # Exits: explicitly excluded (weight = 0, not in dict).
        # (Already handled by not being in entries or holds.)

        # Remove zero or negative weights.
        adjusted = {iid: w for iid, w in adjusted.items() if w > 0}

        # Renormalise so total weight ≤ 1.0.
        total = sum(adjusted.values())
        if total > 1.0 and total > 0:
            scale = 1.0 / total
            adjusted = {iid: w * scale for iid, w in adjusted.items()}

        return adjusted

    def _load_states(
        self,
        portfolio_id: str,
        as_of_date: date,
    ) -> Dict[str, PositionConviction]:
        """Load prior states from storage or in-memory cache."""
        if self.conviction_storage is not None:
            try:
                return self.conviction_storage.load_latest_states(
                    portfolio_id, as_of_date,
                )
            except Exception as exc:
                logger.warning(
                    "ConvictionStorage.load_latest_states failed: %s; using cache",
                    exc,
                )
        return dict(self._state_cache)

    def _save_states(
        self,
        portfolio_id: str,
        states: Dict[str, PositionConviction],
        as_of_date: date,
    ) -> None:
        """Persist states and update in-memory cache."""
        self._state_cache = dict(states)

        if self.conviction_storage is not None:
            try:
                self.conviction_storage.save_states(
                    portfolio_id, states, as_of_date,
                )
            except Exception as exc:
                logger.warning(
                    "ConvictionStorage.save_states failed: %s; states cached in memory",
                    exc,
                )


__all__ = [
    "ConvictionPortfolioModel",
]
