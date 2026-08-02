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
from typing import Any, Callable, Dict, Optional, Set

from apatheon.core.logging import get_logger

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
# PricesProvider receives the as-of date AND the exact instrument set to
# price (held + selected names) so implementations don't have to guess
# which instruments matter or load the whole market.
PricesProvider = Callable[[date, Set[str]], Dict[str, float]]
StressLevelProvider = Callable[[date], StressLevel]
# Returns the instruments ACTUALLY held at the broker as of the date, or
# None when unavailable (reconciliation is skipped rather than treating
# "unknown" as "flat").
PositionsProvider = Callable[[date], Optional[Set[str]]]


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
    positions_provider: Optional[PositionsProvider] = None

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
        # Propagate _last_members from inner model so PortfolioEngine can persist.
        inner_members = getattr(self.inner_model, "_last_members", [])
        if inner_members:
            self._last_members = inner_members  # type: ignore[attr-defined]
        selection_weights = dict(inner_target.weights)
        current_selection: Set[str] = {
            iid for iid, w in selection_weights.items() if w > 0
        }

        # 2) Load prior conviction states.
        prior_states = self._load_states(pid, as_of_date)

        # 2b) Reconcile tracked state against the broker's actual book.
        # Conviction otherwise manages phantom positions indefinitely (a
        # failed fill leaves a state with no position behind it) and has
        # no state for positions it doesn't know about.
        if self.positions_provider is not None and prior_states:
            try:
                actual = self.positions_provider(as_of_date)
            except Exception as exc:
                logger.warning("positions_provider failed for %s: %s", as_of_date, exc)
                actual = None
            if actual is not None:
                phantom = set(prior_states) - actual
                for iid in phantom:
                    logger.warning(
                        "ConvictionPortfolioModel: dropping phantom conviction "
                        "state for %s (tracked but not held at broker)",
                        iid,
                    )
                    prior_states.pop(iid, None)
                untracked = actual - set(prior_states)
                if untracked:
                    logger.warning(
                        "ConvictionPortfolioModel: %d broker positions have no "
                        "conviction state (will enter as NEW if selected): %s",
                        len(untracked),
                        sorted(untracked),
                    )

        # 3) Get prices and stress level. Prices cover exactly the
        # instruments conviction must evaluate: currently tracked + today's
        # selection (entry prices for new names arm the hard stop).
        prices: Dict[str, float] = {}
        if self.prices_provider is not None:
            needed = set(prior_states) | current_selection
            try:
                prices = self.prices_provider(as_of_date, needed)
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

        # 5) Build adjusted weights. This also records each position's
        #    final target weight back into decision.position_states so it
        #    is persisted and re-used for held-but-not-selected names.
        adjusted_weights = self._apply_decision(
            selection_weights, decision, prior_states,
        )

        # 6) Persist states (including tombstones for today's exits).
        self._save_states(
            pid, decision.position_states, as_of_date,
            exited_states=decision.exited_states,
        )

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
        """Produce final weights from the inner model weights + conviction decision.

        Also records the final target weight back into the position's
        :class:`PositionConviction` state (``last_target_weight``) so that
        a position kept alive by conviction on a later day re-uses its own
        last real weight rather than being re-sized to the average of the
        selected names.
        """
        adjusted: Dict[str, float] = {}
        # Pre-taper base weights: persisted as last_target_weight so the
        # exit taper never compounds against its own prior output.
        base_weights: Dict[str, float] = {}

        # New entries: use inner model weight × entry fraction.
        for iid, frac in decision.entries.items():
            base_w = selection_weights.get(iid, 0.0)
            adjusted[iid] = base_w * frac
            base_weights[iid] = adjusted[iid]

        # Holds: may be currently selected or held by conviction.
        for iid, frac in decision.holds.items():
            if iid in selection_weights and selection_weights[iid] > 0:
                # Still selected: use current score-based weight × fraction.
                base = selection_weights[iid] * frac
            else:
                # Not selected but kept alive by conviction. Re-use the
                # position's own last real target weight so it stays a
                # stable size instead of being re-sized every day. Fall
                # back to the average of the selected names only when no
                # prior weight is known (e.g. legacy rows pre-dating
                # last_target_weight).
                prior = prior_states.get(iid)
                last_w = prior.last_target_weight if prior is not None else None
                if last_w is None or last_w <= 0:
                    selected_weights = [
                        w for w in selection_weights.values() if w > 0
                    ]
                    avg_w = (
                        sum(selected_weights) / len(selected_weights)
                        if selected_weights else 0.0
                    )
                    # A held position re-uses its full last weight; the
                    # hold fraction only scales unconfirmed new entries.
                    base = avg_w * frac
                else:
                    base = last_w

            base_weights[iid] = base
            # Exit taper scales the TRADED weight, not the persisted base.
            adjusted[iid] = base * float(decision.taper_fracs.get(iid, 1.0))

        # Exits: explicitly excluded (weight = 0, not in dict).
        # (Already handled by not being in entries or holds.)

        # Remove zero or negative weights.
        adjusted = {iid: w for iid, w in adjusted.items() if w > 0}

        # Renormalise so total weight ≤ 1.0.
        total = sum(adjusted.values())
        if total > 1.0 and total > 0:
            scale = 1.0 / total
            adjusted = {iid: w * scale for iid, w in adjusted.items()}
            base_weights = {iid: w * scale for iid, w in base_weights.items()}

        # Persist each position's pre-taper target weight so held-but-not-
        # selected names converge on a stable size across days.
        for iid in adjusted:
            state = decision.position_states.get(iid)
            if state is not None:
                state.last_target_weight = base_weights.get(iid, adjusted[iid])

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
                    portfolio_id,
                    as_of_date,
                    max_age_days=int(self.conviction_config.state_max_age_days),
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
        exited_states: Optional[Dict[str, PositionConviction]] = None,
    ) -> None:
        """Persist states (and exit tombstones) and update the cache."""
        self._state_cache = dict(states)

        if self.conviction_storage is not None:
            try:
                self.conviction_storage.save_states(
                    portfolio_id, states, as_of_date,
                    exited_states=exited_states,
                )
            except Exception as exc:
                logger.warning(
                    "ConvictionStorage.save_states failed: %s; states cached in memory",
                    exc,
                )


# ── Provider factories ────────────────────────────────────────────────
# Shared by the live pipeline (tasks.py) and the sleeve backtest so both
# run the SAME lifecycle mechanics (armed hard stops, regime decay).


def make_db_prices_provider(data_reader: Any) -> PricesProvider:
    """Prices from ``prices_daily`` — latest ADJUSTED close per instrument.

    Adjusted, not raw: the hard stop compares today's price against the
    stored entry price; a split between entry and today would fire (or
    mask) the stop spuriously on raw closes.
    """

    def _provider(as_of_date: date, instrument_ids: Set[str]) -> Dict[str, float]:
        ids = sorted(instrument_ids)
        if not ids:
            return {}
        from datetime import timedelta

        from prometheus.pricing_utils import adjusted_close_series

        df = data_reader.read_prices(ids, as_of_date - timedelta(days=10), as_of_date)
        if df.empty:
            return {}
        prices: Dict[str, float] = {}
        for inst_id, grp in df.groupby("instrument_id"):
            series = adjusted_close_series(grp.sort_values("trade_date"))
            if series.size and series[-1] > 0:
                prices[str(inst_id)] = float(series[-1])
        return prices

    return _provider


def make_snapshot_positions_provider(
    db_manager: Any,
    snapshot_portfolio_id: str = "IBKR_PAPER",
    max_staleness_days: int = 5,
    market_id: str | None = None,
) -> PositionsProvider:
    """Actually-held instruments from the latest broker positions snapshot.

    Returns ``None`` (reconciliation skipped) when there is no snapshot
    within ``max_staleness_days`` — "unknown" must not read as "flat".

    When ``market_id`` is set, the held set is restricted to instruments of
    that market (join against runtime ``instruments`` on
    ``i.market_id = %s``). This isolates a regional book's conviction
    reconcile from other markets' positions in the shared broker account.
    ``market_id=None`` preserves the legacy behavior (all positions).
    """

    def _provider(as_of_date: date) -> Optional[Set[str]]:
        from datetime import timedelta

        if market_id is None:
            # Legacy path: unchanged query, all markets.
            sql = """
                SELECT instrument_id, quantity
                FROM positions_snapshots
                WHERE portfolio_id = %s
                  AND as_of_date = (
                      SELECT MAX(as_of_date) FROM positions_snapshots
                      WHERE portfolio_id = %s
                        AND as_of_date <= %s
                        AND as_of_date >= %s
                  )
            """
        else:
            # Market-isolated path: LEFT JOIN runtime instruments so snapshot
            # rows never disappear — a snapshot that exists but holds nothing
            # in this market must yield an EMPTY set (broker flat in this
            # market -> phantom drops still fire), not None (no snapshot ->
            # reconcile skipped). Rows failing i.market_id = %s are filtered
            # by NULLing their quantity.
            sql = """
                SELECT ps.instrument_id,
                       CASE WHEN i.market_id = %s THEN ps.quantity ELSE NULL END AS quantity
                FROM positions_snapshots ps
                LEFT JOIN instruments i ON i.instrument_id = ps.instrument_id
                WHERE ps.portfolio_id = %s
                  AND ps.as_of_date = (
                      SELECT MAX(as_of_date) FROM positions_snapshots
                      WHERE portfolio_id = %s
                        AND as_of_date <= %s
                        AND as_of_date >= %s
                  )
            """
        cutoff = as_of_date - timedelta(days=max(1, int(max_staleness_days)))
        params: tuple[Any, ...] = (snapshot_portfolio_id, snapshot_portfolio_id, as_of_date, cutoff)
        if market_id is not None:
            params = (market_id,) + params
        with db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, params)
                rows = cursor.fetchall()
            finally:
                cursor.close()
        if not rows:
            return None
        return {
            str(iid)
            for iid, qty in rows
            if qty is not None and abs(float(qty)) > 1e-9
        }

    return _provider


__all__ = [
    "ConvictionPortfolioModel",
    "make_db_prices_provider",
    "make_snapshot_positions_provider",
]
