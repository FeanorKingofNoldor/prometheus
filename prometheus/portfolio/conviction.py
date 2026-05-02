"""Prometheus v2 – Conviction-based position lifecycle manager.

Replaces symmetric rebalancing exits with a conviction score that
tracks how consistently the universe selection signal supports each
held position.  Positions are only sold when conviction decays below
a threshold (or a hard stop is hit), not simply because the score
dipped for one day.

Core concepts
-------------
- **Conviction score** accumulates +1/day when the instrument is in the
  daily selection set and decays at -2/day (regime-adjusted) when absent.
- New entries start with an *entry credit* (+5) and at half target weight.
  After 3 consecutive days of selection they scale to full weight.
- A *hard stop* (-20 % from average entry price) provides catastrophic
  protection independent of the signal.

Usage
-----
    from prometheus.portfolio.conviction import (
        ConvictionConfig, ConvictionTracker, ConvictionStorage,
    )

    tracker = ConvictionTracker(config)
    decision = tracker.update(
        current_selection={"AAPL.US", "MSFT.US"},
        positions=broker.get_positions(),
        prices={"AAPL.US": 195.0, "MSFT.US": 410.0},
        as_of_date=date.today(),
        stress_level=StressLevel.NORMAL,
    )
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Dict, Optional, Set

from apatheon.core.logging import get_logger

from prometheus.sector.allocator import StressLevel

logger = get_logger(__name__)


# ── Configuration ────────────────────────────────────────────────────

_DEFAULT_DECAY_MULTIPLIERS: Dict[str, float] = {
    StressLevel.NORMAL: 1.0,
    StressLevel.SECTOR_STRESS: 1.5,
    StressLevel.BROAD_STRESS: 2.0,
    # Crisis exits are handled by CrisisOrderManager; keep multiplier
    # at 1.0 so conviction doesn't race the override layer.
    StressLevel.SYSTEMIC_CRISIS: 1.0,
}


@dataclass
class ConvictionConfig:
    """Parameters governing conviction score mechanics."""

    entry_credit: float = 5.0
    build_rate: float = 1.0
    base_decay_rate: float = 2.0
    score_cap: float = 20.0
    sell_threshold: float = 0.0
    hard_stop_pct: float = 0.20
    scale_up_days: int = 3
    entry_weight_fraction: float = 0.50
    decay_multipliers: Dict[str, float] = field(
        default_factory=lambda: dict(_DEFAULT_DECAY_MULTIPLIERS)
    )


# ── Per-position state ───────────────────────────────────────────────

@dataclass
class PositionConviction:
    """Tracked conviction state for a single held position."""

    instrument_id: str
    conviction_score: float
    entry_date: date
    avg_entry_price: float
    consecutive_selected: int = 0
    is_scaled_up: bool = False
    last_updated: date = field(default_factory=date.today)


# ── Decision output ──────────────────────────────────────────────────

@dataclass
class ConvictionDecision:
    """Output of a single day's conviction update.

    Attributes
    ----------
    entries : dict
        instrument_id → weight fraction for new positions.
        Fraction is ``entry_weight_fraction`` (0.5) until confirmed.
    exits : set
        instrument_ids to fully liquidate.
    holds : dict
        instrument_id → weight fraction for existing positions
        (0.5 if unconfirmed, 1.0 if scaled up).
    position_states : dict
        Updated :class:`PositionConviction` objects for persistence.
    exit_reasons : dict
        instrument_id → reason string for each exit.
    """

    entries: Dict[str, float] = field(default_factory=dict)
    exits: Set[str] = field(default_factory=set)
    holds: Dict[str, float] = field(default_factory=dict)
    position_states: Dict[str, PositionConviction] = field(default_factory=dict)
    exit_reasons: Dict[str, str] = field(default_factory=dict)


# ── Core tracker ─────────────────────────────────────────────────────

class ConvictionTracker:
    """Maintains conviction scores and produces entry/exit decisions.

    The tracker is intentionally **stateless** between calls: callers
    pass in the previous day's :class:`PositionConviction` states and
    receive updated states in the returned :class:`ConvictionDecision`.
    This makes the tracker easy to test, back-fill, and serialise.

    Parameters
    ----------
    config : ConvictionConfig
        Tuning parameters.
    """

    def __init__(self, config: Optional[ConvictionConfig] = None) -> None:
        self._cfg = config or ConvictionConfig()

    @property
    def config(self) -> ConvictionConfig:
        return self._cfg

    def update(
        self,
        current_selection: Set[str],
        prior_states: Dict[str, PositionConviction],
        prices: Dict[str, float],
        as_of_date: date,
        stress_level: StressLevel = StressLevel.NORMAL,
    ) -> ConvictionDecision:
        """Run one day's conviction update.

        Parameters
        ----------
        current_selection
            Instrument IDs that the universe/portfolio model *would*
            select today (i.e. those with weight > 0 from the inner
            model).
        prior_states
            Previous day's conviction states, keyed by instrument_id.
            For the very first day this is empty.
        prices
            Current prices keyed by instrument_id.  Used only for
            hard-stop evaluation.
        as_of_date
            Today's date.
        stress_level
            Current market stress level from :class:`SectorAllocator`.

        Returns
        -------
        ConvictionDecision
        """
        cfg = self._cfg
        decay_mult = cfg.decay_multipliers.get(
            stress_level.value if isinstance(stress_level, StressLevel) else str(stress_level),
            1.0,
        )
        effective_decay = cfg.base_decay_rate * decay_mult

        decision = ConvictionDecision()

        # All instruments we need to evaluate: currently held + newly selected.
        all_instruments = set(prior_states.keys()) | current_selection

        for iid in all_instruments:
            prior = prior_states.get(iid)
            is_selected = iid in current_selection
            price = prices.get(iid)

            if prior is not None:
                # ── Existing position: update conviction ─────────
                new_state = self._update_existing(
                    prior, is_selected, price, as_of_date,
                    effective_decay, cfg,
                )

                if new_state is None:
                    # Exit triggered.
                    continue  # already recorded in decision via _update_existing side-effects
                    # (we handle this below instead)

                # Check exits
                exit_reason = self._check_exit(new_state, price, cfg)
                if exit_reason is not None:
                    decision.exits.add(iid)
                    decision.exit_reasons[iid] = exit_reason
                    # Don't persist state for exited positions.
                    logger.info(
                        "ConvictionTracker: EXIT %s reason=%s score=%.1f",
                        iid, exit_reason, new_state.conviction_score,
                    )
                    continue

                # Hold
                weight_frac = 1.0 if new_state.is_scaled_up else cfg.entry_weight_fraction
                decision.holds[iid] = weight_frac
                decision.position_states[iid] = new_state

            elif is_selected:
                # ── New entry ────────────────────────────────────
                entry_price = price if price is not None else 0.0
                new_state = PositionConviction(
                    instrument_id=iid,
                    conviction_score=cfg.entry_credit,
                    entry_date=as_of_date,
                    avg_entry_price=entry_price,
                    consecutive_selected=1,
                    is_scaled_up=False,
                    last_updated=as_of_date,
                )
                decision.entries[iid] = cfg.entry_weight_fraction
                decision.position_states[iid] = new_state
                logger.debug(
                    "ConvictionTracker: ENTRY %s score=%.1f price=%.2f",
                    iid, cfg.entry_credit, entry_price,
                )

        return decision

    # ── Internal helpers ──────────────────────────────────────────────

    def _update_existing(
        self,
        prior: PositionConviction,
        is_selected: bool,
        price: Optional[float],
        as_of_date: date,
        effective_decay: float,
        cfg: ConvictionConfig,
    ) -> PositionConviction:
        """Produce an updated PositionConviction for an existing position."""
        if is_selected:
            new_score = prior.conviction_score + cfg.build_rate
            new_consec = prior.consecutive_selected + 1
        else:
            new_score = prior.conviction_score - effective_decay
            new_consec = 0

        # Clamp to cap.
        new_score = min(new_score, cfg.score_cap)

        # Scale-up: once consecutive_selected reaches threshold, lock in.
        is_scaled = prior.is_scaled_up or (new_consec >= cfg.scale_up_days)

        return PositionConviction(
            instrument_id=prior.instrument_id,
            conviction_score=new_score,
            entry_date=prior.entry_date,
            avg_entry_price=prior.avg_entry_price,
            consecutive_selected=new_consec,
            is_scaled_up=is_scaled,
            last_updated=as_of_date,
        )

    @staticmethod
    def _check_exit(
        state: PositionConviction,
        price: Optional[float],
        cfg: ConvictionConfig,
    ) -> Optional[str]:
        """Return an exit reason string, or None to keep holding."""
        # Hard stop: price has fallen below threshold from entry.
        # Only applies to confirmed (scaled-up) positions — unconfirmed
        # positions are protected by conviction decay instead, avoiding
        # churn from volatile entries that haven't proven themselves.
        if (
            state.is_scaled_up
            and price is not None
            and state.avg_entry_price > 0
            and price <= state.avg_entry_price * (1.0 - cfg.hard_stop_pct)
        ):
            return "hard_stop"

        # Conviction decay.
        if state.conviction_score <= cfg.sell_threshold:
            return "conviction_decay"

        return None


# ── Persistence ──────────────────────────────────────────────────────

class ConvictionStorage:
    """Read/write :class:`PositionConviction` states to the runtime DB.

    Uses the ``position_convictions`` table (created by migration).
    """

    def __init__(self, db_manager: Any) -> None:
        self._db = db_manager

    def save_states(
        self,
        portfolio_id: str,
        states: Dict[str, PositionConviction],
        as_of_date: date,
    ) -> None:
        """Upsert conviction states for the given date."""
        if not states:
            return

        sql = """
            INSERT INTO position_convictions (
                portfolio_id,
                instrument_id,
                as_of_date,
                conviction_score,
                entry_date,
                avg_entry_price,
                consecutive_selected,
                is_scaled_up
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (portfolio_id, instrument_id, as_of_date)
            DO UPDATE SET
                conviction_score = EXCLUDED.conviction_score,
                entry_date = EXCLUDED.entry_date,
                avg_entry_price = EXCLUDED.avg_entry_price,
                consecutive_selected = EXCLUDED.consecutive_selected,
                is_scaled_up = EXCLUDED.is_scaled_up
        """

        with self._db.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                for state in states.values():
                    cursor.execute(sql, (
                        portfolio_id,
                        state.instrument_id,
                        as_of_date,
                        state.conviction_score,
                        state.entry_date,
                        state.avg_entry_price,
                        state.consecutive_selected,
                        state.is_scaled_up,
                    ))
                conn.commit()
            finally:
                cursor.close()

        logger.debug(
            "ConvictionStorage.save_states: saved %d states for %s on %s",
            len(states), portfolio_id, as_of_date,
        )

    def load_latest_states(
        self,
        portfolio_id: str,
        as_of_date: date,
    ) -> Dict[str, PositionConviction]:
        """Load the most recent conviction state for each instrument.

        Returns states from the latest ``as_of_date <= as_of_date`` per
        instrument.
        """
        sql = """
            SELECT DISTINCT ON (instrument_id)
                instrument_id,
                conviction_score,
                entry_date,
                avg_entry_price,
                consecutive_selected,
                is_scaled_up,
                as_of_date
            FROM position_convictions
            WHERE portfolio_id = %s
              AND as_of_date <= %s
            ORDER BY instrument_id, as_of_date DESC
        """

        with self._db.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (portfolio_id, as_of_date))
                rows = cursor.fetchall()
            finally:
                cursor.close()

        states: Dict[str, PositionConviction] = {}
        for row in rows:
            iid, score, entry_dt, entry_px, consec, scaled, last_dt = row
            states[iid] = PositionConviction(
                instrument_id=iid,
                conviction_score=float(score),
                entry_date=entry_dt,
                avg_entry_price=float(entry_px),
                consecutive_selected=int(consec),
                is_scaled_up=bool(scaled),
                last_updated=last_dt,
            )

        logger.debug(
            "ConvictionStorage.load_latest_states: loaded %d states for %s as_of %s",
            len(states), portfolio_id, as_of_date,
        )
        return states


__all__ = [
    "ConvictionConfig",
    "ConvictionDecision",
    "ConvictionStorage",
    "ConvictionTracker",
    "PositionConviction",
]
