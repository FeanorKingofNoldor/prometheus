"""Prometheus v2 – Synthetic Options Position Tracker.

Tracks synthetic option positions throughout a backtest, handling:
- Mark-to-market repricing via BS pricer + IV surface
- Expiration: ITM exercise → cash settlement, OTM → expire worthless
- P&L attribution: delta, theta, vega, gamma components
- Portfolio-level greeks aggregation
- Format conversion for the strategy evaluate() interface

Usage::

    from prometheus.backtest.options_position import SyntheticOptionsBook

    book = SyntheticOptionsBook()
    book.open_position(...)
    book.mark_to_market(today, prices, vix, iv_engine, rfr)
    book.expire_positions(today)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Dict, List, Optional

from apathis.core.logging import get_logger

from prometheus.backtest.iv_surface import IVSurfaceEngine, VolTermStructure
from prometheus.backtest.option_pricer import (
    BSGreeks,
    bs_greeks,
)

logger = get_logger(__name__)


# ── Position dataclass ───────────────────────────────────────────────

@dataclass
class SyntheticOptionPosition:
    """One synthetic option position in the backtest."""

    # Identity
    position_id: str             # Unique ID
    symbol: str                  # Underlying symbol
    right: str                   # "C" or "P"
    expiry: str                  # YYYYMMDD
    strike: float
    quantity: int                # Positive = long, negative = short
    multiplier: int = 100

    # Entry
    entry_price: float = 0.0    # Per-share price at open
    entry_date: Optional[date] = None
    strategy: str = ""

    # Current state (updated by mark_to_market)
    current_price: float = 0.0
    current_greeks: BSGreeks = field(default_factory=lambda: BSGreeks(0, 0, 0, 0, 0, 0))
    current_iv: float = 0.0

    # Previous day state (for P&L attribution)
    prev_price: float = 0.0
    prev_underlying_price: float = 0.0
    prev_iv: float = 0.0

    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def expiry_date(self) -> date:
        y = int(self.expiry[:4])
        m = int(self.expiry[4:6])
        d = int(self.expiry[6:8])
        return date(y, m, d)

    @property
    def dte_from(self, ref: Optional[date] = None) -> int:
        """DTE relative to a reference date."""
        return (self.expiry_date - (ref or date.today())).days

    def dte(self, as_of: date) -> int:
        return (self.expiry_date - as_of).days

    @property
    def is_long(self) -> bool:
        return self.quantity > 0

    @property
    def is_short(self) -> bool:
        return self.quantity < 0

    @property
    def notional(self) -> float:
        return self.strike * self.multiplier * abs(self.quantity)

    @property
    def market_value(self) -> float:
        """Current market value of the position."""
        return self.current_price * self.multiplier * self.quantity

    @property
    def unrealized_pnl(self) -> float:
        """Unrealized P&L since entry."""
        return (self.current_price - self.entry_price) * self.multiplier * self.quantity

    @property
    def cost_basis(self) -> float:
        """Total cost basis."""
        return self.entry_price * self.multiplier * self.quantity

    def to_strategy_dict(self, as_of_date: Optional[date] = None) -> Dict[str, Any]:
        """Convert to the dict format expected by strategy evaluate().

        Parameters
        ----------
        as_of_date : date, optional
            Reference date for DTE calculation.  When omitted, falls back to
            entry_date so that the call remains backward-compatible, though
            callers should always pass the current backtest date.

        Matches the format from OptionPositionEntry.to_dict() in
        options_portfolio.py.
        """
        _ref = as_of_date or self.entry_date or date.today()
        return {
            "instrument_id": f"{self.symbol}_{self.expiry}_{self.right}{self.strike:.0f}",
            "symbol": self.symbol,
            "right": self.right,
            "expiry": self.expiry,
            "strike": self.strike,
            "quantity": self.quantity,
            "market_price": self.current_price,
            "entry_price": self.entry_price,
            "current_price": self.current_price,
            "strategy": self.strategy,
            "sector": self.metadata.get("sector", ""),
            # Bug fix: was (expiry_date - entry_date), which never changes and
            # causes stale DTE values in stop-loss / barrier checks.
            "dte": (self.expiry_date - _ref).days,
            # Expose full metadata so lifecycle barrier-stop can read wing_strike.
            "metadata": dict(self.metadata),
        }


# ── P&L Attribution ──────────────────────────────────────────────────

@dataclass
class PnLAttribution:
    """Daily P&L decomposition for the options book."""
    total_pnl: float = 0.0
    delta_pnl: float = 0.0     # From underlying price move
    theta_pnl: float = 0.0     # From time decay
    vega_pnl: float = 0.0      # From IV change
    gamma_pnl: float = 0.0     # From convexity (2nd order)
    residual_pnl: float = 0.0  # Unexplained (higher order + cross)
    realized_pnl: float = 0.0  # From closed/expired positions today
    n_positions: int = 0
    n_expired: int = 0
    n_closed: int = 0


# ── Aggregate greeks ─────────────────────────────────────────────────

@dataclass
class BookGreeks:
    """Portfolio-level aggregated greeks."""
    net_delta: float = 0.0        # In share-equivalents
    net_gamma: float = 0.0
    net_theta: float = 0.0        # Daily, in dollars
    net_vega: float = 0.0         # Per 1% vol move, in dollars
    total_notional: float = 0.0
    long_count: int = 0
    short_count: int = 0

    def to_dict(self) -> Dict[str, float]:
        return {
            "net_delta": self.net_delta,
            "net_gamma": self.net_gamma,
            "net_theta": self.net_theta,
            "net_vega": self.net_vega,
            "total_notional": self.total_notional,
        }


# ── Options Book ─────────────────────────────────────────────────────

class SyntheticOptionsBook:
    """Collection of synthetic option positions with lifecycle management.

    Parameters
    ----------
    initial_capital : float
        Capital allocated to derivatives (for sizing checks).
    """

    def __init__(self, initial_capital: float = 150_000.0) -> None:
        self._positions: Dict[str, SyntheticOptionPosition] = {}
        self._closed_pnl: float = 0.0  # Accumulated realized P&L
        self._next_id: int = 0
        self._capital = initial_capital

        # Per-strategy realized P&L accumulator (never reset)
        self._realized_pnl_by_strategy: Dict[str, float] = {}
        # Close events pending collection by the engine (cleared by pop_close_events)
        self._close_events: List[Dict[str, Any]] = []

        # Daily tracking
        self._daily_realized: float = 0.0
        self._daily_expired: int = 0
        self._daily_closed: int = 0

    @property
    def positions(self) -> Dict[str, SyntheticOptionPosition]:
        return self._positions

    @property
    def open_position_count(self) -> int:
        return len(self._positions)

    @property
    def total_realized_pnl(self) -> float:
        return self._closed_pnl

    @property
    def total_unrealized_pnl(self) -> float:
        return sum(p.unrealized_pnl for p in self._positions.values())

    @property
    def realized_pnl_by_strategy(self) -> Dict[str, float]:
        """Cumulative realized P&L per strategy (never reset)."""
        return dict(self._realized_pnl_by_strategy)

    def pop_close_events(self) -> List[Dict[str, Any]]:
        """Return and clear accumulated close events since last call.

        Each event dict contains: position_id, strategy, symbol, right,
        expiry, strike, quantity, multiplier, close_price, entry_price,
        realized_pnl, action.
        """
        events = self._close_events
        self._close_events = []
        return events

    @property
    def total_market_value(self) -> float:
        return sum(p.market_value for p in self._positions.values())

    # ── Position management ──────────────────────────────────────────

    def open_position(
        self,
        *,
        symbol: str,
        right: str,
        expiry: str,
        strike: float,
        quantity: int,
        entry_price: float,
        entry_date: date,
        strategy: str = "",
        multiplier: int = 100,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Open a new synthetic option position.

        Returns
        -------
        str
            Position ID.
        """
        self._next_id += 1
        pid = f"SYN_{self._next_id:06d}"

        pos = SyntheticOptionPosition(
            position_id=pid,
            symbol=symbol,
            right=right.upper(),
            expiry=expiry,
            strike=strike,
            quantity=quantity,
            multiplier=multiplier,
            entry_price=entry_price,
            entry_date=entry_date,
            strategy=strategy,
            current_price=entry_price,
            prev_price=entry_price,
            metadata=metadata or {},
        )

        self._positions[pid] = pos
        logger.debug(
            "Opened %s: %s %s %s %.1f x%d @ %.2f (%s)",
            pid, symbol, right, expiry, strike, quantity, entry_price, strategy,
        )
        return pid

    def close_position(
        self,
        position_id: str,
        close_price: float,
        action: str = "CLOSE",
    ) -> float:
        """Close a position and realize P&L.

        Parameters
        ----------
        action : str
            "CLOSE", "EXPIRE", or "ROLL" — recorded in the close event.

        Returns
        -------
        float
            Realized P&L.
        """
        pos = self._positions.pop(position_id, None)
        if pos is None:
            return 0.0

        realized = (close_price - pos.entry_price) * pos.multiplier * pos.quantity
        self._closed_pnl += realized
        self._daily_realized += realized
        self._daily_closed += 1

        strat = pos.strategy or "unknown"
        self._realized_pnl_by_strategy[strat] = (
            self._realized_pnl_by_strategy.get(strat, 0.0) + realized
        )
        self._close_events.append({
            "position_id": position_id,
            "strategy": strat,
            "symbol": pos.symbol,
            "right": pos.right,
            "expiry": pos.expiry,
            "strike": pos.strike,
            "quantity": pos.quantity,
            "multiplier": pos.multiplier,
            "close_price": close_price,
            "entry_price": pos.entry_price,
            "realized_pnl": realized,
            "action": action,
        })

        logger.debug(
            "Closed %s: %s %s %.1f x%d → P&L $%.2f (%s)",
            position_id, pos.symbol, pos.right, pos.strike, pos.quantity, realized, action,
        )
        return realized

    def close_positions_for_symbol(
        self,
        symbol: str,
        strategy: str,
        close_price_func,
    ) -> float:
        """Close all positions for a symbol + strategy combination.

        Parameters
        ----------
        close_price_func : callable
            Function(pos) → close_price for each position.

        Returns
        -------
        float
            Total realized P&L.
        """
        to_close = [
            pid for pid, pos in self._positions.items()
            if pos.symbol == symbol and pos.strategy == strategy
        ]
        total = 0.0
        for pid in to_close:
            pos = self._positions[pid]
            total += self.close_position(pid, close_price_func(pos))
        return total

    # ── Mark-to-market ───────────────────────────────────────────────

    def mark_to_market(
        self,
        as_of_date: date,
        underlying_prices: Dict[str, float],
        vix: float,
        iv_engine: IVSurfaceEngine,
        realized_vols: Dict[str, float],
        risk_free_rate: float = 0.05,
        term_structure: Optional[VolTermStructure] = None,
    ) -> PnLAttribution:
        """Reprice all open positions and compute P&L attribution.

        Parameters
        ----------
        as_of_date : date
            Current backtest date.
        underlying_prices : dict
            symbol → current price.
        vix : float
            Current VIX level.
        iv_engine : IVSurfaceEngine
            For computing IV per option.
        realized_vols : dict
            symbol → realized_vol_21d.
        risk_free_rate : float
            Current risk-free rate.
        term_structure : VolTermStructure, optional
            Real vol term structure for today's date.

        Returns
        -------
        PnLAttribution
            Daily P&L decomposition.
        """
        # Reset daily counters
        self._daily_realized = 0.0
        self._daily_expired = 0
        self._daily_closed = 0

        attr = PnLAttribution(n_positions=len(self._positions))

        for pos in self._positions.values():
            S = underlying_prices.get(pos.symbol, 0.0)
            if S <= 0:
                continue

            dte = pos.dte(as_of_date)
            T = max(dte, 1) / 365.0
            rv = realized_vols.get(pos.symbol, 0.0)

            # Get IV for this option
            iv = iv_engine.get_iv(
                strike=pos.strike,
                underlying_price=S,
                dte=max(dte, 1),
                vix=vix,
                realized_vol_21d=rv,
                symbol=pos.symbol,
                right=pos.right,
                term_structure=term_structure,
            )

            # Price and greeks
            greeks = bs_greeks(S, pos.strike, T, risk_free_rate, iv, pos.right)

            # Total P&L: today's price increment relative to yesterday's mark.
            # We always compute this as long as we have a previous price, so
            # the running sum in pnl_attr.total_pnl correctly telescopes to
            # (close_price - entry_price) over the full life of the position.
            # Note: prev_price is set to greeks.price at the END of each day,
            # so it always equals yesterday's BS mark.
            if pos.prev_price > 0:
                total_pos_pnl = (
                    (greeks.price - pos.prev_price) * pos.multiplier * pos.quantity
                )
                attr.total_pnl += total_pos_pnl

                # Greek decomposition (requires prev_underlying_price from day before).
                if pos.prev_underlying_price > 0:
                    dS = S - pos.prev_underlying_price
                    d_iv = iv - pos.prev_iv
                    prev_greeks = pos.current_greeks

                    delta_pnl = prev_greeks.delta * dS * pos.multiplier * pos.quantity
                    gamma_pnl = 0.5 * prev_greeks.gamma * dS * dS * pos.multiplier * pos.quantity
                    theta_pnl = prev_greeks.theta * pos.multiplier * pos.quantity
                    vega_pnl = prev_greeks.vega * (d_iv / 0.01) * pos.multiplier * pos.quantity

                    residual = total_pos_pnl - delta_pnl - gamma_pnl - theta_pnl - vega_pnl

                    attr.delta_pnl += delta_pnl
                    attr.gamma_pnl += gamma_pnl
                    attr.theta_pnl += theta_pnl
                    attr.vega_pnl += vega_pnl
                    attr.residual_pnl += residual

            # Update position state.
            # IMPORTANT: set prev_price = greeks.price (the NEW mark), not
            # pos.current_price (the old mark).  Using old current as prev
            # causes a 2-day lag: day-2 MTM would compute (day2 - entry) instead
            # of (day2 - day1), inflating cumulative P&L for volatile positions.
            pos.prev_price = greeks.price
            pos.prev_underlying_price = S
            pos.prev_iv = iv

            pos.current_price = greeks.price
            pos.current_greeks = greeks
            pos.current_iv = iv

        return attr

    # ── Expiration handling ──────────────────────────────────────────

    # TODO(issue-29): Options intrinsic value — expiration settlement uses only
    # intrinsic value (max(S-K,0) for calls, max(K-S,0) for puts). For American
    # options, early exercise decisions should also consider time value. For spread
    # positions, the net intrinsic may misstate P&L if only one leg is ITM. Consider
    # adding proper spread-aware settlement logic.
    def expire_positions(
        self,
        as_of_date: date,
        underlying_prices: Dict[str, float],
    ) -> float:
        """Handle positions expiring on as_of_date.

        ITM options: cash-settle at intrinsic value.
        OTM options: expire worthless.

        Returns
        -------
        float
            Total realized P&L from expirations.
        """
        to_expire = [
            pid for pid, pos in self._positions.items()
            if pos.expiry_date <= as_of_date
        ]

        total_incremental_pnl = 0.0
        for pid in to_expire:
            pos = self._positions[pid]
            S = underlying_prices.get(pos.symbol, 0.0)

            # Settlement value
            if pos.right == "C":
                intrinsic = max(S - pos.strike, 0.0)
            else:
                intrinsic = max(pos.strike - S, 0.0)

            # Incremental P&L: from last mark-to-market price → intrinsic.
            # We return this (not the full entry→intrinsic) to avoid double-counting
            # the daily MTM changes that have already been accumulated into
            # cumulative_options_pnl on every prior day this position was open.
            total_incremental_pnl += (
                (intrinsic - pos.current_price) * pos.multiplier * pos.quantity
            )

            # Full attribution tracking (entry→intrinsic) stays correct for
            # realized P&L reporting and _strategy_realized_pnl.
            self.close_position(pid, intrinsic, action="EXPIRE")
            self._daily_expired += 1

            if intrinsic > 0:
                logger.debug(
                    "Expired ITM %s %s %.1f: intrinsic $%.2f",
                    pos.symbol, pos.right, pos.strike, intrinsic,
                )

        if to_expire:
            logger.info(
                "Expired %d positions (incremental P&L: $%.2f)",
                len(to_expire), total_incremental_pnl,
            )

        return total_incremental_pnl

    # ── Greeks aggregation ───────────────────────────────────────────

    def get_portfolio_greeks(self) -> BookGreeks:
        """Aggregate greeks across all open positions."""
        g = BookGreeks()
        for pos in self._positions.values():
            mult = pos.multiplier * pos.quantity
            g.net_delta += pos.current_greeks.delta * mult
            g.net_gamma += pos.current_greeks.gamma * mult
            g.net_theta += pos.current_greeks.theta * mult
            g.net_vega += pos.current_greeks.vega * mult
            g.total_notional += pos.notional
            if pos.is_long:
                g.long_count += 1
            else:
                g.short_count += 1
        return g

    def get_greeks_by_strategy(self) -> Dict[str, BookGreeks]:
        """Aggregate greeks per strategy."""
        by_strat: Dict[str, BookGreeks] = {}
        for pos in self._positions.values():
            strat = pos.strategy or "unknown"
            if strat not in by_strat:
                by_strat[strat] = BookGreeks()
            g = by_strat[strat]
            mult = pos.multiplier * pos.quantity
            g.net_delta += pos.current_greeks.delta * mult
            g.net_gamma += pos.current_greeks.gamma * mult
            g.net_theta += pos.current_greeks.theta * mult
            g.net_vega += pos.current_greeks.vega * mult
            g.total_notional += pos.notional
            if pos.is_long:
                g.long_count += 1
            else:
                g.short_count += 1
        return by_strat

    # ── Strategy interface ───────────────────────────────────────────

    def to_existing_options_list(
        self, as_of_date: Optional[date] = None
    ) -> List[Dict[str, Any]]:
        """Convert all positions to the list-of-dicts format
        expected by strategy evaluate() as ``existing_options``.

        Parameters
        ----------
        as_of_date : date, optional
            Passed through to ``to_strategy_dict`` for correct DTE calculation.
        """
        return [
            pos.to_strategy_dict(as_of_date=as_of_date)
            for pos in self._positions.values()
        ]

    def get_positions_for_symbol(
        self,
        symbol: str,
        strategy: Optional[str] = None,
    ) -> List[SyntheticOptionPosition]:
        """Get all positions for a symbol, optionally filtered by strategy."""
        result = []
        for pos in self._positions.values():
            if pos.symbol != symbol:
                continue
            if strategy is not None and pos.strategy != strategy:
                continue
            result.append(pos)
        return result

    # ── Capital tracking ─────────────────────────────────────────────

    def update_capital(self, new_capital: float) -> None:
        """Update the derivatives capital allocation."""
        self._capital = new_capital

    @property
    def capital(self) -> float:
        return self._capital

    @property
    def capital_deployed(self) -> float:
        """Total premium spent on open positions (absolute value)."""
        return sum(
            abs(pos.entry_price * pos.multiplier * pos.quantity)
            for pos in self._positions.values()
        )

    @property
    def capital_utilization(self) -> float:
        """Fraction of derivatives capital deployed."""
        if self._capital <= 0:
            return 0.0
        return self.capital_deployed / self._capital

    # ── Summary ──────────────────────────────────────────────────────

    def summary(self) -> Dict[str, Any]:
        """Summary dict for logging/output."""
        greeks = self.get_portfolio_greeks()
        return {
            "n_open": self.open_position_count,
            "n_long": greeks.long_count,
            "n_short": greeks.short_count,
            "market_value": round(self.total_market_value, 2),
            "unrealized_pnl": round(self.total_unrealized_pnl, 2),
            "realized_pnl": round(self.total_realized_pnl, 2),
            "net_delta": round(greeks.net_delta, 1),
            "net_theta": round(greeks.net_theta, 2),
            "net_vega": round(greeks.net_vega, 2),
            "capital_util": round(self.capital_utilization, 3),
        }


__all__ = [
    "SyntheticOptionPosition",
    "SyntheticOptionsBook",
    "PnLAttribution",
    "BookGreeks",
]
