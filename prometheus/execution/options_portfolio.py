"""Prometheus v2 – Options & Derivatives Position Management.

Tracks all open option, futures-option (FOP), and futures positions with:
- Per-position greeks (delta, gamma, theta, vega)
- Daily P&L attribution (delta P&L vs theta decay vs vega P&L)
- Expiry calendar with alerts at 14 DTE (warning) and 7 DTE (forced close/roll)
- Portfolio-level aggregated greeks (OPT + FOP combined)
- Futures position tracking alongside options
- Pre-trade margin check via IBKR ``whatIf=True``

Usage
-----
    from prometheus.execution.options_portfolio import OptionsPortfolio

    portfolio = OptionsPortfolio(ibkr_client)
    portfolio.sync()
    print(portfolio.total_delta)
    expiring = portfolio.get_expiring_positions(days=14)
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from apathis.core.logging import get_logger
from prometheus.execution.broker_interface import Order, Position
from prometheus.execution.instrument_mapper import InstrumentMapper

logger = get_logger(__name__)


# ── Greeks container ─────────────────────────────────────────────────

@dataclass
class OptionGreeks:
    """Greeks for a single option position."""
    delta: float = 0.0
    gamma: float = 0.0
    theta: float = 0.0   # Daily theta (negative for long options)
    vega: float = 0.0
    implied_vol: float = 0.0
    underlying_price: float = 0.0


@dataclass
class OptionPositionEntry:
    """Full tracked state for one option position.

    This is Prometheus's internal representation — richer than IBKR's
    raw Position object because we track strategy provenance and greeks.
    """
    instrument_id: str
    symbol: str           # Underlying symbol
    right: str            # "C" or "P"
    expiry: str           # YYYYMMDD
    strike: float
    quantity: int          # Positive = long, negative = short
    multiplier: int = 100
    avg_cost: float = 0.0  # Per-share cost basis
    market_price: float = 0.0  # Current option price per share
    market_value: float = 0.0
    unrealized_pnl: float = 0.0
    greeks: OptionGreeks = field(default_factory=OptionGreeks)
    strategy: str = ""     # Which strategy opened this (e.g. "protective_put")
    opened_date: Optional[date] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def dte(self) -> int:
        """Days to expiration from today."""
        try:
            exp = datetime.strptime(self.expiry[:8], "%Y%m%d").date()
            return (exp - date.today()).days
        except Exception:
            return 0

    @property
    def notional(self) -> float:
        """Notional value: strike * multiplier * |quantity|."""
        return self.strike * self.multiplier * abs(self.quantity)

    @property
    def is_long(self) -> bool:
        return self.quantity > 0

    @property
    def is_short(self) -> bool:
        return self.quantity < 0

    def to_dict(self) -> Dict[str, Any]:
        """Serialise to dict (for passing to strategy evaluate())."""
        return {
            "instrument_id": self.instrument_id,
            "symbol": self.symbol,
            "right": self.right,
            "expiry": self.expiry,
            "strike": self.strike,
            "quantity": self.quantity,
            "market_price": self.market_price,
            "entry_price": self.avg_cost,
            "current_price": self.market_price,
            "strategy": self.strategy,
            "sector": self.metadata.get("sector", ""),
            "dte": self.dte,
        }


# ── Expiry alert levels ──────────────────────────────────────────────

@dataclass(frozen=True)
class ExpiryAlert:
    """An upcoming expiry that needs attention."""
    position: OptionPositionEntry
    dte: int
    level: str  # "WARNING" (14 DTE) or "CRITICAL" (7 DTE)


# ── Portfolio-level aggregated greeks ────────────────────────────────

@dataclass
class PortfolioGreeks:
    """Aggregated greeks across all option positions."""
    total_delta: float = 0.0     # Net delta in share-equivalents
    total_gamma: float = 0.0
    total_theta: float = 0.0     # Daily theta in dollars
    total_vega: float = 0.0      # Vega in dollars per 1% vol move
    total_notional: float = 0.0
    long_count: int = 0
    short_count: int = 0


# ── Greeks budget ────────────────────────────────────────────────────

@dataclass
class GreeksBudgetConfig:
    """Portfolio-level greeks limits."""
    max_delta_pct: float = 0.20       # Max |net delta| as fraction of NAV
    max_gamma: float = 50_000.0       # Max absolute gamma
    min_theta: float = -10_000.0      # Min daily theta (dollars)
    max_vega: float = 100_000.0       # Max vega
    # Per-strategy limits are computed as proportional fractions


@dataclass
class GreeksUtilisation:
    """How much of the greeks budget has been consumed."""
    delta_used: float = 0.0
    delta_limit: float = 0.0
    delta_pct: float = 0.0            # |used| / limit
    gamma_used: float = 0.0
    gamma_limit: float = 0.0
    gamma_pct: float = 0.0
    theta_used: float = 0.0
    theta_limit: float = 0.0
    theta_pct: float = 0.0            # used / limit (both negative)
    vega_used: float = 0.0
    vega_limit: float = 0.0
    vega_pct: float = 0.0
    within_budget: bool = True


# ── Margin estimate ──────────────────────────────────────────────────

@dataclass
class MarginEstimate:
    """Pre-trade margin impact from IBKR whatIf."""
    init_margin_change: float = 0.0
    maint_margin_change: float = 0.0
    equity_with_loan: float = 0.0
    commission: float = 0.0
    max_quantity: int = 0  # Max contracts given available margin
    approved: bool = False
    reason: str = ""


# ── Options Portfolio ────────────────────────────────────────────────

class OptionsPortfolio:
    """Track and manage all open option positions.

    Parameters
    ----------
    ib : Any
        Connected ``ib_insync.IB`` instance (for whatIf margin checks
        and greeks queries).  Can be None for offline usage.
    warning_dte : int
        DTE threshold for expiry warnings.
    critical_dte : int
        DTE threshold for forced close/roll.
    """

    def __init__(
        self,
        ib: Any = None,
        *,
        warning_dte: int = 14,
        critical_dte: int = 7,
    ) -> None:
        self._ib = ib
        self._warning_dte = warning_dte
        self._critical_dte = critical_dte

        self._positions: Dict[str, OptionPositionEntry] = {}
        self._lock = threading.Lock()

        # Strategy provenance: instrument_id → strategy name
        self._strategy_map: Dict[str, str] = {}

    # ── Sync from broker ─────────────────────────────────────────────

    def sync(self, broker_positions: Optional[Dict[str, Position]] = None) -> None:
        """Sync option positions from the broker.

        Parameters
        ----------
        broker_positions : dict, optional
            If provided, filter for option positions.  Otherwise uses
            the IB instance to query portfolio directly.
        """
        if broker_positions is not None:
            self._sync_from_positions(broker_positions)
        elif self._ib is not None:
            self._sync_from_ib()
        else:
            logger.warning("No broker positions or IB instance for sync")

    def _sync_from_ib(self) -> None:
        """Sync from live IB portfolio (OPT + FOP)."""
        try:
            portfolio_items = self._ib.portfolio()
        except Exception as exc:
            logger.error("Failed to get portfolio from IB: %s", exc)
            return

        new_positions: Dict[str, OptionPositionEntry] = {}

        for item in portfolio_items:
            contract = item.contract
            sec_type = getattr(contract, "secType", "")
            if sec_type not in ("OPT", "FOP"):
                continue

            instrument_id = InstrumentMapper.contract_to_instrument_id(contract)

            # FOP multipliers differ from equity options (e.g. ES FOP = 50)
            raw_mult = getattr(contract, "multiplier", 100) or 100

            entry = OptionPositionEntry(
                instrument_id=instrument_id,
                symbol=contract.symbol,
                right=getattr(contract, "right", ""),
                expiry=getattr(contract, "lastTradeDateOrContractMonth", ""),
                strike=float(getattr(contract, "strike", 0) or 0),
                quantity=int(item.position),
                multiplier=int(raw_mult),
                avg_cost=float(item.averageCost),
                market_price=float(item.marketPrice),
                market_value=float(item.marketValue),
                unrealized_pnl=float(item.unrealizedPNL),
                strategy=self._strategy_map.get(instrument_id, ""),
                metadata={"sec_type": sec_type},
            )

            # Query greeks if available
            self._update_greeks(entry, contract)

            new_positions[instrument_id] = entry

        with self._lock:
            self._positions = new_positions

        logger.info(
            "Synced %d option/FOP positions from IB (OPT=%d, FOP=%d)",
            len(new_positions),
            sum(1 for p in new_positions.values() if p.metadata.get("sec_type") == "OPT"),
            sum(1 for p in new_positions.values() if p.metadata.get("sec_type") == "FOP"),
        )

    def _sync_from_positions(self, broker_positions: Dict[str, Position]) -> None:
        """Sync from broker Position objects (filter for options)."""
        new_positions: Dict[str, OptionPositionEntry] = {}

        for iid, pos in broker_positions.items():
            # Heuristic: option instrument_ids contain underscores and C/P
            if "_" not in iid:
                continue
            parts = iid.replace(".US", "").split("_")
            if len(parts) < 3:
                continue

            symbol = parts[0]
            expiry_short = parts[1]
            strike_right = parts[2]

            # Parse strike and right from e.g. "400P" or "175.5C"
            right = strike_right[-1].upper()
            if right not in ("C", "P"):
                continue
            try:
                strike = float(strike_right[:-1])
            except ValueError:
                continue

            # Expand YYMMDD to YYYYMMDD
            expiry = f"20{expiry_short}" if len(expiry_short) == 6 else expiry_short

            entry = OptionPositionEntry(
                instrument_id=iid,
                symbol=symbol,
                right=right,
                expiry=expiry,
                strike=strike,
                quantity=int(pos.quantity),
                avg_cost=pos.avg_cost,
                market_value=pos.market_value,
                unrealized_pnl=pos.unrealized_pnl,
                strategy=self._strategy_map.get(iid, ""),
            )

            new_positions[iid] = entry

        with self._lock:
            self._positions = new_positions

        logger.info("Synced %d option positions from broker", len(new_positions))

    def _update_greeks(self, entry: OptionPositionEntry, contract: Any) -> None:
        """Query greeks from IBKR for a single position."""
        if self._ib is None:
            return

        try:
            ticker = self._ib.reqMktData(contract, genericTickList="106")
            self._ib.sleep(2)  # Brief wait for data

            model = getattr(ticker, "modelGreeks", None)
            if model is not None:
                entry.greeks = OptionGreeks(
                    delta=float(getattr(model, "delta", 0) or 0),
                    gamma=float(getattr(model, "gamma", 0) or 0),
                    theta=float(getattr(model, "theta", 0) or 0),
                    vega=float(getattr(model, "vega", 0) or 0),
                    implied_vol=float(getattr(model, "impliedVol", 0) or 0),
                    underlying_price=float(getattr(model, "undPrice", 0) or 0),
                )

            self._ib.cancelMktData(contract)
        except Exception as exc:
            logger.debug("Could not get greeks for %s: %s",
                        entry.instrument_id, exc)

    # ── Strategy tagging ─────────────────────────────────────────────

    def tag_strategy(self, instrument_id: str, strategy: str) -> None:
        """Associate an option position with the strategy that opened it."""
        self._strategy_map[instrument_id] = strategy
        with self._lock:
            pos = self._positions.get(instrument_id)
            if pos is not None:
                pos.strategy = strategy

    # ── Queries ──────────────────────────────────────────────────────

    def get_position(self, instrument_id: str) -> Optional[OptionPositionEntry]:
        with self._lock:
            return self._positions.get(instrument_id)

    def get_all_positions(self) -> List[OptionPositionEntry]:
        with self._lock:
            return list(self._positions.values())

    def get_positions_by_strategy(self, strategy: str) -> List[OptionPositionEntry]:
        with self._lock:
            return [p for p in self._positions.values() if p.strategy == strategy]

    def get_positions_as_dicts(self) -> List[Dict[str, Any]]:
        """Return all positions as dicts (for strategy evaluate())."""
        with self._lock:
            return [p.to_dict() for p in self._positions.values()]

    # ── Portfolio-level greeks ────────────────────────────────────────

    def compute_portfolio_greeks(self) -> PortfolioGreeks:
        """Aggregate greeks across all positions."""
        pg = PortfolioGreeks()

        with self._lock:
            for pos in self._positions.values():
                g = pos.greeks
                mult = pos.multiplier * pos.quantity

                pg.total_delta += g.delta * mult
                pg.total_gamma += g.gamma * mult
                pg.total_theta += g.theta * mult
                pg.total_vega += g.vega * mult
                pg.total_notional += pos.notional

                if pos.is_long:
                    pg.long_count += 1
                else:
                    pg.short_count += 1

        return pg

    @property
    def total_delta(self) -> float:
        """Net delta in share-equivalents."""
        return self.compute_portfolio_greeks().total_delta

    @property
    def total_theta(self) -> float:
        """Daily theta in dollars."""
        return self.compute_portfolio_greeks().total_theta

    # ── Expiry calendar ──────────────────────────────────────────────

    def get_expiring_positions(
        self,
        days: Optional[int] = None,
    ) -> List[ExpiryAlert]:
        """Return positions approaching expiry.

        Parameters
        ----------
        days : int, optional
            DTE threshold.  Defaults to ``warning_dte``.
        """
        threshold = days or self._warning_dte
        alerts: List[ExpiryAlert] = []

        with self._lock:
            for pos in self._positions.values():
                dte = pos.dte
                if dte <= self._critical_dte:
                    alerts.append(ExpiryAlert(pos, dte, "CRITICAL"))
                elif dte <= threshold:
                    alerts.append(ExpiryAlert(pos, dte, "WARNING"))

        alerts.sort(key=lambda a: a.dte)
        return alerts

    def get_expiry_calendar(self) -> Dict[str, List[OptionPositionEntry]]:
        """Group positions by expiry date."""
        calendar: Dict[str, List[OptionPositionEntry]] = {}
        with self._lock:
            for pos in self._positions.values():
                calendar.setdefault(pos.expiry, []).append(pos)
        return dict(sorted(calendar.items()))

    # ── Daily P&L attribution ────────────────────────────────────────

    def compute_pnl_attribution(
        self,
        prev_underlying_prices: Dict[str, float],
    ) -> Dict[str, Dict[str, float]]:
        """Decompose P&L into delta, theta, vega components.

        Parameters
        ----------
        prev_underlying_prices : dict
            Previous day's underlying prices keyed by symbol.

        Returns
        -------
        dict
            instrument_id → {delta_pnl, theta_pnl, vega_pnl, total_pnl}
        """
        attribution: Dict[str, Dict[str, float]] = {}

        with self._lock:
            for pos in self._positions.values():
                g = pos.greeks
                prev_price = prev_underlying_prices.get(pos.symbol, 0.0)
                curr_price = g.underlying_price

                if prev_price <= 0 or curr_price <= 0:
                    continue

                price_change = curr_price - prev_price
                mult = pos.multiplier * pos.quantity

                delta_pnl = g.delta * price_change * mult
                theta_pnl = g.theta * mult  # Already daily
                # Vega P&L requires IV change — approximate as 0 for now
                vega_pnl = 0.0

                attribution[pos.instrument_id] = {
                    "delta_pnl": delta_pnl,
                    "theta_pnl": theta_pnl,
                    "vega_pnl": vega_pnl,
                    "total_pnl": delta_pnl + theta_pnl + vega_pnl,
                    "unrealized_pnl": pos.unrealized_pnl,
                }

        return attribution

    # ── Margin check ─────────────────────────────────────────────────

    def check_margin(self, order: Order) -> MarginEstimate:
        """Pre-trade margin check using IBKR whatIf=True.

        Parameters
        ----------
        order : Order
            The proposed option order.

        Returns
        -------
        MarginEstimate
        """
        if self._ib is None:
            return MarginEstimate(
                approved=False,
                reason="No IB connection for margin check",
            )

        try:
            from prometheus.execution.ib_compat import LimitOrder, MarketOrder

            mapper = InstrumentMapper()
            contract = mapper.get_contract(order.instrument_id)

            # Qualify
            qualified = self._ib.qualifyContracts(contract)
            if qualified:
                contract = qualified[0]

            action = "BUY" if order.side.value == "BUY" else "SELL"

            if order.limit_price is not None:
                ib_order = LimitOrder(action, order.quantity, order.limit_price)
            else:
                ib_order = MarketOrder(action, order.quantity)

            # whatIf
            what_if = self._ib.whatIfOrder(contract, ib_order)

            init_margin = float(getattr(what_if, "initMarginChange", 0) or 0)
            maint_margin = float(getattr(what_if, "maintMarginChange", 0) or 0)
            equity = float(getattr(what_if, "equityWithLoanAfter", 0) or 0)
            commission = float(getattr(what_if, "commission", 0) or 0)

            approved = equity > 0 and init_margin < equity * 0.8

            return MarginEstimate(
                init_margin_change=init_margin,
                maint_margin_change=maint_margin,
                equity_with_loan=equity,
                commission=commission,
                approved=approved,
                reason="" if approved else "Insufficient margin",
            )

        except Exception as exc:
            logger.error("Margin check failed for %s: %s",
                        order.instrument_id, exc)
            return MarginEstimate(
                approved=False,
                reason=f"Margin check error: {exc}",
            )

    # ── FOP / futures convenience ─────────────────────────────────────

    def get_fop_positions(self) -> List[OptionPositionEntry]:
        """Return only futures-option (FOP) positions."""
        with self._lock:
            return [
                p for p in self._positions.values()
                if p.metadata.get("sec_type") == "FOP"
            ]

    def get_equity_option_positions(self) -> List[OptionPositionEntry]:
        """Return only equity option (OPT) positions."""
        with self._lock:
            return [
                p for p in self._positions.values()
                if p.metadata.get("sec_type", "OPT") == "OPT"
            ]

    def get_positions_by_underlying(self, symbol: str) -> List[OptionPositionEntry]:
        """Return all option/FOP positions for a given underlying."""
        with self._lock:
            return [p for p in self._positions.values() if p.symbol == symbol]

    # ── Per-strategy greeks ────────────────────────────────────────────

    def compute_greeks_by_strategy(self) -> Dict[str, PortfolioGreeks]:
        """Aggregate greeks grouped by strategy."""
        by_strategy: Dict[str, PortfolioGreeks] = {}

        with self._lock:
            for pos in self._positions.values():
                strat = pos.strategy or "_untagged"
                pg = by_strategy.setdefault(strat, PortfolioGreeks())
                g = pos.greeks
                mult = pos.multiplier * pos.quantity

                pg.total_delta += g.delta * mult
                pg.total_gamma += g.gamma * mult
                pg.total_theta += g.theta * mult
                pg.total_vega += g.vega * mult
                pg.total_notional += pos.notional
                if pos.is_long:
                    pg.long_count += 1
                else:
                    pg.short_count += 1

        return by_strategy

    # ── Greeks budget checking ────────────────────────────────────────

    def check_greeks_budget(
        self,
        nav: float,
        config: Optional[GreeksBudgetConfig] = None,
    ) -> GreeksUtilisation:
        """Check current greeks against portfolio-level budget.

        Parameters
        ----------
        nav : float
            Current portfolio NAV (for delta limit calculation).
        config : GreeksBudgetConfig, optional
            Override default budget limits.

        Returns
        -------
        GreeksUtilisation
        """
        cfg = config or GreeksBudgetConfig()
        pg = self.compute_portfolio_greeks()

        delta_limit = nav * cfg.max_delta_pct
        gamma_limit = cfg.max_gamma
        theta_limit = cfg.min_theta
        vega_limit = cfg.max_vega

        delta_pct = abs(pg.total_delta) / max(delta_limit, 1.0)
        gamma_pct = abs(pg.total_gamma) / max(gamma_limit, 1.0)
        theta_pct = pg.total_theta / min(theta_limit, -1.0) if theta_limit < 0 else 0.0
        vega_pct = abs(pg.total_vega) / max(vega_limit, 1.0)

        within_budget = (
            abs(pg.total_delta) <= delta_limit
            and abs(pg.total_gamma) <= gamma_limit
            and pg.total_theta >= theta_limit
            and abs(pg.total_vega) <= vega_limit
        )

        return GreeksUtilisation(
            delta_used=round(pg.total_delta, 2),
            delta_limit=round(delta_limit, 2),
            delta_pct=round(delta_pct, 4),
            gamma_used=round(pg.total_gamma, 4),
            gamma_limit=round(gamma_limit, 2),
            gamma_pct=round(gamma_pct, 4),
            theta_used=round(pg.total_theta, 2),
            theta_limit=round(theta_limit, 2),
            theta_pct=round(theta_pct, 4),
            vega_used=round(pg.total_vega, 2),
            vega_limit=round(vega_limit, 2),
            vega_pct=round(vega_pct, 4),
            within_budget=within_budget,
        )

    def get_greeks_utilisation_summary(
        self,
        nav: float,
        config: Optional[GreeksBudgetConfig] = None,
    ) -> Dict[str, Any]:
        """Return a human-readable greeks utilisation summary."""
        util = self.check_greeks_budget(nav, config)
        return {
            "within_budget": util.within_budget,
            "delta": f"{util.delta_used:,.0f} / {util.delta_limit:,.0f} ({util.delta_pct:.0%})",
            "gamma": f"{util.gamma_used:,.2f} / {util.gamma_limit:,.0f} ({util.gamma_pct:.0%})",
            "theta": f"${util.theta_used:,.0f} / ${util.theta_limit:,.0f} ({util.theta_pct:.0%})",
            "vega": f"{util.vega_used:,.0f} / {util.vega_limit:,.0f} ({util.vega_pct:.0%})",
        }

    # ── Status / diagnostics ─────────────────────────────────────────

    def get_status(self) -> Dict[str, Any]:
        """Return portfolio status summary."""
        pg = self.compute_portfolio_greeks()
        alerts = self.get_expiring_positions()

        fop_count = len(self.get_fop_positions())
        opt_count = len(self._positions) - fop_count

        return {
            "total_positions": len(self._positions),
            "opt_positions": opt_count,
            "fop_positions": fop_count,
            "long_positions": pg.long_count,
            "short_positions": pg.short_count,
            "total_delta": round(pg.total_delta, 2),
            "total_gamma": round(pg.total_gamma, 4),
            "total_theta": round(pg.total_theta, 2),
            "total_vega": round(pg.total_vega, 2),
            "total_notional": round(pg.total_notional, 0),
            "expiry_warnings": len([a for a in alerts if a.level == "WARNING"]),
            "expiry_critical": len([a for a in alerts if a.level == "CRITICAL"]),
        }


__all__ = [
    "OptionGreeks",
    "OptionPositionEntry",
    "ExpiryAlert",
    "PortfolioGreeks",
    "GreeksBudgetConfig",
    "GreeksUtilisation",
    "MarginEstimate",
    "OptionsPortfolio",
]
