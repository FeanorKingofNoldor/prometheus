"""Prometheus v2 – Futures Position & Roll Manager.

Tracks open futures positions, manages roll schedules, and monitors margin
utilisation.  Integrates with :class:`ContractDiscoveryService` to find
the next contract in the chain and with :class:`BrokerInterface` to
submit roll orders.

Supported products (initial set)::

    ES   E-mini S&P 500      CME     quarterly (Mar/Jun/Sep/Dec)
    NQ   E-mini Nasdaq 100   CME     quarterly
    VX   VIX futures         CFE     monthly
    ZB   US Treasury Bond    CBOT    quarterly
    ZN   10-Year T-Note      CBOT    quarterly
    GC   Gold                COMEX   bi-monthly (Feb/Apr/Jun/Aug/Oct/Dec)
    CL   Crude Oil           NYMEX   monthly

Usage::

    from prometheus.execution.futures_manager import FuturesManager

    mgr = FuturesManager(discovery, broker)
    mgr.sync_positions(broker_positions)
    rolls = mgr.check_rolls()
    for roll in rolls:
        mgr.execute_roll(roll)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional

from apathis.core.logging import get_logger

logger = get_logger(__name__)


# ── Product definitions ──────────────────────────────────────────────

class RollFrequency(str, Enum):
    MONTHLY = "MONTHLY"
    QUARTERLY = "QUARTERLY"
    BI_MONTHLY = "BI_MONTHLY"


@dataclass(frozen=True)
class FuturesProduct:
    """Static metadata for a futures product."""

    symbol: str
    exchange: str
    currency: str
    multiplier: float          # Dollar value per point
    roll_frequency: RollFrequency
    roll_days_before_expiry: int  # Start rolling N days before last trade date
    # Monthly codes: F=Jan G=Feb H=Mar J=Apr K=May M=Jun
    #                N=Jul Q=Aug U=Sep V=Oct X=Nov Z=Dec
    active_months: List[int]   # 1-indexed months with active contracts
    tick_size: float           # Minimum price increment
    description: str = ""


# Pre-configured products
PRODUCTS: Dict[str, FuturesProduct] = {
    "ES": FuturesProduct(
        symbol="ES", exchange="CME", currency="USD",
        multiplier=50.0, roll_frequency=RollFrequency.QUARTERLY,
        roll_days_before_expiry=8, active_months=[3, 6, 9, 12],
        tick_size=0.25, description="E-mini S&P 500",
    ),
    "NQ": FuturesProduct(
        symbol="NQ", exchange="CME", currency="USD",
        multiplier=20.0, roll_frequency=RollFrequency.QUARTERLY,
        roll_days_before_expiry=8, active_months=[3, 6, 9, 12],
        tick_size=0.25, description="E-mini Nasdaq 100",
    ),
    "VX": FuturesProduct(
        symbol="VX", exchange="CFE", currency="USD",
        multiplier=1000.0, roll_frequency=RollFrequency.MONTHLY,
        roll_days_before_expiry=5, active_months=list(range(1, 13)),
        tick_size=0.05, description="VIX Futures",
    ),
    "ZB": FuturesProduct(
        symbol="ZB", exchange="CBOT", currency="USD",
        multiplier=1000.0, roll_frequency=RollFrequency.QUARTERLY,
        roll_days_before_expiry=5, active_months=[3, 6, 9, 12],
        tick_size=1 / 32, description="US Treasury Bond",
    ),
    "ZN": FuturesProduct(
        symbol="ZN", exchange="CBOT", currency="USD",
        multiplier=1000.0, roll_frequency=RollFrequency.QUARTERLY,
        roll_days_before_expiry=5, active_months=[3, 6, 9, 12],
        tick_size=1 / 64, description="10-Year T-Note",
    ),
    "GC": FuturesProduct(
        symbol="GC", exchange="COMEX", currency="USD",
        multiplier=100.0, roll_frequency=RollFrequency.BI_MONTHLY,
        roll_days_before_expiry=5, active_months=[2, 4, 6, 8, 10, 12],
        tick_size=0.10, description="Gold",
    ),
    "CL": FuturesProduct(
        symbol="CL", exchange="NYMEX", currency="USD",
        multiplier=1000.0, roll_frequency=RollFrequency.MONTHLY,
        roll_days_before_expiry=5, active_months=list(range(1, 13)),
        tick_size=0.01, description="Crude Oil",
    ),
}


# ── Position tracking ─────────────────────────────────────────────────

@dataclass
class FuturesPositionEntry:
    """Tracked state for a single futures position."""

    instrument_id: str          # e.g. "ES_260320.FUT"
    symbol: str                 # Product symbol (e.g. "ES")
    expiry: str                 # YYYYMMDD
    exchange: str
    quantity: int               # Positive = long, negative = short
    avg_cost: float = 0.0      # Average entry price
    market_price: float = 0.0
    market_value: float = 0.0
    unrealized_pnl: float = 0.0
    ibkr_con_id: int = 0
    strategy: str = ""          # Which strategy opened this

    @property
    def dte(self) -> int:
        try:
            exp = datetime.strptime(self.expiry[:8], "%Y%m%d").date()
            return (exp - date.today()).days
        except ValueError:
            return 0

    @property
    def is_long(self) -> bool:
        return self.quantity > 0

    @property
    def is_short(self) -> bool:
        return self.quantity < 0

    @property
    def notional(self) -> float:
        """Approximate notional value."""
        product = PRODUCTS.get(self.symbol)
        mult = product.multiplier if product else 1.0
        return abs(self.quantity) * self.market_price * mult


# ── Roll directives ──────────────────────────────────────────────────

@dataclass
class RollDirective:
    """Instruction to roll a futures position from one contract to the next."""

    position: FuturesPositionEntry
    new_expiry: str             # YYYYMMDD of the next contract
    new_con_id: Optional[int]   # conId of the next contract (if discovered)
    reason: str
    urgency: str = "NORMAL"     # NORMAL or URGENT (within 2 days of expiry)


# ── Margin snapshot ──────────────────────────────────────────────────

@dataclass
class MarginSnapshot:
    """Account margin state."""

    net_liquidation: float = 0.0
    init_margin_req: float = 0.0
    maint_margin_req: float = 0.0
    available_funds: float = 0.0
    excess_liquidity: float = 0.0

    @property
    def init_margin_utilisation(self) -> float:
        """Initial margin utilisation as a fraction of NLV."""
        if self.net_liquidation > 0:
            return self.init_margin_req / self.net_liquidation
        return 0.0

    @property
    def maint_margin_utilisation(self) -> float:
        """Maintenance margin utilisation as a fraction of NLV."""
        if self.net_liquidation > 0:
            return self.maint_margin_req / self.net_liquidation
        return 0.0


# ── Futures Manager ──────────────────────────────────────────────────

class FuturesManager:
    """Manage futures positions, rolls, and margin.

    Parameters
    ----------
    discovery : ContractDiscoveryService
        For discovering next-contract expirations.
    max_margin_utilisation : float
        Maximum initial margin utilisation (as fraction of NLV).
        New positions are blocked above this threshold.
    """

    def __init__(
        self,
        discovery: Any = None,
        *,
        max_margin_utilisation: float = 0.60,
    ) -> None:
        self._discovery = discovery
        self._max_margin_util = max_margin_utilisation

        self._positions: Dict[str, FuturesPositionEntry] = {}
        self._margin: MarginSnapshot = MarginSnapshot()

        # Strategy provenance
        self._strategy_map: Dict[str, str] = {}

    # ── Position sync ─────────────────────────────────────────────────

    def sync_positions(self, broker_positions: Dict[str, Any]) -> None:
        """Sync futures positions from broker position dict.

        Parameters
        ----------
        broker_positions : dict
            instrument_id → Position objects from the broker.
            Only entries ending in ``.FUT`` are consumed.
        """
        new_pos: Dict[str, FuturesPositionEntry] = {}

        for iid, pos in broker_positions.items():
            if not iid.endswith(".FUT"):
                continue

            # Parse instrument_id: "ES_260320.FUT"
            base = iid.replace(".FUT", "")
            parts = base.split("_")
            if len(parts) < 2:
                continue

            symbol = parts[0]
            expiry_short = parts[1]
            expiry = f"20{expiry_short}" if len(expiry_short) == 6 else expiry_short

            product = PRODUCTS.get(symbol)
            exchange = product.exchange if product else ""

            entry = FuturesPositionEntry(
                instrument_id=iid,
                symbol=symbol,
                expiry=expiry,
                exchange=exchange,
                quantity=int(pos.quantity),
                avg_cost=pos.avg_cost,
                market_value=pos.market_value,
                unrealized_pnl=pos.unrealized_pnl,
                strategy=self._strategy_map.get(iid, ""),
            )

            new_pos[iid] = entry

        self._positions = new_pos
        logger.info("Synced %d futures positions", len(new_pos))

    def sync_margin(self, account_state: Dict[str, Any]) -> None:
        """Update margin snapshot from account state dict."""
        self._margin = MarginSnapshot(
            net_liquidation=float(account_state.get("NetLiquidation", 0)),
            init_margin_req=float(account_state.get("InitMarginReq", 0)),
            maint_margin_req=float(account_state.get("MaintMarginReq", 0)),
            available_funds=float(account_state.get("AvailableFunds", 0)),
            excess_liquidity=float(account_state.get("ExcessLiquidity", 0)),
        )

    # ── Roll management ───────────────────────────────────────────────

    def check_rolls(self) -> List[RollDirective]:
        """Check all positions for required rolls.

        Returns a list of :class:`RollDirective` for positions that
        are within the product's ``roll_days_before_expiry`` window.
        """
        directives: List[RollDirective] = []

        for pos in self._positions.values():
            product = PRODUCTS.get(pos.symbol)
            if product is None:
                continue

            if pos.dte <= 0:
                # Already expired — urgent
                next_exp = self._find_next_expiry(product, pos.expiry)
                if next_exp:
                    directives.append(RollDirective(
                        position=pos,
                        new_expiry=next_exp,
                        new_con_id=None,
                        reason=f"EXPIRED: {pos.instrument_id} has {pos.dte} DTE",
                        urgency="URGENT",
                    ))
                continue

            if pos.dte <= product.roll_days_before_expiry:
                urgency = "URGENT" if pos.dte <= 2 else "NORMAL"
                next_exp = self._find_next_expiry(product, pos.expiry)
                if next_exp:
                    # Try to get conId from discovery
                    con_id = None
                    if self._discovery is not None:
                        fc = self._discovery.build_future_contract(
                            pos.symbol, next_exp, product.exchange,
                            qualify=False,
                        )
                        # We'll qualify at execution time
                        if fc is not None:
                            con_id = getattr(fc, "conId", None)

                    directives.append(RollDirective(
                        position=pos,
                        new_expiry=next_exp,
                        new_con_id=con_id,
                        reason=(
                            f"Roll {pos.instrument_id}: {pos.dte} DTE "
                            f"(threshold={product.roll_days_before_expiry})"
                        ),
                        urgency=urgency,
                    ))

        if directives:
            logger.info(
                "Roll check: %d position(s) need rolling: %s",
                len(directives),
                ", ".join(d.position.instrument_id for d in directives),
            )

        return directives

    def create_roll_orders(
        self,
        directive: RollDirective,
    ) -> List[Dict[str, Any]]:
        """Create close + open orders for a roll.

        Returns a list of order-spec dicts that can be converted to
        ``Order`` objects by the caller.

        Each dict has keys: instrument_id, side, quantity, order_type, metadata.
        """
        pos = directive.position
        product = PRODUCTS.get(pos.symbol)
        if product is None:
            return []

        orders = []

        # Close the current position
        close_side = "SELL" if pos.is_long else "BUY"
        close_qty = abs(pos.quantity)

        orders.append({
            "instrument_id": pos.instrument_id,
            "side": close_side,
            "quantity": close_qty,
            "order_type": "MARKET",
            "metadata": {
                "action": "ROLL_CLOSE",
                "roll_reason": directive.reason,
                "urgency": directive.urgency,
            },
        })

        # Open the new position
        open_side = "BUY" if pos.is_long else "SELL"
        expiry_short = directive.new_expiry[2:] if len(directive.new_expiry) == 8 else directive.new_expiry
        new_iid = f"{pos.symbol}_{expiry_short}.FUT"

        orders.append({
            "instrument_id": new_iid,
            "side": open_side,
            "quantity": close_qty,
            "order_type": "MARKET",
            "metadata": {
                "action": "ROLL_OPEN",
                "roll_reason": directive.reason,
                "new_expiry": directive.new_expiry,
            },
        })

        logger.info(
            "Created roll orders for %s -> %s (%d contracts, %s)",
            pos.instrument_id, new_iid, close_qty, directive.urgency,
        )

        return orders

    # ── Margin checks ─────────────────────────────────────────────────

    @property
    def margin(self) -> MarginSnapshot:
        return self._margin

    def can_open_position(self, estimated_margin: float = 0.0) -> bool:
        """Check if there's room to open a new futures position.

        Parameters
        ----------
        estimated_margin : float
            Estimated initial margin for the proposed position.

        Returns
        -------
        bool
        """
        nlv = self._margin.net_liquidation
        if nlv <= 0:
            return False

        current_util = self._margin.init_margin_utilisation
        new_util = (self._margin.init_margin_req + estimated_margin) / nlv

        if new_util > self._max_margin_util:
            logger.warning(
                "Margin check failed: current=%.1f%%, proposed=%.1f%%, max=%.1f%%",
                current_util * 100, new_util * 100, self._max_margin_util * 100,
            )
            return False

        return True

    # ── Queries ───────────────────────────────────────────────────────

    def get_position(self, instrument_id: str) -> Optional[FuturesPositionEntry]:
        return self._positions.get(instrument_id)

    def get_all_positions(self) -> List[FuturesPositionEntry]:
        return list(self._positions.values())

    def get_positions_by_symbol(self, symbol: str) -> List[FuturesPositionEntry]:
        return [p for p in self._positions.values() if p.symbol == symbol]

    def get_total_notional(self) -> float:
        """Total absolute notional across all futures positions."""
        return sum(p.notional for p in self._positions.values())

    def get_net_delta_equivalent(self) -> Dict[str, float]:
        """Net delta-equivalent exposure per product (in dollars).

        For equity-index futures (ES, NQ), this is a meaningful proxy
        for portfolio beta contribution.
        """
        deltas: Dict[str, float] = {}
        for pos in self._positions.values():
            product = PRODUCTS.get(pos.symbol)
            if product is None:
                continue
            delta = pos.quantity * pos.market_price * product.multiplier
            deltas[pos.symbol] = deltas.get(pos.symbol, 0.0) + delta
        return deltas

    def tag_strategy(self, instrument_id: str, strategy: str) -> None:
        """Associate a futures position with the strategy that opened it."""
        self._strategy_map[instrument_id] = strategy
        pos = self._positions.get(instrument_id)
        if pos is not None:
            pos.strategy = strategy

    # ── Internal helpers ──────────────────────────────────────────────

    @staticmethod
    def _find_next_expiry(product: FuturesProduct, current_expiry: str) -> Optional[str]:
        """Find the next standard expiry after ``current_expiry``.

        Uses the product's ``active_months`` to determine valid contract
        months and returns YYYYMMDD for the 3rd Friday (approximate —
        actual expiry varies by exchange).
        """
        try:
            current = datetime.strptime(current_expiry[:8], "%Y%m%d").date()
        except ValueError:
            return None

        # Start searching from the month after current expiry
        search_date = current.replace(day=1) + timedelta(days=32)
        search_date = search_date.replace(day=1)

        for _ in range(24):  # Search up to 2 years ahead
            if search_date.month in product.active_months:
                # Approximate expiry: 3rd Friday of the month
                first_day = search_date.replace(day=1)
                days_to_friday = (4 - first_day.weekday()) % 7
                first_friday = first_day + timedelta(days=days_to_friday)
                third_friday = first_friday + timedelta(weeks=2)
                return third_friday.strftime("%Y%m%d")

            # Next month
            if search_date.month == 12:
                search_date = search_date.replace(year=search_date.year + 1, month=1)
            else:
                search_date = search_date.replace(month=search_date.month + 1)

        return None

    @staticmethod
    def get_product(symbol: str) -> Optional[FuturesProduct]:
        """Look up a futures product by symbol."""
        return PRODUCTS.get(symbol)


__all__ = [
    "FuturesManager",
    "FuturesPositionEntry",
    "FuturesProduct",
    "MarginSnapshot",
    "RollDirective",
    "PRODUCTS",
]
