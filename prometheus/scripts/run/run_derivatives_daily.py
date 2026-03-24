"""Prometheus v2 – Daily Derivatives Orchestrator.

Connects to IBKR, syncs equity + option + futures positions, refreshes
market signals, runs all derivative strategies, applies risk checks,
and submits orders.

Usage (paper trading)::

    python -m prometheus.scripts.run.run_derivatives_daily \
        --paper --dry-run

Usage (live)::

    python -m prometheus.scripts.run.run_derivatives_daily \
        --port 4001 --account U1234567

Steps
-----
1. Connect to IBKR (paper or live).
2. Sync positions: equities, options, futures.
3. Refresh market signals: FRAG, STAB, MHI, lambda, VIX, ES price.
4. Check futures rolls.
5. Run all derivative strategies → collect ``OptionTradeDirective`` list.
6. Apply risk checks (margin, position limits, greeks limits).
7. Submit orders (or log in dry-run mode).
8. Log activity.
"""

from __future__ import annotations

import argparse
import math
import re
import sys
from datetime import date
from typing import Any, Dict, Optional, Sequence

from apathis.core.logging import get_logger

logger = get_logger(__name__)


# ── Signal loader (stub — wired to real pipelines in production) ──────

def _load_signals(
    ib: Any,
    account_state: Dict[str, Any],
    *,
    positions: Dict[str, Any],
) -> Dict[str, Any]:
    """Assemble the signals dict consumed by all strategies.

    Fetching priority for live prices:
      1. IBKR streaming delayed data (reqMarketDataType=3, non-competing)
      2. DB ``prices_daily`` table (SPY.US / VIX.INDX) as reliable fallback
      3. Hardcoded defaults if both sources fail

    Using streaming instead of reqTickers (snapshot) avoids Error 10197
    "competing live session" that occurs when another TWS/Gateway session
    holds the market data subscription lock.
    """
    nav = float(account_state.get("NetLiquidation", 0))

    signals: Dict[str, Any] = {
        # Portfolio
        "nav": nav,
        "buying_power": float(account_state.get("AvailableFunds", nav)),
        "market_state": "NEUTRAL",
        # Health indices (real pipeline fills these)
        "mhi": 1.0,
        "frag": 0.0,
        # VIX
        "vix_level": 20.0,
        # Equity-index prices
        "spy_price": 0.0,
        "es_price": 0.0,
        # Lambda / STAB scores (keyed by symbol)
        "lambda_scores": {},
        "lambda_aggregate": 0.0,
        "stab_scores": {},
        # Sector
        "sector_shi": {},
        "sector_exposures": {},
        "etf_prices": {},
        # Futures positions (passed to FuturesOverlayStrategy)
        "futures_positions": {},
        # Equity prices (for ShortPutStrategy)
        "equity_prices": {},
    }

    def _valid_price(v: Any) -> Optional[float]:
        """Return float if v is a real positive price, else None."""
        try:
            fv = float(v)
            if fv > 0 and not math.isnan(fv):
                return fv
        except (TypeError, ValueError):
            pass
        return None

    def _fetch_streaming(contract: Any, timeout: float = 5.0) -> Optional[float]:
        """Subscribe to delayed streaming data and wait *timeout* seconds.

        Uses ``reqMarketDataType(3)`` (delayed) which does not compete with
        a live session, so avoids Error 10197.
        """
        try:
            qualified = ib.qualifyContracts(contract)
            if not qualified:
                return None
            ticker = ib.reqMktData(qualified[0], "", False, False)
            ib.sleep(timeout)
            for attr in ("last", "close", "bid", "ask"):
                p = _valid_price(getattr(ticker, attr, None))
                if p is not None:
                    return p
            ib.cancelMktData(qualified[0])
        except Exception as exc:
            logger.debug("IBKR streaming fetch error for %s: %s",
                         getattr(contract, "symbol", str(contract)), exc)
        return None

    def _db_price(instrument_id: str) -> Optional[float]:
        """Fetch the most recent close from ``prices_daily`` in the historical DB."""
        try:
            from apathis.core.database import get_db_manager
            db = get_db_manager()
            with db.get_historical_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT close, trade_date FROM prices_daily "
                        "WHERE instrument_id=%s ORDER BY trade_date DESC LIMIT 1",
                        (instrument_id,),
                    )
                    row = cur.fetchone()
                    if row:
                        p = _valid_price(row[0])
                        if p is not None:
                            logger.info(
                                "DB price for %s: %.4f (as of %s)",
                                instrument_id, p, row[1],
                            )
                            return p
        except Exception as exc:
            logger.debug("DB price lookup failed for %s: %s", instrument_id, exc)
        return None

    # Request delayed data type once — applies to all subsequent reqMktData calls.
    # Type 3 = delayed (15-20 min) which is independent of live session.
    try:
        ib.reqMarketDataType(3)
    except Exception:
        pass

    # ── VIX ────────────────────────────────────────────────────────────
    from prometheus.execution.ib_compat import Index
    vix_ibkr = _fetch_streaming(Index("VIX", "CBOE", "USD"))
    if vix_ibkr is not None:
        signals["vix_level"] = vix_ibkr
        logger.info("VIX from IBKR: %.2f", vix_ibkr)
    else:
        vix_db = _db_price("VIX.INDX")
        if vix_db is not None:
            signals["vix_level"] = vix_db
        else:
            logger.warning("VIX unavailable from IBKR and DB — using default %.1f",
                           signals["vix_level"])

    # ── SPY price ───────────────────────────────────────────────────────
    from prometheus.execution.ib_compat import Stock
    spy_ibkr = _fetch_streaming(Stock("SPY", "ARCA", "USD"))
    if spy_ibkr is not None:
        signals["spy_price"] = spy_ibkr
        logger.info("SPY from IBKR: %.2f", spy_ibkr)
    else:
        spy_db = _db_price("SPY.US")
        if spy_db is not None:
            signals["spy_price"] = spy_db
        else:
            logger.warning("SPY price unavailable from IBKR and DB")

    # ── ES price ────────────────────────────────────────────────────────
    try:
        from prometheus.execution.futures_manager import PRODUCTS
        from prometheus.execution.ib_compat import Future
        if PRODUCTS.get("ES"):
            es_contract = Future("ES", exchange="CME", currency="USD")
            es_contract.secType = "CONTFUT"
            es_ibkr = _fetch_streaming(es_contract)
            if es_ibkr is not None:
                signals["es_price"] = es_ibkr
                logger.info("ES from IBKR: %.2f", es_ibkr)
                # Use ES/10 as secondary SPY proxy only if SPY is still unset
                if signals["spy_price"] == 0.0:
                    signals["spy_price"] = es_ibkr / 10
    except Exception as exc:
        logger.debug("Could not fetch live ES price: %s", exc)

    # Derive ES from SPY if ES is still missing
    if signals["es_price"] == 0.0 and signals["spy_price"] > 0:
        signals["es_price"] = signals["spy_price"] * 10
        logger.debug("ES estimated from SPY×10: %.1f", signals["es_price"])

    # ── Equity prices from live portfolio positions ─────────────────────
    for iid, pos in positions.items():
        if iid.endswith(".US") or (not iid.endswith(".FUT") and "_" not in iid):
            symbol = iid.replace(".US", "").split(".")[0]
            qty = getattr(pos, "quantity", 0) or 0
            mv = getattr(pos, "market_value", 0) or 0
            if qty > 0 and mv > 0:
                signals["equity_prices"][symbol] = mv / qty

    # ── Sector Health Index (SHI) from runtime DB ────────────────────
    # Used by SectorPutSpreadStrategy and CrisisAlphaStrategy.
    try:
        from apathis.core.database import get_db_manager as _get_db
        _db = _get_db()
        with _db.get_runtime_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT sector_name, score
                    FROM sector_health_daily
                    WHERE as_of_date = (SELECT MAX(as_of_date) FROM sector_health_daily)
                """)
                for sector_name, score in cur.fetchall():
                    signals["sector_shi"][str(sector_name)] = float(score)
        if signals["sector_shi"]:
            logger.info("Loaded SHI for %d sectors", len(signals["sector_shi"]))
    except Exception as exc:
        logger.warning("Failed to load sector SHI: %s", exc)

    return signals


# ── Risk checks ───────────────────────────────────────────────────────

def _apply_risk_checks(
    directives: list,
    margin_snapshot: Any,
    portfolio_greeks: Any,
    *,
    max_margin_util: float = 0.60,
    max_total_delta: float = 500_000.0,
    max_total_theta: float = -5_000.0,
) -> list:
    """Filter directives through risk checks.

    Returns only directives that pass all checks.
    """
    approved: list = []

    current_margin_util = 0.0
    if margin_snapshot and margin_snapshot.net_liquidation > 0:
        current_margin_util = margin_snapshot.init_margin_utilisation

    for d in directives:
        # Skip margin-intensive trades when utilisation is high
        if current_margin_util > max_margin_util:
            if d.action.value in ("OPEN", "HEDGE"):
                logger.warning(
                    "Risk: blocking %s %s — margin utilisation %.1f%% > %.1f%%",
                    d.action.value, d.symbol,
                    current_margin_util * 100, max_margin_util * 100,
                )
                continue

        # Greeks limits
        if portfolio_greeks:
            if abs(portfolio_greeks.total_delta) > max_total_delta:
                if d.action.value == "OPEN":
                    logger.warning(
                        "Risk: blocking OPEN %s — portfolio delta %.0f > %.0f limit",
                        d.symbol, abs(portfolio_greeks.total_delta), max_total_delta,
                    )
                    continue

            if portfolio_greeks.total_theta < max_total_theta:
                if d.quantity < 0:  # Selling options adds negative theta
                    logger.warning(
                        "Risk: blocking short %s — portfolio theta $%.0f < $%.0f limit",
                        d.symbol, portfolio_greeks.total_theta, max_total_theta,
                    )
                    continue

        approved.append(d)

    blocked = len(directives) - len(approved)
    if blocked > 0:
        logger.info("Risk: approved %d / %d directives (%d blocked)",
                     len(approved), len(directives), blocked)

    return approved


# ── Main orchestrator ─────────────────────────────────────────────────

def run_derivatives_daily(
    *,
    host: str = "127.0.0.1",
    port: int = 4002,
    client_id: int = 10,
    account: str = "",
    dry_run: bool = True,
    max_margin_util: float = 0.60,
) -> Dict[str, Any]:
    """Run the full daily derivatives pipeline.

    Returns a summary dict with counts and diagnostics.
    """
    from prometheus.execution.broker_interface import BrokerInterface
    from prometheus.execution.contract_discovery import ContractDiscoveryService
    from prometheus.execution.futures_manager import FuturesManager
    from prometheus.execution.ib_compat import IB
    from prometheus.execution.instrument_mapper import InstrumentMapper
    from prometheus.execution.options_portfolio import OptionsPortfolio
    from prometheus.execution.options_strategy import OptionsStrategyManager

    summary: Dict[str, Any] = {
        "date": date.today().isoformat(),
        "dry_run": dry_run,
        "steps_completed": [],
        "errors": [],
    }

    ib = IB()

    # ── Step 1: Connect ───────────────────────────────────────────────
    try:
        logger.info("Connecting to IBKR at %s:%d (client_id=%d)", host, port, client_id)
        ib.connect(host=host, port=port, clientId=client_id, timeout=30)
        summary["steps_completed"].append("connect")
        logger.info("Connected to IBKR")
    except Exception as exc:
        logger.error("Failed to connect to IBKR: %s", exc)
        summary["errors"].append(f"connect: {exc}")
        return summary

    try:
        # ── Step 2: Sync positions & account ──────────────────────────
        logger.info("Syncing positions and account state...")

        # Brief pause so IBKR streams account values before we read them.
        # Without this, accountValues() often returns an empty list immediately
        # after connect() and NAV comes out as $0.
        ib.sleep(2)

        account_values = ib.accountValues()
        account_state: Dict[str, Any] = {}
        for av in account_values:
            # Accept USD and empty-currency tags (always); also accept BASE
            # currency which is present when the account base currency is not
            # USD (e.g. CHF paper accounts).
            if av.currency in ("USD", "BASE", ""):
                account_state[av.tag] = av.value
            # For key financial metrics, accept any currency as a last resort
            # (covers e.g. NetLiquidation [CHF] on CHF-base accounts).
            elif av.tag in ("NetLiquidation", "TotalCashValue",
                            "AvailableFunds", "BuyingPower") \
                    and av.tag not in account_state:
                account_state[av.tag] = av.value
        # NetLiquidationByCurrency [BASE] is the canonical cross-currency NAV;
        # alias it to NetLiquidation if the direct tag wasn't found.
        if "NetLiquidation" not in account_state \
                and "NetLiquidationByCurrency" in account_state:
            account_state["NetLiquidation"] = account_state["NetLiquidationByCurrency"]

        # Fallback: if NetLiquidation still missing, sum portfolio market values
        if not account_state.get("NetLiquidation"):
            portfolio_items = ib.portfolio()
            if portfolio_items:
                total_mv = sum(abs(float(getattr(item, "marketValue", 0) or 0))
                               for item in portfolio_items)
                if total_mv > 0:
                    account_state["NetLiquidation"] = str(total_mv)
                    logger.info("NAV computed from portfolio market values: $%.0f", total_mv)

        # Get all positions and convert to internal Position dataclass.
        # Strategies expect Position.quantity / .market_value / etc. (broker_interface.py),
        # not the raw ib_insync Position namedtuple (.position / .avgCost).
        # Market values come from ib.portfolio() which streams per-position MV.
        raw_positions = ib.positions()
        portfolio_mv: Dict[int, float] = {}  # conId → market_value
        portfolio_unreal: Dict[int, float] = {}
        for item in ib.portfolio():
            con_id = getattr(item.contract, "conId", None)
            if con_id:
                portfolio_mv[con_id] = float(getattr(item, "marketValue", 0) or 0)
                portfolio_unreal[con_id] = float(getattr(item, "unrealizedPNL", 0) or 0)

        from prometheus.execution.broker_interface import Position as InternalPosition
        positions: Dict[str, Any] = {}
        for p in raw_positions:
            contract = p.contract
            iid = InstrumentMapper.contract_to_instrument_id(contract)
            con_id = getattr(contract, "conId", None)
            positions[iid] = InternalPosition(
                instrument_id=iid,
                quantity=float(p.position),
                avg_cost=float(p.avgCost),
                market_value=portfolio_mv.get(con_id, 0.0),
                unrealized_pnl=portfolio_unreal.get(con_id, 0.0),
            )

        summary["position_count"] = len(positions)
        summary["steps_completed"].append("sync_positions")

        # ── Step 3: Initialize services ───────────────────────────────
        discovery = ContractDiscoveryService(ib)
        mapper = InstrumentMapper()

        futures_mgr = FuturesManager(discovery, max_margin_utilisation=max_margin_util)
        futures_mgr.sync_positions(positions)
        futures_mgr.sync_margin(account_state)

        options_portfolio = OptionsPortfolio(ib)
        options_portfolio.sync(broker_positions=positions)

        summary["steps_completed"].append("init_services")

        # ── Step 4: Load signals ──────────────────────────────────────
        signals = _load_signals(ib, account_state, positions=positions)
        summary["nav"] = signals["nav"]
        summary["vix"] = signals["vix_level"]
        summary["es_price"] = signals["es_price"]
        summary["steps_completed"].append("load_signals")

        # ── Step 5: Check futures rolls ───────────────────────────────
        roll_directives = futures_mgr.check_rolls()
        roll_orders: list = []
        for rd in roll_directives:
            orders = futures_mgr.create_roll_orders(rd)
            roll_orders.extend(orders)

        summary["roll_directives"] = len(roll_directives)
        summary["roll_orders"] = len(roll_orders)
        summary["steps_completed"].append("check_rolls")

        if roll_directives:
            logger.info(
                "Futures rolls needed: %d positions, %d orders",
                len(roll_directives), len(roll_orders),
            )
            if dry_run:
                for ro in roll_orders:
                    logger.info("[DRY RUN] Roll order: %s", ro)
            # In live mode, roll orders are submitted separately
            # (they bypass the strategy manager)

        # ── Step 5.5: Compute market situation & allocations ─────────
        logger.info("Computing market situation and strategy allocations...")

        from prometheus.execution.position_lifecycle import PositionLifecycleManager
        from prometheus.execution.strategy_allocator import StrategyAllocator

        # Determine market situation from signals
        market_state = signals.get("market_state", "NEUTRAL")

        allocator = StrategyAllocator()
        portfolio_greeks = options_portfolio.compute_portfolio_greeks()
        existing_options = options_portfolio.get_positions_as_dicts()

        # Inject derivatives-budget cap signals — mirrors options_backtest.py
        # _build_signals() exactly.  Without this, butterfly/condor margin cap
        # defaults to AvailableFunds (full account BP) and margin_used = 0,
        # making the book-level cap a no-op.
        _spread_strats = {"iron_butterfly", "iron_condor"}
        signals["butterfly_condor_margin_used"] = sum(
            abs(opt.get("entry_price", 0)) * abs(opt.get("quantity", 0)) * 100
            for opt in existing_options
            if opt.get("strategy") in _spread_strats and opt.get("quantity", 0) < 0
        )
        # Override buying_power to mean the derivatives budget (NAV × 30%),
        # not the raw IBKR AvailableFunds figure.
        signals["buying_power"] = signals["nav"] * 0.30

        allocations = allocator.allocate(
            market_situation=market_state,
            signals=signals,
            portfolio_greeks=portfolio_greeks,
            existing_positions=existing_options,
        )

        enabled_count = sum(1 for a in allocations.values() if a.enabled)
        summary["market_situation"] = market_state
        summary["strategies_enabled"] = enabled_count
        summary["steps_completed"].append("strategy_allocation")

        # ── Step 6: Run derivative strategies ───────────────────────
        logger.info("Running derivative strategies...")

        # Broker implementations used by the strategy manager.
        # _StubBroker: logs orders (always used inside evaluate_all, which runs
        #              with dry_run=True so it never actually submits).
        # _IbkrDirectBroker: submits real orders via the already-connected `ib`
        #              instance; used in Step 8 when not dry_run.
        class _StubBroker(BrokerInterface):
            """Minimal stub — logs orders instead of submitting."""
            def submit_order(self, order):
                logger.info("[SUBMIT] %s %s x%d", order.side.value, order.instrument_id, order.quantity)
            def cancel_order(self, order_id):
                return False
            def get_positions(self):
                return positions
            def get_order_status(self, order_id):
                return None
            def get_account_state(self):
                return account_state
            def get_fills(self, since=None):
                return []
            def sync(self):
                pass

        class _IbkrDirectBroker(BrokerInterface):
            """Submit option orders via the already-connected ib_insync/ib_async instance."""

            # Instrument-id pattern: SYMBOL_YYMMDD_STRIKEC/P.US
            # e.g.  VIX_260417_32C.US   SPY_260418_560P.US
            _OPT_RE = re.compile(r'^([A-Z0-9]+)_(\d{6}|\d{8})_([\d.]+)([CP])\.US$')

            def submit_order(self, order) -> str:
                from prometheus.execution.broker_interface import OrderSide, OrderType
                from prometheus.execution.ib_compat import (
                    LimitOrder,
                    MarketOrder,
                    Option,
                )

                m = self._OPT_RE.match(order.instrument_id)
                if not m:
                    raise ValueError(
                        f"_IbkrDirectBroker cannot parse instrument_id: "
                        f"{order.instrument_id!r}  (expected SYMBOL_YYMMDD_STRIKE[CP].US)"
                    )

                symbol = m.group(1)
                exp_raw = m.group(2)
                # Accept both YYMMDD (6 digits) and YYYYMMDD (8 digits)
                expiry = "20" + exp_raw if len(exp_raw) == 6 else exp_raw
                strike = float(m.group(3))
                right = m.group(4)  # 'C' or 'P'

                # VIX index options trade on CBOE (not CFE which is for VX futures).
                # Everything else routes through SMART.
                exchange = "CBOE" if symbol == "VIX" else "SMART"

                # VIX options on CBOE require multiplier=100 to uniquely identify
                # the contract (avoids ambiguity with VIX mini-options).
                # IBKR lastTradeDateOrContractMonth = settlement_wednesday - 1 day.
                # Our formula may compute the settlement date rather than the
                # last-trade date, so we try the given expiry and also expiry-1
                # as a fallback (handles the off-by-one seen in some months).
                if symbol == "VIX":
                    from datetime import datetime as _dt
                    from datetime import timedelta as _td
                    _expiry_attempts = [
                        expiry,
                        (_dt.strptime(expiry, "%Y%m%d").date() - _td(days=1)).strftime("%Y%m%d"),
                    ]
                    contract = None
                    for _try_expiry in _expiry_attempts:
                        _c = Option(
                            symbol=symbol,
                            lastTradeDateOrContractMonth=_try_expiry,
                            strike=strike,
                            right=right,
                            exchange="CBOE",
                            currency="USD",
                            multiplier="100",
                        )
                        _q = ib.qualifyContracts(_c)
                        _qualified = _q[0] if _q else None
                        if _qualified and getattr(_qualified, "conId", 0):
                            contract = _qualified
                            logger.debug("VIX contract qualified with expiry=%s", _try_expiry)
                            break
                    if not contract:
                        raise RuntimeError(
                            f"Could not qualify VIX contract for {order.instrument_id} "
                            f"(tried expiries: {_expiry_attempts})"
                        )
                else:
                    contract = Option(
                        symbol=symbol,
                        lastTradeDateOrContractMonth=expiry,
                        strike=strike,
                        right=right,
                        exchange=exchange,
                        currency="USD",
                    )
                    qualified = ib.qualifyContracts(contract)
                    # ib_async may return [None] (not []) when Error 200 fires;
                    # guard against both empty list and None element.
                    contract = qualified[0] if qualified else None
                    if not contract or not getattr(contract, "conId", 0):
                        raise RuntimeError(
                            f"Could not qualify IBKR contract for {order.instrument_id} "
                            f"(no conId — check symbol, expiry, strike, exchange)"
                        )

                action = "BUY" if order.side == OrderSide.BUY else "SELL"
                qty = int(order.quantity)

                if order.order_type == OrderType.LIMIT and order.limit_price is not None:
                    ib_order = LimitOrder(action, qty, round(order.limit_price, 2))
                else:
                    ib_order = MarketOrder(action, qty)

                ib_order.tif = "DAY"

                # ib_async placeOrder accesses contract.secIdType; if the
                # field is None (contract qualified but field not set),
                # it raises AttributeError.  Guard against that here.
                if getattr(contract, "secIdType", None) is None:
                    contract.secIdType = ""

                trade = ib.placeOrder(contract, ib_order)
                logger.info(
                    "[IBKR] Placed %s %s x%d @ %s (orderId=%s)",
                    action, order.instrument_id, qty,
                    order.limit_price, trade.order.orderId,
                )
                return str(trade.order.orderId)

            def cancel_order(self, order_id):
                return False

            def get_positions(self):
                return positions

            def get_order_status(self, order_id):
                return None

            def get_account_state(self):
                return account_state

            def get_fills(self, since=None):
                return []

            def sync(self):
                pass

        # evaluate_all always runs with dry_run=True so it never submits.
        # Actual submission is done below in Step 8 after risk checks.
        broker = _StubBroker()

        strategy_mgr = OptionsStrategyManager(
            broker=broker,
            mapper=mapper,
            discovery=discovery,
            dry_run=True,  # always — real submission handled in Step 8 after risk checks
        )

        all_directives = strategy_mgr.evaluate_all(
            portfolio=positions,
            signals=signals,
            existing_options=existing_options,
            allocations=allocations,
        )

        summary["strategy_directives"] = len(all_directives)
        summary["steps_completed"].append("run_strategies")

        # ── Step 6.5: Position lifecycle management ─────────────────
        lifecycle = PositionLifecycleManager()
        lifecycle_directives = lifecycle.evaluate(
            positions=existing_options,
            signals=signals,
        )
        all_directives.extend(lifecycle_directives)

        summary["lifecycle_directives"] = len(lifecycle_directives)
        summary["steps_completed"].append("lifecycle_management")

        # ── Step 7: Risk checks (with greeks budget) ─────────────────
        portfolio_greeks = options_portfolio.compute_portfolio_greeks()
        margin_snapshot = futures_mgr.margin

        # Check greeks budget
        greeks_util = options_portfolio.check_greeks_budget(
            nav=signals.get("nav", 0.0),
        )
        summary["greeks_within_budget"] = greeks_util.within_budget

        approved = _apply_risk_checks(
            all_directives,
            margin_snapshot,
            portfolio_greeks,
            max_margin_util=max_margin_util,
        )

        summary["approved_directives"] = len(approved)
        summary["blocked_directives"] = len(all_directives) - len(approved)
        summary["steps_completed"].append("risk_checks")

        # ── Step 8: Submit (or log) ───────────────────────────────────
        if dry_run:
            logger.info("=== DRY RUN — %d directives would be submitted ===", len(approved))
            for d in approved:
                logger.info(
                    "  [DRY] %s %s %s %s %.1f x%d — %s",
                    d.strategy, d.action.value, d.symbol, d.right,
                    d.strike, d.quantity, d.reason,
                )
        else:
            logger.info("Submitting %d approved directives via IBKR...", len(approved))
            # Swap in the real broker so _submit_directives routes to IBKR.
            strategy_mgr._broker = _IbkrDirectBroker()
            strategy_mgr._submit_directives(approved)

            # ── Log options decisions to DecisionTracker ──────────────────
            if approved:
                try:
                    from apathis.core.database import get_db_manager

                    from prometheus.decisions.tracker import DecisionTracker

                    # Map underlying symbol → canonical instrument ID for price lookups
                    _UNDERLYING_MAP: Dict[str, str] = {
                        "VIX": "VIX.INDX",
                        "SPY": "SPY.US",
                        "QQQ": "QQQ.US",
                        "IWM": "IWM.US",
                        "EFA": "EFA.US",
                        "TLT": "TLT.US",
                        "GLD": "GLD.US",
                        "ES": "ES.CME",
                    }

                    orders_for_log = []
                    for d in approved:
                        underlying_id = _UNDERLYING_MAP.get(d.symbol, f"{d.symbol}.US")
                        # Instrument ID: SYMBOL_YYMMDD_STRIKEC/P.US
                        exp_short = d.expiry[2:] if len(d.expiry) == 8 else d.expiry
                        instrument_id = f"{d.symbol}_{exp_short}_{d.strike:.0f}{d.right}.US"
                        orders_for_log.append({
                            "symbol": d.symbol,
                            "underlying_id": underlying_id,
                            "instrument_id": instrument_id,
                            "right": d.right,
                            "expiry": d.expiry,
                            "strike": d.strike,
                            "action": "BUY" if d.quantity > 0 else "SELL",
                            "quantity": abs(d.quantity),
                            "entry_price": d.limit_price or 0.0,
                            "strategy": d.strategy,
                            "reason": d.reason,
                            "trade_action": d.action.value,
                        })

                    signals_snap = {
                        "vix_level": signals.get("vix_level"),
                        "nav": signals.get("nav"),
                        "mhi": signals.get("mhi"),
                        "frag": signals.get("frag"),
                        "market_state": signals.get("market_state"),
                    }

                    tracker = DecisionTracker(db_manager=get_db_manager())
                    tracker.record_options_decision(
                        strategy_id="US_OPTIONS",
                        market_id="US_EQ",
                        as_of_date=date.today(),
                        orders=orders_for_log,
                        signals_snapshot=signals_snap,
                    )
                except Exception:
                    logger.exception(
                        "Failed to record options decision in tracker (non-fatal)"
                    )

        summary["steps_completed"].append("submit_orders")

        # ── Step 9: Portfolio status ──────────────────────────────────
        status = options_portfolio.get_status()
        summary["portfolio_status"] = status
        summary["futures_positions"] = len(futures_mgr.get_all_positions())
        summary["futures_notional"] = futures_mgr.get_total_notional()

        logger.info(
            "Derivatives daily complete: %d strategy directives, "
            "%d approved, %d roll orders, NAV=$%.0f",
            len(all_directives), len(approved), len(roll_orders),
            signals["nav"],
        )

    except Exception as exc:
        logger.error("Derivatives daily failed: %s", exc, exc_info=True)
        summary["errors"].append(str(exc))
    finally:
        try:
            ib.disconnect()
            logger.info("Disconnected from IBKR")
        except Exception:
            pass

    return summary


# ── CLI ───────────────────────────────────────────────────────────────

def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Run the daily derivatives pipeline (options, futures, FOP).",
    )
    parser.add_argument(
        "--host", type=str, default="127.0.0.1",
        help="IBKR host (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--port", type=int, default=None,
        help="IBKR port (default: 4002 for paper, 4001 for live)",
    )
    parser.add_argument(
        "--client-id", type=int, default=10,
        help="IBKR client ID (default: 10)",
    )
    parser.add_argument(
        "--account", type=str, default="",
        help="IBKR account ID (optional)",
    )
    parser.add_argument(
        "--paper", action="store_true",
        help="Use paper trading port (4002)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Log directives without submitting orders",
    )
    parser.add_argument(
        "--max-margin-util", type=float, default=0.60,
        help="Maximum margin utilisation threshold (default: 0.60)",
    )

    args = parser.parse_args(argv)

    # Resolve port
    if args.port is not None:
        port = args.port
    elif args.paper:
        port = 4002
    else:
        port = 4001

    dry_run = args.dry_run

    summary = run_derivatives_daily(
        host=args.host,
        port=port,
        client_id=args.client_id,
        account=args.account,
        dry_run=dry_run,
        max_margin_util=args.max_margin_util,
    )

    # Print summary
    print(f"\n{'='*60}")
    print(f"Derivatives Daily Summary — {summary['date']}")
    print(f"{'='*60}")
    print(f"  Mode:                {'DRY RUN' if summary['dry_run'] else 'LIVE'}")
    print(f"  Steps completed:     {', '.join(summary['steps_completed'])}")
    print(f"  Positions synced:    {summary.get('position_count', 'N/A')}")
    print(f"  NAV:                 ${summary.get('nav', 0):,.0f}")
    print(f"  VIX:                 {summary.get('vix', 'N/A')}")
    print(f"  ES Price:            {summary.get('es_price', 'N/A')}")
    print(f"  Market situation:    {summary.get('market_situation', 'N/A')}")
    print(f"  Strategies enabled:  {summary.get('strategies_enabled', 'N/A')}")
    print(f"  Roll directives:     {summary.get('roll_directives', 0)}")
    print(f"  Strategy directives: {summary.get('strategy_directives', 0)}")
    print(f"  Lifecycle directives:{summary.get('lifecycle_directives', 0)}")
    print(f"  Approved:            {summary.get('approved_directives', 0)}")
    print(f"  Blocked by risk:     {summary.get('blocked_directives', 0)}")
    print(f"  Greeks within budget:{summary.get('greeks_within_budget', 'N/A')}")
    print(f"  Futures positions:   {summary.get('futures_positions', 0)}")
    if summary["errors"]:
        print(f"  ERRORS:              {summary['errors']}")
    print(f"{'='*60}")

    if summary["errors"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
