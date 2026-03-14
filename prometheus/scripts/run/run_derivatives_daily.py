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
import sys
import time
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

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

    In production this pulls from the real FRAG, STAB, lambda, SHI
    pipelines and live market data.  For now it provides sensible
    defaults so the orchestration loop can run end-to-end.
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

    # Try to get live VIX
    try:
        from prometheus.execution.ib_compat import Index
        vix_contract = Index("VIX", "CBOE", "USD")
        qualified = ib.qualifyContracts(vix_contract)
        if qualified:
            tickers = ib.reqTickers(qualified[0])
            if tickers:
                vix_val = getattr(tickers[0], "last", None) or getattr(tickers[0], "close", 20.0)
                if vix_val and vix_val > 0:
                    signals["vix_level"] = float(vix_val)
    except Exception as exc:
        logger.debug("Could not fetch live VIX: %s", exc)

    # Try to get live ES price
    try:
        from prometheus.execution.ib_compat import Future
        from prometheus.execution.futures_manager import PRODUCTS
        es_product = PRODUCTS.get("ES")
        if es_product:
            es_contract = Future("ES", exchange="CME", currency="USD")
            es_contract.secType = "CONTFUT"  # Continuous
            qualified = ib.qualifyContracts(es_contract)
            if qualified:
                tickers = ib.reqTickers(qualified[0])
                if tickers:
                    es_val = getattr(tickers[0], "last", None) or getattr(tickers[0], "close", 0)
                    if es_val and es_val > 0:
                        signals["es_price"] = float(es_val)
    except Exception as exc:
        logger.debug("Could not fetch live ES price: %s", exc)

    # Build equity_prices from positions
    for iid, pos in positions.items():
        if iid.endswith(".US") or (not iid.endswith(".FUT") and "_" not in iid):
            symbol = iid.replace(".US", "").split(".")[0]
            qty = getattr(pos, "quantity", 0) or 0
            mv = getattr(pos, "market_value", 0) or 0
            if qty > 0 and mv > 0:
                signals["equity_prices"][symbol] = mv / qty

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
    from prometheus.execution.ib_compat import IB
    from prometheus.execution.instrument_mapper import InstrumentMapper
    from prometheus.execution.contract_discovery import ContractDiscoveryService
    from prometheus.execution.futures_manager import FuturesManager
    from prometheus.execution.options_portfolio import OptionsPortfolio
    from prometheus.execution.options_strategy import OptionsStrategyManager
    from prometheus.execution.broker_interface import BrokerInterface

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

        account_values = ib.accountValues()
        account_state: Dict[str, Any] = {}
        for av in account_values:
            if av.currency == "USD" or av.currency == "":
                account_state[av.tag] = av.value

        # Get all positions
        raw_positions = ib.positions()
        positions: Dict[str, Any] = {}
        for p in raw_positions:
            contract = p.contract
            iid = InstrumentMapper.contract_to_instrument_id(contract)
            positions[iid] = p

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

        from prometheus.execution.strategy_allocator import StrategyAllocator
        from prometheus.execution.position_lifecycle import PositionLifecycleManager

        # Determine market situation from signals
        market_state = signals.get("market_state", "NEUTRAL")

        allocator = StrategyAllocator()
        portfolio_greeks = options_portfolio.compute_portfolio_greeks()
        existing_options = options_portfolio.get_positions_as_dicts()

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

        # Build a stub broker for the strategy manager
        # (in production, use the real IbkrClientImpl)
        class _StubBroker(BrokerInterface):
            """Minimal stub — logs orders instead of submitting."""
            def submit_order(self, order):
                logger.info("[SUBMIT] %s %s x%d", order.side.value, order.instrument_id, order.quantity)
            def cancel_order(self, order_id):
                pass
            def get_positions(self):
                return positions
            def get_order_status(self, order_id):
                return None

        broker = _StubBroker()

        strategy_mgr = OptionsStrategyManager(
            broker=broker,
            mapper=mapper,
            discovery=discovery,
            dry_run=dry_run,
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
            logger.info("Submitting %d approved directives...", len(approved))
            # The strategy manager's _submit_directives handles conversion
            strategy_mgr._submit_directives(approved)

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

    # Default to dry-run for paper
    dry_run = args.dry_run or args.paper

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
