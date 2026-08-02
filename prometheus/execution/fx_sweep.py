"""Prometheus v2 – FX settlement sweep (account-global, US lane).

IBKR does not auto-convert currencies: when a regional book buys stock
in a currency the account doesn't hold, the account goes NEGATIVE in
that currency (a margin loan accruing debit interest) while base-
currency cash sits untouched.

Policy ("convert once, fixed local pots"):

* **Negative local balances are zeroed** by buying the currency against
  USD once they exceed a threshold. In practice this fires big on the
  first multi-market day (establishing each market's pot) and then only
  on drift (dividends, partial fills, P&L).
* **Positive local balances are LEFT ALONE** — they are the book's
  working capital for future buys. Sweeping them back would churn
  conversions every rebalance.
* **KRW is never touched**: IBKR converts KRW trade-linked
  automatically (restricted currency; standalone conversions are
  blocked broker-side).

Conversions route to IDEALPRO via market orders on the conventional
pair (EURUSD / GBPUSD / AUDUSD are currency-based; USDCHF / USDHKD /
USDJPY are USD-based). Rates for thresholding come from our own
``fx_rates_daily`` (deterministic, staleness-bounded).

Kill switch: ``PROMETHEUS_FX_SWEEP_DISABLED=1``.
Threshold: ``PROMETHEUS_FX_SWEEP_THRESHOLD_USD`` (default 2000).
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from typing import Dict, List, Mapping, Optional

from apatheon.core.logging import get_logger

from prometheus.env_utils import env_flag

logger = get_logger(__name__)

# Currencies IBKR settles trade-linked only — never converted by us.
RESTRICTED_CURRENCIES = frozenset({"KRW"})

# Market-convention IBKR pair per currency: (pair, currency_is_base).
# currency_is_base=True → BUY pair acquires the currency, qty in currency
# units. False (USD-based pair) → SELL pair acquires the currency, qty
# in USD units.
PAIR_CONVENTIONS: Dict[str, tuple[str, bool]] = {
    "EUR": ("EURUSD", True),
    "GBP": ("GBPUSD", True),
    "AUD": ("AUDUSD", True),
    "NZD": ("NZDUSD", True),
    "CHF": ("USDCHF", False),
    "HKD": ("USDHKD", False),
    "JPY": ("USDJPY", False),
    "CAD": ("USDCAD", False),
    "SEK": ("USDSEK", False),
    "NOK": ("USDNOK", False),
    "DKK": ("USDDKK", False),
}

DEFAULT_THRESHOLD_USD = 2000.0
# Don't bother IDEALPRO with dust (min commission $2 dominates).
MIN_TICKET_USD = 200.0


@dataclass(frozen=True)
class SweepOrder:
    """One planned FX conversion."""

    pair: str
    action: str  # "BUY" | "SELL" of the pair
    quantity: float  # in the pair's base-currency units
    currency: str  # the non-USD currency being acquired
    usd_value: float  # approximate USD notional
    reason: str


def sweep_threshold_usd() -> float:
    try:
        return float(
            os.environ.get("PROMETHEUS_FX_SWEEP_THRESHOLD_USD", DEFAULT_THRESHOLD_USD)
        )
    except ValueError:
        return DEFAULT_THRESHOLD_USD


def plan_fx_sweep(
    cash_balances: Mapping[str, float],
    usd_rates: Mapping[str, float],
    threshold_usd: float = DEFAULT_THRESHOLD_USD,
    base_currency: str = "USD",
) -> List[SweepOrder]:
    """Plan conversions that zero out negative non-base cash balances.

    Parameters
    ----------
    cash_balances:
        currency → cash balance in local units (IBKR CashBalance rows).
    usd_rates:
        currency → USD per 1 unit (from FxConverter). Currencies with no
        rate are skipped loudly (never convert on a guess).
    """
    orders: List[SweepOrder] = []
    for currency, balance in sorted(cash_balances.items()):
        ccy = currency.upper()
        if ccy == base_currency or ccy in RESTRICTED_CURRENCIES:
            continue
        if balance >= 0:
            continue  # positive balances are the book's pot — leave them
        rate = usd_rates.get(ccy)
        if rate is None or rate <= 0:
            logger.error(
                "plan_fx_sweep: no USD rate for %s (balance %.2f) — skipping, "
                "will retry next run", ccy, balance,
            )
            continue
        needed_local = -balance
        usd_value = needed_local * rate
        if usd_value < max(threshold_usd, MIN_TICKET_USD):
            continue

        convention = PAIR_CONVENTIONS.get(ccy)
        if convention is None:
            logger.error(
                "plan_fx_sweep: no pair convention for %s — add it to "
                "PAIR_CONVENTIONS", ccy,
            )
            continue
        pair, currency_is_base = convention
        if currency_is_base:
            # BUY XXXUSD: qty in XXX units.
            orders.append(SweepOrder(
                pair=pair,
                action="BUY",
                quantity=round(needed_local, 2),
                currency=ccy,
                usd_value=round(usd_value, 2),
                reason=f"zero negative {ccy} balance {balance:,.2f}",
            ))
        else:
            # SELL USDXXX: qty in USD units; delivers qty/  (USD per XXX)
            # → qty = needed_local × rate.
            orders.append(SweepOrder(
                pair=pair,
                action="SELL",
                quantity=round(usd_value, 2),
                currency=ccy,
                usd_value=round(usd_value, 2),
                reason=f"zero negative {ccy} balance {balance:,.2f}",
            ))
    return orders


def _read_cash_balances(ib) -> Dict[str, float]:
    """currency → CashBalance from IBKR account values."""
    balances: Dict[str, float] = {}
    for av in ib.accountValues():
        if getattr(av, "tag", "") != "CashBalance":
            continue
        ccy = getattr(av, "currency", "") or ""
        if not ccy or ccy == "BASE":
            continue
        try:
            balances[ccy.upper()] = float(av.value)
        except (TypeError, ValueError):
            continue
    return balances


def run_fx_sweep(
    db_manager,
    mode: str,
    as_of_date: Optional[date] = None,
    client=None,
) -> Dict[str, object]:
    """Execute the sweep against IBKR. Returns a summary dict.

    ``client`` is an optional pre-connected IbkrClientImpl (tests);
    when None a dedicated connection (client_id 15) is opened and
    closed here.
    """
    summary: Dict[str, object] = {
        "planned": 0, "submitted": 0, "skipped": 0, "errors": [],
    }
    if env_flag("PROMETHEUS_FX_SWEEP_DISABLED"):
        logger.info("run_fx_sweep: disabled via env — skipping")
        return summary
    if mode not in ("paper", "live"):
        return summary

    from prometheus.execution.fx import FxConverter
    from prometheus.execution.ib_compat import Forex, MarketOrder

    as_of = as_of_date or date.today()
    owns_client = client is None
    if owns_client:
        from prometheus.execution.ibkr_client_impl import IbkrClientImpl
        from prometheus.execution.ibkr_config import (
            IbkrGatewayType,
            IbkrMode,
            create_connection_config,
        )

        conn_config = create_connection_config(
            mode=IbkrMode.PAPER if mode == "paper" else IbkrMode.LIVE,
            gateway_type=IbkrGatewayType.GATEWAY,
            client_id=15,
        )
        client = IbkrClientImpl(config=conn_config)
        client.connect()

    try:
        ib = client._ib
        balances = _read_cash_balances(ib)
        if not balances:
            logger.info("run_fx_sweep: no non-base cash balances reported")
            return summary

        fx = FxConverter(db_manager)
        usd_rates: Dict[str, float] = {}
        for ccy in balances:
            if ccy == "USD" or ccy in RESTRICTED_CURRENCIES:
                continue
            try:
                usd_rates[ccy] = fx.usd_rate(ccy, as_of)
            except Exception:  # noqa: BLE001 - planner skips loudly
                pass

        orders = plan_fx_sweep(balances, usd_rates, sweep_threshold_usd())
        summary["planned"] = len(orders)
        logger.info(
            "run_fx_sweep: balances=%s → %d conversion(s) planned",
            {k: round(v, 2) for k, v in balances.items()},
            len(orders),
        )

        for order in orders:
            try:
                contract = Forex(order.pair)
                qualified = ib.qualifyContracts(contract)
                if not qualified:
                    raise RuntimeError(f"could not qualify {order.pair}")
                ib_order = MarketOrder(order.action, order.quantity)
                ib_order.orderRef = f"FXSWEEP_{as_of.isoformat()}_{order.currency}"
                ib.placeOrder(qualified[0], ib_order)
                summary["submitted"] = int(summary["submitted"]) + 1
                logger.info(
                    "run_fx_sweep: %s %s %.2f (%s — ~$%.0f)",
                    order.action, order.pair, order.quantity,
                    order.reason, order.usd_value,
                )
            except Exception as exc:  # noqa: BLE001 - isolate per pair
                summary["errors"].append(f"{order.pair}: {exc}")  # type: ignore[union-attr]
                logger.exception("run_fx_sweep: conversion failed for %s", order.pair)
        if hasattr(ib, "sleep"):
            ib.sleep(2)  # let order transmissions flush before disconnect
    finally:
        if owns_client:
            try:
                client.disconnect()
            except Exception:  # noqa: BLE001
                pass
    return summary
