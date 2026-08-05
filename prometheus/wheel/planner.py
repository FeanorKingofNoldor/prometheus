"""Core+wheel daily plan construction — pure logic, no I/O.

``build_plan`` turns one day's account view + market inputs into a list
of planned orders. All *strategy* rules live in ``wheel.engine`` (they
are pinned by tests); this module adds the account-level concerns the
engine deliberately doesn't know about:

* how many blocks the free capital supports (sizing),
* aggregation of identical per-block intents into one order,
* the IV-event guard (no new short options within the event window),
* ballast targets + quarterly rebalance sizing,
* the 40% drawdown breaker (alert-only: CSP re-entry is exempt by
  design — the wheel IS the recovery mechanism — so no order is ever
  suppressed by the breaker; it just escalates loudly).

The I/O shell (``runner``) supplies broker/market snapshots and executes
the returned plan.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Mapping

from prometheus.wheel.config import WheelStrategyConfig
from prometheus.wheel.engine import (
    BlockPhase,
    BlockState,
    IntentKind,
    MarketInputs,
    OpenOption,
    decide_block,
)

BREAKER_DRAWDOWN = 0.40


@dataclass(frozen=True)
class OpenShortOptionView:
    """One short option position as reconstructed from the broker."""

    right: str                  # "P" | "C"
    strike: float
    expiry: date
    contracts: int              # positive count of short contracts
    credit_per_share: float     # premium received (broker avgCost / multiplier)
    managed: bool               # rich-vol open → profit-take rule armed
    mark_per_share: float | None = None   # current mid, None when unavailable


@dataclass(frozen=True)
class WheelAccountView:
    """Broker truth distilled to what the planner needs."""

    nav: float
    total_cash: float                        # settled cash, all currencies USD-equiv
    underlying_shares: int                   # SPY shares held
    underlying_spot: float
    vix: float
    short_puts: tuple[OpenShortOptionView, ...] = ()
    short_calls: tuple[OpenShortOptionView, ...] = ()
    ballast_values: Mapping[str, float] = field(default_factory=dict)
    ballast_prices: Mapping[str, float] = field(default_factory=dict)
    peak_nav: float = 0.0                    # high-water mark since reset


@dataclass(frozen=True)
class PlannedOrder:
    """One order the runner should place (options aggregated by signature)."""

    category: str               # "csp" | "cc" | "profit_take" | "ballast"
    side: str                   # "BUY" | "SELL"
    quantity: int               # contracts (options) or shares (ballast)
    instrument_id: str          # ballast only; "" for options
    right: str = ""             # options only
    strike: float = 0.0
    target_expiry: date | None = None   # engine target; runner pins to the chain
    manage_with_profit_take: bool = False
    limit_hint: float | None = None      # ballast: last close; options priced live
    reason: str = ""


@dataclass
class WheelPlan:
    as_of: date
    orders: list[PlannedOrder] = field(default_factory=list)
    skips: list[str] = field(default_factory=list)
    breaker_triggered: bool = False
    drawdown: float = 0.0

    def summary(self) -> dict:
        return {
            "as_of": self.as_of.isoformat(),
            "orders": [
                {
                    "category": o.category,
                    "side": o.side,
                    "quantity": o.quantity,
                    "instrument_id": o.instrument_id,
                    "right": o.right,
                    "strike": o.strike,
                    "target_expiry": o.target_expiry.isoformat() if o.target_expiry else None,
                    "managed": o.manage_with_profit_take,
                    "limit_hint": o.limit_hint,
                    "reason": o.reason,
                }
                for o in self.orders
            ],
            "skips": list(self.skips),
            "breaker_triggered": self.breaker_triggered,
            "drawdown": round(self.drawdown, 4),
        }


def build_plan(
    cfg: WheelStrategyConfig,
    view: WheelAccountView,
    *,
    today: date,
    iv_event: str | None = None,
    ballast_rebalance_due: bool = False,
) -> WheelPlan:
    """Compute today's full order plan for the core+wheel book."""

    plan = WheelPlan(as_of=today)
    p = cfg.params

    # ── Drawdown breaker (alert-only, CSP-exempt by design) ──────────
    if view.peak_nav > 0 and view.nav > 0:
        plan.drawdown = max(0.0, 1.0 - view.nav / view.peak_nav)
        plan.breaker_triggered = plan.drawdown >= BREAKER_DRAWDOWN

    if view.underlying_spot <= 0 or view.vix <= 0:
        plan.skips.append(
            f"no_market_inputs: spot={view.underlying_spot} vix={view.vix}"
        )
        return plan

    market = MarketInputs(
        as_of=today,
        spot=view.underlying_spot,
        vix=view.vix,
        available_cash=0.0,
    )

    # ── 1. Profit-takes on managed short puts (always allowed) ───────
    for put in view.short_puts:
        if not put.managed:
            continue
        state = BlockState(
            block_id="pt",
            phase=BlockPhase.CSP_OPEN,
            cash_reserved=put.strike * 100.0,
            open_option=OpenOption(
                right="P", strike=put.strike, expiry=put.expiry,
                credit_per_share=put.credit_per_share, managed=True,
            ),
        )
        pt_market = MarketInputs(
            as_of=today, spot=view.underlying_spot, vix=view.vix,
            option_mark_per_share=put.mark_per_share,
        )
        for intent in decide_block(state, pt_market, p):
            if intent.kind == IntentKind.BUY_TO_CLOSE:
                plan.orders.append(
                    PlannedOrder(
                        category="profit_take",
                        side="BUY",
                        quantity=put.contracts,
                        instrument_id="",
                        right="P",
                        strike=put.strike,
                        target_expiry=put.expiry,
                        reason=intent.reason,
                    )
                )

    # ── 2. Covered calls on uncovered share lots ─────────────────────
    covered = sum(c.contracts for c in view.short_calls)
    uncovered_lots = max(view.underlying_shares // 100 - covered, 0)
    if uncovered_lots > 0:
        if iv_event:
            plan.skips.append(f"cc_skipped_iv_event: {iv_event}")
        else:
            shares_state = BlockState(
                block_id="cc", phase=BlockPhase.SHARES, shares=100,
            )
            cc_intents = decide_block(shares_state, market, p)
            if not cc_intents:
                plan.skips.append(f"cc_skipped_dead_vol: VIX {view.vix:.1f}")
            for intent in cc_intents:
                qty = min(uncovered_lots, cfg.max_contracts_per_day)
                plan.orders.append(
                    PlannedOrder(
                        category="cc",
                        side="SELL",
                        quantity=qty,
                        instrument_id="",
                        right="C",
                        strike=intent.strike,
                        target_expiry=intent.expiry,
                        reason=intent.reason,
                    )
                )

    # ── 3. New cash-secured puts ─────────────────────────────────────
    reserved = sum(o.strike * 100.0 * o.contracts for o in view.short_puts)
    share_value = view.underlying_shares * view.underlying_spot
    wheel_budget = view.nav * cfg.wheel_allocation
    free_budget = wheel_budget - reserved - share_value
    unreserved_cash = view.total_cash - reserved

    if iv_event:
        plan.skips.append(f"csp_skipped_iv_event: {iv_event}")
    else:
        csp_state = BlockState(block_id="csp", phase=BlockPhase.CASH)
        csp_market = MarketInputs(
            as_of=today, spot=view.underlying_spot, vix=view.vix,
            available_cash=unreserved_cash,
        )
        csp_intents = decide_block(csp_state, csp_market, p)
        if not csp_intents and unreserved_cash > 0:
            plan.skips.append(
                f"csp_skipped_insufficient_cash: unreserved=${unreserved_cash:,.0f}"
            )
        for intent in csp_intents:
            required = intent.strike * 100.0
            n = min(
                int(free_budget // required),
                int(unreserved_cash // required),
                cfg.max_contracts_per_day,
            )
            if n <= 0:
                plan.skips.append(
                    f"csp_skipped_budget: free_budget=${free_budget:,.0f} "
                    f"required=${required:,.0f}"
                )
                continue
            plan.orders.append(
                PlannedOrder(
                    category="csp",
                    side="SELL",
                    quantity=n,
                    instrument_id="",
                    right="P",
                    strike=intent.strike,
                    target_expiry=intent.expiry,
                    manage_with_profit_take=intent.manage_with_profit_take,
                    reason=intent.reason,
                )
            )

    # ── 4. Ballast (initial buy + quarterly rebalance) ───────────────
    for leg in cfg.ballast:
        target = view.nav * leg.weight
        current = float(view.ballast_values.get(leg.instrument_id, 0.0))
        price = float(view.ballast_prices.get(leg.instrument_id, 0.0))
        bootstrap = current < target * 0.25
        if not (ballast_rebalance_due or bootstrap):
            continue
        if price <= 0:
            plan.skips.append(f"ballast_skipped_no_price: {leg.instrument_id}")
            continue
        delta = target - current
        if abs(delta) < view.nav * 0.01:
            continue  # inside the drift band — leave it alone
        shares = int(abs(delta) // price)
        if shares <= 0:
            continue
        plan.orders.append(
            PlannedOrder(
                category="ballast",
                side="BUY" if delta > 0 else "SELL",
                quantity=shares,
                instrument_id=leg.instrument_id,
                limit_hint=price,
                reason=(
                    f"ballast {'bootstrap' if bootstrap else 'rebalance'}: "
                    f"{leg.instrument_id} ${current:,.0f} → ${target:,.0f}"
                ),
            )
        )

    return plan


__all__ = [
    "BREAKER_DRAWDOWN",
    "OpenShortOptionView",
    "WheelAccountView",
    "PlannedOrder",
    "WheelPlan",
    "build_plan",
]
