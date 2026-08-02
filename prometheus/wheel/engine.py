"""Wheel decision engine — pure logic, no I/O.

One *block* = the capital backing one option contract (100 shares of the
underlying). The account runs N independent blocks; each is always in
exactly one phase of the cycle:

    CASH ──sell CSP──> CSP_OPEN ──assigned──> SHARES ──sell CC──> CC_OPEN
      ^                    │ expired OTM                             │
      └────────────────────┘<──────────────called away───────────────┘
                                    (CC expired OTM → back to SHARES)

Rules (validated 2026-08, wheel_sim_v2.py VIXCOND variant):
- CSP strike = spot × (1 − put_otm); when VIX > vix_rich_threshold the
  strike widens to (1 − put_otm_rich) AND that option is managed with a
  profit-take (buy back at profit_take_fraction of credit).  Normal-regime
  options are held to expiry — PT50-always tested as a wash.
- CC strike = spot × (1 + call_otm); when VIX < vix_dead_threshold no call
  is written that cycle (selling cheap options tested negative).
- Assignments are ACCEPTED, both directions: put assignment is the entry,
  call assignment is the trim.  Never defensive-close to dodge assignment
  (the 7-DTE rule tested destructive: it amputates the equity leg).
- Cash-secured always: a block may only open a CSP when it holds the full
  strike×100 in reservable cash.

This module decides; it does not price, submit, or persist.  The runner
supplies market inputs and executes the returned intents.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional


class BlockPhase(str, enum.Enum):
    CASH = "CASH"            # no shares, no open option
    CSP_OPEN = "CSP_OPEN"    # short put working
    SHARES = "SHARES"        # 100 shares held, no call written
    CC_OPEN = "CC_OPEN"      # 100 shares held + short call working


class IntentKind(str, enum.Enum):
    SELL_PUT = "SELL_PUT"
    SELL_CALL = "SELL_CALL"
    BUY_TO_CLOSE = "BUY_TO_CLOSE"


@dataclass(frozen=True)
class WheelParams:
    target_dte_days: int = 30
    put_otm: float = 0.02
    put_otm_rich: float = 0.05
    call_otm: float = 0.08
    vix_rich_threshold: float = 25.0
    vix_dead_threshold: float = 13.0
    profit_take_fraction: float = 0.50


@dataclass(frozen=True)
class OpenOption:
    right: str                  # "P" | "C"
    strike: float
    expiry: date
    credit_per_share: float     # premium received at open, per share
    managed: bool               # True → profit-take rule applies (rich-vol opens)


@dataclass
class BlockState:
    block_id: str
    phase: BlockPhase
    cash_reserved: float = 0.0          # cash securing an open CSP
    shares: int = 0                     # 0 or 100
    share_cost_basis: float = 0.0       # per share, informational
    open_option: Optional[OpenOption] = None


@dataclass(frozen=True)
class MarketInputs:
    as_of: date
    spot: float                         # underlying close
    vix: float                          # same-day VIX close
    option_mark_per_share: Optional[float] = None  # current value of the OPEN option
    available_cash: float = 0.0         # unreserved cash available to this block


@dataclass(frozen=True)
class OrderIntent:
    kind: IntentKind
    right: str
    strike: float
    expiry: date
    contracts: int = 1
    manage_with_profit_take: bool = False
    reason: str = ""


def next_expiry_on_or_after(as_of: date, min_dte_days: int) -> date:
    """Nearest Friday at least ``min_dte_days`` calendar days out.

    SPY/XSP list weekly Friday expirations; holiday-shifted expiries
    (Thursday when Friday is an exchange holiday) are resolved by the
    submission layer against the actual chain — this is the target, not
    the contract.
    """
    target = as_of + timedelta(days=min_dte_days)
    # Friday is weekday 4.
    offset = (4 - target.weekday()) % 7
    return target + timedelta(days=offset)


def round_strike(raw: float, increment: float = 1.0) -> float:
    """Round to the underlying's strike grid (SPY: $1 near the money)."""
    return round(raw / increment) * increment


def decide_block(
    state: BlockState,
    market: MarketInputs,
    params: WheelParams,
) -> list[OrderIntent]:
    """Return today's order intents for one block (possibly empty).

    Expiry/assignment processing is NOT done here — the runner reconciles
    fills and assignment events from the broker first, updates the
    BlockState phase accordingly, then calls this for the *current* state.
    """
    intents: list[OrderIntent] = []

    if state.phase == BlockPhase.CASH:
        rich = market.vix > params.vix_rich_threshold
        otm = params.put_otm_rich if rich else params.put_otm
        strike = round_strike(market.spot * (1.0 - otm))
        required = strike * 100.0
        if market.available_cash < required:
            # Cash-secured or nothing. The runner logs/short-circuits sizing.
            return []
        intents.append(
            OrderIntent(
                kind=IntentKind.SELL_PUT,
                right="P",
                strike=strike,
                expiry=next_expiry_on_or_after(market.as_of, params.target_dte_days),
                manage_with_profit_take=rich,
                reason=f"CSP {'rich-vol wide' if rich else 'normal'} (VIX {market.vix:.1f})",
            )
        )

    elif state.phase == BlockPhase.CSP_OPEN:
        opt = state.open_option
        if (
            opt is not None
            and opt.managed
            and market.option_mark_per_share is not None
            and market.option_mark_per_share <= opt.credit_per_share * params.profit_take_fraction
        ):
            intents.append(
                OrderIntent(
                    kind=IntentKind.BUY_TO_CLOSE,
                    right=opt.right,
                    strike=opt.strike,
                    expiry=opt.expiry,
                    reason=(
                        f"profit-take {params.profit_take_fraction:.0%}: mark "
                        f"{market.option_mark_per_share:.2f} vs credit {opt.credit_per_share:.2f}"
                    ),
                )
            )
        # Otherwise: hold to expiry. Assignment is handled by the runner.

    elif state.phase == BlockPhase.SHARES:
        if market.vix < params.vix_dead_threshold:
            # Cheap-vol regime: an 8%-OTM call sells for pennies and caps a
            # melt-up for nothing — skip this cycle, stay uncovered.
            return []
        strike = round_strike(market.spot * (1.0 + params.call_otm))
        intents.append(
            OrderIntent(
                kind=IntentKind.SELL_CALL,
                right="C",
                strike=strike,
                expiry=next_expiry_on_or_after(market.as_of, params.target_dte_days),
                reason=f"covered call (VIX {market.vix:.1f})",
            )
        )

    elif state.phase == BlockPhase.CC_OPEN:
        # Covered calls are always held to expiry: called away = the trim we
        # wanted; expired OTM = premium kept, write the next one tomorrow.
        return []

    return intents


def apply_expiry(
    state: BlockState,
    settlement_spot: float,
) -> BlockState:
    """Advance a block's phase through its option's expiry settlement.

    Pure state transition for the runner (and tests): given the underlying
    settlement price, resolve assignment. Cash/positions bookkeeping beyond
    the phase (actual share transfer, cash release) is reconciled from the
    broker — this keeps the state machine's view consistent meanwhile.
    """
    opt = state.open_option
    if opt is None:
        return state

    if opt.right == "P":
        assigned = settlement_spot < opt.strike
        return BlockState(
            block_id=state.block_id,
            phase=BlockPhase.SHARES if assigned else BlockPhase.CASH,
            cash_reserved=0.0,
            shares=100 if assigned else 0,
            share_cost_basis=opt.strike if assigned else 0.0,
            open_option=None,
        )

    called = settlement_spot > opt.strike
    return BlockState(
        block_id=state.block_id,
        phase=BlockPhase.CASH if called else BlockPhase.SHARES,
        cash_reserved=0.0,
        shares=0 if called else state.shares,
        share_cost_basis=0.0 if called else state.share_cost_basis,
        open_option=None,
    )
