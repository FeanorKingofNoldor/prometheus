"""Unified contract selection.

Every template in the new derivatives architecture funnels through one
function — ``select_contract`` — to go from a *target spec* (underlying,
right, target delta, DTE band) to a concrete strike + expiry pair, with
a full provenance trace explaining the choice.

This replaces the seventeen per-strategy strike-picking routines that
currently each:

* discover the chain themselves,
* pick an expiration by their own DTE rule,
* delta-select using Black-Scholes with **VIX as sigma** for every
  underlying (the documented reason ``ShortPutStrategy`` is disabled),
* skip liquidity validation entirely.

The new pipeline:

1. Discover the chain via ``ContractDiscoveryService``.
2. Pick the best expiration in the requested DTE band (third Friday
   preferred, falls back to the nearest available).
3. Build candidate option contracts spanning a configurable strike
   window around the underlying price.
4. Run them through ``LiquidityFilter`` and drop the no-bid / wide /
   no-quote strikes.
5. Look up *real* IV per surviving strike via ``IvLookupService``.
6. Compute delta per strike using its own IV (not VIX-as-sigma).
7. Pick the strike whose delta is closest to the target.
8. Return the chosen contract together with a ``SelectionTrace`` that
   names every candidate, every rejection, every IV source, and every
   delta — so the audit log can answer "why this strike?".
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from apatheon.core.logging import get_logger

from prometheus.derivatives.iv_lookup import (
    IV_SOURCE_FALLBACK,
    IvLookupLike,
    IvLookupResult,
)
from prometheus.derivatives.liquidity_filter import (
    LiquidityLike,
    LiquidityQuote,
)
from prometheus.execution.contract_discovery import (
    ContractDiscoveryService,
    _bs_delta,
)

logger = get_logger(__name__)


# ── Input types ──────────────────────────────────────────────────────


@dataclass(frozen=True)
class TargetSpec:
    """What the template wants — independent of which strike achieves it."""

    underlying: str                # e.g. "SPY", "XLE"
    right: str                     # "C" or "P"
    target_delta: float            # magnitude (0.25 means 0.25-delta put or call)
    min_dte: int
    max_dte: int
    sec_type: str = "STK"
    exchange: str = "SMART"
    trading_class: str | None = None
    strike_width_pct: float = 0.30  # candidate window: underlying_price * (1 ± this)
    risk_free_rate: float = 0.045
    # Caller may pre-cap the candidate set if it has prior knowledge
    # (e.g. "strikes within 10% OTM only").
    max_candidates: int = 20


# ── Output types ─────────────────────────────────────────────────────


@dataclass(frozen=True)
class StrikeCandidate:
    """A single strike with all the diagnostics needed to rank it."""

    strike: float
    iv: float
    iv_source: str
    delta: float
    delta_diff: float
    quote: LiquidityQuote
    estimated_premium: float       # per-share, mid


@dataclass(frozen=True)
class SelectionTrace:
    """Why this strike was chosen — recorded into the decision log."""

    underlying: str
    underlying_price: float
    expiry: str
    chain_strikes_total: int
    chain_strikes_in_window: int
    liquidity_rejections: dict[str, int]  # reason → count
    candidates: list[StrikeCandidate]
    chosen_index: int | None


@dataclass(frozen=True)
class SelectionResult:
    """Output of ``select_contract`` — the choice + the receipt."""

    underlying: str
    expiry: str
    strike: float
    right: str
    iv: float
    iv_source: str
    delta: float
    estimated_premium_per_share: float
    estimated_premium_per_contract: float
    quote: LiquidityQuote
    trace: SelectionTrace
    skipped_reason: str | None = None

    @property
    def skipped(self) -> bool:
        return self.skipped_reason is not None


# ── Skip-result helpers ──────────────────────────────────────────────


def _skipped(
    target: TargetSpec, underlying_price: float, reason: str,
    expiry: str = "", chain_total: int = 0, chain_in_window: int = 0,
    rejections: dict[str, int] | None = None,
    candidates: list[StrikeCandidate] | None = None,
) -> SelectionResult:
    trace = SelectionTrace(
        underlying=target.underlying,
        underlying_price=underlying_price,
        expiry=expiry,
        chain_strikes_total=chain_total,
        chain_strikes_in_window=chain_in_window,
        liquidity_rejections=rejections or {},
        candidates=candidates or [],
        chosen_index=None,
    )
    return SelectionResult(
        underlying=target.underlying, expiry=expiry, strike=0.0,
        right=target.right, iv=0.0, iv_source="",
        delta=0.0, estimated_premium_per_share=0.0,
        estimated_premium_per_contract=0.0,
        quote=LiquidityQuote(0, 0, 0, 0, 0, 0),
        trace=trace,
        skipped_reason=reason,
    )


# ── Main entry ───────────────────────────────────────────────────────


def select_contract(
    *,
    target: TargetSpec,
    underlying_price: float,
    discovery: ContractDiscoveryService,
    iv_lookup: IvLookupLike,
    liquidity: LiquidityLike,
    fallback_iv: float,
    today: date | None = None,
) -> SelectionResult:
    """Pick the best contract for a target spec.

    Returns ``SelectionResult.skipped == True`` when no contract
    qualifies — the trace explains why.
    """

    if underlying_price <= 0:
        return _skipped(target, underlying_price, "invalid_underlying_price")

    today = today or date.today()

    # 1. Chain discovery.
    chains = discovery.discover_option_chain(
        target.underlying,
        sec_type=target.sec_type,
        exchange=target.exchange,
        trading_class=target.trading_class,
    )
    if not chains:
        return _skipped(target, underlying_price, "no_chain")

    chain = chains[0]

    # 2. Expiration selection.
    expirations = chain.filter_expirations(target.min_dte, target.max_dte, today=today)
    if not expirations:
        return _skipped(target, underlying_price, "no_expiration_in_dte_band")
    expiry = _pick_monthly_or_first(expirations)

    # 3. Candidate strikes inside a window around spot.
    strikes_in_window = chain.filter_strikes(
        underlying_price, width_pct=target.strike_width_pct,
    )
    if not strikes_in_window:
        return _skipped(
            target, underlying_price, "no_strikes_in_window",
            expiry=expiry, chain_total=len(chain.strikes), chain_in_window=0,
        )

    # Cap candidate count: keep the strikes closest to spot if the
    # window is unusually wide (e.g. dense weekly chain on SPY).
    strikes_in_window = sorted(strikes_in_window, key=lambda s: abs(s - underlying_price))
    candidates = strikes_in_window[: target.max_candidates]

    # 4. Build IBKR contracts and run liquidity filter.
    contracts = _build_contracts(
        target.underlying, expiry, candidates, target.right,
        target.exchange, target.trading_class,
    )
    liq_result = liquidity.filter(contracts)
    rejections = liq_result.reasons()
    if not liq_result.accepted:
        return _skipped(
            target, underlying_price, "no_liquid_strikes",
            expiry=expiry,
            chain_total=len(chain.strikes),
            chain_in_window=len(candidates),
            rejections=rejections,
        )

    # 5. IV per surviving strike.
    survived_contracts = [c for c, _ in liq_result.accepted]
    iv_results = iv_lookup.get_iv_batch(survived_contracts, fallback_iv=fallback_iv)

    # 6. Score each candidate by |delta - target_delta|.
    target_bs_delta = (
        -abs(target.target_delta) if target.right.upper() == "P"
        else abs(target.target_delta)
    )
    T_years = _years_to_expiry(expiry, today)
    if T_years <= 0:
        return _skipped(
            target, underlying_price, "expiry_in_past",
            expiry=expiry, chain_total=len(chain.strikes),
            chain_in_window=len(candidates), rejections=rejections,
        )

    scored: list[StrikeCandidate] = []
    for contract, quote in liq_result.accepted:
        key = _contract_key(contract)
        iv_res: IvLookupResult | None = iv_results.get(key)
        iv = iv_res.iv if iv_res else fallback_iv
        iv_source = iv_res.source if iv_res else IV_SOURCE_FALLBACK
        delta = _bs_delta(
            underlying_price, contract.strike, T_years,
            target.risk_free_rate, iv, target.right,
        )
        scored.append(
            StrikeCandidate(
                strike=contract.strike,
                iv=iv,
                iv_source=iv_source,
                delta=delta,
                delta_diff=abs(delta - target_bs_delta),
                quote=quote,
                estimated_premium=quote.mid if quote.mid > 0 else max(quote.last, 0.0),
            )
        )

    # 7. Pick the best.
    scored.sort(key=lambda c: c.delta_diff)
    chosen = scored[0]

    trace = SelectionTrace(
        underlying=target.underlying,
        underlying_price=underlying_price,
        expiry=expiry,
        chain_strikes_total=len(chain.strikes),
        chain_strikes_in_window=len(candidates),
        liquidity_rejections=rejections,
        candidates=scored,
        chosen_index=0,
    )

    premium_per_contract = chosen.estimated_premium * 100  # equity multiplier
    return SelectionResult(
        underlying=target.underlying,
        expiry=expiry,
        strike=chosen.strike,
        right=target.right.upper(),
        iv=chosen.iv,
        iv_source=chosen.iv_source,
        delta=chosen.delta,
        estimated_premium_per_share=chosen.estimated_premium,
        estimated_premium_per_contract=premium_per_contract,
        quote=chosen.quote,
        trace=trace,
    )


# ── Helpers ──────────────────────────────────────────────────────────


def _years_to_expiry(expiry: str, today: date) -> float:
    try:
        exp_date = datetime.strptime(expiry[:8], "%Y%m%d").date()
    except ValueError:
        return 0.0
    days = (exp_date - today).days
    return max(days, 1) / 365.0


def _pick_monthly_or_first(expirations: list[str]) -> str:
    """Prefer the third-Friday monthly if present, else the earliest."""
    for exp_str in expirations:
        try:
            d = datetime.strptime(exp_str[:8], "%Y%m%d").date()
            if d.weekday() == 4 and 15 <= d.day <= 21:
                return exp_str
        except ValueError:
            continue
    return expirations[0]


def _build_contracts(
    underlying: str,
    expiry: str,
    strikes: list[float],
    right: str,
    exchange: str,
    trading_class: str | None,
) -> list[Any]:
    """Build IB Option contracts without qualifying — the liquidity
    filter does its own market-data round-trip and the strikes came
    from a qualified chain, so requalifying here would be wasted.
    """
    from prometheus.execution.ib_compat import Option

    out: list[Any] = []
    r = right.upper()
    for strike in strikes:
        opt = Option(
            symbol=underlying,
            lastTradeDateOrContractMonth=expiry,
            strike=strike,
            right=r,
            exchange=exchange,
        )
        if trading_class:
            opt.tradingClass = trading_class
        out.append(opt)
    return out


def _contract_key(contract: Any) -> str:
    symbol = getattr(contract, "symbol", "?")
    expiry = getattr(contract, "lastTradeDateOrContractMonth", "")
    strike = getattr(contract, "strike", 0)
    right = getattr(contract, "right", "")
    return f"{symbol}:{expiry}:{strike}:{right}"


# ── Multi-leg (spread) support ───────────────────────────────────────
#
# Verticals, collars, butterflies, and condors all share these rules:
# every leg trades the same underlying and the same expiry; each leg
# picks its own right + strike to hit a target delta. ``select_spread``
# does what ``select_contract`` does for each leg, with one shared
# expiry, and computes net debit + max loss for sizing.


@dataclass(frozen=True)
class LegSpec:
    """One leg of a spread. Inherits underlying + expiry from ``SpreadSpec``."""

    right: str               # "C" or "P"
    target_delta: float      # magnitude (0.25 means 0.25Δ on this leg)
    is_long: bool            # True = buy this leg, False = sell
    name: str = ""           # human label (e.g. "long_put", "short_call")


@dataclass(frozen=True)
class SpreadSpec:
    """Multi-leg target — all legs share underlying, expiry, exchange."""

    underlying: str
    min_dte: int
    max_dte: int
    legs: tuple[LegSpec, ...]
    sec_type: str = "STK"
    exchange: str = "SMART"
    trading_class: str | None = None
    strike_width_pct: float = 0.30
    risk_free_rate: float = 0.045
    max_candidates_per_leg: int = 20


@dataclass(frozen=True)
class LegResult:
    """A single resolved leg of a spread."""

    name: str
    leg: LegSpec
    strike: float
    iv: float
    iv_source: str
    delta: float
    quote: LiquidityQuote
    estimated_premium_per_share: float    # always positive (mid price)


@dataclass(frozen=True)
class SpreadSelectionResult:
    """Output of ``select_spread`` — chosen legs + sizing inputs + trace."""

    underlying: str
    expiry: str
    legs: tuple[LegResult, ...]
    net_debit_per_share: float            # signed: +debit / -credit
    max_loss_per_contract: float          # always > 0; basis for sizing
    trace: SelectionTrace                 # aggregated: first leg's trace + leg summary
    skipped_reason: str | None = None

    @property
    def skipped(self) -> bool:
        return self.skipped_reason is not None


def _skipped_spread(
    spread: SpreadSpec, underlying_price: float, reason: str,
    expiry: str = "",
) -> SpreadSelectionResult:
    trace = SelectionTrace(
        underlying=spread.underlying, underlying_price=underlying_price,
        expiry=expiry, chain_strikes_total=0, chain_strikes_in_window=0,
        liquidity_rejections={}, candidates=[], chosen_index=None,
    )
    return SpreadSelectionResult(
        underlying=spread.underlying, expiry=expiry, legs=(),
        net_debit_per_share=0.0, max_loss_per_contract=0.0,
        trace=trace, skipped_reason=reason,
    )


def select_spread(
    *,
    spread: SpreadSpec,
    underlying_price: float,
    discovery: ContractDiscoveryService,
    iv_lookup: IvLookupLike,
    liquidity: LiquidityLike,
    fallback_iv: float,
    today: date | None = None,
) -> SpreadSelectionResult:
    """Resolve a multi-leg spread to concrete strikes at one shared expiry.

    Returns ``SpreadSelectionResult.skipped == True`` when no expiry or
    any leg fails to resolve.
    """

    if underlying_price <= 0:
        return _skipped_spread(spread, underlying_price, "invalid_underlying_price")
    if not spread.legs:
        return _skipped_spread(spread, underlying_price, "no_legs")

    today = today or date.today()

    chains = discovery.discover_option_chain(
        spread.underlying,
        sec_type=spread.sec_type,
        exchange=spread.exchange,
        trading_class=spread.trading_class,
    )
    if not chains:
        return _skipped_spread(spread, underlying_price, "no_chain")
    chain = chains[0]

    expirations = chain.filter_expirations(spread.min_dte, spread.max_dte, today=today)
    if not expirations:
        return _skipped_spread(spread, underlying_price, "no_expiration_in_dte_band")
    expiry = _pick_monthly_or_first(expirations)

    leg_results: list[LegResult] = []
    aggregated_rejections: dict[str, int] = {}

    for idx, leg in enumerate(spread.legs):
        leg_target = TargetSpec(
            underlying=spread.underlying,
            right=leg.right,
            target_delta=leg.target_delta,
            min_dte=spread.min_dte,
            max_dte=spread.max_dte,
            sec_type=spread.sec_type,
            exchange=spread.exchange,
            trading_class=spread.trading_class,
            strike_width_pct=spread.strike_width_pct,
            risk_free_rate=spread.risk_free_rate,
            max_candidates=spread.max_candidates_per_leg,
        )

        leg_pick = _resolve_leg_at_expiry(
            chain=chain, target=leg_target, expiry=expiry,
            underlying_price=underlying_price,
            iv_lookup=iv_lookup, liquidity=liquidity,
            fallback_iv=fallback_iv, today=today,
        )
        if leg_pick is None:
            return _skipped_spread(
                spread, underlying_price,
                f"leg_{idx}_unresolved({leg.name or leg.right})",
                expiry=expiry,
            )
        chosen, rejections = leg_pick
        for k, v in rejections.items():
            aggregated_rejections[k] = aggregated_rejections.get(k, 0) + v
        leg_results.append(
            LegResult(
                name=leg.name or f"leg_{idx}",
                leg=leg,
                strike=chosen.strike,
                iv=chosen.iv,
                iv_source=chosen.iv_source,
                delta=chosen.delta,
                quote=chosen.quote,
                estimated_premium_per_share=chosen.estimated_premium,
            )
        )

    net_debit = sum(
        (+1 if r.leg.is_long else -1) * r.estimated_premium_per_share
        for r in leg_results
    )
    max_loss = _max_loss_per_contract(leg_results, net_debit)

    trace = SelectionTrace(
        underlying=spread.underlying,
        underlying_price=underlying_price,
        expiry=expiry,
        chain_strikes_total=len(chain.strikes),
        chain_strikes_in_window=len(leg_results),
        liquidity_rejections=aggregated_rejections,
        candidates=[],   # leg-level candidates summarised via LegResult
        chosen_index=0,
    )
    return SpreadSelectionResult(
        underlying=spread.underlying,
        expiry=expiry,
        legs=tuple(leg_results),
        net_debit_per_share=net_debit,
        max_loss_per_contract=max_loss,
        trace=trace,
    )


def _resolve_leg_at_expiry(
    *,
    chain: Any,
    target: TargetSpec,
    expiry: str,
    underlying_price: float,
    iv_lookup: IvLookupLike,
    liquidity: LiquidityLike,
    fallback_iv: float,
    today: date,
) -> tuple[StrikeCandidate, dict[str, int]] | None:
    """Pick the best strike for one leg at a pinned expiry.

    Returns ``(chosen_candidate, liquidity_rejections)`` or ``None`` if
    nothing qualifies.
    """
    strikes_in_window = chain.filter_strikes(
        underlying_price, width_pct=target.strike_width_pct,
    )
    if not strikes_in_window:
        return None
    strikes_in_window = sorted(strikes_in_window, key=lambda s: abs(s - underlying_price))
    strikes_in_window = strikes_in_window[: target.max_candidates]

    contracts = _build_contracts(
        target.underlying, expiry, strikes_in_window, target.right,
        target.exchange, target.trading_class,
    )
    liq_result = liquidity.filter(contracts)
    rejections = liq_result.reasons()
    if not liq_result.accepted:
        return None

    survived_contracts = [c for c, _ in liq_result.accepted]
    iv_results = iv_lookup.get_iv_batch(survived_contracts, fallback_iv=fallback_iv)

    T_years = _years_to_expiry(expiry, today)
    if T_years <= 0:
        return None

    target_bs_delta = (
        -abs(target.target_delta) if target.right.upper() == "P"
        else abs(target.target_delta)
    )

    scored: list[StrikeCandidate] = []
    for contract, quote in liq_result.accepted:
        key = _contract_key(contract)
        iv_res = iv_results.get(key)
        iv = iv_res.iv if iv_res else fallback_iv
        iv_source = iv_res.source if iv_res else IV_SOURCE_FALLBACK
        delta = _bs_delta(
            underlying_price, contract.strike, T_years,
            target.risk_free_rate, iv, target.right,
        )
        scored.append(
            StrikeCandidate(
                strike=contract.strike, iv=iv, iv_source=iv_source,
                delta=delta, delta_diff=abs(delta - target_bs_delta),
                quote=quote,
                estimated_premium=quote.mid if quote.mid > 0 else max(quote.last, 0.0),
            )
        )

    if not scored:
        return None
    scored.sort(key=lambda c: c.delta_diff)
    return scored[0], rejections


def _max_loss_per_contract(legs: list[LegResult], net_debit_per_share: float) -> float:
    """Best-effort max loss for sizing a spread (per contract, $).

    Handled topologies:

    * **Vertical** (2 legs, same right) — width − |net credit| for
      credit spreads; |net debit| for debit spreads.
    * **Iron condor / iron butterfly** (4 legs split 2-call + 2-put,
      each side a vertical) — max(call_width, put_width) − net credit.
      For debit four-leg structures (uncommon) we fall back to
      |net debit|.
    * **Anything else** (collars, ratios, custom) — conservative
      floor of ``max(|net debit| × 100, 100)`` so sizing never divides
      by zero and capacity-bound templates (e.g. collars) still
      resolve.
    """
    if len(legs) == 2 and legs[0].leg.right.upper() == legs[1].leg.right.upper():
        # Vertical
        width = abs(legs[0].strike - legs[1].strike)
        if net_debit_per_share > 0:
            return max(net_debit_per_share, 0.01) * 100
        else:
            return max(width + net_debit_per_share, 0.01) * 100

    if len(legs) == 4:
        call_legs = [leg for leg in legs if leg.leg.right.upper() == "C"]
        put_legs = [leg for leg in legs if leg.leg.right.upper() == "P"]
        # Each side must be a single-vertical structure (1 long + 1 short)
        is_4leg_vertical_pair = (
            len(call_legs) == 2 and len(put_legs) == 2
            and sum(1 for leg in call_legs if leg.leg.is_long) == 1
            and sum(1 for leg in put_legs if leg.leg.is_long) == 1
        )
        if is_4leg_vertical_pair:
            call_width = abs(call_legs[0].strike - call_legs[1].strike)
            put_width = abs(put_legs[0].strike - put_legs[1].strike)
            worst_wing = max(call_width, put_width)
            if net_debit_per_share > 0:
                # Net debit four-leg — defensive
                return max(net_debit_per_share, 0.01) * 100
            # Credit four-leg: max loss = worst wing - net credit
            return max(worst_wing + net_debit_per_share, 0.01) * 100

    # Conservative fallback
    return max(abs(net_debit_per_share) * 100, 100.0)


__all__ = [
    "TargetSpec",
    "StrikeCandidate",
    "SelectionTrace",
    "SelectionResult",
    "select_contract",
    "LegSpec",
    "SpreadSpec",
    "LegResult",
    "SpreadSelectionResult",
    "select_spread",
]
