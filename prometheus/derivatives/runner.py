"""Sleeve runner — converts a sleeve config into a list of directives.

This is the orchestration glue between the configuration surface
(``sleeves.py``) and the selection / sizing primitives. For each
template in a sleeve the runner:

1. Evaluates the trigger against current signals.
2. If fired, builds the ``TargetSpec`` and calls ``select_contract``
   to resolve a concrete strike + expiry.
3. Sizes the position via the unified ``size_position`` with the
   template's allocated sleeve budget.
4. Emits a ``SleeveDirective`` carrying the contract, quantity, the
   full ``SelectionTrace``, and trigger/sizing provenance — or a
   ``SleeveSkip`` row explaining why nothing was emitted.

Every template produces exactly one entry (directive *or* skip) per
run, so the audit log always has a row per template. That's how
shadow mode (Phase 1d) compares "what the new pipeline would do" with
"what the legacy strategies did do".
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import date
from typing import Any

from apatheon.core.logging import get_logger

from prometheus.backtest.option_pricer import bs_greeks
from prometheus.derivatives.iv_lookup import IvLookupLike
from prometheus.derivatives.liquidity_filter import LiquidityLike
from prometheus.derivatives.margin import MarginChecker, NullMarginChecker
from prometheus.derivatives.selection import (
    LegResult,
    SelectionResult,
    SelectionTrace,
    SpreadSelectionResult,
    select_contract,
    select_spread,
)
from prometheus.derivatives.sizing import (
    GreeksHeadroom,
    PerContractGreeks,
    SizingResult,
    size_position,
)
from prometheus.derivatives.sleeves import (
    Sleeve,
    SleeveConfig,
    TemplateConfig,
    TriggerResult,
)
from prometheus.execution.contract_discovery import ContractDiscoveryService

logger = get_logger(__name__)


SKIP_TRIGGER = "trigger_not_fired"
SKIP_NO_PRICE = "no_underlying_price"
SKIP_SELECTION = "selection_failed"
SKIP_SIZING = "sizing_zero"
SKIP_REGIME = "regime_gate_blocked"
SKIP_MARGIN = "margin_check_failed"


@dataclass(frozen=True)
class SleeveDirective:
    """One concrete trade the runner wants to put on."""

    sleeve: Sleeve
    template_name: str
    action: str                            # OPEN / CLOSE / ROLL
    underlying: str
    right: str
    expiry: str
    strike: float
    quantity: int                          # signed: +long, -short
    limit_price: float                     # estimated mid, per share
    iv_used: float
    iv_source: str
    delta: float
    estimated_premium_per_contract: float
    trigger_reason: str
    trigger_metadata: Mapping[str, Any]
    selection_trace: SelectionTrace
    sizing: SizingResult
    reason: str                            # one-line audit explanation


@dataclass(frozen=True)
class SleeveSkip:
    """Template evaluated but did not produce a directive."""

    sleeve: Sleeve
    template_name: str
    reason: str
    detail: str
    trigger: TriggerResult | None = None
    selection: SelectionResult | None = None
    sizing: SizingResult | None = None


@dataclass(frozen=True)
class SleeveRunResult:
    sleeve: Sleeve
    directives: list[SleeveDirective] = field(default_factory=list)
    skips: list[SleeveSkip] = field(default_factory=list)

    @property
    def fired(self) -> int:
        return len(self.directives)

    @property
    def skipped(self) -> int:
        return len(self.skips)


UnderlyingPriceFn = Callable[[str], float]


def run_sleeve(
    sleeve_cfg: SleeveConfig,
    *,
    signals: Mapping[str, Any],
    nav: float,
    open_contracts_by_template: Mapping[str, int],
    underlying_price_fn: UnderlyingPriceFn,
    discovery: ContractDiscoveryService,
    iv_lookup: IvLookupLike,
    liquidity: LiquidityLike,
    today: date | None = None,
    greeks_headroom: GreeksHeadroom | None = None,
    margin_checker: MarginChecker | None = None,
) -> SleeveRunResult:
    """Evaluate every template in a sleeve and emit directives/skips.

    When ``greeks_headroom`` is provided, each template's sizing is
    additionally capped to fit inside the remaining portfolio greeks
    budget. The headroom is decremented after each template fires so
    multiple templates in the same sleeve cannot collectively breach
    the budget.

    When ``margin_checker`` is provided, each directive runs through a
    pre-trade margin check after sizing. A failed check produces a
    SKIP_MARGIN entry instead of the directive. Default is the
    permissive ``NullMarginChecker``.
    """

    result = SleeveRunResult(sleeve=sleeve_cfg.sleeve)
    sleeve_budget = sleeve_cfg.budget(nav)
    remaining_headroom = greeks_headroom
    checker: MarginChecker = margin_checker or NullMarginChecker()

    for tmpl in sleeve_cfg.templates:
        outcomes, remaining_headroom = _evaluate_template(
            tmpl,
            sleeve_budget=sleeve_budget,
            signals=signals,
            open_contracts=open_contracts_by_template.get(tmpl.name, 0),
            underlying_price_fn=underlying_price_fn,
            discovery=discovery,
            iv_lookup=iv_lookup,
            liquidity=liquidity,
            today=today,
            greeks_headroom=remaining_headroom,
        )
        _apply_margin_gate(outcomes, checker, result)

    return result


def _apply_margin_gate(
    outcomes: list[SleeveDirective | SleeveSkip],
    checker: MarginChecker,
    result: SleeveRunResult,
) -> None:
    """Margin-gate outcomes with spread-aware semantics.

    Directives sharing a ``spread_group_id`` are checked as a unit:
    if any leg's margin check fails, the whole spread is rejected
    (we never want to submit a half-open spread). Single-leg
    directives are gated independently.
    """
    # Partition into single-leg and spread groups.
    spread_groups: dict[str, list[SleeveDirective]] = {}
    for o in outcomes:
        if not isinstance(o, SleeveDirective):
            result.skips.append(o)
            continue
        sgid = o.trigger_metadata.get("spread_group_id") if o.trigger_metadata else None
        if sgid:
            spread_groups.setdefault(str(sgid), []).append(o)
        else:
            gated = _margin_check_one(o, checker)
            if isinstance(gated, SleeveDirective):
                result.directives.append(gated)
            else:
                result.skips.append(gated)

    # Spread groups: check every leg; any rejection fails the whole spread.
    for sgid, legs in spread_groups.items():
        leg_results: list[tuple[SleeveDirective, SleeveDirective | SleeveSkip]] = [
            (leg, _margin_check_one(leg, checker)) for leg in legs
        ]
        rejected = [
            (leg, gated) for leg, gated in leg_results
            if isinstance(gated, SleeveSkip)
        ]
        if rejected:
            # Whole spread fails. Emit a single skip per leg referencing
            # the first rejection so the audit log says "this spread
            # didn't submit because leg X failed margin".
            first_leg, first_skip_obj = rejected[0]
            assert isinstance(first_skip_obj, SleeveSkip)
            first_skip: SleeveSkip = first_skip_obj
            for leg, _ in leg_results:
                result.skips.append(SleeveSkip(
                    sleeve=leg.sleeve,
                    template_name=leg.template_name,
                    reason=SKIP_MARGIN,
                    detail=(
                        f"spread {sgid} rejected: leg "
                        f"{first_leg.trigger_metadata.get('leg_name', '?')} "
                        f"failed margin — {first_skip.detail}"
                    ),
                    trigger=None,
                ))
        else:
            for leg, gated in leg_results:
                assert isinstance(gated, SleeveDirective)
                result.directives.append(gated)


def _margin_check_one(
    directive: SleeveDirective,
    checker: MarginChecker,
) -> SleeveDirective | SleeveSkip:
    """Run the pre-trade margin check on one directive."""
    check = checker.check(
        underlying=directive.underlying,
        right=directive.right,
        strike=directive.strike,
        quantity=directive.quantity,
        limit_price=directive.limit_price,
    )
    if check.approved:
        return directive
    return SleeveSkip(
        sleeve=directive.sleeve,
        template_name=directive.template_name,
        reason=SKIP_MARGIN,
        detail=(
            f"margin rejected: {check.reason} "
            f"(est_init=${check.estimated_init_margin:,.0f})"
        ),
        trigger=None,
    )


def _evaluate_template(
    tmpl: TemplateConfig,
    *,
    sleeve_budget: float,
    signals: Mapping[str, Any],
    open_contracts: int,
    underlying_price_fn: UnderlyingPriceFn,
    discovery: ContractDiscoveryService,
    iv_lookup: IvLookupLike,
    liquidity: LiquidityLike,
    today: date | None,
    greeks_headroom: GreeksHeadroom | None,
) -> tuple[list[SleeveDirective | SleeveSkip], GreeksHeadroom | None]:
    """Evaluate one template and decrement greeks headroom on fire.

    Returns ``(outcomes, headroom_after_this_template)``. Headroom is
    passed through unchanged on skip; decremented by the directive's
    greeks × contract count on fire.
    """
    # Regime gate first — cheap and template-level. Empty tuple = all
    # regimes allowed (back-compat default).
    if tmpl.allowed_market_states:
        state = str(signals.get("market_state", "") or "").upper()
        if state and state not in tmpl.allowed_market_states:
            return [SleeveSkip(
                sleeve=tmpl.sleeve, template_name=tmpl.name,
                reason=SKIP_REGIME,
                detail=(
                    f"market_state={state!r} not in "
                    f"{list(tmpl.allowed_market_states)}"
                ),
                trigger=None,
            )], greeks_headroom

    trigger = tmpl.trigger(signals)
    if not trigger.fire:
        return [SleeveSkip(
            sleeve=tmpl.sleeve, template_name=tmpl.name,
            reason=SKIP_TRIGGER, detail=trigger.reason, trigger=trigger,
        )], greeks_headroom

    if tmpl.is_spread:
        return _evaluate_spread_template(
            tmpl, sleeve_budget=sleeve_budget, signals=signals,
            trigger=trigger, open_contracts=open_contracts,
            underlying_price_fn=underlying_price_fn,
            discovery=discovery, iv_lookup=iv_lookup, liquidity=liquidity,
            today=today, greeks_headroom=greeks_headroom,
        )
    return _evaluate_single_leg_template(
        tmpl, sleeve_budget=sleeve_budget, signals=signals,
        trigger=trigger, open_contracts=open_contracts,
        underlying_price_fn=underlying_price_fn,
        discovery=discovery, iv_lookup=iv_lookup, liquidity=liquidity,
        today=today, greeks_headroom=greeks_headroom,
    )


def _evaluate_single_leg_template(
    tmpl: TemplateConfig,
    *,
    sleeve_budget: float,
    signals: Mapping[str, Any],
    trigger: TriggerResult,
    open_contracts: int,
    underlying_price_fn: UnderlyingPriceFn,
    discovery: ContractDiscoveryService,
    iv_lookup: IvLookupLike,
    liquidity: LiquidityLike,
    today: date | None,
    greeks_headroom: GreeksHeadroom | None,
) -> tuple[list[SleeveDirective | SleeveSkip], GreeksHeadroom | None]:
    target = tmpl.target_spec_factory(signals, trigger.metadata)  # type: ignore[misc]
    underlying_price = float(underlying_price_fn(target.underlying) or 0.0)
    if underlying_price <= 0:
        return [SleeveSkip(
            sleeve=tmpl.sleeve, template_name=tmpl.name,
            reason=SKIP_NO_PRICE,
            detail=f"underlying_price={underlying_price} for {target.underlying}",
            trigger=trigger,
        )], greeks_headroom

    selection = select_contract(
        target=target, underlying_price=underlying_price,
        discovery=discovery, iv_lookup=iv_lookup, liquidity=liquidity,
        fallback_iv=tmpl.fallback_iv, today=today,
    )
    if selection.skipped:
        return [SleeveSkip(
            sleeve=tmpl.sleeve, template_name=tmpl.name,
            reason=SKIP_SELECTION,
            detail=selection.skipped_reason or "unknown",
            trigger=trigger, selection=selection,
        )], greeks_headroom

    pcg = _per_contract_greeks_single(
        selection, today or date.today(), is_long=tmpl.is_long,
    )

    template_budget = sleeve_budget * tmpl.sizing_pct_of_sleeve
    sizing = size_position(
        category_budget_usd=template_budget,
        premium_per_contract_usd=selection.estimated_premium_per_contract,
        max_contracts=tmpl.max_concurrent,
        already_open_contracts=open_contracts,
        greeks_headroom=greeks_headroom,
        per_contract_greeks=pcg,
    )
    if sizing.skipped:
        return [SleeveSkip(
            sleeve=tmpl.sleeve, template_name=tmpl.name,
            reason=SKIP_SIZING,
            detail=sizing.skipped_reason or "unknown",
            trigger=trigger, selection=selection, sizing=sizing,
        )], greeks_headroom

    qty = sizing.contracts if tmpl.is_long else -sizing.contracts
    limit_price = (
        selection.quote.ask if tmpl.is_long else selection.quote.bid
    ) or selection.estimated_premium_per_share

    reason = (
        f"{tmpl.name}: {trigger.reason} | "
        f"strike={selection.strike} expiry={selection.expiry} "
        f"delta={selection.delta:+.3f} iv={selection.iv:.2%} "
        f"({selection.iv_source}) qty={qty}"
    )

    directive = SleeveDirective(
        sleeve=tmpl.sleeve, template_name=tmpl.name,
        action="OPEN", underlying=selection.underlying,
        right=selection.right, expiry=selection.expiry,
        strike=selection.strike, quantity=qty,
        limit_price=round(float(limit_price), 2),
        iv_used=selection.iv, iv_source=selection.iv_source,
        delta=selection.delta,
        estimated_premium_per_contract=selection.estimated_premium_per_contract,
        trigger_reason=trigger.reason, trigger_metadata=trigger.metadata,
        selection_trace=selection.trace, sizing=sizing, reason=reason,
    )
    new_headroom = _decrement_headroom(greeks_headroom, pcg, sizing.contracts)
    return [directive], new_headroom


def _evaluate_spread_template(
    tmpl: TemplateConfig,
    *,
    sleeve_budget: float,
    signals: Mapping[str, Any],
    trigger: TriggerResult,
    open_contracts: int,
    underlying_price_fn: UnderlyingPriceFn,
    discovery: ContractDiscoveryService,
    iv_lookup: IvLookupLike,
    liquidity: LiquidityLike,
    today: date | None,
    greeks_headroom: GreeksHeadroom | None,
) -> tuple[list[SleeveDirective | SleeveSkip], GreeksHeadroom | None]:
    spread = tmpl.spread_spec_factory(signals, trigger.metadata)  # type: ignore[misc]
    underlying_price = float(underlying_price_fn(spread.underlying) or 0.0)
    if underlying_price <= 0:
        return [SleeveSkip(
            sleeve=tmpl.sleeve, template_name=tmpl.name,
            reason=SKIP_NO_PRICE,
            detail=f"underlying_price={underlying_price} for {spread.underlying}",
            trigger=trigger,
        )], greeks_headroom

    spread_result = select_spread(
        spread=spread, underlying_price=underlying_price,
        discovery=discovery, iv_lookup=iv_lookup, liquidity=liquidity,
        fallback_iv=tmpl.fallback_iv, today=today,
    )
    if spread_result.skipped:
        return [SleeveSkip(
            sleeve=tmpl.sleeve, template_name=tmpl.name,
            reason=SKIP_SELECTION,
            detail=spread_result.skipped_reason or "unknown",
            trigger=trigger,
        )], greeks_headroom

    pcg = _per_contract_greeks_spread(spread_result, today or date.today())

    template_budget = sleeve_budget * tmpl.sizing_pct_of_sleeve
    sizing = size_position(
        category_budget_usd=template_budget,
        premium_per_contract_usd=spread_result.max_loss_per_contract,
        max_contracts=tmpl.max_concurrent,
        already_open_contracts=open_contracts,
        greeks_headroom=greeks_headroom,
        per_contract_greeks=pcg,
    )
    if sizing.skipped:
        return [SleeveSkip(
            sleeve=tmpl.sleeve, template_name=tmpl.name,
            reason=SKIP_SIZING,
            detail=sizing.skipped_reason or "unknown",
            trigger=trigger, sizing=sizing,
        )], greeks_headroom

    # Emit one SleeveDirective per leg, sharing a spread_group_id so the
    # submit path / lifecycle / shadow log can correlate them.
    spread_group_id = f"sg-{uuid.uuid4().hex[:12]}"
    directives: list[SleeveDirective | SleeveSkip] = []
    for idx, leg_res in enumerate(spread_result.legs):
        directives.append(
            _spread_leg_directive(
                tmpl=tmpl, trigger=trigger, sizing=sizing,
                spread_result=spread_result, leg_res=leg_res,
                spread_group_id=spread_group_id, leg_index=idx,
            )
        )
    new_headroom = _decrement_headroom(greeks_headroom, pcg, sizing.contracts)
    return directives, new_headroom


# ── Greeks helpers ───────────────────────────────────────────────────


def _years_to_expiry(expiry: str, today: date) -> float:
    from datetime import datetime
    try:
        d = datetime.strptime(expiry[:8], "%Y%m%d").date()
    except (ValueError, IndexError):
        return 0.0
    return max((d - today).days, 1) / 365.0


def _per_contract_greeks_single(
    selection: SelectionResult, today: date, *, is_long: bool,
) -> PerContractGreeks:
    """Greeks per contract for a single-leg directive, signed by side."""
    S = selection.trace.underlying_price
    K = selection.strike
    T = _years_to_expiry(selection.expiry, today)
    g = bs_greeks(S=S, K=K, T=T, r=0.045, sigma=selection.iv, right=selection.right)
    sign = 1 if is_long else -1
    # × 100 = standard equity options multiplier; VIX/SPX use the same.
    return PerContractGreeks(
        delta=sign * g.delta * 100.0,
        gamma=sign * g.gamma * 100.0,
        theta=sign * g.theta * 100.0,
        vega=sign * g.vega * 100.0,
    )


def _per_contract_greeks_spread(
    spread_result: SpreadSelectionResult, today: date,
) -> PerContractGreeks:
    """Greeks per contract (of *each* leg) for a spread — net across legs."""
    S = spread_result.trace.underlying_price
    T = _years_to_expiry(spread_result.expiry, today)
    delta = gamma = theta = vega = 0.0
    for leg in spread_result.legs:
        g = bs_greeks(
            S=S, K=leg.strike, T=T, r=0.045,
            sigma=leg.iv, right=leg.leg.right,
        )
        sign = 1 if leg.leg.is_long else -1
        delta += sign * g.delta
        gamma += sign * g.gamma
        theta += sign * g.theta
        vega += sign * g.vega
    return PerContractGreeks(
        delta=delta * 100.0, gamma=gamma * 100.0,
        theta=theta * 100.0, vega=vega * 100.0,
    )


def _decrement_headroom(
    headroom: GreeksHeadroom | None,
    pcg: PerContractGreeks,
    contracts: int,
) -> GreeksHeadroom | None:
    """Subtract the directive's greeks from the remaining headroom.

    Convention matches the sizing module: delta/vega are tracked as
    absolute capacity; theta consumed only when the position bleeds
    (negative theta); gamma consumed only when positive.
    """
    if headroom is None or contracts <= 0:
        return headroom
    return GreeksHeadroom(
        delta_abs=max(headroom.delta_abs - abs(pcg.delta) * contracts, 0.0),
        gamma=max(
            headroom.gamma - max(pcg.gamma, 0.0) * contracts, 0.0,
        ),
        theta=max(
            headroom.theta - max(-pcg.theta, 0.0) * contracts, 0.0,
        ),
        vega=max(headroom.vega - abs(pcg.vega) * contracts, 0.0),
    )


def _spread_leg_directive(
    *,
    tmpl: TemplateConfig,
    trigger: TriggerResult,
    sizing: SizingResult,
    spread_result: SpreadSelectionResult,
    leg_res: LegResult,
    spread_group_id: str,
    leg_index: int,
) -> SleeveDirective:
    qty = sizing.contracts if leg_res.leg.is_long else -sizing.contracts
    limit_price = (
        leg_res.quote.ask if leg_res.leg.is_long else leg_res.quote.bid
    ) or leg_res.estimated_premium_per_share

    leg_meta: dict[str, Any] = dict(trigger.metadata)
    leg_meta.update({
        "spread_group_id": spread_group_id,
        "leg_index": leg_index,
        "leg_name": leg_res.name,
        "leg_count": len(spread_result.legs),
        "net_debit_per_share": spread_result.net_debit_per_share,
        "max_loss_per_contract": spread_result.max_loss_per_contract,
    })

    reason = (
        f"{tmpl.name}[{leg_res.name}]: {trigger.reason} | "
        f"strike={leg_res.strike} expiry={spread_result.expiry} "
        f"delta={leg_res.delta:+.3f} iv={leg_res.iv:.2%} "
        f"({leg_res.iv_source}) qty={qty} "
        f"net_debit={spread_result.net_debit_per_share:+.2f} "
        f"max_loss=${spread_result.max_loss_per_contract:.0f}"
    )

    return SleeveDirective(
        sleeve=tmpl.sleeve,
        template_name=tmpl.name,
        action="OPEN",
        underlying=spread_result.underlying,
        right=leg_res.leg.right.upper(),
        expiry=spread_result.expiry,
        strike=leg_res.strike,
        quantity=qty,
        limit_price=round(float(limit_price), 2),
        iv_used=leg_res.iv,
        iv_source=leg_res.iv_source,
        delta=leg_res.delta,
        estimated_premium_per_contract=leg_res.estimated_premium_per_share * 100,
        trigger_reason=trigger.reason,
        trigger_metadata=leg_meta,
        selection_trace=spread_result.trace,
        sizing=sizing,
        reason=reason,
    )


__all__ = [
    "SleeveDirective",
    "SleeveSkip",
    "SleeveRunResult",
    "UnderlyingPriceFn",
    "SKIP_TRIGGER",
    "SKIP_NO_PRICE",
    "SKIP_SELECTION",
    "SKIP_SIZING",
    "SKIP_REGIME",
    "SKIP_MARGIN",
    "run_sleeve",
]
