"""Unified options sizing.

Every options strategy in the redesign calls one function — ``size_position`` —
to decide how many contracts to trade. Strategy-specific quirks (delta-
neutralised sizing, share-based covering, sector exposure matching) are
expressed as inputs to this function rather than as separate per-strategy
formulas.

The function reproduces the dominant pattern across the existing
seventeen strategy classes:

    n = max(min_contracts, min(
        budget / cost_per_contract,
        max_contracts - already_open,
    ))

with explicit handling for the edge cases (negative budget, zero cost,
exhausted cap) that today are scattered across ad-hoc ``max(1, ...)``
expressions.

Two share-based strategies (covered_call, wheel) size off equity
holdings rather than capital — they pass ``share_count // 100`` as
``max_contracts`` and a sentinel cost, so the same call site works.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class GreeksHeadroom:
    """How much delta / gamma / theta / vega capacity remains in the
    portfolio greeks budget before sizing this template.

    Sign convention:

    * ``delta_abs`` — remaining ``|net delta|`` capacity (positive).
      A directive that adds 30 net delta consumes 30 from here.
    * ``gamma`` — remaining absolute gamma capacity (positive).
    * ``theta`` — remaining *room to bleed* (positive). A long option
      that loses $5/day of theta consumes 5 from here. When theta is
      0, no more theta-negative positions can be added.
    * ``vega`` — remaining ``|net vega|`` capacity (positive).
    """

    delta_abs: float
    gamma: float
    theta: float
    vega: float


@dataclass(frozen=True)
class PerContractGreeks:
    """Estimated greeks one *contract* of this template will add.

    All values are signed × multiplier (so a 100-share-equivalent long
    put with per-share delta -0.27 contributes ``-27`` here). For
    spreads this is the net across legs.
    """

    delta: float
    gamma: float
    theta: float
    vega: float


@dataclass(frozen=True)
class SizingResult:
    """Result of a sizing decision, with provenance for the audit log."""

    contracts: int
    capacity_bound: bool        # True when ``max_contracts`` was the binding constraint
    budget_bound: bool          # True when budget / cost was the binding constraint
    skipped_reason: str | None  # Non-None when contracts == 0
    greeks_bound: bool = False  # True when a greek headroom was the binding constraint

    @property
    def skipped(self) -> bool:
        return self.contracts == 0


def size_position(
    *,
    category_budget_usd: float,
    premium_per_contract_usd: float,
    max_contracts: int | None = None,
    min_contracts: int = 1,
    already_open_contracts: int = 0,
    greeks_headroom: GreeksHeadroom | None = None,
    per_contract_greeks: PerContractGreeks | None = None,
) -> SizingResult:
    """Decide how many contracts to trade for a single template.

    Parameters
    ----------
    category_budget_usd
        Dollar capital available for *this* trade — already net of any
        sleeve-level allocation, sector cap, or portfolio greeks check.
        The caller is responsible for computing this.
    premium_per_contract_usd
        Expected debit (long premium), max loss (defined-risk spread),
        or notional cost (sector exposure match) per contract, in
        dollars. Must be positive to size > 0.
    max_contracts
        Optional hard cap from the template (e.g. ``max_concurrent`` or
        ``shares_held // 100`` for covered calls). ``None`` means
        unbounded by capacity. Net of ``already_open_contracts``.
    min_contracts
        Floor when sizing is otherwise positive. Default ``1`` — the
        dominant behaviour today is "always at least one contract if
        we're going to trade." Pass ``0`` to allow "skip if can't
        afford a meaningful position."
    already_open_contracts
        Existing concurrent positions opened by this template; consumed
        from ``max_contracts`` before sizing.
    greeks_headroom
        Optional remaining portfolio greeks budget. When provided
        together with ``per_contract_greeks``, sizing is additionally
        capped so the directive's collective greeks don't breach the
        headroom. ``None`` (default) skips the greeks check.
    per_contract_greeks
        Estimated per-contract greeks (signed × multiplier).

    Returns
    -------
    SizingResult
        Decision plus provenance for the audit log.
    """

    if category_budget_usd <= 0:
        return SizingResult(0, False, True, "budget_non_positive")
    if premium_per_contract_usd <= 0:
        return SizingResult(0, False, True, "premium_estimate_non_positive")

    n_by_budget = int(category_budget_usd / premium_per_contract_usd)

    if max_contracts is not None:
        remaining_capacity = max_contracts - already_open_contracts
        if remaining_capacity <= 0:
            return SizingResult(0, True, False, "capacity_exhausted")
        n = min(n_by_budget, remaining_capacity)
        capacity_bound = remaining_capacity <= n_by_budget
        budget_bound = n_by_budget <= remaining_capacity
    else:
        n = n_by_budget
        capacity_bound = False
        budget_bound = True

    greeks_bound = False
    if greeks_headroom is not None and per_contract_greeks is not None:
        n_by_greeks = _max_contracts_within_greeks(
            greeks_headroom, per_contract_greeks,
        )
        if n_by_greeks <= 0:
            return SizingResult(
                0, capacity_bound, budget_bound,
                "greeks_headroom_exhausted", greeks_bound=True,
            )
        if n_by_greeks < n:
            n = n_by_greeks
            greeks_bound = True
            # Greeks dominates — other "bound" flags become advisory.
            capacity_bound = capacity_bound and n_by_greeks == (
                (max_contracts or 0) - already_open_contracts
            )
            budget_bound = False

    if n < min_contracts:
        if min_contracts > 0 and n_by_budget >= 1 and (
            greeks_headroom is None
            or _max_contracts_within_greeks(greeks_headroom, per_contract_greeks)
            >= min_contracts
        ):
            n = min_contracts
        else:
            return SizingResult(
                0, capacity_bound, budget_bound,
                "below_min_contracts", greeks_bound=greeks_bound,
            )

    # Final guard: when ``min_contracts=0`` the path above never lifts,
    # so a zero-contracts outcome would otherwise return with
    # ``skipped_reason=None`` (inconsistent with ``.skipped == True``).
    if n <= 0:
        return SizingResult(
            0, capacity_bound, budget_bound,
            "budget_below_one_contract", greeks_bound=greeks_bound,
        )

    return SizingResult(
        n, capacity_bound, budget_bound, None, greeks_bound=greeks_bound,
    )


def _max_contracts_within_greeks(
    headroom: GreeksHeadroom,
    per_contract: PerContractGreeks | None,
) -> int:
    """Max contracts that fit inside every greek's remaining headroom.

    The check is conservative: each greek's contribution must be ≤
    its headroom in absolute terms. A contract that adds zero of a
    given greek doesn't constrain it.

    Negative headroom means we're already over-budget on that greek
    and the next contract would make it worse → return 0.
    """
    if per_contract is None:
        # No greeks estimate → no greeks-based cap (treat as unbounded).
        return 10**9

    caps: list[int] = []
    for hdr_val, leg_val, sign_ok in (
        (headroom.delta_abs, per_contract.delta, _adds_to_delta(per_contract.delta)),
        (headroom.gamma, per_contract.gamma, per_contract.gamma > 0),
        (headroom.vega, per_contract.vega, abs(per_contract.vega) > 1e-9),
    ):
        if not sign_ok:
            continue
        if hdr_val <= 0:
            return 0
        denom = abs(leg_val)
        if denom < 1e-9:
            continue
        caps.append(int(hdr_val / denom))

    # Theta has its own semantics: negative theta consumes headroom.
    if per_contract.theta < 0:
        if headroom.theta <= 0:
            return 0
        caps.append(int(headroom.theta / abs(per_contract.theta)))

    if not caps:
        return 10**9
    return max(min(caps), 0)


def _adds_to_delta(delta: float) -> bool:
    return abs(delta) > 1e-9


__all__ = [
    "SizingResult",
    "GreeksHeadroom",
    "PerContractGreeks",
    "size_position",
]
