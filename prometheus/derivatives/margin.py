"""Pre-trade margin checker for the new sleeve pipeline.

Two implementations ship today:

* ``NullMarginChecker`` — approves every directive. Default; preserves
  back-compat for callers that don't pass a checker.
* ``NotionalMarginChecker`` — rough estimate based on
  ``notional × initial_margin_pct``. Useful in backtest and as a
  conservative gate when an IBKR connection isn't available.

A production ``IbkrMarginChecker`` (using ``ib.whatIfOrder``) ships
later as part of the production wiring step. The checker is wired
into the runner *behind* sizing, so it cannot over-reject — sizing
has already capped the contract count via budget / capacity /
greeks; the margin checker is the final gate.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class MarginCheck:
    """One pre-trade margin decision."""

    approved: bool
    estimated_init_margin: float       # estimated initial margin in $
    estimated_util_after: float        # margin util after this trade (0-1)
    reason: str                        # short audit string


class MarginChecker(Protocol):
    """Protocol for pre-trade per-directive margin estimation."""

    def check(
        self,
        *,
        underlying: str,
        right: str,
        strike: float,
        quantity: int,
        limit_price: float,
        multiplier: int = 100,
    ) -> MarginCheck: ...


class NullMarginChecker:
    """Approves everything — preserves back-compat when no checker
    is wired."""

    def check(
        self,
        *,
        underlying: str,
        right: str,
        strike: float,
        quantity: int,
        limit_price: float,
        multiplier: int = 100,
    ) -> MarginCheck:
        return MarginCheck(
            approved=True,
            estimated_init_margin=0.0,
            estimated_util_after=0.0,
            reason="null_checker_approves_all",
        )


class NotionalMarginChecker:
    """Conservative notional-based margin estimate.

    Sensible defaults for US equity options:

    * Long premium: margin = debit paid (i.e. ``|qty| × |price| × mult``).
    * Short premium: margin = ``max(20% × notional - OTM_amount, 10% × notional)``
      as a rough Reg-T proxy. The implementation here uses 20% notional
      for shorts as the floor.

    The checker also tracks running margin utilisation against
    ``account_equity`` so a sequence of trades can collectively
    exceed the limit even if each one looks fine in isolation.
    """

    def __init__(
        self,
        *,
        account_equity: float,
        max_margin_util: float = 0.60,
        current_margin_used: float = 0.0,
        short_margin_pct: float = 0.20,
    ) -> None:
        if account_equity <= 0:
            raise ValueError("account_equity must be positive")
        self._equity = float(account_equity)
        self._max_util = float(max_margin_util)
        self._used = float(current_margin_used)
        self._short_margin_pct = float(short_margin_pct)

    @property
    def current_margin_used(self) -> float:
        return self._used

    def check(
        self,
        *,
        underlying: str,
        right: str,
        strike: float,
        quantity: int,
        limit_price: float,
        multiplier: int = 100,
    ) -> MarginCheck:
        if quantity == 0 or limit_price <= 0 or multiplier <= 0:
            return MarginCheck(
                approved=False,
                estimated_init_margin=0.0,
                estimated_util_after=self._used / self._equity,
                reason="zero_or_invalid_inputs",
            )

        abs_qty = abs(quantity)
        notional = strike * multiplier * abs_qty
        debit = limit_price * multiplier * abs_qty

        if quantity > 0:
            # Long premium: margin requirement = debit paid
            est_init = debit
        else:
            # Short premium: % of notional (Reg-T proxy)
            est_init = max(
                self._short_margin_pct * notional,
                debit,
            )

        util_after = (self._used + est_init) / self._equity
        if util_after > self._max_util:
            return MarginCheck(
                approved=False,
                estimated_init_margin=est_init,
                estimated_util_after=util_after,
                reason=(
                    f"util_after={util_after:.1%} exceeds max "
                    f"{self._max_util:.1%}"
                ),
            )

        # Approve and book the margin for subsequent checks.
        self._used += est_init
        return MarginCheck(
            approved=True,
            estimated_init_margin=est_init,
            estimated_util_after=util_after,
            reason=f"util_after={util_after:.1%}",
        )


__all__ = [
    "MarginCheck",
    "MarginChecker",
    "NullMarginChecker",
    "NotionalMarginChecker",
]
