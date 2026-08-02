"""Deterministic order references for derivatives (option / FOP) orders.

Mirrors :func:`prometheus.execution.order_planner.deterministic_order_id`
but hashes the option-specific tuple — the equity helper only keys on
(portfolio, instrument, side, date), which cannot distinguish two
strategies trading the same contract on the same day.

The same (portfolio, strategy, contract, side, as-of-date) tuple always
yields the same UUID-shaped token. It is stamped onto the IBKR order as
``orderRef`` so a crash-then-retry of the same daily cycle produces an
identical ref, letting the pre-submission open-order check recognise —
and skip — an order that is still working from a previous attempt.
"""

from __future__ import annotations

import hashlib
from datetime import date
from typing import Optional


def deterministic_option_order_ref(
    *,
    portfolio_id: Optional[str],
    strategy: str,
    underlying: str,
    right: str,
    expiry: str,
    strike: float,
    side: str,
    as_of_date: date,
) -> str:
    """Stable ref for one option order within one daily cycle.

    Unlike ``deterministic_order_id`` this never falls back to a random
    UUID: every field is coerced to a canonical string (empty when
    missing), so re-running the same cycle always reproduces the ref —
    that reproducibility is the whole idempotency guarantee.

    Parameters mirror the directive → order conversion in
    ``run_derivatives_daily``: ``expiry`` is the normalised YYYYMMDD
    string, ``side`` is "BUY"/"SELL", ``strategy`` is the legacy
    strategy or template name carried in the order metadata.
    """
    raw = "|".join(
        (
            str(portfolio_id or ""),
            str(strategy or ""),
            str(underlying or "").upper(),
            str(right or "").upper(),
            str(expiry or ""),
            f"{float(strike):.4f}",
            str(side or "").upper(),
            as_of_date.isoformat(),
        )
    )
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()
    # UUID-shaped so downstream code/columns expecting a uuid-like token
    # keep working (same formatting as order_planner.deterministic_order_id).
    return (
        f"{digest[0:8]}-{digest[8:12]}-{digest[12:16]}-"
        f"{digest[16:20]}-{digest[20:32]}"
    )


__all__ = ["deterministic_option_order_ref"]
