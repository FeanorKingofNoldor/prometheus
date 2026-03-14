"""Prometheus v2 – Show portfolio allocations over a date range.

This script summarizes portfolio targets stored in `target_portfolios` by date.
It is especially useful for allocator books where metadata includes:
- hedge_allocation
- long_allocation
- cash_weight
- fragility_score
- market_situation

Examples
--------

Summarize allocator targets over a period:

    python -m prometheus.scripts.show.show_portfolio_allocations \
        --portfolio-id US_EQ_ALLOCATOR \
        --start 2020-02-01 --end 2020-04-30

Summarize a single date:

    python -m prometheus.scripts.show.show_portfolio_allocations \
        --portfolio-id US_EQ_ALLOCATOR \
        --start 2024-12-02 --end 2024-12-02
"""

from __future__ import annotations

import argparse
from datetime import date, datetime
from typing import Any, Optional, Sequence

from apathis.core.database import get_db_manager


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except Exception as exc:  # pragma: no cover - CLI validation
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


def _get_float(meta: dict[str, Any], key: str) -> float | None:
    v = meta.get(key)
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def _get_str(meta: dict[str, Any], key: str) -> str | None:
    v = meta.get(key)
    if isinstance(v, str) and v.strip():
        return v.strip()
    return None


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Summarize target_portfolios allocations over a date range",
    )

    parser.add_argument(
        "--portfolio-id",
        type=str,
        required=True,
        help="Portfolio id (e.g. US_EQ_ALLOCATOR)",
    )
    parser.add_argument(
        "--start",
        type=_parse_date,
        required=True,
        help="Start date (YYYY-MM-DD, inclusive)",
    )
    parser.add_argument(
        "--end",
        type=_parse_date,
        required=True,
        help="End date (YYYY-MM-DD, inclusive)",
    )

    args = parser.parse_args(list(argv) if argv is not None else None)

    db = get_db_manager()

    sql = """
        SELECT DISTINCT ON (as_of_date)
            as_of_date,
            created_at,
            target_positions,
            metadata
        FROM target_portfolios
        WHERE portfolio_id = %s
          AND as_of_date >= %s
          AND as_of_date <= %s
        ORDER BY as_of_date ASC, created_at DESC
    """

    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, (str(args.portfolio_id), args.start, args.end))
            rows = cur.fetchall()
        finally:
            cur.close()

    print(
        "as_of_date,created_at,portfolio_id,market_situation,fragility_score,"
        "hedge_allocation,long_allocation,cash_weight,net_exposure,gross_exposure,"
        "risk_num_capped,risk_num_rejected"
    )

    for as_of_date_db, created_at_db, positions_raw, meta_raw in rows:
        as_of_s = as_of_date_db.isoformat() if isinstance(as_of_date_db, date) else ""
        created_s = created_at_db.isoformat() if isinstance(created_at_db, datetime) else ""

        meta: dict[str, Any] = meta_raw if isinstance(meta_raw, dict) else {}
        positions: dict[str, Any] = positions_raw if isinstance(positions_raw, dict) else {}
        weights_raw: dict[str, Any] = positions.get("weights") if isinstance(positions.get("weights"), dict) else {}

        weights: dict[str, float] = {}
        for k, v in weights_raw.items():
            try:
                weights[str(k)] = float(v)
            except Exception:
                continue

        net = sum(weights.values())
        gross = sum(abs(w) for w in weights.values())

        market_situation = _get_str(meta, "market_situation")
        if market_situation is None:
            mb = meta.get("meta_budget")
            if isinstance(mb, dict):
                market_situation = _get_str(mb, "market_situation")

        fragility_score = _get_float(meta, "fragility_score")
        hedge_alloc = _get_float(meta, "hedge_allocation")
        long_alloc = _get_float(meta, "long_allocation")
        cash_weight = _get_float(meta, "cash_weight")
        num_capped = _get_float(meta, "risk_num_capped")
        num_rejected = _get_float(meta, "risk_num_rejected")

        def _fmt(x: float | None) -> str:
            return "" if x is None else f"{x:.6f}"

        print(
            f"{as_of_s},{created_s},{args.portfolio_id},{market_situation or ''},{_fmt(fragility_score)},"
            f"{_fmt(hedge_alloc)},{_fmt(long_alloc)},{_fmt(cash_weight)},"
            f"{net:.6f},{gross:.6f},{_fmt(num_capped)},{_fmt(num_rejected)}"
        )


if __name__ == "__main__":  # pragma: no cover
    main()
