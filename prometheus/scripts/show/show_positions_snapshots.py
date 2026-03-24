"""Prometheus v2 – Positions snapshots inspection CLI.

This script prints rows from the ``positions_snapshots`` table for a
single portfolio. It is useful for validating that execution (orders and
fills) results in sensible holdings over time in BACKTEST, PAPER, or
LIVE modes.

Example
-------

    python -m prometheus.scripts.show_positions_snapshots \
        --portfolio-id TEST_PORTFOLIO \
        --mode BACKTEST \
        --limit 100
"""

from __future__ import annotations

import argparse
from datetime import date, datetime
from typing import Optional, Sequence

from apathis.core.database import get_db_manager
from apathis.core.logging import get_logger

logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:  # pragma: no cover - CLI validation
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


def _parse_args(argv: Optional[Sequence[str]]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Show positions_snapshots rows for a portfolio",
    )

    parser.add_argument(
        "--portfolio-id",
        type=str,
        required=True,
        help="Portfolio identifier (positions_snapshots.portfolio_id)",
    )
    parser.add_argument(
        "--as-of",
        type=_parse_date,
        default=None,
        help="Optional as_of_date filter (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=["LIVE", "PAPER", "BACKTEST"],
        default=None,
        help="Optional execution mode filter (LIVE/PAPER/BACKTEST)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=200,
        help="Maximum number of rows to display (default: 200)",
    )

    args = parser.parse_args(argv)

    if args.limit <= 0:
        parser.error("--limit must be positive")

    return args


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = _parse_args(argv)
    db_manager = get_db_manager()

    where_clauses = ["portfolio_id = %s"]
    params: list[object] = [args.portfolio_id]

    if args.as_of is not None:
        where_clauses.append("as_of_date = %s")
        params.append(args.as_of)

    if args.mode is not None:
        where_clauses.append("mode = %s")
        params.append(args.mode)

    where_sql = " WHERE " + " AND ".join(where_clauses)

    sql = (
        "SELECT portfolio_id, timestamp, as_of_date, instrument_id, quantity, "
        "avg_cost, market_value, unrealized_pnl, mode "
        "FROM positions_snapshots" + where_sql +
        " ORDER BY timestamp DESC, instrument_id LIMIT %s"
    )
    params.append(args.limit)

    rows: list[tuple] = []
    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, tuple(params))
            rows = cursor.fetchall()
        finally:
            cursor.close()

    print(
        "portfolio_id,timestamp,as_of_date,instrument_id,quantity,avg_cost,market_value,unrealized_pnl,mode",
    )
    for (
        portfolio_id,
        ts,
        as_of_date,
        instrument_id,
        quantity,
        avg_cost,
        market_value,
        unrealized_pnl,
        mode,
    ) in rows:
        ts_str = ts.isoformat() if isinstance(ts, datetime) else str(ts)
        as_of_str = as_of_date.isoformat() if isinstance(as_of_date, date) else str(as_of_date)
        print(
            f"{portfolio_id},{ts_str},{as_of_str},{instrument_id},{float(quantity):.6f},"
            f"{float(avg_cost):.6f},{float(market_value):.6f},{float(unrealized_pnl):.6f},{mode}",
        )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
