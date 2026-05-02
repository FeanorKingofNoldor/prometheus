"""Prometheus v2 – Orders inspection CLI.

This script prints recent entries from the ``orders`` table for quick
inspection of execution activity. It is intended for debugging the
execution bridge in BACKTEST mode and, later, in PAPER/LIVE modes.

Example
-------

    # Show recent BACKTEST orders for a portfolio
    python -m prometheus.scripts.show_orders \
        --portfolio-id TEST_PORTFOLIO \
        --mode BACKTEST \
        --limit 50
"""

from __future__ import annotations

import argparse
from datetime import datetime
from typing import Optional, Sequence

from apatheon.core.database import get_db_manager
from apatheon.core.logging import get_logger

logger = get_logger(__name__)


def _parse_args(argv: Optional[Sequence[str]]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Show recent rows from the orders table (execution core)",
    )

    parser.add_argument(
        "--portfolio-id",
        type=str,
        default=None,
        help="Optional portfolio_id filter",
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
        default=100,
        help="Maximum number of rows to display (default: 100)",
    )

    args = parser.parse_args(argv)

    if args.limit <= 0:
        parser.error("--limit must be positive")

    return args


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = _parse_args(argv)
    db_manager = get_db_manager()

    where_clauses = []
    params: list[object] = []

    if args.portfolio_id is not None:
        where_clauses.append("portfolio_id = %s")
        params.append(args.portfolio_id)

    if args.mode is not None:
        where_clauses.append("mode = %s")
        params.append(args.mode)

    where_sql = ""
    if where_clauses:
        where_sql = " WHERE " + " AND ".join(where_clauses)

    sql = (
        "SELECT timestamp, portfolio_id, instrument_id, side, order_type, "
        "quantity, status, mode, decision_id "
        "FROM orders" + where_sql + " ORDER BY timestamp DESC LIMIT %s"
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
        "timestamp,portfolio_id,instrument_id,side,order_type,quantity,status,mode,decision_id",
    )
    for (
        ts,
        portfolio_id,
        instrument_id,
        side,
        order_type,
        quantity,
        status,
        mode,
        decision_id,
    ) in rows:
        ts_str = ts.isoformat() if isinstance(ts, datetime) else str(ts)
        print(
            f"{ts_str},{portfolio_id or ''},{instrument_id},"  # portfolio_id may be NULL
            f"{side},{order_type},{float(quantity):.6f},{status},{mode},{decision_id or ''}",
        )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
