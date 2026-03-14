"""Prometheus v2 – Fills inspection CLI.

This script prints recent entries from the ``fills`` table for quick
inspection of simulated or real executions. It is mode-aware and can be
filtered by portfolio via a join to the ``orders`` table.

Example
-------

    # Show recent BACKTEST fills for a portfolio
    python -m prometheus.scripts.show_fills \
        --portfolio-id TEST_PORTFOLIO \
        --mode BACKTEST \
        --limit 50
"""

from __future__ import annotations

import argparse
from datetime import datetime
from typing import Optional, Sequence

from apathis.core.database import get_db_manager
from apathis.core.logging import get_logger


logger = get_logger(__name__)


def _parse_args(argv: Optional[Sequence[str]]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Show recent rows from the fills table (execution core)",
    )

    parser.add_argument(
        "--portfolio-id",
        type=str,
        default=None,
        help="Optional portfolio_id filter (joins via orders table)",
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

    join_sql = ""
    if args.portfolio_id is not None:
        # Join fills to orders to filter by portfolio_id.
        join_sql = " JOIN orders o ON o.order_id = f.order_id "
        where_clauses.append("o.portfolio_id = %s")
        params.append(args.portfolio_id)

    if args.mode is not None:
        where_clauses.append("f.mode = %s")
        params.append(args.mode)

    where_sql = ""
    if where_clauses:
        where_sql = " WHERE " + " AND ".join(where_clauses)

    sql = (
        "SELECT f.timestamp, f.order_id, f.instrument_id, f.side, f.quantity, "
        "f.price, f.commission, f.mode "
        "FROM fills f" + join_sql + where_sql + " ORDER BY f.timestamp DESC LIMIT %s"
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

    print("timestamp,order_id,instrument_id,side,quantity,price,commission,mode")
    for ts, order_id, instrument_id, side, quantity, price, commission, mode in rows:
        ts_str = ts.isoformat() if isinstance(ts, datetime) else str(ts)
        print(
            f"{ts_str},{order_id},{instrument_id},{side},{float(quantity):.6f},{float(price):.6f},"
            f"{float(commission):.6f},{mode}",
        )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
