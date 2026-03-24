"""Prometheus v2 – Execution status inspection CLI.

This script prints a compact summary of recent execution activity for a
single portfolio, combining information from the ``orders``, ``fills``,
and ``positions_snapshots`` tables.

It is intended as an operator tool for quickly answering questions like:
- "What orders did we just send for portfolio X?"
- "What fills have we received in PAPER/LIVE?"
- "What does the latest positions snapshot look like?"

Example
-------

    python -m prometheus.scripts.show_execution_status \
        --portfolio-id US_CORE_LONG_EQ \
        --mode PAPER \
        --limit-orders 25 \
        --limit-fills 25
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
        description=(
            "Show recent execution activity for a portfolio from orders, "
            "fills, and positions_snapshots."
        ),
    )

    parser.add_argument(
        "--portfolio-id",
        type=str,
        required=True,
        help="Portfolio identifier (orders.portfolio_id / positions_snapshots.portfolio_id)",
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=["LIVE", "PAPER", "BACKTEST"],
        default=None,
        help="Optional execution mode filter (LIVE/PAPER/BACKTEST)",
    )
    parser.add_argument(
        "--limit-orders",
        type=int,
        default=25,
        help="Maximum number of orders to show (default: 25)",
    )
    parser.add_argument(
        "--limit-fills",
        type=int,
        default=25,
        help="Maximum number of fills to show (default: 25)",
    )

    args = parser.parse_args(argv)

    if args.limit_orders <= 0:
        parser.error("--limit-orders must be positive")
    if args.limit_fills <= 0:
        parser.error("--limit-fills must be positive")

    return args


def _fmt_ts(value: object) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = _parse_args(argv)
    db_manager = get_db_manager()

    port_id = args.portfolio_id
    mode_norm = args.mode

    print(f"Execution status for portfolio_id={port_id!r} mode={mode_norm or 'ANY'}")
    print("".ljust(80, "="))

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            # Orders
            where_clauses = ["portfolio_id = %s"]
            params: list[object] = [port_id]
            if mode_norm is not None:
                where_clauses.append("mode = %s")
                params.append(mode_norm)
            where_sql = " WHERE " + " AND ".join(where_clauses)
            sql_orders = (
                "SELECT timestamp, order_id, instrument_id, side, order_type, "
                "quantity, status, mode "
                "FROM orders" + where_sql + " ORDER BY timestamp DESC LIMIT %s"
            )
            params.append(args.limit_orders)
            cursor.execute(sql_orders, tuple(params))
            order_rows = cursor.fetchall()

            # Fills (join to orders to filter by portfolio_id)
            where_clauses_f = ["o.portfolio_id = %s"]
            params_f: list[object] = [port_id]
            if mode_norm is not None:
                where_clauses_f.append("f.mode = %s")
                params_f.append(mode_norm)
            where_sql_f = " WHERE " + " AND ".join(where_clauses_f)
            sql_fills = (
                "SELECT f.timestamp, f.fill_id, f.order_id, f.instrument_id, "
                "f.side, f.quantity, f.price, f.commission, f.mode "
                "FROM fills f JOIN orders o ON o.order_id = f.order_id" +
                where_sql_f + " ORDER BY f.timestamp DESC LIMIT %s"
            )
            params_f.append(args.limit_fills)
            cursor.execute(sql_fills, tuple(params_f))
            fill_rows = cursor.fetchall()

            # Latest positions snapshot
            cursor.execute(
                """
                SELECT MAX(timestamp) FROM positions_snapshots
                WHERE portfolio_id = %s
                """,
                (port_id,),
            )
            row_ts = cursor.fetchone()
            latest_ts = row_ts[0] if row_ts is not None else None
            pos_rows: list[tuple] = []
            if latest_ts is not None:
                cursor.execute(
                    """
                    SELECT instrument_id, quantity, avg_cost, market_value,
                           unrealized_pnl, mode
                    FROM positions_snapshots
                    WHERE portfolio_id = %s AND timestamp = %s
                    ORDER BY instrument_id
                    """,
                    (port_id, latest_ts),
                )
                pos_rows = cursor.fetchall()
        finally:
            cursor.close()

    # Orders
    print("\nRecent orders:")
    if not order_rows:
        print("  (none)")
    else:
        print(
            "  timestamp,order_id,instrument_id,side,order_type,quantity,status,mode",
        )
        for (
            ts,
            order_id,
            instrument_id,
            side,
            order_type,
            quantity,
            status,
            mode,
        ) in order_rows:
            print(
                f"  {_fmt_ts(ts)},{order_id},{instrument_id},{side},{order_type},"
                f"{float(quantity):.6f},{status},{mode}",
            )

    # Fills
    print("\nRecent fills:")
    if not fill_rows:
        print("  (none)")
    else:
        print(
            "  timestamp,fill_id,order_id,instrument_id,side,quantity,price,commission,mode",
        )
        for (
            ts,
            fill_id,
            order_id,
            instrument_id,
            side,
            quantity,
            price,
            commission,
            mode,
        ) in fill_rows:
            print(
                f"  {_fmt_ts(ts)},{fill_id},{order_id},{instrument_id},{side},"
                f"{float(quantity):.6f},{float(price):.6f},{float(commission):.6f},{mode}",
            )

    # Positions
    print("\nLatest positions snapshot:")
    if latest_ts is None or not pos_rows:
        print("  (none)")
    else:
        print(f"  timestamp={_fmt_ts(latest_ts)}")
        print("  instrument_id,quantity,avg_cost,market_value,unrealized_pnl,mode")
        for (
            instrument_id,
            quantity,
            avg_cost,
            market_value,
            unrealized_pnl,
            mode,
        ) in pos_rows:
            print(
                f"  {instrument_id},{float(quantity):.6f},{float(avg_cost):.6f},"
                f"{float(market_value):.6f},{float(unrealized_pnl):.6f},{mode}",
            )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
