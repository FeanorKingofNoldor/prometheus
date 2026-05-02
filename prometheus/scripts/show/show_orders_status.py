"""Prometheus v2 – Show orders status (Layer 4 validation).

Validates basic Layer 4 contracts for ``orders``:
- order_id, instrument_id, side, order_type, status, mode are non-empty
- side is in a controlled set (BUY/SELL)
- order_type is in a controlled set (MARKET/LIMIT/STOP/STOP_LIMIT)
- status is in a controlled set (PENDING/FILLED/CANCELLED/REJECTED)
- mode is in a controlled set (LIVE/PAPER/BACKTEST)
- quantity is finite and > 0
- limit_price/stop_price are finite when present
- portfolio_id/decision_id are either NULL or non-empty
- metadata is either NULL or a JSON object

Reports results for both runtime_db and historical_db.

Note: order/fill reconciliation is a higher-level audit.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from typing import Any, Optional, Sequence

from apatheon.core.database import get_db_manager

_ALLOWED_SIDES = ("BUY", "SELL")
_ALLOWED_ORDER_TYPES = ("MARKET", "LIMIT", "STOP", "STOP_LIMIT")
_ALLOWED_STATUSES = ("PENDING", "SUBMITTED", "FILLED", "CANCELLED", "REJECTED")
_ALLOWED_MODES = ("LIVE", "PAPER", "BACKTEST")
_NONFINITE = "('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision)"


def _summarise(db, which: str) -> dict[str, Any]:
    if which == "runtime":
        conn_cm = db.get_runtime_connection()
    elif which == "historical":
        conn_cm = db.get_historical_connection()
    else:  # pragma: no cover
        raise ValueError(f"Unknown db selector: {which!r}")

    sql = f"""
        SELECT
            COUNT(*) AS total,
            MIN(timestamp) AS min_timestamp,
            MAX(timestamp) AS max_timestamp,
            SUM(CASE WHEN btrim(order_id) = '' THEN 1 ELSE 0 END) AS empty_order_id,
            SUM(CASE WHEN btrim(instrument_id) = '' THEN 1 ELSE 0 END) AS empty_instrument_id,
            SUM(CASE WHEN btrim(side) = '' THEN 1 ELSE 0 END) AS empty_side,
            SUM(CASE WHEN side NOT IN {tuple(_ALLOWED_SIDES)!r} THEN 1 ELSE 0 END) AS bad_side,
            SUM(CASE WHEN btrim(order_type) = '' THEN 1 ELSE 0 END) AS empty_order_type,
            SUM(CASE WHEN order_type NOT IN {tuple(_ALLOWED_ORDER_TYPES)!r} THEN 1 ELSE 0 END) AS bad_order_type,
            SUM(CASE WHEN btrim(status) = '' THEN 1 ELSE 0 END) AS empty_status,
            SUM(CASE WHEN status NOT IN {tuple(_ALLOWED_STATUSES)!r} THEN 1 ELSE 0 END) AS bad_status,
            SUM(CASE WHEN btrim(mode) = '' THEN 1 ELSE 0 END) AS empty_mode,
            SUM(CASE WHEN mode NOT IN {tuple(_ALLOWED_MODES)!r} THEN 1 ELSE 0 END) AS bad_mode,
            SUM(CASE WHEN quantity IN {_NONFINITE} THEN 1 ELSE 0 END) AS quantity_nonfinite,
            SUM(CASE WHEN quantity <= 0.0 THEN 1 ELSE 0 END) AS quantity_nonpositive,
            SUM(CASE WHEN limit_price IS NOT NULL AND limit_price IN {_NONFINITE} THEN 1 ELSE 0 END) AS limit_price_nonfinite,
            SUM(CASE WHEN stop_price IS NOT NULL AND stop_price IN {_NONFINITE} THEN 1 ELSE 0 END) AS stop_price_nonfinite,
            SUM(CASE WHEN portfolio_id IS NOT NULL AND btrim(portfolio_id) = '' THEN 1 ELSE 0 END) AS empty_portfolio_id,
            SUM(CASE WHEN decision_id IS NOT NULL AND btrim(decision_id) = '' THEN 1 ELSE 0 END) AS empty_decision_id,
            SUM(CASE WHEN metadata IS NOT NULL AND jsonb_typeof(metadata) <> 'object' THEN 1 ELSE 0 END) AS metadata_not_object
        FROM orders
    """

    sql_preview = """
        SELECT timestamp, order_id, instrument_id, side, order_type, quantity, status, mode
        FROM orders
        ORDER BY timestamp DESC
        LIMIT 25
    """

    with conn_cm as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            (
                total,
                min_timestamp,
                max_timestamp,
                empty_order_id,
                empty_instrument_id,
                empty_side,
                bad_side,
                empty_order_type,
                bad_order_type,
                empty_status,
                bad_status,
                empty_mode,
                bad_mode,
                quantity_nonfinite,
                quantity_nonpositive,
                limit_price_nonfinite,
                stop_price_nonfinite,
                empty_portfolio_id,
                empty_decision_id,
                metadata_not_object,
            ) = cur.fetchone()

            cur.execute(sql_preview)
            preview_rows = cur.fetchall()
        finally:
            cur.close()

    preview = [
        {
            "timestamp": ts.isoformat() if isinstance(ts, datetime) else None,
            "order_id": str(order_id),
            "instrument_id": str(instrument_id),
            "side": str(side),
            "order_type": str(order_type),
            "quantity": float(quantity) if quantity is not None else None,
            "status": str(status),
            "mode": str(mode),
        }
        for ts, order_id, instrument_id, side, order_type, quantity, status, mode in preview_rows
    ]

    checks_passed = (
        int(empty_order_id or 0) == 0
        and int(empty_instrument_id or 0) == 0
        and int(empty_side or 0) == 0
        and int(bad_side or 0) == 0
        and int(empty_order_type or 0) == 0
        and int(bad_order_type or 0) == 0
        and int(empty_status or 0) == 0
        and int(bad_status or 0) == 0
        and int(empty_mode or 0) == 0
        and int(bad_mode or 0) == 0
        and int(quantity_nonfinite or 0) == 0
        and int(quantity_nonpositive or 0) == 0
        and int(limit_price_nonfinite or 0) == 0
        and int(stop_price_nonfinite or 0) == 0
        and int(empty_portfolio_id or 0) == 0
        and int(empty_decision_id or 0) == 0
        and int(metadata_not_object or 0) == 0
    )

    return {
        "total_rows": int(total or 0),
        "min_timestamp": min_timestamp.isoformat() if isinstance(min_timestamp, datetime) else None,
        "max_timestamp": max_timestamp.isoformat() if isinstance(max_timestamp, datetime) else None,
        "empty_order_id_rows": int(empty_order_id or 0),
        "empty_instrument_id_rows": int(empty_instrument_id or 0),
        "empty_side_rows": int(empty_side or 0),
        "bad_side_rows": int(bad_side or 0),
        "empty_order_type_rows": int(empty_order_type or 0),
        "bad_order_type_rows": int(bad_order_type or 0),
        "empty_status_rows": int(empty_status or 0),
        "bad_status_rows": int(bad_status or 0),
        "empty_mode_rows": int(empty_mode or 0),
        "bad_mode_rows": int(bad_mode or 0),
        "quantity_nonfinite_rows": int(quantity_nonfinite or 0),
        "quantity_nonpositive_rows": int(quantity_nonpositive or 0),
        "limit_price_nonfinite_rows": int(limit_price_nonfinite or 0),
        "stop_price_nonfinite_rows": int(stop_price_nonfinite or 0),
        "empty_portfolio_id_rows": int(empty_portfolio_id or 0),
        "empty_decision_id_rows": int(empty_decision_id or 0),
        "metadata_not_object_rows": int(metadata_not_object or 0),
        "recent_rows_preview": preview,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Show orders status and basic Layer 4 validation checks"
    )
    parser.parse_args(argv)

    db = get_db_manager()

    report = {
        "runtime": _summarise(db, "runtime"),
        "historical": _summarise(db, "historical"),
    }

    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":  # pragma: no cover
    main()
