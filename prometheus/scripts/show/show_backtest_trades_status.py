"""Prometheus v2 – Show backtest_trades status (Layer 5 validation).

Validates basic Layer 5 contracts for ``backtest_trades``:
- run_id, ticker, direction are non-empty
- direction is in a controlled set (BUY/SELL or LONG/SHORT)
- size/price are finite and > 0
- optional IDs are either NULL or non-empty
- decision_metadata_json is either NULL or a JSON object

Reports results for both runtime_db and historical_db.

Note: trade linkage to backtest_runs is higher-level auditing.
"""

from __future__ import annotations

import argparse
import json
from datetime import date
from typing import Any, Optional, Sequence

from apatheon.core.database import get_db_manager

_ALLOWED_DIRECTIONS = ("BUY", "SELL", "LONG", "SHORT")
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
            COUNT(DISTINCT run_id) AS distinct_runs,
            MIN(trade_date) AS min_trade_date,
            MAX(trade_date) AS max_trade_date,
            SUM(CASE WHEN btrim(run_id) = '' THEN 1 ELSE 0 END) AS empty_run_id,
            SUM(CASE WHEN btrim(ticker) = '' THEN 1 ELSE 0 END) AS empty_ticker,
            SUM(CASE WHEN btrim(direction) = '' THEN 1 ELSE 0 END) AS empty_direction,
            SUM(CASE WHEN direction NOT IN {tuple(_ALLOWED_DIRECTIONS)!r} THEN 1 ELSE 0 END) AS bad_direction,
            SUM(CASE WHEN size IN {_NONFINITE} THEN 1 ELSE 0 END) AS size_nonfinite,
            SUM(CASE WHEN size <= 0.0 THEN 1 ELSE 0 END) AS size_nonpositive,
            SUM(CASE WHEN price IN {_NONFINITE} THEN 1 ELSE 0 END) AS price_nonfinite,
            SUM(CASE WHEN price <= 0.0 THEN 1 ELSE 0 END) AS price_nonpositive,
            SUM(CASE WHEN regime_id IS NOT NULL AND btrim(regime_id) = '' THEN 1 ELSE 0 END) AS empty_regime_id,
            SUM(CASE WHEN universe_id IS NOT NULL AND btrim(universe_id) = '' THEN 1 ELSE 0 END) AS empty_universe_id,
            SUM(CASE WHEN decision_metadata_json IS NOT NULL AND jsonb_typeof(decision_metadata_json) <> 'object' THEN 1 ELSE 0 END) AS decision_metadata_json_not_object
        FROM backtest_trades
    """

    sql_preview = """
        SELECT trade_date, run_id, ticker, direction, size, price
        FROM backtest_trades
        ORDER BY trade_date DESC, trade_id DESC
        LIMIT 25
    """

    with conn_cm as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            (
                total,
                distinct_runs,
                min_trade_date,
                max_trade_date,
                empty_run_id,
                empty_ticker,
                empty_direction,
                bad_direction,
                size_nonfinite,
                size_nonpositive,
                price_nonfinite,
                price_nonpositive,
                empty_regime_id,
                empty_universe_id,
                decision_metadata_json_not_object,
            ) = cur.fetchone()

            cur.execute(sql_preview)
            preview_rows = cur.fetchall()
        finally:
            cur.close()

    preview = [
        {
            "trade_date": trade_date_db.isoformat() if isinstance(trade_date_db, date) else None,
            "run_id": str(run_id),
            "ticker": str(ticker),
            "direction": str(direction),
            "size": float(size) if size is not None else None,
            "price": float(price) if price is not None else None,
        }
        for trade_date_db, run_id, ticker, direction, size, price in preview_rows
    ]

    checks_passed = (
        int(empty_run_id or 0) == 0
        and int(empty_ticker or 0) == 0
        and int(empty_direction or 0) == 0
        and int(bad_direction or 0) == 0
        and int(size_nonfinite or 0) == 0
        and int(size_nonpositive or 0) == 0
        and int(price_nonfinite or 0) == 0
        and int(price_nonpositive or 0) == 0
        and int(empty_regime_id or 0) == 0
        and int(empty_universe_id or 0) == 0
        and int(decision_metadata_json_not_object or 0) == 0
    )

    return {
        "total_rows": int(total or 0),
        "distinct_runs": int(distinct_runs or 0),
        "min_trade_date": min_trade_date.isoformat() if isinstance(min_trade_date, date) else None,
        "max_trade_date": max_trade_date.isoformat() if isinstance(max_trade_date, date) else None,
        "empty_run_id_rows": int(empty_run_id or 0),
        "empty_ticker_rows": int(empty_ticker or 0),
        "empty_direction_rows": int(empty_direction or 0),
        "bad_direction_rows": int(bad_direction or 0),
        "size_nonfinite_rows": int(size_nonfinite or 0),
        "size_nonpositive_rows": int(size_nonpositive or 0),
        "price_nonfinite_rows": int(price_nonfinite or 0),
        "price_nonpositive_rows": int(price_nonpositive or 0),
        "empty_regime_id_rows": int(empty_regime_id or 0),
        "empty_universe_id_rows": int(empty_universe_id or 0),
        "decision_metadata_json_not_object_rows": int(decision_metadata_json_not_object or 0),
        "recent_rows_preview": preview,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Show backtest_trades status and basic Layer 5 validation checks"
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
