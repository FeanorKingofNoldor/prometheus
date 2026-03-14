"""Prometheus v2 – Show positions_snapshots status (Layer 4 validation).

Validates basic Layer 4 contracts for ``positions_snapshots``:
- portfolio_id, instrument_id, mode are non-empty
- mode is in a controlled set (LIVE/PAPER/BACKTEST)
- quantity/avg_cost/market_value/unrealized_pnl are finite
- as_of_date is present

Reports results for both runtime_db and historical_db.

Note: position arithmetic consistency is a higher-level audit.
"""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from typing import Any, Optional, Sequence

from apathis.core.database import get_db_manager


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
            MIN(as_of_date) AS min_as_of_date,
            MAX(as_of_date) AS max_as_of_date,
            SUM(CASE WHEN btrim(portfolio_id) = '' THEN 1 ELSE 0 END) AS empty_portfolio_id,
            SUM(CASE WHEN btrim(instrument_id) = '' THEN 1 ELSE 0 END) AS empty_instrument_id,
            SUM(CASE WHEN btrim(mode) = '' THEN 1 ELSE 0 END) AS empty_mode,
            SUM(CASE WHEN mode NOT IN {tuple(_ALLOWED_MODES)!r} THEN 1 ELSE 0 END) AS bad_mode,
            SUM(CASE WHEN quantity IN {_NONFINITE} THEN 1 ELSE 0 END) AS quantity_nonfinite,
            SUM(CASE WHEN avg_cost IN {_NONFINITE} THEN 1 ELSE 0 END) AS avg_cost_nonfinite,
            SUM(CASE WHEN market_value IN {_NONFINITE} THEN 1 ELSE 0 END) AS market_value_nonfinite,
            SUM(CASE WHEN unrealized_pnl IN {_NONFINITE} THEN 1 ELSE 0 END) AS unrealized_pnl_nonfinite
        FROM positions_snapshots
    """

    sql_preview = """
        SELECT timestamp, portfolio_id, as_of_date, instrument_id, quantity, market_value, mode
        FROM positions_snapshots
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
                min_as_of_date,
                max_as_of_date,
                empty_portfolio_id,
                empty_instrument_id,
                empty_mode,
                bad_mode,
                quantity_nonfinite,
                avg_cost_nonfinite,
                market_value_nonfinite,
                unrealized_pnl_nonfinite,
            ) = cur.fetchone()

            cur.execute(sql_preview)
            preview_rows = cur.fetchall()
        finally:
            cur.close()

    preview = [
        {
            "timestamp": ts.isoformat() if isinstance(ts, datetime) else None,
            "portfolio_id": str(portfolio_id),
            "as_of_date": as_of_date_db.isoformat() if isinstance(as_of_date_db, date) else None,
            "instrument_id": str(instrument_id),
            "quantity": float(quantity) if quantity is not None else None,
            "market_value": float(market_value) if market_value is not None else None,
            "mode": str(mode),
        }
        for ts, portfolio_id, as_of_date_db, instrument_id, quantity, market_value, mode in preview_rows
    ]

    checks_passed = (
        int(empty_portfolio_id or 0) == 0
        and int(empty_instrument_id or 0) == 0
        and int(empty_mode or 0) == 0
        and int(bad_mode or 0) == 0
        and int(quantity_nonfinite or 0) == 0
        and int(avg_cost_nonfinite or 0) == 0
        and int(market_value_nonfinite or 0) == 0
        and int(unrealized_pnl_nonfinite or 0) == 0
    )

    return {
        "total_rows": int(total or 0),
        "min_timestamp": min_timestamp.isoformat() if isinstance(min_timestamp, datetime) else None,
        "max_timestamp": max_timestamp.isoformat() if isinstance(max_timestamp, datetime) else None,
        "min_as_of_date": min_as_of_date.isoformat() if isinstance(min_as_of_date, date) else None,
        "max_as_of_date": max_as_of_date.isoformat() if isinstance(max_as_of_date, date) else None,
        "empty_portfolio_id_rows": int(empty_portfolio_id or 0),
        "empty_instrument_id_rows": int(empty_instrument_id or 0),
        "empty_mode_rows": int(empty_mode or 0),
        "bad_mode_rows": int(bad_mode or 0),
        "quantity_nonfinite_rows": int(quantity_nonfinite or 0),
        "avg_cost_nonfinite_rows": int(avg_cost_nonfinite or 0),
        "market_value_nonfinite_rows": int(market_value_nonfinite or 0),
        "unrealized_pnl_nonfinite_rows": int(unrealized_pnl_nonfinite or 0),
        "recent_rows_preview": preview,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Show positions_snapshots status and basic Layer 4 validation checks"
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
