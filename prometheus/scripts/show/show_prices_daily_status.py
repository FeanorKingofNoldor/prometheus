"""Prometheus v2 – Show prices_daily status (Layer 1 validation).

Validates basic Layer 1 contracts for ``prices_daily``:
- unique per (instrument_id, trade_date) (enforced by PK; not checked here)
- OHLCV values are non-negative
- high >= low and open/close are within [low, high]
- currency values are non-empty and in a consistent format

Reports results for both runtime_db and historical_db.

Note: calendar correctness (trade_date matches market calendar) is a
higher-level audit.
"""

from __future__ import annotations

import argparse
import json
from datetime import date
from typing import Any, Optional, Sequence

from apatheon.core.database import get_db_manager


def _summarise(db, which: str) -> dict[str, Any]:
    if which == "runtime":
        conn_cm = db.get_runtime_connection()
    elif which == "historical":
        conn_cm = db.get_historical_connection()
    else:  # pragma: no cover
        raise ValueError(f"Unknown db selector: {which!r}")

    sql = """
        SELECT
            COUNT(*) AS total,
            COUNT(DISTINCT instrument_id) AS distinct_instruments,
            MIN(trade_date) AS min_trade_date,
            MAX(trade_date) AS max_trade_date,
            SUM(CASE WHEN EXTRACT(ISODOW FROM trade_date) IN (6,7) THEN 1 ELSE 0 END) AS weekend_trade_date,
            SUM(CASE WHEN btrim(instrument_id) = '' THEN 1 ELSE 0 END) AS empty_instrument_id,
            SUM(CASE WHEN open < 0 THEN 1 ELSE 0 END) AS neg_open,
            SUM(CASE WHEN high < 0 THEN 1 ELSE 0 END) AS neg_high,
            SUM(CASE WHEN low < 0 THEN 1 ELSE 0 END) AS neg_low,
            SUM(CASE WHEN close < 0 THEN 1 ELSE 0 END) AS neg_close,
            SUM(CASE WHEN adjusted_close < 0 THEN 1 ELSE 0 END) AS neg_adjusted_close,
            SUM(CASE WHEN volume < 0 THEN 1 ELSE 0 END) AS neg_volume,
            SUM(CASE WHEN high < low THEN 1 ELSE 0 END) AS high_lt_low,
            SUM(CASE WHEN open < low OR open > high THEN 1 ELSE 0 END) AS open_outside_range,
            SUM(CASE WHEN close < low OR close > high THEN 1 ELSE 0 END) AS close_outside_range,
            SUM(CASE WHEN currency IS NULL OR btrim(currency) = '' THEN 1 ELSE 0 END) AS empty_currency,
            SUM(CASE WHEN currency !~ '^[A-Z]{3}$' THEN 1 ELSE 0 END) AS bad_currency_format
        FROM prices_daily
    """

    sql_preview = """
        SELECT instrument_id, trade_date, open, high, low, close, adjusted_close, volume, currency
        FROM prices_daily
        ORDER BY trade_date DESC, instrument_id
        LIMIT 25
    """

    with conn_cm as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            (
                total,
                distinct_instruments,
                min_trade_date,
                max_trade_date,
                weekend_trade_date,
                empty_instrument_id,
                neg_open,
                neg_high,
                neg_low,
                neg_close,
                neg_adjusted_close,
                neg_volume,
                high_lt_low,
                open_outside_range,
                close_outside_range,
                empty_currency,
                bad_currency_format,
            ) = cur.fetchone()

            cur.execute(sql_preview)
            preview_rows = cur.fetchall()
        finally:
            cur.close()

    preview = [
        {
            "instrument_id": str(iid),
            "trade_date": td.isoformat() if isinstance(td, date) else None,
            "open": float(o) if o is not None else None,
            "high": float(h) if h is not None else None,
            "low": float(low_price) if low_price is not None else None,
            "close": float(c) if c is not None else None,
            "adjusted_close": float(ac) if ac is not None else None,
            "volume": float(v) if v is not None else None,
            "currency": str(cur),
        }
        for iid, td, o, h, low_price, c, ac, v, cur in preview_rows
    ]

    checks_passed = (
        int(weekend_trade_date or 0) == 0
        and int(empty_instrument_id or 0) == 0
        and int(neg_open or 0) == 0
        and int(neg_high or 0) == 0
        and int(neg_low or 0) == 0
        and int(neg_close or 0) == 0
        and int(neg_adjusted_close or 0) == 0
        and int(neg_volume or 0) == 0
        and int(high_lt_low or 0) == 0
        and int(open_outside_range or 0) == 0
        and int(close_outside_range or 0) == 0
        and int(empty_currency or 0) == 0
        and int(bad_currency_format or 0) == 0
    )

    return {
        "total_rows": int(total or 0),
        "distinct_instruments": int(distinct_instruments or 0),
        "min_trade_date": min_trade_date.isoformat() if isinstance(min_trade_date, date) else None,
        "max_trade_date": max_trade_date.isoformat() if isinstance(max_trade_date, date) else None,
        "weekend_trade_date_rows": int(weekend_trade_date or 0),
        "empty_instrument_id_rows": int(empty_instrument_id or 0),
        "negative_open_rows": int(neg_open or 0),
        "negative_high_rows": int(neg_high or 0),
        "negative_low_rows": int(neg_low or 0),
        "negative_close_rows": int(neg_close or 0),
        "negative_adjusted_close_rows": int(neg_adjusted_close or 0),
        "negative_volume_rows": int(neg_volume or 0),
        "high_less_than_low_rows": int(high_lt_low or 0),
        "open_outside_range_rows": int(open_outside_range or 0),
        "close_outside_range_rows": int(close_outside_range or 0),
        "empty_currency_rows": int(empty_currency or 0),
        "bad_currency_format_rows": int(bad_currency_format or 0),
        "recent_rows_preview": preview,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Show prices_daily status and basic Layer 1 validation checks")
    parser.parse_args(argv)

    db = get_db_manager()

    report = {
        "runtime": _summarise(db, "runtime"),
        "historical": _summarise(db, "historical"),
    }

    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":  # pragma: no cover
    main()
