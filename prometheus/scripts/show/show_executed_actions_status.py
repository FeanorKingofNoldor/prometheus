"""Prometheus v2 – Show executed_actions status (Layer 3 validation).

Validates basic Layer 3 contracts for ``executed_actions``:
- action_id and side are non-empty
- side is in a controlled set (BUY/SELL)
- quantity and price are finite; quantity > 0; price >= 0
- slippage/fees are finite when present
- decision_id/run_id/portfolio_id/instrument_id are either NULL or non-empty
- metadata is either NULL or a JSON object
- instrument_id exists in instruments for canonical-looking IDs (contains
  a dot, e.g. "AAPL.US"), when the target DB has instruments populated

Reports results for both runtime_db and historical_db.

Note: execution semantics and broker-layer constraints are higher-level audits.
"""

from __future__ import annotations

import argparse
import json
from datetime import date
from typing import Any, Optional, Sequence

from apathis.core.database import get_db_manager


_ALLOWED_SIDES = ("BUY", "SELL")
_NONFINITE = "('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision)"


def _summarise(db, which: str) -> dict[str, Any]:
    if which == "runtime":
        conn_cm = db.get_runtime_connection()
    elif which == "historical":
        conn_cm = db.get_historical_connection()
    else:  # pragma: no cover
        raise ValueError(f"Unknown db selector: {which!r}")

    sql_instruments_total = "SELECT COUNT(*) FROM instruments"

    sql_without_instruments_ref = f"""
        SELECT
            COUNT(*) AS total,
            MIN(ea.trade_date) AS min_trade_date,
            MAX(ea.trade_date) AS max_trade_date,
            SUM(CASE WHEN btrim(ea.action_id) = '' THEN 1 ELSE 0 END) AS empty_action_id,
            SUM(CASE WHEN btrim(ea.side) = '' THEN 1 ELSE 0 END) AS empty_side,
            SUM(CASE WHEN ea.side NOT IN {tuple(_ALLOWED_SIDES)!r} THEN 1 ELSE 0 END) AS bad_side,
            SUM(CASE WHEN ea.quantity IN {_NONFINITE} THEN 1 ELSE 0 END) AS quantity_nonfinite,
            SUM(CASE WHEN ea.quantity <= 0.0 THEN 1 ELSE 0 END) AS quantity_nonpositive,
            SUM(CASE WHEN ea.price IN {_NONFINITE} THEN 1 ELSE 0 END) AS price_nonfinite,
            SUM(CASE WHEN ea.price < 0.0 THEN 1 ELSE 0 END) AS price_negative,
            SUM(CASE WHEN ea.slippage IS NOT NULL AND ea.slippage IN {_NONFINITE} THEN 1 ELSE 0 END) AS slippage_nonfinite,
            SUM(CASE WHEN ea.fees IS NOT NULL AND ea.fees IN {_NONFINITE} THEN 1 ELSE 0 END) AS fees_nonfinite,
            SUM(CASE WHEN ea.decision_id IS NOT NULL AND btrim(ea.decision_id) = '' THEN 1 ELSE 0 END) AS empty_decision_id,
            SUM(CASE WHEN ea.run_id IS NOT NULL AND btrim(ea.run_id) = '' THEN 1 ELSE 0 END) AS empty_run_id,
            SUM(CASE WHEN ea.portfolio_id IS NOT NULL AND btrim(ea.portfolio_id) = '' THEN 1 ELSE 0 END) AS empty_portfolio_id,
            SUM(CASE WHEN ea.instrument_id IS NOT NULL AND btrim(ea.instrument_id) = '' THEN 1 ELSE 0 END) AS empty_instrument_id,
            SUM(CASE WHEN ea.instrument_id IS NOT NULL AND ea.instrument_id NOT LIKE '%.%' THEN 1 ELSE 0 END) AS noncanonical_instrument_id,
            SUM(CASE WHEN ea.metadata IS NOT NULL AND jsonb_typeof(ea.metadata) <> 'object' THEN 1 ELSE 0 END) AS metadata_not_object
        FROM executed_actions ea
    """

    sql_with_instruments_ref = f"""
        SELECT
            COUNT(*) AS total,
            MIN(ea.trade_date) AS min_trade_date,
            MAX(ea.trade_date) AS max_trade_date,
            SUM(CASE WHEN btrim(ea.action_id) = '' THEN 1 ELSE 0 END) AS empty_action_id,
            SUM(CASE WHEN btrim(ea.side) = '' THEN 1 ELSE 0 END) AS empty_side,
            SUM(CASE WHEN ea.side NOT IN {tuple(_ALLOWED_SIDES)!r} THEN 1 ELSE 0 END) AS bad_side,
            SUM(CASE WHEN ea.quantity IN {_NONFINITE} THEN 1 ELSE 0 END) AS quantity_nonfinite,
            SUM(CASE WHEN ea.quantity <= 0.0 THEN 1 ELSE 0 END) AS quantity_nonpositive,
            SUM(CASE WHEN ea.price IN {_NONFINITE} THEN 1 ELSE 0 END) AS price_nonfinite,
            SUM(CASE WHEN ea.price < 0.0 THEN 1 ELSE 0 END) AS price_negative,
            SUM(CASE WHEN ea.slippage IS NOT NULL AND ea.slippage IN {_NONFINITE} THEN 1 ELSE 0 END) AS slippage_nonfinite,
            SUM(CASE WHEN ea.fees IS NOT NULL AND ea.fees IN {_NONFINITE} THEN 1 ELSE 0 END) AS fees_nonfinite,
            SUM(CASE WHEN ea.decision_id IS NOT NULL AND btrim(ea.decision_id) = '' THEN 1 ELSE 0 END) AS empty_decision_id,
            SUM(CASE WHEN ea.run_id IS NOT NULL AND btrim(ea.run_id) = '' THEN 1 ELSE 0 END) AS empty_run_id,
            SUM(CASE WHEN ea.portfolio_id IS NOT NULL AND btrim(ea.portfolio_id) = '' THEN 1 ELSE 0 END) AS empty_portfolio_id,
            SUM(CASE WHEN ea.instrument_id IS NOT NULL AND btrim(ea.instrument_id) = '' THEN 1 ELSE 0 END) AS empty_instrument_id,
            SUM(CASE WHEN ea.instrument_id IS NOT NULL AND ea.instrument_id NOT LIKE '%.%' THEN 1 ELSE 0 END) AS noncanonical_instrument_id,
            SUM(CASE WHEN ea.metadata IS NOT NULL AND jsonb_typeof(ea.metadata) <> 'object' THEN 1 ELSE 0 END) AS metadata_not_object,
            SUM(
                CASE
                    WHEN ea.instrument_id IS NOT NULL
                     AND ea.instrument_id LIKE '%.%'
                     AND i.instrument_id IS NULL
                    THEN 1
                    ELSE 0
                END
            ) AS orphan_canonical_instrument_id
        FROM executed_actions ea
        LEFT JOIN instruments i ON i.instrument_id = ea.instrument_id
    """

    sql_preview = """
        SELECT trade_date, portfolio_id, instrument_id, side, quantity, price, decision_id
        FROM executed_actions
        ORDER BY trade_date DESC, created_at DESC
        LIMIT 25
    """

    with conn_cm as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql_instruments_total)
            instruments_total = int(cur.fetchone()[0] or 0)

            instrument_reference_check_skipped = instruments_total == 0

            if instrument_reference_check_skipped:
                cur.execute(sql_without_instruments_ref)
                (
                    total,
                    min_trade_date,
                    max_trade_date,
                    empty_action_id,
                    empty_side,
                    bad_side,
                    quantity_nonfinite,
                    quantity_nonpositive,
                    price_nonfinite,
                    price_negative,
                    slippage_nonfinite,
                    fees_nonfinite,
                    empty_decision_id,
                    empty_run_id,
                    empty_portfolio_id,
                    empty_instrument_id,
                    noncanonical_instrument_id,
                    metadata_not_object,
                ) = cur.fetchone()
                orphan_canonical_instrument_id = 0
            else:
                cur.execute(sql_with_instruments_ref)
                (
                    total,
                    min_trade_date,
                    max_trade_date,
                    empty_action_id,
                    empty_side,
                    bad_side,
                    quantity_nonfinite,
                    quantity_nonpositive,
                    price_nonfinite,
                    price_negative,
                    slippage_nonfinite,
                    fees_nonfinite,
                    empty_decision_id,
                    empty_run_id,
                    empty_portfolio_id,
                    empty_instrument_id,
                    noncanonical_instrument_id,
                    metadata_not_object,
                    orphan_canonical_instrument_id,
                ) = cur.fetchone()

            cur.execute(sql_preview)
            preview_rows = cur.fetchall()
        finally:
            cur.close()

    preview = [
        {
            "trade_date": trade_date_db.isoformat() if isinstance(trade_date_db, date) else None,
            "portfolio_id": str(portfolio_id) if portfolio_id is not None else None,
            "instrument_id": str(instrument_id) if instrument_id is not None else None,
            "side": str(side),
            "quantity": float(quantity) if quantity is not None else None,
            "price": float(price) if price is not None else None,
            "decision_id": str(decision_id) if decision_id is not None else None,
        }
        for trade_date_db, portfolio_id, instrument_id, side, quantity, price, decision_id in preview_rows
    ]

    checks_passed = (
        int(empty_action_id or 0) == 0
        and int(empty_side or 0) == 0
        and int(bad_side or 0) == 0
        and int(quantity_nonfinite or 0) == 0
        and int(quantity_nonpositive or 0) == 0
        and int(price_nonfinite or 0) == 0
        and int(price_negative or 0) == 0
        and int(slippage_nonfinite or 0) == 0
        and int(fees_nonfinite or 0) == 0
        and int(empty_decision_id or 0) == 0
        and int(empty_run_id or 0) == 0
        and int(empty_portfolio_id or 0) == 0
        and int(empty_instrument_id or 0) == 0
        and int(metadata_not_object or 0) == 0
        and (
            instrument_reference_check_skipped
            or int(orphan_canonical_instrument_id or 0) == 0
        )
    )

    return {
        "total_rows": int(total or 0),
        "min_trade_date": min_trade_date.isoformat() if isinstance(min_trade_date, date) else None,
        "max_trade_date": max_trade_date.isoformat() if isinstance(max_trade_date, date) else None,
        "empty_action_id_rows": int(empty_action_id or 0),
        "empty_side_rows": int(empty_side or 0),
        "bad_side_rows": int(bad_side or 0),
        "quantity_nonfinite_rows": int(quantity_nonfinite or 0),
        "quantity_nonpositive_rows": int(quantity_nonpositive or 0),
        "price_nonfinite_rows": int(price_nonfinite or 0),
        "price_negative_rows": int(price_negative or 0),
        "slippage_nonfinite_rows": int(slippage_nonfinite or 0),
        "fees_nonfinite_rows": int(fees_nonfinite or 0),
        "empty_decision_id_rows": int(empty_decision_id or 0),
        "empty_run_id_rows": int(empty_run_id or 0),
        "empty_portfolio_id_rows": int(empty_portfolio_id or 0),
        "empty_instrument_id_rows": int(empty_instrument_id or 0),
        "noncanonical_instrument_id_rows": int(noncanonical_instrument_id or 0),
        "metadata_not_object_rows": int(metadata_not_object or 0),
        "orphan_canonical_instrument_id_rows": int(orphan_canonical_instrument_id or 0),
        "instrument_table_total_rows": int(instruments_total),
        "instrument_reference_check_skipped": bool(instrument_reference_check_skipped),
        "recent_rows_preview": preview,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Show executed_actions status and basic Layer 3 validation checks"
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
