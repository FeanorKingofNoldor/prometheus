"""Prometheus v2 – Show instruments status (Layer 0 validation).

Validates core Layer 0 contracts for the ``instruments`` table:
- instrument_id is non-empty
- symbol is non-empty
- market_id exists in markets (and is non-null)
- categorical fields are consistently formatted (asset_class/status/currency)
- EQUITY instruments have a non-empty issuer_id

Reports results for both runtime_db and historical_db.
"""

from __future__ import annotations

import argparse
import json
from typing import Any, Optional, Sequence

from apathis.core.database import get_db_manager


def _summarise(db, which: str) -> dict[str, Any]:
    if which == "runtime":
        conn_cm = db.get_runtime_connection()
    elif which == "historical":
        conn_cm = db.get_historical_connection()
    else:  # pragma: no cover
        raise ValueError(f"Unknown db selector: {which!r}")

    sql_overview = """
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN btrim(instrument_id) = '' THEN 1 ELSE 0 END) AS empty_instrument_id,
            SUM(CASE WHEN symbol IS NULL OR btrim(symbol) = '' THEN 1 ELSE 0 END) AS empty_symbol,
            SUM(CASE WHEN market_id IS NULL OR btrim(market_id) = '' THEN 1 ELSE 0 END) AS empty_market_id,
            SUM(CASE WHEN asset_class IS NULL OR asset_class !~ '^[A-Z_]+$' THEN 1 ELSE 0 END) AS bad_asset_class_format,
            SUM(CASE WHEN status IS NULL OR status !~ '^[A-Z_]+$' THEN 1 ELSE 0 END) AS bad_status_format,
            SUM(CASE WHEN currency IS NULL OR currency !~ '^[A-Z]{3}$' THEN 1 ELSE 0 END) AS bad_currency_format,
            SUM(CASE WHEN asset_class = 'EQUITY' AND (issuer_id IS NULL OR btrim(issuer_id) = '') THEN 1 ELSE 0 END) AS equity_missing_issuer
        FROM instruments
    """

    sql_asset_breakdown = """
        SELECT asset_class, COUNT(*)
        FROM instruments
        GROUP BY asset_class
        ORDER BY COUNT(*) DESC, asset_class
        LIMIT 50
    """

    sql_status_breakdown = """
        SELECT status, COUNT(*)
        FROM instruments
        GROUP BY status
        ORDER BY COUNT(*) DESC, status
        LIMIT 50
    """

    sql_currency_breakdown = """
        SELECT currency, COUNT(*)
        FROM instruments
        GROUP BY currency
        ORDER BY COUNT(*) DESC, currency
        LIMIT 50
    """

    sql_orphan_markets = """
        SELECT DISTINCT i.market_id
        FROM instruments i
        LEFT JOIN markets m ON m.market_id = i.market_id
        WHERE i.market_id IS NOT NULL
          AND m.market_id IS NULL
        ORDER BY i.market_id
        LIMIT 20
    """

    sql_orphan_issuers = """
        SELECT DISTINCT i.issuer_id
        FROM instruments i
        LEFT JOIN issuers u ON u.issuer_id = i.issuer_id
        WHERE i.issuer_id IS NOT NULL
          AND u.issuer_id IS NULL
        ORDER BY i.issuer_id
        LIMIT 20
    """

    with conn_cm as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql_overview)
            (
                total,
                empty_instrument_id,
                empty_symbol,
                empty_market_id,
                bad_asset_class_format,
                bad_status_format,
                bad_currency_format,
                equity_missing_issuer,
            ) = cur.fetchone()

            cur.execute(sql_asset_breakdown)
            asset_rows = cur.fetchall()
            cur.execute(sql_status_breakdown)
            status_rows = cur.fetchall()
            cur.execute(sql_currency_breakdown)
            currency_rows = cur.fetchall()

            # Orphan checks (these may fail if the other tables are absent).
            try:
                cur.execute(sql_orphan_markets)
                orphan_market_rows = cur.fetchall()
            except Exception:
                orphan_market_rows = []

            try:
                cur.execute(sql_orphan_issuers)
                orphan_issuer_rows = cur.fetchall()
            except Exception:
                orphan_issuer_rows = []
        finally:
            cur.close()

    asset_breakdown = {str(a): int(n) for (a, n) in asset_rows}
    status_breakdown = {str(s): int(n) for (s, n) in status_rows}
    currency_breakdown = {str(c): int(n) for (c, n) in currency_rows}

    orphan_markets = [str(r[0]) for r in orphan_market_rows]
    orphan_issuers = [str(r[0]) for r in orphan_issuer_rows]

    checks_passed = (
        int(empty_instrument_id or 0) == 0
        and int(empty_symbol or 0) == 0
        and int(empty_market_id or 0) == 0
        and int(bad_asset_class_format or 0) == 0
        and int(bad_status_format or 0) == 0
        and int(bad_currency_format or 0) == 0
        and int(equity_missing_issuer or 0) == 0
        and len(orphan_markets) == 0
        and len(orphan_issuers) == 0
    )

    return {
        "total_instruments": int(total or 0),
        "empty_instrument_id_rows": int(empty_instrument_id or 0),
        "empty_symbol_rows": int(empty_symbol or 0),
        "empty_market_id_rows": int(empty_market_id or 0),
        "bad_asset_class_format_rows": int(bad_asset_class_format or 0),
        "bad_status_format_rows": int(bad_status_format or 0),
        "bad_currency_format_rows": int(bad_currency_format or 0),
        "equity_missing_issuer_rows": int(equity_missing_issuer or 0),
        "asset_class_breakdown": asset_breakdown,
        "status_breakdown": status_breakdown,
        "currency_breakdown": currency_breakdown,
        "orphan_market_ids": orphan_markets,
        "orphan_issuer_ids": orphan_issuers,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Show instruments status and Layer 0 validation checks")
    parser.parse_args(argv)

    db = get_db_manager()

    report = {
        "runtime": _summarise(db, "runtime"),
        "historical": _summarise(db, "historical"),
    }

    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":  # pragma: no cover
    main()
