"""Prometheus v2 – Show news_links status (Layer 1 validation).

Validates basic Layer 1 contracts for ``news_links``:
- issuer_id and instrument_id are non-empty
- all rows reference an existing news_articles.article_id (FK should enforce)

Reports results for both runtime_db and historical_db.

Note: whether links are *correct* is a higher-level audit.
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

    sql = """
        SELECT
            COUNT(*) AS total,
            COUNT(DISTINCT article_id) AS distinct_articles,
            COUNT(DISTINCT issuer_id) AS distinct_issuers,
            COUNT(DISTINCT instrument_id) AS distinct_instruments,
            SUM(CASE WHEN btrim(issuer_id) = '' THEN 1 ELSE 0 END) AS empty_issuer_id,
            SUM(CASE WHEN btrim(instrument_id) = '' THEN 1 ELSE 0 END) AS empty_instrument_id
        FROM news_links
    """

    sql_orphans = """
        SELECT COUNT(*)
        FROM news_links AS nl
        WHERE NOT EXISTS (
            SELECT 1
            FROM news_articles AS na
            WHERE na.article_id = nl.article_id
        )
    """

    sql_preview = """
        SELECT article_id, issuer_id, instrument_id
        FROM news_links
        ORDER BY article_id DESC
        LIMIT 25
    """

    with conn_cm as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            (
                total,
                distinct_articles,
                distinct_issuers,
                distinct_instruments,
                empty_issuer_id,
                empty_instrument_id,
            ) = cur.fetchone()

            cur.execute(sql_orphans)
            orphan_rows = cur.fetchone()[0]

            cur.execute(sql_preview)
            preview_rows = cur.fetchall()
        finally:
            cur.close()

    preview = [
        {
            "article_id": int(aid) if aid is not None else None,
            "issuer_id": str(iid),
            "instrument_id": str(inst),
        }
        for aid, iid, inst in preview_rows
    ]

    checks_passed = (
        int(empty_issuer_id or 0) == 0
        and int(empty_instrument_id or 0) == 0
        and int(orphan_rows or 0) == 0
    )

    return {
        "total_rows": int(total or 0),
        "distinct_articles": int(distinct_articles or 0),
        "distinct_issuers": int(distinct_issuers or 0),
        "distinct_instruments": int(distinct_instruments or 0),
        "empty_issuer_id_rows": int(empty_issuer_id or 0),
        "empty_instrument_id_rows": int(empty_instrument_id or 0),
        "orphan_article_rows": int(orphan_rows or 0),
        "recent_rows_preview": preview,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Show news_links status and basic Layer 1 validation checks")
    parser.parse_args(argv)

    db = get_db_manager()

    report = {
        "runtime": _summarise(db, "runtime"),
        "historical": _summarise(db, "historical"),
    }

    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":  # pragma: no cover
    main()
