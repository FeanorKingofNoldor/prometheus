"""Prometheus v2 – Show news_articles status (Layer 1 validation).

Validates basic Layer 1 contracts for ``news_articles``:
- timestamps exist
- source is non-empty
- headline is non-empty
- language is either NULL or non-empty
- metadata is either NULL or a JSON object

Reports results for both runtime_db and historical_db.

Note: content quality, deduplication, and downstream entity-linking
quality are higher-level audits.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
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
            MIN(timestamp) AS min_ts,
            MAX(timestamp) AS max_ts,
            SUM(CASE WHEN btrim(source) = '' THEN 1 ELSE 0 END) AS empty_source,
            SUM(CASE WHEN btrim(headline) = '' THEN 1 ELSE 0 END) AS empty_headline,
            SUM(CASE WHEN language IS NOT NULL AND btrim(language) = '' THEN 1 ELSE 0 END) AS empty_language,
            SUM(CASE WHEN metadata IS NOT NULL AND jsonb_typeof(metadata) <> 'object' THEN 1 ELSE 0 END) AS metadata_not_object
        FROM news_articles
    """

    sql_preview = """
        SELECT article_id, timestamp, source, language, headline
        FROM news_articles
        ORDER BY timestamp DESC, article_id DESC
        LIMIT 25
    """

    with conn_cm as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            (
                total,
                min_ts,
                max_ts,
                empty_source,
                empty_headline,
                empty_language,
                metadata_not_object,
            ) = cur.fetchone()

            cur.execute(sql_preview)
            preview_rows = cur.fetchall()
        finally:
            cur.close()

    preview = []
    for article_id, ts, source, language, headline in preview_rows:
        preview.append(
            {
                "article_id": int(article_id) if article_id is not None else None,
                "timestamp": ts.isoformat() if isinstance(ts, datetime) else None,
                "source": str(source),
                "language": str(language) if language is not None else None,
                "headline": str(headline),
            }
        )

    checks_passed = (
        int(empty_source or 0) == 0
        and int(empty_headline or 0) == 0
        and int(empty_language or 0) == 0
        and int(metadata_not_object or 0) == 0
    )

    return {
        "total_rows": int(total or 0),
        "min_timestamp": min_ts.isoformat() if isinstance(min_ts, datetime) else None,
        "max_timestamp": max_ts.isoformat() if isinstance(max_ts, datetime) else None,
        "empty_source_rows": int(empty_source or 0),
        "empty_headline_rows": int(empty_headline or 0),
        "empty_language_rows": int(empty_language or 0),
        "metadata_not_object_rows": int(metadata_not_object or 0),
        "recent_rows_preview": preview,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Show news_articles status and basic Layer 1 validation checks")
    parser.parse_args(argv)

    db = get_db_manager()

    report = {
        "runtime": _summarise(db, "runtime"),
        "historical": _summarise(db, "historical"),
    }

    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":  # pragma: no cover
    main()
