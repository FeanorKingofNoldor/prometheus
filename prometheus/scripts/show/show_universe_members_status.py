"""Prometheus v2 – Show universe_members status (Layer 3 validation).

Validates basic Layer 3 contracts for ``universe_members``:
- universe_member_id, universe_id, entity_type, entity_id, tier are non-empty
- tier is in a controlled set (CORE/SATELLITE/EXCLUDED)
- included/tier consistency (included implies tier != EXCLUDED)
- score is finite (no NaN/Inf)
- reasons is a JSON object
- entity_id exists in instruments when entity_type=INSTRUMENT and the
  target DB has instruments populated

Reports results for both runtime_db and historical_db.

Note: determinism and lookahead safety are higher-level audits.
"""

from __future__ import annotations

import argparse
import json
from datetime import date
from typing import Any, Optional, Sequence

from apathis.core.database import get_db_manager

_ALLOWED_TIERS = ("CORE", "SATELLITE", "EXCLUDED")
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
            COUNT(DISTINCT um.universe_id) AS distinct_universes,
            MIN(um.as_of_date) AS min_as_of_date,
            MAX(um.as_of_date) AS max_as_of_date,
            SUM(CASE WHEN btrim(um.universe_member_id) = '' THEN 1 ELSE 0 END) AS empty_universe_member_id,
            SUM(CASE WHEN btrim(um.universe_id) = '' THEN 1 ELSE 0 END) AS empty_universe_id,
            SUM(CASE WHEN btrim(um.entity_type) = '' THEN 1 ELSE 0 END) AS empty_entity_type,
            SUM(CASE WHEN btrim(um.entity_id) = '' THEN 1 ELSE 0 END) AS empty_entity_id,
            SUM(CASE WHEN btrim(um.tier) = '' THEN 1 ELSE 0 END) AS empty_tier,
            SUM(CASE WHEN um.tier NOT IN {tuple(_ALLOWED_TIERS)!r} THEN 1 ELSE 0 END) AS bad_tier,
            SUM(
                CASE
                    WHEN (um.included = TRUE AND um.tier = 'EXCLUDED')
                      OR (um.included = FALSE AND um.tier IN ('CORE', 'SATELLITE'))
                    THEN 1
                    ELSE 0
                END
            ) AS tier_included_inconsistent,
            SUM(CASE WHEN um.score IN {_NONFINITE} THEN 1 ELSE 0 END) AS score_nonfinite,
            SUM(CASE WHEN jsonb_typeof(um.reasons) <> 'object' THEN 1 ELSE 0 END) AS reasons_not_object
        FROM universe_members um
    """

    sql_with_instruments_ref = f"""
        SELECT
            COUNT(*) AS total,
            COUNT(DISTINCT um.universe_id) AS distinct_universes,
            MIN(um.as_of_date) AS min_as_of_date,
            MAX(um.as_of_date) AS max_as_of_date,
            SUM(CASE WHEN btrim(um.universe_member_id) = '' THEN 1 ELSE 0 END) AS empty_universe_member_id,
            SUM(CASE WHEN btrim(um.universe_id) = '' THEN 1 ELSE 0 END) AS empty_universe_id,
            SUM(CASE WHEN btrim(um.entity_type) = '' THEN 1 ELSE 0 END) AS empty_entity_type,
            SUM(CASE WHEN btrim(um.entity_id) = '' THEN 1 ELSE 0 END) AS empty_entity_id,
            SUM(CASE WHEN btrim(um.tier) = '' THEN 1 ELSE 0 END) AS empty_tier,
            SUM(CASE WHEN um.tier NOT IN {tuple(_ALLOWED_TIERS)!r} THEN 1 ELSE 0 END) AS bad_tier,
            SUM(
                CASE
                    WHEN (um.included = TRUE AND um.tier = 'EXCLUDED')
                      OR (um.included = FALSE AND um.tier IN ('CORE', 'SATELLITE'))
                    THEN 1
                    ELSE 0
                END
            ) AS tier_included_inconsistent,
            SUM(CASE WHEN um.score IN {_NONFINITE} THEN 1 ELSE 0 END) AS score_nonfinite,
            SUM(CASE WHEN jsonb_typeof(um.reasons) <> 'object' THEN 1 ELSE 0 END) AS reasons_not_object,
            SUM(
                CASE
                    WHEN um.entity_type = 'INSTRUMENT' AND i.instrument_id IS NULL
                    THEN 1
                    ELSE 0
                END
            ) AS orphan_instrument_id
        FROM universe_members um
        LEFT JOIN instruments i ON i.instrument_id = um.entity_id
    """

    sql_preview = """
        SELECT universe_id, as_of_date, entity_type, entity_id, tier, included, score
        FROM universe_members
        ORDER BY as_of_date DESC, universe_id, included DESC, score DESC, entity_id
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
                    distinct_universes,
                    min_as_of_date,
                    max_as_of_date,
                    empty_universe_member_id,
                    empty_universe_id,
                    empty_entity_type,
                    empty_entity_id,
                    empty_tier,
                    bad_tier,
                    tier_included_inconsistent,
                    score_nonfinite,
                    reasons_not_object,
                ) = cur.fetchone()
                orphan_instrument_id = 0
            else:
                cur.execute(sql_with_instruments_ref)
                (
                    total,
                    distinct_universes,
                    min_as_of_date,
                    max_as_of_date,
                    empty_universe_member_id,
                    empty_universe_id,
                    empty_entity_type,
                    empty_entity_id,
                    empty_tier,
                    bad_tier,
                    tier_included_inconsistent,
                    score_nonfinite,
                    reasons_not_object,
                    orphan_instrument_id,
                ) = cur.fetchone()

            cur.execute(sql_preview)
            preview_rows = cur.fetchall()
        finally:
            cur.close()

    preview = [
        {
            "universe_id": str(universe_id),
            "as_of_date": as_of_date_db.isoformat() if isinstance(as_of_date_db, date) else None,
            "entity_type": str(entity_type),
            "entity_id": str(entity_id),
            "tier": str(tier),
            "included": bool(included),
            "score": float(score) if score is not None else None,
        }
        for universe_id, as_of_date_db, entity_type, entity_id, tier, included, score in preview_rows
    ]

    checks_passed = (
        int(empty_universe_member_id or 0) == 0
        and int(empty_universe_id or 0) == 0
        and int(empty_entity_type or 0) == 0
        and int(empty_entity_id or 0) == 0
        and int(empty_tier or 0) == 0
        and int(bad_tier or 0) == 0
        and int(tier_included_inconsistent or 0) == 0
        and int(score_nonfinite or 0) == 0
        and int(reasons_not_object or 0) == 0
        and (instrument_reference_check_skipped or int(orphan_instrument_id or 0) == 0)
    )

    return {
        "total_rows": int(total or 0),
        "distinct_universe_ids": int(distinct_universes or 0),
        "min_as_of_date": min_as_of_date.isoformat() if isinstance(min_as_of_date, date) else None,
        "max_as_of_date": max_as_of_date.isoformat() if isinstance(max_as_of_date, date) else None,
        "empty_universe_member_id_rows": int(empty_universe_member_id or 0),
        "empty_universe_id_rows": int(empty_universe_id or 0),
        "empty_entity_type_rows": int(empty_entity_type or 0),
        "empty_entity_id_rows": int(empty_entity_id or 0),
        "empty_tier_rows": int(empty_tier or 0),
        "bad_tier_rows": int(bad_tier or 0),
        "tier_included_inconsistent_rows": int(tier_included_inconsistent or 0),
        "score_nonfinite_rows": int(score_nonfinite or 0),
        "reasons_not_object_rows": int(reasons_not_object or 0),
        "orphan_instrument_id_rows": int(orphan_instrument_id or 0),
        "instrument_table_total_rows": int(instruments_total),
        "instrument_reference_check_skipped": bool(instrument_reference_check_skipped),
        "recent_rows_preview": preview,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Show universe_members status and basic Layer 3 validation checks"
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
