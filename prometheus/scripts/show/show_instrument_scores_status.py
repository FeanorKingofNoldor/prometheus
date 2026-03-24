"""Prometheus v2 – Show instrument_scores status (Layer 2 validation).

Validates basic Layer 2 contracts for ``instrument_scores``:
- score_id, strategy_id, market_id, instrument_id are non-empty
- horizon_days > 0
- expected_return, score, confidence are finite
- score in [-1, 1]; confidence in [0, 1]
- signal_label is non-empty and in a controlled set
- alpha_components is a JSON object
- metadata is either NULL or a JSON object
- instrument_id exists in instruments (checked via LEFT JOIN)

Reports results for both runtime_db and historical_db.

Note: score model calibration and lookahead safety are higher-level audits.
"""

from __future__ import annotations

import argparse
import json
from datetime import date
from typing import Any, Optional, Sequence

from apathis.core.database import get_db_manager

_ALLOWED_LABELS = ("HOLD", "BUY", "STRONG_BUY", "SELL", "STRONG_SELL")
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
            COUNT(DISTINCT iscore.instrument_id) AS distinct_instruments,
            MIN(iscore.as_of_date) AS min_as_of_date,
            MAX(iscore.as_of_date) AS max_as_of_date,
            SUM(CASE WHEN btrim(iscore.score_id) = '' THEN 1 ELSE 0 END) AS empty_score_id,
            SUM(CASE WHEN btrim(iscore.strategy_id) = '' THEN 1 ELSE 0 END) AS empty_strategy_id,
            SUM(CASE WHEN btrim(iscore.market_id) = '' THEN 1 ELSE 0 END) AS empty_market_id,
            SUM(CASE WHEN btrim(iscore.instrument_id) = '' THEN 1 ELSE 0 END) AS empty_instrument_id,
            SUM(CASE WHEN iscore.horizon_days <= 0 THEN 1 ELSE 0 END) AS bad_horizon_days,
            SUM(CASE WHEN iscore.expected_return IN {_NONFINITE} THEN 1 ELSE 0 END) AS expected_return_nonfinite,
            SUM(CASE WHEN iscore.score IN {_NONFINITE} THEN 1 ELSE 0 END) AS score_nonfinite,
            SUM(CASE WHEN iscore.score < -1.0 OR iscore.score > 1.0 THEN 1 ELSE 0 END) AS score_out_of_range,
            SUM(CASE WHEN iscore.confidence IN {_NONFINITE} THEN 1 ELSE 0 END) AS confidence_nonfinite,
            SUM(CASE WHEN iscore.confidence < 0.0 OR iscore.confidence > 1.0 THEN 1 ELSE 0 END) AS confidence_out_of_range,
            SUM(CASE WHEN btrim(iscore.signal_label) = '' THEN 1 ELSE 0 END) AS empty_signal_label,
            SUM(CASE WHEN iscore.signal_label NOT IN {tuple(_ALLOWED_LABELS)!r} THEN 1 ELSE 0 END) AS bad_signal_label,
            SUM(CASE WHEN jsonb_typeof(iscore.alpha_components) <> 'object' THEN 1 ELSE 0 END) AS alpha_components_not_object,
            SUM(CASE WHEN iscore.metadata IS NOT NULL AND jsonb_typeof(iscore.metadata) <> 'object' THEN 1 ELSE 0 END) AS metadata_not_object,
            SUM(CASE WHEN i.instrument_id IS NULL THEN 1 ELSE 0 END) AS orphan_instrument_id
        FROM instrument_scores iscore
        LEFT JOIN instruments i ON i.instrument_id = iscore.instrument_id
    """

    sql_preview = """
        SELECT instrument_id, as_of_date, horizon_days, score, confidence, signal_label
        FROM instrument_scores
        ORDER BY as_of_date DESC, instrument_id
        LIMIT 25
    """

    with conn_cm as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            (
                total,
                distinct_instruments,
                min_as_of_date,
                max_as_of_date,
                empty_score_id,
                empty_strategy_id,
                empty_market_id,
                empty_instrument_id,
                bad_horizon_days,
                expected_return_nonfinite,
                score_nonfinite,
                score_out_of_range,
                confidence_nonfinite,
                confidence_out_of_range,
                empty_signal_label,
                bad_signal_label,
                alpha_components_not_object,
                metadata_not_object,
                orphan_instrument_id,
            ) = cur.fetchone()

            cur.execute(sql_preview)
            preview_rows = cur.fetchall()
        finally:
            cur.close()

    preview = [
        {
            "instrument_id": str(inst_id),
            "as_of_date": as_of.isoformat() if isinstance(as_of, date) else None,
            "horizon_days": int(h) if h is not None else None,
            "score": float(score) if score is not None else None,
            "confidence": float(conf) if conf is not None else None,
            "signal_label": str(label),
        }
        for inst_id, as_of, h, score, conf, label in preview_rows
    ]

    checks_passed = (
        int(empty_score_id or 0) == 0
        and int(empty_strategy_id or 0) == 0
        and int(empty_market_id or 0) == 0
        and int(empty_instrument_id or 0) == 0
        and int(bad_horizon_days or 0) == 0
        and int(expected_return_nonfinite or 0) == 0
        and int(score_nonfinite or 0) == 0
        and int(score_out_of_range or 0) == 0
        and int(confidence_nonfinite or 0) == 0
        and int(confidence_out_of_range or 0) == 0
        and int(empty_signal_label or 0) == 0
        and int(bad_signal_label or 0) == 0
        and int(alpha_components_not_object or 0) == 0
        and int(metadata_not_object or 0) == 0
        and int(orphan_instrument_id or 0) == 0
    )

    return {
        "total_rows": int(total or 0),
        "distinct_instruments": int(distinct_instruments or 0),
        "min_as_of_date": min_as_of_date.isoformat() if isinstance(min_as_of_date, date) else None,
        "max_as_of_date": max_as_of_date.isoformat() if isinstance(max_as_of_date, date) else None,
        "empty_score_id_rows": int(empty_score_id or 0),
        "empty_strategy_id_rows": int(empty_strategy_id or 0),
        "empty_market_id_rows": int(empty_market_id or 0),
        "empty_instrument_id_rows": int(empty_instrument_id or 0),
        "bad_horizon_days_rows": int(bad_horizon_days or 0),
        "expected_return_nonfinite_rows": int(expected_return_nonfinite or 0),
        "score_nonfinite_rows": int(score_nonfinite or 0),
        "score_out_of_range_rows": int(score_out_of_range or 0),
        "confidence_nonfinite_rows": int(confidence_nonfinite or 0),
        "confidence_out_of_range_rows": int(confidence_out_of_range or 0),
        "empty_signal_label_rows": int(empty_signal_label or 0),
        "bad_signal_label_rows": int(bad_signal_label or 0),
        "alpha_components_not_object_rows": int(alpha_components_not_object or 0),
        "metadata_not_object_rows": int(metadata_not_object or 0),
        "orphan_instrument_id_rows": int(orphan_instrument_id or 0),
        "recent_rows_preview": preview,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Show instrument_scores status and basic Layer 2 validation checks"
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
