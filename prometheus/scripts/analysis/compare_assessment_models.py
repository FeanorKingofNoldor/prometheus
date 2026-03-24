"""Prometheus v2 – Assessment model comparison CLI.

This script compares two Assessment Engine backends/models using rows in
the ``instrument_scores`` table. It is designed to answer questions such
as:

- How similar are the scores from the basic price/STAB backend and the
  context-based backend over the same universe and period?
- For a given strategy/market/horizon, what is the empirical
  correlation between ``score`` and ``expected_return`` for two
  Assessment model_ids?

The script expects that ``InstrumentScoreStorage`` has tagged
``metadata["model_id"]`` with the Assessment ``model_id`` used when
the scores were written.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date
from typing import Iterable, List, Optional, Sequence

import numpy as np
from apathis.core.config import get_config
from apathis.core.database import DatabaseManager
from apathis.core.logging import get_logger

logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:  # pragma: no cover - CLI validation
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


@dataclass
class ScorePair:
    as_of_date: date
    instrument_id: str
    expected_return_a: float
    score_a: float
    expected_return_b: float
    score_b: float


def _load_score_pairs(
    db_manager: DatabaseManager,
    *,
    strategy_id: str,
    market_id: str,
    model_id_a: str,
    model_id_b: str,
    horizon_days: int,
    start: Optional[date],
    end: Optional[date],
    min_confidence: float,
) -> List[ScorePair]:
    """Load joined instrument_scores rows for two model_ids.

    Rows are joined on (strategy_id, market_id, instrument_id, as_of_date,
    horizon_days). Only rows where both models have scores and both
    confidences >= ``min_confidence`` are returned.
    """

    where_clauses = [
        "a.strategy_id = %s",
        "a.market_id = %s",
        "a.horizon_days = %s",
        "a.strategy_id = b.strategy_id",
        "a.market_id = b.market_id",
        "a.instrument_id = b.instrument_id",
        "a.as_of_date = b.as_of_date",
        "a.horizon_days = b.horizon_days",
        "(a.metadata->>'model_id') = %s",
        "(b.metadata->>'model_id') = %s",
    ]

    params: List[object] = [
        strategy_id,
        market_id,
        horizon_days,
        model_id_a,
        model_id_b,
    ]

    if start is not None:
        where_clauses.append("a.as_of_date >= %s")
        params.append(start)
    if end is not None:
        where_clauses.append("a.as_of_date <= %s")
        params.append(end)

    if min_confidence > 0.0:
        where_clauses.append("a.confidence >= %s")
        where_clauses.append("b.confidence >= %s")
        params.extend([min_confidence, min_confidence])

    where_sql = " AND ".join(where_clauses)

    sql = f"""
        SELECT
            a.as_of_date,
            a.instrument_id,
            a.expected_return AS expected_return_a,
            a.score AS score_a,
            b.expected_return AS expected_return_b,
            b.score AS score_b
        FROM instrument_scores AS a
        JOIN instrument_scores AS b
          ON a.strategy_id = b.strategy_id
         AND a.market_id = b.market_id
         AND a.instrument_id = b.instrument_id
         AND a.as_of_date = b.as_of_date
         AND a.horizon_days = b.horizon_days
        WHERE {where_sql}
        ORDER BY a.as_of_date, a.instrument_id
    """

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, tuple(params))
            rows = cursor.fetchall()
        finally:
            cursor.close()

    pairs: List[ScorePair] = []
    for as_of_date_db, inst_id, er_a, s_a, er_b, s_b in rows:
        pairs.append(
            ScorePair(
                as_of_date=as_of_date_db,
                instrument_id=str(inst_id),
                expected_return_a=float(er_a),
                score_a=float(s_a),
                expected_return_b=float(er_b),
                score_b=float(s_b),
            )
        )

    return pairs


def _pearson_safe(x: np.ndarray, y: np.ndarray) -> float | None:
    if x.size < 2 or y.size < 2:
        return None
    if np.allclose(x, x[0]) or np.allclose(y, y[0]):
        return None
    return float(np.corrcoef(x, y)[0, 1])


def _summarise_pairs(pairs: Sequence[ScorePair]) -> None:
    n = len(pairs)
    if n == 0:
        print("No overlapping scores found for the given filters.")
        return

    er_a = np.array([p.expected_return_a for p in pairs], dtype=float)
    er_b = np.array([p.expected_return_b for p in pairs], dtype=float)
    s_a = np.array([p.score_a for p in pairs], dtype=float)
    s_b = np.array([p.score_b for p in pairs], dtype=float)

    er_corr = _pearson_safe(er_a, er_b)
    s_corr = _pearson_safe(s_a, s_b)

    print(f"Number of overlapping score pairs: {n}")
    print()
    print("Score statistics (model A vs model B):")
    print(f"  score_a mean={s_a.mean():.4f} std={s_a.std(ddof=1):.4f}")
    print(f"  score_b mean={s_b.mean():.4f} std={s_b.std(ddof=1):.4f}")
    if s_corr is not None:
        print(f"  corr(score_a, score_b)={s_corr:.4f}")
    else:
        print("  corr(score_a, score_b)=N/A (insufficient variance or pairs)")

    print()
    print("Expected return statistics (model A vs model B):")
    print(f"  er_a mean={er_a.mean():.4f} std={er_a.std(ddof=1):.4f}")
    print(f"  er_b mean={er_b.mean():.4f} std={er_b.std(ddof=1):.4f}")
    if er_corr is not None:
        print(f"  corr(er_a, er_b)={er_corr:.4f}")
    else:
        print("  corr(er_a, er_b)=N/A (insufficient variance or pairs)")


def _dump_pairs_csv(pairs: Iterable[ScorePair]) -> None:
    print(
        "as_of_date,instrument_id,expected_return_a,score_a,"
        "expected_return_b,score_b"
    )
    for p in pairs:
        print(
            f"{p.as_of_date:%Y-%m-%d},{p.instrument_id},"
            f"{p.expected_return_a:.6f},{p.score_a:.6f},"
            f"{p.expected_return_b:.6f},{p.score_b:.6f}"
        )


def _parse_args(argv: Optional[Sequence[str]]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compare two Assessment models/backends using instrument_scores "
            "for a given strategy/market/horizon."
        ),
    )

    parser.add_argument("--strategy-id", type=str, required=True, help="Strategy identifier")
    parser.add_argument("--market-id", type=str, required=True, help="Market identifier (e.g. US_EQ)")
    parser.add_argument(
        "--model-a-id",
        type=str,
        required=True,
        help="Assessment model_id for backend A (metadata['model_id'])",
    )
    parser.add_argument(
        "--model-b-id",
        type=str,
        required=True,
        help="Assessment model_id for backend B (metadata['model_id'])",
    )
    parser.add_argument(
        "--horizon-days",
        type=int,
        required=True,
        help="Prediction horizon in trading days to filter on",
    )
    parser.add_argument(
        "--start",
        type=_parse_date,
        default=None,
        help="Optional start date (YYYY-MM-DD) for as_of_date filter",
    )
    parser.add_argument(
        "--end",
        type=_parse_date,
        default=None,
        help="Optional end date (YYYY-MM-DD) for as_of_date filter",
    )
    parser.add_argument(
        "--min-confidence",
        type=float,
        default=0.0,
        help="Minimum confidence required for both models (default: 0.0)",
    )
    parser.add_argument(
        "--dump-pairs",
        action="store_true",
        help="If set, also print a CSV of joined score pairs to stdout",
    )

    args = parser.parse_args(argv)

    if args.end is not None and args.start is not None and args.end < args.start:
        parser.error("--end must be >= --start")
    if args.horizon_days <= 0:
        parser.error("--horizon-days must be positive")
    if args.min_confidence < 0.0:
        parser.error("--min-confidence must be >= 0")

    return args


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = _parse_args(argv)

    config = get_config()
    db_manager = DatabaseManager(config)

    pairs = _load_score_pairs(
        db_manager=db_manager,
        strategy_id=args.strategy_id,
        market_id=args.market_id,
        model_id_a=args.model_a_id,
        model_id_b=args.model_b_id,
        horizon_days=args.horizon_days,
        start=args.start,
        end=args.end,
        min_confidence=args.min_confidence,
    )

    _summarise_pairs(pairs)

    if args.dump_pairs and pairs:
        print()
        _dump_pairs_csv(pairs)


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
