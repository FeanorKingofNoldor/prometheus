"""Backfill numeric portfolio embeddings (num-portfolio-core-v1).

This script reads portfolio risk reports from the runtime
`portfolio_risk_reports` table, constructs numeric feature vectors per
(portfolio_id, as_of_date), and stores 384-dim embeddings into
`numeric_window_embeddings` using the `num-portfolio-core-v1` encoder
interface.

For v0, the encoder is implemented as a simple flatten + pad/truncate
projection of a feature vector into `R^384` using
`PadToDimNumericEmbeddingModel`.

Embeddings are written into `historical_db.numeric_window_embeddings`
with:

- `entity_type = 'PORTFOLIO'`.
- `entity_id = portfolio_id`.
- `window_spec` describing the feature source.
- `model_id = 'num-portfolio-core-v1'` (by default).

Examples
--------

    # Backfill portfolio embeddings for all portfolios with risk reports
    # on a given date
    python -m prometheus.scripts.backfill_numeric_portfolio_embeddings \
        --as-of 2025-01-31 \
        --model-id num-portfolio-core-v1 \
        --limit 100
"""

from __future__ import annotations

import argparse
from datetime import date
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np

from apathis.core.config import get_config
from apathis.core.database import DatabaseManager
from apathis.core.logging import get_logger
from apathis.encoders import NumericEmbeddingStore, NumericWindowSpec
from apathis.encoders.models_simple_numeric import PadToDimNumericEmbeddingModel


logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:  # pragma: no cover - CLI validation
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


def _load_portfolio_reports(
    db_manager: DatabaseManager,
    *,
    as_of: date,
    portfolio_ids: Optional[List[str]] = None,
    limit: Optional[int] = None,
) -> List[Tuple[str, date, Dict[str, float], Dict[str, float], Dict[str, float]]]:
    """Load portfolio risk report rows and extract JSON fields.

    Returns a list of tuples:

    - portfolio_id
    - as_of_date
    - risk_metrics (dict)
    - exposures_by_sector (dict)
    - exposures_by_factor (dict)
    """

    where_clauses = ["as_of_date = %s"]
    params: List[object] = [as_of]

    if portfolio_ids:
        where_clauses.append("portfolio_id = ANY(%s)")
        params.append(portfolio_ids)

    where_sql = " WHERE " + " AND ".join(where_clauses)

    sql = (
        "SELECT portfolio_id, as_of_date, risk_metrics, "
        "exposures_by_sector, exposures_by_factor "
        "FROM portfolio_risk_reports" + where_sql + " ORDER BY portfolio_id ASC"
    )

    if limit is not None and limit > 0:
        sql += " LIMIT %s"
        params.append(limit)

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, tuple(params))
            rows = cursor.fetchall()
        finally:
            cursor.close()

    results: List[Tuple[str, date, Dict[str, float], Dict[str, float], Dict[str, float]]] = []
    for portfolio_id, as_of_date_db, risk_metrics, exp_sector, exp_factor in rows:
        rm = risk_metrics or {}
        es = exp_sector or {}
        ef = exp_factor or {}
        results.append((str(portfolio_id), as_of_date_db, rm, es, ef))

    return results


def _build_feature_vector(
    risk_metrics: Dict[str, float],
    exposures_by_sector: Dict[str, float],
    exposures_by_factor: Dict[str, float],
) -> np.ndarray:
    """Construct a 1D feature vector from risk and exposure dictionaries.

    The exact ordering is deterministic but simple:

    - Risk metrics sorted by key.
    - Sector exposures sorted by sector name.
    - Factor exposures sorted by factor id.
    """

    values: List[float] = []

    for key in sorted(risk_metrics.keys()):
        v = risk_metrics.get(key)
        if isinstance(v, (int, float)):
            values.append(float(v))

    for key in sorted(exposures_by_sector.keys()):
        v = exposures_by_sector.get(key)
        if isinstance(v, (int, float)):
            values.append(float(v))

    for key in sorted(exposures_by_factor.keys()):
        v = exposures_by_factor.get(key)
        if isinstance(v, (int, float)):
            values.append(float(v))

    return np.asarray(values, dtype=np.float32)


def _parse_args(argv: Optional[Sequence[str]]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill numeric portfolio embeddings (num-portfolio-core-v1) "
            "into numeric_window_embeddings from portfolio_risk_reports."
        ),
    )

    parser.add_argument(
        "--as-of",
        type=_parse_date,
        required=True,
        help="As-of date (YYYY-MM-DD) for portfolio risk reports",
    )
    parser.add_argument(
        "--portfolio-id",
        dest="portfolio_ids",
        action="append",
        help="Optional portfolio_id to restrict to (can be repeated)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of portfolios to process (default: 100)",
    )
    parser.add_argument(
        "--model-id",
        type=str,
        default="num-portfolio-core-v1",
        help="Model identifier to tag embeddings with (default: num-portfolio-core-v1)",
    )

    args = parser.parse_args(argv)

    if args.limit is not None and args.limit <= 0:
        parser.error("--limit must be positive")

    return args


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = _parse_args(argv)

    config = get_config()
    db_manager = DatabaseManager(config)

    portfolio_ids: Optional[List[str]] = args.portfolio_ids if args.portfolio_ids else None

    reports = _load_portfolio_reports(
        db_manager=db_manager,
        as_of=args.as_of,
        portfolio_ids=portfolio_ids,
        limit=args.limit,
    )

    if not reports:
        logger.warning("No portfolio_risk_reports rows found for as_of=%s; nothing to do", args.as_of)
        return

    logger.info(
        "Backfilling numeric portfolio embeddings: as_of=%s portfolios=%d model_id=%s",
        args.as_of,
        len(reports),
        args.model_id,
    )

    store = NumericEmbeddingStore(db_manager=db_manager)
    model = PadToDimNumericEmbeddingModel(target_dim=384)

    success = 0
    failures = 0

    for portfolio_id, as_of_date_db, risk_metrics, exp_sector, exp_factor in reports:
        features = _build_feature_vector(risk_metrics, exp_sector, exp_factor)
        if features.size == 0:
            logger.debug(
                "Portfolio %s as_of=%s has no numeric features; skipping",
                portfolio_id,
                as_of_date_db,
            )
            continue

        # Treat the feature vector as a single-row window for encoding.
        window = features.reshape(1, -1).astype(np.float32)

        try:
            embedding = model.encode(window)
        except Exception as exc:  # pragma: no cover - defensive
            failures += 1
            logger.exception(
                "Failed to encode portfolio %s as_of=%s: %s",
                portfolio_id,
                as_of_date_db,
                exc,
            )
            continue

        spec = NumericWindowSpec(
            entity_type="PORTFOLIO",
            entity_id=portfolio_id,
            window_days=1,
        )

        try:
            store.save_embedding(
                spec=spec,
                as_of_date=as_of_date_db,
                model_id=args.model_id,
                vector=embedding,
            )
            success += 1
        except Exception as exc:  # pragma: no cover - defensive
            failures += 1
            logger.exception(
                "Failed to save embedding for portfolio %s as_of=%s: %s",
                portfolio_id,
                as_of_date_db,
                exc,
            )

    logger.info(
        "Numeric portfolio embeddings backfill complete: success=%d failures=%d",
        success,
        failures,
    )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
