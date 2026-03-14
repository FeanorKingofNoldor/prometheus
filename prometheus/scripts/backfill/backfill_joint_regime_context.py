"""Backfill joint regime context embeddings (numeric + text).

This script constructs a first v0 joint space for regime context by
combining:

- Numeric regime embeddings from the ``regimes`` table, and
- Aggregated text embeddings from ``text_embeddings`` / ``news_articles``.

For each (region, as_of_date) where both a regime embedding and at least
one text embedding are available, it builds a JointExample and embeds it
using SimpleAverageJointModel, storing the result in ``joint_embeddings``
with ``model_id = 'joint-regime-core-v1'``.

This is an offline/research workflow and is not part of the daily live
pipeline.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date
from typing import Dict, Iterable, List, Mapping, Optional, Sequence

import numpy as np

from apathis.core.config import get_config
from apathis.core.database import DatabaseManager
from apathis.core.logging import get_logger
from apathis.encoders import (
    JointEmbeddingService,
    JointEmbeddingStore,
    JointExample,
)
from apathis.encoders.models_joint_simple import SimpleAverageJointModel


logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:  # pragma: no cover - CLI validation
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


@dataclass(frozen=True)
class RegimePoint:
    as_of_date: date
    region: str
    embedding: np.ndarray


def _load_regime_embeddings(
    db_manager: DatabaseManager,
    regions: Iterable[str],
    *,
    start_date: date,
    end_date: date,
) -> List[RegimePoint]:
    """Load regime embeddings for given regions and date range.

    Embeddings are read from ``runtime_db.regimes.regime_embedding`` and
    decoded as float32 vectors.
    """

    results: List[RegimePoint] = []

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            for region in regions:
                cursor.execute(
                    """
                    SELECT as_of_date, regime_embedding
                    FROM regimes
                    WHERE region = %s
                      AND as_of_date BETWEEN %s AND %s
                    ORDER BY as_of_date ASC
                    """,
                    (region, start_date, end_date),
                )
                rows = cursor.fetchall()
                for as_of_date, embedding_bytes in rows:
                    if embedding_bytes is None:
                        continue
                    vec = np.frombuffer(embedding_bytes, dtype=np.float32)
                    results.append(RegimePoint(as_of_date=as_of_date, region=region, embedding=vec))
        finally:
            cursor.close()

    return results


def _load_text_embedding_for_date(
    db_manager: DatabaseManager,
    *,
    as_of_date: date,
    model_id: str,
    language: Optional[str] = None,
) -> Optional[np.ndarray]:
    """Load an aggregated text embedding for a single as_of_date.

    This aggregates all ``text_embeddings`` rows of type ``NEWS`` with the
    given ``model_id`` whose corresponding ``news_articles.published_at``
    falls on ``as_of_date`` (and optionally matches ``language``), and
    returns their mean vector. If no such rows exist, returns None.
    """

    where_clauses = ["DATE(na.published_at) = %s", "te.source_type = 'NEWS'", "te.model_id = %s"]
    params: List[object] = [as_of_date, model_id]

    if language is not None:
        where_clauses.append("na.language = %s")
        params.append(language)

    where_sql = " WHERE " + " AND ".join(where_clauses)

    sql = (
        "SELECT te.vector "
        "FROM text_embeddings te "
        "JOIN news_articles na "
        "  ON te.source_id = na.article_id::text "
        + where_sql
    )

    with db_manager.get_historical_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, tuple(params))
            rows = cursor.fetchall()
        finally:
            cursor.close()

    if not rows:
        return None

    vectors = [np.frombuffer(row[0], dtype=np.float32) for row in rows]
    # Ensure all vectors have the same shape.
    first_shape = vectors[0].shape
    for v in vectors[1:]:
        if v.shape != first_shape:
            raise ValueError(
                "Inconsistent text embedding shapes for date "
                f"{as_of_date}: {v.shape} vs {first_shape}"
            )

    stacked = np.stack(vectors, axis=0)
    return stacked.mean(axis=0).astype(np.float32)


def _build_joint_examples(
    regime_points: List[RegimePoint],
    text_by_date: Mapping[date, np.ndarray],
) -> List[JointExample]:
    """Build JointExample objects for dates where both branches exist."""

    examples: List[JointExample] = []
    for rp in regime_points:
        text_vec = text_by_date.get(rp.as_of_date)
        if text_vec is None:
            continue

        if rp.embedding.shape != text_vec.shape:
            raise ValueError(
                "Numeric regime embedding and text embedding shapes must match; "
                f"got {rp.embedding.shape} and {text_vec.shape} for date {rp.as_of_date}"
            )

        examples.append(
            JointExample(
                joint_type="REGIME_CONTEXT_V0",
                as_of_date=rp.as_of_date,
                entity_scope={
                    "region": rp.region,
                    "source": "regime+news",
                },
                numeric_embedding=rp.embedding,
                text_embedding=text_vec,
            )
        )

    return examples


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill joint regime context embeddings into joint_embeddings "
            "by combining regime embeddings with aggregated NEWS text embeddings."
        ),
    )

    date_group = parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument(
        "--as-of",
        type=_parse_date,
        help="Single as-of date (YYYY-MM-DD) to backfill joint embeddings for",
    )
    date_group.add_argument(
        "--date-range",
        nargs=2,
        metavar=("START", "END"),
        help="Date range [START, END] (YYYY-MM-DD YYYY-MM-DD) to backfill",
    )

    parser.add_argument(
        "--region",
        dest="regions",
        action="append",
        required=True,
        help=(
            "Region to include (can be specified multiple times). "
            "Must match regions used in the regimes table."
        ),
    )
    parser.add_argument(
        "--text-model-id",
        type=str,
        default="text-fin-general-v1",
        help="Text embedding model_id to use (default: text-fin-general-v1)",
    )
    parser.add_argument(
        "--joint-model-id",
        type=str,
        default="joint-regime-core-v1",
        help="Joint embedding model_id to tag outputs with (default: joint-regime-core-v1)",
    )
    parser.add_argument(
        "--language",
        type=str,
        default=None,
        help="Optional language filter for news_articles.language",
    )

    args = parser.parse_args(argv)

    if args.date_range is not None:
        start = _parse_date(args.date_range[0])
        end = _parse_date(args.date_range[1])
        if end < start:
            parser.error("date-range END must be >= START")
        start_date: date = start
        end_date: date = end
    else:
        start_date = end_date = args.as_of

    config = get_config()
    db_manager = DatabaseManager(config)

    regions: List[str] = args.regions

    logger.info(
        "Loading regime embeddings for regions=%s start=%s end=%s",
        regions,
        start_date,
        end_date,
    )

    regime_points = _load_regime_embeddings(
        db_manager=db_manager,
        regions=regions,
        start_date=start_date,
        end_date=end_date,
    )

    if not regime_points:
        logger.warning("No regime embeddings found for the given regions/date range; nothing to do")
        return

    # Collect unique dates from regime points and load aggregated text
    # embeddings for each.
    unique_dates = sorted({rp.as_of_date for rp in regime_points})

    text_by_date: Dict[date, np.ndarray] = {}
    for d in unique_dates:
        text_vec = _load_text_embedding_for_date(
            db_manager=db_manager,
            as_of_date=d,
            model_id=args.text_model_id,
            language=args.language,
        )
        if text_vec is None:
            logger.warning("No text embeddings found for date=%s; skipping", d)
            continue
        text_by_date[d] = text_vec

    if not text_by_date:
        logger.warning(
            "No text embeddings available for any dates in range; nothing to do"
        )
        return

    examples = _build_joint_examples(regime_points, text_by_date)
    if not examples:
        logger.warning(
            "No joint examples constructed (likely due to missing text for all dates); nothing to do"
        )
        return

    logger.info(
        "Embedding %d joint regime context examples with joint_model_id=%s", len(examples), args.joint_model_id
    )

    store = JointEmbeddingStore(db_manager=db_manager)
    model = SimpleAverageJointModel()
    service = JointEmbeddingService(model=model, store=store, model_id=args.joint_model_id)

    _ = service.embed_and_store(examples)

    logger.info(
        "Joint regime context backfill complete: wrote %d embeddings to joint_embeddings",
        len(examples),
    )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
