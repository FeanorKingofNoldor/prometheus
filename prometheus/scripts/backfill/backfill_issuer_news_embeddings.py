"""Backfill per-issuer daily NEWS embeddings from article-level text embeddings.

This script aggregates article-level `NEWS` text embeddings from
`text_embeddings` / `news_articles` / `news_links` into one embedding per
(issuer_id, news_date) pair and stores the result back into
`text_embeddings` with a distinct `source_type`.

For a given date range and text `model_id`:

- Group all `NEWS` embeddings by `(issuer_id, DATE(timestamp))`.
- For each group:
  - Compute the mean vector over all articles linked to that issuer/date.
  - Write a row into `text_embeddings` with:
    - `source_type = source_type` (CLI arg, default: "NEWS_ISSUER_DAY"),
    - `source_id = f"{issuer_id}:{news_date.isoformat()}"`,
    - `model_id = model_id` (same as underlying text encoder),
    - `vector` = aggregated embedding bytes.

This provides a simple issuer×day news feature layer that downstream
engines can join on `(issuer_id, as_of_date)` by convention via the
`source_id` string.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
from apatheon.core.config import get_config
from apatheon.core.database import DatabaseManager
from apatheon.core.logging import get_logger
from apatheon.encoders.text import TextDoc, TextEmbeddingStore
from numpy.typing import NDArray

logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:  # pragma: no cover - CLI validation
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


@dataclass(frozen=True)
class IssuerNewsKey:
    """Logical key for an issuer×day news aggregate."""

    issuer_id: str
    news_date: date


def _load_issuer_news_vectors(
    db_manager: DatabaseManager,
    *,
    start_date: date,
    end_date: date,
    model_id: str,
    language: Optional[str] = None,
) -> Dict[IssuerNewsKey, List[NDArray[np.float_]]]:
    """Load article-level NEWS embeddings grouped by issuer and date.

    Returns a mapping from (issuer_id, news_date) to a list of
    float32 vectors (one per article) drawn from `text_embeddings`.
    """

    where_clauses = [
        "nl.issuer_id IS NOT NULL",
        "te.source_type = 'NEWS'",
        "te.model_id = %s",
        "DATE(na.timestamp) BETWEEN %s AND %s",
    ]
    params: List[object] = [model_id, start_date, end_date]

    if language is not None:
        where_clauses.append("LOWER(na.language) = LOWER(%s)")
        params.append(language)

    where_sql = " WHERE " + " AND ".join(where_clauses)

    sql = (
        "SELECT nl.issuer_id, DATE(na.timestamp) AS news_date, te.vector "
        "FROM news_links nl "
        "JOIN news_articles na ON na.article_id = nl.article_id "
        "JOIN text_embeddings te ON te.source_id = na.article_id::text "
        + where_sql +
        " ORDER BY nl.issuer_id, news_date"
    )

    groups: Dict[IssuerNewsKey, List[NDArray[np.float_]]] = {}
    first_shape: Optional[Tuple[int, ...]] = None

    with db_manager.get_historical_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, tuple(params))
            rows = cursor.fetchall()
        finally:
            cursor.close()

    if not rows:
        logger.warning(
            "_load_issuer_news_vectors: no NEWS embeddings for model_id=%s in %s→%s",
            model_id,
            start_date,
            end_date,
        )
        return groups

    for issuer_id, news_date, vec_bytes in rows:
        if issuer_id is None or vec_bytes is None:
            continue

        vec = np.frombuffer(vec_bytes, dtype=np.float32)
        if first_shape is None:
            first_shape = vec.shape
        elif vec.shape != first_shape:
            raise ValueError(
                "Inconsistent NEWS embedding shapes for issuer/day aggregate: "
                f"got {vec.shape} vs {first_shape}"
            )

        key = IssuerNewsKey(issuer_id=str(issuer_id), news_date=news_date)
        groups.setdefault(key, []).append(vec)

    logger.info(
        "Loaded NEWS embeddings for %d issuer×day groups in %s→%s (model_id=%s)",
        len(groups),
        start_date,
        end_date,
        model_id,
    )
    return groups


def _build_docs_and_vectors(
    groups: Dict[IssuerNewsKey, List[NDArray[np.float_]]],
    *,
    source_type: str,
) -> Tuple[List[TextDoc], NDArray[np.float_]]:
    """Convert grouped vectors into TextDoc + aggregated vector batches."""

    if not groups:
        return [], np.zeros((0, 0), dtype=np.float32)

    docs: List[TextDoc] = []
    agg_vectors: List[NDArray[np.float_]] = []

    for key, vec_list in groups.items():
        if not vec_list:
            continue
        stacked = np.stack(vec_list, axis=0)
        mean_vec = stacked.mean(axis=0).astype(np.float32)

        source_id = f"{key.issuer_id}:{key.news_date.isoformat()}"
        docs.append(TextDoc(source_type=source_type, source_id=source_id, text=""))
        agg_vectors.append(mean_vec)

    if not docs:
        return [], np.zeros((0, 0), dtype=np.float32)

    vectors = np.stack(agg_vectors, axis=0).astype(np.float32)
    return docs, vectors


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill per-issuer daily NEWS embeddings into text_embeddings "
            "by aggregating article-level NEWS vectors."
        ),
    )

    parser.add_argument(
        "--start",
        required=True,
        type=_parse_date,
        help="Inclusive start date (YYYY-MM-DD) for news aggregation",
    )
    parser.add_argument(
        "--end",
        required=True,
        type=_parse_date,
        help="Inclusive end date (YYYY-MM-DD) for news aggregation",
    )
    parser.add_argument(
        "--model-id",
        type=str,
        default="text-fin-general-v1",
        help="Text embedding model_id to use (default: text-fin-general-v1)",
    )
    parser.add_argument(
        "--source-type",
        type=str,
        default="NEWS_ISSUER_DAY",
        help=(
            "source_type to use for aggregated embeddings in text_embeddings "
            "(default: NEWS_ISSUER_DAY)"
        ),
    )
    parser.add_argument(
        "--language",
        type=str,
        default=None,
        help="Optional language filter for news_articles.language",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Load and aggregate issuer×day NEWS embeddings but do not "
            "write results to text_embeddings.",
        ),
    )

    args = parser.parse_args(argv)

    if args.end < args.start:
        parser.error("--end must be >= --start")

    config = get_config()
    db_manager = DatabaseManager(config)

    logger.info(
        "Loading issuer×day NEWS embeddings for %s→%s model_id=%s language=%s",
        args.start,
        args.end,
        args.model_id,
        args.language,
    )

    groups = _load_issuer_news_vectors(
        db_manager=db_manager,
        start_date=args.start,
        end_date=args.end,
        model_id=args.model_id,
        language=args.language,
    )

    if not groups:
        logger.warning("No issuer×day groups found; nothing to do")
        return

    logger.info("Building aggregated issuer×day NEWS vectors for %d groups", len(groups))

    docs, vectors = _build_docs_and_vectors(groups, source_type=args.source_type)
    if not docs:
        logger.warning("No aggregated embeddings constructed; nothing to do")
        return

    logger.info(
        "Prepared %d aggregated issuer×day NEWS embeddings with source_type=%s model_id=%s",
        len(docs),
        args.source_type,
        args.model_id,
    )

    if args.dry_run:
        logger.info("DRY RUN enabled - not writing aggregated embeddings to text_embeddings")
        return

    store = TextEmbeddingStore(db_manager=db_manager)
    store.save_embeddings(docs, args.model_id, vectors)

    logger.info(
        "Issuer×day NEWS embedding backfill complete: wrote %d rows to text_embeddings",
        len(docs),
    )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
