"""Backfill text embeddings for news articles (and similar text sources).

This script reads raw text rows from the historical ``news_articles``
(table name and columns can be adjusted as needed), embeds them using a
Hugging Face encoder-only transformer model via
:class:`HuggingFaceTextEmbeddingModel`, and writes vectors into the
``text_embeddings`` table using :class:`TextEmbeddingService`.

It is intended for offline/research use and is **not** part of the core
live pipeline.

Examples
--------

    # Embed up to 10,000 English news articles for a single date
    python -m prometheus.scripts.backfill_text_embeddings \
        --as-of 2024-01-15 \
        --limit 10000 \
        --model-id text-fin-general-v1 \
        --hf-model-name sentence-transformers/all-MiniLM-L6-v2
"""

from __future__ import annotations

import argparse
from datetime import date
from typing import List, Optional, Sequence, Tuple

from apatheon.core.config import get_config
from apatheon.core.database import DatabaseManager
from apatheon.core.logging import get_logger
from apatheon.encoders import TextDoc, TextEmbeddingService, TextEmbeddingStore
from apatheon.encoders.models_text_hf import HuggingFaceTextEmbeddingModel

logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:  # pragma: no cover - CLI validation
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


def _load_news_articles(
    db_manager: DatabaseManager,
    *,
    as_of: Optional[date] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    language: Optional[str] = None,
    limit: Optional[int] = None,
) -> List[Tuple[str, str]]:
    """Load (source_id, text) pairs from ``news_articles``.

    This helper assumes a historical DB table ``news_articles`` with at
    least the following columns (adjust as needed for the actual schema):

    - article_id (PK)
    - timestamp (publication timestamp)
    - language (optional)
    - headline (text)
    - body (text)

    The returned `source_id` is ``str(article_id)`` and `text` is a
    simple concatenation of headline and body.
    """

    # Build simple WHERE conditions based on the date arguments.
    where_clauses = []
    params: List[object] = []

    if as_of is not None:
        where_clauses.append("DATE(timestamp) = %s")
        params.append(as_of)
    else:
        if start_date is not None:
            where_clauses.append("DATE(timestamp) >= %s")
            params.append(start_date)
        if end_date is not None:
            where_clauses.append("DATE(timestamp) <= %s")
            params.append(end_date)

    if language is not None:
        # Case-insensitive language match (e.g. "en" vs "EN").
        where_clauses.append("LOWER(language) = LOWER(%s)")
        params.append(language)

    where_sql = ""
    if where_clauses:
        where_sql = " WHERE " + " AND ".join(where_clauses)

    sql = (
        "SELECT article_id, COALESCE(headline || ' ' || body, body, headline) AS text "
        "FROM news_articles" + where_sql + " ORDER BY timestamp ASC"
    )

    if limit is not None and limit > 0:
        sql += " LIMIT %s"
        params.append(limit)

    with db_manager.get_historical_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, tuple(params))
            rows = cursor.fetchall()
        finally:
            cursor.close()

    results: List[Tuple[str, str]] = []
    for article_id, text in rows:
        if not text:
            continue
        results.append((str(article_id), str(text)))
    return results


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill text embeddings for news articles into text_embeddings "
            "using a Hugging Face encoder model."
        ),
    )

    date_group = parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument(
        "--as-of",
        type=_parse_date,
        help="Single as-of date (YYYY-MM-DD) for which to embed articles",
    )
    date_group.add_argument(
        "--date-range",
        nargs=2,
        metavar=("START", "END"),
        help="Date range [START, END] (YYYY-MM-DD YYYY-MM-DD) to embed",
    )

    parser.add_argument(
        "--language",
        type=str,
        default=None,
        help=(
            "Optional language filter for news_articles.language. "
            "If omitted, embeds all languages (language IS NOT filtered)."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10000,
        help="Maximum number of articles to embed (default: 10,000)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=64,
        help="Number of documents to embed per batch (default: 64)",
    )
    parser.add_argument(
        "--model-id",
        type=str,
        default="text-fin-general-v1",
        help="Logical model_id to tag embeddings with (default: text-fin-general-v1)",
    )
    parser.add_argument(
        "--hf-model-name",
        type=str,
        default="sentence-transformers/all-MiniLM-L6-v2",
        help="Hugging Face model name/path for the encoder (default: MiniLM 384-dim)",
    )
    parser.add_argument(
        "--device",
        type=str,
        default=None,
        help="Torch device for the model (e.g. cpu, cuda). Default: auto-detect",
    )

    args = parser.parse_args(argv)

    if args.limit is not None and args.limit <= 0:
        parser.error("--limit must be positive")
    if args.batch_size is not None and args.batch_size <= 0:
        parser.error("--batch-size must be positive")

    if args.date_range is not None:
        start = _parse_date(args.date_range[0])
        end = _parse_date(args.date_range[1])
        if end < start:
            parser.error("date-range END must be >= START")
        as_of: Optional[date] = None
        start_date: Optional[date] = start
        end_date: Optional[date] = end
    else:
        as_of = args.as_of
        start_date = None
        end_date = None

    config = get_config()
    db_manager = DatabaseManager(config)

    logger.info(
        "Loading news articles for embedding: as_of=%s start=%s end=%s lang=%s limit=%s",
        as_of,
        start_date,
        end_date,
        args.language,
        args.limit,
    )

    articles = _load_news_articles(
        db_manager=db_manager,
        as_of=as_of,
        start_date=start_date,
        end_date=end_date,
        language=args.language,
        limit=args.limit,
    )

    if not articles:
        logger.warning("No articles found for the given filters; nothing to do")
        return

    logger.info(
        "Embedding %d articles with model_id=%s hf_model_name=%s",
        len(articles),
        args.model_id,
        args.hf_model_name,
    )

    try:
        model = HuggingFaceTextEmbeddingModel(
            model_name=args.hf_model_name,
            device=args.device,
            pooling="mean",
        )
    except Exception as exc:  # pragma: no cover - defensive path
        logger.exception("Failed to initialise HuggingFaceTextEmbeddingModel: %s", exc)
        print(
            "Error: could not initialise Hugging Face model. Ensure that "
            "'transformers' and 'torch' are installed and the model name is valid.",
        )
        return

    store = TextEmbeddingStore(db_manager=db_manager)
    service = TextEmbeddingService(model=model, store=store, model_id=args.model_id)

    docs: List[TextDoc] = [
        TextDoc(source_type="NEWS", source_id=source_id, text=text)
        for source_id, text in articles
    ]

    batch_size = args.batch_size
    total = len(docs)
    logger.info("Embedding in batches of size %d (total docs=%d)", batch_size, total)

    for start_idx in range(0, total, batch_size):
        end_idx = min(start_idx + batch_size, total)
        batch = docs[start_idx:end_idx]
        _ = service.embed_and_store(batch)
        logger.info(
            "Embedded batch %d-%d/%d for model_id=%s",
            start_idx,
            end_idx,
            total,
            args.model_id,
        )

    logger.info(
        "Text embeddings backfill complete: embedded %d NEWS documents with model_id=%s",
        total,
        args.model_id,
    )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
