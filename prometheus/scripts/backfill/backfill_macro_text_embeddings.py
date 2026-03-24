"""Backfill text embeddings for macro/policy events.

This script reads rows from the historical ``macro_events`` table,
embeds their text using a Hugging Face encoder-only transformer model via
:class:`HuggingFaceTextEmbeddingModel`, and writes vectors into the
``text_embeddings`` table using :class:`TextEmbeddingService`.

It is intended for offline/research use and is **not** part of the core
live pipeline.

Examples
--------

    # Embed macro events for a full year
    python -m prometheus.scripts.backfill_macro_text_embeddings \
        --date-range 2024-01-01 2024-12-31 \
        --model-id text-macro-v1 \
        --hf-model-name sentence-transformers/all-MiniLM-L6-v2
"""

from __future__ import annotations

import argparse
from datetime import date
from typing import List, Optional, Sequence, Tuple

from apathis.core.config import get_config
from apathis.core.database import DatabaseManager
from apathis.core.logging import get_logger
from apathis.encoders import TextDoc, TextEmbeddingService, TextEmbeddingStore
from apathis.encoders.models_text_hf import HuggingFaceTextEmbeddingModel

logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:  # pragma: no cover - CLI validation
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


def _load_macro_events(
    db_manager: DatabaseManager,
    *,
    as_of: Optional[date] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    country: Optional[str] = None,
    event_type: Optional[str] = None,
    limit: Optional[int] = None,
) -> List[Tuple[str, str]]:
    """Load (source_id, text) pairs from ``macro_events``.

    This helper assumes a historical DB table ``macro_events`` with at
    least the following columns (see 020_data_model.md):

    - event_id (PK)
    - event_type (text)
    - timestamp (timestamptz)
    - country (text, nullable)
    - description (text)
    - text_ref (text, nullable)

    The returned `source_id` is ``str(event_id)`` and `text` is currently
    just the ``description`` field. If full statement text is stored
    externally and accessible via ``text_ref``, this helper can be
    extended to fetch and concatenate it.
    """

    where_clauses: List[str] = []
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

    if country is not None:
        where_clauses.append("country = %s")
        params.append(country)

    if event_type is not None:
        where_clauses.append("event_type = %s")
        params.append(event_type)

    where_sql = ""
    if where_clauses:
        where_sql = " WHERE " + " AND ".join(where_clauses)

    sql = "SELECT event_id, description FROM macro_events" + where_sql + " ORDER BY timestamp ASC"

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
    for event_id, text in rows:
        if not text:
            continue
        results.append((str(event_id), str(text)))
    return results


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill text embeddings for macro_events into text_embeddings "
            "using a Hugging Face encoder model."
        ),
    )

    date_group = parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument(
        "--as-of",
        type=_parse_date,
        help="Single as-of date (YYYY-MM-DD) for which to embed macro events",
    )
    date_group.add_argument(
        "--date-range",
        nargs=2,
        metavar=("START", "END"),
        help="Date range [START, END] (YYYY-MM-DD YYYY-MM-DD) to embed",
    )

    parser.add_argument(
        "--country",
        type=str,
        default=None,
        help="Optional country filter for macro_events.country",
    )
    parser.add_argument(
        "--event-type",
        type=str,
        default=None,
        help="Optional filter for macro_events.event_type (e.g. FOMC)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10000,
        help="Maximum number of events to embed (default: 10,000)",
    )
    parser.add_argument(
        "--model-id",
        type=str,
        default="text-macro-v1",
        help="Logical model_id to tag embeddings with (default: text-macro-v1)",
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
        "Loading macro events for embedding: as_of=%s start=%s end=%s country=%s event_type=%s limit=%s",
        as_of,
        start_date,
        end_date,
        args.country,
        args.event_type,
        args.limit,
    )

    events = _load_macro_events(
        db_manager=db_manager,
        as_of=as_of,
        start_date=start_date,
        end_date=end_date,
        country=args.country,
        event_type=args.event_type,
        limit=args.limit,
    )

    if not events:
        logger.warning("No macro events found for the given filters; nothing to do")
        return

    logger.info(
        "Embedding %d macro events with model_id=%s hf_model_name=%s",
        len(events),
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
        TextDoc(source_type="MACRO", source_id=source_id, text=text)
        for source_id, text in events
    ]

    _ = service.embed_and_store(docs)
    logger.info(
        "Text embeddings backfill complete: embedded %d MACRO documents with model_id=%s",
        len(docs),
        args.model_id,
    )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
