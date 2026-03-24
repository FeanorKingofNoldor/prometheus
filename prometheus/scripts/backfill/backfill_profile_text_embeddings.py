"""Backfill text embeddings for issuer/country profiles.

This script reads profile snapshots from the runtime ``profiles`` table,
constructs textual representations from structured fields (e.g.
`business_description`, `risk_summary`, `recent_events_summary`),
embeds them using a Hugging Face encoder-only transformer model via
:class:`HuggingFaceTextEmbeddingModel`, and writes vectors into the
``text_embeddings`` table using :class:`TextEmbeddingService`.

It is intended for offline/research use and is **not** part of the core
live pipeline.

Examples
--------

    # Embed all profiles as of a given date
    python -m prometheus.scripts.backfill_profile_text_embeddings \
        --as-of 2025-01-31 \
        --model-id text-profile-v1 \
        --hf-model-name sentence-transformers/all-MiniLM-L6-v2
"""

from __future__ import annotations

import argparse
from datetime import date
from typing import Dict, List, Optional, Sequence, Tuple

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


def _profiles_table_exists(db_manager: DatabaseManager) -> bool:
    """Return True if the `profiles` table exists in the runtime DB.

    This mirrors the check used in the profiles integration tests so that
    the script fails gracefully when migrations have not been applied.
    """

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_name = 'profiles'
                )
                """
            )
            (exists,) = cursor.fetchone()
        finally:
            cursor.close()

    return bool(exists)


def _build_text_from_structured(structured: Dict[str, object]) -> str:
    """Build a profile text from structured JSON fields.

    We prioritise explicit summary fields if present, falling back to an
    empty string otherwise. This keeps the logic simple and
    schema-agnostic while aligning with the 035 Profiles spec.
    """

    parts: List[str] = []
    for key in ("business_description", "risk_summary", "recent_events_summary"):
        value = structured.get(key)
        if isinstance(value, str) and value.strip():
            parts.append(value.strip())

    return "\n\n".join(parts)


def _load_profile_texts(
    db_manager: DatabaseManager,
    *,
    as_of: Optional[date] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    limit: Optional[int] = None,
) -> List[Tuple[str, str]]:
    """Load (source_id, text) pairs from the `profiles` table.

    The current runtime schema (see ProfileStorage and integration tests)
    uses ``issuer_id`` + ``as_of_date`` as the natural key. We therefore
    construct a deterministic ``source_id`` of the form
    ``"{issuer_id}:{as_of_date}"`` so that downstream scripts can
    reconstruct the mapping when needed.
    """

    where_clauses: List[str] = []
    params: List[object] = []

    if as_of is not None:
        where_clauses.append("as_of_date = %s")
        params.append(as_of)
    else:
        if start_date is not None:
            where_clauses.append("as_of_date >= %s")
            params.append(start_date)
        if end_date is not None:
            where_clauses.append("as_of_date <= %s")
            params.append(end_date)

    where_sql = ""
    if where_clauses:
        where_sql = " WHERE " + " AND ".join(where_clauses)

    sql = (
        "SELECT issuer_id, as_of_date, structured "
        "FROM profiles" + where_sql + " ORDER BY as_of_date ASC, issuer_id ASC"
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

    results: List[Tuple[str, str]] = []
    for issuer_id, as_of_date_db, structured in rows:
        if not isinstance(structured, dict):
            continue
        text = _build_text_from_structured(structured)
        if not text.strip():
            continue
        source_id = f"{issuer_id}:{as_of_date_db.isoformat()}"
        results.append((source_id, text))

    return results


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill text embeddings for profile snapshots into text_embeddings "
            "using a Hugging Face encoder model."
        ),
    )

    date_group = parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument(
        "--as-of",
        type=_parse_date,
        help="Single as-of date (YYYY-MM-DD) for which to embed profiles",
    )
    date_group.add_argument(
        "--date-range",
        nargs=2,
        metavar=("START", "END"),
        help="Date range [START, END] (YYYY-MM-DD YYYY-MM-DD) to embed",
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=10000,
        help="Maximum number of profiles to embed (default: 10,000)",
    )
    parser.add_argument(
        "--model-id",
        type=str,
        default="text-profile-v1",
        help="Logical model_id to tag embeddings with (default: text-profile-v1)",
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

    if not _profiles_table_exists(db_manager):
        logger.warning("profiles table does not exist; run migration 0007 to enable profile text backfill")
        return

    logger.info(
        "Loading profiles for embedding: as_of=%s start=%s end=%s limit=%s",
        as_of,
        start_date,
        end_date,
        args.limit,
    )

    profiles = _load_profile_texts(
        db_manager=db_manager,
        as_of=as_of,
        start_date=start_date,
        end_date=end_date,
        limit=args.limit,
    )

    if not profiles:
        logger.warning("No profile rows with usable text found for the given filters; nothing to do")
        return

    logger.info(
        "Embedding %d profiles with model_id=%s hf_model_name=%s",
        len(profiles),
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
        TextDoc(source_type="PROFILE", source_id=source_id, text=text)
        for source_id, text in profiles
    ]

    _ = service.embed_and_store(docs)
    logger.info(
        "Text embeddings backfill complete: embedded %d PROFILE documents with model_id=%s",
        len(docs),
        args.model_id,
    )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
