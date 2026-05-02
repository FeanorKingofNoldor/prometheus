"""Backfill joint episode embeddings (numeric + text).

This script constructs a v0 joint space for **episodes** (crisis/event
windows) by combining:

- Numeric regime embeddings from the ``regimes`` table using
  ``num-regime-core-v1``, and
- Aggregated NEWS text embeddings from ``text_embeddings`` /
  ``news_articles`` using ``text-fin-general-v1``.

For each episode defined in a JSON file, it builds a single joint
embedding in ``R^384`` using ``SimpleAverageJointModel`` and persists it
into ``joint_embeddings`` with ``joint_type = 'EPISODE_V0'`` and
``model_id = 'joint-episode-core-v1'``.

Episodes are provided via a JSON file containing a list of objects with
at least:

- ``episode_id`` (string)
- ``label`` (string)
- ``region`` (string)
- ``start_date`` (YYYY-MM-DD)
- ``end_date`` (YYYY-MM-DD)

This is an offline/research workflow and is not part of the daily live
pipeline.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import date
from typing import Any, List, Optional, Sequence

import numpy as np
from apatheon.core.config import get_config
from apatheon.core.database import DatabaseManager
from apatheon.core.logging import get_logger
from apatheon.encoders import (
    JointEmbeddingService,
    JointEmbeddingStore,
    JointExample,
)
from apatheon.encoders.models_joint_simple import SimpleAverageJointModel

logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:  # pragma: no cover - CLI validation
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


@dataclass(frozen=True)
class EpisodeSpec:
    episode_id: str
    label: str
    region: str
    start_date: date
    end_date: date


def _load_episodes_from_file(path: str) -> List[EpisodeSpec]:
    """Load episode definitions from a JSON file.

    The expected format is a list of objects with keys:
    ``episode_id``, ``label``, ``region``, ``start_date``, ``end_date``.
    """

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    specs: List[EpisodeSpec] = []
    for obj in data:
        try:
            episode_id = str(obj["episode_id"])
            label = str(obj.get("label", episode_id))
            region = str(obj["region"])
            start = _parse_date(str(obj["start_date"]))
            end = _parse_date(str(obj["end_date"]))
        except KeyError as exc:
            raise ValueError(f"Missing required episode field: {exc!r} in {obj!r}") from exc

        if end < start:
            raise ValueError(
                f"Episode {episode_id!r} has end_date < start_date: {start} > {end}"
            )

        specs.append(
            EpisodeSpec(
                episode_id=episode_id,
                label=label,
                region=region,
                start_date=start,
                end_date=end,
            )
        )

    return specs


def _compute_numeric_embedding_for_episode(
    db_manager: DatabaseManager,
    episode: EpisodeSpec,
) -> Optional[np.ndarray]:
    """Compute episode-level numeric embedding by averaging regime embeddings.

    Returns None if no regime embeddings are found for the episode
    window.
    """

    sql = """
        SELECT regime_embedding
        FROM regimes
        WHERE region = %s
          AND as_of_date BETWEEN %s AND %s
        ORDER BY as_of_date ASC
    """

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (episode.region, episode.start_date, episode.end_date))
            rows = cursor.fetchall()
        finally:
            cursor.close()

    if not rows:
        logger.warning(
            "No regime embeddings found for episode_id=%s region=%s window=[%s,%s]",
            episode.episode_id,
            episode.region,
            episode.start_date,
            episode.end_date,
        )
        return None

    vectors = [np.frombuffer(row[0], dtype=np.float32) for row in rows if row[0] is not None]
    if not vectors:
        return None

    first_shape = vectors[0].shape
    for v in vectors[1:]:
        if v.shape != first_shape:
            raise ValueError(
                "Inconsistent regime embedding shapes for episode_id="
                f"{episode.episode_id}: {v.shape} vs {first_shape}"
            )

    stacked = np.stack(vectors, axis=0)
    return stacked.mean(axis=0).astype(np.float32)


def _compute_text_embedding_for_episode(
    db_manager: DatabaseManager,
    episode: EpisodeSpec,
    *,
    text_model_id: str,
    language: Optional[str] = None,
) -> Optional[np.ndarray]:
    """Compute episode-level text embedding by averaging NEWS embeddings.

    Returns None if no text embeddings are found for the episode window.
    """

    where_clauses = [
        "DATE(na.published_at) BETWEEN %s AND %s",
        "te.source_type = 'NEWS'",
        "te.model_id = %s",
    ]
    params: List[Any] = [episode.start_date, episode.end_date, text_model_id]

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
        logger.warning(
            "No text embeddings found for episode_id=%s window=[%s,%s] model_id=%s",
            episode.episode_id,
            episode.start_date,
            episode.end_date,
            text_model_id,
        )
        return None

    vectors = [np.frombuffer(row[0], dtype=np.float32) for row in rows]
    first_shape = vectors[0].shape
    for v in vectors[1:]:
        if v.shape != first_shape:
            raise ValueError(
                "Inconsistent text embedding shapes for episode_id="
                f"{episode.episode_id}: {v.shape} vs {first_shape}"
            )

    stacked = np.stack(vectors, axis=0)
    return stacked.mean(axis=0).astype(np.float32)


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill joint episode embeddings into joint_embeddings "
            "by combining regime and NEWS text embeddings over episode windows."
        ),
    )

    parser.add_argument(
        "--episodes-file",
        type=str,
        required=True,
        help=(
            "Path to JSON file defining episodes. The file must contain a list of "
            "objects with episode_id, label, region, start_date, end_date."
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
        default="joint-episode-core-v1",
        help="Joint embedding model_id to tag outputs with (default: joint-episode-core-v1)",
    )
    parser.add_argument(
        "--language",
        type=str,
        default=None,
        help="Optional language filter for news_articles.language",
    )

    args = parser.parse_args(argv)

    episodes = _load_episodes_from_file(args.episodes_file)
    if not episodes:
        logger.warning("No episodes loaded from %s; nothing to do", args.episodes_file)
        return

    config = get_config()
    db_manager = DatabaseManager(config)

    examples: List[JointExample] = []

    for episode in episodes:
        z_num = _compute_numeric_embedding_for_episode(db_manager, episode)
        if z_num is None:
            continue

        z_text = _compute_text_embedding_for_episode(
            db_manager,
            episode,
            text_model_id=args.text_model_id,
            language=args.language,
        )
        if z_text is None:
            continue

        if z_num.shape != z_text.shape:
            raise ValueError(
                "Numeric and text episode embeddings must have the same shape; "
                f"got {z_num.shape} and {z_text.shape} for episode_id={episode.episode_id}"
            )

        examples.append(
            JointExample(
                joint_type="EPISODE_V0",
                as_of_date=episode.end_date,
                entity_scope={
                    "episode_id": episode.episode_id,
                    "label": episode.label,
                    "region": episode.region,
                    "window": {
                        "start_date": episode.start_date.isoformat(),
                        "end_date": episode.end_date.isoformat(),
                    },
                    "source": "regime+news",
                },
                numeric_embedding=z_num,
                text_embedding=z_text,
            )
        )

    if not examples:
        logger.warning("No joint episode examples constructed; nothing to write")
        return

    logger.info(
        "Embedding %d joint episodes with joint_model_id=%s", len(examples), args.joint_model_id
    )

    store = JointEmbeddingStore(db_manager=db_manager)
    model = SimpleAverageJointModel()
    service = JointEmbeddingService(model=model, store=store, model_id=args.joint_model_id)

    _ = service.embed_and_store(examples)

    logger.info(
        "Joint episode backfill complete: wrote %d embeddings to joint_embeddings",
        len(examples),
    )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
