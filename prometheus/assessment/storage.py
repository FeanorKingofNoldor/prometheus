"""Prometheus v2 – Assessment Engine storage helpers.

This module provides a thin abstraction around writing instrument-level
assessment scores into the ``instrument_scores`` table in the runtime
database.

For Iteration 4 we implement only batched inserts; deduplication or
model-version-specific pruning can be added later if needed.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from apatheon.core.database import DatabaseManager
from apatheon.core.ids import generate_uuid
from apatheon.core.logging import get_logger
from psycopg2.extras import Json

from prometheus.assessment.api import InstrumentScore

logger = get_logger(__name__)


@dataclass
class InstrumentScoreStorage:
    """Persistence helper for instrument assessment scores.

    The storage writes rows into the ``instrument_scores`` table in the
    runtime database. The expected schema (see 30_database_schema.md) is::

        instrument_scores(
            score_id        TEXT PRIMARY KEY,
            strategy_id     TEXT,
            market_id       TEXT,
            instrument_id   TEXT,
            as_of_date      DATE,
            horizon_days    INTEGER,
            expected_return NUMERIC,
            score           NUMERIC,
            confidence      NUMERIC,
            signal_label    TEXT,
            alpha_components JSONB,
            metadata        JSONB,
            created_at      TIMESTAMPTZ
        )

    No uniqueness is enforced beyond ``score_id``; each engine run may
    emit a fresh set of scores for the same strategy/market/date.
    """

    db_manager: DatabaseManager

    def save_scores(
        self,
        strategy_id: str,
        market_id: str,
        model_id: str,
        scores: Mapping[str, InstrumentScore],
    ) -> None:
        """Insert a batch of instrument scores.

        Args:
            strategy_id: Strategy identifier associated with the scores.
            market_id: Market identifier (e.g. ``US_EQ``).
            model_id: Assessment model identifier (used for tracing but
                not stored as a separate column in this early iteration).
            scores: Mapping from instrument_id to :class:`InstrumentScore`.
        """

        if not scores:
            return

        sql = """
            INSERT INTO instrument_scores (
                score_id,
                strategy_id,
                market_id,
                instrument_id,
                as_of_date,
                horizon_days,
                expected_return,
                score,
                confidence,
                signal_label,
                alpha_components,
                metadata,
                created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """

        with self.db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                for instrument_id, score in scores.items():
                    score_id = generate_uuid()
                    alpha_payload = Json(score.alpha_components or {})

                    # Ensure model_id is captured in metadata for later
                    # analysis (e.g. comparing basic vs context backends).
                    metadata_dict = dict(score.metadata or {})
                    metadata_dict.setdefault("model_id", model_id)
                    metadata_payload = Json(metadata_dict)

                    cursor.execute(
                        sql,
                        (
                            score_id,
                            strategy_id,
                            market_id,
                            instrument_id,
                            score.as_of_date,
                            score.horizon_days,
                            score.expected_return,
                            score.score,
                            score.confidence,
                            score.signal_label,
                            alpha_payload,
                            metadata_payload,
                        ),
                    )
                conn.commit()
            finally:
                cursor.close()

        logger.info(
            "InstrumentScoreStorage.save_scores: strategy=%s market=%s model_id=%s n=%d",
            strategy_id,
            market_id,
            model_id,
            len(scores),
        )
