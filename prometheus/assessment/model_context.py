"""Prometheus v2 – Assessment model based on joint Assessment context.

This module provides an :class:`AssessmentModel` implementation that uses
joint Assessment context embeddings (``ASSESSMENT_CTX_V0`` /
``joint-assessment-context-v1``) as its primary feature source.

The goal is to demonstrate how the Assessment Engine can consume the
joint space directly. The scoring logic is deliberately simple and
deterministic: it maps the L2 norm of the context vector to an
expected_return and normalised score via a hand-crafted transformation.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, Sequence, Tuple

import numpy as np

from apathis.core.database import DatabaseManager
from apathis.core.logging import get_logger
from prometheus.assessment.api import AssessmentModel, InstrumentScore


logger = get_logger(__name__)


@dataclass
class ContextAssessmentModel(AssessmentModel):
    """Assessment model using joint Assessment context embeddings as features.

    For each instrument and as-of date the model:

    - Loads ``ASSESSMENT_CTX_V0`` vector from ``joint_embeddings``.
    - Computes its L2 norm and a simple z-score relative to
      :attr:`norm_reference`.
    - Maps that into ``expected_return`` in
      ``[-max_abs_expected_return, max_abs_expected_return]`` via
      ``tanh``, and a corresponding normalised score in ``[-1, 1]``.

    This is a v0, hand-crafted model intended for experimentation and for
    validating that the joint Assessment context space is wired correctly
    into the Assessment Engine. A future iteration can replace the
    transformation with learned heads trained on historical returns.
    """

    db_manager: DatabaseManager

    # Joint model identifier used when looking up Assessment context
    # vectors in ``joint_embeddings``.
    assessment_context_model_id: str = "joint-assessment-context-v1"

    # Reference scale for context vector norm (approximate typical norm
    # seen in practice; only affects z-score and therefore the mapping to
    # expected_return).
    norm_reference: float = 10.0

    # Maximum absolute expected_return magnitude produced by this model.
    max_abs_expected_return: float = 0.05

    def _load_context_embedding(
        self,
        instrument_id: str,
        as_of_date: date,
    ) -> Tuple[np.ndarray, Dict[str, Any]] | None:
        """Load Assessment context embedding and entity_scope for an instrument.

        Returns ``(vector, entity_scope)`` or ``None`` if no suitable row
        is found.
        """

        sql = """
            SELECT entity_scope, vector
            FROM joint_embeddings
            WHERE joint_type = 'ASSESSMENT_CTX_V0'
              AND model_id = %s
              AND as_of_date = %s
              AND (entity_scope->>'entity_id') = %s
            ORDER BY joint_id DESC
            LIMIT 1
        """

        with self.db_manager.get_historical_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    sql,
                    (
                        self.assessment_context_model_id,
                        as_of_date,
                        instrument_id,
                    ),
                )
                row = cursor.fetchone()
            finally:
                cursor.close()

        if row is None:
            return None

        entity_scope, vector_bytes = row
        if vector_bytes is None:
            return None

        vec = np.frombuffer(vector_bytes, dtype=np.float32)
        if vec.size == 0:
            return None

        if isinstance(entity_scope, dict):
            scope = dict(entity_scope)
        else:
            scope = {}

        return vec, scope

    def _build_score(
        self,
        instrument_id: str,
        strategy_id: str,
        market_id: str,
        as_of_date: date,
        horizon_days: int,
    ) -> InstrumentScore:
        """Compute an :class:`InstrumentScore` from context embedding only."""

        ctx = self._load_context_embedding(instrument_id, as_of_date)

        insufficient_context = False
        ctx_norm = 0.0
        ctx_norm_z = 0.0
        ctx_scope: Dict[str, Any] = {}

        if ctx is None:
            insufficient_context = True
        else:
            vec, ctx_scope = ctx
            ctx_norm = float(np.linalg.norm(vec))
            if ctx_norm == 0.0:
                insufficient_context = True
            else:
                ref = self.norm_reference if self.norm_reference > 0.0 else ctx_norm
                if ref <= 0.0:
                    ref = ctx_norm
                if ref <= 0.0:
                    insufficient_context = True
                else:
                    ctx_norm_z = float((ctx_norm - ref) / ref)

        if insufficient_context:
            expected_return = 0.0
            score = 0.0
            confidence = 0.0
            label = "HOLD"
        else:
            max_abs = self.max_abs_expected_return
            if max_abs <= 0.0:
                max_abs = 0.05

            # Map context norm z-score into a bounded expected_return.
            expected_return = max_abs * float(np.tanh(ctx_norm_z))

            # Normalised score derived directly from expected_return.
            score = float(max(-1.0, min(1.0, expected_return / max_abs)))

            # Confidence increases with |ctx_norm_z| but is clipped to [0, 1].
            confidence = float(min(1.0, max(0.0, abs(ctx_norm_z))))

            # Discrete signal label thresholds in expected_return space.
            if expected_return >= 0.03:
                label = "STRONG_BUY"
            elif expected_return >= 0.01:
                label = "BUY"
            elif expected_return <= -0.03:
                label = "STRONG_SELL"
            elif expected_return <= -0.01:
                label = "SELL"
            else:
                label = "HOLD"

        alpha_components: Dict[str, float] = {
            "ctx_norm": float(ctx_norm),
            "ctx_norm_z": float(ctx_norm_z),
        }

        metadata: Dict[str, Any] = {
            "strategy_id": strategy_id,
            "market_id": market_id,
            "insufficient_context": insufficient_context,
            "assessment_ctx_model_id": self.assessment_context_model_id,
            "ctx_scope": ctx_scope,
        }

        return InstrumentScore(
            instrument_id=instrument_id,
            as_of_date=as_of_date,
            horizon_days=horizon_days,
            expected_return=float(expected_return),
            score=float(score),
            confidence=float(confidence),
            signal_label=label,
            alpha_components=alpha_components,
            metadata=metadata,
        )

    def score_instruments(
        self,
        strategy_id: str,
        market_id: str,
        instrument_ids: Sequence[str],
        as_of_date: date,
        horizon_days: int,
    ) -> Dict[str, InstrumentScore]:  # type: ignore[override]
        """Score a batch of instruments using context embeddings only."""

        if horizon_days <= 0:
            raise ValueError("horizon_days must be positive")

        scores: Dict[str, InstrumentScore] = {}
        for instrument_id in instrument_ids:
            try:
                scores[instrument_id] = self._build_score(
                    instrument_id=instrument_id,
                    strategy_id=strategy_id,
                    market_id=market_id,
                    as_of_date=as_of_date,
                    horizon_days=horizon_days,
                )
            except Exception:  # pragma: no cover - defensive
                logger.exception(
                    "ContextAssessmentModel.score_instruments: failed to score instrument %s on %s",
                    instrument_id,
                    as_of_date,
                )
        return scores
