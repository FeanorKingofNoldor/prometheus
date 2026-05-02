"""Prometheus v2 – Assessment Engine public API.

This module defines the core in-memory types and orchestration API for the
Assessment Engine (130):

- :class:`InstrumentScore` – per-instrument assessment output.
- :class:`AssessmentModel` – protocol for pluggable scoring models.
- :class:`AssessmentEngine` – façade that delegates to an
  :class:`AssessmentModel` implementation and persists scores via a
  storage abstraction.

The concrete storage implementation lives in :mod:`prometheus.assessment.storage`
(which implements the :class:`InstrumentScoreStorageLike` protocol), and
concrete models live in modules such as
:mod:`prometheus.assessment.model_basic`.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Dict, Mapping, Protocol, Sequence

from apatheon.core.logging import get_logger
from apatheon.core.types import MetadataDict

logger = get_logger(__name__)


@dataclass(frozen=True)
class InstrumentScore:
    """Assessment output for a single instrument and horizon.

    Attributes:
        instrument_id: Identifier of the instrument.
        as_of_date: Date as of which the score was computed.
        horizon_days: Prediction horizon in trading days.
        expected_return: Point estimate of forward return over the horizon.
        score: Normalised score suitable for ranking (model-specific
            scale, but typically centred around 0 with higher = better
            for long positions).
        confidence: Confidence in [0.0, 1.0].
        signal_label: Discrete signal label (e.g. ``STRONG_BUY``, ``BUY``,
            ``HOLD``, ``SELL``, ``STRONG_SELL``).
        alpha_components: Decomposed contributions from different alpha
            families (e.g. value, momentum, fragility_penalty).
        metadata: Optional additional diagnostics and context.
    """

    instrument_id: str
    as_of_date: date
    horizon_days: int
    expected_return: float
    score: float
    confidence: float
    signal_label: str
    alpha_components: Dict[str, float]
    metadata: MetadataDict | None = None


class AssessmentModel(Protocol):
    """Protocol for per-instrument assessment models.

    Implementations encapsulate all feature engineering and scoring logic
    for a given assessment family/model. They are deliberately stateless
    with respect to persistence; the :class:`AssessmentEngine` handles DB
    writes via a storage abstraction.
    """

    def score_instruments(  # pragma: no cover - interface
        self,
        strategy_id: str,
        market_id: str,
        instrument_ids: Sequence[str],
        as_of_date: date,
        horizon_days: int,
    ) -> Dict[str, InstrumentScore]:
        """Score a set of instruments for a given strategy/market/horizon."""


class InstrumentScoreStorageLike(Protocol):
    """Minimal protocol for instrument score storage used by the engine.

    This allows :class:`AssessmentEngine` to depend only on the storage
    behaviour it needs while remaining decoupled from the concrete
    :class:`InstrumentScoreStorage` implementation.
    """

    def save_scores(  # pragma: no cover - interface
        self,
        strategy_id: str,
        market_id: str,
        model_id: str,
        scores: Mapping[str, InstrumentScore],
    ) -> None:
        """Persist a batch of instrument scores for a strategy/market/model."""


@dataclass
class AssessmentEngine:
    """Orchestrator and persistence façade for the Assessment Engine.

    The engine delegates all scoring to an :class:`AssessmentModel`
    instance and uses a storage implementation conforming to
    :class:`InstrumentScoreStorageLike` to persist results into the
    runtime database.
    """

    model: AssessmentModel
    storage: InstrumentScoreStorageLike
    model_id: str

    def score_universe(
        self,
        strategy_id: str,
        market_id: str,
        instrument_ids: Sequence[str],
        as_of_date: date,
        horizon_days: int,
    ) -> Dict[str, InstrumentScore]:
        """Score a list of instruments and persist their scores.

        Returns a mapping from instrument_id to :class:`InstrumentScore`.
        """

        if not instrument_ids:
            return {}

        scores = self.model.score_instruments(
            strategy_id=strategy_id,
            market_id=market_id,
            instrument_ids=instrument_ids,
            as_of_date=as_of_date,
            horizon_days=horizon_days,
        )

        # Persist in a single batch.
        self.storage.save_scores(
            strategy_id=strategy_id,
            market_id=market_id,
            model_id=self.model_id,
            scores=scores,
        )

        logger.info(
            "AssessmentEngine.score_universe: strategy=%s market=%s date=%s horizon=%d instruments=%d",
            strategy_id,
            market_id,
            as_of_date,
            horizon_days,
            len(scores),
        )

        return scores

    def score_strategy_default(
        self,
        strategy_id: str,
        market_id: str,
        as_of_date: date,
    ) -> Dict[str, InstrumentScore]:
        """Score a strategy's default universe at its default horizons.

        For Iteration 4 this convenience method is not yet wired to a
        strategy/universe configuration source and therefore raises
        :class:`NotImplementedError`. Call :meth:`score_universe` with an
        explicit universe instead.
        """

        raise NotImplementedError(
            "score_strategy_default is not implemented yet; use score_universe "
            "with an explicit instrument universe instead"
        )
