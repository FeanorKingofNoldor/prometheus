"""Prometheus v2 – Basic numeric AssessmentModel implementation.

This module implements a simple, fully deterministic assessment model
based on:

- Recent price momentum and realised volatility from ``prices_daily``.
- Optional fragility penalties derived from the latest STAB state.

The goal is to provide a minimal but real AssessmentModel that can be
used for early experiments and end-to-end wiring without introducing a
heavy ML stack.
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Dict, Sequence

import numpy as np
from apathis.core.database import DatabaseManager
from apathis.core.logging import get_logger
from apathis.core.time import TradingCalendar
from apathis.data.reader import DataReader
from apathis.stability.storage import StabilityStorage
from apathis.stability.types import SoftTargetState

from prometheus.assessment.api import AssessmentModel, InstrumentScore

logger = get_logger(__name__)

# Maximum number of per-instrument insufficient-history warnings to emit
# per (strategy_id, as_of_date) before switching to a single summary
# message. This keeps logs from exploding when many instruments share the
# same data gap.
_WARNING_LIMIT_PER_RUN = 50
_warning_counts: Dict[str, int] = {}
_warning_lock = threading.Lock()


@dataclass
class BasicAssessmentModel(AssessmentModel):
    """Price/STAB-based implementation of :class:`AssessmentModel`.

    This model computes a simple momentum-style score for each
    instrument, then applies a penalty based on the latest STAB
    soft-target state when available.

    Optionally, it can also look up joint Assessment context embeddings
    (``ASSESSMENT_CTX_V0`` / ``joint-assessment-context-v1``) from the
    ``joint_embeddings`` table and record simple diagnostics (e.g.
    L2-norm) in the score metadata. This does not currently affect the
    numeric score and is intended for analysis and future model
    development.
    """

    data_reader: DataReader
    calendar: TradingCalendar
    stability_storage: StabilityStorage | None = None
    db_manager: DatabaseManager | None = None

    # If True, attempt to load joint Assessment context embeddings from
    # ``joint_embeddings`` and attach a basic norm diagnostic to
    # InstrumentScore.metadata.
    use_assessment_context: bool = False

    # Joint model identifier used when looking up Assessment context
    # vectors.
    assessment_context_model_id: str = "joint-assessment-context-v1"

    # Trading-day lookback window for momentum and realised-vol computation.
    # Empirically, cross-sectional momentum works on 6–12 month lookbacks
    # (126–252 trading days).  Using the same window as horizon_days (21 days)
    # puts the model in short-term reversal territory, producing negative IC.
    # Default is 126 days (~6 months), the canonical cross-sectional momentum
    # window in the factor literature.  Decoupled from horizon_days so signal
    # formation and prediction horizon are independently tunable.
    momentum_window_days: int = 126

    # Reference scale for mapping raw momentum into a normalised score.
    # 6-month moves are larger than 1-month; 20% (~1 std of annual return)
    # gives a well-distributed signal across the universe.
    momentum_ref: float = 0.20  # 20% move over 6-month window

    # Strength of the fragility penalty applied to raw momentum. Higher
    # values produce more conservative scores in the presence of high
    # soft-target scores.
    #
    # The penalty is (soft_target_score / 100) * weight, so with the
    # median STAB score ~32 the effective penalty at weight=0.15 is
    # ~0.048 — comparable to typical momentum magnitudes (±0.05–0.15).
    # The previous default of 1.0 caused penalty ~0.32 which dominated
    # momentum and clipped virtually all normalised scores to -1.0.
    fragility_penalty_weight: float = 0.15

    # Additional multiplier applied to the fragility penalty when the STAB
    # state reports ``weak_profile=True``.
    weak_profile_penalty_multiplier: float = 0.5

    # Thresholds for mapping adjusted scores into discrete signal labels.
    buy_threshold: float = 0.01
    strong_buy_threshold: float = 0.03
    sell_threshold: float = 0.01
    strong_sell_threshold: float = 0.03

    # Maximum number of worker threads to use when scoring instruments in
    # parallel. A value of 1 preserves the original single-threaded
    # behaviour.
    max_workers: int = 1

    def _compute_price_features(
        self,
        instrument_id: str,
        as_of_date: date,
        window_days: int,
    ) -> tuple[float, float]:
        """Return (momentum, realised_vol) for the given window.

        Raises ValueError if there is insufficient price history.
        """

        if window_days <= 0:
            raise ValueError("window_days must be positive")

        search_start = as_of_date - timedelta(days=window_days * 3)
        trading_days = self.calendar.trading_days_between(search_start, as_of_date)
        if len(trading_days) < window_days:
            raise ValueError(
                f"Not enough trading history to compute assessment window of {window_days} days "
                f"for {instrument_id} ending at {as_of_date}"
            )

        window_days_list = trading_days[-window_days:]
        start_date = window_days_list[0]

        df = self.data_reader.read_prices([instrument_id], start_date, as_of_date)
        if df.empty or len(df) < window_days:
            raise ValueError(
                f"Insufficient price rows ({len(df)}) for {instrument_id} between {start_date} and {as_of_date}"
            )

        df_sorted = df.sort_values(["trade_date"]).reset_index(drop=True)
        df_window = df_sorted.tail(window_days)
        closes = df_window["close"].astype(float).to_numpy()

        if closes.shape[0] != window_days:
            raise ValueError(
                "Price history length does not match expected window size: "
                f"{closes.shape[0]} != {window_days}"
            )

        if closes[0] > 0.0:
            momentum = float((closes[-1] - closes[0]) / closes[0])
        else:
            momentum = 0.0

        log_rets = np.zeros_like(closes, dtype=float)
        log_rets[1:] = np.log(closes[1:] / closes[:-1])
        realised_vol = float(np.std(log_rets[1:], ddof=1)) if log_rets.shape[0] > 1 else 0.0

        return momentum, realised_vol

    def _lookup_stab_state(self, instrument_id: str, as_of_date: date) -> SoftTargetState | None:
        if self.stability_storage is None:
            return None
        try:
            return self.stability_storage.get_latest_state(
                "INSTRUMENT",
                instrument_id,
                as_of_date=as_of_date,
            )
        except Exception:  # pragma: no cover - defensive
            logger.exception(
                "BasicAssessmentModel._lookup_stab_state: failed to load STAB state for instrument %s",
                instrument_id,
            )
            return None

    # ------------------------------------------------------------------
    # Optional joint Assessment context lookup
    # ------------------------------------------------------------------

    def _load_assessment_context_norm(
        self,
        instrument_id: str,
        as_of_date: date,
    ) -> float | None:
        """Return L2 norm of joint Assessment context embedding, if enabled.

        When ``use_assessment_context`` is False or ``db_manager`` is
        None, this returns None without querying the database.
        """

        if not self.use_assessment_context or self.db_manager is None:
            return None

        sql = """
            SELECT vector
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

        (vector_bytes,) = row
        if vector_bytes is None:
            return None

        vec = np.frombuffer(vector_bytes, dtype=np.float32)
        if vec.size == 0:
            return None
        return float(np.linalg.norm(vec))

    def _build_score(
        self,
        instrument_id: str,
        strategy_id: str,
        market_id: str,
        as_of_date: date,
        horizon_days: int,
    ) -> InstrumentScore:
        """Compute an InstrumentScore for a single instrument.

        This method is resilient to data gaps: if price history is
        insufficient, it returns a neutral HOLD score with zero
        confidence and an ``insufficient_history`` flag in metadata.
        """

        window_days = self.momentum_window_days

        insufficient_history = False
        try:
            momentum, realised_vol = self._compute_price_features(
                instrument_id=instrument_id,
                as_of_date=as_of_date,
                window_days=window_days,
            )
        except ValueError as exc:
            # Throttle noisy warnings when many instruments lack sufficient
            # history for the same strategy/date. We log the first
            # _WARNING_LIMIT_PER_RUN per (strategy_id, as_of_date) and then a
            # single summary message, suppressing the rest.
            key = f"{strategy_id}:{as_of_date.isoformat()}"
            with _warning_lock:
                count = _warning_counts.get(key, 0)
                if count < _WARNING_LIMIT_PER_RUN:
                    logger.warning(
                        "BasicAssessmentModel._build_score: insufficient history for %s on %s: %s",
                        instrument_id,
                        as_of_date,
                        exc,
                    )
                    _warning_counts[key] = count + 1
                elif count == _WARNING_LIMIT_PER_RUN:
                    logger.warning(
                        "BasicAssessmentModel._build_score: further insufficient-history "
                        "warnings suppressed for strategy_id=%s as_of_date=%s",
                        strategy_id,
                        as_of_date,
                    )
                    _warning_counts[key] = count + 1

            momentum = 0.0
            realised_vol = 0.0
            insufficient_history = True

        stab_state = self._lookup_stab_state(instrument_id, as_of_date)

        fragility_penalty = 0.0
        weak_profile = False
        soft_class_str: str | None = None
        if stab_state is not None:
            fragility_penalty = stab_state.soft_target_score / 100.0
            weak_profile = stab_state.weak_profile
            soft_class_str = stab_state.soft_target_class.value
            if weak_profile:
                fragility_penalty *= 1.0 + self.weak_profile_penalty_multiplier

        # Optional joint Assessment context diagnostic (L2 norm of
        # ASSESSMENT_CTX_V0 vector) – does not affect scoring for now.
        assessment_ctx_norm = self._load_assessment_context_norm(
            instrument_id=instrument_id,
            as_of_date=as_of_date,
        )

        # Raw score = simple momentum; adjusted by fragility penalty.
        raw_score = momentum
        adjusted_score = raw_score - self.fragility_penalty_weight * fragility_penalty

        # Map adjusted_score into a roughly [-1, 1] band for ranking.
        ref = self.momentum_ref if self.momentum_ref > 0.0 else 0.10
        normalised_score = 0.0
        if ref > 0.0:
            normalised_score = float(max(-1.0, min(1.0, adjusted_score / ref)))

        # Confidence increases with absolute raw momentum but is clipped
        # to [0, 1].
        conf_ref = self.momentum_ref if self.momentum_ref > 0.0 else 0.10
        confidence = 0.0
        if not insufficient_history and conf_ref > 0.0:
            confidence = float(min(1.0, max(0.0, abs(raw_score) / conf_ref)))

        # Discrete signal label.
        label = "HOLD"
        if adjusted_score >= self.strong_buy_threshold:
            label = "STRONG_BUY"
        elif adjusted_score >= self.buy_threshold:
            label = "BUY"
        elif adjusted_score <= -self.strong_sell_threshold:
            label = "STRONG_SELL"
        elif adjusted_score <= -self.sell_threshold:
            label = "SELL"

        alpha_components: Dict[str, float] = {
            "momentum": float(momentum),
            "fragility_penalty": float(fragility_penalty),
        }

        metadata = {
            "window_days": window_days,
            "realised_vol": realised_vol,
            "strategy_id": strategy_id,
            "market_id": market_id,
            "weak_profile": weak_profile,
            "insufficient_history": insufficient_history,
        }
        if soft_class_str is not None:
            metadata["soft_target_class"] = soft_class_str
        if assessment_ctx_norm is not None:
            metadata["assessment_ctx_norm"] = assessment_ctx_norm

        expected_return = float(adjusted_score)

        return InstrumentScore(
            instrument_id=instrument_id,
            as_of_date=as_of_date,
            horizon_days=horizon_days,
            expected_return=expected_return,
            score=normalised_score,
            confidence=confidence,
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
        """Score a batch of instruments for a given strategy/market/horizon."""

        if horizon_days <= 0:
            raise ValueError("horizon_days must be positive")

        scores: Dict[str, InstrumentScore] = {}

        def _score_one(inst_id: str) -> tuple[str, InstrumentScore | None]:
            try:
                score = self._build_score(
                    instrument_id=inst_id,
                    strategy_id=strategy_id,
                    market_id=market_id,
                    as_of_date=as_of_date,
                    horizon_days=horizon_days,
                )
                return inst_id, score
            except Exception:  # pragma: no cover - defensive
                logger.exception(
                    "BasicAssessmentModel.score_instruments: failed to score instrument %s on %s",
                    inst_id,
                    as_of_date,
                )
                return inst_id, None

        ids_list = list(instrument_ids)
        if self.max_workers > 1 and len(ids_list) > 1:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {executor.submit(_score_one, inst_id): inst_id for inst_id in ids_list}
                for fut in as_completed(futures):
                    inst_id, score = fut.result()
                    if score is not None:
                        scores[inst_id] = score
        else:
            for inst_id in ids_list:
                inst_id, score = _score_one(inst_id)
                if score is not None:
                    scores[inst_id] = score

        return scores
