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
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Dict, Sequence

import numpy as np
from apatheon.core.database import DatabaseManager
from apatheon.core.logging import get_logger
from apatheon.core.time import TradingCalendar
from apatheon.data.reader import DataReader
from apatheon.stability.storage import StabilityStorage
from apatheon.stability.types import SoftTargetState

from prometheus.assessment.api import AssessmentModel, InstrumentScore
from prometheus.pricing_utils import adjusted_close_series as _adjusted_close_series

logger = get_logger(__name__)

# Maximum number of per-instrument insufficient-history warnings to emit
# per (strategy_id, as_of_date) before switching to a single summary
# message. This keeps logs from exploding when many instruments share the
# same data gap.
_WARNING_LIMIT_PER_RUN = 50
_WARNING_MAX_KEYS = 500  # Bound dict size to prevent memory leak in long-running daemons
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
    assessment_context_model_id: str = "joint-issuer-day-core-v1"

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

    # Most-recent trading days EXCLUDED from the momentum lookback (the
    # "skip-month" of the 12-1/6-1 momentum convention). The last month of
    # returns exhibits short-term reversal, so measuring momentum up to
    # t-21 instead of t improves IC. Realised vol still uses the full
    # window (it is a risk estimate, not a signal). Set to 0 to disable.
    momentum_skip_days: int = 21

    # Signal-label thresholds for the standardized (cross-sectional
    # z-score) path, in z units. The legacy buy/sell thresholds below are
    # on the raw-return scale and only apply to the single-instrument
    # fallback path.
    z_buy_threshold: float = 0.5
    z_strong_buy_threshold: float = 1.5

    # Rough calibration for expected_return on the standardized path:
    # forward return attributed per 1 sigma of cross-sectional momentum
    # tilt, per 21 trading days of horizon. Placeholder until live
    # scorecard data provides an empirical spread; consumers should treat
    # expected_return as indicative, `score` as the ranking signal.
    expected_return_per_sigma: float = 0.025

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

        Uses batch-loaded price cache if available (set by score_instruments),
        otherwise falls back to individual DB query.

        Raises ValueError if there is insufficient price history.
        """

        if window_days <= 0:
            raise ValueError("window_days must be positive")

        min_required = max(2, int(window_days * 0.85))

        # Try batch cache first (populated by score_instruments)
        cache = getattr(self, "_price_cache", {})
        if instrument_id in cache:
            closes = cache[instrument_id]
            if len(closes) < min_required:
                raise ValueError(
                    f"Insufficient price rows ({len(closes)}, need {min_required}) "
                    f"for {instrument_id} on {as_of_date}"
                )
        else:
            # Fallback: individual DB query (for single-instrument scoring)
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
            if df.empty or len(df) < min_required:
                raise ValueError(
                    f"Insufficient price rows ({len(df)}, need {min_required}) for {instrument_id} between {start_date} and {as_of_date}"
                )

            df_sorted = df.sort_values(["trade_date"]).reset_index(drop=True)
            closes = _adjusted_close_series(df_sorted)

        # Momentum over the window EXCLUDING the most recent
        # ``momentum_skip_days`` (short-term reversal region). Falls back
        # to the full window when history is too short for the skip.
        skip = max(0, int(self.momentum_skip_days))
        end_idx = -1 - skip if skip and len(closes) > skip + 1 else -1
        if closes[0] > 0.0:
            momentum = float((closes[end_idx] - closes[0]) / closes[0])
        else:
            momentum = 0.0

        log_rets = np.zeros_like(closes, dtype=float)
        log_rets[1:] = np.log(closes[1:] / closes[:-1])
        realised_vol = float(np.std(log_rets[1:], ddof=1)) if log_rets.shape[0] > 1 else 0.0

        return momentum, realised_vol

    @staticmethod
    def _vol_scaled_momentum(momentum: float, realised_vol: float) -> float:
        """Vol-scale the lookback return by its realised vol over the window.

        Dividing the raw momentum by realised vol converts a return into a
        per-unit-risk number (a Sharpe-like quantity). Without this, raw
        momentum is dominated by high-vol names and the signal is effectively a
        volatility bet (the harness showed strong NEGATIVE vol-IC for the raw
        version). When realised vol is unavailable/zero we fall back to the raw
        momentum so the signal degrades gracefully rather than blowing up.
        """
        if realised_vol is not None and realised_vol > 1e-9:
            return float(momentum / realised_vol)
        return float(momentum)

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
        """Return L2 norm of joint embedding for an instrument, if enabled.

        The L2 norm serves as an uncertainty/regime-anomaly proxy:
        higher norms indicate the instrument is in an unusual regime state
        relative to training history. Used as a small confidence adjustment.

        When ``use_assessment_context`` is False or ``db_manager`` is
        None, this returns None without querying the database.
        """

        if not self.use_assessment_context or self.db_manager is None:
            return None

        # Strip .US suffix to get issuer_id from instrument_id
        issuer_id = instrument_id.replace(".US", "").replace(".us", "")

        sql = """
            SELECT vector
            FROM joint_embeddings
            WHERE joint_type = 'ISSUER_DAY_V0'
              AND model_id = %s
              AND as_of_date = %s
              AND (entity_scope->>'issuer_id') = %s
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
                        issuer_id,
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
        momentum_component: float | None = None,
    ) -> InstrumentScore:
        """Compute an InstrumentScore for a single instrument.

        This method is resilient to data gaps: if price history is
        insufficient, it returns a neutral HOLD score with zero
        confidence and an ``insufficient_history`` flag in metadata.

        ``momentum_component`` is the cross-sectionally standardized,
        vol-scaled momentum z-score for this instrument on ``as_of_date``,
        precomputed by :meth:`score_instruments` so every name is z-scored
        against the same as-of cross-section. When None (single-instrument
        scoring without a cross-section), the method falls back to the raw
        vol-scaled momentum for this instrument alone.
        """

        window_days = self.momentum_window_days

        insufficient_history = False
        # Reuse price features computed once in score_instruments' Pass 1 so we
        # never re-query/recompute (and never double-count read_prices calls).
        feature_cache = getattr(self, "_feature_cache", None)
        have_cache_entry = feature_cache is not None and instrument_id in feature_cache
        cached_features = feature_cache.get(instrument_id) if have_cache_entry else None
        try:
            if have_cache_entry:
                if cached_features is None:
                    # Pass 1 recorded insufficient history for this instrument.
                    raise ValueError(
                        f"Insufficient price history for {instrument_id} on {as_of_date}"
                    )
                momentum, realised_vol = cached_features
            else:
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

                # Prune oldest entries if dict grows too large
                if len(_warning_counts) > _WARNING_MAX_KEYS:
                    _warning_counts.clear()

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

        # Sector guidance penalty: when >25% of a sector's companies have
        # lowered guidance, apply a mild penalty to all instruments in that sector.
        # Uses _instrument_sectors cache (batch-loaded by score_instruments).
        guidance_penalty = 0.0
        sector_guidance = getattr(self, "_sector_guidance", {})
        inst_sectors = getattr(self, "_instrument_sectors", {})
        if sector_guidance:
            sector = inst_sectors.get(instrument_id)
            if sector and sector in sector_guidance:
                pct_lowered = sector_guidance[sector]
                if pct_lowered > 0.25:
                    # 25% lowered = 0, 50% = 0.025, 75% = 0.05
                    guidance_penalty = min(0.05, (pct_lowered - 0.25) * 0.10)

        # Momentum component: vol-scaled, then cross-sectionally standardized
        # (z-scored within the as-of cross-section). ``momentum_component`` is
        # passed in by score_instruments after computing the cross-section mean
        # and std of every name's vol-scaled momentum.
        #
        # Two scales coexist:
        #  - standardized (batch / production) path: momentum_component is a
        #    z-score (~unit std). Penalties (raw-return scale, ~0.05) are mapped
        #    onto that z-scale by dividing by momentum_ref so they stay
        #    comparable to the standardized momentum, and the [-1,1] band uses a
        #    2-std reference (covers ~95% of the cross-section).
        #  - fallback (single-instrument, no cross-section) path: when no
        #    cross-section z-score is available we keep the original raw-return
        #    momentum and the original momentum_ref mapping, so standalone
        #    _build_score behaviour is unchanged and penalties still bite.
        standardized = momentum_component is not None
        if standardized:
            momentum_component = 0.0 if insufficient_history else float(momentum_component)
            penalty_term = (
                self.fragility_penalty_weight * fragility_penalty + guidance_penalty
            ) / self.momentum_ref
            ref = 2.0
        else:
            # Original behaviour: raw lookback return as the momentum component.
            momentum_component = 0.0 if insufficient_history else float(momentum)
            penalty_term = (
                self.fragility_penalty_weight * fragility_penalty + guidance_penalty
            )
            ref = self.momentum_ref if self.momentum_ref > 0.0 else 0.10

        # Raw score = momentum component; adjusted by fragility + guidance.
        raw_score = float(momentum_component)
        adjusted_score = raw_score - penalty_term

        # Map adjusted_score into a roughly [-1, 1] band for ranking.
        normalised_score = float(max(-1.0, min(1.0, adjusted_score / ref)))

        # Confidence uses adjusted_score (not raw) so penalties are reflected.
        confidence = 0.0
        if not insufficient_history:
            confidence = float(min(1.0, max(0.0, abs(adjusted_score) / ref)))

        # Discrete signal label. On the standardized path adjusted_score is
        # a z-score, so thresholds must be in z units — the legacy raw-return
        # thresholds (0.01/0.03) would label nearly every above-median name
        # STRONG_BUY on that scale.
        label = "HOLD"
        if standardized:
            buy_t, strong_t = self.z_buy_threshold, self.z_strong_buy_threshold
        else:
            buy_t, strong_t = self.buy_threshold, self.strong_buy_threshold
        sell_t = buy_t if standardized else self.sell_threshold
        strong_sell_t = strong_t if standardized else self.strong_sell_threshold
        if adjusted_score >= strong_t:
            label = "STRONG_BUY"
        elif adjusted_score >= buy_t:
            label = "BUY"
        elif adjusted_score <= -strong_sell_t:
            label = "STRONG_SELL"
        elif adjusted_score <= -sell_t:
            label = "SELL"

        alpha_components: Dict[str, float] = {
            "momentum": float(momentum),
            "momentum_component": float(momentum_component),
            "fragility_penalty": float(fragility_penalty),
            "guidance_penalty": float(guidance_penalty),
        }

        metadata = {
            "window_days": window_days,
            "realised_vol": realised_vol,
            "vol_scaled_momentum": float(self._vol_scaled_momentum(momentum, realised_vol)),
            "momentum_component": float(momentum_component),
            "strategy_id": strategy_id,
            "market_id": market_id,
            "weak_profile": weak_profile,
            "insufficient_history": insufficient_history,
        }
        if soft_class_str is not None:
            metadata["soft_target_class"] = soft_class_str

        # expected_return semantics: on the standardized path adjusted_score
        # is a z-score, NOT a return — storing it verbatim poisons any
        # consumer that treats the field as a forward-return estimate. Map z
        # to an indicative return via the configured per-sigma spread,
        # scaled by horizon and capped. On the raw fallback path the
        # adjusted score already is a return-scale quantity.
        if standardized:
            expected_return = float(
                np.clip(
                    adjusted_score
                    * self.expected_return_per_sigma
                    * (horizon_days / 21.0),
                    -0.20,
                    0.20,
                )
            )
            metadata["momentum_z"] = float(momentum_component)
            metadata["expected_return_calibration"] = "z_x_per_sigma_v1"
        else:
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
        """Score a batch of instruments for a given strategy/market/horizon.

        Batch-loads all instrument prices in a single DB query, then
        scores each instrument from the in-memory cache.
        """

        if horizon_days <= 0:
            raise ValueError("horizon_days must be positive")

        ids_list = list(instrument_ids)
        window_days = self.momentum_window_days

        # ── Batch-load prices for ALL instruments in one query ──────
        search_start = as_of_date - timedelta(days=window_days * 3)
        trading_days = self.calendar.trading_days_between(search_start, as_of_date)
        if len(trading_days) >= window_days:
            price_start = trading_days[-window_days]
        else:
            price_start = search_start

        df_all = self.data_reader.read_prices(ids_list, price_start, as_of_date)

        # Build per-instrument close arrays: {instrument_id: np.ndarray}
        self._price_cache: Dict[str, np.ndarray] = {}
        if not df_all.empty:
            for inst_id, grp in df_all.groupby("instrument_id"):
                sorted_grp = grp.sort_values("trade_date")
                self._price_cache[str(inst_id)] = _adjusted_close_series(sorted_grp)

        # ── Load instrument→sector mapping (batch) ──────────────────
        self._instrument_sectors: Dict[str, str] = {}
        if self.db_manager is not None:
            try:
                with self.db_manager.get_runtime_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            SELECT i.instrument_id, ic.sector
                            FROM instruments i
                            JOIN issuer_classifications ic ON i.issuer_id = ic.issuer_id
                            WHERE i.instrument_id = ANY(%s)
                        """, (ids_list,))
                        for inst_id, sector in cur.fetchall():
                            self._instrument_sectors[inst_id] = sector
            except Exception:
                logger.debug("Failed to load instrument sectors", exc_info=True)

        # ── Load sector guidance breadth (corporate guidance signal) ──
        self._sector_guidance: Dict[str, float] = {}  # sector → pct_lowered
        if self.db_manager is not None:
            try:
                with self.db_manager.get_runtime_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            SELECT sector, direction, COUNT(*) as cnt
                            FROM corporate_guidance
                            WHERE filing_date >= %s AND filing_date <= %s
                              AND direction IN ('raised', 'lowered')
                            GROUP BY sector, direction
                        """, (as_of_date - timedelta(days=90), as_of_date))
                        sector_counts: Dict[str, Dict[str, int]] = {}
                        for sector, direction, cnt in cur.fetchall():
                            if sector not in sector_counts:
                                sector_counts[sector] = {"raised": 0, "lowered": 0}
                            sector_counts[sector][direction] = cnt
                        for sector, counts in sector_counts.items():
                            total = counts["raised"] + counts["lowered"]
                            if total >= 3:  # Need at least 3 data points
                                self._sector_guidance[sector] = counts["lowered"] / total
            except Exception:
                logger.debug("Failed to load sector guidance (table may not exist yet)", exc_info=True)

        # ── Pass 1: cross-sectional standardization of vol-scaled momentum ──
        # Compute each name's price features ONCE (cached in _feature_cache so
        # Pass 2 / _build_score never recompute), derive its vol-scaled
        # momentum, then z-score the whole as-of cross-section so the momentum
        # component is a standardized tilt rather than a raw return (which is a
        # volatility bet). Names with insufficient history are recorded as None
        # in the feature cache, excluded from the mean/std estimate, and get a
        # neutral 0.0 momentum component downstream.
        self._feature_cache: Dict[str, tuple[float, float] | None] = {}
        vol_scaled: Dict[str, float] = {}
        for inst_id in ids_list:
            try:
                mom, rvol = self._compute_price_features(
                    instrument_id=inst_id,
                    as_of_date=as_of_date,
                    window_days=window_days,
                )
            except ValueError:
                self._feature_cache[inst_id] = None
                continue
            self._feature_cache[inst_id] = (mom, rvol)
            vol_scaled[inst_id] = self._vol_scaled_momentum(mom, rvol)

        momentum_z: Dict[str, float] = {}
        if vol_scaled:
            # Robust standardization: median/MAD instead of mean/std. A
            # single outlier (bad print, residual split artifact, near-zero
            # vol name blowing up the momentum/vol ratio) inflates the plain
            # std and compresses every other name's z toward zero; the MAD
            # is insensitive to it. 1.4826 scales MAD to std-equivalent
            # units under normality so downstream z thresholds keep their
            # usual interpretation.
            vals = np.array(list(vol_scaled.values()), dtype=float)
            center = float(np.median(vals))
            mad = float(np.median(np.abs(vals - center)))
            sigma = 1.4826 * mad
            if sigma <= 1e-12:
                # MAD degenerate (>=50% identical values): fall back to std.
                sigma = float(np.std(vals, ddof=1)) if vals.size > 1 else 0.0
            if sigma > 1e-12:
                for inst_id, v in vol_scaled.items():
                    momentum_z[inst_id] = (v - center) / sigma
            else:
                # Degenerate cross-section (all equal): everyone neutral.
                for inst_id in vol_scaled:
                    momentum_z[inst_id] = 0.0

        # ── Pass 2: score each instrument from cache ────────────────
        scores: Dict[str, InstrumentScore] = {}

        def _score_one(inst_id: str) -> tuple[str, InstrumentScore | None]:
            try:
                score = self._build_score(
                    instrument_id=inst_id,
                    strategy_id=strategy_id,
                    market_id=market_id,
                    as_of_date=as_of_date,
                    horizon_days=horizon_days,
                    momentum_component=momentum_z.get(inst_id),
                )
                return inst_id, score
            except Exception:  # pragma: no cover - defensive
                logger.exception(
                    "BasicAssessmentModel.score_instruments: failed to score instrument %s on %s",
                    inst_id,
                    as_of_date,
                )
                return inst_id, None

        for inst_id in ids_list:
            inst_id, score = _score_one(inst_id)
            if score is not None:
                scores[inst_id] = score

        # Clear caches after use
        self._price_cache = {}
        self._sector_guidance = {}
        self._instrument_sectors = {}
        self._feature_cache = {}

        return scores
