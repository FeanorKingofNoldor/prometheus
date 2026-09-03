"""Prometheus v2 – Universe engine.

The Universe engine constructs per-date trading universes based on
available instruments, stability (STAB) scores, profiles, and basic
liquidity filters. It follows the same pattern as the Regime and STAB
engines:

- UniverseModel encapsulates all selection logic.
- UniverseEngine orchestrates and persists results via UniverseStorage.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Protocol, Sequence

import numpy as np
from apatheon.core.database import DatabaseManager
from apatheon.core.ids import generate_uuid
from apatheon.core.logging import get_logger
from apatheon.core.time import TradingCalendar
from apatheon.data.classifications import DEFAULT_CLASSIFICATION_TAXONOMY
from apatheon.data.reader import DataReader
from apatheon.profiles.service import ProfileService
from apatheon.stability.storage import StabilityStorage
from apatheon.stability.types import SoftTargetClass
from psycopg2.extras import Json

from prometheus.execution.eligibility import (
    load_ineligible_instrument_ids,
    static_fallback_ineligible_ids,
)
from prometheus.pricing_utils import adjusted_close_series

logger = get_logger(__name__)

# One-shot guard so the survivorship-bias warning is logged once per process.
_SURVIVORSHIP_WARNED = [False]

# Constant positive offset applied to the standardized (z-scored) ranking blend
# before the multiplicative risk-haircut modifiers. Larger than any realistic
# |z| so the shifted score stays positive (keeping "haircut reduces rank"
# semantics) while preserving the cross-sectional ordering of the blend.
_Z_SCORE_OFFSET = 10.0


def _default_assessment_horizon_days() -> int:
    """Return assessment horizon from env var or compiled default (21)."""
    raw = os.environ.get("PROMETHEUS_ASSESSMENT_HORIZON_DAYS")
    if raw is not None:
        try:
            return int(raw)
        except (ValueError, TypeError):
            pass
    return 21


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class UniverseMember:
    """Single universe membership decision for an entity.

    Attributes:
        as_of_date: Date the universe is defined for.
        universe_id: Logical universe identifier (e.g. "CORE_EQ").
        entity_type: Entity type (currently "INSTRUMENT").
        entity_id: Identifier of the entity.
        included: Whether the entity is included in the universe.
        score: A numeric ranking score (higher = more attractive).
        reasons: Structured diagnostics explaining the decision.
        tier: Qualitative tier for the entity (e.g. "CORE", "SATELLITE",
            "EXCLUDED"). This is primarily used for higher-level
            portfolio construction and monitoring; the :attr:`included`
            flag continues to drive the effective universe in this
            iteration.
    """

    as_of_date: date
    universe_id: str
    entity_type: str
    entity_id: str
    included: bool
    score: float
    reasons: dict[str, float | str | bool]
    tier: str = "EXCLUDED"


@dataclass
class UniverseStorage:
    """Persistence helper for universe membership decisions."""

    db_manager: DatabaseManager

    def save_members(self, members: Sequence[UniverseMember]) -> None:
        """Insert or upsert a batch of universe members.

        Uses INSERT ... ON CONFLICT to ensure one row per
        (universe_id, as_of_date, entity_type, entity_id).
        """

        if not members:
            return

        sql = """
            INSERT INTO universe_members (
                universe_member_id,
                universe_id,
                as_of_date,
                entity_type,
                entity_id,
                tier,
                included,
                score,
                reasons,
                created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (universe_id, as_of_date, entity_type, entity_id)
            DO UPDATE SET
                tier = EXCLUDED.tier,
                included = EXCLUDED.included,
                score = EXCLUDED.score,
                reasons = EXCLUDED.reasons,
                created_at = NOW()
        """

        with self.db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                for m in members:
                    universe_member_id = generate_uuid()
                    reasons_payload = Json(m.reasons)
                    cursor.execute(
                        sql,
                        (
                            universe_member_id,
                            m.universe_id,
                            m.as_of_date,
                            m.entity_type,
                            m.entity_id,
                            m.tier,
                            m.included,
                            m.score,
                            reasons_payload,
                        ),
                    )
                conn.commit()
            finally:
                cursor.close()

    def get_universe(
        self,
        as_of_date: date,
        universe_id: str,
        entity_type: str = "INSTRUMENT",
        included_only: bool = True,
    ) -> list[UniverseMember]:
        """Load universe members for a given date/universe.

        If ``included_only`` is True, only returns included entities.
        """

        if included_only:
            sql = """
                SELECT as_of_date,
                       universe_id,
                       entity_type,
                       entity_id,
                       tier,
                       included,
                       score,
                       reasons
                FROM universe_members
                WHERE universe_id = %s
                  AND as_of_date = %s
                  AND entity_type = %s
                  AND included = TRUE
                ORDER BY score DESC, entity_id ASC
            """
            params = (universe_id, as_of_date, entity_type)
        else:
            sql = """
                SELECT as_of_date,
                       universe_id,
                       entity_type,
                       entity_id,
                       tier,
                       included,
                       score,
                       reasons
                FROM universe_members
                WHERE universe_id = %s
                  AND as_of_date = %s
                  AND entity_type = %s
                ORDER BY included DESC, score DESC, entity_id ASC
            """
            params = (universe_id, as_of_date, entity_type)

        with self.db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, params)
                rows = cursor.fetchall()
            finally:
                cursor.close()

        members: list[UniverseMember] = []
        for (
            as_of_db,
            univ_id_db,
            ent_type_db,
            ent_id_db,
            tier_db,
            included_db,
            score_db,
            reasons_db,
        ) in rows:
            members.append(
                UniverseMember(
                    as_of_date=as_of_db,
                    universe_id=univ_id_db,
                    entity_type=ent_type_db,
                    entity_id=ent_id_db,
                    included=bool(included_db),
                    score=float(score_db),
                    reasons=reasons_db or {},
                    tier=str(tier_db) if tier_db is not None else "EXCLUDED",
                )
            )
        return members


# ---------------------------------------------------------------------------
# Model / engine interfaces
# ---------------------------------------------------------------------------


class UniverseModel(Protocol):
    """Protocol for universe construction models.

    Implementations encapsulate all selection logic and return a list of
    :class:`UniverseMember` objects for a given date/universe.
    """

    def build_universe(self, as_of_date: date, universe_id: str) -> list[UniverseMember]:
        ...  # pragma: no cover - interface


@dataclass
class UniverseEngine:
    """Orchestrator and persistence façade for universe construction."""

    model: UniverseModel
    storage: UniverseStorage

    def build_and_save(self, as_of_date: date, universe_id: str) -> list[UniverseMember]:
        """Build a universe and persist its members.

        Returns the list of :class:`UniverseMember` objects.
        """

        members = self.model.build_universe(as_of_date, universe_id)
        self.storage.save_members(members)

        logger.info(
            "UniverseEngine.build_and_save: date=%s universe=%s members=%d included=%d",
            as_of_date,
            universe_id,
            len(members),
            sum(1 for m in members if m.included),
        )

        return members

    def get_universe(
        self,
        as_of_date: date,
        universe_id: str,
        entity_type: str = "INSTRUMENT",
        included_only: bool = True,
    ) -> list[UniverseMember]:
        """Convenience wrapper around :meth:`UniverseStorage.get_universe`."""

        return self.storage.get_universe(as_of_date, universe_id, entity_type, included_only)


# ---------------------------------------------------------------------------
# Basic universe model implementation
# ---------------------------------------------------------------------------


@dataclass
class BasicUniverseModel:
    """Basic price/profile/STAB/Assessment-based equity universe model.

    This implementation focuses on equity instruments in specified
    markets and uses:

    - 63-day realised volatility and average volume for basic liquidity.
    - Latest STAB soft-target state for fragility filters.
    - ProfileService-derived structural risk (including leverage via
      ``weak_profile``) propagated through STAB.
    - Optional Assessment scores (from ``instrument_scores``) to favour
      names with stronger alpha in the ranking.

    The selection logic is deterministic and parameterised by a small
    set of thresholds, with optional global and sector-level capacity
    constraints and tiering (CORE/SATELLITE/EXCLUDED).
    """

    db_manager: DatabaseManager
    calendar: TradingCalendar
    data_reader: DataReader
    profile_service: ProfileService
    stability_storage: StabilityStorage

    market_ids: Sequence[str] = ("US_EQ",)

    # Classification taxonomy used when attaching issuer sector/industry.
    classification_taxonomy: str = DEFAULT_CLASSIFICATION_TAXONOMY

    # Transitional fallback: when True, we fall back to the denormalized
    # issuers.sector value if no as-of classification is available.
    allow_legacy_issuer_sector_fallback: bool = False

    # NOTE: Market cap filtering is intentionally omitted. The liquidity
    # filter (min_avg_volume) combined with STAB/fragility scoring is
    # sufficient to exclude illiquid micro-caps without imposing an
    # arbitrary capitalisation threshold.
    min_avg_volume: float = 100_000.0
    max_soft_target_score: float = 80.0
    exclude_breakers: bool = True
    exclude_weak_profile_when_fragile: bool = True

    # Optional global and per-sector capacity limits. A value of ``None``
    # or ``<= 0`` disables the corresponding cap.
    max_universe_size: int | None = None
    sector_max_names: int | None = None

    # Hard price floor; instruments with last close below this are
    # excluded even if they otherwise pass liquidity and STAB checks.
    min_price: float = 0.0

    # Explicit exclusion lists applied before any scoring.
    hard_exclusion_list: Sequence[str] = ()
    issuer_exclusion_list: Sequence[str] = ()

    window_days: int = 63

    # Fraction of ``window_days`` for which an instrument must have price
    # observations to be eligible. Requiring *every* trading day in the
    # window makes the universe brittle: a single missed ingestion day
    # collapses coverage to only the always-ingested core names. A small
    # tolerance (default 90%) keeps the liquidity/vol estimates robust
    # while surviving occasional upstream ingestion gaps.
    min_history_coverage: float = 0.9

    # Optional Assessment integration. When ``use_assessment_scores`` is
    # True and ``assessment_strategy_id`` is provided, the model will
    # read scores from ``instrument_scores`` for the given
    # (strategy_id, market_ids, as_of_date, assessment_horizon_days) and
    # incorporate them into the ranking score.
    use_assessment_scores: bool = False
    assessment_strategy_id: str | None = None
    assessment_horizon_days: int = field(default_factory=lambda: _default_assessment_horizon_days())
    assessment_score_weight: float = 50.0

    # Optional regime-conditional, IC-weighted signal-combination layer.
    #
    # DEFAULT OFF (2026-06-11): when ``signal_combiner`` is None the blend is the
    # existing additive z-blend (``z(base) + alpha_weight_z * z(alpha)``), so
    # production behaviour is UNCHANGED. When a
    # ``prometheus.research.combiner.SignalCombiner`` is injected, the per-date
    # cross-section is combined by it instead, over the standardized components
    # {momentum-z / alpha "alpha_z", STAB/liquidity base "base_z"}. A regime
    # label for the as-of date may be supplied via ``regime_label_provider``
    # (any object exposing ``get_label(as_of_date) -> str | RegimeLabel | None``;
    # None → the combiner falls back to its default weight set). This is
    # research/backtest infrastructure for when better signals exist; it is not
    # wired in the daily pipeline.
    signal_combiner: object | None = None
    regime_label_provider: object | None = None

    # Optional global regime risk integration. When ``regime_forecaster``
    # is provided and ``regime_risk_alpha`` is non-zero, the model will
    # query a per-region, per-horizon regime risk score and apply a
    # multiplicative modifier to all candidate scores on a given date.
    # The forecaster is expected to expose a ``forecast(region,
    # horizon_steps)`` method returning an object with ``risk_score`` and
    # ``p_change_any`` attributes (e.g. RegimeStateChangeForecaster).
    regime_forecaster: object | None = None
    regime_region: str = "GLOBAL"
    regime_risk_alpha: float = 0.0
    regime_risk_horizon_steps: int = 1

    # Optional STAB state-change risk integration. When
    # ``stability_state_change_forecaster`` is provided and
    # ``stability_risk_alpha`` is non-zero, the model will query a
    # per-instrument soft-target state-change risk and apply a
    # multiplicative modifier to the ranking score. The forecaster is
    # expected to expose a ``forecast(entity_id, horizon_steps)`` method
    # returning an object with a ``risk_score`` attribute in [0, 1] and,
    # optionally, additional diagnostics such as ``p_worsen_any`` and
    # ``p_to_targetable_or_breaker``.
    stability_state_change_forecaster: object | None = None
    stability_risk_alpha: float = 0.0
    stability_risk_horizon_steps: int = 1

    # Optional nation risk integration. When ``nation_score_provider`` is
    # provided and ``nation_risk_alpha`` is non-zero, the model will
    # query the nation composite_stability score and apply a multiplicative
    # modifier to candidate scores. The provider is expected to expose a
    # ``get_latest(nation, as_of_date=)`` method returning an object with
    # ``composite_stability`` in [0, 1].
    #
    # NOT WIRED in the daily pipeline (2026-06-11): the daily UNIVERSES path
    # (run_universes_for_run in pipeline/tasks.py) does not pass a
    # ``nation_score_provider``, so it defaults to None and the nation
    # modifier early-returns the score unchanged. The hook is retained for
    # research/backtest callers that inject a provider explicitly. To enable
    # it in production, wire a NationScoreStorage-like provider plus a
    # non-zero ``nation_risk_alpha`` at the daily call site.
    nation_score_provider: object | None = None
    nation_risk_alpha: float = 0.0
    nation_risk_nation: str = "USA"

    # Optional lambda opportunity integration.
    #
    # SHELVED as additive alpha (2026-06-11): lambda is an opportunity-density
    # / uncertainty (risk-vol) signal, not alpha. As additive alpha it sizes UP
    # into high-uncertainty clusters and hurts Sharpe. The production daily
    # config (configs/universe/core_long_eq_daily.yaml) now sets
    # ``lambda_score_weight: 0.0`` so the additive ``lambda_w * lambda_score``
    # term below is never applied in the live pipeline. The provider/CSV infra
    # is KEPT (the term is fully gated on a non-zero weight) so lambda can be
    # repurposed as a risk signal later. ``lambda_score_weight`` defaults to
    # 0.0 here; the term only activates when a caller sets a non-zero weight.
    #
    # Historically a single ``lambda_score_weight`` affected both:
    #  - the universe inclusion ranking (which names get included), and
    #  - the score stored on UniverseMember.score (used by the portfolio
    #    model for sizing).
    # For research we often want to separate these effects. The
    # *_selection/_portfolio weights, when provided, take precedence over
    # the legacy ``lambda_score_weight``.
    lambda_score_provider: object | None = None
    lambda_score_weight: float = 0.0
    lambda_score_weight_selection: float | None = None
    lambda_score_weight_portfolio: float | None = None

    def _enumerate_instruments(self, as_of_date: date) -> list[tuple[str, str, str, str, str]]:
        """Return list of (instrument_id, issuer_id, sector, market_id, sector_source).

        Sector is primarily sourced from the time-versioned
        ``issuer_classifications`` table *as-of* the requested date.

        For transition/back-compat we optionally fall back to
        ``issuers.sector`` when no as-of classification exists.

        The resulting sector is used for optional sector caps in the tiering
        phase and for coarse cluster assignment.

        Point-in-time membership / survivorship: the ``instruments`` table has
        no listing/delisting *date* columns (only a current ``status`` of
        ACTIVE/DELISTED plus a metadata ``is_delisted`` flag), so we cannot
        derive exact membership by date here. We therefore include both ACTIVE
        and DELISTED equities and rely on the price-availability prefilter in
        ``build_universe`` (requires a close price *on* as_of_date) to enforce
        tradability point-in-time: a name that delisted in the past is included
        on the historical dates it actually traded and naturally dropped once
        it has no price. This removes the hard survivorship exclusion of
        delisted names (e.g. LEH.US). It remains approximate — without true
        delisting dates we cannot model the final trading day or include names
        that never ingested prices. Populate listing/delisting dates upstream
        for exact PIT membership.
        """

        sector_expr = "COALESCE(NULLIF(ic.sector, ''), 'UNKNOWN')"
        sector_source_case = "CASE WHEN NULLIF(ic.sector, '') IS NOT NULL THEN 'issuer_classifications' ELSE 'UNKNOWN' END"
        joins = ""

        if self.allow_legacy_issuer_sector_fallback:
            sector_expr = "COALESCE(NULLIF(ic.sector, ''), NULLIF(u.sector, ''), 'UNKNOWN')"
            sector_source_case = (
                "CASE "
                "WHEN NULLIF(ic.sector, '') IS NOT NULL THEN 'issuer_classifications' "
                "WHEN NULLIF(u.sector, '') IS NOT NULL THEN 'issuers' "
                "ELSE 'UNKNOWN' END"
            )
            joins = "LEFT JOIN issuers AS u ON u.issuer_id = i.issuer_id"

        sql = f"""
            SELECT
                i.instrument_id,
                i.issuer_id,
                {sector_expr} AS sector,
                i.market_id,
                {sector_source_case} AS sector_source
            FROM instruments AS i
            LEFT JOIN LATERAL (
                SELECT ic.sector
                FROM issuer_classifications AS ic
                WHERE ic.issuer_id = i.issuer_id
                  AND ic.taxonomy = %s
                  AND ic.effective_start <= %s
                  AND (ic.effective_end IS NULL OR %s < ic.effective_end)
                ORDER BY ic.effective_start DESC
                LIMIT 1
            ) AS ic ON TRUE
            {joins}
            WHERE i.market_id = ANY(%s)
              AND i.asset_class = 'EQUITY'
              AND i.status IN ('ACTIVE', 'DELISTED')
              AND i.instrument_id NOT LIKE 'SYNTH_%%'
        """

        with self.db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    sql,
                    (
                        self.classification_taxonomy,
                        as_of_date,
                        as_of_date,
                        list(self.market_ids),
                    ),
                )
                rows = cursor.fetchall()
            finally:
                cursor.close()

        if not _SURVIVORSHIP_WARNED[0]:
            _SURVIVORSHIP_WARNED[0] = True
            logger.warning(
                "Universe enumeration includes DELISTED equities; point-in-time membership "
                "relies on the close-price-on-as_of_date prefilter because the instruments "
                "table has no listing/delisting date columns. Historical universes remain "
                "approximate (final trading day not modelled; names without ingested prices "
                "are still missing) until listing/delisting dates are populated upstream."
            )

        return [
            (str(inst_id), str(issuer_id), str(sector), str(market_id), str(sector_source))
            for inst_id, issuer_id, sector, market_id, sector_source in rows
        ]

    def _compute_liquidity_features(self, instrument_id: str, as_of_date: date) -> dict[str, float]:
        """Compute 63d average volume and realised volatility for an instrument.

        Returns an empty dict if there is insufficient history.
        """

        if self.window_days <= 0:
            return {}

        # Minimum number of observed trading days required within the
        # window. A small tolerance (``min_history_coverage``) prevents a
        # few missed upstream ingestion days from collapsing the universe.
        coverage = min(max(self.min_history_coverage, 0.0), 1.0)
        min_required = max(2, int(round(self.window_days * coverage)))

        search_start = as_of_date - timedelta(days=self.window_days * 3)
        trading_days = self.calendar.trading_days_between(search_start, as_of_date)
        if len(trading_days) < min_required:
            return {}

        window_days = trading_days[-self.window_days :]
        start_date = window_days[0]

        df = self.data_reader.read_prices([instrument_id], start_date, as_of_date)
        if df.empty or len(df) < min_required:
            return {}

        df_sorted = df.sort_values(["trade_date"]).reset_index(drop=True)
        df_window = df_sorted.tail(self.window_days)

        # Realised vol runs on ADJUSTED closes (split discontinuities read
        # as fake returns); the price-level filter keeps the RAW close
        # because it screens on the actual trade price.
        closes = df_window["close"].astype(float).to_numpy()
        adj_closes = adjusted_close_series(df_window)
        volumes = df_window["volume"].astype(float).to_numpy()

        if closes.shape[0] < min_required:
            return {}

        log_rets = np.zeros_like(adj_closes, dtype=float)
        log_rets[1:] = np.log(adj_closes[1:] / adj_closes[:-1])

        sigma = float(np.std(log_rets[1:], ddof=1)) if log_rets.shape[0] > 1 else 0.0
        avg_volume = float(volumes.mean()) if volumes.size > 0 else 0.0
        last_close = float(closes[-1]) if closes.size > 0 else 0.0

        return {
            "realised_vol_63d": sigma,
            "avg_volume_63d": avg_volume,
            "last_close": last_close,
        }

    def _load_assessment_scores(self, as_of_date: date) -> dict[str, float]:
        """Load Assessment scores for the configured strategy/markets/date.

        Returns a mapping from instrument_id to assessment score. If
        Assessment integration is disabled or scores are unavailable,
        returns an empty dict.
        """

        if not self.use_assessment_scores or not self.assessment_strategy_id:
            return {}

        # ``instrument_scores`` has no uniqueness beyond ``score_id`` (each run,
        # and each model, may emit a fresh row for the same
        # strategy/market/instrument/date/horizon). Deduplicate to the most
        # recently written row per instrument so the loaded score is
        # deterministic instead of arbitrary across duplicate runs.
        sql = """
            SELECT DISTINCT ON (instrument_id) instrument_id, score
            FROM instrument_scores
            WHERE strategy_id = %s
              AND market_id = ANY(%s)
              AND as_of_date = %s
              AND horizon_days = %s
            ORDER BY instrument_id, created_at DESC
        """

        with self.db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    sql,
                    (
                        self.assessment_strategy_id,
                        list(self.market_ids),
                        as_of_date,
                        self.assessment_horizon_days,
                    ),
                )
                rows = cursor.fetchall()
            except Exception:  # pragma: no cover - defensive
                # If the scores table is missing or the query fails for any
                # reason, log and fall back to no Assessment integration.
                logger.exception(
                    "BasicUniverseModel._load_assessment_scores: failed to load scores for "
                    "strategy=%s markets=%s as_of=%s horizon=%d",
                    self.assessment_strategy_id,
                    self.market_ids,
                    as_of_date,
                    self.assessment_horizon_days,
                )
                rows = []
            finally:
                cursor.close()

        scores: dict[str, float] = {}
        for instrument_id, score in rows:
            scores[str(instrument_id)] = float(score)

        return scores

    def _assign_cluster(self, market_id: str, sector: str, stab_state) -> str:
        """Assign a coarse cluster identifier for an instrument.

        This v1 implementation groups instruments by (market_id, sector,
        soft-target class). The resulting ``cluster_id`` is used for
        opportunity-density (lambda) experiments and can be refined in
        later iterations to incorporate regimes and profiles.
        """

        stab_class = getattr(stab_state.soft_target_class, "value", str(stab_state.soft_target_class))
        return f"{market_id}|{sector}|{stab_class}"

    def build_universe(self, as_of_date: date, universe_id: str) -> list[UniverseMember]:
        """Construct a universe for the given date/universe_id.

        The current implementation:

        - Considers all active equity instruments in configured markets.
        - Requires sufficient price history.
        - Excludes instruments with average volume below ``min_avg_volume``.
        - Excludes instruments with soft-target score > ``max_soft_target_score``.
        - Optionally excludes BREAKER-class names and fragile names with
          weak profiles.
        - Optionally applies global and per-sector caps and assigns CORE
          and SATELLITE tiers to included members.
        """

        instruments = self._enumerate_instruments(as_of_date)

        # EU-retail PRIIPs purchase eligibility (live-parity on paper): an
        # instrument the live account cannot BUY must never enter a book.
        # Loaded ONCE per build; ``load_ineligible_instrument_ids`` already
        # degrades to a static known-US-ETF snapshot on DB errors, and the
        # extra guard here catches anything else so an infrastructure
        # failure is loud and can never silently re-admit SPY.US et al.
        try:
            retail_ineligible_ids: set[str] = load_ineligible_instrument_ids(self.db_manager)
        except Exception:  # pragma: no cover - defensive
            logger.exception(
                "BasicUniverseModel.build_universe: retail-eligibility load FAILED on %s — "
                "using the static known-US-ETF fallback so PRIIPs-blocked instruments stay "
                "excluded (fail-open only for instruments unknown to the snapshot).",
                as_of_date,
            )
            retail_ineligible_ids = static_fallback_ineligible_ids()

        # Pre-filter candidates by whether they have a close price on as_of_date.
        # This avoids spending compute on stale/delisted instruments (still marked
        # ACTIVE in the runtime instruments table) and provides a dedicated
        # exclusion reason for explainability.
        price_today_ids: set[str] | None = None
        try:
            instrument_ids_all = [
                inst_id
                for inst_id, _issuer_id, _sector, _market_id, _sector_source in instruments
            ]
            if instrument_ids_all:
                df_today = self.data_reader.read_prices_close(
                    instrument_ids_all,
                    as_of_date,
                    as_of_date,
                )
                if df_today.empty:
                    price_today_ids = set()
                else:
                    price_today_ids = {
                        str(inst_id)
                        for inst_id, close in zip(
                            df_today["instrument_id"].astype(str),
                            df_today["close"].astype(float),
                        )
                        if float(close) > 0.0
                    }
            else:
                price_today_ids = set()
        except Exception:  # pragma: no cover - defensive
            logger.exception(
                "BasicUniverseModel.build_universe: failed to prefilter instruments by price on %s",
                as_of_date,
            )
            price_today_ids = None

        # Optional Assessment scores keyed by instrument_id.
        assessment_scores: dict[str, float] = {}
        if self.use_assessment_scores:
            assessment_scores = self._load_assessment_scores(as_of_date)

        # Members that fail hard filters and are immediately excluded.
        hard_fail_members: list[UniverseMember] = []

        # Candidates that pass hard filters and are eligible for capacity
        # constraints and tiering. Each element is
        # (instrument_id, issuer_id, sector, score, reasons).
        candidates: list[tuple[str, str, str, float, dict[str, float | str | bool]]] = []

        # Pending candidates collected in the per-instrument loop. The final
        # blended score requires cross-sectional statistics (mean/std of the
        # STAB+liquidity base and of the alpha) that are only known once the
        # whole cross-section is built, so we defer the blend to a second pass.
        pending: list[dict] = []

        for instrument_id, issuer_id, sector, market_id, sector_source in instruments:
            reasons: dict[str, float | str | bool] = {
                "sector": sector,
                "sector_source": sector_source,
                "market_id": market_id,
            }

            if instrument_id in self.hard_exclusion_list:
                reasons["hard_excluded_instrument"] = True
                hard_fail_members.append(
                    UniverseMember(
                        as_of_date=as_of_date,
                        universe_id=universe_id,
                        entity_type="INSTRUMENT",
                        entity_id=instrument_id,
                        included=False,
                        score=0.0,
                        reasons=reasons,
                        tier="EXCLUDED",
                    )
                )
                continue

            if issuer_id in self.issuer_exclusion_list:
                reasons["hard_excluded_issuer"] = True
                hard_fail_members.append(
                    UniverseMember(
                        as_of_date=as_of_date,
                        universe_id=universe_id,
                        entity_type="INSTRUMENT",
                        entity_id=instrument_id,
                        included=False,
                        score=0.0,
                        reasons=reasons,
                        tier="EXCLUDED",
                    )
                )
                continue

            # EU-retail PRIIPs gate: US-domiciled packaged products (ETFs
            # etc.) cannot be BOUGHT on the live account, so they never
            # enter the universe (paper must mirror live).
            if instrument_id in retail_ineligible_ids:
                reasons["retail_ineligible_priips"] = True
                hard_fail_members.append(
                    UniverseMember(
                        as_of_date=as_of_date,
                        universe_id=universe_id,
                        entity_type="INSTRUMENT",
                        entity_id=instrument_id,
                        included=False,
                        score=0.0,
                        reasons=reasons,
                        tier="EXCLUDED",
                    )
                )
                continue

            # Require a close price on as_of_date (tradable today). If we
            # failed to compute the price_today_ids set, we skip this filter.
            has_price_today = True
            if price_today_ids is not None:
                has_price_today = instrument_id in price_today_ids
            reasons["has_price_today"] = has_price_today
            if price_today_ids is not None and not has_price_today:
                reasons["no_price_today"] = True
                hard_fail_members.append(
                    UniverseMember(
                        as_of_date=as_of_date,
                        universe_id=universe_id,
                        entity_type="INSTRUMENT",
                        entity_id=instrument_id,
                        included=False,
                        score=0.0,
                        reasons=reasons,
                        tier="EXCLUDED",
                    )
                )
                continue

            # Liquidity and basic realised vol.
            liq = self._compute_liquidity_features(instrument_id, as_of_date)
            if not liq:
                reasons["insufficient_history"] = True
                hard_fail_members.append(
                    UniverseMember(
                        as_of_date=as_of_date,
                        universe_id=universe_id,
                        entity_type="INSTRUMENT",
                        entity_id=instrument_id,
                        included=False,
                        score=0.0,
                        reasons=reasons,
                        tier="EXCLUDED",
                    )
                )
                continue

            realised_vol = liq["realised_vol_63d"]
            avg_volume = liq["avg_volume_63d"]
            last_close = liq.get("last_close", 0.0)
            reasons["realised_vol_63d"] = realised_vol
            reasons["avg_volume_63d"] = avg_volume
            reasons["last_close"] = last_close

            if avg_volume < self.min_avg_volume:
                reasons["illiquid"] = True
                hard_fail_members.append(
                    UniverseMember(
                        as_of_date=as_of_date,
                        universe_id=universe_id,
                        entity_type="INSTRUMENT",
                        entity_id=instrument_id,
                        included=False,
                        score=0.0,
                        reasons=reasons,
                        tier="EXCLUDED",
                    )
                )
                continue

            if self.min_price > 0.0 and last_close < self.min_price:
                reasons["below_min_price"] = True
                hard_fail_members.append(
                    UniverseMember(
                        as_of_date=as_of_date,
                        universe_id=universe_id,
                        entity_type="INSTRUMENT",
                        entity_id=instrument_id,
                        included=False,
                        score=0.0,
                        reasons=reasons,
                        tier="EXCLUDED",
                    )
                )
                continue

            # STAB soft-target state (no look-ahead).
            stab_state = self.stability_storage.get_latest_state(
                "INSTRUMENT",
                instrument_id,
                as_of_date=as_of_date,
            )
            if stab_state is None:
                reasons["no_stab_state"] = True
                hard_fail_members.append(
                    UniverseMember(
                        as_of_date=as_of_date,
                        universe_id=universe_id,
                        entity_type="INSTRUMENT",
                        entity_id=instrument_id,
                        included=False,
                        score=0.0,
                        reasons=reasons,
                        tier="EXCLUDED",
                    )
                )
                continue

            reasons["soft_target_score"] = stab_state.soft_target_score
            reasons["soft_target_class"] = stab_state.soft_target_class.value
            reasons["weak_profile"] = stab_state.weak_profile

            # Coarse cluster identifier for lambda/opportunity-density work.
            cluster_id = self._assign_cluster(market_id, sector, stab_state)
            reasons["cluster_id"] = cluster_id

            # Lambda opportunity score is cluster-level. For research we may
            # apply different weights for:
            #  (a) universe selection/inclusion ranking
            #  (b) portfolio sizing (UniverseMember.score)
            lambda_w_selection = (
                self.lambda_score_weight_selection
                if self.lambda_score_weight_selection is not None
                else float(self.lambda_score_weight)
            )
            lambda_w_portfolio = (
                self.lambda_score_weight_portfolio
                if self.lambda_score_weight_portfolio is not None
                else float(self.lambda_score_weight)
            )

            lambda_score_f: float | None = None
            if (
                self.lambda_score_provider is not None
                and (lambda_w_selection != 0.0 or lambda_w_portfolio != 0.0)
            ):
                provider_fn = getattr(self.lambda_score_provider, "get_cluster_score", None)
                if provider_fn is not None:
                    try:
                        lambda_score = provider_fn(
                            as_of_date=as_of_date,
                            market_id=market_id,
                            sector=sector,
                            soft_target_class=stab_state.soft_target_class.value,
                        )
                    except Exception:  # pragma: no cover - defensive
                        # TODO(issue-24): Lambda provider errors are silently swallowed.
                        # Consider tracking failure rate and disabling the provider if it
                        # fails consistently, rather than silently falling back to zero
                        # lambda weight per instrument.
                        logger.exception(
                            "BasicUniverseModel: lambda provider failed for as_of=%s market=%s sector=%s soft_target_class=%s",
                            as_of_date,
                            market_id,
                            sector,
                            stab_state.soft_target_class.value,
                        )
                        lambda_score = None

                    if lambda_score is not None:
                        lambda_score_f = float(lambda_score)
                        reasons["lambda_score"] = lambda_score_f
                        # Preserve legacy key for diagnostics; use portfolio
                        # weight as the effective score weight.
                        reasons["lambda_score_weight"] = float(lambda_w_portfolio)
                        reasons["lambda_score_weight_selection"] = float(lambda_w_selection)
                        reasons["lambda_score_weight_portfolio"] = float(lambda_w_portfolio)

                        experiment_id = getattr(self.lambda_score_provider, "experiment_id", None)
                        if experiment_id is not None:
                            reasons["lambda_experiment_id"] = str(experiment_id)
                        score_column = getattr(self.lambda_score_provider, "score_column", None)
                        if score_column is not None:
                            reasons["lambda_score_column"] = str(score_column)

            # Attach Assessment score if available.
            ass_score = assessment_scores.get(instrument_id)
            if ass_score is not None:
                reasons["assessment_score"] = ass_score

            if self.exclude_breakers and stab_state.soft_target_class == SoftTargetClass.BREAKER:
                reasons["excluded_breaker"] = True
                hard_fail_members.append(
                    UniverseMember(
                        as_of_date=as_of_date,
                        universe_id=universe_id,
                        entity_type="INSTRUMENT",
                        entity_id=instrument_id,
                        included=False,
                        score=0.0,
                        reasons=reasons,
                        tier="EXCLUDED",
                    )
                )
                continue
            if stab_state.soft_target_score > self.max_soft_target_score:
                reasons["excluded_high_soft_target_score"] = True
                hard_fail_members.append(
                    UniverseMember(
                        as_of_date=as_of_date,
                        universe_id=universe_id,
                        entity_type="INSTRUMENT",
                        entity_id=instrument_id,
                        included=False,
                        score=0.0,
                        reasons=reasons,
                        tier="EXCLUDED",
                    )
                )
                continue
            if (
                self.exclude_weak_profile_when_fragile
                and stab_state.weak_profile
                and stab_state.soft_target_class
                in {SoftTargetClass.FRAGILE, SoftTargetClass.TARGETABLE, SoftTargetClass.BREAKER}
            ):
                reasons["excluded_weak_profile_fragile"] = True
                hard_fail_members.append(
                    UniverseMember(
                        as_of_date=as_of_date,
                        universe_id=universe_id,
                        entity_type="INSTRUMENT",
                        entity_id=instrument_id,
                        included=False,
                        score=0.0,
                        reasons=reasons,
                        tier="EXCLUDED",
                    )
                )
                continue

            # Ranking base: favour lower soft-target (STAB) scores and higher
            # liquidity. This is the fragility/liquidity quality screen.
            base_score = max(0.0, 100.0 - stab_state.soft_target_score) + min(
                50.0, avg_volume / 1_000_000.0
            )
            reasons["base_score_raw"] = float(base_score)

            # Alpha: the Assessment expected-return signal. We keep the SIGN
            # (do NOT clip negative alpha here — clipping discarded half the
            # signal and turned the universe into a pure fragility/liquidity
            # screen). Both base and alpha are standardized cross-sectionally in
            # the post-loop pass so neither swamps the other.
            alpha_raw: float | None = None
            if self.use_assessment_scores and ass_score is not None:
                alpha_raw = float(ass_score)
                reasons["alpha_raw"] = alpha_raw

            # Defer the blend + risk modifiers until the whole cross-section is
            # known (we need its mean/std to standardize base and alpha).
            pending.append(
                {
                    "instrument_id": instrument_id,
                    "issuer_id": issuer_id,
                    "sector": sector,
                    "base_score": float(base_score),
                    "alpha_raw": alpha_raw,
                    "lambda_score_f": lambda_score_f,
                    "lambda_w_selection": float(lambda_w_selection),
                    "lambda_w_portfolio": float(lambda_w_portfolio),
                    "reasons": reasons,
                }
            )

        # ------------------------------------------------------------------
        # Cross-sectional standardization + blend (post-loop, second pass)
        # ------------------------------------------------------------------
        # Put the STAB/liquidity base and the Assessment alpha on a COMMON
        # z-scale so alpha actually tilts the ranking instead of being swamped
        # by the 0-150 base. We z-score each across the as-of cross-section,
        # then blend: score = z(base) + assessment_score_weight_z * z(alpha),
        # where the alpha weight is expressed in "base std-devs per alpha
        # std-dev" so it is scale-free. Negative alpha keeps its sign and
        # genuinely pushes a name down the ranking.
        def _zmap(values: dict[str, float]) -> dict[str, float]:
            if not values:
                return {}
            arr = np.array(list(values.values()), dtype=float)
            mu = float(np.mean(arr))
            sigma = float(np.std(arr, ddof=1)) if arr.size > 1 else 0.0
            if sigma <= 1e-12:
                return {k: 0.0 for k in values}
            return {k: (v - mu) / sigma for k, v in values.items()}

        base_vals = {p["instrument_id"]: p["base_score"] for p in pending}
        alpha_vals = {
            p["instrument_id"]: p["alpha_raw"]
            for p in pending
            if p["alpha_raw"] is not None
        }
        base_z = _zmap(base_vals)
        alpha_z = _zmap(alpha_vals)

        # Convert the legacy 0-150-scale assessment weight into a z-scale tilt.
        # The old code multiplied raw alpha by assessment_score_weight (default
        # 50) against a base spanning ~150; on the standardized scale a tilt of
        # ~1 std of alpha per std of base is comparable, so we normalise the
        # configured weight by 50 (its default) to preserve operator intent
        # while keeping both components on the same z-scale.
        alpha_weight_z = (
            float(self.assessment_score_weight) / 50.0
            if self.use_assessment_scores
            else 0.0
        )

        # Optional regime-conditional, IC-weighted combiner (default OFF). When
        # injected, it replaces the additive z-blend below with a learned/
        # configured weighting of the standardized components. Resolved once per
        # cross-section (weights are cross-section-wide).
        combined_blend: dict[str, float] | None = None
        combiner_regime: str | None = None
        if self.signal_combiner is not None:
            regime_label = None
            if self.regime_label_provider is not None:
                get_label = getattr(self.regime_label_provider, "get_label", None)
                if get_label is not None:
                    try:
                        regime_label = get_label(as_of_date)
                    except Exception:  # pragma: no cover - defensive
                        logger.exception(
                            "BasicUniverseModel: regime_label_provider failed on %s",
                            as_of_date,
                        )
                        regime_label = None
            combiner_regime = getattr(regime_label, "value", regime_label)
            combined_blend = self.signal_combiner.combine_cross_section(
                {"base_z": base_z, "momentum_z": alpha_z, "alpha_z": alpha_z},
                regime_label=regime_label,
            )

        for p in pending:
            instrument_id = p["instrument_id"]
            reasons = p["reasons"]
            lambda_score_f = p["lambda_score_f"]

            bz = base_z.get(instrument_id, 0.0)
            az = alpha_z.get(instrument_id, 0.0)
            reasons["base_score_z"] = float(bz)
            if instrument_id in alpha_z:
                reasons["alpha_z"] = float(az)
                reasons["alpha_contrib_z"] = float(alpha_weight_z * az)

            if combined_blend is not None:
                blended_z = float(combined_blend.get(instrument_id, 0.0))
                reasons["combiner_blend_z"] = blended_z
                if combiner_regime is not None:
                    reasons["combiner_regime"] = str(combiner_regime)
            else:
                blended_z = bz + alpha_weight_z * az
            reasons["score_base_z"] = float(blended_z)

            # The downstream risk modifiers are MULTIPLICATIVE haircuts
            # (score * (1 - alpha*risk)) designed for a positive score: a
            # haircut should always *reduce* a name's rank. A z-scored blend is
            # signed, so we shift it into a positive range by a fixed offset
            # (well above any realistic |z|) before the haircuts. The offset is
            # constant across the cross-section, so it preserves the relative
            # ordering from the standardized blend while restoring correct
            # haircut semantics.
            score_base = _Z_SCORE_OFFSET + blended_z
            reasons["score_base"] = float(score_base)

            score_selection = score_base
            score_portfolio = score_base
            if lambda_score_f is not None:
                lw_sel = p["lambda_w_selection"]
                lw_port = p["lambda_w_portfolio"]
                score_selection = score_base + lw_sel * float(lambda_score_f)
                score_portfolio = score_base + lw_port * float(lambda_score_f)
                reasons["lambda_score_contrib_selection"] = lw_sel * float(lambda_score_f)
                reasons["lambda_score_contrib_portfolio"] = lw_port * float(lambda_score_f)

            reasons["score_selection_pre_risk"] = float(score_selection)
            reasons["score_portfolio_pre_risk"] = float(score_portfolio)

            # Apply optional per-instrument STAB state-change risk modifier
            # before global regime risk. We compute the multiplier once and
            # apply it consistently to both selection and portfolio scores.
            score_selection = self._apply_stability_risk_modifier(
                as_of_date=as_of_date,
                instrument_id=instrument_id,
                score=score_selection,
                reasons=reasons,
            )
            stab_multiplier = float(reasons.get("stab_risk_multiplier", 1.0))
            score_portfolio = score_portfolio * stab_multiplier

            # Finally, apply any global regime risk modifier (also a
            # multiplier). Again we compute once and apply to both scores.
            score_selection = self._apply_regime_risk_modifier(as_of_date, score_selection, reasons)
            regime_multiplier = float(reasons.get("regime_risk_multiplier", 1.0))
            score_portfolio = score_portfolio * regime_multiplier

            # Nation risk modifier: penalise or exclude based on nation
            # composite risk score from the Nation Profile Engine.
            score_selection = self._apply_nation_risk_modifier(as_of_date, score_selection, reasons)
            nation_multiplier = float(reasons.get("nation_risk_multiplier", 1.0))
            score_portfolio = score_portfolio * nation_multiplier

            reasons["score_selection"] = float(score_selection)
            reasons["score_portfolio"] = float(score_portfolio)

            candidates.append(
                (p["instrument_id"], p["issuer_id"], p["sector"], score_selection, score_portfolio, reasons)
            )

        # ------------------------------------------------------------------
        # Capacity constraints and tiering
        # ------------------------------------------------------------------

        members: list[UniverseMember] = []

        if candidates:
            # Sort candidates by score (desc) then instrument_id for
            # deterministic behaviour.
            # Sort by selection score (desc) then instrument_id for deterministic behaviour.
            candidates_sorted = sorted(
                candidates,
                key=lambda c: (c[3], c[0]),
                reverse=True,
            )

            # Apply optional per-sector caps first.
            sector_caps_enabled = self.sector_max_names is not None and self.sector_max_names > 0
            sector_counts: dict[str, int] = {}
            after_sector_caps: list[
                tuple[str, str, str, float, float, dict[str, float | str | bool]]
            ] = []
            excluded_by_caps: list[UniverseMember] = []

            for instrument_id, issuer_id, sector, score_sel, score_port, reasons in candidates_sorted:
                if sector_caps_enabled:
                    current = sector_counts.get(sector, 0)
                    if current >= int(self.sector_max_names or 0):
                        # Over sector limit: mark as excluded but keep its
                        # scores for diagnostics.
                        reasons_cap = dict(reasons)
                        reasons_cap["excluded_sector_cap"] = True
                        excluded_by_caps.append(
                            UniverseMember(
                                as_of_date=as_of_date,
                                universe_id=universe_id,
                                entity_type="INSTRUMENT",
                                entity_id=instrument_id,
                                included=False,
                                # Store portfolio score on the member; selection score lives in reasons.
                                score=score_port,
                                reasons=reasons_cap,
                                tier="EXCLUDED",
                            )
                        )
                        continue

                    sector_counts[sector] = current + 1

                after_sector_caps.append((instrument_id, issuer_id, sector, score_sel, score_port, reasons))

            # Apply optional global max_universe_size on top of any sector
            # caps.
            if self.max_universe_size is not None and self.max_universe_size > 0:
                kept = after_sector_caps[: self.max_universe_size]
                overflow = after_sector_caps[self.max_universe_size :]

                for instrument_id, issuer_id, sector, score_sel, score_port, reasons in overflow:
                    reasons_cap = dict(reasons)
                    reasons_cap["excluded_max_universe_size"] = True
                    excluded_by_caps.append(
                        UniverseMember(
                            as_of_date=as_of_date,
                            universe_id=universe_id,
                            entity_type="INSTRUMENT",
                            entity_id=instrument_id,
                            included=False,
                            score=score_port,
                            reasons=reasons_cap,
                            tier="EXCLUDED",
                        )
                    )
            else:
                kept = after_sector_caps

            # Split kept candidates into CORE and SATELLITE tiers. We use a
            # simple heuristic: top 50% (at least one) by score are CORE,
            # the rest SATELLITE.
            n_kept = len(kept)
            if n_kept > 0:
                n_core = max(1, n_kept // 2)
            else:
                n_core = 0

            for idx, (instrument_id, issuer_id, sector, score_sel, score_port, reasons) in enumerate(kept):
                tier = "CORE" if idx < n_core else "SATELLITE"
                members.append(
                    UniverseMember(
                        as_of_date=as_of_date,
                        universe_id=universe_id,
                        entity_type="INSTRUMENT",
                        entity_id=instrument_id,
                        included=True,
                        # Store portfolio score (used for sizing).
                        score=score_port,
                        reasons=reasons,
                        tier=tier,
                    )
                )

            members.extend(excluded_by_caps)

        # Always include hard-fail members so diagnostics can be inspected.
        members.extend(hard_fail_members)

        return members

    def _apply_lambda_opportunity_modifier(
        self,
        as_of_date: date,
        market_id: str,
        sector: str,
        soft_target_class: str,
        score: float | None,
        reasons: dict[str, float | str | bool],
    ) -> float:
        """Optionally add a lambda-based opportunity component to score.

        If ``lambda_score_provider`` is configured and
        ``lambda_score_weight`` is non-zero, this method queries a
        per-cluster lambda score using the provided
        (as_of_date, market_id, sector, soft_target_class) tuple and
        returns either the unmodified score (if ``score`` is not None)
        plus the weighted lambda score, or just the weighted lambda
        score if ``score`` is None.

        When the provider is missing or returns None, this is a no-op
        and returns ``score`` (or 0.0 if ``score`` is None).
        """

        if self.lambda_score_provider is None or self.lambda_score_weight == 0.0:
            return 0.0 if score is None else score

        provider_fn = getattr(self.lambda_score_provider, "get_cluster_score", None)
        if provider_fn is None:  # pragma: no cover - defensive
            return 0.0 if score is None else score

        try:
            lambda_score = provider_fn(
                as_of_date=as_of_date,
                market_id=market_id,
                sector=sector,
                soft_target_class=soft_target_class,
            )
        except Exception:  # pragma: no cover - defensive
            logger.exception(
                "BasicUniverseModel._apply_lambda_opportunity_modifier: get_cluster_score failed for "
                "as_of=%s market=%s sector=%s soft_target_class=%s",
                as_of_date,
                market_id,
                sector,
                soft_target_class,
            )
            return 0.0 if score is None else score

        if lambda_score is None:
            return 0.0 if score is None else score

        lambda_score_f = float(lambda_score)
        reasons["lambda_score"] = lambda_score_f
        weight = float(self.lambda_score_weight)
        reasons["lambda_score_weight"] = weight

        # If the provider exposes experiment metadata (e.g. experiment_id
        # and score_column), surface it in the reasons for easier
        # diagnostics when multiple lambda experiments are compared.
        experiment_id = getattr(self.lambda_score_provider, "experiment_id", None)
        if experiment_id is not None:
            reasons["lambda_experiment_id"] = str(experiment_id)
        score_column = getattr(self.lambda_score_provider, "score_column", None)
        if score_column is not None:
            reasons["lambda_score_column"] = str(score_column)

        base = 0.0 if score is None else float(score)
        return base + weight * lambda_score_f

    def _apply_stability_risk_modifier(
        self,
        *,
        as_of_date: date,
        instrument_id: str,
        score: float,
        reasons: dict[str, float | str | bool],
    ) -> float:
        """Apply an optional per-instrument STAB state-change risk modifier.

        This is a thin hook around an injected
        ``stability_state_change_forecaster`` object which is expected to
        expose a ``forecast(entity_id, horizon_steps)`` method returning
        an object with a ``risk_score`` attribute in [0, 1] and,
        optionally, diagnostics like ``p_worsen_any`` and
        ``p_to_targetable_or_breaker``. The modifier is:

            score * max(0, 1 - alpha * risk_score),

        where ``alpha`` is :attr:`stability_risk_alpha`. When
        ``stability_state_change_forecaster`` is not provided or
        ``alpha`` is zero, the score is returned unchanged.
        """

        if (
            self.stability_state_change_forecaster is None
            or self.stability_risk_alpha == 0.0
        ):
            return score

        forecast_fn = getattr(self.stability_state_change_forecaster, "forecast", None)
        if forecast_fn is None:  # pragma: no cover - defensive
            return score

        try:
            risk = forecast_fn(
                entity_id=instrument_id,
                horizon_steps=self.stability_risk_horizon_steps,
                as_of_date=as_of_date,
            )
        except Exception:  # pragma: no cover - defensive
            logger.exception(
                "BasicUniverseModel._apply_stability_risk_modifier: forecast failed for instrument_id=%s",
                instrument_id,
            )
            return score

        if risk is None:
            return score

        risk_score = getattr(risk, "risk_score", None)
        if risk_score is None:
            return score

        risk_score_f = float(risk_score)
        reasons["stab_risk_score"] = risk_score_f

        p_worsen_any = getattr(risk, "p_worsen_any", None)
        if p_worsen_any is not None:
            reasons["stab_p_worsen_any"] = float(p_worsen_any)

        p_to_targetable_or_breaker = getattr(risk, "p_to_targetable_or_breaker", None)
        if p_to_targetable_or_breaker is not None:
            reasons["stab_p_to_targetable_or_breaker"] = float(p_to_targetable_or_breaker)

        alpha = float(self.stability_risk_alpha)
        reasons["stab_risk_alpha"] = alpha

        multiplier = 1.0 - alpha * risk_score_f
        if multiplier < 0.0:
            multiplier = 0.0
        reasons["stab_risk_multiplier"] = multiplier

        return score * multiplier

    def _apply_nation_risk_modifier(
        self,
        as_of_date: date,
        score: float,
        reasons: dict[str, float | str | bool],
    ) -> float:
        """Apply an optional nation risk modifier to a score.

        Reads the composite_stability for the configured nation from a
        NationScoreStorage-like provider. If composite_stability < 0.3,
        applies a 0.5× penalty. If < 0.15, zeros the score entirely.

        Otherwise applies:
            score * max(0, 1 - alpha * (1 - composite_stability))
        """

        if self.nation_score_provider is None or self.nation_risk_alpha == 0.0:
            return score

        get_latest_fn = getattr(self.nation_score_provider, "get_latest", None)
        if get_latest_fn is None:  # pragma: no cover
            return score

        try:
            nation_scores = get_latest_fn(
                self.nation_risk_nation,
                as_of_date=as_of_date,
            )
        except Exception:  # pragma: no cover
            logger.exception(
                "BasicUniverseModel._apply_nation_risk_modifier: failed for nation=%s",
                self.nation_risk_nation,
            )
            return score

        if nation_scores is None:
            return score

        composite = getattr(nation_scores, "composite_stability", None)
        if composite is None:
            return score

        composite_f = float(composite)
        reasons["nation_composite_risk"] = composite_f
        reasons["nation_risk_alpha"] = float(self.nation_risk_alpha)

        # Hard thresholds per framework spec.
        if composite_f < 0.15:
            reasons["nation_risk_excluded"] = True
            reasons["nation_risk_multiplier"] = 0.0
            return 0.0

        if composite_f < 0.3:
            reasons["nation_risk_penalty"] = True
            reasons["nation_risk_multiplier"] = 0.5
            return score * 0.5

        # Continuous modifier: higher risk (lower composite) = lower score.
        alpha = float(self.nation_risk_alpha)
        multiplier = 1.0 - alpha * (1.0 - composite_f)
        if multiplier < 0.0:
            multiplier = 0.0
        reasons["nation_risk_multiplier"] = multiplier

        return score * multiplier

    def _apply_regime_risk_modifier(
        self,
        as_of_date: date,
        score: float,
        reasons: dict[str, float | str | bool],
    ) -> float:
        """Apply an optional global regime risk modifier to a score.

        This is a thin hook around an injected ``regime_forecaster``
        object which is expected to expose a ``forecast(region,
        horizon_steps)`` method returning an object with a
        ``risk_score`` attribute in [0, 1] and, optionally,
        ``p_change_any``. The modifier is:

            score * max(0, 1 - alpha * risk_score),

        where ``alpha`` is :attr:`regime_risk_alpha`. When
        ``regime_forecaster`` is not provided or ``alpha`` is zero, the
        score is returned unchanged.
        """

        if self.regime_forecaster is None or self.regime_risk_alpha == 0.0:
            return score

        # Duck-typed call into a RegimeStateChangeForecaster-like object.
        forecast_fn = getattr(self.regime_forecaster, "forecast", None)
        if forecast_fn is None:  # pragma: no cover - defensive
            return score

        try:
            risk = forecast_fn(
                region=self.regime_region,
                horizon_steps=self.regime_risk_horizon_steps,
                as_of_date=as_of_date,
            )
        except Exception:  # pragma: no cover - defensive
            logger.exception(
                "BasicUniverseModel._apply_regime_risk_modifier: forecast failed for region=%s",
                self.regime_region,
            )
            return score

        if risk is None:
            return score

        risk_score = getattr(risk, "risk_score", None)
        if risk_score is None:
            return score

        risk_score_f = float(risk_score)
        reasons["regime_risk_score"] = risk_score_f

        p_change_any = getattr(risk, "p_change_any", None)
        if p_change_any is not None:
            reasons["regime_p_change_any"] = float(p_change_any)

        alpha = float(self.regime_risk_alpha)
        reasons["regime_risk_alpha"] = alpha

        multiplier = 1.0 - alpha * risk_score_f
        if multiplier < 0.0:
            multiplier = 0.0
        reasons["regime_risk_multiplier"] = multiplier

        return score * multiplier
