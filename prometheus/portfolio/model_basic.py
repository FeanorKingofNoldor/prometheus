"""Prometheus v2 – Basic long-only portfolio model.

This module implements a simple long-only portfolio construction model
for equity books using UniverseEngine outputs. It is not a full
mean-variance optimiser; instead it:

- Normalises universe ranking scores into weights.
- Applies per-name max-weight caps from :class:`PortfolioConfig`.
- Computes simple sector and fragility exposure diagnostics.

The goal is to provide a deterministic, inspectable baseline that can be
replaced with a more sophisticated optimisation model later.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Callable, Dict, List

from apathis.core.database import get_db_manager
from apathis.core.logging import get_logger
from apathis.fragility.storage import FragilityStorage
from apathis.fragility.types import FragilityClass

from prometheus.portfolio.scenario_risk import compute_portfolio_scenario_pnl
from prometheus.universe.engine import UniverseMember, UniverseStorage

from .config import PortfolioConfig
from .types import RiskReport, TargetPortfolio

logger = get_logger(__name__)


# ── Conviction scaling ───────────────────────────────────────────────

def scale_weight_by_conviction(
    base_weight: float,
    conviction: float,
    min_scale: float = 0.5,
    max_scale: float = 1.5,
) -> float:
    """Scale a target position weight by its conviction score.

    Linear interpolation: conviction 0 -> min_scale, conviction 1 -> max_scale.
    Conviction 0.5 (neutral) -> 1.0x (unchanged).
    """
    scale = min_scale + (max_scale - min_scale) * conviction
    return base_weight * scale


def _load_assessment_confidences(
    instrument_ids: list[str],
    as_of_date: date,
) -> Dict[str, float]:
    """Load latest assessment confidence scores for instruments.

    Queries the ``instrument_scores`` table for the most recent confidence
    value per instrument on or before ``as_of_date``.  Returns a mapping
    from instrument_id to confidence (0-1).
    """
    if not instrument_ids:
        return {}

    try:
        db_manager = get_db_manager()
    except Exception:
        logger.debug("_load_assessment_confidences: failed to get db_manager", exc_info=True)
        return {}

    sql = """
        SELECT DISTINCT ON (instrument_id)
            instrument_id, confidence
        FROM instrument_scores
        WHERE instrument_id = ANY(%s)
          AND as_of_date <= %s
        ORDER BY instrument_id, as_of_date DESC, created_at DESC
    """

    try:
        with db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (instrument_ids, as_of_date))
                rows = cursor.fetchall()
            finally:
                cursor.close()
    except Exception:
        logger.debug(
            "_load_assessment_confidences: failed to query instrument_scores; "
            "conviction scaling will be skipped",
        )
        return {}

    return {str(row[0]): float(row[1]) for row in rows if row[1] is not None}


@dataclass
class BasicLongOnlyPortfolioModel:
    """Basic long-only portfolio model built from universe members.

    This model assumes a single equity universe per region and constructs
    weights by normalising :class:`UniverseMember.score` values, subject
    to a per-instrument max-weight cap.
    """

    universe_storage: UniverseStorage
    config: PortfolioConfig
    universe_id: str

    # Optional provider for current holdings (instrument_ids). When set,
    # the model can apply turnover-reduction heuristics such as rank-buffer
    # hysteresis when max_names is configured.
    held_ids_provider: Callable[[date], set[str]] | None = None

    # Internal cache of the last set of universe members used for
    # optimisation. PortfolioEngine relies on this to persist weights via
    # PortfolioStorage without re-querying the universe.
    _last_members: List[UniverseMember] = field(default_factory=list, init=False)

    def _load_members(self, as_of_date: date) -> List[UniverseMember]:
        members = self.universe_storage.get_universe(
            as_of_date=as_of_date,
            universe_id=self.universe_id,
            entity_type="INSTRUMENT",
            included_only=True,
        )
        self._last_members = members
        return members

    def build_target_portfolio(self, portfolio_id: str, as_of_date: date) -> TargetPortfolio:  # type: ignore[override]
        members = self._load_members(as_of_date)
        if not members:
            logger.info(
                "BasicLongOnlyPortfolioModel: no universe members for %s on %s",
                portfolio_id,
                as_of_date,
            )
            return TargetPortfolio(
                portfolio_id=portfolio_id,
                as_of_date=as_of_date,
                weights={},
                expected_return=0.0,
                expected_volatility=0.0,
                risk_metrics={},
                factor_exposures={},
                constraints_status={},
                metadata={"risk_model_id": self.config.risk_model_id},
            )

        # Optional portfolio-stage top-K culling. This is intentionally
        # separate from universe_max_size so callers can keep a wide
        # eligibility universe (for diagnostics) while trading a small
        # number of names.
        max_names_raw = getattr(self.config, "max_names", None)
        max_names: int | None
        try:
            max_names = int(max_names_raw) if max_names_raw is not None else None
        except (TypeError, ValueError):
            max_names = None

        # Optional turnover control: rank buffer (top-K hysteresis).
        buf_raw = getattr(self.config, "hysteresis_buffer", None)
        try:
            hysteresis_buffer = int(buf_raw) if buf_raw is not None and int(buf_raw) > 0 else 0
        except (TypeError, ValueError):
            hysteresis_buffer = 0

        max_names_binding = False
        hysteresis_active = False

        if max_names is not None and max_names > 0 and len(members) > max_names:
            max_names_binding = True

            # Deterministic ranking by score desc, then entity_id desc.
            ranked = sorted(members, key=lambda m: (float(m.score), str(m.entity_id)), reverse=True)

            held_ids: set[str] = set()
            if hysteresis_buffer > 0 and self.held_ids_provider is not None:
                try:
                    held_ids = set(self.held_ids_provider(as_of_date) or [])
                except Exception:  # pragma: no cover - defensive
                    logger.exception(
                        "BasicLongOnlyPortfolioModel: held_ids_provider failed for %s on %s; ignoring hysteresis",
                        portfolio_id,
                        as_of_date,
                    )
                    held_ids = set()

            if hysteresis_buffer > 0 and held_ids:
                # Keep held names as long as they remain within rank
                # (max_names + hysteresis_buffer). This reduces churn but
                # may delay entry of new names near the cutoff.
                rank_by_id: dict[str, int] = {
                    str(m.entity_id): idx for idx, m in enumerate(ranked, start=1)
                }
                keep_rank = max_names + hysteresis_buffer

                kept_ids = [
                    inst_id
                    for inst_id in held_ids
                    if inst_id in rank_by_id and rank_by_id[inst_id] <= keep_rank
                ]
                kept_ids.sort(key=lambda inst_id: rank_by_id[inst_id])
                if len(kept_ids) > max_names:
                    kept_ids = kept_ids[:max_names]

                selected_ids: list[str] = list(kept_ids)
                selected_set = set(selected_ids)
                for m in ranked:
                    if len(selected_ids) >= max_names:
                        break
                    mid = str(m.entity_id)
                    if mid in selected_set:
                        continue
                    selected_ids.append(mid)
                    selected_set.add(mid)

                members = [m for m in ranked if str(m.entity_id) in selected_set]
                hysteresis_active = True
            else:
                members = ranked[:max_names]

            # Ensure persistence only writes targets for the post-cull members.
            self._last_members = members

        # Base weights from non-negative scores with optional concentration.
        power = max(0.1, float(getattr(self.config, "score_concentration_power", 1.0)))
        raw_scores = [max(0.0, m.score) for m in members]
        if power != 1.0:
            raw_scores = [s ** power for s in raw_scores]
        total_score = sum(raw_scores)
        if total_score <= 0.0:
            n = len(members)
            base_weights = [1.0 / n for _ in members]
        else:
            base_weights = [s / total_score for s in raw_scores]

        # Apply per-name max-weight cap.
        w_max = max(0.0, float(self.config.per_instrument_max_weight))
        redistribute = bool(getattr(self.config, "redistribute_capped_residual", True))
        any_clipped = False

        if w_max <= 0.0 or w_max >= 1.0:
            final_weights = base_weights
        else:
            n = len(base_weights)
            final_weights = [0.0 for _ in base_weights]

            remaining_idx = set(range(n))
            remaining_mass = 1.0
            eps = 1e-12

            while remaining_idx and remaining_mass > eps:
                remaining_base_sum = sum(base_weights[i] for i in remaining_idx)
                if remaining_base_sum <= eps:
                    break

                to_cap: list[int] = []
                for i in remaining_idx:
                    w_i = base_weights[i] / remaining_base_sum * remaining_mass
                    if w_i > w_max + 1e-9:
                        to_cap.append(i)

                if not to_cap:
                    for i in remaining_idx:
                        final_weights[i] = base_weights[i] / remaining_base_sum * remaining_mass
                    remaining_mass = 0.0
                    break

                any_clipped = True
                for i in to_cap:
                    final_weights[i] = w_max
                    remaining_mass -= w_max
                    remaining_idx.remove(i)

                if remaining_mass <= eps:
                    remaining_mass = 0.0
                    break

            total_final = sum(final_weights)
            if total_final <= eps:
                n = len(members)
                eq = 1.0 / n
                if eq > w_max + 1e-9:
                    final_weights = [w_max for _ in members]
                else:
                    final_weights = [eq for _ in members]

            # ── Redistribute residual to eliminate cash drag ──────────
            # After capping, weights may sum to < 1.0.  Redistribute the
            # shortfall proportionally to uncapped names so the portfolio
            # is fully invested.
            if redistribute:
                total_final = sum(final_weights)
                residual = 1.0 - total_final
                if residual > eps:
                    uncapped_idx = [
                        i for i in range(len(final_weights))
                        if final_weights[i] < w_max - 1e-9 and final_weights[i] > 0
                    ]
                    # Iteratively redistribute until all weight is allocated
                    # or every name hits the cap.
                    for _ in range(20):  # Safety bound
                        if residual <= eps or not uncapped_idx:
                            break
                        uncapped_sum = sum(final_weights[i] for i in uncapped_idx)
                        if uncapped_sum <= eps:
                            break
                        still_uncapped = []
                        distributed = 0.0
                        for i in uncapped_idx:
                            share = (final_weights[i] / uncapped_sum) * residual
                            new_w = final_weights[i] + share
                            if new_w > w_max:
                                distributed += w_max - final_weights[i]
                                final_weights[i] = w_max
                            else:
                                distributed += share
                                final_weights[i] = new_w
                                still_uncapped.append(i)
                        residual -= distributed
                        uncapped_idx = still_uncapped

                    redistributed = 1.0 - total_final - residual
                    if redistributed > eps:
                        logger.debug(
                            "BasicLongOnlyPortfolioModel: redistributed %.2f%% residual to %d uncapped names",
                            redistributed * 100,
                            len([i for i in range(len(final_weights)) if final_weights[i] < w_max - 1e-9]),
                        )

        weights: Dict[str, float] = {
            m.entity_id: float(w) for m, w in zip(members, final_weights)
        }

        # ── Conviction scaling: adjust weights by assessment confidence ──
        conviction_scaling_applied = False
        conviction_scaling_enabled = getattr(self.config, "conviction_scaling_enabled", False)
        if conviction_scaling_enabled:
            min_scale = float(getattr(self.config, "conviction_scaling_min", 0.5))
            max_scale = float(getattr(self.config, "conviction_scaling_max", 1.5))
            confidences = _load_assessment_confidences(
                list(weights.keys()), as_of_date,
            )
            if confidences:
                scaled_weights: Dict[str, float] = {}
                for inst_id, w in weights.items():
                    conf = confidences.get(inst_id)
                    if conf is not None:
                        new_w = scale_weight_by_conviction(w, conf, min_scale, max_scale)
                        scale_applied = new_w / w if w > 0 else 1.0
                        if abs(scale_applied - 1.0) > 0.20:
                            logger.info(
                                "ConvictionScaling: %s weight %.4f -> %.4f "
                                "(conviction=%.2f, scale=%.2fx)",
                                inst_id, w, new_w, conf, scale_applied,
                            )
                        scaled_weights[inst_id] = new_w
                    else:
                        scaled_weights[inst_id] = w

                # Re-normalise so total weight sums to original gross exposure.
                original_total = sum(weights.values())
                scaled_total = sum(scaled_weights.values())
                if scaled_total > 0 and original_total > 0:
                    norm_factor = original_total / scaled_total
                    weights = {k: v * norm_factor for k, v in scaled_weights.items()}
                else:
                    weights = scaled_weights

                conviction_scaling_applied = True
                logger.info(
                    "ConvictionScaling: applied to %d/%d instruments (min_scale=%.2f, max_scale=%.2f)",
                    len(confidences), len(weights), min_scale, max_scale,
                )

                # Update final_weights list for downstream diagnostics.
                member_id_to_idx = {m.entity_id: i for i, m in enumerate(members)}
                for inst_id, w in weights.items():
                    idx = member_id_to_idx.get(inst_id)
                    if idx is not None:
                        final_weights[idx] = w

        # Diagnostics: sector and fragility exposures.
        sector_exposures: Dict[str, float] = {}
        fragile_weight = 0.0
        total_weight = 0.0
        for m, w in zip(members, final_weights):
            sector = str(m.reasons.get("sector", "UNKNOWN"))
            sector_exposures[sector] = sector_exposures.get(sector, 0.0) + float(w)

            soft_class = str(m.reasons.get("soft_target_class", ""))
            weak_profile = bool(m.reasons.get("weak_profile", False))
            is_fragile = soft_class in {"FRAGILE", "TARGETABLE", "BREAKER"} or weak_profile
            if is_fragile:
                fragile_weight += float(w)
            total_weight += float(w)

        gross_exposure = sum(abs(w) for w in final_weights)
        net_exposure = sum(final_weights)
        cash_weight = max(0.0, 1.0 - net_exposure)

        frag_limit = self.config.fragility_exposure_limit
        constraints_status = {
            "portfolio_max_names_binding": bool(max_names_binding),
            "portfolio_hysteresis_active": bool(hysteresis_active),
            "per_instrument_max_weight_binding": any_clipped,
            "fragility_exposure_within_limit": fragile_weight <= frag_limit,
            "conviction_scaling_applied": conviction_scaling_applied,
        }

        risk_metrics = {
            "gross_exposure": gross_exposure,
            "net_exposure": net_exposure,
            "cash_weight": cash_weight,
            "fragility_exposure": fragile_weight,
            "num_names": float(len(members)),
        }

        # Compute simple factor-based risk metrics using historical factor
        # exposures and returns. If factor data is unavailable, this
        # gracefully falls back to zero volatility and no factor
        # exposures.
        factor_exposures: Dict[str, float] = {}
        expected_volatility: float = 0.0
        risk_window_days: int = 0

        try:
            factor_risk_result = self._compute_factor_risk(
                as_of_date=as_of_date,
                members=members,
                weights_vector=final_weights,
            )
            if factor_risk_result is None:
                # _compute_factor_risk signalled insufficient data — use placeholders.
                factor_exposures = {}
                expected_volatility = 0.0
                risk_window_days = 0
            else:
                factor_exposures, expected_volatility, risk_window_days = factor_risk_result
        except Exception:  # pragma: no cover - defensive
            # Factor-risk computation is best-effort; failures should not
            # break portfolio construction.
            logger.exception(
                "BasicLongOnlyPortfolioModel: error computing factor-based risk; "
                "continuing with placeholder risk metrics",
            )
            factor_exposures = {}
            expected_volatility = 0.0
            risk_window_days = 0

        if risk_window_days > 0:
            risk_metrics["risk_window_days"] = float(risk_window_days)
        risk_metrics["expected_volatility"] = float(expected_volatility)

        # For expected return we continue to treat the universe scores as a
        # proxy, independent of the risk model used.
        expected_return = float(sum(w * s for w, s in zip(final_weights, raw_scores)))

        # If we could not compute factor exposures, fall back to
        # sector-based exposures so callers still see a breakdown.
        effective_exposures = factor_exposures or sector_exposures

        return TargetPortfolio(
            portfolio_id=portfolio_id,
            as_of_date=as_of_date,
            weights=weights,
            expected_return=expected_return,
            expected_volatility=expected_volatility,
            risk_metrics=risk_metrics,
            factor_exposures=effective_exposures,
            constraints_status=constraints_status,
            metadata={
                "risk_model_id": self.config.risk_model_id,
                "portfolio_max_names": int(max_names or 0),
                "portfolio_hysteresis_buffer": int(hysteresis_buffer or 0),
            },
        )

    def build_risk_report(
        self,
        portfolio_id: str,
        as_of_date: date,
        target: TargetPortfolio | None = None,
    ) -> RiskReport | None:  # type: ignore[override]
        """Return a basic risk report derived from the target portfolio.

        For this iteration the risk report mirrors the risk_metrics and
        factor_exposures contained in the :class:`TargetPortfolio`.
        """

        if target is None:
            target = self.build_target_portfolio(portfolio_id, as_of_date)

        exposures: Dict[str, float] = {}
        exposures.update(target.factor_exposures)

        # Augment scalar risk metrics with fragility-based aggregates
        # derived from the latest ``fragility_measures`` per instrument.
        risk_metrics = dict(target.risk_metrics)
        frag_metrics, frag_weight_by_class = self._compute_fragility_metrics(
            as_of_date=as_of_date,
            weights=target.weights,
        )
        risk_metrics.update(frag_metrics)

        metadata: Dict[str, object] = {"risk_model_id": self.config.risk_model_id}
        if frag_weight_by_class:
            metadata["fragility_weight_by_class"] = frag_weight_by_class

        # Optionally compute scenario-based P&L for configured scenario
        # sets. This is deliberately conservative: if anything fails, the
        # rest of the risk report remains intact.
        scenario_pnl: Dict[str, float] = {}
        db_manager = getattr(self.universe_storage, "db_manager", None)
        if db_manager is not None and self.config.scenario_risk_scenario_set_ids:
            for scenario_set_id in self.config.scenario_risk_scenario_set_ids:
                try:
                    result = compute_portfolio_scenario_pnl(
                        db_manager=db_manager,
                        scenario_set_id=scenario_set_id,
                        as_of_date=as_of_date,
                        weights=target.weights,
                    )
                except Exception:  # pragma: no cover - defensive
                    logger.exception(
                        "BasicLongOnlyPortfolioModel.build_risk_report: scenario risk computation "
                        "failed for portfolio_id=%s scenario_set_id=%s as_of=%s",
                        portfolio_id,
                        scenario_set_id,
                        as_of_date,
                    )
                    continue

                scenario_pnl.update(result.scenario_pnl)
                for key, value in result.summary_metrics.items():
                    metric_key = f"{scenario_set_id}:{key}"
                    risk_metrics[metric_key] = float(value)

        return RiskReport(
            portfolio_id=portfolio_id,
            as_of_date=as_of_date,
            exposures=exposures,
            risk_metrics=risk_metrics,
            scenario_pnl=scenario_pnl,
            metadata=metadata,
        )

    # ------------------------------------------------------------------
    # Internal helpers – factor-based risk and fragility
    # ------------------------------------------------------------------

    def _compute_fragility_metrics(
        self,
        as_of_date: date,
        weights: Dict[str, float],
    ) -> tuple[Dict[str, float], Dict[str, float]]:
        """Compute portfolio-level fragility aggregates.

        Returns a tuple ``(metrics, weight_by_class)`` where ``metrics``
        contains scalar values that can be stored in
        ``portfolio_risk_reports.risk_metrics`` and ``weight_by_class`` is
        a mapping from :class:`FragilityClass` value to aggregate absolute
        weight used for metadata/monitoring.
        """

        if not weights:
            return {}, {}

        # Use the same DatabaseManager backing the universe storage to
        # avoid creating a separate connection manager here.
        db_manager = getattr(self.universe_storage, "db_manager", None)
        if db_manager is None:
            return {}, {}

        storage = FragilityStorage(db_manager=db_manager)
        instrument_ids = [inst_id for inst_id, w in weights.items() if float(w) != 0.0]
        measures = storage.get_latest_measures_for_entities("INSTRUMENT", instrument_ids)
        if not measures:
            return {}, {}

        total_abs_weight = 0.0
        frag_weight_total = 0.0
        frag_weight_by_class: Dict[str, float] = {}
        score_weighted_abs = 0.0
        score_max = 0.0
        num_with_measure = 0

        for inst_id, w in weights.items():
            measure = measures.get(inst_id)
            if measure is None:
                continue
            abs_w = abs(float(w))
            if abs_w <= 0.0:
                continue

            total_abs_weight += abs_w
            num_with_measure += 1

            score = float(measure.fragility_score)
            score_weighted_abs += score * abs_w
            if score > score_max:
                score_max = score

            if measure.class_label is not FragilityClass.NONE:
                frag_weight_total += abs_w

            cls_key = measure.class_label.value
            frag_weight_by_class[cls_key] = frag_weight_by_class.get(cls_key, 0.0) + abs_w

        if total_abs_weight <= 0.0:
            return {}, frag_weight_by_class

        metrics: Dict[str, float] = {}
        metrics["fragility_weight_total"] = frag_weight_total
        metrics["fragility_weight_fraction"] = frag_weight_total / total_abs_weight
        metrics["fragility_score_weighted_mean"] = score_weighted_abs / total_abs_weight
        metrics["fragility_score_max"] = score_max
        metrics["fragility_num_names_with_measure"] = float(num_with_measure)

        return metrics, frag_weight_by_class

    def _compute_factor_risk(
        self,
        as_of_date: date,
        members: List[UniverseMember],
        weights_vector: List[float],
    ) -> tuple[Dict[str, float], float, int]:
        """Compute simple factor-based exposures and portfolio volatility.

        The implementation uses ``instrument_factors_daily`` for
        per-instrument factor exposures and ``factors_daily`` for factor
        returns, both in the historical database. Correlations between
        factors are approximated as zero, yielding::

            sigma_portfolio = sqrt(sum_f (E_f * sigma_f) ** 2)

        where ``E_f`` is the portfolio exposure to factor ``f`` and
        ``sigma_f`` is the realised volatility of that factor over a
        window determined by any correlation panel that covers
        ``as_of_date`` (or a 63-day fallback window).

        The function is deliberately defensive: if any step fails or
        there is insufficient data, it returns empty exposures and
        zero volatility.
        """

        if not members or not weights_vector:
            return {}, 0.0, 0

        # Map instrument_id -> weight for instruments with non-zero weight.
        weights_by_instrument: Dict[str, float] = {}
        for m, w in zip(members, weights_vector):
            w_f = float(w)
            if abs(w_f) > 0.0:
                weights_by_instrument[m.entity_id] = w_f

        if not weights_by_instrument:
            return {}, 0.0, 0

        try:
            db_manager = get_db_manager()
        except Exception:  # pragma: no cover - defensive
            logger.warning(
                "BasicLongOnlyPortfolioModel._compute_factor_risk: failed to initialise DatabaseManager; "
                "skipping factor risk computation",
            )
            return {}, 0.0, 0

        # Query factor exposures and returns from the historical DB. Any
        # errors here should cause a graceful fallback.
        try:
            with db_manager.get_historical_connection() as conn:  # type: ignore[attr-defined]
                cursor = conn.cursor()
                try:
                    # 1) Load per-instrument factor exposures for the date.
                    sql_exposures = """
                        SELECT instrument_id, factor_id, exposure
                        FROM instrument_factors_daily
                        WHERE trade_date = %s
                          AND instrument_id = ANY(%s)
                    """
                    cursor.execute(sql_exposures, (as_of_date, list(weights_by_instrument.keys())))
                    rows = cursor.fetchall()

                    if not rows:
                        return {}, 0.0, 0

                    # Aggregate portfolio factor exposures E_f.
                    factor_exposures: Dict[str, float] = {}
                    factor_ids: set[str] = set()
                    for instrument_id, factor_id, exposure in rows:
                        w = weights_by_instrument.get(instrument_id)
                        if w is None or w == 0.0:
                            continue
                        f_id = str(factor_id)
                        factor_ids.add(f_id)
                        factor_exposures[f_id] = factor_exposures.get(f_id, 0.0) + float(w) * float(exposure)

                    if not factor_exposures:
                        return {}, 0.0, 0

                    # 2) Determine risk window from correlation_panels, if
                    # available, otherwise fall back to a 63-day calendar
                    # window ending at as_of_date.
                    sql_panel = """
                        SELECT panel_id, start_date, end_date
                        FROM correlation_panels
                        WHERE start_date <= %s
                          AND end_date >= %s
                        ORDER BY (end_date - start_date) ASC
                        LIMIT 1
                    """
                    cursor.execute(sql_panel, (as_of_date, as_of_date))
                    panel_row = cursor.fetchone()

                    if panel_row:
                        _panel_id, start_date, end_date = panel_row
                        # Ensure the window is not empty and is bounded by
                        # as_of_date on the upper side.
                        if end_date > as_of_date:
                            end_date = as_of_date
                        if start_date >= end_date:
                            start_date = as_of_date - timedelta(days=63)
                    else:
                        start_date = as_of_date - timedelta(days=63)
                        end_date = as_of_date

                    # 3) Load factor returns over the chosen window.
                    sql_factors = """
                        SELECT factor_id, trade_date, value
                        FROM factors_daily
                        WHERE trade_date BETWEEN %s AND %s
                          AND factor_id = ANY(%s)
                    """
                    cursor.execute(sql_factors, (start_date, end_date, list(factor_ids)))
                    factor_rows = cursor.fetchall()
                finally:
                    cursor.close()
        except Exception:  # pragma: no cover - defensive
            logger.exception(
                "BasicLongOnlyPortfolioModel._compute_factor_risk: error loading factor data; "
                "skipping factor risk computation",
            )
            return {}, 0.0, 0

        if not factor_rows:
            return {}, 0.0, 0

        # Group factor returns by factor_id and compute realised
        # volatility sigma_f for each.
        returns_by_factor: Dict[str, list[float]] = {}
        for factor_id, _trade_date, value in factor_rows:
            f_id = str(factor_id)
            returns_by_factor.setdefault(f_id, []).append(float(value))

        sigma_by_factor: Dict[str, float] = {}
        for f_id, values in returns_by_factor.items():
            n = len(values)
            if n < 2:
                continue
            mean_val = sum(values) / n
            var = sum((v - mean_val) ** 2 for v in values) / (n - 1)
            sigma = math.sqrt(var) if var > 0.0 else 0.0
            sigma_by_factor[f_id] = sigma

        if not sigma_by_factor:
            return {}, 0.0, 0

        # Portfolio variance under diagonal factor covariance
        # approximation.
        variance = 0.0
        for f_id, exposure in factor_exposures.items():
            sigma = sigma_by_factor.get(f_id)
            if sigma is None or sigma <= 0.0:
                continue
            contribution = exposure * sigma
            variance += contribution * contribution

        if variance <= 0.0:
            logger.warning("Factor risk computation failed: variance=%s", variance)
            return None  # Signal failure to caller

        window_days = (end_date - start_date).days + 1
        volatility = math.sqrt(variance)

        return factor_exposures, volatility, max(window_days, 0)
