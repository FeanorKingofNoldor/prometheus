"""Prometheus v2 – Meta-Orchestrator engine.

This module implements a minimal Meta-Orchestrator that:

* Reads backtest runs and metrics from ``backtest_runs``.
* Reconstructs :class:`SleeveConfig` objects from the stored
  ``config_json``.
* Provides simple helpers to evaluate and rank sleeves for a strategy.

More sophisticated behaviour (regime-conditioned analytics, online
learning over decision logs, etc.) can be layered on top of this v1
implementation.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

import numpy as np
from apatheon.core.logging import get_logger

from prometheus.backtest.config import SleeveConfig
from prometheus.meta.market_situation import MarketSituation
from prometheus.meta.storage import MetaStorage
from prometheus.meta.types import SleeveEvaluation

logger = get_logger(__name__)


META_CONFIG_ENV_MODEL_ID = "joint-meta-config-env-v1"


@dataclass
class MetaOrchestrator:
    """Minimal Meta-Orchestrator over backtest runs.

    The orchestrator operates purely on historical backtest results; it
    does not yet record live engine decisions or outcomes.
    """

    storage: MetaStorage

    # ------------------------------------------------------------------
    # Sleeve evaluation API
    # ------------------------------------------------------------------

    def evaluate_sleeves(self, strategy_id: str) -> List[SleeveEvaluation]:
        """Return all sleeve evaluations for a given strategy.

        This loads all backtest runs for ``strategy_id``, reconstructs
        :class:`SleeveConfig` objects from each run's ``config_json``, and
        pairs them with the corresponding metrics.
        """

        runs = self.storage.load_backtest_runs_for_strategy(strategy_id)
        evaluations: List[SleeveEvaluation] = []

        # Optionally enrich metrics with META_CONFIG_ENV_V0 context norms.
        try:
            meta_ctx_norms = self._load_meta_context_norms(
                strategy_id=strategy_id,
                model_id=META_CONFIG_ENV_MODEL_ID,
            )
        except Exception:  # pragma: no cover - defensive enrichment only
            logger.exception(
                "MetaOrchestrator.evaluate_sleeves: failed to load META_CONFIG_ENV_V0 norms for strategy_id=%s",
                strategy_id,
            )
            meta_ctx_norms = {}

        for run in runs:
            sleeve_config = self._config_from_json(run.config)
            if sleeve_config is None:
                continue

            # Copy metrics so we can safely annotate them without mutating
            # the underlying backtest_runs payload.
            metrics: Dict[str, float] = dict(run.metrics or {})
            norm = meta_ctx_norms.get(run.run_id)
            if norm is not None:
                metrics["meta_ctx_norm"] = norm

            evaluations.append(
                SleeveEvaluation(
                    run_id=run.run_id,
                    sleeve_config=sleeve_config,
                    metrics=metrics,
                )
            )

        return evaluations

    def select_top_sleeves(self, strategy_id: str, k: int) -> List[SleeveEvaluation]:
        """Select the top-k sleeves for a strategy based on metrics.

        The current implementation sorts sleeves primarily by
        ``annualised_sharpe`` (descending), then ``cumulative_return``,
        and finally by *less negative* max drawdown.
        """

        if k <= 0:
            return []

        evaluations = self.evaluate_sleeves(strategy_id)
        if not evaluations:
            return []

        sorted_evals = sorted(evaluations, key=self._sort_key, reverse=True)
        return sorted_evals[:k]

    def select_top_sleeves_for_situation(
        self,
        strategy_id: str,
        situation: MarketSituation,
        k: int,
        *,
        min_bucket_days: int = 3,
    ) -> List[SleeveEvaluation]:
        """Select the top-k sleeves conditional on a MarketSituation.

        This uses situation bucket metrics written into
        ``backtest_runs.metrics_json`` by the backtest runner (e.g.
        ``situation_bucket_CRISIS_annualised_sharpe``).

        Args:
            strategy_id: Strategy identifier to evaluate.
            situation: MarketSituation bucket to rank on.
            k: Number of sleeves to return.
            min_bucket_days: Minimum number of daily return observations
                required in the bucket for the sleeve to be considered.

        Returns:
            Top-k sleeves ordered by the situation bucket Sharpe.
        """

        if k <= 0:
            return []

        evaluations = self.evaluate_sleeves(strategy_id)
        if not evaluations:
            return []

        key_days = f"situation_bucket_{situation.value}_num_days"
        key_sharpe = f"situation_bucket_{situation.value}_annualised_sharpe"
        key_cumret = f"situation_bucket_{situation.value}_cumulative_return"

        def _f(metrics: Dict[str, Any], key: str) -> float:
            val = metrics.get(key)
            try:
                return float(val)
            except Exception:
                return 0.0

        eligible: List[SleeveEvaluation] = []
        for ev in evaluations:
            m = ev.metrics
            if not isinstance(m, dict):
                continue
            days = _f(m, key_days)
            if days < float(min_bucket_days):
                continue
            eligible.append(ev)

        if not eligible:
            return []

        def _sit_key(ev: SleeveEvaluation) -> tuple[float, float, float, float]:
            m = ev.metrics
            sit_sharpe = _f(m, key_sharpe)
            sit_cumret = _f(m, key_cumret)
            overall_sharpe = _f(m, "annualised_sharpe")
            maxdd = _f(m, "max_drawdown")
            return (sit_sharpe, sit_cumret, overall_sharpe, -maxdd)

        sorted_evals = sorted(eligible, key=_sit_key, reverse=True)
        return sorted_evals[:k]

    def select_robust_across_situations(
        self,
        strategy_id: str,
        k: int,
        *,
        min_bucket_days: int = 3,
        min_required_situations: int = 3,
    ) -> List[SleeveEvaluation]:
        """Select sleeves that are robust across MarketSituation buckets.

        Robust sleeves are those whose *worst* situation bucket Sharpe is
        as high as possible, subject to having enough observations in at
        least ``min_required_situations`` buckets.
        """

        if k <= 0:
            return []

        evaluations = self.evaluate_sleeves(strategy_id)
        if not evaluations:
            return []

        def _f(metrics: Dict[str, Any], key: str) -> float:
            val = metrics.get(key)
            try:
                return float(val)
            except Exception:
                return 0.0

        scored: List[tuple[float, int, SleeveEvaluation]] = []

        for ev in evaluations:
            m = ev.metrics
            if not isinstance(m, dict):
                continue

            sharpes: List[float] = []
            for sit in MarketSituation:
                days = _f(m, f"situation_bucket_{sit.value}_num_days")
                if days < float(min_bucket_days):
                    continue
                sharpes.append(_f(m, f"situation_bucket_{sit.value}_annualised_sharpe"))

            if len(sharpes) < int(min_required_situations):
                continue

            worst = min(sharpes)
            scored.append((worst, len(sharpes), ev))

        if not scored:
            return []

        scored_sorted = sorted(
            scored,
            key=lambda t: (t[0], t[1], _f(t[2].metrics, "annualised_sharpe")),
            reverse=True,
        )

        return [ev for _worst, _count, ev in scored_sorted[:k]]

    def select_top_sleeves_lambda_uplift(self, strategy_id: str, k: int) -> List[SleeveEvaluation]:
        """Select sleeves with the strongest lambda bucket uplift.

        This helper focuses on configurations that exhibit higher average
        daily returns in high-lambda regimes relative to low-lambda
        regimes, as captured by
        ``lambda_bucket_high_minus_low_return_diff``. It only considers
        runs where the lambda bucket metrics were computed (i.e. at least
        a few days with valid lambda exposure).
        """

        if k <= 0:
            return []

        evaluations = self.evaluate_sleeves(strategy_id)
        if not evaluations:
            return []

        eligible: List[SleeveEvaluation] = []
        for ev in evaluations:
            m = ev.metrics
            days = float(m.get("lambda_bucket_total_num_days", 0.0) or 0.0)
            diff = m.get("lambda_bucket_high_minus_low_return_diff")
            if days >= 3 and isinstance(diff, (int, float)):
                eligible.append(ev)

        if not eligible:
            return []

        def _uplift_key(ev: SleeveEvaluation) -> tuple[float, float, float]:
            m = ev.metrics
            diff = float(m.get("lambda_bucket_high_minus_low_return_diff", 0.0))
            sharpe = float(m.get("annualised_sharpe", 0.0))
            cumret = float(m.get("cumulative_return", 0.0))
            return (diff, sharpe, cumret)

        sorted_evals = sorted(eligible, key=_uplift_key, reverse=True)
        return sorted_evals[:k]

    def select_top_sleeves_lambda_robust(self, strategy_id: str, k: int) -> List[SleeveEvaluation]:
        """Select sleeves that are robust across lambda regimes.

        Robust sleeves are those whose performance does not depend
        strongly on lambda level (i.e. small absolute difference between
        high- and low-lambda bucket returns) while still delivering
        reasonable Sharpe and cumulative return.
        """

        if k <= 0:
            return []

        evaluations = self.evaluate_sleeves(strategy_id)
        if not evaluations:
            return []

        eligible: List[SleeveEvaluation] = []
        for ev in evaluations:
            m = ev.metrics
            days = float(m.get("lambda_bucket_total_num_days", 0.0) or 0.0)
            diff = m.get("lambda_bucket_high_minus_low_return_diff")
            if days >= 3 and isinstance(diff, (int, float)):
                eligible.append(ev)

        if not eligible:
            return []

        def _robust_key(ev: SleeveEvaluation) -> tuple[float, float, float]:
            m = ev.metrics
            diff = float(m.get("lambda_bucket_high_minus_low_return_diff", 0.0))
            sharpe = float(m.get("annualised_sharpe", 0.0))
            cumret = float(m.get("cumulative_return", 0.0))
            robustness = -abs(diff)
            return (robustness, sharpe, cumret)

        sorted_evals = sorted(eligible, key=_robust_key, reverse=True)
        return sorted_evals[:k]

    def select_top_sleeves_stab_scenario_exposed(
        self,
        strategy_id: str,
        k: int,
        *,
        scenario_set_id: str | None = None,
    ) -> List[SleeveEvaluation]:
        """Select sleeves that are most exposed to STAB scenarios.

        This helper looks for runs where STAB-scenario diagnostics have
        been backfilled (via ``backfill_backtest_stab_scenario_metrics``)
        and ranks sleeves by their average cosine similarity to the
        closest stress scenarios. Higher values indicate that the
        portfolio tended to sit close to, or move through, stressed
        regions of the STAB joint space.

        If ``scenario_set_id`` is provided, only runs whose
        ``stab_scenario_set_id`` matches are considered.
        """

        if k <= 0:
            return []

        evaluations = self.evaluate_sleeves(strategy_id)
        if not evaluations:
            return []

        eligible: List[SleeveEvaluation] = []
        for ev in evaluations:
            m = ev.metrics
            days = float(m.get("stab_num_days", 0.0) or 0.0)
            cos_mean = m.get("stab_closest_scenario_cosine_mean")
            scen_id = m.get("stab_scenario_set_id")
            if scenario_set_id is not None and scen_id is not None and scen_id != scenario_set_id:
                continue
            if days >= 1 and isinstance(cos_mean, (int, float)):
                eligible.append(ev)

        if not eligible:
            return []

        def _exposed_key(ev: SleeveEvaluation) -> tuple[float, float, float]:
            m = ev.metrics
            cos_mean = float(m.get("stab_closest_scenario_cosine_mean", 0.0))
            sharpe = float(m.get("annualised_sharpe", 0.0))
            cumret = float(m.get("cumulative_return", 0.0))
            return (cos_mean, sharpe, cumret)

        sorted_evals = sorted(eligible, key=_exposed_key, reverse=True)
        return sorted_evals[:k]

    def select_top_sleeves_stab_scenario_robust(
        self,
        strategy_id: str,
        k: int,
        *,
        scenario_set_id: str | None = None,
    ) -> List[SleeveEvaluation]:
        """Select sleeves that are robust to adverse STAB scenarios.

        Robust sleeves are those that rarely get extremely close to their
        stress scenarios (low maximum cosine similarity) while still
        delivering solid overall performance.
        """

        if k <= 0:
            return []

        evaluations = self.evaluate_sleeves(strategy_id)
        if not evaluations:
            return []

        eligible: List[SleeveEvaluation] = []
        for ev in evaluations:
            m = ev.metrics
            days = float(m.get("stab_num_days", 0.0) or 0.0)
            cos_max = m.get("stab_closest_scenario_cosine_max")
            scen_id = m.get("stab_scenario_set_id")
            if scenario_set_id is not None and scen_id is not None and scen_id != scenario_set_id:
                continue
            if days >= 1 and isinstance(cos_max, (int, float)):
                eligible.append(ev)

        if not eligible:
            return []

        def _stab_robust_key(ev: SleeveEvaluation) -> tuple[float, float, float]:
            m = ev.metrics
            cos_max = m.get("stab_closest_scenario_cosine_max")
            if not isinstance(cos_max, (int, float)):
                # Fall back to mean cosine if the max metric is absent.
                cos_max = m.get("stab_closest_scenario_cosine_mean", 0.0)
            cos_max_f = float(cos_max)
            sharpe = float(m.get("annualised_sharpe", 0.0))
            cumret = float(m.get("cumulative_return", 0.0))
            # Lower max cosine => more robust; we sort by (-cos_max,
            # sharpe, cumret) in descending order.
            robustness = -cos_max_f
            return (robustness, sharpe, cumret)

        sorted_evals = sorted(eligible, key=_stab_robust_key, reverse=True)
        return sorted_evals[:k]

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _load_meta_context_norms(self, strategy_id: str, model_id: str) -> Dict[str, float]:
        """Return META_CONFIG_ENV_V0 L2 norms keyed by run_id for a strategy.

        If the joint embeddings table or requested model_id is missing, an
        empty mapping is returned.
        """

        db_manager = self.storage.db_manager

        sql = """
            SELECT (entity_scope->>'run_id') AS run_id,
                   vector
            FROM joint_embeddings
            WHERE joint_type = 'META_CONFIG_ENV_V0'
              AND model_id = %s
              AND (entity_scope->>'strategy_id') = %s
        """

        norms: Dict[str, float] = {}
        with db_manager.get_historical_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (model_id, strategy_id))
                rows = cursor.fetchall()
            finally:
                cursor.close()

        for run_id_db, vector_bytes in rows:
            if vector_bytes is None:
                continue
            vec = np.frombuffer(vector_bytes, dtype=np.float32)
            if vec.size == 0:
                continue
            norms[str(run_id_db)] = float(np.linalg.norm(vec))

        return norms

    @staticmethod
    def _sort_key(ev: SleeveEvaluation) -> tuple[float, float, float]:
        m = ev.metrics
        sharpe = float(m.get("annualised_sharpe", 0.0))
        cumret = float(m.get("cumulative_return", 0.0))
        maxdd = float(m.get("max_drawdown", 0.0))
        # Higher Sharpe and cumulative return are better; less negative
        # drawdown is better (hence the minus sign in the key tuple).
        return (sharpe, cumret, -maxdd)

    def _config_from_json(self, cfg: Dict[str, Any]) -> SleeveConfig | None:
        """Reconstruct a :class:`SleeveConfig` from ``config_json``.

        The :class:`BacktestRunner` stores a superset of sleeve
        configuration fields in ``config_json``. Here we extract just the
        keys required by :class:`SleeveConfig`.
        """

        required_keys = {
            "sleeve_id",
            "strategy_id",
            "market_id",
            "universe_id",
            "portfolio_id",
            "assessment_strategy_id",
            "assessment_horizon_days",
        }

        data: Dict[str, Any] = {k: cfg[k] for k in required_keys if k in cfg}
        missing = required_keys - set(data.keys())
        if missing:
            logger.warning(
                "MetaOrchestrator._config_from_json: missing keys %s in config_json; skipping run",
                ",".join(sorted(missing)),
            )
            return None

        try:
            return SleeveConfig(**data)
        except Exception:
            logger.exception(
                "MetaOrchestrator._config_from_json: failed to parse SleeveConfig from %s",
                data,
            )
            return None
