"""Prometheus v2 – Meta/Iris Diagnostics Engine.

This module analyzes backtest results to identify performance patterns,
strengths, and weaknesses across different regimes, strategies, and market
conditions. The diagnostics form the foundation for generating configuration
improvement proposals.

Key responsibilities:
- Compute performance metrics by regime/strategy/market
- Identify underperforming configurations
- Analyze risk-adjusted returns and drawdown patterns
- Compare configurations to find winners/losers
- Generate actionable diagnostic insights

Author: Prometheus Team
Created: 2025-12-02
Status: Development
Version: v0.1.0
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Dict, List

import numpy as np
from apathis.core.database import DatabaseManager
from apathis.core.logging import get_logger

from prometheus.meta.types import BacktestRunRecord

logger = get_logger(__name__)


@dataclass(frozen=True)
class PerformanceStats:
    """Performance statistics for a backtest run or group of runs.

    Attributes:
        sharpe: Annualized Sharpe ratio
        return_: Cumulative return
        volatility: Annualized volatility
        max_drawdown: Maximum drawdown (negative value)
        win_rate: Fraction of positive daily returns
        sample_size: Number of observations
        run_ids: List of run IDs included in this statistic
    """

    sharpe: float
    return_: float
    volatility: float
    max_drawdown: float
    win_rate: float
    sample_size: int
    run_ids: List[str]


@dataclass(frozen=True)
class RegimePerformance:
    """Performance breakdown by regime.

    Attributes:
        regime_id: Regime identifier (or 'ALL' for aggregate)
        stats: Performance statistics
        relative_sharpe: Sharpe relative to overall average (0.0 = average)
    """

    regime_id: str
    stats: PerformanceStats
    relative_sharpe: float


@dataclass(frozen=True)
class ConfigComparison:
    """Comparison between two configurations.

    Attributes:
        config_key: Configuration parameter being compared
        baseline_value: Baseline configuration value
        alternative_value: Alternative configuration value
        sharpe_delta: Sharpe improvement (alternative - baseline)
        return_delta: Return improvement
        risk_delta: Volatility change
        sample_count: Number of runs in comparison
    """

    config_key: str
    baseline_value: any
    alternative_value: any
    sharpe_delta: float
    return_delta: float
    risk_delta: float
    sample_count: int


@dataclass(frozen=True)
class DiagnosticReport:
    """Complete diagnostic analysis for a strategy.

    Attributes:
        strategy_id: Strategy being analyzed
        overall_performance: Aggregate performance across all runs
        regime_breakdown: Performance by regime
        config_comparisons: Pairwise config comparisons showing improvements
        underperforming_configs: Configurations below threshold
        high_risk_configs: Configurations with excessive volatility or drawdown
        sample_metadata: Additional metadata about the analysis
    """

    strategy_id: str
    overall_performance: PerformanceStats
    regime_breakdown: List[RegimePerformance]
    config_comparisons: List[ConfigComparison]
    underperforming_configs: List[Dict[str, any]]
    high_risk_configs: List[Dict[str, any]]
    sample_metadata: Dict[str, any]


@dataclass
class DiagnosticsEngine:
    """Analyzes backtest performance to identify optimization opportunities.

    The engine loads backtest results from the database, computes performance
    statistics across different dimensions (regime, strategy, config parameters),
    and generates actionable diagnostic insights.
    """

    db_manager: DatabaseManager
    min_sharpe_threshold: float = 0.5
    max_drawdown_threshold: float = -0.20
    max_volatility_threshold: float = 0.30

    def analyze_strategy(
        self, strategy_id: str, min_sample_size: int = 5
    ) -> DiagnosticReport:
        """Generate a complete diagnostic report for a strategy.

        Args:
            strategy_id: Strategy identifier
            min_sample_size: Minimum number of runs required for analysis

        Returns:
            Complete diagnostic report with performance breakdown and recommendations

        Raises:
            ValueError: If insufficient data available
        """
        runs = self._load_backtest_runs(strategy_id)

        if len(runs) < min_sample_size:
            raise ValueError(
                f"Insufficient data: {len(runs)} runs available, need {min_sample_size}"
            )

        logger.info(
            f"DiagnosticsEngine: analyzing strategy={strategy_id} with {len(runs)} runs"
        )

        overall_perf = self._compute_overall_performance(runs)
        regime_breakdown = self._analyze_by_regime(runs, overall_perf.sharpe)
        config_comparisons = self._compare_configurations(runs)
        underperforming = self._identify_underperforming_configs(runs)
        high_risk = self._identify_high_risk_configs(runs)

        return DiagnosticReport(
            strategy_id=strategy_id,
            overall_performance=overall_perf,
            regime_breakdown=regime_breakdown,
            config_comparisons=config_comparisons,
            underperforming_configs=underperforming,
            high_risk_configs=high_risk,
            sample_metadata={
                "total_runs": len(runs),
                "analysis_timestamp": date.today().isoformat(),
            },
        )

    def _load_backtest_runs(self, strategy_id: str) -> List[BacktestRunRecord]:
        """Load all backtest runs for a strategy."""
        sql = """
            SELECT run_id, strategy_id, universe_id, config_json, metrics_json
            FROM backtest_runs
            WHERE strategy_id = %s
              AND metrics_json IS NOT NULL
            ORDER BY created_at DESC
        """

        with self.db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (strategy_id,))
                rows = cursor.fetchall()
            finally:
                cursor.close()

        runs = []
        for run_id, strat_id, universe_id, config_json, metrics_json in rows:
            runs.append(
                BacktestRunRecord(
                    run_id=str(run_id),
                    strategy_id=str(strat_id),
                    universe_id=str(universe_id) if universe_id else None,
                    config=config_json or {},
                    metrics=metrics_json or {},
                )
            )

        logger.info(f"Loaded {len(runs)} backtest runs for strategy={strategy_id}")
        return runs

    def _compute_overall_performance(
        self, runs: List[BacktestRunRecord]
    ) -> PerformanceStats:
        """Compute aggregate performance statistics."""
        sharpes = []
        returns = []
        vols = []
        drawdowns = []
        win_rates = []

        for run in runs:
            metrics = run.metrics
            if metrics:
                sharpes.append(metrics.get("annualised_sharpe", 0.0))
                returns.append(metrics.get("cumulative_return", 0.0))
                vols.append(metrics.get("annualised_vol", 0.0))
                drawdowns.append(metrics.get("max_drawdown", 0.0))
                # Win rate not typically in metrics; compute from daily data if needed
                win_rates.append(0.5)  # Placeholder

        return PerformanceStats(
            sharpe=float(np.mean(sharpes)) if sharpes else 0.0,
            return_=float(np.mean(returns)) if returns else 0.0,
            volatility=float(np.mean(vols)) if vols else 0.0,
            max_drawdown=float(np.min(drawdowns)) if drawdowns else 0.0,
            win_rate=float(np.mean(win_rates)) if win_rates else 0.0,
            sample_size=len(runs),
            run_ids=[r.run_id for r in runs],
        )

    def _analyze_by_regime(
        self, runs: List[BacktestRunRecord], baseline_sharpe: float
    ) -> List[RegimePerformance]:
        """Analyze performance breakdown by regime (if available in metadata)."""
        # For now, return overall performance as a single "ALL" regime
        # Future: extract regime-specific performance from config/metrics
        regime_breakdown = []

        stats = self._compute_overall_performance(runs)

        regime_breakdown.append(
            RegimePerformance(
                regime_id="ALL",
                stats=stats,
                relative_sharpe=stats.sharpe - baseline_sharpe,
            )
        )

        return regime_breakdown

    def _compare_configurations(
        self, runs: List[BacktestRunRecord]
    ) -> List[ConfigComparison]:
        """Compare different configuration parameters to identify improvements.

        For each config parameter that varies across runs, compute the
        performance difference between different values.
        """
        comparisons = []

        # Extract varying config parameters
        config_keys = set()
        for run in runs:
            config_keys.update(run.config.keys())

        # For each parameter, group runs by value and compare
        for key in sorted(config_keys):
            value_to_runs: Dict[any, List[BacktestRunRecord]] = {}

            for run in runs:
                value = run.config.get(key)
                if value is not None:
                    # Convert to hashable type
                    hashable_value = (
                        tuple(value) if isinstance(value, list) else value
                    )
                    if hashable_value not in value_to_runs:
                        value_to_runs[hashable_value] = []
                    value_to_runs[hashable_value].append(run)

            # Compare pairs of values
            values = list(value_to_runs.keys())
            if len(values) >= 2:
                # Compare first vs second most common values
                baseline_val = values[0]
                alternative_val = values[1]

                baseline_runs = value_to_runs[baseline_val]
                alternative_runs = value_to_runs[alternative_val]

                if len(baseline_runs) >= 2 and len(alternative_runs) >= 2:
                    baseline_stats = self._compute_overall_performance(baseline_runs)
                    alternative_stats = self._compute_overall_performance(
                        alternative_runs
                    )

                    comparisons.append(
                        ConfigComparison(
                            config_key=key,
                            baseline_value=baseline_val,
                            alternative_value=alternative_val,
                            sharpe_delta=alternative_stats.sharpe
                            - baseline_stats.sharpe,
                            return_delta=alternative_stats.return_
                            - baseline_stats.return_,
                            risk_delta=alternative_stats.volatility
                            - baseline_stats.volatility,
                            sample_count=len(baseline_runs) + len(alternative_runs),
                        )
                    )

        # Sort by sharpe improvement
        comparisons.sort(key=lambda c: c.sharpe_delta, reverse=True)

        logger.info(f"Generated {len(comparisons)} configuration comparisons")
        return comparisons

    def _identify_underperforming_configs(
        self, runs: List[BacktestRunRecord]
    ) -> List[Dict[str, any]]:
        """Identify configurations with Sharpe below threshold."""
        underperforming = []

        for run in runs:
            sharpe = run.metrics.get("annualised_sharpe", 0.0)
            if sharpe < self.min_sharpe_threshold:
                underperforming.append(
                    {
                        "run_id": run.run_id,
                        "sharpe": sharpe,
                        "config": run.config,
                        "reason": f"Sharpe {sharpe:.2f} below threshold {self.min_sharpe_threshold:.2f}",
                    }
                )

        logger.info(f"Identified {len(underperforming)} underperforming configurations")
        return underperforming

    def _identify_high_risk_configs(
        self, runs: List[BacktestRunRecord]
    ) -> List[Dict[str, any]]:
        """Identify configurations with excessive risk (volatility or drawdown)."""
        high_risk = []

        for run in runs:
            vol = run.metrics.get("annualised_vol", 0.0)
            drawdown = run.metrics.get("max_drawdown", 0.0)

            reasons = []
            if vol > self.max_volatility_threshold:
                reasons.append(
                    f"Volatility {vol:.2%} exceeds threshold {self.max_volatility_threshold:.2%}"
                )
            if drawdown < self.max_drawdown_threshold:
                reasons.append(
                    f"Drawdown {drawdown:.2%} exceeds threshold {self.max_drawdown_threshold:.2%}"
                )

            if reasons:
                high_risk.append(
                    {
                        "run_id": run.run_id,
                        "volatility": vol,
                        "max_drawdown": drawdown,
                        "config": run.config,
                        "reasons": reasons,
                    }
                )

        logger.info(f"Identified {len(high_risk)} high-risk configurations")
        return high_risk

    def compute_confidence_score(
        self,
        sample_size: int,
        sharpe_delta: float,
        consistency: float = 0.5,
    ) -> float:
        """Compute confidence score for a proposal.

        Args:
            sample_size: Number of runs supporting the proposal
            sharpe_delta: Expected Sharpe improvement
            consistency: Fraction of runs showing improvement (0.0-1.0)

        Returns:
            Confidence score between 0.0 and 1.0
        """
        # Sample size contribution (sigmoid-like)
        size_score = min(1.0, sample_size / 20.0)

        # Sharpe improvement contribution
        sharpe_score = min(1.0, max(0.0, sharpe_delta / 0.5))

        # Consistency contribution
        consistency_score = max(0.0, min(1.0, consistency))

        # Weighted average
        confidence = 0.4 * size_score + 0.4 * sharpe_score + 0.2 * consistency_score

        return float(confidence)
