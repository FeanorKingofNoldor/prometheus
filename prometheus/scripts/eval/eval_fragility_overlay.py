"""Evaluate fragility overlay impact on portfolio performance.

This script tests how exposure scaling based on market fragility affects
risk-adjusted returns. It compares:
- Baseline: 100% equity exposure
- Linear scaling: exposure = 1 - fragility
- Threshold: full exposure if fragility < threshold, else 0%
- Exponential: exposure = exp(-k * fragility)

Metrics: CAGR, max drawdown, Sharpe, Sortino, opportunity cost.

Usage:
    python -m prometheus.scripts.eval.eval_fragility_overlay \
        --start-date 2015-01-01 \
        --end-date 2024-12-31 \
        --strategies linear threshold exponential \
        --output-dir results/fragility_overlay
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import List, Sequence

import numpy as np
import pandas as pd

from apathis.core.config import get_config
from apathis.core.database import DatabaseManager
from apathis.fragility.storage import FragilityStorage
from apathis.data.reader import DataReader


def _parse_date(value: str) -> date:
    """Parse YYYY-MM-DD date string."""
    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


@dataclass
class OverlayStrategy:
    """Fragility-based exposure scaling strategy."""
    
    name: str
    description: str
    
    def compute_exposure(self, fragility: float) -> float:
        """Return exposure level [0, 1] given fragility score [0, 1]."""
        raise NotImplementedError


class BaselineStrategy(OverlayStrategy):
    """Always 100% exposed."""
    
    def __init__(self):
        super().__init__("baseline", "100% exposure (no overlay)")
    
    def compute_exposure(self, fragility: float) -> float:
        return 1.0


class LinearStrategy(OverlayStrategy):
    """Exposure = 1 - fragility."""
    
    def __init__(self):
        super().__init__("linear", "exposure = 1 - fragility")
    
    def compute_exposure(self, fragility: float) -> float:
        return max(0.0, 1.0 - fragility)


class ThresholdStrategy(OverlayStrategy):
    """Full exposure if fragility < threshold, else 0%."""
    
    def __init__(self, threshold: float = 0.5):
        super().__init__(f"threshold_{threshold}", f"100% if fragility < {threshold}, else 0%")
        self.threshold = threshold
    
    def compute_exposure(self, fragility: float) -> float:
        return 1.0 if fragility < self.threshold else 0.0


class ExponentialStrategy(OverlayStrategy):
    """Exposure = exp(-k * fragility)."""
    
    def __init__(self, k: float = 3.0):
        super().__init__(f"exponential_k{k}", f"exposure = exp(-{k} * fragility)")
        self.k = k
    
    def compute_exposure(self, fragility: float) -> float:
        return np.exp(-self.k * fragility)


class StepStrategy(OverlayStrategy):
    """Step function: 100% if NONE, 50% if WATCHLIST, 0% if SHORT_CANDIDATE."""
    
    def __init__(self):
        super().__init__("step", "100% if <0.3, 50% if 0.3-0.5, 0% if >0.5")
    
    def compute_exposure(self, fragility: float) -> float:
        if fragility < 0.3:
            return 1.0
        elif fragility < 0.5:
            return 0.5
        else:
            return 0.0


def load_fragility_and_returns(
    storage: FragilityStorage,
    reader: DataReader,
    market_id: str,
    market_proxy: str,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """Load fragility scores and market returns, merged on date."""
    # Load fragility
    measures = storage.get_history("MARKET", market_id, start_date, end_date)
    
    if not measures:
        return pd.DataFrame()
    
    fragility_data = []
    for m in measures:
        fragility_data.append({
            "date": m.as_of_date,
            "fragility_score": m.fragility_score,
            "class_label": m.class_label.value,
        })
    
    df_fragility = pd.DataFrame(fragility_data)
    df_fragility["date"] = pd.to_datetime(df_fragility["date"])
    
    # Load returns
    prices = reader.read_prices([market_proxy], start_date, end_date)
    
    if prices.empty:
        return pd.DataFrame()
    
    df_returns = prices.sort_values("trade_date")
    df_returns["return"] = df_returns["close"].pct_change()
    df_returns = df_returns[["trade_date", "close", "return"]].rename(columns={"trade_date": "date"})
    df_returns["date"] = pd.to_datetime(df_returns["date"])
    
    # Merge
    df = df_fragility.merge(df_returns, on="date", how="inner")
    df = df.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
    
    # Forward-fill fragility to handle weekends/holidays
    df = df.set_index("date").asfreq("D", method="ffill").reset_index()
    
    return df


def backtest_strategy(
    df: pd.DataFrame,
    strategy: OverlayStrategy,
    risk_free_rate: float = 0.0,
    execution_lag_days: int = 0,
    stochastic_lag: bool = False,
    slippage_bps: float = 0.0,
) -> dict:
    """Run backtest for a single overlay strategy.
    
    Args:
        df: DataFrame with columns [date, fragility_score, return]
        strategy: Overlay strategy defining exposure rules
        risk_free_rate: Annual risk-free rate for Sharpe calculation
    
    Returns:
        Dictionary with performance metrics
    """
    df = df.copy()
    
    # Compute raw exposures
    df["exposure"] = df["fragility_score"].apply(strategy.compute_exposure)

    # Apply execution lag
    if stochastic_lag:
        # 2/3 probability same-day, 1/3 one-day lag
        lag_mask = np.random.rand(len(df)) < (1/3)
        df["exposure_lagged"] = df["exposure"].shift(1)
        df["exposure"] = df["exposure"].where(~lag_mask, df["exposure_lagged"])
        df = df.drop(columns=["exposure_lagged"])
        df = df.dropna(subset=["exposure"])
    elif execution_lag_days > 0:
        df["exposure"] = df["exposure"].shift(execution_lag_days)
        df = df.dropna(subset=["exposure"])

    # Compute exposure changes for slippage
    df["exposure_change"] = df["exposure"].diff().fillna(0.0)
    
    # Trading cost: slippage applied on absolute exposure change
    slippage = abs(df["exposure_change"]) * (slippage_bps / 10000.0)

    # Scaled returns net of slippage
    df["strategy_return"] = df["return"] * df["exposure"] - slippage
    
    # Drop NaN returns (first row)
    df = df.dropna(subset=["strategy_return"])
    
    if len(df) == 0:
        return {}
    
    # Cumulative returns
    df["cumulative_return"] = (1 + df["strategy_return"]).cumprod()
    df["cumulative_baseline"] = (1 + df["return"]).cumprod()
    
    # Metrics
    total_return = df["cumulative_return"].iloc[-1] - 1
    baseline_return = df["cumulative_baseline"].iloc[-1] - 1
    
    years = len(df) / 252
    cagr = (1 + total_return) ** (1 / years) - 1 if years > 0 else 0.0
    baseline_cagr = (1 + baseline_return) ** (1 / years) - 1 if years > 0 else 0.0
    
    # Drawdown
    cumulative = df["cumulative_return"]
    running_max = cumulative.expanding().max()
    drawdown = (cumulative - running_max) / running_max
    max_drawdown = drawdown.min()
    
    baseline_cumulative = df["cumulative_baseline"]
    baseline_running_max = baseline_cumulative.expanding().max()
    baseline_drawdown = (baseline_cumulative - baseline_running_max) / baseline_running_max
    baseline_max_drawdown = baseline_drawdown.min()
    
    # Sharpe ratio
    daily_rf = (1 + risk_free_rate) ** (1 / 252) - 1
    excess_returns = df["strategy_return"] - daily_rf
    sharpe = (excess_returns.mean() / excess_returns.std() * np.sqrt(252)) if excess_returns.std() > 0 else 0.0
    
    baseline_excess = df["return"] - daily_rf
    baseline_sharpe = (baseline_excess.mean() / baseline_excess.std() * np.sqrt(252)) if baseline_excess.std() > 0 else 0.0
    
    # Sortino ratio (downside deviation)
    downside_returns = excess_returns[excess_returns < 0]
    downside_std = downside_returns.std() if len(downside_returns) > 1 else 0.0
    sortino = (excess_returns.mean() / downside_std * np.sqrt(252)) if downside_std > 0 else 0.0
    
    baseline_downside = baseline_excess[baseline_excess < 0]
    baseline_downside_std = baseline_downside.std() if len(baseline_downside) > 1 else 0.0
    baseline_sortino = (baseline_excess.mean() / baseline_downside_std * np.sqrt(252)) if baseline_downside_std > 0 else 0.0
    
    # Opportunity cost (CAGR difference)
    opportunity_cost = baseline_cagr - cagr
    
    # Average exposure
    avg_exposure = df["exposure"].mean()
    
    # Time in market
    time_in_market = (df["exposure"] > 0).mean()
    
    return {
        "strategy": strategy.name,
        "total_return": total_return,
        "cagr": cagr,
        "max_drawdown": max_drawdown,
        "sharpe": sharpe,
        "sortino": sortino,
        "avg_exposure": avg_exposure,
        "time_in_market": time_in_market,
        "opportunity_cost": opportunity_cost,
        "baseline_cagr": baseline_cagr,
        "baseline_max_drawdown": baseline_max_drawdown,
        "baseline_sharpe": baseline_sharpe,
        "baseline_sortino": baseline_sortino,
    }


def main(argv: Sequence[str] | None = None) -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Evaluate fragility overlay strategies"
    )
    
    parser.add_argument("--start-date", type=_parse_date, required=True)
    parser.add_argument("--end-date", type=_parse_date, required=True)
    parser.add_argument("--market-id", type=str, default="US_EQ")
    parser.add_argument("--market-proxy", type=str, default="SPY.US")
    parser.add_argument(
        "--strategies",
        nargs="+",
        choices=["baseline", "linear", "threshold", "exponential", "step", "all"],
        default=["all"],
        help="Strategies to test",
    )
    parser.add_argument("--risk-free-rate", type=float, default=0.03, help="Annual risk-free rate")
    parser.add_argument("--execution-lag-days", type=int, default=0, help="Delay in days before exposure change applies")
    parser.add_argument("--stochastic-lag", action="store_true", help="Use stochastic lag: 2/3 same-day, 1/3 one-day")
    parser.add_argument("--slippage-bps", type=float, default=0.0, help="Per-unit slippage cost in basis points applied to exposure changes")
    parser.add_argument(
        "--output-dir",
        type=str,
        default="results/fragility_overlay",
        help="Output directory for results",
    )
    
    args = parser.parse_args(argv)
    
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"\n{'='*60}")
    print(f"Fragility Overlay Backtest")
    print(f"{'='*60}")
    print(f"Period: {args.start_date} to {args.end_date}")
    print(f"Market: {args.market_id}")
    print(f"Proxy: {args.market_proxy}")
    print(f"Risk-free rate: {args.risk_free_rate*100:.1f}%")
    print(f"Output: {output_dir}")
    print(f"{'='*60}\n")
    
    # Load data
    config = get_config()
    db_manager = DatabaseManager(config)
    storage = FragilityStorage(db_manager=db_manager)
    reader = DataReader(db_manager=db_manager)
    
    print("Loading fragility and returns...")
    df = load_fragility_and_returns(
        storage, reader, args.market_id, args.market_proxy, args.start_date, args.end_date
    )
    print(f"  Loaded {len(df)} trading days\n")
    
    if df.empty:
        print("ERROR: No data available")
        return
    
    # Define strategies
    all_strategies = [
        BaselineStrategy(),
        LinearStrategy(),
        ThresholdStrategy(0.45),  # default best performer in 2015-2024 w/ costs
        ThresholdStrategy(0.5),
        ThresholdStrategy(0.55),
        ThresholdStrategy(0.6),
        ExponentialStrategy(2.0),
        ExponentialStrategy(3.0),
        ExponentialStrategy(4.0),
        StepStrategy(),
    ]
    
    # Filter strategies
    if "all" not in args.strategies:
        strategy_names = set(args.strategies)
        all_strategies = [s for s in all_strategies if any(name in s.name for name in strategy_names)]
    
    print(f"Testing {len(all_strategies)} strategies...\n")
    
    # Run backtests
    results = []
    for strategy in all_strategies:
        print(f"  {strategy.name:25s} - {strategy.description}")
        metrics = backtest_strategy(
            df,
            strategy,
            risk_free_rate=args.risk_free_rate,
            execution_lag_days=args.execution_lag_days,
            stochastic_lag=args.stochastic_lag,
            slippage_bps=args.slippage_bps,
        )
        if metrics:
            results.append(metrics)
    
    if not results:
        print("\nERROR: No backtest results")
        return
    
    df_results = pd.DataFrame(results)
    
    # Save results
    df_results.to_csv(output_dir / "overlay_results.csv", index=False)
    print(f"\n  Saved: {output_dir / 'overlay_results.csv'}")
    
    # Print summary
    print(f"\n{'='*60}")
    print("Results Summary")
    print(f"{'='*60}\n")
    
    print(f"{'Strategy':<25s} {'CAGR':>8s} {'MaxDD':>8s} {'Sharpe':>8s} {'Exposure':>10s} {'OppCost':>10s}")
    print("-" * 80)
    
    for _, row in df_results.iterrows():
        print(f"{row['strategy']:<25s} "
              f"{row['cagr']*100:7.2f}% "
              f"{row['max_drawdown']*100:7.2f}% "
              f"{row['sharpe']:7.2f} "
              f"{row['avg_exposure']*100:8.1f}% "
              f"{row['opportunity_cost']*100:8.2f}%")
    
    print(f"\n{'='*60}")
    print("Baseline Comparison")
    print(f"{'='*60}\n")
    
    baseline_row = df_results[df_results["strategy"] == "baseline"].iloc[0]
    print(f"Baseline CAGR: {baseline_row['baseline_cagr']*100:.2f}%")
    print(f"Baseline Max DD: {baseline_row['baseline_max_drawdown']*100:.2f}%")
    print(f"Baseline Sharpe: {baseline_row['baseline_sharpe']:.2f}")
    
    print(f"\n{'='*60}")
    print("Best Risk-Adjusted Strategy")
    print(f"{'='*60}\n")
    
    # Find best by Sharpe ratio (excluding baseline)
    overlay_results = df_results[df_results["strategy"] != "baseline"]
    if len(overlay_results) > 0:
        best_sharpe = overlay_results.loc[overlay_results["sharpe"].idxmax()]
        print(f"Strategy: {best_sharpe['strategy']}")
        print(f"CAGR: {best_sharpe['cagr']*100:.2f}% (opportunity cost: {best_sharpe['opportunity_cost']*100:.2f}%)")
        print(f"Max DD: {best_sharpe['max_drawdown']*100:.2f}% (vs baseline {baseline_row['baseline_max_drawdown']*100:.2f}%)")
        print(f"Sharpe: {best_sharpe['sharpe']:.2f} (vs baseline {baseline_row['baseline_sharpe']:.2f})")
        print(f"Avg Exposure: {best_sharpe['avg_exposure']*100:.1f}%")
    
    print(f"\n{'='*60}")
    print("Evaluation complete!")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
