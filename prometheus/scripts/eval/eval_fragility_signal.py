"""Evaluate market fragility signal quality against forward outcomes.

This script analyzes how well market fragility scores predict future
drawdowns, tail risk, and crash events. It does NOT predict event start
dates - instead it measures vulnerability/shock sensitivity.

Usage:
    python -m prometheus.scripts.eval.eval_fragility_signal \\
        --start-date 2015-01-01 \\
        --end-date 2024-12-31 \\
        --horizons 21 63 \\
        --output-dir results/fragility_eval
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
from typing import List, Sequence, Tuple

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


def load_market_fragility(
    storage: FragilityStorage,
    market_id: str,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """Load market fragility history as DataFrame."""
    measures = storage.get_history("MARKET", market_id, start_date, end_date)
    
    if not measures:
        return pd.DataFrame()
    
    data = []
    for m in measures:
        data.append({
            "date": m.as_of_date,
            "fragility_score": m.fragility_score,
            "class_label": m.class_label.value,
        })
    
    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date").reset_index(drop=True)


def load_market_returns(
    reader: DataReader,
    market_proxy_ticker: str,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """Load market proxy returns (e.g., SPY)."""
    # Extend date range to capture forward returns
    extended_end = end_date + timedelta(days=365)
    
    prices = reader.read_prices([market_proxy_ticker], start_date, extended_end)
    
    if prices.empty:
        return pd.DataFrame()
    
    prices = prices.sort_values("trade_date")
    prices["return"] = prices["close"].pct_change()
    
    result = prices[["trade_date", "close", "return"]].rename(columns={"trade_date": "date"})
    result["date"] = pd.to_datetime(result["date"])
    return result


def compute_forward_outcomes(
    fragility: pd.DataFrame,
    returns: pd.DataFrame,
    horizons: List[int],
) -> pd.DataFrame:
    """Compute forward drawdowns and returns for each fragility observation.
    
    Args:
        fragility: DataFrame with columns [date, fragility_score, class_label]
        returns: DataFrame with columns [date, close, return]
        horizons: List of forward horizons in trading days
    
    Returns:
        DataFrame with fragility + forward outcomes for each horizon
    """
    # Merge fragility with returns
    df = fragility.merge(returns, on="date", how="inner")
    df = df.sort_values("date").reset_index(drop=True)
    
    results = []
    
    for idx, row in df.iterrows():
        current_date = row["date"]
        current_price = row["close"]
        
        record = {
            "date": current_date,
            "fragility_score": row["fragility_score"],
            "class_label": row["class_label"],
        }
        
        # Compute forward outcomes for each horizon
        for horizon in horizons:
            # Get future prices within horizon window
            future_mask = (returns["date"] > current_date) & (returns["date"] <= current_date + pd.Timedelta(days=horizon * 2))
            future_prices = returns[future_mask].head(horizon)
            
            if len(future_prices) < horizon // 2:
                # Insufficient future data
                record[f"fwd_return_{horizon}d"] = np.nan
                record[f"fwd_max_dd_{horizon}d"] = np.nan
                record[f"fwd_crash_{horizon}d"] = np.nan
                continue
            
            # Forward return
            if len(future_prices) > 0:
                final_price = future_prices.iloc[-1]["close"]
                fwd_return = (final_price - current_price) / current_price
                record[f"fwd_return_{horizon}d"] = fwd_return
                
                # Max drawdown over horizon
                running_max = future_prices["close"].expanding().max()
                drawdowns = (future_prices["close"] - running_max) / running_max
                max_dd = drawdowns.min()
                record[f"fwd_max_dd_{horizon}d"] = max_dd
                
                # Crash indicator (>10% drawdown)
                record[f"fwd_crash_{horizon}d"] = 1 if max_dd < -0.10 else 0
            else:
                record[f"fwd_return_{horizon}d"] = np.nan
                record[f"fwd_max_dd_{horizon}d"] = np.nan
                record[f"fwd_crash_{horizon}d"] = np.nan
        
        results.append(record)
    
    return pd.DataFrame(results)


def analyze_by_decile(df: pd.DataFrame, horizons: List[int]) -> pd.DataFrame:
    """Analyze forward outcomes by fragility decile."""
    # Drop rows with NaN fragility or forward outcomes
    valid = df.dropna(subset=["fragility_score"])
    
    # Assign deciles
    valid["fragility_decile"] = pd.qcut(
        valid["fragility_score"],
        q=10,
        labels=False,
        duplicates="drop"
    ) + 1
    
    results = []
    
    for decile in range(1, 11):
        decile_data = valid[valid["fragility_decile"] == decile]
        
        if len(decile_data) == 0:
            continue
        
        record = {
            "decile": decile,
            "count": len(decile_data),
            "mean_fragility": decile_data["fragility_score"].mean(),
            "min_fragility": decile_data["fragility_score"].min(),
            "max_fragility": decile_data["fragility_score"].max(),
        }
        
        for horizon in horizons:
            fwd_return_col = f"fwd_return_{horizon}d"
            fwd_dd_col = f"fwd_max_dd_{horizon}d"
            fwd_crash_col = f"fwd_crash_{horizon}d"
            
            if fwd_return_col in decile_data.columns:
                valid_returns = decile_data[fwd_return_col].dropna()
                if len(valid_returns) > 0:
                    record[f"mean_return_{horizon}d"] = valid_returns.mean()
                    record[f"median_return_{horizon}d"] = valid_returns.median()
                    record[f"p5_return_{horizon}d"] = valid_returns.quantile(0.05)
                    record[f"p95_return_{horizon}d"] = valid_returns.quantile(0.95)
            
            if fwd_dd_col in decile_data.columns:
                valid_dd = decile_data[fwd_dd_col].dropna()
                if len(valid_dd) > 0:
                    record[f"mean_max_dd_{horizon}d"] = valid_dd.mean()
                    record[f"median_max_dd_{horizon}d"] = valid_dd.median()
                    record[f"p5_max_dd_{horizon}d"] = valid_dd.quantile(0.05)
            
            if fwd_crash_col in decile_data.columns:
                valid_crash = decile_data[fwd_crash_col].dropna()
                if len(valid_crash) > 0:
                    record[f"crash_rate_{horizon}d"] = valid_crash.mean()
        
        results.append(record)
    
    return pd.DataFrame(results)


def analyze_by_class(df: pd.DataFrame, horizons: List[int]) -> pd.DataFrame:
    """Analyze forward outcomes by fragility class."""
    results = []
    
    for class_label in ["NONE", "WATCHLIST", "SHORT_CANDIDATE", "CRISIS"]:
        class_data = df[df["class_label"] == class_label]
        
        if len(class_data) == 0:
            continue
        
        record = {
            "class_label": class_label,
            "count": len(class_data),
            "mean_fragility": class_data["fragility_score"].mean(),
        }
        
        for horizon in horizons:
            fwd_return_col = f"fwd_return_{horizon}d"
            fwd_dd_col = f"fwd_max_dd_{horizon}d"
            fwd_crash_col = f"fwd_crash_{horizon}d"
            
            if fwd_return_col in class_data.columns:
                valid_returns = class_data[fwd_return_col].dropna()
                if len(valid_returns) > 0:
                    record[f"mean_return_{horizon}d"] = valid_returns.mean()
                    record[f"p5_return_{horizon}d"] = valid_returns.quantile(0.05)
            
            if fwd_dd_col in class_data.columns:
                valid_dd = class_data[fwd_dd_col].dropna()
                if len(valid_dd) > 0:
                    record[f"mean_max_dd_{horizon}d"] = valid_dd.mean()
                    record[f"p5_max_dd_{horizon}d"] = valid_dd.quantile(0.05)
            
            if fwd_crash_col in class_data.columns:
                valid_crash = class_data[fwd_crash_col].dropna()
                if len(valid_crash) > 0:
                    record[f"crash_rate_{horizon}d"] = valid_crash.mean()
        
        results.append(record)
    
    return pd.DataFrame(results)


def compute_lead_time_analysis(df: pd.DataFrame, crash_threshold: float = -0.10) -> pd.DataFrame:
    """Analyze how far in advance fragility elevates before crashes."""
    # Find crash events (>10% drawdown)
    crash_dates = []
    for idx, row in df.iterrows():
        if any(row.get(f"fwd_max_dd_{h}d", 0) < crash_threshold for h in [21, 63]):
            crash_dates.append(row["date"])
    
    if not crash_dates:
        return pd.DataFrame()
    
    # For each crash, find when fragility was elevated before it
    lead_times = []
    
    for crash_date in crash_dates:
        # Look back 126 days (6 months)
        lookback_start = crash_date - pd.Timedelta(days=126)
        pre_crash = df[(df["date"] >= lookback_start) & (df["date"] < crash_date)]
        
        # Find first day fragility exceeded 0.5
        elevated = pre_crash[pre_crash["fragility_score"] >= 0.5]
        
        if len(elevated) > 0:
            first_elevated = elevated.iloc[0]["date"]
            lead_days = (crash_date - first_elevated).days
            lead_times.append({
                "crash_date": crash_date,
                "first_elevated_date": first_elevated,
                "lead_days": lead_days,
            })
    
    return pd.DataFrame(lead_times)


def main(argv: Sequence[str] | None = None) -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Evaluate market fragility signal quality"
    )
    
    parser.add_argument("--start-date", type=_parse_date, required=True)
    parser.add_argument("--end-date", type=_parse_date, required=True)
    parser.add_argument(
        "--horizons",
        type=int,
        nargs="+",
        default=[21, 63],
        help="Forward horizons in trading days",
    )
    parser.add_argument("--market-id", type=str, default="US_EQ")
    parser.add_argument(
        "--market-proxy",
        type=str,
        default="SPY",
        help="Market proxy ticker for returns",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="results/fragility_eval",
        help="Output directory for results",
    )
    
    args = parser.parse_args(argv)
    
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"\n{'='*60}")
    print(f"Market Fragility Signal Evaluation")
    print(f"{'='*60}")
    print(f"Period: {args.start_date} to {args.end_date}")
    print(f"Market: {args.market_id}")
    print(f"Horizons: {args.horizons} days")
    print(f"Output: {output_dir}")
    print(f"{'='*60}\n")
    
    # Load data
    config = get_config()
    db_manager = DatabaseManager(config)
    storage = FragilityStorage(db_manager=db_manager)
    reader = DataReader(db_manager=db_manager)
    
    print("Loading market fragility scores...")
    fragility = load_market_fragility(storage, args.market_id, args.start_date, args.end_date)
    print(f"  Loaded {len(fragility)} fragility observations")
    
    print("Loading market returns...")
    returns = load_market_returns(reader, args.market_proxy, args.start_date, args.end_date)
    print(f"  Loaded {len(returns)} return observations")
    
    if fragility.empty or returns.empty:
        print("ERROR: Insufficient data")
        return
    
    print("\nComputing forward outcomes...")
    outcomes = compute_forward_outcomes(fragility, returns, args.horizons)
    print(f"  Computed outcomes for {len(outcomes)} observations")
    
    # Save raw outcomes
    outcomes.to_csv(output_dir / "outcomes_raw.csv", index=False)
    print(f"  Saved: {output_dir / 'outcomes_raw.csv'}")
    
    print("\nAnalyzing by decile...")
    decile_analysis = analyze_by_decile(outcomes, args.horizons)
    decile_analysis.to_csv(output_dir / "analysis_by_decile.csv", index=False)
    print(f"  Saved: {output_dir / 'analysis_by_decile.csv'}")
    
    print("\nDecile Analysis (63-day horizon):")
    print("-" * 80)
    for _, row in decile_analysis.iterrows():
        print(f"Decile {int(row['decile']):2d} | "
              f"Fragility: {row['mean_fragility']:.3f} | "
              f"Mean Return: {row.get('mean_return_63d', np.nan)*100:6.2f}% | "
              f"Mean Max DD: {row.get('mean_max_dd_63d', np.nan)*100:6.2f}% | "
              f"Crash Rate: {row.get('crash_rate_63d', np.nan)*100:5.1f}%")
    
    print("\nAnalyzing by class...")
    class_analysis = analyze_by_class(outcomes, args.horizons)
    class_analysis.to_csv(output_dir / "analysis_by_class.csv", index=False)
    print(f"  Saved: {output_dir / 'analysis_by_class.csv'}")
    
    print("\nClass Analysis (63-day horizon):")
    print("-" * 80)
    for _, row in class_analysis.iterrows():
        print(f"{row['class_label']:20s} | "
              f"Count: {int(row['count']):4d} | "
              f"Mean DD: {row.get('mean_max_dd_63d', np.nan)*100:6.2f}% | "
              f"Crash Rate: {row.get('crash_rate_63d', np.nan)*100:5.1f}%")
    
    print("\nAnalyzing lead times...")
    lead_times = compute_lead_time_analysis(outcomes)
    if not lead_times.empty:
        lead_times.to_csv(output_dir / "lead_time_analysis.csv", index=False)
        print(f"  Saved: {output_dir / 'lead_time_analysis.csv'}")
        print(f"\n  Found {len(lead_times)} crash events")
        print(f"  Mean lead time: {lead_times['lead_days'].mean():.1f} days")
        print(f"  Median lead time: {lead_times['lead_days'].median():.1f} days")
    else:
        print("  No crash events found with elevated fragility")
    
    print(f"\n{'='*60}")
    print("Evaluation complete!")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
