"""Compare engine parameter variants via backtest.

Runs 6 variants over 2020-2025 and prints a comparison table.
Usage: python scripts/compare_engine_variants.py [--start 2020-01-02] [--end 2025-12-31]
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

# Ensure both projects on path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "apatheon"))

import logging
logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(name)s %(levelname)s %(message)s")
# Show info for our modules only
logging.getLogger("prometheus.backtest").setLevel(logging.INFO)

from apatheon.core.database import get_db_manager
from apatheon.core.time import TradingCalendar
from prometheus.backtest import run_backtest_campaign, SleeveConfig


def build_variants() -> list[SleeveConfig]:
    """Build the 6 variant configs."""

    # Common base
    base = dict(
        market_id="US_EQ",
        universe_id="CORE_EQ_US",
        portfolio_id="US_EQ_LONG",
        assessment_strategy_id="US_CORE_LONG_EQ",
        assessment_horizon_days=21,
        assessment_backend="basic",
        portfolio_max_names=25,
        portfolio_per_instrument_max_weight=0.10,
        portfolio_hysteresis_buffer=5,
        # Current production defaults
        stability_risk_alpha=0.0,
        regime_risk_alpha=0.0,
        conviction_enabled=False,
        lambda_score_weight=0.0,
        apply_sector_allocator=False,
        meta_budget_enabled=False,
    )

    variants = []

    # 0. BASELINE — current production V12/K25
    variants.append(SleeveConfig(
        sleeve_id="BASELINE_V12K25",
        strategy_id="BASELINE_V12K25",
        **base,
    ))

    # 1. Enable STAB + regime risk modifiers in universe
    v1 = {**base, "stability_risk_alpha": 0.3, "regime_risk_alpha": 0.3}
    variants.append(SleeveConfig(
        sleeve_id="V1_STAB_REGIME",
        strategy_id="V1_STAB_REGIME",
        **v1,
    ))

    # 2. Loosen assessment thresholds (via score_concentration_power as proxy)
    # Note: assessment thresholds are in the model, not the sleeve config.
    # We test the effect by reducing fragility penalty (stability_risk_alpha
    # in universe) and using higher concentration_power to amplify signals.
    v2 = {**base}
    # The sleeve config doesn't directly control assessment thresholds,
    # but stability_risk_alpha=0.5 is the default that was designed to work
    # with the assessment model. Let's use it.
    v2["stability_risk_alpha"] = 0.5
    variants.append(SleeveConfig(
        sleeve_id="V2_STAB_DEFAULT",
        strategy_id="V2_STAB_DEFAULT",
        **v2,
    ))

    # 3. Conviction model enabled
    v3 = {**base, "conviction_enabled": True}
    variants.append(SleeveConfig(
        sleeve_id="V3_CONVICTION",
        strategy_id="V3_CONVICTION",
        **v3,
    ))

    # 4. Lambda opportunity scoring
    v4 = {**base, "lambda_score_weight": 10.0}
    variants.append(SleeveConfig(
        sleeve_id="V4_LAMBDA",
        strategy_id="V4_LAMBDA",
        **v4,
    ))

    # 5. All combined: STAB + regime + conviction + lambda + sector allocator
    v5 = {
        **base,
        "stability_risk_alpha": 0.3,
        "regime_risk_alpha": 0.3,
        "conviction_enabled": True,
        "lambda_score_weight": 10.0,
        "apply_sector_allocator": True,
        "meta_budget_enabled": True,
        "meta_budget_alpha": 1.0,
        "meta_budget_min": 0.35,
    }
    variants.append(SleeveConfig(
        sleeve_id="V5_ALL_COMBINED",
        strategy_id="V5_ALL_COMBINED",
        **v5,
    ))

    return variants


def main():
    parser = argparse.ArgumentParser(description="Compare engine parameter variants")
    parser.add_argument("--start", type=str, default="2020-01-02", help="Start date YYYY-MM-DD")
    parser.add_argument("--end", type=str, default="2025-12-31", help="End date YYYY-MM-DD")
    parser.add_argument("--cash", type=float, default=1_000_000, help="Initial cash per sleeve")
    args = parser.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)

    print(f"=== Engine Variant Comparison: {start} to {end} ===")
    print(f"Initial cash: ${args.cash:,.0f}")
    print()

    db = get_db_manager()
    calendar = TradingCalendar()
    variants = build_variants()

    print(f"Running {len(variants)} variants...")
    for v in variants:
        print(f"  {v.strategy_id}: stab={v.stability_risk_alpha} regime={v.regime_risk_alpha} "
              f"conviction={v.conviction_enabled} lambda={v.lambda_score_weight} "
              f"sector_alloc={v.apply_sector_allocator} meta_budget={v.meta_budget_enabled}")
    print()

    summaries = run_backtest_campaign(
        db_manager=db,
        calendar=calendar,
        market_id="US_EQ",
        start_date=start,
        end_date=end,
        sleeve_configs=variants,
        initial_cash=args.cash,
        apply_risk=True,
    )

    # Print comparison table
    print()
    print(f"{'Variant':<25s} {'Cum.Ret':>10s} {'CAGR':>8s} {'Sharpe':>8s} {'MaxDD':>8s} {'Trades':>8s}")
    print("-" * 72)

    years = (end - start).days / 365.25

    for s in summaries:
        m = s.metrics or {}
        cum = float(m.get("cumulative_return", 0))
        sharpe = float(m.get("annualised_sharpe", 0))
        dd = float(m.get("max_drawdown", 0))
        trades = int(m.get("total_trades", 0))
        # CAGR from cumulative return
        if cum > -1:
            cagr = ((1 + cum) ** (1 / years) - 1) * 100 if years > 0 else 0
        else:
            cagr = -100.0

        print(f"{s.strategy_id:<25s} {cum:>9.1%} {cagr:>7.1f}% {sharpe:>8.3f} {dd:>7.1%} {trades:>8d}")

    print()
    print("Done. Run IDs saved to backtest_runs table.")


if __name__ == "__main__":
    main()
