"""Compute a v1 regime hazard *signals* CSV from a market proxy instrument.

This script generates Option-B-correct, forward-looking hazard signals
for backtests and regime detection:
- down_risk, up_risk in [0,1]

The signals are computed using only information available up to each
as_of_date (trailing vol/momentum/drawdown features).

Optionally it also computes hindsight labels (future max drawdown and
future return) for evaluation/tuning.

Output CSV schema (always):
- as_of_date
- down_risk
- up_risk

Additional columns may be included for debugging/evaluation.
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta
from pathlib import Path
from typing import Optional, Sequence

from apathis.core.config import get_config
from apathis.core.database import DatabaseManager
from apathis.core.logging import get_logger
from apathis.data.reader import DataReader
from apathis.regime.hazard_overlay import (
    RegimeHazardConfig,
    compute_future_max_drawdown_label,
    compute_future_return_label,
    compute_hazard_scores,
    compute_proxy_features,
)

logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:  # pragma: no cover - CLI validation
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Compute a regime hazard signals CSV (down_risk/up_risk) from a single market proxy instrument."
        )
    )

    parser.add_argument("--instrument-id", type=str, required=True, help="Proxy instrument_id (e.g. SPY.US)")
    parser.add_argument("--start", type=_parse_date, required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=_parse_date, required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--output", type=str, required=True, help="Output CSV path")

    parser.add_argument(
        "--price-col",
        type=str,
        default="adjusted_close",
        choices=["close", "adjusted_close"],
        help="Price column to use (default: adjusted_close)",
    )

    parser.add_argument(
        "--lookback-calendar-days",
        type=int,
        default=900,
        help="Calendar-day lookback buffer for feature windows (default: 900)",
    )

    # Feature windows.
    parser.add_argument("--vol-window-days", type=int, default=21)
    parser.add_argument("--mom-window-days", type=int, default=63)
    parser.add_argument("--dd-window-days", type=int, default=252)

    # Normalisation scales.
    parser.add_argument("--vol-low", type=float, default=0.01)
    parser.add_argument("--vol-high", type=float, default=0.03)
    parser.add_argument("--mom-scale", type=float, default=0.10)
    parser.add_argument("--dd-scale", type=float, default=0.15)

    # Score weights.
    parser.add_argument("--w-vol-down", type=float, default=0.5)
    parser.add_argument("--w-mom-down", type=float, default=0.3)
    parser.add_argument("--w-dd-down", type=float, default=0.2)
    parser.add_argument("--w-mom-up", type=float, default=0.7)
    parser.add_argument("--w-vol-up", type=float, default=0.3)


    # Optional evaluation labels.
    parser.add_argument("--label-horizon-days", type=int, default=63)
    parser.add_argument("--label-dd-threshold", type=float, default=0.15)
    parser.add_argument("--label-up-return-threshold", type=float, default=0.10)
    parser.add_argument(
        "--with-labels",
        action="store_true",
        help="Also compute hindsight labels (future drawdown/return) for evaluation.",
    )

    args = parser.parse_args(argv)

    if args.end < args.start:
        parser.error("--end must be >= --start")

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    start_buf = args.start - timedelta(days=int(args.lookback_calendar_days))

    cfg = RegimeHazardConfig(
        vol_window_days=int(args.vol_window_days),
        mom_window_days=int(args.mom_window_days),
        dd_window_days=int(args.dd_window_days),
        vol_low=float(args.vol_low),
        vol_high=float(args.vol_high),
        mom_scale=float(args.mom_scale),
        dd_scale=float(args.dd_scale),
        w_vol_down=float(args.w_vol_down),
        w_mom_down=float(args.w_mom_down),
        w_dd_down=float(args.w_dd_down),
        w_mom_up=float(args.w_mom_up),
        w_vol_up=float(args.w_vol_up),
    )

    config = get_config()
    db_manager = DatabaseManager(config)
    reader = DataReader(db_manager=db_manager)

    df_prices = reader.read_prices_close(
        [str(args.instrument_id)],
        start_buf,
        args.end,
        price_col=str(args.price_col),
    )

    if df_prices.empty:
        raise SystemExit(f"No price data for instrument_id={args.instrument_id} in requested window")

    # Keep only the requested instrument.
    df_prices = df_prices[df_prices["instrument_id"].astype(str) == str(args.instrument_id)].copy()

    # Compute trailing features + hazard scores.
    df_feat = compute_proxy_features(df_prices=df_prices, price_col="close", cfg=cfg)
    df_scores = compute_hazard_scores(df_features=df_feat, cfg=cfg)

    # Join for output.
    df_out = df_scores.join(df_feat[["close", "vol", "mom", "dd"]], how="left")

    # Filter to [start, end].
    idx = df_out.index
    df_out = df_out[(idx >= args.start) & (idx <= args.end)].copy()

    if args.with_labels:
        closes = df_out["close"].astype(float).to_numpy()
        y_dd = compute_future_max_drawdown_label(
            closes,
            horizon_days=int(args.label_horizon_days),
            dd_threshold=float(args.label_dd_threshold),
        )
        y_up = compute_future_return_label(
            closes,
            horizon_days=int(args.label_horizon_days),
            return_threshold=float(args.label_up_return_threshold),
        )
        df_out["y_future_maxdd"] = y_dd
        df_out["y_future_upret"] = y_up

    # Output schema (stable columns first).
    df_out = df_out.reset_index().rename(columns={"trade_date": "as_of_date"})

    cols_first = ["as_of_date", "down_risk", "up_risk"]
    cols_rest = [c for c in df_out.columns if c not in cols_first]
    df_out = df_out[cols_first + cols_rest]

    df_out.to_csv(out_path, index=False)

    logger.info(
        "Wrote regime hazard signals: instrument_id=%s rows=%d start=%s end=%s -> %s",
        args.instrument_id,
        df_out.shape[0],
        args.start,
        args.end,
        out_path,
    )


if __name__ == "__main__":  # pragma: no cover
    main()
