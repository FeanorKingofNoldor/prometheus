"""Compute a regime hazard overlay CSV from a synthetic market proxy (basket).

This is an Option-B-correct helper for research/backtests when an index/ETF
proxy (SPY/QQQ/IWM) is unavailable in the DB.

Approach
- Select a basket of instruments using only data *before* the requested
  start date (to avoid forward-looking selection).
- Build a synthetic proxy series from cross-sectional daily returns.
- Run the same hazard overlay feature + score pipeline as
  `compute_regime_hazard_overlay`.

Output CSV schema (always)
- as_of_date
- down_risk
- up_risk

Additional debug columns may be included.
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta
from pathlib import Path
from typing import Optional, Sequence

import numpy as np
import pandas as pd
from apatheon.core.config import get_config
from apatheon.core.database import DatabaseManager
from apatheon.core.logging import get_logger
from apatheon.data.reader import DataReader
from apatheon.regime.hazard_overlay import (
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


def _select_instruments(
    *,
    db_manager: DatabaseManager,
    start_date: date,
    end_date: date,
    instrument_limit: int,
    min_obs: int,
    selector: str,
    market_suffix: str | None,
) -> list[str]:
    # We query historical.prices_daily directly so this can work even if the
    # runtime instruments table is missing common ETFs.
    where_suffix = ""
    params: list[object] = [start_date, end_date, int(min_obs), int(instrument_limit)]

    if market_suffix:
        # NOTE: instrument_id format in this repo appears to be like AAPL.US.
        where_suffix = " AND instrument_id LIKE %s"
        params.insert(2, f"%.{market_suffix}")

    if selector == "volume_sum":
        order_expr = "SUM(COALESCE(volume, 0)) DESC"
    elif selector == "volume_avg":
        order_expr = "AVG(COALESCE(volume, 0)) DESC"
    elif selector == "count":
        order_expr = "COUNT(*) DESC"
    else:  # pragma: no cover
        raise ValueError(f"Unknown selector: {selector}")

    sql = f"""
        SELECT instrument_id
        FROM prices_daily
        WHERE trade_date BETWEEN %s AND %s
        {where_suffix}
        GROUP BY instrument_id
        HAVING COUNT(*) >= %s
        ORDER BY {order_expr}, instrument_id
        LIMIT %s
    """

    with db_manager.get_historical_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, params)
            rows = cur.fetchall()
        finally:
            cur.close()

    return [str(r[0]) for r in rows]


def _build_proxy_series(
    df_prices: pd.DataFrame,
    *,
    agg: str,
    winsor_pct: tuple[float, float] | None,
    base_level: float,
) -> pd.DataFrame:
    if df_prices.empty:
        return pd.DataFrame(columns=["trade_date", "close"])

    df = df_prices.copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    df = df.sort_values(["instrument_id", "trade_date"]).reset_index(drop=True)

    df["close"] = pd.to_numeric(df["close"], errors="coerce")

    # Guard against bad price rows (0/negative/NaN) which would create +/-inf logs.
    df = df[df["close"].notna() & (df["close"] > 0.0)].copy()
    if df.empty:
        return pd.DataFrame(columns=["trade_date", "close"])

    df["log_close"] = np.log(df["close"])
    df["log_ret_1d"] = df.groupby("instrument_id", group_keys=False)["log_close"].diff()

    # Drop non-finite returns before cross-sectional aggregation.
    df = df[np.isfinite(df["log_ret_1d"].to_numpy(dtype=float, copy=False)) | df["log_ret_1d"].isna()].copy()

    # Cross-sectional aggregation per day.
    rets = df[["trade_date", "log_ret_1d"]].dropna()
    if rets.empty:
        return pd.DataFrame(columns=["trade_date", "close"])

    if winsor_pct is not None:
        lo, hi = float(winsor_pct[0]), float(winsor_pct[1])
        if not (0.0 <= lo < hi <= 100.0):
            raise ValueError("winsor_pct must satisfy 0 <= lo < hi <= 100")

        def _winsor_one(x: pd.Series) -> pd.Series:
            arr = x.to_numpy(dtype=float, copy=False)
            q_lo = np.nanpercentile(arr, lo)
            q_hi = np.nanpercentile(arr, hi)
            return x.clip(lower=q_lo, upper=q_hi)

        rets["log_ret_1d"] = rets.groupby("trade_date", group_keys=False)["log_ret_1d"].transform(
            _winsor_one
        )

    if agg == "median":
        proxy_lr = rets.groupby("trade_date")["log_ret_1d"].median()
    elif agg == "mean":
        proxy_lr = rets.groupby("trade_date")["log_ret_1d"].mean()
    else:  # pragma: no cover
        raise ValueError(f"Unknown agg: {agg}")

    proxy_lr = proxy_lr.sort_index()
    # Fill any gaps with 0 return (should be rare if trade_date coverage is good).
    proxy_lr = proxy_lr.fillna(0.0)

    proxy_level = float(base_level) * np.exp(proxy_lr.cumsum().to_numpy(dtype=float))
    out = pd.DataFrame({"trade_date": proxy_lr.index, "close": proxy_level})
    return out


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Compute a regime hazard signals CSV (down_risk/up_risk) from a synthetic market proxy built from many instruments."
        )
    )

    parser.add_argument("--market-suffix", type=str, default="US", help="Instrument_id suffix filter (default: US)")

    parser.add_argument("--start", type=_parse_date, required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=_parse_date, required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--output", type=str, required=True, help="Output CSV path")

    # Proxy construction.
    parser.add_argument(
        "--selector-lookback-calendar-days",
        type=int,
        default=900,
        help="Calendar-day window *before* start used to select basket instruments (default: 900)",
    )
    parser.add_argument(
        "--instrument-limit",
        type=int,
        default=500,
        help="Number of instruments to include in the proxy basket (default: 500)",
    )
    parser.add_argument(
        "--selector",
        type=str,
        choices=["volume_sum", "volume_avg", "count"],
        default="volume_sum",
        help="How to rank/select instruments in the pre-start window (default: volume_sum)",
    )
    parser.add_argument(
        "--min-obs",
        type=int,
        default=200,
        help="Minimum number of price rows required in the selection window (default: 200)",
    )
    parser.add_argument(
        "--agg",
        type=str,
        choices=["median", "mean"],
        default="median",
        help="Cross-sectional aggregation of daily returns (default: median)",
    )
    parser.add_argument(
        "--winsor-pct",
        type=float,
        nargs=2,
        default=[1.0, 99.0],
        metavar=("P_LOW", "P_HIGH"),
        help="Winsorize daily cross-section of log returns (default: 1 99)",
    )
    parser.add_argument(
        "--base-level",
        type=float,
        default=100.0,
        help="Starting level for synthetic proxy series (default: 100)",
    )

    parser.add_argument(
        "--price-col",
        type=str,
        default="adjusted_close",
        choices=["close", "adjusted_close"],
        help="Price column to use when building proxy (default: adjusted_close)",
    )

    parser.add_argument(
        "--lookback-calendar-days",
        type=int,
        default=900,
        help="Calendar-day lookback buffer for hazard feature windows (default: 900)",
    )

    # Hazard config (same defaults as RegimeHazardConfig).
    parser.add_argument("--vol-window-days", type=int, default=21)
    parser.add_argument("--mom-window-days", type=int, default=63)
    parser.add_argument("--dd-window-days", type=int, default=252)

    parser.add_argument("--vol-low", type=float, default=0.01)
    parser.add_argument("--vol-high", type=float, default=0.03)
    parser.add_argument("--mom-scale", type=float, default=0.10)
    parser.add_argument("--dd-scale", type=float, default=0.15)

    parser.add_argument("--w-vol-down", type=float, default=0.5)
    parser.add_argument("--w-mom-down", type=float, default=0.3)
    parser.add_argument("--w-dd-down", type=float, default=0.2)
    parser.add_argument("--w-mom-up", type=float, default=0.7)
    parser.add_argument("--w-vol-up", type=float, default=0.3)


    # Optional evaluation labels.
    parser.add_argument("--label-horizon-days", type=int, default=63)
    parser.add_argument("--label-dd-threshold", type=float, default=0.15)
    parser.add_argument("--label-up-return-threshold", type=float, default=0.10)
    parser.add_argument("--with-labels", action="store_true")

    args = parser.parse_args(argv)

    if args.end < args.start:
        parser.error("--end must be >= --start")

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Instrument selection uses only the pre-start window to avoid selection
    # bias / leakage.
    selector_end = args.start - timedelta(days=1)
    selector_start = selector_end - timedelta(days=int(args.selector_lookback_calendar_days))

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

    instrument_ids = _select_instruments(
        db_manager=db_manager,
        start_date=selector_start,
        end_date=selector_end,
        instrument_limit=int(args.instrument_limit),
        min_obs=int(args.min_obs),
        selector=str(args.selector),
        market_suffix=str(args.market_suffix) if args.market_suffix else None,
    )

    if not instrument_ids:
        raise SystemExit(
            "No instruments selected for proxy basket. Try increasing selector-lookback, "
            "decreasing min-obs, or removing market-suffix."
        )

    logger.info(
        "Selected %d instruments for proxy basket using selector=%s window=%s..%s",
        len(instrument_ids),
        args.selector,
        selector_start,
        selector_end,
    )

    df_prices = reader.read_prices_close(
        instrument_ids,
        start_buf,
        args.end,
        price_col=str(args.price_col),
    )

    if df_prices.empty:
        raise SystemExit("No price data returned for selected instruments")

    winsor = (float(args.winsor_pct[0]), float(args.winsor_pct[1])) if args.winsor_pct else None
    df_proxy = _build_proxy_series(
        df_prices,
        agg=str(args.agg),
        winsor_pct=winsor,
        base_level=float(args.base_level),
    )

    if df_proxy.empty:
        raise SystemExit("Proxy series construction produced no rows")

    # Compute trailing features + hazard scores on proxy.
    df_feat = compute_proxy_features(df_prices=df_proxy, price_col="close", cfg=cfg)
    df_scores = compute_hazard_scores(df_features=df_feat, cfg=cfg)

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

    df_out = df_out.reset_index().rename(columns={"trade_date": "as_of_date"})

    cols_first = ["as_of_date", "down_risk", "up_risk"]
    cols_rest = [c for c in df_out.columns if c not in cols_first]
    df_out = df_out[cols_first + cols_rest]

    df_out.to_csv(out_path, index=False)

    logger.info(
        "Wrote market proxy hazard signals: rows=%d start=%s end=%s -> %s",
        df_out.shape[0],
        args.start,
        args.end,
        out_path,
    )


if __name__ == "__main__":  # pragma: no cover
    main()
