"""Evaluate regime label quality against forward outcomes.

This script is a lightweight *historical validation harness* for the
runtime RegimeEngine output stored in the `regimes` table.

It:
- loads daily regime labels for a region from `RegimeStorage`,
- loads a synthetic market proxy close series (and hazard signals) from the
  cached overlay CSV (see `prometheus.regime.overlay_cache`),
- computes forward return and forward max drawdown over selected horizons,
- reports summary statistics by regime label and basic transition counts.

Decision timing convention:
- regime labels are assumed computed using close[t] data
- forward outcomes are measured from close[t] -> close[t+h]

This is intended for validation/tuning, not for live decision making.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from apathis.core.database import get_db_manager
from apathis.core.markets import MARKETS_BY_REGION
from apathis.regime.overlay_cache import ensure_overlay_csv
from apathis.regime.storage import RegimeStorage
from apathis.regime.types import RegimeLabel


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except Exception as exc:  # pragma: no cover - CLI validation
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


def _compute_forward_metrics(
    closes: np.ndarray,
    *,
    horizon_days: int,
) -> tuple[np.ndarray, np.ndarray]:
    """Return (fwd_return, fwd_max_dd) arrays for a close series.

    fwd_return[t] = close[t+h]/close[t]-1

    fwd_max_dd[t] is the minimum drawdown within the window [t..t+h]
    relative to the running peak within that window.
    """

    n = int(len(closes))
    h = int(horizon_days)

    fwd_ret = np.full(n, np.nan, dtype=float)
    fwd_dd = np.full(n, np.nan, dtype=float)

    if n <= 0 or h <= 0 or n <= h:
        return fwd_ret, fwd_dd

    for i in range(0, n - h):
        start = float(closes[i])
        if not np.isfinite(start) or start <= 0.0:
            continue

        end = float(closes[i + h])
        if np.isfinite(end) and end > 0.0:
            fwd_ret[i] = end / start - 1.0

        peak = start
        max_dd = 0.0
        for j in range(i + 1, i + h + 1):
            p = float(closes[j])
            if not np.isfinite(p) or p <= 0.0:
                continue
            if p > peak:
                peak = p
            dd = p / peak - 1.0 if peak > 0.0 else 0.0
            if dd < max_dd:
                max_dd = dd
        fwd_dd[i] = max_dd

    return fwd_ret, fwd_dd


def _summarise_by_label(
    df: pd.DataFrame,
    *,
    label_col: str,
    ret_col: str,
    dd_col: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    for label in [l.value for l in RegimeLabel]:
        sub = df[df[label_col] == label]
        r = pd.to_numeric(sub[ret_col], errors="coerce")
        d = pd.to_numeric(sub[dd_col], errors="coerce")

        r_valid = r.dropna()
        d_valid = d.dropna()

        rows.append(
            {
                "regime_label": label,
                "num_obs": int(len(r_valid)),
                "fwd_return_mean": float(r_valid.mean()) if not r_valid.empty else None,
                "fwd_return_median": float(r_valid.median()) if not r_valid.empty else None,
                "fwd_return_neg_frac": float((r_valid < 0.0).mean()) if not r_valid.empty else None,
                "fwd_maxdd_mean": float(d_valid.mean()) if not d_valid.empty else None,
                "fwd_maxdd_p05": float(d_valid.quantile(0.05)) if not d_valid.empty else None,
                "fwd_maxdd_p50": float(d_valid.quantile(0.50)) if not d_valid.empty else None,
            }
        )

    return rows


def _transition_stats(labels: list[str]) -> dict[str, Any]:
    if not labels:
        return {"num_days": 0, "num_transitions": 0, "by_pair": {}, "durations": {}}

    num_transitions = 0
    by_pair: Counter[str] = Counter()

    durations: dict[str, list[int]] = {}

    cur = labels[0]
    run_len = 1

    for prev, nxt in zip(labels[:-1], labels[1:]):
        if nxt == prev:
            run_len += 1
            continue

        num_transitions += 1
        by_pair[f"{prev}->{nxt}"] += 1

        durations.setdefault(prev, []).append(run_len)
        cur = nxt
        run_len = 1

    durations.setdefault(cur, []).append(run_len)

    durations_summary: dict[str, dict[str, float]] = {}
    for label, lens in durations.items():
        arr = np.asarray(lens, dtype=float)
        durations_summary[label] = {
            "episodes": float(len(lens)),
            "mean_len_days": float(arr.mean()) if arr.size else 0.0,
            "median_len_days": float(np.median(arr)) if arr.size else 0.0,
            "p10_len_days": float(np.quantile(arr, 0.10)) if arr.size else 0.0,
            "p90_len_days": float(np.quantile(arr, 0.90)) if arr.size else 0.0,
        }

    return {
        "num_days": int(len(labels)),
        "num_transitions": int(num_transitions),
        "by_pair": dict(by_pair),
        "durations": durations_summary,
    }


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Evaluate stored regime labels against forward outcomes")

    p.add_argument("--region", type=str, default="US", help="Regime region (e.g. US)")
    p.add_argument(
        "--market-id",
        type=str,
        default=None,
        help="Market id for proxy overlay selection (default: first market for region)",
    )
    p.add_argument(
        "--hazard-profile",
        type=str,
        default="DEFAULT",
        help="Hazard overlay profile name (see prometheus.regime.overlay_cache)",
    )

    p.add_argument("--start", type=_parse_date, required=True)
    p.add_argument("--end", type=_parse_date, required=True)

    p.add_argument(
        "--horizons",
        type=int,
        nargs="+",
        default=[21, 63],
        help="Forward horizons in trading days",
    )

    p.add_argument(
        "--out-dir",
        type=str,
        default=None,
        help="Optional output directory for CSV/JSON artifacts",
    )

    args = p.parse_args(argv)

    region = str(args.region).strip().upper()
    if not region:
        raise ValueError("region must be non-empty")

    start: date = args.start
    end: date = args.end
    if end < start:
        raise ValueError("end must be >= start")

    markets = MARKETS_BY_REGION.get(region, ())
    market_id = str(args.market_id).strip().upper() if args.market_id else (str(markets[0]) if markets else "US_EQ")

    horizons = [int(h) for h in args.horizons if int(h) > 0]
    horizons = sorted(set(horizons))

    db = get_db_manager()

    # Load synthetic proxy overlay CSV (includes close series).
    csv_path = ensure_overlay_csv(
        db_manager=db,
        start_date=start,
        end_date=end,
        profile_name=str(args.hazard_profile),
    )

    df = pd.read_csv(csv_path, low_memory=False)
    if df.empty or "as_of_date" not in df.columns:
        raise RuntimeError(f"Overlay CSV {csv_path} has no data")

    df["as_of_date"] = pd.to_datetime(df["as_of_date"], errors="coerce").dt.date
    df = df[df["as_of_date"].notna()].copy()
    df = df[(df["as_of_date"] >= start) & (df["as_of_date"] <= end)].copy()
    df = df.sort_values("as_of_date").reset_index(drop=True)

    if "close" not in df.columns:
        raise RuntimeError(f"Overlay CSV {csv_path} missing required column 'close'")

    # Load regime history for the region.
    storage = RegimeStorage(db_manager=db)
    regimes: list[tuple[date, RegimeLabel]] = []

    prev = storage.get_latest_regime(region, as_of_date=start, inclusive=False)
    if prev is not None:
        regimes.append((prev.as_of_date, prev.regime_label))

    for st in storage.get_history(region, start, end):
        regimes.append((st.as_of_date, st.regime_label))

    regimes.sort(key=lambda x: x[0])

    # Join regime labels onto the trading-day spine without lookahead.
    labels: list[str] = []
    idx = 0
    cur_label: RegimeLabel | None = None

    dates = df["as_of_date"].tolist()
    for d in dates:
        while idx < len(regimes) and regimes[idx][0] <= d:
            cur_label = regimes[idx][1]
            idx += 1
        labels.append(cur_label.value if cur_label is not None else RegimeLabel.NEUTRAL.value)

    df["regime_label"] = labels

    # Compute forward outcomes.
    closes = pd.to_numeric(df["close"], errors="coerce").to_numpy(dtype=float)

    for h in horizons:
        ret, dd = _compute_forward_metrics(closes, horizon_days=h)
        df[f"fwd_return_{h}d"] = ret
        df[f"fwd_maxdd_{h}d"] = dd

    # Summaries.
    transition_summary = _transition_stats(labels)

    summaries: dict[str, Any] = {
        "args": {
            "region": region,
            "market_id": market_id,
            "hazard_profile": str(args.hazard_profile),
            "start": start.isoformat(),
            "end": end.isoformat(),
            "horizons": horizons,
            "overlay_csv": str(csv_path),
        },
        "transitions": transition_summary,
        "by_horizon": {},
    }

    for h in horizons:
        rows = _summarise_by_label(
            df,
            label_col="regime_label",
            ret_col=f"fwd_return_{h}d",
            dd_col=f"fwd_maxdd_{h}d",
        )
        summaries["by_horizon"][f"{h}d"] = rows

    # Console output.
    print(json.dumps(summaries, indent=2, sort_keys=True))

    # Optional artifacts.
    if args.out_dir:
        out_dir = Path(str(args.out_dir))
        out_dir.mkdir(parents=True, exist_ok=True)

        df_out = df.copy()
        df_out.to_csv(out_dir / "regime_eval_daily.csv", index=False)
        (out_dir / "regime_eval_summary.json").write_text(json.dumps(summaries, indent=2, sort_keys=True))

        for h in horizons:
            rows = summaries["by_horizon"].get(f"{h}d", [])
            pd.DataFrame(rows).to_csv(out_dir / f"regime_eval_summary_{h}d.csv", index=False)


if __name__ == "__main__":  # pragma: no cover
    main()
