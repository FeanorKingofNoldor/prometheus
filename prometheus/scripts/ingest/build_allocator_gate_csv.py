"""Build a hedge gate CSV (date,breadth_flag,credit_flag) for the allocator.

Breadth flag proxy:
    - Uses SPY daily closes from EODHD.
    - breadth_flag = 1 when close < 200d SMA AND 50d SMA < 200d SMA.

Credit flag:
    - Uses FRED series BAMLH0A0HYM2 (USD HY OAS).
    - credit_flag = 1 when spread >= credit_threshold OR
      20d change >= credit_delta_threshold.

Outputs a CSV sorted by date with integer flags.
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests
from apatheon.core.logging import get_logger
from apatheon.data_ingestion.eodhd_client import EodhdClient

logger = get_logger(__name__)


FRED_SERIES_ID = "BAMLH0A0HYM2"  # ICE BofA US High Yield Option-Adjusted Spread


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def fetch_spy_prices(client: EodhdClient, start: date, end: date) -> pd.DataFrame:
    bars = client.get_eod_prices("SPY.US", start_date=start, end_date=end, adjusted=True)
    df = pd.DataFrame(
        {
            "date": [b.trade_date for b in bars],
            "close": [b.adjusted_close for b in bars],
        }
    )
    df = df.sort_values("date").reset_index(drop=True)
    return df


def fetch_fred_series(api_key: str, start: date, end: date) -> pd.DataFrame:
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "api_key": api_key,
        "series_id": FRED_SERIES_ID,
        "file_type": "json",
        "observation_start": start.isoformat(),
        "observation_end": end.isoformat(),
    }
    resp = requests.get(url, params=params, timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(f"FRED request failed status={resp.status_code} body={resp.text[:200]}")
    payload = resp.json()
    observations = payload.get("observations", [])
    rows = []
    for obs in observations:
        ds = obs.get("date")
        val = obs.get("value")
        if ds is None or val in (".", None):
            continue
        try:
            d = _parse_date(ds)
            v = float(val)
        except Exception:
            continue
        rows.append((d, v))
    df = pd.DataFrame(rows, columns=["date", "hy_oas"])
    df = df.sort_values("date").reset_index(drop=True)
    return df


@dataclass(frozen=True)
class GateParams:
    breadth_window_short: int = 50
    breadth_window_long: int = 200
    credit_threshold: float = 6.0
    credit_delta_window: int = 20
    credit_delta_threshold: float = 0.75


def build_gates(
    spy_df: pd.DataFrame,
    hy_df: pd.DataFrame,
    params: GateParams,
) -> pd.DataFrame:
    df = spy_df.copy()
    df["close"] = df["close"].astype(float)
    df[f"sma_{params.breadth_window_short}"] = df["close"].rolling(
        params.breadth_window_short, min_periods=params.breadth_window_short
    ).mean()
    df[f"sma_{params.breadth_window_long}"] = df["close"].rolling(
        params.breadth_window_long, min_periods=params.breadth_window_long
    ).mean()
    df["breadth_flag"] = (
        (df["close"] < df[f"sma_{params.breadth_window_long}"])
        & (df[f"sma_{params.breadth_window_short}"] < df[f"sma_{params.breadth_window_long}"])
    ).astype(int)

    hy_df = hy_df.copy()
    hy_df["hy_oas"] = hy_df["hy_oas"].astype(float)
    hy_df["hy_oas_delta"] = hy_df["hy_oas"].diff(params.credit_delta_window)
    hy_df["credit_flag"] = (
        (hy_df["hy_oas"] >= params.credit_threshold)
        | (hy_df["hy_oas_delta"] >= params.credit_delta_threshold)
    ).astype(int)

    merged = pd.merge(df[["date", "breadth_flag"]], hy_df[["date", "credit_flag"]], on="date", how="left")
    merged = merged.sort_values("date").reset_index(drop=True)
    merged["credit_flag"] = merged["credit_flag"].ffill().fillna(0).astype(int)
    merged["breadth_flag"] = merged["breadth_flag"].fillna(0).astype(int)
    return merged[["date", "breadth_flag", "credit_flag"]]


def write_csv(out_path: Path, rows: Iterable[tuple[date, int, int]]) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        f.write("date,breadth_flag,credit_flag\n")
        for d, b, c in rows:
            f.write(f"{d.isoformat()},{int(b)},{int(c)}\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build allocator hedge gate CSV from EODHD + FRED.")
    parser.add_argument("--start", required=True, type=_parse_date, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, type=_parse_date, help="End date YYYY-MM-DD")
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("data/gates/allocator_gates.csv"),
        help="Output CSV path (default: data/gates/allocator_gates.csv)",
    )
    parser.add_argument("--credit-threshold", type=float, default=6.0, help="HY OAS level to trigger credit_flag (default 6.0)")
    parser.add_argument(
        "--credit-delta-threshold",
        type=float,
        default=0.75,
        help="HY OAS change over delta window to trigger credit_flag (default 0.75)",
    )
    parser.add_argument(
        "--credit-delta-window",
        type=int,
        default=20,
        help="Window (days) for HY OAS change (default 20).",
    )

    args = parser.parse_args()

    eodhd_key = os.getenv("EODHD_API_KEY")
    fred_key = os.getenv("FRED_API_KEY")
    if not eodhd_key:
        raise SystemExit("EODHD_API_KEY not set")
    if not fred_key:
        raise SystemExit("FRED_API_KEY not set")

    params = GateParams(
        credit_threshold=float(args.credit_threshold),
        credit_delta_window=int(args.credit_delta_window),
        credit_delta_threshold=float(args.credit_delta_threshold),
    )

    logger.info("Fetching SPY prices from EODHD")
    client = EodhdClient(api_token=eodhd_key)
    try:
        spy_df = fetch_spy_prices(client, args.start, args.end)
    finally:
        client.close()

    logger.info("Fetching HY OAS from FRED series %s", FRED_SERIES_ID)
    hy_df = fetch_fred_series(fred_key, args.start, args.end)

    logger.info("Computing gate flags")
    merged = build_gates(spy_df, hy_df, params)

    logger.info("Writing %d rows to %s", len(merged), args.out)
    write_csv(args.out, merged.itertuples(index=False, name=None))


if __name__ == "__main__":
    main()
