"""Fetch FRED series to local CSV files.

Writes graph-style CSVs compatible with data_sources.load_fred_series:
  observation_date,SERIES_ID
  YYYY-MM-DD,value

Reads FRED_API_KEY from .env.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from urllib.request import urlopen

import pandas as pd
from dotenv import dotenv_values

DEFAULT_SERIES = [
    "DGS2",
    "DGS10",
    "DGS3MO",
    "DFII10",
    "VIXCLS",
    "FEDFUNDS",
    "UNRATE",
    "ICSA",
    "STLFSI2",
]


def _fetch_series(series_id: str, api_key: str) -> pd.DataFrame:
    url = (
        "https://api.stlouisfed.org/fred/series/observations"
        f"?series_id={series_id}&api_key={api_key}&file_type=json"
    )
    with urlopen(url) as resp:
        payload = json.load(resp)
    obs = payload.get("observations", [])
    df = pd.DataFrame(obs)
    if df.empty:
        raise RuntimeError(f"No observations returned for {series_id}")
    if "date" not in df.columns or "value" not in df.columns:
        raise RuntimeError(f"Unexpected columns for {series_id}: {list(df.columns)}")
    out = pd.DataFrame({
        "observation_date": df["date"],
        series_id: df["value"],
    })
    return out


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--out-dir", type=str, default="data/fred")
    p.add_argument("--series", type=str, nargs="*", default=DEFAULT_SERIES)
    args = p.parse_args()

    env = dotenv_values(".env")
    api_key = env.get("FRED_API_KEY")
    if not api_key:
        raise SystemExit("FRED_API_KEY missing from .env")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    for sid in args.series:
        df = _fetch_series(sid, api_key)
        out_path = out_dir / f"{sid.lower()}.csv"
        df.to_csv(out_path, index=False)
        print(f"wrote {out_path} rows={len(df)}")


if __name__ == "__main__":
    main()
