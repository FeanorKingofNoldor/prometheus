"""Analyze issuer×day NEWS/returns panel for lead–lag signal.

This script operates on the CSV exported by
`prometheus.scripts.export_news_lead_lag_panel` and computes:

- Basic sanity checks (row count, date range, number of instruments/issuers).
- Cross-sectional Pearson correlations between each NEWS factor and each
  forward-return horizon.
- Simple decile sorts: for each factor and horizon, it reports the average
  forward return per factor decile.

It is intentionally implemented using only the Python standard library so it
can run in lightweight environments without additional dependencies.

Example usage::

    python -m prometheus.scripts.analyze_news_lead_lag_panel \
      --input data/news_lead_lag_US_EQ_2015_2024.csv
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import date
from math import sqrt
from typing import Dict, List, Sequence, Tuple


@dataclass
class FactorStats:
    """Hold raw samples for a factor/horizon pair.

    We store (x, y) pairs where x is the factor value and y is the
    corresponding forward return for a given horizon.
    """

    xs: List[float]
    ys: List[float]


def _parse_date(value: str) -> date:
    year, month, day = map(int, value.split("-"))
    return date(year, month, day)


def _compute_correlation(xs: Sequence[float], ys: Sequence[float]) -> float:
    """Compute Pearson correlation between two equal-length sequences.

    Returns 0.0 if there are fewer than 2 observations or zero variance.
    """

    n = len(xs)
    if n < 2 or n != len(ys):
        return 0.0

    mean_x = sum(xs) / n
    mean_y = sum(ys) / n

    num = 0.0
    denom_x = 0.0
    denom_y = 0.0
    for x, y in zip(xs, ys):
        dx = x - mean_x
        dy = y - mean_y
        num += dx * dy
        denom_x += dx * dx
        denom_y += dy * dy

    if denom_x <= 0.0 or denom_y <= 0.0:
        return 0.0

    return num / sqrt(denom_x * denom_y)


def _compute_deciles(xs: Sequence[float], ys: Sequence[float], num_deciles: int = 10) -> List[Tuple[float, float, float]]:
    """Return (p_low, p_high, mean_y) for each decile.

    - xs, ys: aligned factor and forward-return values.
    - num_deciles: typically 10.

    The function sorts by x and then chunks into deciles (last decile may
    have slightly different size).
    """

    n = len(xs)
    if n == 0 or n != len(ys) or num_deciles <= 0:
        return []

    pairs = sorted(zip(xs, ys), key=lambda t: t[0])
    base = n // num_deciles
    remainder = n % num_deciles

    results: List[Tuple[float, float, float]] = []
    idx = 0
    for d in range(num_deciles):
        # Distribute remainder across the first few deciles.
        size = base + (1 if d < remainder else 0)
        if size == 0:
            continue
        chunk = pairs[idx : idx + size]
        idx += size
        xs_chunk = [x for x, _ in chunk]
        ys_chunk = [y for _, y in chunk]
        p_low = xs_chunk[0]
        p_high = xs_chunk[-1]
        mean_y = sum(ys_chunk) / float(len(ys_chunk))
        results.append((p_low, p_high, mean_y))

    return results


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Analyze NEWS/returns lead–lag panel CSV.")
    parser.add_argument("--input", required=True, help="Path to CSV exported by export_news_lead_lag_panel")
    args = parser.parse_args(argv)

    path = args.input

    with open(path, "r", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)

        # Identify columns by name.
        try:
            idx_instrument = header.index("instrument_id")
            idx_issuer = header.index("issuer_id")
            idx_date = header.index("as_of_date")
            header.index("ret_1d")
        except ValueError as exc:
            raise SystemExit(f"Missing expected columns in CSV header: {exc}") from exc

        horizon_cols = [c for c in header if c.startswith("fwd_ret_")]
        factor_names = [
            c
            for c in header
            if c
            not in {"instrument_id", "issuer_id", "as_of_date", "ret_1d"}
            and not c.startswith("fwd_ret_")
        ]

        # Pre-allocate stats structures.
        stats: Dict[Tuple[str, str], FactorStats] = {}
        for factor in factor_names:
            for h_col in horizon_cols:
                stats[(factor, h_col)] = FactorStats(xs=[], ys=[])

        n_rows = 0
        instruments: set[str] = set()
        issuers: set[str] = set()
        first_date: date | None = None
        last_date: date | None = None

        # Main pass over rows.
        for row in reader:
            n_rows += 1
            inst = row[idx_instrument]
            issuer = row[idx_issuer]
            dt = _parse_date(row[idx_date])
            instruments.add(inst)
            issuers.add(issuer)
            if first_date is None or dt < first_date:
                first_date = dt
            if last_date is None or dt > last_date:
                last_date = dt

            # Parse factor and horizon values once per row.
            values: Dict[str, float] = {}
            for name in factor_names + horizon_cols:
                try:
                    v = float(row[header.index(name)])
                except (ValueError, IndexError):
                    v = 0.0
                values[name] = v

            for factor in factor_names:
                x = values[factor]
                for h_col in horizon_cols:
                    y = values[h_col]
                    stats[(factor, h_col)].xs.append(x)
                    stats[(factor, h_col)].ys.append(y)

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------
    print("=== Panel summary ===")
    print(f"Rows:        {n_rows}")
    print(f"Instruments: {len(instruments)}")
    print(f"Issuers:     {len(issuers)}")
    if first_date and last_date:
        print(f"Dates:       {first_date} → {last_date}")
    print(f"Factors:     {factor_names}")
    print(f"Horizons:    {horizon_cols}")

    print("\n=== Pearson correlations (factor vs horizon) ===")
    for factor in factor_names:
        for h_col in horizon_cols:
            s = stats[(factor, h_col)]
            corr = _compute_correlation(s.xs, s.ys)
            print(f"{factor:24s} vs {h_col:10s}: {corr:+.4f}")

    print("\n=== Decile sorts: mean forward return per factor decile ===")
    for factor in factor_names:
        print(f"\nFactor: {factor}")
        for h_col in horizon_cols:
            s = stats[(factor, h_col)]
            deciles = _compute_deciles(s.xs, s.ys, num_deciles=10)
            print(f"  Horizon: {h_col}")
            for i, (p_low, p_high, mean_y) in enumerate(deciles, start=1):
                print(
                    f"    D{i:02d}: x∈[{p_low:.4f}, {p_high:.4f}] → mean({h_col})={mean_y:+.6f}",
                )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
