"""Build a C++-compatible lambda score table from raw multihorizon lambda.

Inputs
------
The expected input is the output of
`prometheus.scripts.backfill.backfill_opportunity_density_multihorizon`,
which contains (at minimum):

- as_of_date
- market_id
- sector
- soft_target_class
- lambda_h{H} columns (e.g. lambda_h5, lambda_h21, lambda_h63)

Output
------
The output CSV must match the C++ LambdaScoreTable loader contract
(see `cpp/include/prom2/engines/lambda_scores.hpp`):

  as_of_date, market_id, sector, soft_target_class, lambda_value,
  lambda_score_h5, lambda_score_h21, lambda_score_h63

Notes
-----
- C++ loads by *column position*, not header names.
- Python CsvLambdaClusterScoreProvider uses header names.

Transform
---------
Default pipeline (configurable via CLI):

1) transform raw lambda via log1p
2) winsorize per-horizon using global percentiles
3) normalize cross-sectionally per-date with robust z-score (median/MAD)
4) optional EWMA smoothing per cluster over time

The resulting lambda_score_h{H} values are meant to be used as a relative
ranking signal (higher = more opportunity).
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import numpy as np
import pandas as pd


_VALID_SOFT_TARGET = {"STABLE", "WATCH", "FRAGILE", "TARGETABLE", "BREAKER"}


def _parse_date_col(df: pd.DataFrame, col: str) -> None:
    if col not in df.columns:
        raise ValueError(f"Missing required column: {col}")
    df[col] = pd.to_datetime(df[col]).dt.date


def _robust_z_by_date(df: pd.DataFrame, value_col: str, group_cols: List[str]) -> pd.Series:
    """Robust z-score within each group using median / MAD.

    This must return a 1D Series aligned to ``df.index``.
    """

    def _one(x: pd.Series) -> pd.Series:
        x = x.astype(float)
        med = float(x.median())
        mad = float((x - med).abs().median())
        # Consistent with normal distribution: MAD * 1.4826 ~ sigma.
        scale = 1.4826 * mad
        if not np.isfinite(scale) or scale <= 0.0:
            return pd.Series(np.zeros(len(x), dtype=float), index=x.index)
        return (x - med) / scale

    return df.groupby(group_cols, group_keys=False)[value_col].transform(_one)


def _winsorize(series: pd.Series, lo: float, hi: float) -> tuple[pd.Series, Dict[str, float]]:
    x = series.astype(float)
    q_lo = float(np.nanpercentile(x.to_numpy(), lo))
    q_hi = float(np.nanpercentile(x.to_numpy(), hi))
    out = x.clip(lower=q_lo, upper=q_hi)
    return out, {"p_lo": float(lo), "p_hi": float(hi), "q_lo": q_lo, "q_hi": q_hi}


@dataclass(frozen=True)
class _Cfg:
    horizons: List[int]
    transform: str
    winsor_lo: float
    winsor_hi: float
    normalize: str
    smoothing: str
    ewma_span: int


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Build lambda score table CSV for C++ and Python")

    parser.add_argument("--input", type=str, required=True, help="Raw multihorizon lambda CSV")
    parser.add_argument("--output", type=str, required=True, help="Output lambda score table CSV")
    parser.add_argument("--horizons", type=int, nargs="+", default=[5, 21, 63])

    parser.add_argument(
        "--transform",
        type=str,
        choices=["none", "log1p"],
        default="log1p",
        help="Transform applied to raw lambda_h{H} before normalization (default: log1p)",
    )
    parser.add_argument(
        "--winsor-pct",
        type=float,
        nargs=2,
        default=[1.0, 99.0],
        metavar=("P_LOW", "P_HIGH"),
        help="Winsorization percentiles (default: 1 99)",
    )
    parser.add_argument(
        "--normalize",
        type=str,
        choices=["none", "per-date-robust-z"],
        default="per-date-robust-z",
        help="Normalization strategy (default: per-date-robust-z)",
    )
    parser.add_argument(
        "--smoothing",
        type=str,
        choices=["none", "ewma"],
        default="ewma",
        help="Optional smoothing over time per cluster (default: ewma)",
    )
    parser.add_argument(
        "--ewma-span",
        type=int,
        default=20,
        help="EWMA span used when --smoothing=ewma (default: 20)",
    )

    parser.add_argument("--report-out", type=str, default=None, help="Optional JSON report path")

    parser.add_argument("--strict", action="store_true", help="Fail if coverage gates are not met")
    parser.add_argument("--max-unknown-frac", type=float, default=0.01)
    parser.add_argument("--min-score-nonnull-frac", type=float, default=0.99)

    args = parser.parse_args(argv)

    horizons = sorted(set(int(h) for h in args.horizons))
    if any(h <= 0 for h in horizons):
        parser.error("--horizons must be positive")

    p_lo, p_hi = float(args.winsor_pct[0]), float(args.winsor_pct[1])
    if not (0.0 <= p_lo < p_hi <= 100.0):
        parser.error("--winsor-pct must satisfy 0 <= P_LOW < P_HIGH <= 100")

    cfg = _Cfg(
        horizons=horizons,
        transform=str(args.transform),
        winsor_lo=p_lo,
        winsor_hi=p_hi,
        normalize=str(args.normalize),
        smoothing=str(args.smoothing),
        ewma_span=int(args.ewma_span),
    )

    in_path = Path(str(args.input))
    out_path = Path(str(args.output))
    out_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(in_path)
    rows_in = int(df.shape[0])
    _parse_date_col(df, "as_of_date")

    required_base = {"as_of_date", "market_id", "sector", "soft_target_class"}
    missing_base = required_base - set(df.columns)
    if missing_base:
        raise SystemExit(f"Input CSV missing required columns: {sorted(missing_base)}")

    # Ensure horizon columns exist.
    for h in horizons:
        col = f"lambda_h{h}"
        if col not in df.columns:
            raise SystemExit(f"Input CSV missing required column: {col}")

    # Track UNKNOWN fraction in input.
    # Prefer instrument-weighted UNKNOWN fraction when num_instruments is available.
    soft = df["soft_target_class"].astype(str)
    unknown = soft == "UNKNOWN"

    unknown_frac_in_row = float(unknown.mean()) if len(df) else 1.0

    unknown_frac_in_weighted = unknown_frac_in_row
    if len(df) and "num_instruments" in df.columns:
        w = pd.to_numeric(df["num_instruments"], errors="coerce").fillna(0.0)
        total = float(w.sum())
        if total > 0:
            unknown_frac_in_weighted = float(w[unknown].sum() / total)

    # Use weighted (when available) for gating/reporting.
    unknown_frac_in = float(unknown_frac_in_weighted)

    # Drop UNKNOWN rows in output (Universe/STAB has no UNKNOWN class).
    df = df[soft.isin(_VALID_SOFT_TARGET)].copy()

    # Build transformed score columns.
    report: Dict[str, Any] = {
        "input": str(in_path),
        "output": str(out_path),
        "horizons": horizons,
        "transform": cfg.transform,
        "winsor_pct": [cfg.winsor_lo, cfg.winsor_hi],
        "normalize": cfg.normalize,
        "smoothing": cfg.smoothing,
        "ewma_span": cfg.ewma_span,
        "input_unknown_frac": unknown_frac_in,
        "input_unknown_frac_row": unknown_frac_in_row,
        "input_unknown_frac_weighted": float(unknown_frac_in_weighted),
    }

    winsor_stats: Dict[str, Dict[str, float]] = {}

    # If nothing remains after dropping UNKNOWN, still write an empty table
    # with the correct schema so downstream steps can fail fast.
    if df.empty:
        out_cols = [
            "as_of_date",
            "market_id",
            "sector",
            "soft_target_class",
            "lambda_value",
        ] + [f"lambda_score_h{h}" for h in horizons]

        out_df = pd.DataFrame(columns=out_cols)

        report["winsor_stats"] = winsor_stats
        report["rows_in"] = rows_in
        report["rows_out"] = 0
        report["output_unknown_frac"] = 1.0
        report["output_nonnull_fracs"] = {f"lambda_score_h{h}": 0.0 for h in horizons}

        gate_ok = False
        gate_reasons: List[str] = []
        gate_reasons.append("no rows remain after filtering UNKNOWN soft_target_class")
        if unknown_frac_in > float(args.max_unknown_frac):
            if unknown_frac_in_row != unknown_frac_in:
                gate_reasons.append(
                    f"input_UNKNOWN_frac(weighted)={unknown_frac_in:.4%} (row={unknown_frac_in_row:.4%}) > {float(args.max_unknown_frac):.2%}"
                )
            else:
                gate_reasons.append(
                    f"input_UNKNOWN_frac={unknown_frac_in:.4%} > {float(args.max_unknown_frac):.2%}"
                )

        report["gate_passed"] = False
        report["gate_reasons"] = gate_reasons

        out_df.to_csv(out_path, index=False)
        if args.report_out:
            rep_path = Path(str(args.report_out))
            rep_path.parent.mkdir(parents=True, exist_ok=True)
            rep_path.write_text(json.dumps(report, indent=2, sort_keys=False), encoding="utf-8")

        if args.strict:
            raise SystemExit("; ".join(gate_reasons))
        return

    for h in horizons:
        raw_col = f"lambda_h{h}"
        work = df[raw_col].astype(float)

        if cfg.transform == "log1p":
            work = np.log1p(work)
        elif cfg.transform == "none":
            pass
        else:  # pragma: no cover
            raise SystemExit(f"Unknown transform {cfg.transform!r}")

        work, ws = _winsorize(work, cfg.winsor_lo, cfg.winsor_hi)
        winsor_stats[raw_col] = ws

        tmp_col = f"__tmp_{raw_col}"
        df[tmp_col] = work

        if cfg.normalize == "per-date-robust-z":
            z = _robust_z_by_date(df, tmp_col, ["as_of_date"])
        elif cfg.normalize == "none":
            z = df[tmp_col].astype(float)
        else:  # pragma: no cover
            raise SystemExit(f"Unknown normalize {cfg.normalize!r}")

        out_col = f"lambda_score_h{h}"
        df[out_col] = z.astype(float)

        df.drop(columns=[tmp_col], inplace=True)

    # Optional smoothing per cluster (market_id, sector, soft_target_class).
    if cfg.smoothing == "ewma":
        if cfg.ewma_span <= 1:
            raise SystemExit("--ewma-span must be > 1")

        df.sort_values(["market_id", "sector", "soft_target_class", "as_of_date"], inplace=True)
        group_cols = ["market_id", "sector", "soft_target_class"]
        for h in horizons:
            col = f"lambda_score_h{h}"
            df[col] = (
                df.groupby(group_cols, group_keys=False)[col]
                .apply(lambda s: s.ewm(span=cfg.ewma_span, adjust=False).mean())
                .astype(float)
            )
    elif cfg.smoothing == "none":
        pass
    else:  # pragma: no cover
        raise SystemExit(f"Unknown smoothing {cfg.smoothing!r}")

    # lambda_value is not used by C++ backtests, but keep a stable meaning.
    # We define it as the raw lambda for the median horizon (or first).
    h_ref = 21 if 21 in horizons else horizons[0]
    df["lambda_value"] = df[f"lambda_h{h_ref}"].astype(float)

    # Output schema (fixed order for C++ loader).
    out_cols = [
        "as_of_date",
        "market_id",
        "sector",
        "soft_target_class",
        "lambda_value",
    ] + [f"lambda_score_h{h}" for h in horizons]

    out_df = df[out_cols].copy()

    # Coverage gates.
    unknown_frac_out = float((out_df["soft_target_class"].astype(str) == "UNKNOWN").mean()) if len(out_df) else 1.0
    nonnull_fracs = {
        c: float(out_df[c].notna().mean()) for c in out_df.columns if c.startswith("lambda_score_h")
    }

    report["winsor_stats"] = winsor_stats
    report["rows_in"] = rows_in
    report["rows_out"] = int(len(out_df))
    report["output_unknown_frac"] = unknown_frac_out
    report["output_nonnull_fracs"] = nonnull_fracs

    gate_ok = True
    gate_reasons: List[str] = []

    if unknown_frac_in > float(args.max_unknown_frac):
        gate_ok = False
        if unknown_frac_in_row != unknown_frac_in:
            gate_reasons.append(
                f"input_UNKNOWN_frac(weighted)={unknown_frac_in:.4%} (row={unknown_frac_in_row:.4%}) > {float(args.max_unknown_frac):.2%}"
            )
        else:
            gate_reasons.append(
                f"input_UNKNOWN_frac={unknown_frac_in:.4%} > {float(args.max_unknown_frac):.2%}"
            )

    if unknown_frac_out > float(args.max_unknown_frac):
        gate_ok = False
        gate_reasons.append(
            f"output_UNKNOWN_frac={unknown_frac_out:.4%} > {float(args.max_unknown_frac):.2%}"
        )

    for col, frac in nonnull_fracs.items():
        if frac < float(args.min_score_nonnull_frac):
            gate_ok = False
            gate_reasons.append(
                f"{col}_nonnull={frac:.4%} < {float(args.min_score_nonnull_frac):.2%}"
            )

    report["gate_passed"] = bool(gate_ok)
    report["gate_reasons"] = gate_reasons

    # Write output.
    out_df.to_csv(out_path, index=False)

    if args.report_out:
        rep_path = Path(str(args.report_out))
        rep_path.parent.mkdir(parents=True, exist_ok=True)
        rep_path.write_text(json.dumps(report, indent=2, sort_keys=False), encoding="utf-8")

    if args.strict and not gate_ok:
        raise SystemExit("; ".join(gate_reasons) if gate_reasons else "lambda score table gate failed")


if __name__ == "__main__":  # pragma: no cover
    main()
