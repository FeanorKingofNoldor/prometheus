"""Run a suite of regime transition research variants and aggregate summaries.

This is a convenience wrapper around `run_regime_transition_research.py`.
It executes multiple variants (base / macro / cftc / macro_cftc) into
subdirectories and writes a single CSV/JSON summary for quick comparison.

Research-only; provider-only.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

import pandas as pd


def _run(cmd: list[str]) -> None:
    print(" ".join(cmd))
    subprocess.run(cmd, check=True)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--out-root", type=str, required=True)
    p.add_argument("--train-years", type=int, default=5)
    p.add_argument("--test-days", type=int, default=252)
    p.add_argument("--step-days", type=int, default=252)
    p.add_argument("--feature-mode", choices=["raw", "derived", "both"], default="derived")
    p.add_argument("--target-burden", type=float, default=0.1)
    p.add_argument("--threshold-calib-days", type=int, default=252)
    p.add_argument("--threshold-target", choices=["all", "neg"], default="all")
    p.add_argument("--model", choices=["gb", "logreg"], default="gb")
    p.add_argument("--include-proto-sim", action="store_true")
    p.add_argument("--include-pcr", action="store_true")
    p.add_argument("--no-include-pcr", dest="include_pcr", action="store_false")
    p.set_defaults(include_pcr=True)

    args = p.parse_args()

    out_root = Path(args.out_root)
    out_root.mkdir(parents=True, exist_ok=True)

    runner = Path("prometheus/scripts/analysis/run_regime_transition_research.py")
    base_cmd = [
        sys.executable,
        str(runner),
        "--train-years",
        str(args.train_years),
        "--test-days",
        str(args.test_days),
        "--step-days",
        str(args.step_days),
        "--feature-mode",
        str(args.feature_mode),
        "--target-burden",
        str(args.target_burden),
        "--threshold-calib-days",
        str(args.threshold_calib_days),
        "--threshold-target",
        str(args.threshold_target),
        "--model",
        str(args.model),
    ]

    if not args.include_pcr:
        base_cmd.append("--no-include-pcr")

    if args.include_proto_sim:
        base_cmd.append("--include-proto-sim")

    variants = {
        "base": ["--no-macro"],
        "macro": [],
        "cftc": ["--include-cftc", "--no-macro"],
        "macro_cftc": ["--include-cftc"],
    }

    summaries: list[dict] = []

    for name, extra_flags in variants.items():
        out_dir = out_root / name
        cmd = base_cmd + ["--out-dir", str(out_dir)] + extra_flags
        print(f"\n=== {name} ===")
        _run(cmd)

        summary_path = out_dir / "run_summary.json"
        if not summary_path.exists():
            raise FileNotFoundError(f"Missing {summary_path}")
        summaries.append({"variant": name, **json.loads(summary_path.read_text())})

    # Flatten into a simple per-(variant,horizon) table.
    rows: list[dict] = []
    for s in summaries:
        variant = s["variant"]
        args_meta = s.get("args", {})
        dataset = s.get("dataset", {})
        for horizon, block in (s.get("horizons", {}) or {}).items():
            wf = (block.get("walk_forward") or {})
            ev = (block.get("event_eval") or {})
            rows.append(
                {
                    "variant": variant,
                    "horizon": horizon,
                    "rows": dataset.get("rows"),
                    "rows_valid": dataset.get("rows_valid"),
                    "date_min": dataset.get("date_min"),
                    "date_max": dataset.get("date_max"),
                    "auc": wf.get("auc_mean"),
                    "pr_auc": wf.get("pr_auc_mean"),
                    "brier": wf.get("brier_mean"),
                    "burden": wf.get("alert_burden_mean"),
                    "precision": wf.get("precision_mean"),
                    "recall": wf.get("recall_mean"),
                    "lead_median": wf.get("lead_median_median"),
                    "lead_min": wf.get("lead_min_min"),
                    "threshold_median": wf.get("threshold_median"),
                    "calib_burden": wf.get("calib_alert_burden_mean"),
                    "calib_neg_burden": wf.get("calib_neg_alert_burden_mean"),
                    "events": ev.get("events"),
                    "events_with_pos_lead": ev.get("events_with_pos_lead"),
                    "event_lead_pos_median": ev.get("lead_time_pos_median"),
                    "event_pr_auc": ev.get("pr_auc_mean"),
                    "event_brier": ev.get("brier_mean"),
                    "event_burden": ev.get("alert_burden_eval_mean"),
                    "model": args_meta.get("model"),
                    "feature_mode": args_meta.get("feature_mode"),
                    "include_macro": args_meta.get("include_macro"),
                    "include_cftc": args_meta.get("include_cftc"),
                }
            )

    df = pd.DataFrame(rows)
    df = df.sort_values(["horizon", "variant"]).reset_index(drop=True)

    (out_root / "suite_summary.json").write_text(json.dumps({"runs": summaries}, indent=2, sort_keys=True))
    df.to_csv(out_root / "suite_summary.csv", index=False)

    # Print a minimal console summary.
    show_cols = [
        "variant",
        "horizon",
        "auc",
        "pr_auc",
        "brier",
        "burden",
        "precision",
        "recall",
        "events_with_pos_lead",
        "events",
    ]
    print("\n=== suite_summary ===")
    with pd.option_context("display.max_columns", 50, "display.width", 140):
        print(df[show_cols].to_string(index=False))


if __name__ == "__main__":
    main()
