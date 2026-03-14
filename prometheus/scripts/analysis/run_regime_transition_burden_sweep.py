"""Run a burden sweep for regime transition research and produce a scorecard.

This script:
1) Builds datasets once for each variant (base/macro/cftc/macro_cftc) into
   out_root/datasets/<variant>/ (using run_regime_transition_research.py).
2) Re-evaluates from the saved dataset CSV for multiple target burdens
   into out_root/runs/b<burden>/<variant>/.
3) Aggregates results into sweep_summary.csv and sweep_scorecard.csv.

Research-only; provider-only.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd


def _run(cmd: list[str]) -> None:
    print(" ".join(cmd))
    subprocess.run(cmd, check=True)


def _parse_burdens(s: str) -> list[float]:
    out: list[float] = []
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        out.append(float(part))
    return out


def _score_practical(row: dict) -> dict:
    """Practical success criteria (defaults).

    Uses per-event horizon-consistent lead times (lead_time_pos_median) and
    coverage (events_with_pos_lead / events_trainable).
    """

    horizon = row["horizon"]
    events_trainable = row.get("events_trainable")
    events_with_pos_lead = row.get("events_with_pos_lead")
    lead_pos_median = row.get("lead_time_pos_median")

    def _safe_float(x):
        try:
            return float(x)
        except Exception:
            return float("nan")

    def _safe_int(x):
        try:
            return int(x)
        except Exception:
            return 0

    et = _safe_int(events_trainable)
    ew = _safe_int(events_with_pos_lead)
    lead = _safe_float(lead_pos_median)

    capture_rate = (ew / et) if et > 0 else float("nan")

    if horizon == "21d":
        capture_min = 0.70
        lead_min = 10.0
    elif horizon == "63d":
        capture_min = 0.60
        lead_min = 30.0
    else:
        capture_min = 0.0
        lead_min = float("nan")

    passed = bool((not np.isnan(capture_rate)) and capture_rate >= capture_min and (not np.isnan(lead)) and lead >= lead_min)

    return {
        "capture_rate": capture_rate,
        "capture_min_required": capture_min,
        "lead_min_required": lead_min,
        "passed_practical": passed,
    }


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--out-root", type=str, required=True)
    p.add_argument("--burdens", type=str, default="0.05,0.1,0.2")

    p.add_argument("--train-years", type=int, default=5)
    p.add_argument("--test-days", type=int, default=252)
    p.add_argument("--step-days", type=int, default=252)
    p.add_argument("--feature-mode", choices=["raw", "derived", "both"], default="derived")

    p.add_argument("--threshold-calib-days", type=int, default=252)
    p.add_argument("--threshold-target", choices=["all", "neg"], default="all")
    p.add_argument("--threshold-mode", choices=["fixed", "rolling"], default="fixed")
    p.add_argument("--rolling-window-days", type=int, default=252)

    p.add_argument("--model", choices=["gb", "logreg"], default="logreg")
    p.add_argument("--include-proto-sim", action="store_true")
    p.add_argument("--include-pcr", action="store_true")
    p.add_argument("--no-include-pcr", dest="include_pcr", action="store_false")
    p.set_defaults(include_pcr=True)

    p.add_argument("--rebuild-datasets", action="store_true")

    args = p.parse_args()

    out_root = Path(args.out_root)
    out_root.mkdir(parents=True, exist_ok=True)

    burdens = _parse_burdens(args.burdens)
    if not burdens:
        raise ValueError("No burdens provided")

    runner = Path("prometheus/scripts/analysis/run_regime_transition_research.py")
    evaluator = Path("prometheus/scripts/analysis/eval_regime_transition_from_csv.py")

    variants = {
        "base": ["--no-macro"],
        "macro": [],
        "cftc": ["--include-cftc", "--no-macro"],
        "macro_cftc": ["--include-cftc"],
    }

    # 1) Build datasets once per variant.
    dataset_root = out_root / "datasets"
    dataset_root.mkdir(parents=True, exist_ok=True)

    for name, extra_flags in variants.items():
        ddir = dataset_root / name
        dataset_csv = ddir / "features_daily_joined.csv"
        if dataset_csv.exists() and not args.rebuild_datasets:
            continue

        ddir.mkdir(parents=True, exist_ok=True)
        cmd = [
            sys.executable,
            str(runner),
            "--out-dir",
            str(ddir),
            "--train-years",
            str(args.train_years),
            "--test-days",
            str(args.test_days),
            "--step-days",
            str(args.step_days),
            "--feature-mode",
            str(args.feature_mode),
            "--target-burden",
            str(burdens[0]),
            "--threshold-calib-days",
            str(args.threshold_calib_days),
            "--threshold-target",
            str(args.threshold_target),
            "--threshold-mode",
            str(args.threshold_mode),
            "--rolling-window-days",
            str(args.rolling_window_days),
            "--model",
            str(args.model),
        ]
        if not args.include_pcr:
            cmd.append("--no-include-pcr")
        if args.include_proto_sim:
            cmd.append("--include-proto-sim")

        cmd += extra_flags

        print(f"\n=== build_dataset: {name} ===")
        _run(cmd)

    # 2) Evaluate multiple burdens from the saved datasets.
    rows: list[dict] = []
    for b in burdens:
        btag = f"b{b:.3f}".replace(".", "p")
        for name in variants.keys():
            ddir = dataset_root / name
            dataset_csv = ddir / "features_daily_joined.csv"
            if not dataset_csv.exists():
                raise FileNotFoundError(f"Missing dataset for {name}: {dataset_csv}")

            out_dir = out_root / "runs" / btag / name
            out_dir.mkdir(parents=True, exist_ok=True)

            cmd = [
                sys.executable,
                str(evaluator),
                "--dataset",
                str(dataset_csv),
                "--out-dir",
                str(out_dir),
                "--train-years",
                str(args.train_years),
                "--test-days",
                str(args.test_days),
                "--step-days",
                str(args.step_days),
                "--feature-mode",
                str(args.feature_mode),
                "--target-burden",
                str(b),
                "--threshold-calib-days",
                str(args.threshold_calib_days),
                "--threshold-target",
                str(args.threshold_target),
                "--threshold-mode",
                str(args.threshold_mode),
                "--rolling-window-days",
                str(args.rolling_window_days),
                "--model",
                str(args.model),
            ]
            if args.include_proto_sim:
                cmd.append("--include-proto-sim")

            print(f"\n=== eval: burden={b} variant={name} ===")
            _run(cmd)

            summary_path = out_dir / "run_summary.json"
            summary = json.loads(summary_path.read_text())

            dataset_meta = summary.get("dataset", {})
            for horizon, block in (summary.get("horizons", {}) or {}).items():
                wf = block.get("walk_forward", {})
                ev = block.get("event_eval", {})
                row = {
                    "burden": b,
                    "variant": name,
                    "horizon": horizon,
                    "rows": dataset_meta.get("rows"),
                    "rows_valid": dataset_meta.get("rows_valid"),
                    "date_min": dataset_meta.get("date_min"),
                    "date_max": dataset_meta.get("date_max"),
                    "auc": wf.get("auc_mean"),
                    "pr_auc": wf.get("pr_auc_mean"),
                    "brier": wf.get("brier_mean"),
                    "burden_actual": wf.get("alert_burden_mean"),
                    "precision": wf.get("precision_mean"),
                    "recall": wf.get("recall_mean"),
                    "lead_median": wf.get("lead_median_median"),
                    "lead_min": wf.get("lead_min_min"),
                    "threshold_median": wf.get("threshold_median"),
                    "calib_burden": wf.get("calib_alert_burden_mean"),
                    "calib_neg_burden": wf.get("calib_neg_alert_burden_mean"),
                    "events": ev.get("events"),
                    "events_trainable": ev.get("events_trainable"),
                    "events_with_any_lead": ev.get("events_with_any_lead"),
                    "events_with_pos_lead": ev.get("events_with_pos_lead"),
                    "lead_time_any_median": ev.get("lead_time_any_median"),
                    "lead_time_pos_median": ev.get("lead_time_pos_median"),
                    "event_pr_auc": ev.get("pr_auc_mean"),
                    "event_brier": ev.get("brier_mean"),
                    "event_burden": ev.get("alert_burden_eval_mean"),
                    "event_precision": ev.get("precision_eval_mean"),
                    "event_recall": ev.get("recall_eval_mean"),
                    "model": args.model,
                    "feature_mode": args.feature_mode,
                    "threshold_target": args.threshold_target,
                    "threshold_mode": args.threshold_mode,
                    "rolling_window_days": args.rolling_window_days,
                    "threshold_calib_days": args.threshold_calib_days,
                }

                rows.append(row)

    df = pd.DataFrame(rows)
    df = df.sort_values(["burden", "horizon", "variant"]).reset_index(drop=True)
    df.to_csv(out_root / "sweep_summary.csv", index=False)

    # 3) Scorecard
    score_rows: list[dict] = []
    for _, r in df.iterrows():
        row = r.to_dict()
        sc = _score_practical(row)
        score_rows.append({**row, **sc})
    score = pd.DataFrame(score_rows)
    score.to_csv(out_root / "sweep_scorecard.csv", index=False)

    print("\n=== sweep_scorecard (practical) ===")
    show_cols = [
        "burden",
        "variant",
        "horizon",
        "events_trainable",
        "events_with_pos_lead",
        "capture_rate",
        "lead_time_pos_median",
        "capture_min_required",
        "lead_min_required",
        "passed_practical",
    ]
    with pd.option_context("display.max_columns", 60, "display.width", 160):
        print(score[show_cols].to_string(index=False))


if __name__ == "__main__":
    main()
