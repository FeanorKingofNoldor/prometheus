"""Evaluate regime transition early-warning performance from an existing dataset CSV.

This script is useful for parameter sweeps (e.g., alert burden) without
rebuilding the dataset and derived features each time.

Input CSV is expected to contain:
- as_of_date
- crisis_21d, crisis_63d
- is_valid
- active_event (optional but recommended)
- raw + derived features (derived columns contain '__')

Research-only; provider-only.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from apathis.regime.event_eval import run_event_eval
from apathis.regime.eval_baseline import run_baseline_walk_forward
from apathis.regime.reporting import build_rollup_dataframe, summarize_folds


def _select_eval_df(df: pd.DataFrame, mode: str) -> pd.DataFrame:
    base_cols = ["as_of_date", "is_valid", "active_event", "notes", "crisis_21d", "crisis_63d"]
    if mode == "both":
        return df
    if mode == "derived":
        derived_cols = [c for c in df.columns if "__" in c]
        keep = [c for c in base_cols if c in df.columns] + derived_cols
        return df[keep]
    if mode == "raw":
        keep = [c for c in df.columns if "__" not in c]
        return df[keep]
    raise ValueError(f"Unknown feature mode: {mode}")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--dataset", type=str, required=True, help="Path to features_daily_joined.csv")
    p.add_argument("--out-dir", type=str, required=True)

    p.add_argument("--train-years", type=int, default=5)
    p.add_argument("--test-days", type=int, default=252)
    p.add_argument("--step-days", type=int, default=252)
    p.add_argument("--feature-mode", choices=["raw", "derived", "both"], default="derived")

    p.add_argument("--target-burden", type=float, default=0.1)
    p.add_argument("--threshold-calib-days", type=int, default=252)
    p.add_argument("--threshold-target", choices=["all", "neg"], default="all")
    p.add_argument("--threshold-mode", choices=["fixed", "rolling"], default="fixed")
    p.add_argument("--rolling-window-days", type=int, default=252)

    p.add_argument("--model", choices=["gb", "logreg"], default="gb")
    p.add_argument("--include-proto-sim", action="store_true")

    args = p.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    joined = pd.read_csv(args.dataset, low_memory=False)

    run_summary: dict = {
        "args": {
            "dataset": str(args.dataset),
            "train_years": int(args.train_years),
            "test_days": int(args.test_days),
            "step_days": int(args.step_days),
            "feature_mode": str(args.feature_mode),
            "target_burden": float(args.target_burden),
            "threshold_calib_days": int(args.threshold_calib_days),
            "threshold_target": str(args.threshold_target),
            "threshold_mode": str(args.threshold_mode),
            "rolling_window_days": int(args.rolling_window_days),
            "model": str(args.model),
            "include_proto_sim": bool(args.include_proto_sim),
        },
        "dataset": {
            "rows": int(len(joined)),
            "rows_valid": int((joined.get("is_valid", pd.Series([True] * len(joined))) == True).sum()),  # noqa: E712
            "date_min": str(pd.to_datetime(joined["as_of_date"]).min().date()),
            "date_max": str(pd.to_datetime(joined["as_of_date"]).max().date()),
            "pos_21d": int(joined.get("crisis_21d", pd.Series([0] * len(joined))).sum()),
            "pos_63d": int(joined.get("crisis_63d", pd.Series([0] * len(joined))).sum()),
        },
        "horizons": {},
    }

    print(
        "dataset:",
        f"rows={run_summary['dataset']['rows']}",
        f"valid={run_summary['dataset']['rows_valid']}",
        f"range={run_summary['dataset']['date_min']}..{run_summary['dataset']['date_max']}",
        f"pos21={run_summary['dataset']['pos_21d']}",
        f"pos63={run_summary['dataset']['pos_63d']}",
    )

    eval_df = _select_eval_df(joined, args.feature_mode)

    for horizon in (21, 63):
        label_col = f"crisis_{horizon}d"
        valid = eval_df[eval_df["is_valid"] == True].copy()  # noqa: E712
        valid["as_of_date"] = pd.to_datetime(valid["as_of_date"])
        valid = valid.sort_values("as_of_date")

        folds, pred_df = run_baseline_walk_forward(
            df=valid,
            label_col=label_col,
            date_col="as_of_date",
            train_days=252 * args.train_years,
            test_days=args.test_days,
            step_days=args.step_days,
            class_weight="balanced",
            target_alert_burden=args.target_burden,
            threshold_calibration_days=args.threshold_calib_days,
            threshold_target=args.threshold_target,
            threshold_mode=args.threshold_mode,
            rolling_window_days=args.rolling_window_days,
            model_name=args.model,
            add_proto_similarity=args.include_proto_sim,
        )

        fold_df = build_rollup_dataframe(folds)
        fold_df.to_csv(out_dir / f"folds_{horizon}d.csv", index=False)
        pred_df.to_csv(out_dir / f"preds_{horizon}d.csv", index=False)

        fold_summary = summarize_folds(folds)

        ev = run_event_eval(
            eval_df,
            horizon_days=horizon,
            eval_lookback_days=252,
            target_alert_burden=args.target_burden,
            threshold_calibration_days=args.threshold_calib_days,
            threshold_target=args.threshold_target,
            threshold_mode=args.threshold_mode,
            rolling_window_days=args.rolling_window_days,
            model_name=args.model,
        )
        ev.to_csv(out_dir / f"event_eval_{horizon}d.csv", index=False)

        lead_any = pd.to_numeric(ev["lead_time_days"], errors="coerce")
        lead_pos = pd.to_numeric(ev["lead_time_pos_days"], errors="coerce")

        event_summary = {
            "events": int(len(ev)),
            "events_trainable": int(pd.to_numeric(ev["train_pos"], errors="coerce").fillna(0).gt(0).sum()),
            "events_with_any_lead": int(lead_any.notna().sum()),
            "events_with_pos_lead": int(lead_pos.notna().sum()),
            "lead_time_any_median": float(lead_any.median(skipna=True)) if lead_any.notna().any() else None,
            "lead_time_pos_median": float(lead_pos.median(skipna=True)) if lead_pos.notna().any() else None,
            "pr_auc_mean": float(pd.to_numeric(ev["pr_auc"], errors="coerce").mean(skipna=True)),
            "brier_mean": float(pd.to_numeric(ev["brier"], errors="coerce").mean(skipna=True)),
            "alert_burden_eval_mean": float(pd.to_numeric(ev["alert_burden_eval"], errors="coerce").mean(skipna=True)),
            "precision_eval_mean": float(pd.to_numeric(ev["precision_eval"], errors="coerce").mean(skipna=True)),
            "recall_eval_mean": float(pd.to_numeric(ev["recall_eval"], errors="coerce").mean(skipna=True)),
        }

        run_summary["horizons"][f"{horizon}d"] = {
            "walk_forward": fold_summary.__dict__,
            "event_eval": event_summary,
        }

        lead_med = (
            f"{fold_summary.lead_median_median:.0f}d" if fold_summary.lead_median_median is not None else "n/a"
        )
        lead_min = f"{fold_summary.lead_min_min:.0f}d" if fold_summary.lead_min_min is not None else "n/a"
        print(
            f"{horizon}d: folds={fold_summary.num_folds} auc={fold_summary.auc_mean:.3f} pr={fold_summary.pr_auc_mean:.3f} "
            f"brier={fold_summary.brier_mean:.4f} burden={fold_summary.alert_burden_mean:.3f} "
            f"prec={fold_summary.precision_mean:.3f} rec={fold_summary.recall_mean:.3f} "
            f"lead_med={lead_med} lead_min={lead_min} "
            f"events_lead_pos={event_summary['events_with_pos_lead']}/{event_summary['events']}"
        )

    (out_dir / "run_summary.json").write_text(json.dumps(run_summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
