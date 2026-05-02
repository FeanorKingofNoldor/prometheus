"""Run the regime transition research pipeline end-to-end.

This script:
- builds the daily feature/label dataset from locally stored sources,
- runs walk-forward evaluation for selected horizons,
- runs per-event evaluation,
- writes CSV outputs under data/.

Research-only; no budgeting logic.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd
from apatheon.regime.cftc_features import build_tff_weekly_features, forward_fill_weekly_to_daily
from apatheon.regime.data_sources import (
    load_breadth,
    load_cftc_tff,
    load_finra_margin,
    load_fred_dfii10,
    load_fred_dgs2,
    load_fred_dgs3mo,
    load_fred_dgs10,
    load_fred_fedfunds,
    load_fred_icsa,
    load_fred_oas_hy,
    load_fred_oas_ig,
    load_fred_stlfsi2,
    load_fred_unrate,
    load_fred_vixcls,
    load_pcr_total,
)
from apatheon.regime.eval_baseline import run_baseline_walk_forward
from apatheon.regime.event_eval import run_event_eval
from apatheon.regime.event_labels import generate_labels
from apatheon.regime.features_numeric import FeatureConfig, compute_feature_matrix, join_features_and_labels
from apatheon.regime.reporting import build_rollup_dataframe, summarize_folds


def build_daily_dataset(*, include_pcr: bool = True, include_cftc: bool = False, include_macro: bool = True) -> pd.DataFrame:
    breadth = load_breadth().rename(columns={"trade_date": "as_of_date"}).sort_values("as_of_date")

    # FINRA monthly -> daily ffill on trading-day spine
    finra = load_finra_margin().rename(columns={"date": "as_of_date"}).sort_values("as_of_date")
    finra_daily = breadth[["as_of_date"]].merge(finra, on="as_of_date", how="left").sort_values("as_of_date")
    finra_daily[["debit_balances", "free_credit_cash", "free_credit_securities"]] = (
        finra_daily[["debit_balances", "free_credit_cash", "free_credit_securities"]].ffill()
    )

    hy = load_fred_oas_hy().rename(columns={"date": "as_of_date"}).sort_values("as_of_date")
    ig = load_fred_oas_ig().rename(columns={"date": "as_of_date"}).sort_values("as_of_date")
    spine = breadth[["as_of_date"]]
    oas = spine.merge(hy, on="as_of_date", how="left").merge(ig, on="as_of_date", how="left").sort_values("as_of_date")
    oas[["oas_hy", "oas_ig"]] = oas[["oas_hy", "oas_ig"]].ffill()
    oas["oas_hy_ig_diff"] = oas["oas_hy"] - oas["oas_ig"]
    oas["oas_hy_ig_ratio"] = oas["oas_hy"] / oas["oas_ig"].replace(0, pd.NA)

    features = breadth.merge(finra_daily, on="as_of_date", how="left")
    features = features.merge(oas, on="as_of_date", how="left")

    if include_macro:
        dgs2 = load_fred_dgs2().rename(columns={"date": "as_of_date"}).sort_values("as_of_date")
        dgs10 = load_fred_dgs10().rename(columns={"date": "as_of_date"}).sort_values("as_of_date")
        dgs3mo = load_fred_dgs3mo().rename(columns={"date": "as_of_date"}).sort_values("as_of_date")
        dfii10 = load_fred_dfii10().rename(columns={"date": "as_of_date"}).sort_values("as_of_date")
        vix = load_fred_vixcls().rename(columns={"date": "as_of_date"}).sort_values("as_of_date")
        fedfunds = load_fred_fedfunds().rename(columns={"date": "as_of_date"}).sort_values("as_of_date")
        unrate = load_fred_unrate().rename(columns={"date": "as_of_date"}).sort_values("as_of_date")
        icsa = load_fred_icsa().rename(columns={"date": "as_of_date"}).sort_values("as_of_date")
        stlfsi2 = load_fred_stlfsi2().rename(columns={"date": "as_of_date"}).sort_values("as_of_date")

        spine = features[["as_of_date"]].sort_values("as_of_date")
        macro = spine.merge(dgs2, on="as_of_date", how="left")
        macro = macro.merge(dgs10, on="as_of_date", how="left")
        macro = macro.merge(dgs3mo, on="as_of_date", how="left")
        macro = macro.merge(dfii10, on="as_of_date", how="left")
        macro = macro.merge(vix, on="as_of_date", how="left")
        macro = macro.merge(fedfunds, on="as_of_date", how="left")
        macro = macro.merge(unrate, on="as_of_date", how="left")
        macro = macro.merge(icsa, on="as_of_date", how="left")
        macro = macro.merge(stlfsi2, on="as_of_date", how="left")

        # Forward-fill mixed-frequency macros
        macro_cols = [c for c in macro.columns if c != "as_of_date"]
        macro[macro_cols] = macro[macro_cols].ffill()

        macro["curve_10y_2y"] = macro["dgs10"] - macro["dgs2"]
        macro["curve_10y_3m"] = macro["dgs10"] - macro["dgs3mo"]

        features = features.merge(macro, on="as_of_date", how="left")

    if include_pcr:
        pcr = load_pcr_total().rename(columns={"date": "as_of_date"}).sort_values("as_of_date")
        features = features.merge(pcr, on="as_of_date", how="left")

    if include_cftc:
        usecols = [
            "report_date_as_yyyy_mm_dd",
            "commodity_name",
            "open_interest_all",
            "dealer_positions_long_all",
            "dealer_positions_short_all",
            "asset_mgr_positions_long",
            "asset_mgr_positions_short",
            "lev_money_positions_long",
            "lev_money_positions_short",
            "futonly_or_combined",
        ]
        tff = load_cftc_tff(usecols=usecols)
        weekly = build_tff_weekly_features(tff)
        daily = forward_fill_weekly_to_daily(weekly, features["as_of_date"])
        features = features.merge(daily, on="as_of_date", how="left")

    # Leak-free rolling features for every numeric column.
    raw = features.sort_values("as_of_date").set_index("as_of_date")
    numeric_cols = [c for c in raw.columns if pd.api.types.is_numeric_dtype(raw[c])]
    series_dict = {c: raw[c].astype(float) for c in numeric_cols}
    derived = compute_feature_matrix(series_dict, config=FeatureConfig())
    features = pd.concat([raw, derived], axis=1)
    features = features.reset_index()

    labels = pd.DataFrame(
        generate_labels(
            start_date=features["as_of_date"].min().date(),
            end_date=features["as_of_date"].max().date(),
        )
    )
    labels["as_of_date"] = pd.to_datetime(labels["as_of_date"])

    joined = join_features_and_labels(features, labels, drop_invalid=False)
    return joined


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--out-dir", type=str, default="data")
    p.add_argument("--include-pcr", action="store_true")
    p.add_argument("--no-include-pcr", dest="include_pcr", action="store_false")
    p.set_defaults(include_pcr=True)
    p.add_argument("--train-years", type=int, default=5)
    p.add_argument("--test-days", type=int, default=252)
    p.add_argument("--step-days", type=int, default=252)
    p.add_argument(
        "--target-burden",
        type=float,
        default=0.1,
        help="Target training alert burden used to pick score thresholds",
    )
    p.add_argument(
        "--threshold-calib-days",
        type=int,
        default=252,
        help="Days at the end of each train window reserved for out-of-sample threshold calibration",
    )
    p.add_argument(
        "--threshold-target",
        choices=["all", "neg"],
        default="all",
        help="When calibrating thresholds, target burden on (all) days or only (neg)atives",
    )
    p.add_argument(
        "--threshold-mode",
        choices=["fixed", "rolling"],
        default="fixed",
        help="Thresholding mode: fixed per-fold threshold, or rolling quantile to enforce burden",
    )
    p.add_argument(
        "--rolling-window-days",
        type=int,
        default=252,
        help="Rolling quantile window size (in observations) when --threshold-mode=rolling",
    )
    p.add_argument("--include-cftc", action="store_true", help="Add CFTC TFF positioning features")
    p.add_argument("--no-macro", dest="include_macro", action="store_false", help="Disable FRED macro series")
    p.set_defaults(include_macro=True)
    p.add_argument(
        "--feature-mode",
        choices=["raw", "derived", "both"],
        default="derived",
        help="Which features to use for model evaluation (dataset always includes raw + derived)",
    )
    p.add_argument(
        "--model",
        choices=["gb", "logreg"],
        default="gb",
        help="Baseline model type",
    )
    p.add_argument("--include-proto-sim", action="store_true", help="Add prototype similarity feature")
    args = p.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    joined = build_daily_dataset(
        include_pcr=args.include_pcr,
        include_cftc=args.include_cftc,
        include_macro=args.include_macro,
    )
    joined.to_csv(out_dir / "features_daily_joined.csv", index=False)
    joined[joined["is_valid"] == True].to_csv(out_dir / "features_daily_joined_valid.csv", index=False)  # noqa: E712

    run_summary: dict = {
        "args": {
            "include_pcr": bool(args.include_pcr),
            "include_cftc": bool(args.include_cftc),
            "include_macro": bool(args.include_macro),
            "train_years": int(args.train_years),
            "test_days": int(args.test_days),
            "step_days": int(args.step_days),
            "target_burden": float(args.target_burden),
            "threshold_calib_days": int(args.threshold_calib_days),
            "threshold_target": str(args.threshold_target),
            "threshold_mode": str(args.threshold_mode),
            "rolling_window_days": int(args.rolling_window_days),
            "feature_mode": str(args.feature_mode),
            "model": str(args.model),
            "include_proto_sim": bool(args.include_proto_sim),
        },
        "dataset": {
            "rows": int(len(joined)),
            "rows_valid": int((joined["is_valid"] == True).sum()),  # noqa: E712
            "date_min": str(pd.to_datetime(joined["as_of_date"]).min().date()),
            "date_max": str(pd.to_datetime(joined["as_of_date"]).max().date()),
            "pos_21d": int(joined["crisis_21d"].sum()),
            "pos_63d": int(joined["crisis_63d"].sum()),
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

    eval_df = _select_eval_df(joined, args.feature_mode)

    # Walk-forward evaluations
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

        # Per-event eval
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
            "alert_burden_eval_mean": float(
                pd.to_numeric(ev["alert_burden_eval"], errors="coerce").mean(skipna=True)
            ),
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
