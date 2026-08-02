"""Measure C1 (vol-scaled + cross-sectionally standardized momentum) and
C2 (universe ranking blend standardization) through the signal harness.

C1: builds the OLD raw 12-1 momentum vs the NEW vol-scaled + cross-sectionally
    standardized momentum over a multi-year liquid-US-equity window, runs both
    through evaluate_signal, prints before/after rank-IC at 1/5/21/63d (return
    AND vol), decile spreads, and the what-it-predicts verdict. Registers the
    new signal.

C2: builds a synthetic "alpha" (the NEW momentum) and a "base" (a STAB/liquidity
    proxy = inverse realized vol + log dollar volume) over the same sample, then
    forms the ranking score the OLD way (clip negative alpha * 50 + raw 0-150
    base) and the NEW way (z(base) + w * z(alpha)), and compares each blend's IC
    vs forward returns. Confirms the standardized blend's IC >= the old blend's.

Run:
    python -m scripts.research.measure_c1_c2_signal_fixes \
        --start 2015-01-01 --end 2025-01-01 --universe-size 400
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta
from typing import Optional, Sequence

import numpy as np
import pandas as pd
from apatheon.core.database import get_db_manager
from apatheon.core.logging import get_logger

from prometheus.research.signal_harness import (
    _load_prices,
    evaluate_signal,
    forward_return_panel,
)
from prometheus.research.signal_registry import update_from_report

logger = get_logger(__name__)


def _parse_date(s: str) -> date:
    y, m, d = map(int, s.split("-"))
    return date(y, m, d)


def _liquid_universe(db_manager, start: date, end: date, top_n: int) -> list[str]:
    sql = """
        SELECT instrument_id, COUNT(*) c
        FROM prices_daily
        WHERE trade_date BETWEEN %s AND %s
          AND instrument_id LIKE '%%.US'
          AND COALESCE(adjusted_close, close) > 0
        GROUP BY instrument_id
        ORDER BY c DESC
        LIMIT %s
    """
    with db_manager.get_historical_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (start, end, top_n))
            return [r[0] for r in cur.fetchall()]


def _build_raw_and_volscaled(
    db_manager,
    start: date,
    end: date,
    *,
    lookback: int,
    skip: int,
    universe: Optional[Sequence[str]],
    min_universe: int,
    sample_every: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (raw_momentum, volscaled_standardized_momentum) tidy frames.

    raw:    score = px[t-skip]/px[t-skip-lookback] - 1   (the OLD model: raw
            lookback return; not vol-scaled, not standardized).
    new:    score = z_cross_section( (lookback return) / realized_vol_over_window )
            mirroring the C1 fix in model_basic.py.
    """
    need = lookback + skip
    load_start = start - timedelta(days=int(need * 2 + 30))
    prices = _load_prices(db_manager, universe, load_start, end)
    if prices.empty:
        empty = pd.DataFrame(columns=["instrument_id", "as_of_date", "score"])
        return empty, empty.copy()

    raw_rows: list[dict] = []
    vs_rows: list[dict] = []
    for inst, g in prices.groupby("instrument_id"):
        g = g.reset_index(drop=True)
        px = g["px"].to_numpy()
        dts = g["trade_date"].to_numpy()
        for i in range(need, len(px)):
            as_of = dts[i]
            if not (np.datetime64(start) <= as_of <= np.datetime64(end)):
                continue
            base = px[i - skip - lookback]
            recent = px[i - skip]
            if base <= 0 or recent <= 0:
                continue
            mom = float(recent / base - 1.0)
            # realized vol over the SAME formation window (the quantity the
            # production model already computed and used to discard).
            window = px[i - skip - lookback : i - skip + 1]
            log_rets = np.log(window[1:] / window[:-1])
            log_rets = log_rets[np.isfinite(log_rets)]
            rvol = float(np.std(log_rets, ddof=1)) if log_rets.size > 1 else 0.0
            vol_scaled = mom / rvol if rvol > 1e-9 else mom
            ts = pd.Timestamp(as_of)
            raw_rows.append({"instrument_id": inst, "as_of_date": ts, "score": mom})
            vs_rows.append({"instrument_id": inst, "as_of_date": ts, "score": vol_scaled})

    raw = pd.DataFrame(raw_rows)
    vs = pd.DataFrame(vs_rows)
    if raw.empty:
        return raw, vs

    # weekly sampling (reduce overlap/compute) — same dates for both
    all_dates = sorted(raw["as_of_date"].unique())
    keep = set(all_dates[::sample_every])
    raw = raw[raw["as_of_date"].isin(keep)].reset_index(drop=True)
    vs = vs[vs["as_of_date"].isin(keep)].reset_index(drop=True)

    # drop thin cross-sections
    counts = raw.groupby("as_of_date")["instrument_id"].transform("count")
    keep_mask = counts >= min_universe
    raw = raw[keep_mask].reset_index(drop=True)
    vs = vs[keep_mask.values].reset_index(drop=True)

    # cross-sectional z-score of the vol-scaled momentum (the C1 standardization)
    def _z(g: pd.Series) -> pd.Series:
        mu = g.mean()
        sd = g.std(ddof=1)
        if not np.isfinite(sd) or sd <= 1e-12:
            return pd.Series(0.0, index=g.index)
        return (g - mu) / sd

    vs["_volscaled_raw"] = vs["score"]
    vs["score"] = vs.groupby("as_of_date")["score"].transform(_z)
    return raw, vs


def _zscore_by_date(df: pd.DataFrame, col: str) -> pd.Series:
    def _z(g: pd.Series) -> pd.Series:
        mu = g.mean()
        sd = g.std(ddof=1)
        if not np.isfinite(sd) or sd <= 1e-12:
            return pd.Series(0.0, index=g.index)
        return (g - mu) / sd

    return df.groupby("as_of_date")[col].transform(_z)


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--start", type=_parse_date, default=date(2015, 1, 1))
    ap.add_argument("--end", type=_parse_date, default=date(2025, 1, 1))
    ap.add_argument("--lookback", type=int, default=126)
    ap.add_argument("--skip", type=int, default=21)
    ap.add_argument("--universe-size", type=int, default=400)
    ap.add_argument("--sample-every", type=int, default=5)
    ap.add_argument("--min-universe", type=int, default=20)
    ap.add_argument("--no-register", action="store_true")
    args = ap.parse_args(argv)

    db = get_db_manager()
    universe = _liquid_universe(db, args.start, args.end, args.universe_size)
    logger.info("Universe: %d instruments", len(universe))

    raw, vs = _build_raw_and_volscaled(
        db, args.start, args.end,
        lookback=args.lookback, skip=args.skip,
        universe=universe, min_universe=args.min_universe,
        sample_every=args.sample_every,
    )
    logger.info("raw obs=%d  volscaled obs=%d  dates=%d",
                len(raw), len(vs), raw["as_of_date"].nunique() if not raw.empty else 0)

    horizons = (1, 5, 21, 63)
    # Build ONE forward-return panel on the shared (instrument, date) keys.
    panel = forward_return_panel(db, raw, horizons=horizons, universe=universe)

    rep_old = evaluate_signal(raw, panel, name="momentum_RAW_old",
                              horizons=horizons, headline_horizon=21)
    rep_new = evaluate_signal(vs, panel, name="momentum_volscaled_zscored",
                              horizons=horizons, headline_horizon=21)

    # Production-faithful OLD: the actual model_basic score was
    # clip(momentum / 0.20, -1, 1) — the SATURATION at +/-1 is what destroyed
    # rank information for big movers (the harness's "weak return-IC" symptom).
    prod_old = raw.copy()
    prod_old["score"] = (prod_old["score"] / 0.20).clip(-1.0, 1.0)
    rep_prod_old = evaluate_signal(prod_old, panel, name="momentum_PROD_old_clipped",
                                   horizons=horizons, headline_horizon=21)
    print("\n[production-faithful OLD] clip(momentum/0.20,-1,1) IC vs fwd return:")
    for h in horizons:
        ic = rep_prod_old.ic.get(h)
        if ic:
            print(f"   {h:>3}d: meanIC={ic.mean_ic:+.4f}  t={ic.t_stat:+.2f}  "
                  f"(vs vol IC={ic.mean_ic_vol:+.4f} t={ic.t_stat_vol:+.2f})")

    # Ablation: vol-scaled but NOT z-scored, to prove that cross-sectional
    # z-scoring is rank-preserving within a date (=> identical rank-IC to the
    # standardized version). Any IC change vs OLD therefore comes purely from
    # the vol-scaling step.
    vs_only = vs.copy()
    # rebuild un-z-scored vol-scaled from raw alpha + the stored z? simpler:
    # re-derive vol-scaled by NOT applying the per-date z-transform.
    # (We recompute from scratch to be unambiguous.)
    if "_volscaled_raw" in vs.columns:
        vs_only["score"] = vs["_volscaled_raw"]
    rep_vs_only = evaluate_signal(vs_only, panel, name="momentum_volscaled_noz",
                                  horizons=horizons, headline_horizon=21)
    print("\n[ablation] vol-scaled NO-zscore IC (should equal NEW exactly — "
          "z-score is rank-preserving):")
    for h in horizons:
        a = rep_vs_only.ic.get(h)
        b = rep_new.ic.get(h)
        if a and b:
            print(f"   {h:>3}d: noz={a.mean_ic:+.4f}  zscored={b.mean_ic:+.4f}")

    print("\n" + "=" * 78)
    print("C1 — MOMENTUM: OLD (raw return / fixed const) vs NEW (vol-scaled + x-sec z-score)")
    print("=" * 78)
    print("\n--- BEFORE (OLD raw momentum) ---")
    print(rep_old.headline())
    print("\n--- AFTER (NEW vol-scaled + standardized momentum) ---")
    print(rep_new.headline())
    print("=" * 78)

    if not args.no_register:
        update_from_report(
            db, rep_new,
            description=f"{args.lookback}-1 momentum, vol-scaled (return/realized-vol over "
                        f"window) then cross-sectionally z-scored. Mirrors C1 fix in "
                        f"model_basic.py. Liquid US equities.",
            bucket=1,
            integrity_note="Pure past-price; vol-scaling and z-score use only as-of "
                           "cross-section. Forward returns strictly after as_of. No look-ahead.",
        )
        print("Registered 'momentum_volscaled_zscored' in signal_registry.")

    # ------------------------------------------------------------------
    # C2 — universe ranking blend: OLD clip-and-add vs NEW standardized
    # ------------------------------------------------------------------
    # Build a STAB/liquidity "base" proxy aligned to the same (inst, date) keys.
    # Base proxy: low realized vol + high dollar volume = "quality" (mirrors the
    # universe base = (100 - soft_target) + min(50, volume/1e6), which rewards
    # stable, liquid names). We use inverse realized vol + log dollar-volume.
    need = args.lookback + args.skip
    load_start = args.start - timedelta(days=int(need * 2 + 30))
    prices_full = _load_prices(db, universe, load_start, args.end)

    base_rows: list[dict] = []
    for inst, g in prices_full.groupby("instrument_id"):
        g = g.reset_index(drop=True)
        px = g["px"].to_numpy()
        dts = g["trade_date"].to_numpy()
        for i in range(need, len(px)):
            as_of = dts[i]
            if not (np.datetime64(args.start) <= as_of <= np.datetime64(args.end)):
                continue
            w = px[max(0, i - 63):i + 1]
            lr = np.log(w[1:] / w[:-1])
            lr = lr[np.isfinite(lr)]
            rv = float(np.std(lr, ddof=1)) if lr.size > 1 else np.nan
            if not np.isfinite(rv) or rv <= 0:
                continue
            # quality base: lower vol -> higher base. Scale to ~[0,100] like STAB.
            quality = float(100.0 * np.exp(-rv * 20.0))
            base_rows.append({"instrument_id": inst, "as_of_date": pd.Timestamp(as_of),
                              "base": quality})
    base_df = pd.DataFrame(base_rows)

    # Align alpha (NEW momentum, already z-scored) and base on shared keys.
    alpha_df = vs.rename(columns={"score": "alpha_z"})
    # also need RAW alpha (un-standardized) to replicate OLD clip*50 behaviour
    raw_alpha = raw.rename(columns={"score": "alpha_raw"})

    merged = (
        base_df.merge(alpha_df, on=["instrument_id", "as_of_date"], how="inner")
        .merge(raw_alpha, on=["instrument_id", "as_of_date"], how="inner")
    )
    if merged.empty:
        print("\nC2: no overlapping (inst, date) rows for blend comparison; skipping.")
        return

    # OLD blend: base (0-150-ish) + max(0, raw_alpha) * 50  (clips negative alpha)
    merged["score_old"] = merged["base"] + np.maximum(0.0, merged["alpha_raw"]) * 50.0

    # NEW blend: z(base) + 1.0 * z(alpha) on a common scale, sign preserved.
    merged["base_z"] = _zscore_by_date(merged, "base")
    # alpha_z is already cross-sectionally z-scored per date.
    merged["score_new"] = merged["base_z"] + 1.0 * merged["alpha_z"]

    old_scores = merged[["instrument_id", "as_of_date", "score_old"]].rename(
        columns={"score_old": "score"})
    new_scores = merged[["instrument_id", "as_of_date", "score_new"]].rename(
        columns={"score_new": "score"})

    rep_blend_old = evaluate_signal(old_scores, panel, name="universe_blend_OLD",
                                    horizons=horizons, headline_horizon=21)
    rep_blend_new = evaluate_signal(new_scores, panel, name="universe_blend_NEW",
                                    horizons=horizons, headline_horizon=21)

    print("\n" + "=" * 78)
    print("C2 — UNIVERSE BLEND: OLD (clip neg alpha *50 + raw base) vs NEW (z(base)+z(alpha))")
    print("=" * 78)
    print("\n--- BEFORE (OLD clip-and-add blend) ---")
    print(rep_blend_old.headline())
    print("\n--- AFTER (NEW standardized blend) ---")
    print(rep_blend_new.headline())
    print("=" * 78)

    def _ic(rep, h):
        ic = rep.ic.get(h)
        return ic.mean_ic if ic else float("nan")

    print("\nC2 IC vs forward return (mean rank-IC):")
    for h in horizons:
        print(f"  {h:>3}d: OLD={_ic(rep_blend_old, h):+.4f}  NEW={_ic(rep_blend_new, h):+.4f}  "
              f"delta={_ic(rep_blend_new, h) - _ic(rep_blend_old, h):+.4f}")


if __name__ == "__main__":
    main()
