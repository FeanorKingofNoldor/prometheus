"""Harness comparison: COMBINED signal-combination layer vs additive blend vs best-single.

Builds, over a multi-year window, the standardized signal components used by the
production scoring path:

  - momentum_z : vol-scaled 6-1 cross-sectional momentum (the same family as the
                 assessment 'alpha'), cross-sectionally z-scored per date.
  - base_z     : the universe STAB/liquidity quality base, cross-sectionally
                 z-scored per date.

Then it scores four variants and runs each through the research harness
(rank-IC + decile spread), printing an HONEST comparison:

  1. best-single        — the best individual component on its own.
  2. additive           — current production additive z-blend.
  3. combined(default)   — SignalCombiner, static default weights.
  4. combined(regime)    — SignalCombiner, regime-conditional weights (US regime).
  5. combined(ic-tilt)   — SignalCombiner, regime + trailing-IC tilt.

Usage:
    python scripts/compare_signal_combiner.py --start 2016-01-01 --end 2024-12-31
"""

from __future__ import annotations

import argparse
from datetime import date

import pandas as pd
from apatheon.core.database import get_db_manager

from prometheus.research.combiner import (
    CARRY,
    CRISIS,
    NEUTRAL,
    RISK_OFF,
    CombinerConfig,
    SignalCombiner,
    additive_blend_panel,
    trailing_rank_ic_by_date,
)
from prometheus.research.signal_harness import (
    evaluate_signal,
    forward_return_panel,
    momentum_signal,
    stab_base_signal,
    zscore_by_date,
)

HORIZONS = (5, 21, 63)
HEADLINE = 21


def _load_regime_by_date(db, dates, region: str = "US") -> dict:
    if len(dates) == 0:
        return {}
    lo, hi = min(dates), max(dates)
    sql = """
        SELECT as_of_date, regime_label
        FROM regimes
        WHERE region = %s AND as_of_date BETWEEN %s AND %s
        ORDER BY as_of_date
    """
    with db.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (region, lo.date(), hi.date()))
            rows = cur.fetchall()
    if not rows:
        return {}
    import numpy as np

    reg = pd.DataFrame(rows, columns=["as_of_date", "regime_label"])
    reg["as_of_date"] = pd.to_datetime(reg["as_of_date"])
    reg = reg.sort_values("as_of_date")
    # As-of join: for each signal date use the latest regime on/before it.
    out: dict = {}
    rdates = reg["as_of_date"].to_numpy(dtype="datetime64[ns]")
    rlabels = reg["regime_label"].to_numpy()

    for d in dates:
        dt = np.datetime64(pd.Timestamp(d), "ns")
        i = int(np.searchsorted(rdates, dt, side="right") - 1)
        if i >= 0:
            out[d] = str(rlabels[i])
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2016-01-01")
    ap.add_argument("--end", default="2024-12-31")
    ap.add_argument("--sample-every", type=int, default=5)
    args = ap.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)

    db = get_db_manager()

    print(f"Building components {start}..{end} (weekly sampling)...")
    mom = momentum_signal(db, start, end, sample_every=args.sample_every)
    if mom.empty:
        print("No momentum signal rows — aborting.")
        return
    print(f"  momentum rows={len(mom)} dates={mom['as_of_date'].nunique()}")

    mom_z = zscore_by_date(mom).rename(columns={"score": "momentum_z"})

    base = stab_base_signal(db, mom[["instrument_id", "as_of_date"]])
    print(f"  stab-base rows={len(base)}")
    if base.empty:
        base_z = pd.DataFrame(columns=["instrument_id", "as_of_date", "base_z"])
    else:
        base_z = zscore_by_date(base).rename(columns={"score": "base_z"})

    components = mom_z.merge(base_z, on=["instrument_id", "as_of_date"], how="left")
    components["momentum_z"] = components["momentum_z"].astype(float)
    if "base_z" not in components.columns:
        components["base_z"] = 0.0
    components["base_z"] = components["base_z"].fillna(0.0)

    # Forward-return panel (point-in-time) on the component grid.
    print("Building forward-return panel...")
    fwd = forward_return_panel(db, components, horizons=HORIZONS)
    print(f"  fwd panel rows={len(fwd)}")

    dates = sorted(pd.to_datetime(components["as_of_date"].unique()))
    regime_by_date = _load_regime_by_date(db, dates, region="US")
    if regime_by_date:
        from collections import Counter

        dist = Counter(regime_by_date.values())
        print(f"  regime coverage: {len(regime_by_date)}/{len(dates)} dates  dist={dict(dist)}")
    else:
        print("  no regime labels found (combiner will use default weights)")

    # ---- variants -------------------------------------------------------
    reports = {}

    # 1. best-single: evaluate each component alone, keep the strongest |IC|.
    for col in ("momentum_z", "base_z"):
        sub = components[["instrument_id", "as_of_date", col]].rename(
            columns={col: "score"}
        )
        rep = evaluate_signal(
            sub, fwd, name=col, horizons=HORIZONS, headline_horizon=HEADLINE
        )
        reports[col] = rep

    # 2. additive (production): z(base) + 1.0 * z(momentum). assessment_score_weight
    #    default 50 -> alpha_weight_z = 1.0; base weight 1.0.
    add = additive_blend_panel(
        components, weights={"momentum_z": 1.0, "base_z": 1.0}
    )
    reports["additive"] = evaluate_signal(
        add, fwd, name="additive", horizons=HORIZONS, headline_horizon=HEADLINE
    )

    # 3. combined(default)
    comb_default = SignalCombiner(
        CombinerConfig(
            weights={"momentum_z": 1.0, "base_z": 0.25},
            regime_weights={},
            use_regime=False,
        )
    )
    panel_default = comb_default.combine_panel(
        components, component_cols=["momentum_z", "base_z"]
    )
    reports["combined_default"] = evaluate_signal(
        panel_default, fwd, name="combined_default", horizons=HORIZONS,
        headline_horizon=HEADLINE,
    )

    # 4. combined(regime): regime-conditional weights.
    regime_cfg = CombinerConfig(
        weights={"momentum_z": 1.0, "base_z": 0.25},
        regime_weights={
            CRISIS: {"momentum_z": 0.4, "base_z": 0.6},
            RISK_OFF: {"momentum_z": 0.7, "base_z": 0.4},
            CARRY: {"momentum_z": 1.2, "base_z": 0.2},
            NEUTRAL: {"momentum_z": 1.0, "base_z": 0.25},
        },
        use_regime=True,
    )
    comb_regime = SignalCombiner(regime_cfg)
    panel_regime = comb_regime.combine_panel(
        components,
        component_cols=["momentum_z", "base_z"],
        regime_by_date=regime_by_date,
    )
    reports["combined_regime"] = evaluate_signal(
        panel_regime, fwd, name="combined_regime", horizons=HORIZONS,
        headline_horizon=HEADLINE,
    )

    # 5. combined(regime + IC-tilt)
    tic = trailing_rank_ic_by_date(
        components, fwd, component_cols=["momentum_z", "base_z"],
        horizon=HEADLINE, window=12,
    )
    ic_cfg = CombinerConfig(
        weights=regime_cfg.weights,
        regime_weights=regime_cfg.regime_weights,
        use_regime=True,
        use_ic_tilt=True,
        ic_tilt_strength=0.5,
        ic_floor=0.01,
    )
    comb_ic = SignalCombiner(ic_cfg)
    panel_ic = comb_ic.combine_panel(
        components,
        component_cols=["momentum_z", "base_z"],
        regime_by_date=regime_by_date,
        trailing_ic_by_date=tic,
    )
    reports["combined_ic_tilt"] = evaluate_signal(
        panel_ic, fwd, name="combined_ic_tilt", horizons=HORIZONS,
        headline_horizon=HEADLINE,
    )

    # ---- report ---------------------------------------------------------
    print("\n" + "=" * 78)
    print(f"COMBINER COMPARISON  ({start}..{end})  headline horizon = {HEADLINE}d")
    print("=" * 78)
    order = [
        "momentum_z", "base_z", "additive",
        "combined_default", "combined_regime", "combined_ic_tilt",
    ]
    print(f"{'variant':<20}{'IC@5d':>10}{'IC@21d':>10}{'IC@63d':>10}"
          f"{'t@21d':>9}{'dec@21d':>11}{'turnover':>10}")
    for name in order:
        r = reports[name]
        ic5 = r.ic.get(5)
        ic21 = r.ic.get(21)
        ic63 = r.ic.get(63)
        dec21 = r.deciles.get(21)
        print(
            f"{name:<20}"
            f"{(ic5.mean_ic if ic5 else float('nan')):>+10.4f}"
            f"{(ic21.mean_ic if ic21 else float('nan')):>+10.4f}"
            f"{(ic63.mean_ic if ic63 else float('nan')):>+10.4f}"
            f"{(ic21.t_stat if ic21 else float('nan')):>+9.2f}"
            f"{(dec21.top_minus_bottom if dec21 else float('nan')):>+11.4%}"
            f"{r.turnover:>10.3f}"
        )

    # honest verdict line
    best_single = max(
        ("momentum_z", "base_z"),
        key=lambda n: abs(reports[n].ic.get(HEADLINE).mean_ic)
        if reports[n].ic.get(HEADLINE) else 0.0,
    )
    bs_ic = reports[best_single].ic.get(HEADLINE)
    add_ic = reports["additive"].ic.get(HEADLINE)
    cr_ic = reports["combined_regime"].ic.get(HEADLINE)
    print("\nHEADLINE (21d) rank-IC:")
    print(f"  best-single ({best_single}): {bs_ic.mean_ic:+.4f} (t={bs_ic.t_stat:+.2f})")
    print(f"  additive             : {add_ic.mean_ic:+.4f} (t={add_ic.t_stat:+.2f})")
    print(f"  combined(regime)     : {cr_ic.mean_ic:+.4f} (t={cr_ic.t_stat:+.2f})")
    delta_vs_add = cr_ic.mean_ic - add_ic.mean_ic
    delta_vs_best = cr_ic.mean_ic - bs_ic.mean_ic
    print(f"  combined - additive  : {delta_vs_add:+.4f}")
    print(f"  combined - best-single: {delta_vs_best:+.4f}")


if __name__ == "__main__":
    main()
