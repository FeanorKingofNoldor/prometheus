"""Core+wheel equity-core campaign: 36-combo grid x 4 windows."""
import json
import sys
import time

import prom2_cpp

WINDOWS = {
    "2000s": ("2000-01-03", "2009-12-31"),
    "2010s": ("2010-01-04", "2019-12-31"),
    "2020s": ("2020-01-02", "2026-07-31"),
    "full": ("2000-01-03", "2026-07-31"),
}

BASE = {
    "market_id": "US_EQ",
    "universe_max_size": 200,
    "initial_cash": 250_000.0,
    "entry_drawdown_pct": [0.10, 0.15, 0.20],
    "max_positions": [5, 6, 8],
    "exit_recovery_pct": [0.0, 0.05],
    "hard_stop_pct": [0.0, 0.25],
    "min_rebalance_pct": 0.02,
    "min_holding_days": 5,
    "cooldown_days": 10,
    "apply_fragility_overlay": True,
    "invested_fraction_calm": 0.90,
    "invested_fraction_stressed": 0.60,
    "fragility_stressed_threshold": 0.30,
    "include_delisted_instruments": True,
    "run_id_prefix": "BT_CORE_WHEEL",
}

out = {}
for name, (start, end) in WINDOWS.items():
    cfg = dict(BASE, start=start, end=end)
    t0 = time.time()
    results = prom2_cpp.run_core_wheel_backtests(cfg)
    elapsed = time.time() - t0
    rows = []
    for r in results:
        m = r["metrics"]
        rows.append({
            "combo": r.get("combo", r["run_id"].split("BT_CORE_WHEEL_")[-1].rsplit("_", 1)[0]),
            "run_id": r["run_id"],
            "cum_return": m["cumulative_return"],
            "sharpe": m["annualised_sharpe"],
            "max_dd": m["max_drawdown"],
            "trades": m.get("n_trades"),
            "tim": m.get("time_in_market"),
            "avg_hold": m.get("avg_holding_days"),
            "per_year": m.get("per_year_returns", {}),
        })
    rows.sort(key=lambda x: -(x["sharpe"] or -99))
    out[name] = {"elapsed_s": round(elapsed, 1), "results": rows}
    print(f"=== {name} ({start}..{end}) {elapsed:.1f}s, {len(rows)} combos ===", flush=True)
    for r in rows[:5]:
        print(f"  {r['combo']:<22} ret={r['cum_return']:+8.1%} sharpe={r['sharpe']:5.2f} "
              f"dd={r['max_dd']:+7.1%} tim={r['tim']:.0%} hold={r['avg_hold']:.0f}d", flush=True)
    w = rows[-1]
    print(f"  worst: {w['combo']:<15} ret={w['cum_return']:+8.1%} sharpe={w['sharpe']:5.2f} dd={w['max_dd']:+7.1%}", flush=True)

with open(sys.argv[1], "w") as f:
    json.dump(out, f, indent=1)
print("saved:", sys.argv[1])
