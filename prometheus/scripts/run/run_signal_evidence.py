"""Run the evidence sweep: measure IC of in-house vs standard signals.

MEASUREMENT, not speculation. Runs each replayable signal through the harness,
prints its IC / decile / what-it-predicts, registers the verdict, and emits a
final EVIDENCE TABLE:

  - forward-indicator macro/regime stress  -> TIME-SERIES IC vs forward SPY
  - soft-target fragility (STAB)            -> cross-sectional IC vs fwd returns
  - short-term (1-week) reversal            -> standard benchmark factor
  - low-volatility (inverse realized vol)   -> standard benchmark factor

Usage::

    python -m prometheus.scripts.run.run_signal_evidence --start 2022-01-01 --end 2025-12-31
"""

from __future__ import annotations

import argparse
from datetime import date

from apatheon.core.database import get_db_manager
from apatheon.core.logging import get_logger

from prometheus.research.measure_signals import (
    forward_indicator_stress_series,
    low_vol_signal,
    market_forward_returns,
    short_term_reversal_signal,
    soft_target_fragility_signal,
)
from prometheus.research.signal_harness import (
    evaluate_signal,
    forward_return_panel,
    time_series_ic,
)
from prometheus.research.signal_registry import register_signal, update_from_report

logger = get_logger(__name__)

HORIZONS = (5, 21, 63)


def _parse_date(s: str) -> date:
    y, m, d = map(int, s.split("-"))
    return date(y, m, d)


def _liquid_universe(db, start, end, top_n):
    sql = """
        SELECT instrument_id, COUNT(*) c
        FROM prices_daily
        WHERE trade_date BETWEEN %s AND %s
          AND instrument_id LIKE '%%.US'
          AND COALESCE(adjusted_close, close) > 0
        GROUP BY instrument_id ORDER BY c DESC LIMIT %s
    """
    with db.get_historical_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (start, end, top_n))
            return [r[0] for r in cur.fetchall()]


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--start", type=_parse_date, default=date(2022, 1, 1))
    ap.add_argument("--end", type=_parse_date, default=date(2025, 12, 31))
    ap.add_argument("--universe-size", type=int, default=400)
    ap.add_argument("--sample-every", type=int, default=5)
    ap.add_argument("--no-register", action="store_true")
    args = ap.parse_args(argv)

    db = get_db_manager()
    universe = _liquid_universe(db, args.start, args.end, args.universe_size)
    logger.info("Universe: %d instruments", len(universe))

    evidence = []  # (name, kind, horizon, ic, extra, verdict)

    # ---- 1) MACRO / REGIME forward-indicator stress (time-series) ----------
    print("\n" + "#" * 78)
    print("# FORWARD-INDICATOR MACRO/REGIME STRESS  (time-series IC vs forward SPY)")
    print("#" * 78)
    stress = forward_indicator_stress_series(db, args.start, args.end)
    mkt = market_forward_returns(db, args.start, args.end, index_id="SPY.US", horizons=HORIZONS)
    if not stress.empty and not mkt.empty:
        # align stress (daily) to trading days present in mkt
        for h in HORIZONS:
            joined_ret = mkt[f"fwd_ret_{h}d"]
            joined_vol = mkt[f"fwd_vol_{h}d"]
            sig_aligned = stress.reindex(joined_ret.index).ffill(limit=3)
            res = time_series_ic(sig_aligned, joined_ret, fwd_vol=joined_vol,
                                 name="forward_indicator_stress", horizon=h)
            print(f"  {h:>3}d: IC_ret={res.ic_return:+.4f}  IC_vol={res.ic_vol:+.4f}  "
                  f"hit={res.hit_rate:.3f}  meanRet[hi-stress]={res.mean_ret_high:+.4%} "
                  f"meanRet[lo-stress]={res.mean_ret_low:+.4%}  n={res.n}")
            evidence.append((
                "forward_indicator_stress", "macro-timeseries", h,
                res.ic_return, f"IC_vol={res.ic_vol:+.3f} hit={res.hit_rate:.2f}",
                _macro_verdict(res.ic_return, res.ic_vol),
            ))
        # register macro layer (use 21d as headline)
        h = 21
        sig_aligned = stress.reindex(mkt.index).ffill(limit=3)
        res21 = time_series_ic(sig_aligned, mkt[f"fwd_ret_{h}d"], fwd_vol=mkt[f"fwd_vol_{h}d"],
                               name="forward_indicator_stress", horizon=h)
        if not args.no_register:
            register_signal(
                db, name="forward_indicator_stress",
                description="Apatheon forward-indicator aggregate macro/regime stress (FRED z-scores: "
                            "HY OAS, 2-10 curve, real yield, 10y, VIX) vs forward SPY return",
                bucket=1,
                integrity_note="Reconstructed from FRED history; each component trailing z up to date. "
                               "Time-series IC vs forward SPY (market timing, not cross-section).",
                what_predicts=_macro_what(res21.ic_return, res21.ic_vol),
                headline_ic=res21.ic_return, headline_horizon=h,
                verdict=_macro_verdict(res21.ic_return, res21.ic_vol),
                turnover=float("nan"), n_obs=res21.n,
                metrics={"kind": "time_series", "ic_return": res21.ic_return,
                         "ic_vol": res21.ic_vol, "hit_rate": res21.hit_rate,
                         "mean_ret_high_stress": res21.mean_ret_high,
                         "mean_ret_low_stress": res21.mean_ret_low},
            )
    else:
        print("  SKIP: no stress series or market returns available")

    # ---- cross-sectional signals -------------------------------------------
    builders = [
        ("soft_target_fragility",
         lambda: soft_target_fragility_signal(db, args.start, args.end, universe=universe,
                                              sample_every=args.sample_every),
         "STAB soft-target fragility (trailing vol+drawdown+neg-trend); HIGH=fragile. "
         "Tests claim: fragility predicts negative forward return.",
         "Stored point-in-time score; deterministic trailing function of prices => replayable."),
        ("reversal_1w",
         lambda: short_term_reversal_signal(db, args.start, args.end, universe=universe,
                                            sample_every=args.sample_every),
         "Standard 1-week reversal: score = -(5d return). HIGH=recent loser.",
         "Pure past-price; no look-ahead. Standard benchmark factor."),
        ("low_vol_63d",
         lambda: low_vol_signal(db, args.start, args.end, universe=universe,
                                sample_every=args.sample_every),
         "Standard low-volatility factor: score = -(63d realized vol). HIGH=calm.",
         "Pure past-price; no look-ahead. Standard benchmark factor."),
    ]

    for name, build, desc, integ in builders:
        print("\n" + "#" * 78)
        print(f"# {name}")
        print("#" * 78)
        scores = build()
        if scores.empty:
            print("  SKIP: empty signal (no replayable data)")
            evidence.append((name, "cross-sectional", 21, float("nan"), "no data", "skip"))
            continue
        logger.info("%s: %d obs over %d dates", name, len(scores), scores["as_of_date"].nunique())
        panel = forward_return_panel(db, scores, horizons=HORIZONS, universe=None)
        report = evaluate_signal(scores, panel, name=name, horizons=HORIZONS, headline_horizon=21)
        print(report.headline())
        if not args.no_register:
            update_from_report(db, report, description=desc, bucket=1, integrity_note=integ)
        for h in HORIZONS:
            ic = report.ic.get(h)
            d = report.deciles.get(h)
            if ic:
                evidence.append((
                    name, "cross-sectional", h, ic.mean_ic,
                    f"t={ic.t_stat:+.2f} IC_vol={ic.mean_ic_vol:+.3f} "
                    f"dec={d.top_minus_bottom:+.3%}" if d else f"t={ic.t_stat:+.2f}",
                    report.verdict_hint,
                ))

    # ---- EVIDENCE TABLE -----------------------------------------------------
    print("\n" + "=" * 96)
    print("EVIDENCE TABLE — rank-IC by horizon (honest)")
    print("=" * 96)
    print(f"{'signal':28} {'kind':18} {'h':>4} {'IC':>9}  {'detail':38} {'verdict'}")
    print("-" * 96)
    for name, kind, h, ic, extra, verdict in evidence:
        ic_s = f"{ic:+.4f}" if ic == ic else "   nan"
        print(f"{name:28} {kind:18} {h:>4} {ic_s:>9}  {extra:38} {verdict}")
    print("=" * 96)


def _macro_verdict(ic_ret: float, ic_vol: float) -> str:
    if ic_ret != ic_ret:
        return "skip"
    if abs(ic_ret) >= 0.06:
        return "timing"
    if abs(ic_vol) >= 0.10 and abs(ic_vol) > abs(ic_ret):
        return "risk"
    return "shelve"


def _macro_what(ic_ret: float, ic_vol: float) -> str:
    if ic_ret != ic_ret:
        return "predicts-neither"
    if abs(ic_ret) >= 0.06:
        return "predicts-timing"
    if abs(ic_vol) >= 0.10 and abs(ic_vol) > abs(ic_ret):
        return "predicts-vol"
    return "predicts-neither"


if __name__ == "__main__":
    main()
