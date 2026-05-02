"""Prometheus v2 – Allocator backtest diagnostics & lead-time analysis.

This CLI is intended to answer questions like:
- What did the regime detector output on each day?
- What did fragility say, and how did it map into hedge allocation?
- Did we hedge *before* a bad regime/drawdown started (lead time)?

It reads per-day diagnostics from `backtest_daily_equity.exposure_metrics_json`
for a given `run_id`.

Examples
--------

Write per-day diagnostics CSV (stdout) and show summary to stderr:

    python -m prometheus.scripts.show.show_allocator_backtest_diagnostics \
        --run-id <RUN_ID> \
        --start 2020-02-01 --end 2020-04-30 \
        --market-instrument-id SPY.US \
        --lead-event crisis --lead-event dd20

"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import date
from statistics import mean
from typing import Any, Optional, Sequence

from apatheon.core.database import get_db_manager
from apatheon.core.logging import get_logger
from apatheon.data.reader import DataReader

logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except Exception as exc:  # pragma: no cover
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


def _to_float_or_none(value: object) -> float | None:
    if value is None:
        return None
    try:
        x = float(value)  # handles numpy scalars
    except Exception:
        return None
    if x != x:  # NaN
        return None
    return x


def _fmt(x: float | None) -> str:
    return "" if x is None else f"{float(x):.6f}"


def _parse_jsonb(value: object) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


@dataclass(frozen=True)
class _DiagRow:
    as_of_date: date

    market_situation: str | None
    regime_label: str | None
    prev_regime_label: str | None
    fragility_score: float | None

    hedge_allocation: float | None
    long_allocation: float | None
    cash_weight: float | None

    equity_curve_value: float | None
    drawdown: float | None

    # Market proxy stats.
    market_close: float | None
    market_ret_1d: float | None
    market_drawdown: float | None
    market_fwd_ret_5d: float | None
    market_fwd_ret_21d: float | None


def _lead_summary(
    *,
    label: str,
    onsets: list[int],
    dates: list[date],
    hedge: list[float | None],
    frag: list[float | None],
    lookback: int,
    hedge_threshold: float,
    fragility_threshold: float,
    min_consecutive: int,
) -> None:
    if not onsets:
        logger.info("lead_analysis[%s]: no onsets found", label)
        return

    leads: list[int] = []
    frag_leads: list[int] = []

    logger.info(
        "lead_analysis[%s]: onsets=%d lookback=%d hedge_threshold=%.3f fragility_threshold=%.3f",
        label,
        len(onsets),
        lookback,
        hedge_threshold,
        fragility_threshold,
    )

    for onset_idx in onsets:
        onset_date = dates[onset_idx]
        start_idx = max(0, onset_idx - lookback)

        def _first_idx_ge(series: list[float | None], threshold: float) -> int | None:
            # Find earliest index in [start_idx, onset_idx) where series is >= threshold for
            # at least min_consecutive consecutive days.
            if min_consecutive <= 1:
                for j in range(start_idx, onset_idx):
                    v = series[j]
                    if v is not None and float(v) >= threshold:
                        return j
                return None

            j = start_idx
            while j < onset_idx:
                ok = True
                for k in range(min_consecutive):
                    idx = j + k
                    if idx >= onset_idx:
                        ok = False
                        break
                    v = series[idx]
                    if v is None or float(v) < threshold:
                        ok = False
                        break
                if ok:
                    return j
                j += 1
            return None

        hedge_start = _first_idx_ge(hedge, hedge_threshold)
        frag_start = _first_idx_ge(frag, fragility_threshold)

        if hedge_start is not None:
            leads.append(onset_idx - hedge_start)
        if frag_start is not None:
            frag_leads.append(onset_idx - frag_start)

        pre = [x for x in (hedge[start_idx:onset_idx]) if x is not None]
        pre_avg = mean(pre) if pre else None

        logger.info(
            "lead_event[%s]: onset=%s hedge_start=%s lead_td=%s frag_start=%s frag_lead_td=%s pre_avg_hedge=%s",
            label,
            onset_date.isoformat(),
            dates[hedge_start].isoformat() if hedge_start is not None else "",
            str(onset_idx - hedge_start) if hedge_start is not None else "",
            dates[frag_start].isoformat() if frag_start is not None else "",
            str(onset_idx - frag_start) if frag_start is not None else "",
            _fmt(pre_avg),
        )

    if leads:
        logger.info(
            "lead_summary[%s]: hedge_lead_td min=%d max=%d mean=%.2f",
            label,
            min(leads),
            max(leads),
            mean(leads),
        )
    else:
        logger.info(
            "lead_summary[%s]: hedge_lead_td none (never hedged above threshold before onsets)",
            label,
        )

    if frag_leads:
        logger.info(
            "lead_summary[%s]: fragility_lead_td min=%d max=%d mean=%.2f",
            label,
            min(frag_leads),
            max(frag_leads),
            mean(frag_leads),
        )
    else:
        logger.info(
            "lead_summary[%s]: fragility_lead_td none (fragility never exceeded threshold before onsets)",
            label,
        )


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Allocator backtest diagnostics (situations, allocations, lead-time)",
    )

    parser.add_argument("--run-id", type=str, required=True)
    parser.add_argument("--start", type=_parse_date, default=None)
    parser.add_argument("--end", type=_parse_date, default=None)

    parser.add_argument(
        "--market-instrument-id",
        type=str,
        default="SPY.US",
        help="Instrument id used for market return/drawdown stats (default: SPY.US)",
    )

    parser.add_argument(
        "--fragility-threshold",
        type=float,
        default=None,
        help="Fragility threshold to treat as a 'signal' for lead analysis (default: inferred from exposure json, else 0.30)",
    )

    parser.add_argument(
        "--hedge-threshold",
        type=float,
        default=0.25,
        help="Hedge allocation threshold used for lead analysis (default: 0.25)",
    )

    parser.add_argument(
        "--lead-lookback",
        type=int,
        default=60,
        help="Lookback window in trading days for lead analysis (default: 60)",
    )

    parser.add_argument(
        "--min-consecutive",
        type=int,
        default=1,
        help="Require this many consecutive days meeting a threshold before counting as a signal (default: 1)",
    )

    parser.add_argument(
        "--lead-event",
        action="append",
        default=[],
        choices=["crisis", "risk_off", "dd10", "dd20"],
        help="Which onset events to analyze (repeatable).",
    )

    args = parser.parse_args(list(argv) if argv is not None else None)

    db = get_db_manager()

    # Resolve start/end from DB if omitted.
    sql_range = """
        SELECT MIN(date) AS min_date, MAX(date) AS max_date
        FROM backtest_daily_equity
        WHERE run_id = %s
    """

    min_date_db: date | None
    max_date_db: date | None

    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql_range, (str(args.run_id),))
            min_date_db, max_date_db = cur.fetchone()
        finally:
            cur.close()

    if min_date_db is None or max_date_db is None:
        logger.warning("No backtest_daily_equity rows found for run_id=%s", args.run_id)
        return 0

    start = args.start or min_date_db
    end = args.end or max_date_db

    sql = """
        SELECT date, equity_curve_value, drawdown, exposure_metrics_json
        FROM backtest_daily_equity
        WHERE run_id = %s
          AND date >= %s
          AND date <= %s
        ORDER BY date ASC
    """

    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, (str(args.run_id), start, end))
            rows = cur.fetchall()
        finally:
            cur.close()

    if not rows:
        logger.warning("No backtest_daily_equity rows found for run_id=%s between %s and %s", args.run_id, start, end)
        return 0

    # Market proxy prices.
    reader = DataReader(db_manager=db)
    prices_df = reader.read_prices_close(
        [str(args.market_instrument_id)],
        start,
        end,
        price_col="adjusted_close",
    )

    price_by_date: dict[date, float] = {}
    if not prices_df.empty:
        for inst, trade_date, close in zip(
            prices_df["instrument_id"].astype(str),
            prices_df["trade_date"],
            prices_df["close"],
        ):
            if str(inst) != str(args.market_instrument_id):
                continue
            d = trade_date if isinstance(trade_date, date) else None
            if d is None:
                continue
            fv = _to_float_or_none(close)
            if fv is None or fv <= 0:
                continue
            price_by_date[d] = float(fv)

    dates: list[date] = [d for d, *_ in rows if isinstance(d, date)]

    market_close: dict[date, float] = {}
    market_ret_1d: dict[date, float] = {}
    market_dd: dict[date, float] = {}
    market_fwd_5d: dict[date, float] = {}
    market_fwd_21d: dict[date, float] = {}

    priced_dates = [d for d in dates if d in price_by_date]
    priced_dates = sorted(dict.fromkeys(priced_dates))

    peak = None
    for i, d in enumerate(priced_dates):
        px = price_by_date[d]
        market_close[d] = px

        if i > 0:
            prev = price_by_date[priced_dates[i - 1]]
            market_ret_1d[d] = (px / prev) - 1.0 if prev > 0 else 0.0

        if peak is None or px > peak:
            peak = px
        if peak and peak > 0:
            market_dd[d] = (px / peak) - 1.0

        if i + 5 < len(priced_dates):
            px_fwd = price_by_date[priced_dates[i + 5]]
            market_fwd_5d[d] = (px_fwd / px) - 1.0 if px > 0 else 0.0

        if i + 21 < len(priced_dates):
            px_fwd = price_by_date[priced_dates[i + 21]]
            market_fwd_21d[d] = (px_fwd / px) - 1.0 if px > 0 else 0.0

    out_rows: list[_DiagRow] = []

    for as_of_date_db, equity_db, dd_db, exposure_raw in rows:
        if not isinstance(as_of_date_db, date):
            continue

        exposure = _parse_jsonb(exposure_raw)

        sit = exposure.get("market_situation")
        regime = exposure.get("regime_label")
        prev_regime = exposure.get("prev_regime_label")

        frag = _to_float_or_none(exposure.get("fragility_score"))
        hedge_alloc = _to_float_or_none(exposure.get("hedge_allocation"))
        long_alloc = _to_float_or_none(exposure.get("long_allocation"))
        cash_weight = _to_float_or_none(exposure.get("cash_weight"))

        out_rows.append(
            _DiagRow(
                as_of_date=as_of_date_db,
                market_situation=str(sit) if sit is not None else None,
                regime_label=str(regime) if regime is not None else None,
                prev_regime_label=str(prev_regime) if prev_regime is not None else None,
                fragility_score=frag,
                hedge_allocation=hedge_alloc,
                long_allocation=long_alloc,
                cash_weight=cash_weight,
                equity_curve_value=_to_float_or_none(equity_db),
                drawdown=_to_float_or_none(dd_db),
                market_close=_to_float_or_none(market_close.get(as_of_date_db)),
                market_ret_1d=_to_float_or_none(market_ret_1d.get(as_of_date_db)),
                market_drawdown=_to_float_or_none(market_dd.get(as_of_date_db)),
                market_fwd_ret_5d=_to_float_or_none(market_fwd_5d.get(as_of_date_db)),
                market_fwd_ret_21d=_to_float_or_none(market_fwd_21d.get(as_of_date_db)),
            )
        )

    # Infer fragility threshold for lead analysis.
    frag_thr = 0.30
    if args.fragility_threshold is not None:
        frag_thr = float(args.fragility_threshold)
    else:
        for _, _, _, exposure_raw in rows:
            exposure = _parse_jsonb(exposure_raw)
            ft = _to_float_or_none(exposure.get("fragility_threshold"))
            if ft is not None:
                frag_thr = float(ft)
                break

    # Emit CSV.
    print(
        "as_of_date,run_id,market_situation,regime_label,prev_regime_label,fragility_score,"
        "hedge_allocation,long_allocation,cash_weight,equity_curve_value,drawdown,"
        "market_instrument_id,market_close,market_ret_1d,market_drawdown,market_fwd_ret_5d,market_fwd_ret_21d"
    )

    for r in out_rows:
        print(
            f"{r.as_of_date.isoformat()},{args.run_id},"
            f"{r.market_situation or ''},{r.regime_label or ''},{r.prev_regime_label or ''},{_fmt(r.fragility_score)},"
            f"{_fmt(r.hedge_allocation)},{_fmt(r.long_allocation)},{_fmt(r.cash_weight)},"
            f"{_fmt(r.equity_curve_value)},{_fmt(r.drawdown)},"
            f"{args.market_instrument_id},{_fmt(r.market_close)},{_fmt(r.market_ret_1d)},{_fmt(r.market_drawdown)},"
            f"{_fmt(r.market_fwd_ret_5d)},{_fmt(r.market_fwd_ret_21d)}"
        )

    # Summary stats (printed to stderr via logger).
    by_sit: dict[str, list[_DiagRow]] = {}
    for r in out_rows:
        key = r.market_situation or ""
        by_sit.setdefault(key, []).append(r)

    if by_sit:
        logger.info(
            "situation_summary: run_id=%s market_instrument_id=%s rows=%d",
            args.run_id,
            args.market_instrument_id,
            len(out_rows),
        )
        for sit_key in sorted(by_sit.keys()):
            grp = by_sit[sit_key]

            def _avg(vals: list[float | None]) -> float | None:
                xs = [float(x) for x in vals if x is not None]
                return mean(xs) if xs else None

            def _min(vals: list[float | None]) -> float | None:
                xs = [float(x) for x in vals if x is not None]
                return min(xs) if xs else None

            logger.info(
                "situation=%s days=%d avg_hedge=%s avg_frag=%s avg_ret_1d=%s min_dd=%s",
                sit_key or "(missing)",
                len(grp),
                _fmt(_avg([x.hedge_allocation for x in grp])),
                _fmt(_avg([x.fragility_score for x in grp])),
                _fmt(_avg([x.market_ret_1d for x in grp])),
                _fmt(_min([x.market_drawdown for x in grp])),
            )

    # Lead-time analysis summary to stderr.
    if args.lead_event:
        dates2 = [r.as_of_date for r in out_rows]
        situations = [r.market_situation for r in out_rows]
        hedges = [r.hedge_allocation for r in out_rows]
        frags = [r.fragility_score for r in out_rows]
        dds = [r.market_drawdown for r in out_rows]

        def _onsets_for(label_value: str) -> list[int]:
            out: list[int] = []
            for i, s in enumerate(situations):
                if s != label_value:
                    continue
                if i == 0 or situations[i - 1] != label_value:
                    out.append(i)
            return out

        def _onsets_for_dd(thr: float) -> list[int]:
            out: list[int] = []
            for i, dd in enumerate(dds):
                if dd is None:
                    continue
                if float(dd) > thr:
                    continue
                prev_ok = True
                if i > 0 and dds[i - 1] is not None:
                    prev_ok = float(dds[i - 1]) > thr
                if i == 0 or prev_ok:
                    out.append(i)
            return out

        for ev in args.lead_event:
            if ev == "crisis":
                _lead_summary(
                    label="CRISIS",
                    onsets=_onsets_for("CRISIS"),
                    dates=dates2,
                    hedge=hedges,
                    frag=frags,
                    lookback=int(args.lead_lookback),
                    hedge_threshold=float(args.hedge_threshold),
                    fragility_threshold=float(frag_thr),
                    min_consecutive=int(args.min_consecutive),
                )
            elif ev == "risk_off":
                _lead_summary(
                    label="RISK_OFF",
                    onsets=_onsets_for("RISK_OFF"),
                    dates=dates2,
                    hedge=hedges,
                    frag=frags,
                    lookback=int(args.lead_lookback),
                    hedge_threshold=float(args.hedge_threshold),
                    fragility_threshold=float(frag_thr),
                    min_consecutive=int(args.min_consecutive),
                )
            elif ev == "dd10":
                _lead_summary(
                    label="DD10",
                    onsets=_onsets_for_dd(-0.10),
                    dates=dates2,
                    hedge=hedges,
                    frag=frags,
                    lookback=int(args.lead_lookback),
                    hedge_threshold=float(args.hedge_threshold),
                    fragility_threshold=float(frag_thr),
                    min_consecutive=int(args.min_consecutive),
                )
            elif ev == "dd20":
                _lead_summary(
                    label="DD20",
                    onsets=_onsets_for_dd(-0.20),
                    dates=dates2,
                    hedge=hedges,
                    frag=frags,
                    lookback=int(args.lead_lookback),
                    hedge_threshold=float(args.hedge_threshold),
                    fragility_threshold=float(frag_thr),
                    min_consecutive=int(args.min_consecutive),
                )

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
