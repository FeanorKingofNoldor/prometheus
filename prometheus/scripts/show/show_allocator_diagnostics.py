"""Prometheus v2 – Allocator diagnostics & lead-time analysis.

This CLI is intended to answer questions like:
- What did the regime detector output on each day?
- What did fragility say, and how did it map into hedge allocation?
- Did we hedge *before* a bad regime/drawdown started (lead time)?

It reads allocator targets from `target_portfolios` and enriches each row with:
- MarketSituationInfo (situation, regime_label/confidence, fragility score/class)
- Market proxy price stats (1d return, drawdown, forward returns)

It also prints a summary lead-time analysis to stderr.

Examples
--------

Write per-day diagnostics CSV (stdout) and show summary to stderr:

    python -m prometheus.scripts.show.show_allocator_diagnostics \
        --portfolio-id US_EQ_ALLOCATOR \
        --market-id US_EQ \
        --start 2020-02-01 --end 2020-04-30 \
        --market-instrument-id SPY.US \
        --lead-event crisis --lead-event dd20

"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date
from statistics import mean
from typing import Any, Optional, Sequence

from apathis.core.database import get_db_manager
from apathis.core.logging import get_logger
from apathis.data.reader import DataReader

from prometheus.books.registry import BookKind, load_book_registry
from prometheus.meta.market_situation import MarketSituationService

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


@dataclass(frozen=True)
class _DiagRow:
    as_of_date: date
    created_at_s: str

    # Situation inputs.
    market_situation: str | None
    regime_label: str | None
    regime_confidence: float | None
    prev_regime_label: str | None
    fragility_score: float | None
    fragility_class: str | None

    # Allocations.
    hedge_allocation: float | None
    long_allocation: float | None
    cash_weight: float | None

    # Market proxy stats.
    market_close: float | None
    market_ret_1d: float | None
    market_drawdown: float | None
    market_fwd_ret_5d: float | None
    market_fwd_ret_21d: float | None


def _extract_weights(positions_raw: object) -> dict[str, float]:
    positions = positions_raw if isinstance(positions_raw, dict) else {}
    weights_raw = positions.get("weights") if isinstance(positions.get("weights"), dict) else {}
    out: dict[str, float] = {}
    for k, v in weights_raw.items():
        fv = _to_float_or_none(v)
        if fv is None:
            continue
        out[str(k)] = float(fv)
    return out


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

    logger.info("lead_analysis[%s]: onsets=%d lookback=%d hedge_threshold=%.3f fragility_threshold=%.3f",
                label, len(onsets), lookback, hedge_threshold, fragility_threshold)

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

        # Pre-window stats.
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
        logger.info("lead_summary[%s]: hedge_lead_td none (never hedged above threshold before onsets)", label)

    if frag_leads:
        logger.info(
            "lead_summary[%s]: fragility_lead_td min=%d max=%d mean=%.2f",
            label,
            min(frag_leads),
            max(frag_leads),
            mean(frag_leads),
        )
    else:
        logger.info("lead_summary[%s]: fragility_lead_td none (fragility never exceeded threshold before onsets)", label)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Allocator diagnostics (situations, allocations, lead-time)",
    )

    parser.add_argument("--portfolio-id", type=str, required=True)
    parser.add_argument("--market-id", type=str, default="US_EQ")
    parser.add_argument("--region", type=str, default="US")
    parser.add_argument("--start", type=_parse_date, required=True)
    parser.add_argument("--end", type=_parse_date, required=True)

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
        help="Fragility threshold to treat as a 'signal' for lead analysis (default: from allocator sleeve if available, else 0.30)",
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

    # Best-effort: infer allocator sleeve parameters so the CLI 'just works'.
    registry = load_book_registry()
    hedge_ids: tuple[str, ...] = ()
    sleeve_frag_thr: float | None = None

    book = registry.get(str(args.portfolio_id))
    if book is not None and book.kind == BookKind.ALLOCATOR:
        sid = book.default_sleeve_id
        if sid and sid in book.sleeves:
            sleeve = book.sleeves[sid]
            hedge_ids = getattr(sleeve, "hedge_instrument_ids", ()) or ()
            try:
                sleeve_frag_thr = float(getattr(sleeve, "fragility_threshold", 0.30))
            except Exception:
                sleeve_frag_thr = None

    frag_thr = float(args.fragility_threshold) if args.fragility_threshold is not None else float(sleeve_frag_thr or 0.30)

    db = get_db_manager()

    # Load allocator targets from runtime DB.
    sql = """
        SELECT DISTINCT ON (as_of_date)
            as_of_date,
            created_at,
            target_positions,
            metadata
        FROM target_portfolios
        WHERE portfolio_id = %s
          AND as_of_date >= %s
          AND as_of_date <= %s
        ORDER BY as_of_date ASC, created_at DESC
    """

    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, (str(args.portfolio_id), args.start, args.end))
            rows = cur.fetchall()
        finally:
            cur.close()

    if not rows:
        logger.warning("No target_portfolios rows found for portfolio_id=%s between %s and %s",
                       args.portfolio_id, args.start, args.end)
        return 0

    # Load market proxy prices from historical DB.
    reader = DataReader(db_manager=db)
    prices_df = reader.read_prices_close(
        [str(args.market_instrument_id)],
        args.start,
        args.end,
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
            if fv is None:
                continue
            if fv <= 0:
                continue
            price_by_date[d] = float(fv)

    # Build a price-aligned list over the dates we actually have portfolio rows for.
    dates: list[date] = [r[0] for r in rows if isinstance(r[0], date)]

    # Precompute market stats keyed by date.
    market_close: dict[date, float] = {}
    market_ret_1d: dict[date, float] = {}
    market_dd: dict[date, float] = {}
    market_fwd_5d: dict[date, float] = {}
    market_fwd_21d: dict[date, float] = {}

    # Use only dates that have prices.
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

    svc = MarketSituationService(db_manager=db)

    out_rows: list[_DiagRow] = []

    for as_of_date_db, created_at_db, positions_raw, meta_raw in rows:
        if not isinstance(as_of_date_db, date):
            continue

        created_s = getattr(created_at_db, "isoformat", lambda: "")()
        meta: dict[str, Any] = meta_raw if isinstance(meta_raw, dict) else {}
        weights = _extract_weights(positions_raw)

        # Situation inputs (computed live to avoid depending on what the book stored).
        info = svc.get_situation(
            market_id=str(args.market_id),
            as_of_date=as_of_date_db,
            region=str(args.region),
        )

        # Allocations: prefer metadata; fall back to summing hedge weights when possible.
        hedge_alloc = _to_float_or_none(meta.get("hedge_allocation"))
        if hedge_alloc is None and hedge_ids:
            hedge_alloc = float(sum(weights.get(i, 0.0) for i in hedge_ids))

        long_alloc = _to_float_or_none(meta.get("long_allocation"))
        cash_weight = _to_float_or_none(meta.get("cash_weight"))

        out_rows.append(
            _DiagRow(
                as_of_date=as_of_date_db,
                created_at_s=str(created_s),
                market_situation=info.situation.value if info is not None else None,
                regime_label=info.regime_label.value if info.regime_label else None,
                regime_confidence=_to_float_or_none(info.regime_confidence),
                prev_regime_label=info.prev_regime_label.value if info.prev_regime_label else None,
                fragility_score=_to_float_or_none(info.fragility_score),
                fragility_class=str(info.fragility_class) if info.fragility_class else None,
                hedge_allocation=_to_float_or_none(hedge_alloc),
                long_allocation=_to_float_or_none(long_alloc),
                cash_weight=_to_float_or_none(cash_weight),
                market_close=_to_float_or_none(market_close.get(as_of_date_db)),
                market_ret_1d=_to_float_or_none(market_ret_1d.get(as_of_date_db)),
                market_drawdown=_to_float_or_none(market_dd.get(as_of_date_db)),
                market_fwd_ret_5d=_to_float_or_none(market_fwd_5d.get(as_of_date_db)),
                market_fwd_ret_21d=_to_float_or_none(market_fwd_21d.get(as_of_date_db)),
            )
        )

    # Emit CSV.
    print(
        "as_of_date,created_at,portfolio_id,market_situation,regime_label,regime_confidence,prev_regime_label,"
        "fragility_score,fragility_class,hedge_allocation,long_allocation,cash_weight,"
        "market_instrument_id,market_close,market_ret_1d,market_drawdown,market_fwd_ret_5d,market_fwd_ret_21d"
    )

    for r in out_rows:
        print(
            f"{r.as_of_date.isoformat()},{r.created_at_s},{args.portfolio_id},"
            f"{r.market_situation or ''},{r.regime_label or ''},{_fmt(r.regime_confidence)},{r.prev_regime_label or ''},"
            f"{_fmt(r.fragility_score)},{r.fragility_class or ''},"
            f"{_fmt(r.hedge_allocation)},{_fmt(r.long_allocation)},{_fmt(r.cash_weight)},"
            f"{args.market_instrument_id},{_fmt(r.market_close)},{_fmt(r.market_ret_1d)},{_fmt(r.market_drawdown)},"
            f"{_fmt(r.market_fwd_ret_5d)},{_fmt(r.market_fwd_ret_21d)}"
        )

    # Quick per-situation summary stats (printed to stderr via logger).
    by_sit: dict[str, list[_DiagRow]] = {}
    for r in out_rows:
        key = r.market_situation or ""
        by_sit.setdefault(key, []).append(r)

    if by_sit:
        logger.info("situation_summary: portfolio_id=%s market_id=%s market_instrument_id=%s rows=%d",
                    args.portfolio_id, args.market_id, args.market_instrument_id, len(out_rows))
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
