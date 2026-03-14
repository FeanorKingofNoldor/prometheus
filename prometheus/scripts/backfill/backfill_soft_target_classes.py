"""Backfill STAB soft-target classes (soft_target_classes) for a market/date range.

Why this exists
---------------

Several research workflows (e.g. lambda/opportunity-density cluster backfills)
need a historical record of STAB classes per instrument. Earlier scripts
joined against ``soft_target_classes`` but that table only had partial
coverage, causing ``soft_target_class=UNKNOWN`` and making lambda have
no effect.

This CLI computes instrument-level STAB states from historical prices and
persists them into the runtime DB.

Computation contract
--------------------

The stability computation mirrors the C++ implementation in
`cpp/src/engines/stability.cpp`:

- Inputs: a rolling window of `window_days` closes.
- Requires all closes in the window be positive (no missing rows).
- Features: sigma(log returns), max drawdown, trend.
- Component scores and class thresholds match Python BasicPriceStabilityModel.

Performance notes
-----------------

- This implementation is vectorized per-instrument using NumPy sliding
  windows. It is still a potentially large backfill.
- Writes are batched with psycopg2 execute_values.

Report
------

If --report-out is provided, a JSON report is written with:
- counts (instruments, dates, rows inserted)
- coverage per day (rows / instruments-with-prices-on-day)
- post_warmup_frac_written (average coverage after `window_days` trading days)

"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np
from psycopg2.extras import Json, execute_values

from apathis.core.config import get_config
from apathis.core.database import DatabaseManager
from apathis.core.ids import generate_uuid
from apathis.core.logging import get_logger


logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:  # pragma: no cover
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


@dataclass(frozen=True)
class _StabCfg:
    window_days: int
    vol_ref: float = 0.02
    dd_ref: float = 0.20
    trend_ref: float = 0.20
    vol_weight: float = 0.4
    dd_weight: float = 0.4
    trend_weight: float = 0.2


_SOFT_LABELS = np.array(["STABLE", "WATCH", "FRAGILE", "TARGETABLE", "BREAKER"], dtype=object)
_SOFT_BINS = np.array([30.0, 45.0, 60.0, 75.0], dtype=float)


def _load_active_equity_instruments(
    db_manager: DatabaseManager,
    *,
    market_id: str,
    instrument_limit: int,
    exclude_synthetic: bool = False,
) -> List[str]:
    sql = """
        SELECT instrument_id
        FROM instruments
        WHERE market_id = %s
          AND asset_class = 'EQUITY'
          AND status = 'ACTIVE'
    """

    if exclude_synthetic:
        sql += "  AND instrument_id NOT LIKE 'SYNTH\\_%%'\n"

    sql += "        ORDER BY instrument_id\n"

    if instrument_limit > 0:
        sql += " LIMIT %s"
        params = (market_id, int(instrument_limit))
    else:
        params = (market_id,)

    with db_manager.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, params)
            rows = cur.fetchall()
        finally:
            cur.close()

    return [str(r[0]) for r in rows]


def _load_trading_days_from_prices(
    db_manager: DatabaseManager,
    *,
    instrument_ids: Sequence[str],
    start_date: date,
    end_date: date,
    min_instruments_per_day: int,
) -> List[date]:
    """Return a trading-day calendar derived from prices_daily.

    prices_daily does not include market_id, so we derive a calendar from the
    provided instrument universe and drop obvious outlier dates where only a
    tiny number of instruments have prices (e.g. bad rows on market holidays).

    These outliers can break rolling-window logic that assumes every
    "trading day" is a real session.
    """

    if not instrument_ids:
        return []

    min_instruments_per_day = int(min_instruments_per_day)
    if min_instruments_per_day <= 0:
        raise ValueError("min_instruments_per_day must be > 0")

    sql = """
        SELECT trade_date, COUNT(DISTINCT instrument_id) AS n
        FROM prices_daily
        WHERE instrument_id IN %s
          AND trade_date BETWEEN %s AND %s
        GROUP BY trade_date
        ORDER BY trade_date
    """

    with db_manager.get_historical_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, (tuple(instrument_ids), start_date, end_date))
            rows = cur.fetchall()
        finally:
            cur.close()

    days_all = [r[0] for r in rows]
    days = [d for d, n in rows if int(n) >= min_instruments_per_day]

    dropped = len(days_all) - len(days)
    if dropped > 0:
        logger.warning(
            "Dropped %d outlier dates from trading calendar (min_instruments_per_day=%d)",
            dropped,
            min_instruments_per_day,
        )

    return days


def _load_closes_for_instrument(
    db_manager: DatabaseManager,
    *,
    instrument_id: str,
    start_date: date,
    end_date: date,
) -> List[Tuple[date, float]]:
    sql = """
        SELECT trade_date, close
        FROM prices_daily
        WHERE instrument_id = %s
          AND trade_date BETWEEN %s AND %s
        ORDER BY trade_date
    """

    with db_manager.get_historical_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, (instrument_id, start_date, end_date))
            rows = cur.fetchall()
        finally:
            cur.close()

    out: List[Tuple[date, float]] = []
    for d, c in rows:
        try:
            out.append((d, float(c)))
        except Exception:
            out.append((d, 0.0))
    return out


def _compute_stability_for_closes(
    closes: np.ndarray,
    *,
    cfg: _StabCfg,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Vectorized stability computation for many windows.

    Args:
        closes: 2D array [n_windows, window_days]
        cfg: stability config

    Returns:
        (overall, soft_class_str, vol_score, dd_score, trend_score, max_dd)
        as arrays aligned with rows of `closes`.
    """

    # Trend.
    first = closes[:, 0]
    last = closes[:, -1]
    trend = np.where(first > 0.0, (last - first) / first, 0.0)

    # Sigma of log returns.
    with np.errstate(divide="ignore", invalid="ignore"):
        log_rets = np.log(closes[:, 1:] / closes[:, :-1])
    # Filter non-finite (should not happen if closes are all >0, but be defensive).
    log_rets = np.where(np.isfinite(log_rets), log_rets, 0.0)
    sigma = log_rets.std(axis=1, ddof=1)

    # Max drawdown.
    running_max = np.maximum.accumulate(closes, axis=1)
    with np.errstate(divide="ignore", invalid="ignore"):
        dd = closes / running_max - 1.0
    dd = np.where(np.isfinite(dd), dd, 0.0)
    max_dd = dd.min(axis=1)

    # Component scores (match python/cpp scaling).
    vol_score = np.zeros_like(sigma)
    if cfg.vol_ref > 0.0:
        vol_score = np.clip((sigma / cfg.vol_ref) * 50.0, 0.0, 100.0)

    dd_mag = np.abs(max_dd)
    dd_score = np.zeros_like(dd_mag)
    if cfg.dd_ref > 0.0:
        dd_score = np.clip((dd_mag / cfg.dd_ref) * 50.0, 0.0, 100.0)

    trend_score = np.zeros_like(trend)
    if cfg.trend_ref > 0.0:
        trend_score = np.where(
            trend < 0.0,
            np.clip((np.abs(trend) / cfg.trend_ref) * 50.0, 0.0, 100.0),
            0.0,
        )

    total_w = cfg.vol_weight + cfg.dd_weight + cfg.trend_weight
    if total_w > 0.0:
        overall = (
            cfg.vol_weight * vol_score + cfg.dd_weight * dd_score + cfg.trend_weight * trend_score
        ) / total_w
    else:
        overall = (vol_score + dd_score + trend_score) / 3.0

    overall = np.clip(overall, 0.0, 100.0)

    # Classify.
    # digitize gives 0..4 for bins [30,45,60,75] with right=False.
    idx = np.digitize(overall, _SOFT_BINS, right=False)
    soft_class = _SOFT_LABELS[idx]

    return overall, soft_class, vol_score, dd_score, trend_score, max_dd


def _delete_existing(
    db_manager: DatabaseManager,
    *,
    market_id: str,
    start_date: date,
    end_date: date,
) -> int:
    """Delete existing soft_target_classes rows for active equity instruments in market/date range."""

    sql = """
        DELETE FROM soft_target_classes AS st
        USING instruments AS i
        WHERE st.entity_type = 'INSTRUMENT'
          AND st.entity_id = i.instrument_id
          AND i.market_id = %s
          AND i.asset_class = 'EQUITY'
          AND i.status = 'ACTIVE'
          AND st.as_of_date BETWEEN %s AND %s
    """

    with db_manager.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, (market_id, start_date, end_date))
            deleted = int(cur.rowcount or 0)
            conn.commit()
        finally:
            cur.close()

    return deleted


def _insert_rows(
    db_manager: DatabaseManager,
    *,
    rows: List[Tuple[Any, ...]],
) -> None:
    if not rows:
        return

    sql = """
        INSERT INTO soft_target_classes (
            soft_target_id,
            entity_type,
            entity_id,
            as_of_date,
            soft_target_class,
            soft_target_score,
            weak_profile,
            instability,
            high_fragility,
            complacent_pricing,
            metadata,
            created_at
        ) VALUES %s
    """

    # We set created_at explicitly to NOW() so execute_values can expand values.
    template = "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())"

    with db_manager.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            execute_values(cur, sql, rows, template=template, page_size=5000)
            conn.commit()
        finally:
            cur.close()


def _compute_coverage_report(
    db_manager: DatabaseManager,
    *,
    market_id: str,
    instrument_ids: Sequence[str],
    start_date: date,
    end_date: date,
    window_days: int,
    min_instruments_per_day: int,
) -> Dict[str, Any]:
    """Compute STAB coverage as (rows_written / instruments_with_prices_on_day)."""

    n_inst = int(len(instrument_ids))
    min_instruments_per_day = int(min_instruments_per_day)

    # Per-day price availability for this instrument universe.
    sql_price_counts = """
        SELECT trade_date, COUNT(DISTINCT instrument_id) AS n
        FROM prices_daily
        WHERE instrument_id IN %s
          AND trade_date BETWEEN %s AND %s
        GROUP BY trade_date
        ORDER BY trade_date
    """

    # Per-day STAB rows.
    sql_stab_counts = """
        SELECT st.as_of_date, COUNT(*)
        FROM soft_target_classes AS st
        JOIN instruments AS i
          ON i.instrument_id = st.entity_id
        WHERE st.entity_type = 'INSTRUMENT'
          AND i.market_id = %s
          AND i.asset_class = 'EQUITY'
          AND i.status = 'ACTIVE'
          AND st.as_of_date BETWEEN %s AND %s
        GROUP BY st.as_of_date
        ORDER BY st.as_of_date
    """

    with db_manager.get_historical_connection() as hconn:
        cur = hconn.cursor()
        try:
            cur.execute(sql_price_counts, (tuple(instrument_ids), start_date, end_date))
            price_rows = cur.fetchall()
        finally:
            cur.close()

    # Filter out outlier dates to match the calendar used for the backfill.
    days = [d for d, n in price_rows if int(n) >= min_instruments_per_day]
    price_by_day: Dict[str, int] = {
        str(d): int(n) for d, n in price_rows if int(n) >= min_instruments_per_day
    }

    with db_manager.get_runtime_connection() as rconn:
        cur = rconn.cursor()
        try:
            cur.execute(sql_stab_counts, (market_id, start_date, end_date))
            stab_rows = cur.fetchall()
        finally:
            cur.close()

    stab_by_day: Dict[str, int] = {str(d): int(n) for d, n in stab_rows}

    per_day: Dict[str, float] = {}
    for d in days:
        key = str(d)
        n_price = price_by_day.get(key, 0)
        n_stab = stab_by_day.get(key, 0)
        per_day[key] = float(n_stab) / float(n_price) if n_price > 0 else 0.0

    # Compute post-warmup coverage metrics.
    days_sorted = [str(d) for d in days]
    post_days = days_sorted[window_days:] if window_days > 0 else days_sorted

    if post_days:
        post_cov = [per_day[d] for d in post_days]
        post_avg = float(sum(post_cov) / len(post_cov))
        post_min = float(min(post_cov))
    else:
        post_avg = 0.0
        post_min = 0.0

    return {
        "num_instruments": n_inst,
        "min_instruments_per_day": min_instruments_per_day,
        "coverage_by_day": per_day,
        "post_warmup_frac_written": post_avg,
        "post_warmup_min_coverage": post_min,
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Backfill soft_target_classes (STAB) for active equity instruments",
    )

    parser.add_argument("--market-id", type=str, required=True)
    parser.add_argument("--start", type=_parse_date, required=True)
    parser.add_argument("--end", type=_parse_date, required=True)

    parser.add_argument("--window-days", type=int, default=63)
    parser.add_argument(
        "--history-lookback-calendar-days",
        type=int,
        default=0,
        help="Lookback (calendar days) used to include prior history for the first as_of_date. Default: 3*window_days.",
    )

    parser.add_argument(
        "--instrument-limit",
        type=int,
        default=0,
        help="Optional limit of instruments to backfill (debug/smoke).",
    )

    parser.add_argument(
        "--on-conflict",
        type=str,
        choices=["replace", "skip"],
        default="replace",
        help="How to handle existing rows in the date range (default: replace).",
    )

    parser.add_argument(
        "--report-out",
        type=str,
        default=None,
        help="Optional path to write a JSON report.",
    )

    parser.add_argument(
        "--progress",
        action="store_true",
        help="Show an interactive progress bar while processing instruments.",
    )
    parser.add_argument(
        "--exclude-synthetic",
        action="store_true",
        help="Exclude SYNTH_ instruments from the backfill (use only real instruments).",
    )

    args = parser.parse_args(argv)

    start_date: date = args.start
    end_date: date = args.end
    if end_date < start_date:
        parser.error("--end must be >= --start")

    window_days = int(args.window_days)
    if window_days <= 1:
        parser.error("--window-days must be > 1")


    lookback_cal_days = int(args.history_lookback_calendar_days)
    if lookback_cal_days <= 0:
        lookback_cal_days = window_days * 3

    cfg = _StabCfg(window_days=window_days)

    config = get_config()
    db_manager = DatabaseManager(config)

    instrument_ids = _load_active_equity_instruments(
        db_manager,
        market_id=str(args.market_id),
        instrument_limit=int(args.instrument_limit),
        exclude_synthetic=bool(getattr(args, 'exclude_synthetic', False)),
    )

    if not instrument_ids:
        logger.warning("No active equity instruments for market_id=%s; nothing to do", args.market_id)
        return

    buffer_start = start_date - timedelta(days=lookback_cal_days)

    if len(instrument_ids) <= 2:
        min_instruments_per_day = 1
    else:
        # Drop outlier dates where only a tiny fraction of instruments have prices.
        min_instruments_per_day = max(2, len(instrument_ids) // 20)  # ~5%

    trading_days = _load_trading_days_from_prices(
        db_manager,
        instrument_ids=instrument_ids,
        start_date=buffer_start,
        end_date=end_date,
        min_instruments_per_day=min_instruments_per_day,
    )

    if not trading_days:
        logger.warning("No trading days found in prices_daily between %s and %s", buffer_start, end_date)
        return

    # Date indices.
    date_index: Dict[date, int] = {d: i for i, d in enumerate(trading_days)}

    # Determine [start, end] indices in trading_days.
    start_idx = next((i for i, d in enumerate(trading_days) if d >= start_date), None)
    end_idx = next((i for i, d in reversed(list(enumerate(trading_days))) if d <= end_date), None)

    if start_idx is None or end_idx is None or end_idx < start_idx:
        logger.warning("No trading days in requested range %s..%s", start_date, end_date)
        return

    logger.info(
        "STAB backfill: market=%s instruments=%d dates=%d (%s..%s) window_days=%d buffer_start=%s",
        args.market_id,
        len(instrument_ids),
        end_idx - start_idx + 1,
        trading_days[start_idx],
        trading_days[end_idx],
        window_days,
        buffer_start,
    )

    deleted = 0
    if str(args.on_conflict) == "replace":
        deleted = _delete_existing(
            db_manager,
            market_id=str(args.market_id),
            start_date=start_date,
            end_date=end_date,
        )
        logger.info("Deleted %d existing soft_target_classes rows (replace mode)", deleted)

    total_inserted = 0

    # Optional progress bar.
    show_progress = bool(getattr(args, "progress", False))
    progress_interactive = show_progress and sys.stderr.isatty()
    bar_width = 28

    def _fmt_hhmmss(seconds: float) -> str:
        seconds = max(0.0, float(seconds))
        h = int(seconds // 3600)
        m = int((seconds % 3600) // 60)
        s = int(seconds % 60)
        if h > 0:
            return f"{h:d}:{m:02d}:{s:02d}"
        return f"{m:02d}:{s:02d}"

    def _render_progress(done: int, total: int, *, current: str, t0: float) -> None:
        if not show_progress or total <= 0:
            return

        pct = done * 100.0 / float(total)
        filled = int(bar_width * done / float(total))
        filled = max(0, min(bar_width, filled))
        bar = "#" * filled + "-" * (bar_width - filled)

        elapsed = time.perf_counter() - t0
        eta = 0.0
        if done > 0 and total > done:
            eta = (elapsed / float(done)) * float(total - done)

        msg = (
            f"STAB backfill [{bar}] {done}/{total} ({pct:5.1f}%) "
            f"elapsed={_fmt_hhmmss(elapsed)} eta={_fmt_hhmmss(eta)} current={current}"
        )

        if progress_interactive:
            sys.stderr.write("\r" + msg + "\x1b[K")
            sys.stderr.flush()
        else:
            sys.stderr.write(msg + "\n")
            sys.stderr.flush()

    t0 = time.perf_counter()

    # Process instruments.
    total_instruments = len(instrument_ids)
    for idx_inst, inst_id in enumerate(instrument_ids):
        # Update progress before we do any heavy I/O for this instrument.
        _render_progress(idx_inst + 1, total_instruments, current=inst_id, t0=t0)

        if (idx_inst + 1) % 100 == 0 or idx_inst == 0:
            logger.info("Processing instrument %d/%d: %s", idx_inst + 1, total_instruments, inst_id)

        price_rows = _load_closes_for_instrument(
            db_manager,
            instrument_id=inst_id,
            start_date=buffer_start,
            end_date=end_date,
        )
        if not price_rows:
            continue

        closes_full = np.zeros(len(trading_days), dtype=float)
        for d, c in price_rows:
            j = date_index.get(d)
            if j is None:
                continue
            closes_full[j] = float(c)

        if closes_full.shape[0] < window_days:
            continue

        windows = np.lib.stride_tricks.sliding_window_view(closes_full, window_days)
        # windows index i corresponds to end date index t_end = i + window_days - 1
        valid = (windows > 0.0).all(axis=1)

        # Range of window indices whose end date is within [start_idx, end_idx]
        i0 = max(0, start_idx - (window_days - 1))
        i1 = min(windows.shape[0] - 1, end_idx - (window_days - 1))
        if i1 < i0:
            continue

        idx_range = np.arange(i0, i1 + 1)
        idx_ok = idx_range[valid[idx_range]]
        if idx_ok.size == 0:
            continue

        windows_ok = windows[idx_ok]

        overall, soft_class, vol_score, dd_score, trend_score, _max_dd = _compute_stability_for_closes(
            windows_ok,
            cfg=cfg,
        )

        # Build DB rows.
        metadata = {"window_days": window_days}
        meta_json = Json(metadata)

        rows_to_insert: List[Tuple[Any, ...]] = []

        for k in range(idx_ok.size):
            t_end = int(idx_ok[k] + (window_days - 1))
            as_of = trading_days[t_end]
            if as_of < start_date or as_of > end_date:
                continue

            rows_to_insert.append(
                (
                    generate_uuid(),
                    "INSTRUMENT",
                    inst_id,
                    as_of,
                    str(soft_class[k]),
                    float(overall[k]),
                    False,
                    float(vol_score[k]),
                    float(dd_score[k]),
                    float(trend_score[k]),
                    meta_json,
                )
            )

        if not rows_to_insert:
            continue

        if str(args.on_conflict) == "skip":
            # Filter out existing keys (best-effort idempotency without a unique constraint).
            sql_existing = """
                SELECT as_of_date
                FROM soft_target_classes
                WHERE entity_type = 'INSTRUMENT'
                  AND entity_id = %s
                  AND as_of_date BETWEEN %s AND %s
            """
            with db_manager.get_runtime_connection() as conn:
                cur = conn.cursor()
                try:
                    cur.execute(sql_existing, (inst_id, start_date, end_date))
                    existing = {r[0] for r in cur.fetchall()}
                finally:
                    cur.close()

            rows_to_insert = [r for r in rows_to_insert if r[3] not in existing]
            if not rows_to_insert:
                continue

        _insert_rows(db_manager, rows=rows_to_insert)
        total_inserted += len(rows_to_insert)

    # Finalise progress bar.
    if show_progress and progress_interactive:
        sys.stderr.write("\n")
        sys.stderr.flush()

    logger.info(
        "STAB backfill complete: market=%s instruments=%d rows_inserted=%d (deleted=%d)",
        args.market_id,
        len(instrument_ids),
        total_inserted,
        deleted,
    )

    report: Dict[str, Any] = {
        "market_id": str(args.market_id),
        "start": start_date.isoformat(),
        "end": end_date.isoformat(),
        "window_days": window_days,
        "history_lookback_calendar_days": lookback_cal_days,
        "instrument_count": len(instrument_ids),
        "min_instruments_per_day": int(min_instruments_per_day),
        "rows_inserted": total_inserted,
        "rows_deleted": deleted,
    }

    try:
        coverage = _compute_coverage_report(
            db_manager,
            market_id=str(args.market_id),
            instrument_ids=instrument_ids,
            start_date=start_date,
            end_date=end_date,
            window_days=window_days,
            min_instruments_per_day=min_instruments_per_day,
        )
        report["coverage"] = coverage
    except Exception:  # pragma: no cover
        logger.exception("Failed to compute coverage report")

    if args.report_out:
        out_path = Path(str(args.report_out))
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(report, indent=2, sort_keys=False), encoding="utf-8")
        logger.info("Wrote report to %s", out_path)


if __name__ == "__main__":  # pragma: no cover
    main()
