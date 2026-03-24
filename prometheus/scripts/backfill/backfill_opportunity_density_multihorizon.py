"""Backfill multi-horizon opportunity-density (lambda) metrics per cluster.

This script computes **forward-looking** opportunity-density labels over
multiple horizons (e.g. h5/h21/h63). It is intended for offline research and
backtests.

Key properties
--------------
- **Bulk/fast**: loads prices once for the entire date range (+ buffer) and
  computes forward metrics via per-instrument prefix sums.
- **Resumable**: can stream output incrementally and resume from a checkpoint.
- **Schema-stable**: emits the same columns expected by downstream transforms.

Definition
----------
For each horizon h and cluster x = (market_id, sector, soft_target_class):
- forward_ret(i): sum of returns inside the forward window for instrument i
- forward_vol(i): realised vol of returns inside the forward window for i
- dispersion_h(x): cross-sectional std dev of forward_ret across members
- avg_vol_h(x): mean of forward_vol across members
- lambda_h(x) = dispersion_h(x) + avg_vol_h(x)

Notes
-----
- This is a *label surface* (uses future data). Do not use it directly as a
  live signal.
- Missing price days are handled similarly to the legacy implementation: we
  operate on the instrument's available trading-day samples and require at
  least 50% of forward-window prices to be present.
- We optionally skip instruments whose price series contains extreme single-day
  return spikes (often due to bad/corrupt price prints on delisted or
  placeholder symbols). This prevents tiny clusters from dominating the
  cross-sectional lambda tails.

Output CSV columns
------------------
Base:
  as_of_date, market_id, sector, soft_target_class, num_instruments
Per horizon h:
  lambda_h{h}, dispersion_h{h}, avg_vol_h{h}
"""

from __future__ import annotations

import argparse
import json
import os
import time
from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
from apathis.core.config import get_config
from apathis.core.database import DatabaseManager
from apathis.core.logging import get_logger
from apathis.core.time import US_EQ, TradingCalendar, TradingCalendarConfig
from apathis.data.classifications import DEFAULT_CLASSIFICATION_TAXONOMY
from apathis.data.reader import DataReader

logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


@dataclass(frozen=True)
class MultiHorizonLambda:
    """Multi-horizon lambda observation for a single cluster/date."""

    as_of_date: date
    market_id: str
    sector: str
    soft_target_class: str
    num_instruments: int
    # These are kept for backwards-compat with the existing default horizons.
    lambda_h5: float
    lambda_h21: float
    lambda_h63: float
    dispersion_h5: float
    dispersion_h21: float
    dispersion_h63: float
    avg_vol_h5: float
    avg_vol_h21: float
    avg_vol_h63: float


@dataclass(frozen=True)
class _InstrumentReturnSeries:
    """Sparse returns series for one instrument aligned to the trading-day index."""

    td_idxs: np.ndarray  # int32 trading-day indices where a valid close exists
    pref_ret: np.ndarray  # float64 prefix sums of returns (len == len(td_idxs))
    pref_ret2: np.ndarray  # float64 prefix sums of returns^2 (len == len(td_idxs))


_SOFT_TARGET_LEVELS: Tuple[str, ...] = (
    "UNKNOWN",
    "STABLE",
    "WATCH",
    "FRAGILE",
    "TARGETABLE",
    "BREAKER",
)
_SOFT_TARGET_TO_CODE: Dict[str, int] = {k: i for i, k in enumerate(_SOFT_TARGET_LEVELS)}


def _std_ddof1_from_sums(*, n: int, sum_x: float, sum_x2: float) -> float:
    if n <= 1:
        return 0.0
    var = (sum_x2 - (sum_x * sum_x) / float(n)) / float(n - 1)
    # Numerical guard.
    if var < 0.0 and var > -1e-12:
        var = 0.0
    if var <= 0.0:
        return 0.0
    return float(np.sqrt(var))


def _compute_forward_ret_and_vol(
    series: _InstrumentReturnSeries,
    *,
    t_idx: int,
    horizon: int,
) -> Optional[Tuple[float, float]]:
    """Return (forward_ret, forward_vol) for a single instrument/date/horizon.

    Semantics match the legacy implementation:
    - select closes whose trading-day index is in [t+1, t+h]
    - compute pct-change returns across consecutive *available* closes
    - require at least 50% of the horizon's closes to be present
    """

    start_td = t_idx + 1
    end_td = t_idx + int(horizon)

    td_idxs = series.td_idxs

    L = bisect_left(td_idxs, start_td)
    R = bisect_right(td_idxs, end_td)

    n_prices = int(R - L)
    if n_prices < max(int(horizon) // 2, 3):
        return None

    # Returns live between consecutive prices in the selected slice.
    b = R - 1
    n_rets = int(b - L)
    if n_rets < 2:
        return None

    sum_ret = float(series.pref_ret[b] - series.pref_ret[L])
    sum_ret2 = float(series.pref_ret2[b] - series.pref_ret2[L])

    if not np.isfinite(sum_ret) or not np.isfinite(sum_ret2):
        return None

    # Realised vol of returns within the window.
    vol = _std_ddof1_from_sums(n=n_rets, sum_x=sum_ret, sum_x2=sum_ret2)
    if not np.isfinite(vol):
        return None

    return sum_ret, vol


def _load_active_equity_instruments(
    db_manager: DatabaseManager,
    *,
    market_ids: Sequence[str],
    exclude_synthetic: bool = False,
) -> pd.DataFrame:
    """Load the active equity instrument universe (static membership).

    Returns columns:
    - instrument_id
    - issuer_id
    - sector (baseline "UNKNOWN"; overridden by issuer_classifications intervals)
    - market_id
    """

    sql = """
        SELECT
            i.instrument_id,
            i.issuer_id,
            'UNKNOWN' AS sector,
            i.market_id
        FROM instruments AS i
        WHERE i.market_id = ANY(%s)
          AND i.asset_class = 'EQUITY'
          AND i.status = 'ACTIVE'
    """
    if exclude_synthetic:
        sql += "          AND i.instrument_id NOT LIKE 'SYNTH\\_%%'\n"

    with db_manager.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, (list(market_ids),))
            rows = cur.fetchall()
        finally:
            cur.close()

    df = pd.DataFrame(rows, columns=["instrument_id", "issuer_id", "sector", "market_id"])
    if df.empty:
        return df

    df["instrument_id"] = df["instrument_id"].astype(str)
    df["issuer_id"] = df["issuer_id"].astype(str)
    df["sector"] = df["sector"].astype(str)
    df["market_id"] = df["market_id"].astype(str)
    return df


def _load_soft_target_classes_bulk(
    db_manager: DatabaseManager,
    *,
    instrument_ids: Sequence[str],
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """Load STAB soft_target_class for all instruments across a date range."""

    if not instrument_ids:
        return pd.DataFrame(columns=["as_of_date", "instrument_id", "soft_target_class"])

    sql = """
        SELECT
            as_of_date,
            entity_id AS instrument_id,
            soft_target_class
        FROM soft_target_classes
        WHERE entity_type = 'INSTRUMENT'
          AND entity_id = ANY(%s)
          AND as_of_date BETWEEN %s AND %s
    """

    with db_manager.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, (list(instrument_ids), start_date, end_date))
            rows = cur.fetchall()
        finally:
            cur.close()

    df = pd.DataFrame(rows, columns=["as_of_date", "instrument_id", "soft_target_class"])
    if df.empty:
        return df

    df["as_of_date"] = pd.to_datetime(df["as_of_date"]).dt.date
    df["instrument_id"] = df["instrument_id"].astype(str)
    df["soft_target_class"] = df["soft_target_class"].fillna("UNKNOWN").astype(str)
    return df


def _load_issuer_classification_intervals_bulk(
    db_manager: DatabaseManager,
    *,
    issuer_ids: Sequence[str],
    taxonomy: str,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """Load classification intervals intersecting [start_date, end_date].

    Result columns:
    - issuer_id
    - effective_start
    - effective_end
    - sector

    Note: `effective_end` is exclusive.
    """

    if not issuer_ids:
        return pd.DataFrame(columns=["issuer_id", "effective_start", "effective_end", "sector"])

    sql = """
        SELECT
            issuer_id,
            effective_start,
            effective_end,
            sector
        FROM issuer_classifications
        WHERE issuer_id = ANY(%s)
          AND taxonomy = %s
          AND effective_start <= %s
          AND (effective_end IS NULL OR effective_end > %s)
        ORDER BY issuer_id, effective_start
    """

    with db_manager.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, (list(issuer_ids), taxonomy, end_date, start_date))
            rows = cur.fetchall()
        finally:
            cur.close()

    df = pd.DataFrame(rows, columns=["issuer_id", "effective_start", "effective_end", "sector"])
    if df.empty:
        return df

    df["issuer_id"] = df["issuer_id"].astype(str)
    df["sector"] = df["sector"].fillna("UNKNOWN").astype(str)
    return df


def _default_checkpoint_path(out_path: Path) -> Path:
    return out_path.with_suffix(out_path.suffix + ".checkpoint.json")


def _load_checkpoint(path: Path) -> Optional[Dict[str, str]]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except Exception:
        return None


def _infer_last_as_of_date_from_csv(path: Path) -> Optional[date]:
    try:
        with path.open("rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            if size <= 0:
                return None
            read_size = min(size, 65536)
            f.seek(size - read_size)
            tail = f.read(read_size).decode("utf-8", errors="replace")
    except FileNotFoundError:
        return None

    lines = [ln for ln in tail.splitlines() if ln.strip()]
    for ln in reversed(lines):
        if ln.startswith("as_of_date,"):
            continue
        first = ln.split(",", 1)[0].strip()
        try:
            return _parse_date(first)
        except Exception:
            continue
    return None


def _sanitize_output_csv(*, path: Path, last_ok: date) -> None:
    """Rewrite CSV to drop any rows with as_of_date > last_ok.

    This makes resume robust if the process was interrupted mid-write.
    """

    if not path.exists():
        return

    tmp = path.with_suffix(path.suffix + ".tmp")
    keep_upto = last_ok.isoformat()

    with path.open("r", encoding="utf-8", errors="replace") as fin, tmp.open(
        "w", encoding="utf-8"
    ) as fout:
        header = fin.readline()
        if header:
            fout.write(header)
        for line in fin:
            if not line.strip():
                continue
            as_of = line.split(",", 1)[0]
            # ISO dates compare lexicographically.
            if as_of <= keep_upto:
                fout.write(line)

    tmp.replace(path)


def _append_rows_csv(*, out_path: Path, rows: List[Dict[str, object]], col_order: List[str]) -> None:
    if not rows:
        return

    df = pd.DataFrame(rows)
    # Ensure stable column ordering (downstream scripts assume these names exist).
    df = df.reindex(columns=col_order)

    header = not out_path.exists() or out_path.stat().st_size == 0
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, mode="a", header=header, index=False)


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Backfill multi-horizon opportunity-density (lambda) per cluster (fast/resumable)",
    )

    parser.add_argument("--start", type=_parse_date, required=True)
    parser.add_argument("--end", type=_parse_date, required=True)

    parser.add_argument(
        "--market",
        dest="markets",
        action="append",
        default=None,
        help="Market ID (can specify multiple times, default: US_EQ)",
    )
    parser.add_argument(
        "--horizons",
        type=int,
        nargs="+",
        default=[5, 21, 63],
        help="Forward horizons in trading days (default: 5 21 63)",
    )

    parser.add_argument(
        "--classification-taxonomy",
        type=str,
        default=DEFAULT_CLASSIFICATION_TAXONOMY,
        help=f"Issuer classification taxonomy to use for sector (default: {DEFAULT_CLASSIFICATION_TAXONOMY})",
    )
    parser.add_argument("--min-cluster-size", type=int, default=5)
    parser.add_argument(
        "--max-abs-daily-return",
        type=float,
        default=5.0,
        help=(
            "Skip instruments whose price series contains an extreme single-day return with "
            "abs(ret) > threshold. Use 0 to disable. Default: 5.0 (500 percent)."
        ),
    )

    parser.add_argument("--output", type=str, required=True)

    parser.add_argument("--resume", action="store_true", help="Resume from checkpoint/output if present")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite any existing output and checkpoint")
    parser.add_argument("--checkpoint-path", type=str, default=None)
    parser.add_argument("--flush-every-dates", type=int, default=25, help="Flush output every N dates")
    parser.add_argument("--report-out", type=str, default=None, help="Optional JSON report path")
    parser.add_argument(
        "--exclude-synthetic",
        action="store_true",
        help="Exclude SYNTH_ instruments from the backfill (use only real instruments).",
    )

    args = parser.parse_args(argv)

    start_date: date = args.start
    end_date: date = args.end
    if end_date < start_date:
        raise SystemExit("--end must be >= --start")

    markets: List[str] = args.markets if args.markets else [US_EQ]
    horizons: List[int] = sorted(set(int(h) for h in args.horizons))
    if any(h <= 0 for h in horizons):
        raise SystemExit("--horizons must all be positive")

    min_cluster_size = int(args.min_cluster_size)
    if min_cluster_size <= 0:
        raise SystemExit("--min-cluster-size must be positive")

    max_abs_daily_return = float(args.max_abs_daily_return)
    if max_abs_daily_return < 0.0:
        raise SystemExit("--max-abs-daily-return must be >= 0")

    out_path = Path(str(args.output))
    checkpoint_path = Path(args.checkpoint_path) if args.checkpoint_path else _default_checkpoint_path(out_path)

    if args.overwrite and args.resume:
        raise SystemExit("Choose only one of --resume or --overwrite")

    if args.overwrite:
        if out_path.exists():
            out_path.unlink()
        if checkpoint_path.exists():
            checkpoint_path.unlink()

    last_completed: Optional[date] = None
    if args.resume:
        ck = _load_checkpoint(checkpoint_path)
        if isinstance(ck, dict) and isinstance(ck.get("last_completed_as_of_date"), str):
            try:
                last_completed = _parse_date(str(ck["last_completed_as_of_date"]))
            except Exception:
                last_completed = None

        if last_completed is None and out_path.exists():
            last_completed = _infer_last_as_of_date_from_csv(out_path)

        if last_completed is not None and out_path.exists():
            # Drop any partial tail beyond checkpoint.
            _sanitize_output_csv(path=out_path, last_ok=last_completed)

    config = get_config()
    db_manager = DatabaseManager(config)
    data_reader = DataReader(db_manager=db_manager)

    # Calendar/trading days.
    calendar = TradingCalendar(TradingCalendarConfig(market=US_EQ))
    max_horizon = max(horizons)
    buffer_end = end_date + timedelta(days=max_horizon * 2)

    all_trading_days = calendar.trading_days_between(start_date, buffer_end)
    if not all_trading_days:
        logger.warning("No trading days between %s and %s", start_date, buffer_end)
        return

    compute_days = [d for d in all_trading_days if start_date <= d <= end_date]
    if not compute_days:
        logger.warning("No compute trading days between %s and %s", start_date, end_date)
        return

    # Skip any already-completed dates.
    if last_completed is not None:
        compute_days = [d for d in compute_days if d > last_completed]

    if not compute_days:
        logger.info("Nothing to do (already complete through %s)", last_completed)
        return

    # Map trading day -> index.
    td_to_idx: Dict[date, int] = {d: i for i, d in enumerate(all_trading_days)}

    logger.info(
        "Computing multi-horizon lambda (bulk) markets=%s horizons=%s dates=%d (resume=%s)",
        markets,
        horizons,
        len(compute_days),
        bool(args.resume),
    )

    t0 = time.time()

    # Load instrument universe.
    inst_df = _load_active_equity_instruments(
        db_manager,
        market_ids=markets,
        exclude_synthetic=bool(getattr(args, 'exclude_synthetic', False)),
    )
    if inst_df.empty:
        logger.warning("No active equity instruments for markets=%s", markets)
        return

    inst_df = inst_df.sort_values("instrument_id").reset_index(drop=True)
    instrument_ids: List[str] = inst_df["instrument_id"].tolist()
    n_inst = len(instrument_ids)

    inst_to_col: Dict[str, int] = {iid: i for i, iid in enumerate(instrument_ids)}

    # Encode market to compact ints (static).
    market_cat = pd.Categorical(inst_df["market_id"].astype(str))
    market_codes = market_cat.codes.astype(np.int16)
    market_levels = [str(x) for x in market_cat.categories]

    # ------------------------------------------------------------------
    # Sector codes (time-versioned): start from issuers.sector fallback and
    # override with issuer_classifications intervals when present.
    # ------------------------------------------------------------------

    issuer_ids = inst_df["issuer_id"].astype(str).tolist()
    uniq_issuer_ids = sorted(set(issuer_ids))

    cls_df = _load_issuer_classification_intervals_bulk(
        db_manager,
        issuer_ids=uniq_issuer_ids,
        taxonomy=str(args.classification_taxonomy),
        start_date=start_date,
        end_date=end_date,
    )

    sectors = {str(x) for x in inst_df["sector"].astype(str).tolist()}
    if not cls_df.empty:
        sectors.update(str(x) for x in cls_df["sector"].astype(str).tolist())
    sectors.add("UNKNOWN")

    # Deterministic sector encoding.
    sector_levels = sorted(sectors)
    sector_to_code: Dict[str, int] = {s: i for i, s in enumerate(sector_levels)}
    unknown_sector_code = int(sector_to_code.get("UNKNOWN", 0))

    fallback_codes = np.array(
        [sector_to_code.get(str(s), unknown_sector_code) for s in inst_df["sector"].astype(str).tolist()],
        dtype=np.int16,
    )

    # Shape: (num_dates, num_instruments)
    sector_codes = np.tile(fallback_codes.reshape(1, n_inst), (len(compute_days), 1)).astype(np.int16)

    if not cls_df.empty:
        issuer_to_cols: Dict[str, List[int]] = {}
        for col, iss in enumerate(issuer_ids):
            issuer_to_cols.setdefault(str(iss), []).append(int(col))

        for issuer_id, eff_start, eff_end, sector in cls_df.itertuples(index=False, name=None):
            cols = issuer_to_cols.get(str(issuer_id))
            if not cols:
                continue

            left = bisect_left(compute_days, eff_start)
            right = bisect_left(compute_days, eff_end) if eff_end is not None else len(compute_days)
            if left >= right:
                continue

            code = sector_to_code.get(str(sector), unknown_sector_code)
            sector_codes[left:right, cols] = np.int16(code)

    # Load STAB classes across the range.
    stab_df = _load_soft_target_classes_bulk(
        db_manager,
        instrument_ids=instrument_ids,
        start_date=start_date,
        end_date=end_date,
    )

    # Build per-date soft_target_code matrix for compute_days.
    compute_idx: Dict[date, int] = {d: i for i, d in enumerate(compute_days)}
    unknown_code = _SOFT_TARGET_TO_CODE["UNKNOWN"]
    soft_codes = np.full((len(compute_days), n_inst), unknown_code, dtype=np.int16)

    if not stab_df.empty:
        # Vectorised assignment.
        d_idx = stab_df["as_of_date"].map(compute_idx).to_numpy()
        i_idx = stab_df["instrument_id"].map(inst_to_col).to_numpy()
        s_codes = stab_df["soft_target_class"].map(lambda x: _SOFT_TARGET_TO_CODE.get(str(x), unknown_code)).to_numpy()

        mask = (~pd.isna(d_idx)) & (~pd.isna(i_idx))
        if np.any(mask):
            soft_codes[d_idx[mask].astype(int), i_idx[mask].astype(int)] = s_codes[mask].astype(np.int16)

    # Load prices once for the entire window.
    logger.info(
        "Loading prices (adjusted_close) for %d instruments from %s to %s",
        n_inst,
        start_date,
        buffer_end,
    )

    prices = data_reader.read_prices_close(
        instrument_ids=instrument_ids,
        start_date=start_date,
        end_date=buffer_end,
        price_col="adjusted_close",
    )

    if prices.empty:
        logger.warning("No price rows returned for requested window")
        return

    prices = prices[["instrument_id", "trade_date", "close"]].copy()
    prices["trade_date"] = pd.to_datetime(prices["trade_date"]).dt.date
    prices["close"] = pd.to_numeric(prices["close"], errors="coerce")

    # Build per-instrument sparse return series.
    series_by_col: List[Optional[_InstrumentReturnSeries]] = [None] * n_inst

    skipped_extreme_ret = 0
    extreme_examples: List[Tuple[str, float]] = []

    for inst_id, g in prices.groupby("instrument_id", sort=False):
        col = inst_to_col.get(str(inst_id))
        if col is None:
            continue

        # Keep only valid closes.
        g2 = g.dropna(subset=["trade_date", "close"]).copy()
        g2 = g2[g2["close"] > 0.0]
        if g2.shape[0] < 3:
            continue

        td_idxs = g2["trade_date"].map(td_to_idx)
        g2 = g2.loc[td_idxs.notna()].copy()
        if g2.shape[0] < 3:
            continue

        td_idx_arr = td_idxs.loc[g2.index].to_numpy(dtype=np.int32)
        close_arr = g2["close"].to_numpy(dtype=np.float64)

        # Ensure increasing order.
        order = np.argsort(td_idx_arr, kind="mergesort")
        td_idx_arr = td_idx_arr[order]
        close_arr = close_arr[order]

        if td_idx_arr.size < 3:
            continue

        rets = (close_arr[1:] / close_arr[:-1]) - 1.0
        if rets.size < 2:
            continue

        if max_abs_daily_return > 0.0:
            max_abs = float(np.nanmax(np.abs(rets)))
            if np.isfinite(max_abs) and max_abs > max_abs_daily_return:
                skipped_extreme_ret += 1
                if len(extreme_examples) < 10:
                    extreme_examples.append((str(inst_id), max_abs))
                continue

        pref = np.empty(td_idx_arr.size, dtype=np.float64)
        pref2 = np.empty(td_idx_arr.size, dtype=np.float64)
        pref[0] = 0.0
        pref2[0] = 0.0
        np.cumsum(rets, out=pref[1:])
        np.cumsum(rets * rets, out=pref2[1:])

        series_by_col[col] = _InstrumentReturnSeries(td_idxs=td_idx_arr, pref_ret=pref, pref_ret2=pref2)

    if skipped_extreme_ret:
        msg = f"Skipped {skipped_extreme_ret} instruments due to extreme daily return spikes"
        if extreme_examples:
            ex = ", ".join(f"{iid} (max_abs_ret={v:.2f})" for iid, v in extreme_examples)
            msg += f"; examples: {ex}"
        logger.warning(msg)

    if not any(s is not None for s in series_by_col):
        logger.warning("No instruments had sufficient price history to compute returns")
        return

    # Output columns (stable ordering).
    # Keep the legacy ordering to minimize downstream surprises:
    #   base cols,
    #   all lambda_h*,
    #   all dispersion_h*,
    #   all avg_vol_h*
    out_cols: List[str] = [
        "as_of_date",
        "market_id",
        "sector",
        "soft_target_class",
        "num_instruments",
    ]
    out_cols += [f"lambda_h{h}" for h in horizons]
    out_cols += [f"dispersion_h{h}" for h in horizons]
    out_cols += [f"avg_vol_h{h}" for h in horizons]

    primary_horizon = 21 if 21 in horizons else horizons[len(horizons) // 2]

    total_rows = 0
    rows_buf: List[Dict[str, object]] = []
    dates_since_flush = 0

    for i, as_of in enumerate(compute_days):
        if i % 100 == 0:
            logger.info("Processing date %d/%d: %s", i + 1, len(compute_days), as_of)

        t_idx = td_to_idx.get(as_of)
        if t_idx is None:
            continue

        soft_row = soft_codes[i]

        # Horizon -> {cluster_key -> [n, sum_ret, sum_ret2, sum_vol]}
        accs_by_h: Dict[int, Dict[Tuple[int, int, int], List[float]]] = {int(h): {} for h in horizons}

        for col in range(n_inst):
            series = series_by_col[col]
            if series is None:
                continue

            soft_code = int(soft_row[col])
            cluster_key = (int(market_codes[col]), int(sector_codes[i, col]), soft_code)

            for h in horizons:
                m = _compute_forward_ret_and_vol(series, t_idx=int(t_idx), horizon=int(h))
                if m is None:
                    continue
                fwd_ret, fwd_vol = m

                # Guard against pathological data.
                if not np.isfinite(fwd_ret) or not np.isfinite(fwd_vol):
                    continue

                d = accs_by_h[int(h)]
                acc = d.get(cluster_key)
                if acc is None:
                    # n, sum_ret, sum_ret2, sum_vol
                    acc = [0.0, 0.0, 0.0, 0.0]
                    d[cluster_key] = acc
                acc[0] += 1.0
                acc[1] += float(fwd_ret)
                acc[2] += float(fwd_ret) * float(fwd_ret)
                acc[3] += float(fwd_vol)

        # cluster_key -> horizon -> (n, lambda, dispersion, avg_vol)
        cluster_stats: Dict[Tuple[int, int, int], Dict[int, Tuple[int, float, float, float]]] = {}

        for h in horizons:
            for ck, acc in accs_by_h[int(h)].items():
                n = int(acc[0])
                if n < min_cluster_size:
                    continue

                sum_ret = float(acc[1])
                sum_ret2 = float(acc[2])
                sum_vol = float(acc[3])

                disp = _std_ddof1_from_sums(n=n, sum_x=sum_ret, sum_x2=sum_ret2)
                avg_vol = sum_vol / float(n)
                lam = disp + avg_vol

                cluster_stats.setdefault(ck, {})[int(h)] = (n, lam, disp, avg_vol)

        if not cluster_stats:
            continue

        for ck, by_h in cluster_stats.items():
            primary = by_h.get(int(primary_horizon))
            if primary is None:
                continue

            market_code, sector_code, soft_code = ck
            market_id = market_levels[market_code]
            sector = sector_levels[sector_code]
            soft = _SOFT_TARGET_LEVELS[soft_code] if 0 <= soft_code < len(_SOFT_TARGET_LEVELS) else "UNKNOWN"

            n_primary = int(primary[0])

            row: Dict[str, object] = {
                "as_of_date": as_of.isoformat(),
                "market_id": market_id,
                "sector": sector,
                "soft_target_class": soft,
                "num_instruments": n_primary,
            }

            for h in horizons:
                rec = by_h.get(int(h))
                if rec is None:
                    row[f"lambda_h{h}"] = 0.0
                    row[f"dispersion_h{h}"] = 0.0
                    row[f"avg_vol_h{h}"] = 0.0
                else:
                    _, lam, disp, avg_vol = rec
                    row[f"lambda_h{h}"] = float(lam)
                    row[f"dispersion_h{h}"] = float(disp)
                    row[f"avg_vol_h{h}"] = float(avg_vol)

            rows_buf.append(row)

        total_rows += len(cluster_stats)
        dates_since_flush += 1

        if dates_since_flush >= int(args.flush_every_dates):
            _append_rows_csv(out_path=out_path, rows=rows_buf, col_order=out_cols)
            rows_buf = []
            dates_since_flush = 0

            # Update checkpoint.
            checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
            checkpoint_path.write_text(
                json.dumps({"last_completed_as_of_date": as_of.isoformat()}, indent=2) + "\n",
                encoding="utf-8",
            )

    # Final flush.
    if rows_buf:
        _append_rows_csv(out_path=out_path, rows=rows_buf, col_order=out_cols)
        rows_buf = []

    # Ensure checkpoint reflects completion.
    last_done = compute_days[-1]
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_path.write_text(
        json.dumps({"last_completed_as_of_date": last_done.isoformat()}, indent=2) + "\n",
        encoding="utf-8",
    )

    t1 = time.time()
    elapsed = float(t1 - t0)

    logger.info("Saved multihorizon lambda to %s", out_path)
    logger.info("Elapsed: %.1fs", elapsed)

    if args.report_out:
        rep_path = Path(str(args.report_out))
        rep_path.parent.mkdir(parents=True, exist_ok=True)
        rep = {
            "markets": markets,
            "start": start_date.isoformat(),
            "end": end_date.isoformat(),
            "buffer_end": buffer_end.isoformat(),
            "horizons": horizons,
            "primary_horizon": int(primary_horizon),
            "min_cluster_size": int(min_cluster_size),
            "price_col": "adjusted_close",
            "max_abs_daily_return": float(max_abs_daily_return),
            "skipped_instruments_extreme_daily_return": int(skipped_extreme_ret),
            "resume": bool(args.resume),
            "overwrite": bool(args.overwrite),
            "output": str(out_path),
            "checkpoint": str(checkpoint_path),
            "elapsed_sec": elapsed,
        }
        rep_path.write_text(json.dumps(rep, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
