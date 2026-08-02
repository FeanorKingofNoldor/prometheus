"""Prometheus — Forward-log store and event-study helpers for the signal harness.

Two additions for signals that cannot be replayed point-in-time off stored
history:

1. **forward_log** — a point-in-time snapshot store (table
   ``signal_forward_log`` in ``prometheus_runtime``, keyed by
   ``(signal_name, as_of_date, instrument_id)``). Bucket-3 (forward-only)
   signals — ones with no clean reconstructable history — get a daily snapshot
   appended here so they start accruing a real out-of-sample series from today.
   ``append_forward_snapshot`` writes today's values; ``read_forward_series``
   reads the accrued tidy frame back in the same shape the harness consumes.

2. **event_study** — for sparse / geopolitical event signals that can't run a
   daily cross-sectional backtest. Given a set of event dates and the assets
   exposed to each event, it averages the forward returns of the exposed assets
   in a window around the events (CAR-style), returning a mean forward-return
   path and a simple t-stat. Pure given a forward-return loader callback.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Callable, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
from apatheon.core.database import DatabaseManager
from apatheon.core.logging import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# forward_log: point-in-time snapshot store for forward-only signals
# ---------------------------------------------------------------------------


def ensure_forward_log_table(db_manager: DatabaseManager) -> None:
    """Create the ``signal_forward_log`` table if it doesn't exist.

    Self-provisioning research table (same convention as ``signal_registry``).
    One row per (signal, as_of_date, instrument); the score is whatever the
    signal emitted point-in-time on that date.
    """
    with db_manager.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS signal_forward_log (
                    signal_name   VARCHAR(120) NOT NULL,
                    as_of_date    DATE NOT NULL,
                    instrument_id VARCHAR(64) NOT NULL,
                    score         DOUBLE PRECISION,
                    created_at    TIMESTAMPTZ DEFAULT NOW(),
                    PRIMARY KEY (signal_name, as_of_date, instrument_id)
                )
                """
            )
        conn.commit()
    logger.info("signal_forward_log table ensured")


def append_forward_snapshot(
    db_manager: DatabaseManager,
    signal_name: str,
    scores: pd.DataFrame,
    *,
    as_of_date: Optional[date] = None,
) -> int:
    """Append today's point-in-time snapshot of a signal to the forward log.

    Args:
        signal_name: registry key of the signal.
        scores: tidy frame with ``instrument_id`` and ``score`` columns. If it
            also carries ``as_of_date`` and ``as_of_date`` arg is None, those
            dates are used per row; otherwise every row is stamped ``as_of_date``.
        as_of_date: override the snapshot date for every row (default: the
            frame's own ``as_of_date`` column, else today).

    Returns:
        number of rows written. Upserts on the (signal, date, instrument) key so
        re-running for the same day is idempotent.
    """
    if scores.empty:
        return 0
    ensure_forward_log_table(db_manager)

    df = scores.copy()
    if as_of_date is not None:
        df["as_of_date"] = pd.Timestamp(as_of_date)
    elif "as_of_date" not in df.columns:
        df["as_of_date"] = pd.Timestamp(date.today())
    df["as_of_date"] = pd.to_datetime(df["as_of_date"])

    rows = [
        (
            signal_name,
            r.as_of_date.date(),
            str(r.instrument_id),
            None if not np.isfinite(r.score) else float(r.score),
        )
        for r in df.itertuples(index=False)
    ]
    with db_manager.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO signal_forward_log (signal_name, as_of_date, instrument_id, score)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (signal_name, as_of_date, instrument_id)
                DO UPDATE SET score = EXCLUDED.score, created_at = NOW()
                """,
                rows,
            )
        conn.commit()
    logger.info("signal_forward_log: appended %d rows for '%s'", len(rows), signal_name)
    return len(rows)


def read_forward_series(
    db_manager: DatabaseManager,
    signal_name: str,
    *,
    start: Optional[date] = None,
    end: Optional[date] = None,
) -> pd.DataFrame:
    """Read the accrued forward-log series for a signal as a tidy frame.

    Returns columns ``instrument_id``, ``as_of_date``, ``score`` — the exact
    shape ``evaluate_signal`` consumes. Empty frame if nothing logged yet.
    """
    sql = (
        "SELECT instrument_id, as_of_date, score FROM signal_forward_log "
        "WHERE signal_name = %s"
    )
    params: List = [signal_name]
    if start is not None:
        sql += " AND as_of_date >= %s"
        params.append(start)
    if end is not None:
        sql += " AND as_of_date <= %s"
        params.append(end)
    sql += " ORDER BY as_of_date, instrument_id"
    with db_manager.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=["instrument_id", "as_of_date", "score"])
    if df.empty:
        return df
    df["as_of_date"] = pd.to_datetime(df["as_of_date"])
    df["score"] = df["score"].astype(float)
    return df


# ---------------------------------------------------------------------------
# event_study: CAR-style average forward returns around sparse events
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EventStudyResult:
    """Average forward-return path of exposed assets around a set of events."""

    name: str
    n_events: int
    n_pairs: int  # total (event, asset) observations used
    horizons: Tuple[int, ...]
    mean_fwd_return: Dict[int, float]  # horizon -> mean forward return of exposed
    std_fwd_return: Dict[int, float]
    t_stat: Dict[int, float]
    mean_baseline: Dict[int, float]  # horizon -> mean forward return of the baseline/control
    mean_abnormal: Dict[int, float]  # exposed minus baseline (the CAR)
    notes: List[str]

    def headline(self) -> str:
        lines = [
            f"Event study '{self.name}': {self.n_events} events, "
            f"{self.n_pairs} (event,asset) pairs"
        ]
        lines.append("  mean forward return of EXPOSED assets (abnormal = vs baseline):")
        for h in self.horizons:
            m = self.mean_fwd_return.get(h, float("nan"))
            t = self.t_stat.get(h, float("nan"))
            ab = self.mean_abnormal.get(h, float("nan"))
            lines.append(
                f"    {h:>3}d: exposed={m:+.4%}  t={t:+.2f}  abnormal(vs baseline)={ab:+.4%}"
            )
        for note in self.notes:
            lines.append(f"  note: {note}")
        return "\n".join(lines)


def event_study(
    events: Sequence[Tuple[date, Sequence[str]]],
    forward_return_fn: Callable[[str, date, int], Optional[float]],
    *,
    name: str = "event_study",
    horizons: Sequence[int] = (1, 5, 21),
    baseline_assets: Optional[Sequence[str]] = None,
) -> EventStudyResult:
    """Average forward returns of exposed assets in a window around events.

    Args:
        events: list of (event_date, exposed_asset_ids). Each event names the
            assets it touches (e.g. a chokepoint closure → tanker/energy names).
        forward_return_fn: callback ``(instrument_id, event_date, horizon) ->
            forward return or None``. The caller supplies a point-in-time loader
            (e.g. one backed by ``prices_daily``). Returns None when no clean
            forward window exists; those pairs are dropped.
        name: label for the report.
        horizons: forward horizons (trading days) to evaluate.
        baseline_assets: optional control set evaluated at the same event dates;
            the mean of their forward returns is subtracted to form an abnormal
            return (CAR). If None, baseline is 0 and abnormal == raw.

    Returns:
        EventStudyResult with per-horizon mean/std/t-stat of exposed forward
        returns and the abnormal (exposed-minus-baseline) path.
    """
    horizons = tuple(int(h) for h in horizons)
    notes: List[str] = []

    exposed: Dict[int, List[float]] = {h: [] for h in horizons}
    baseline: Dict[int, List[float]] = {h: [] for h in horizons}
    n_pairs = 0

    for ev_date, assets in events:
        for inst in assets:
            for h in horizons:
                r = forward_return_fn(inst, ev_date, h)
                if r is not None and np.isfinite(r):
                    exposed[h].append(float(r))
                    if h == horizons[0]:
                        n_pairs += 1
        if baseline_assets:
            for inst in baseline_assets:
                for h in horizons:
                    r = forward_return_fn(inst, ev_date, h)
                    if r is not None and np.isfinite(r):
                        baseline[h].append(float(r))

    mean_fwd: Dict[int, float] = {}
    std_fwd: Dict[int, float] = {}
    t_stat: Dict[int, float] = {}
    mean_base: Dict[int, float] = {}
    mean_abn: Dict[int, float] = {}

    for h in horizons:
        vals = np.array(exposed[h], dtype=float)
        if vals.size == 0:
            mean_fwd[h] = std_fwd[h] = t_stat[h] = float("nan")
            mean_base[h] = mean_abn[h] = float("nan")
            continue
        m = float(vals.mean())
        s = float(vals.std(ddof=1)) if vals.size > 1 else 0.0
        mean_fwd[h] = m
        std_fwd[h] = s
        t_stat[h] = float(m / (s / np.sqrt(vals.size))) if s > 0 else float("nan")
        bvals = np.array(baseline[h], dtype=float)
        b = float(bvals.mean()) if bvals.size else 0.0
        mean_base[h] = b
        mean_abn[h] = m - b

    if n_pairs == 0:
        notes.append("no usable (event, asset, horizon) observations — empty study")

    return EventStudyResult(
        name=name,
        n_events=len(events),
        n_pairs=n_pairs,
        horizons=horizons,
        mean_fwd_return=mean_fwd,
        std_fwd_return=std_fwd,
        t_stat=t_stat,
        mean_baseline=mean_base,
        mean_abnormal=mean_abn,
        notes=notes,
    )


def make_price_forward_return_fn(
    prices: pd.DataFrame,
) -> Callable[[str, date, int], Optional[float]]:
    """Build a forward-return callback for ``event_study`` from a tidy price frame.

    ``prices`` needs columns ``instrument_id``, ``trade_date``, ``px``. For an
    (instrument, event_date, horizon) the return is px[first trading day on/
    after event_date] -> px[that index + horizon] - 1, strictly forward. Returns
    None if the instrument has no price at/after the event or no full forward
    window. Pure (no DB) so it is unit-testable on synthetic prices.
    """
    by_inst: Dict[str, Tuple[np.ndarray, np.ndarray]] = {}
    p = prices.copy()
    p["trade_date"] = pd.to_datetime(p["trade_date"])
    for inst, g in p.sort_values("trade_date").groupby("instrument_id"):
        by_inst[str(inst)] = (
            g["trade_date"].to_numpy(),
            g["px"].to_numpy(dtype=float),
        )

    def _fn(instrument_id: str, event_date: date, horizon: int) -> Optional[float]:
        entry = by_inst.get(str(instrument_id))
        if entry is None:
            return None
        dts, px = entry
        i = int(np.searchsorted(dts, np.datetime64(pd.Timestamp(event_date)), side="left"))
        if i >= len(px):
            return None
        j = i + horizon
        if j >= len(px) or px[i] <= 0:
            return None
        return float(px[j] / px[i] - 1.0)

    return _fn
