"""Prometheus — Signal Registry.

A small persisted registry (table ``signal_registry`` in ``prometheus_runtime``)
recording, per signal, the verdict of the research harness so we never re-litigate
a shelved signal or forget what a live one actually predicts.

Buckets (point-in-time integrity class):
    1 = replayable      — computable purely from historical data, no look-ahead
                          risk (e.g. price momentum). Trustworthy IC.
    2 = reconstructable — needs state we logged but can rebuild point-in-time
                          (e.g. assessment scores from engine_decisions).
    3 = forward-only    — only observable going forward (no clean history); IC
                          must be accumulated live.

Verdict:
    alpha   — predicts forward return; tradeable directionally.
    risk    — predicts forward volatility, not return (the lambda lesson).
    timing  — predicts short-horizon return that decays; entry/exit timing only.
    shelve  — predicts neither at a useful level.

Usage::

    from prometheus.research.signal_registry import (
        ensure_signal_registry_table, register_signal, update_from_report,
        list_signals, get_signal,
    )
    ensure_signal_registry_table(db)
    update_from_report(db, report, description="6-1 cross-sectional momentum",
                       bucket=1, integrity_note="pure past-price; no look-ahead")
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

from apatheon.core.database import DatabaseManager
from apatheon.core.logging import get_logger

from prometheus.research.signal_harness import SignalReport

logger = get_logger(__name__)


@dataclass(frozen=True)
class SignalRecord:
    """One row of the signal registry."""

    name: str
    description: str
    bucket: int
    integrity_note: str
    what_predicts: str
    headline_ic: float
    headline_horizon: int
    verdict: str
    turnover: float
    n_obs: int
    last_evaluated: Optional[datetime]
    metrics: dict


def ensure_signal_registry_table(db_manager: DatabaseManager) -> None:
    """Create the ``signal_registry`` table if it doesn't exist.

    Self-provisioning research/meta table (same convention as trade_journal).
    """
    with db_manager.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS signal_registry (
                    name             VARCHAR(120) PRIMARY KEY,
                    description      TEXT,
                    bucket           SMALLINT NOT NULL DEFAULT 1,
                    integrity_note   TEXT,
                    what_predicts    VARCHAR(40),
                    headline_ic      DOUBLE PRECISION,
                    headline_horizon INTEGER,
                    verdict          VARCHAR(20),
                    turnover         DOUBLE PRECISION,
                    n_obs            INTEGER,
                    metrics          JSONB DEFAULT '{}',
                    last_evaluated   TIMESTAMPTZ,
                    created_at       TIMESTAMPTZ DEFAULT NOW(),
                    updated_at       TIMESTAMPTZ DEFAULT NOW()
                )
                """
            )
        conn.commit()
    logger.info("signal_registry table ensured")


def _f(x: float):
    """JSON-safe float: NaN/inf -> None (Postgres jsonb rejects NaN)."""
    if x is None or (isinstance(x, float) and not math.isfinite(x)):
        return None
    return float(x)


def _report_metrics(report: SignalReport) -> dict:
    """Compact JSON-serializable metrics blob from a SignalReport."""
    return {
        "horizons": list(report.horizons),
        "n_dates": report.n_dates,
        "date_range": [
            report.date_range[0].isoformat() if report.date_range[0] else None,
            report.date_range[1].isoformat() if report.date_range[1] else None,
        ],
        "ic": {
            str(h): {
                "mean_ic": _f(ic.mean_ic),
                "t_stat": _f(ic.t_stat),
                "ic_ir": _f(ic.ic_ir),
                "mean_ic_vol": _f(ic.mean_ic_vol),
                "t_stat_vol": _f(ic.t_stat_vol),
                "n_dates": ic.n_dates,
            }
            for h, ic in report.ic.items()
        },
        "deciles": {
            str(h): {
                "top_minus_bottom": _f(d.top_minus_bottom),
                "monotonic": d.monotonic,
                "spearman_bucket": _f(d.spearman_bucket),
            }
            for h, d in report.deciles.items()
        },
    }


def register_signal(
    db_manager: DatabaseManager,
    *,
    name: str,
    description: str,
    bucket: int,
    integrity_note: str,
    what_predicts: str,
    headline_ic: float,
    headline_horizon: int,
    verdict: str,
    turnover: float,
    n_obs: int,
    metrics: Optional[dict] = None,
    last_evaluated: Optional[datetime] = None,
) -> None:
    """Upsert a signal's registry record."""
    ensure_signal_registry_table(db_manager)
    when = last_evaluated or datetime.utcnow()
    with db_manager.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO signal_registry (
                    name, description, bucket, integrity_note, what_predicts,
                    headline_ic, headline_horizon, verdict, turnover, n_obs,
                    metrics, last_evaluated, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
                )
                ON CONFLICT (name) DO UPDATE SET
                    description      = EXCLUDED.description,
                    bucket           = EXCLUDED.bucket,
                    integrity_note   = EXCLUDED.integrity_note,
                    what_predicts    = EXCLUDED.what_predicts,
                    headline_ic      = EXCLUDED.headline_ic,
                    headline_horizon = EXCLUDED.headline_horizon,
                    verdict          = EXCLUDED.verdict,
                    turnover         = EXCLUDED.turnover,
                    n_obs            = EXCLUDED.n_obs,
                    metrics          = EXCLUDED.metrics,
                    last_evaluated   = EXCLUDED.last_evaluated,
                    updated_at       = NOW()
                """,
                (
                    name, description, int(bucket), integrity_note, what_predicts,
                    _nan_to_none(headline_ic), int(headline_horizon), verdict,
                    _nan_to_none(turnover), int(n_obs),
                    json.dumps(metrics or {}), when,
                ),
            )
        conn.commit()
    logger.info("signal_registry: upserted '%s' (verdict=%s)", name, verdict)


def update_from_report(
    db_manager: DatabaseManager,
    report: SignalReport,
    *,
    description: str,
    bucket: int,
    integrity_note: str,
    last_evaluated: Optional[datetime] = None,
) -> None:
    """Register/update a signal directly from a harness SignalReport."""
    register_signal(
        db_manager,
        name=report.name,
        description=description,
        bucket=bucket,
        integrity_note=integrity_note,
        what_predicts=report.what_predicts,
        headline_ic=report.headline_ic,
        headline_horizon=report.headline_horizon,
        verdict=report.verdict_hint,
        turnover=report.turnover,
        n_obs=report.n_obs,
        metrics=_report_metrics(report),
        last_evaluated=last_evaluated,
    )


def get_signal(db_manager: DatabaseManager, name: str) -> Optional[SignalRecord]:
    """Fetch one registry record by name, or None."""
    with db_manager.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(_SELECT + " WHERE name = %s", (name,))
            row = cur.fetchone()
    return _row_to_record(row) if row else None


def list_signals(db_manager: DatabaseManager) -> List[SignalRecord]:
    """List all registry records, newest evaluation first."""
    with db_manager.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(_SELECT + " ORDER BY last_evaluated DESC NULLS LAST")
            rows = cur.fetchall()
    return [_row_to_record(r) for r in rows]


_SELECT = """
    SELECT name, description, bucket, integrity_note, what_predicts,
           headline_ic, headline_horizon, verdict, turnover, n_obs,
           last_evaluated, metrics
    FROM signal_registry
"""


def _row_to_record(row) -> SignalRecord:
    metrics = row[11]
    if isinstance(metrics, str):
        metrics = json.loads(metrics)
    return SignalRecord(
        name=row[0],
        description=row[1] or "",
        bucket=int(row[2]),
        integrity_note=row[3] or "",
        what_predicts=row[4] or "",
        headline_ic=float(row[5]) if row[5] is not None else float("nan"),
        headline_horizon=int(row[6]) if row[6] is not None else 0,
        verdict=row[7] or "",
        turnover=float(row[8]) if row[8] is not None else float("nan"),
        n_obs=int(row[9]) if row[9] is not None else 0,
        last_evaluated=row[10],
        metrics=metrics or {},
    )


def _nan_to_none(x: float) -> Optional[float]:
    import math
    return None if x is None or (isinstance(x, float) and math.isnan(x)) else float(x)
