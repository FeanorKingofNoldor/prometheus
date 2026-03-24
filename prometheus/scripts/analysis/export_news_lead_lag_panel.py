"""Prometheus v2: Export issuer×day NEWS/return panel for lead–lag analysis.

This script exports a tidy CSV panel suitable for offline analysis of
lead–lag relationships between NEWS factors and future returns/volatility.

For each `(instrument_id, as_of_date)` in a given window it joins:

- Scalar NEWS factors from `historical_db.news_factors_daily` keyed by
  `(issuer_id, as_of_date, model_id)`.
- Instrument-level returns from `historical_db.returns_daily`:
  - contemporaneous `ret_1d` (optional), and
  - forward returns `ret_1d`, `ret_5d`, `ret_21d` shifted ahead by the
    specified horizons.

The output CSV can be used in notebooks/ML pipelines to estimate
`f_news` → future returns/vol models, run decile sorts, etc.

External dependencies:
- None beyond core Prometheus infrastructure.

Database tables accessed:
- runtime_db.instruments (Read): map instrument → issuer.
- historical_db.news_factors_daily (Read).
- historical_db.returns_daily (Read).

Thread safety: Not thread-safe (offline batch export).

Author: Prometheus Team
Created: 2025-12-10
Last Modified: 2025-12-10
Status: Development
Version: v0.1.0
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from apathis.core.config import get_config
from apathis.core.database import DatabaseManager
from apathis.core.logging import get_logger

logger = get_logger(__name__)


# ============================================================================
# Data structures
# ============================================================================


@dataclass(frozen=True)
class InstrumentKey:
    """Key for an instrument×day row in the export panel."""

    instrument_id: str
    as_of_date: date


@dataclass(frozen=True)
class NewsFactorRow:
    """Scalar NEWS factors for a single issuer×day."""

    issuer_id: str
    as_of_date: date
    factors: Dict[str, float]


@dataclass(frozen=True)
class ReturnRow:
    """Daily returns for a single instrument×day."""

    instrument_id: str
    trade_date: date
    ret_1d: float
    ret_5d: Optional[float]
    ret_21d: Optional[float]


# ============================================================================
# Helpers
# ============================================================================


def _parse_date(value: str) -> date:
    """Parse a YYYY-MM-DD date string into a :class:`date`."""

    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:  # pragma: no cover - CLI validation
        raise argparse.ArgumentTypeError(
            f"Invalid date {value!r}, expected YYYY-MM-DD",
        ) from exc


def _load_instrument_issuer_mapping(
    db_manager: DatabaseManager,
    *,
    market_id: Optional[str] = None,
) -> Dict[str, str]:
    """Load instrument_id → issuer_id mapping from runtime_db.instruments.

    If ``market_id`` is provided, restrict to that market; otherwise include
    all instruments.
    """

    where_clauses = ["issuer_id IS NOT NULL"]
    params: List[object] = []

    if market_id is not None:
        where_clauses.append("market_id = %s")
        params.append(market_id)

    where_sql = " WHERE " + " AND ".join(where_clauses)

    sql = (
        "SELECT instrument_id, issuer_id "
        "FROM instruments "
        + where_sql
    )

    mapping: Dict[str, str] = {}

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, tuple(params))
            for instrument_id, issuer_id in cursor.fetchall():
                if issuer_id is None:
                    continue
                mapping[str(instrument_id)] = str(issuer_id)
        finally:
            cursor.close()

    logger.info(
        "Loaded %d instrument→issuer mappings (market_id=%s)",
        len(mapping),
        market_id,
    )
    return mapping


def _load_news_factors(
    db_manager: DatabaseManager,
    *,
    model_id: str,
    start_date: date,
    end_date: date,
) -> Dict[Tuple[str, date], NewsFactorRow]:
    """Load scalar NEWS factors for issuer×day keys.

    Returns a mapping from (issuer_id, as_of_date) to a NewsFactorRow
    containing a dict of factor_name → factor_value.
    """

    sql = """
        SELECT issuer_id, as_of_date, factor_name, factor_value
        FROM news_factors_daily
        WHERE model_id = %s
          AND as_of_date BETWEEN %s AND %s
        ORDER BY issuer_id, as_of_date
    """

    params = (model_id, start_date, end_date)

    factors_by_key: Dict[Tuple[str, date], Dict[str, float]] = {}

    with db_manager.get_historical_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, params)
            for issuer_id, as_of_date, factor_name, factor_value in cursor.fetchall():
                key = (str(issuer_id), as_of_date)
                d = factors_by_key.setdefault(key, {})
                d[str(factor_name)] = float(factor_value)
        finally:
            cursor.close()

    out: Dict[Tuple[str, date], NewsFactorRow] = {}
    for (issuer_id, as_of_date), d in factors_by_key.items():
        out[(issuer_id, as_of_date)] = NewsFactorRow(
            issuer_id=issuer_id,
            as_of_date=as_of_date,
            factors=d,
        )

    logger.info(
        "Loaded NEWS factors for %d issuer×day keys (model_id=%s)",
        len(out),
        model_id,
    )
    return out


def _load_returns(
    db_manager: DatabaseManager,
    *,
    start_date: date,
    end_date: date,
    instrument_ids: Sequence[str],
) -> Dict[Tuple[str, date], ReturnRow]:
    """Load daily returns for a set of instruments and date range."""

    if not instrument_ids:
        return {}

    sql = """
        SELECT instrument_id, trade_date, ret_1d, ret_5d, ret_21d
        FROM returns_daily
        WHERE trade_date BETWEEN %s AND %s
          AND instrument_id = ANY(%s)
        ORDER BY instrument_id, trade_date
    """

    params = (start_date, end_date, list(instrument_ids))

    results: Dict[Tuple[str, date], ReturnRow] = {}

    with db_manager.get_historical_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, params)
            for inst_id, dt, r1, r5, r21 in cursor.fetchall():
                key = (str(inst_id), dt)
                results[key] = ReturnRow(
                    instrument_id=str(inst_id),
                    trade_date=dt,
                    ret_1d=float(r1) if r1 is not None else 0.0,
                    ret_5d=float(r5) if r5 is not None else None,
                    ret_21d=float(r21) if r21 is not None else None,
                )
        finally:
            cursor.close()

    logger.info(
        "Loaded returns for %d instrument×day keys (%d instruments)",
        len(results),
        len(instrument_ids),
    )
    return results


def _compute_forward_returns(
    returns_by_key: Dict[Tuple[str, date], ReturnRow],
    *,
    horizons: Sequence[int],
) -> Dict[InstrumentKey, Dict[int, float]]:
    """Compute simple forward returns for each horizon in days.

    For each instrument×day key, we look up the ReturnRow at
    `as_of_date + horizon` and use its `ret_1d` as the forward return over
    that horizon. This is a simple approximation; more sophisticated
    compounding can be added later if needed.
    """

    horizons_set = set(horizons)
    out: Dict[InstrumentKey, Dict[int, float]] = {}

    # Build an index by (instrument_id, date).
    for (inst_id, dt), row in returns_by_key.items():
        key = InstrumentKey(instrument_id=inst_id, as_of_date=dt)
        fwd: Dict[int, float] = {}
        for h in horizons_set:
            target_date = dt + timedelta(days=h)
            target_key = (inst_id, target_date)
            target_row = returns_by_key.get(target_key)
            if target_row is None or target_row.ret_1d is None:
                continue
            fwd[h] = float(target_row.ret_1d)
        if fwd:
            out[key] = fwd

    logger.info(
        "Computed forward returns for %d instrument×day keys across horizons=%s",
        len(out),
        sorted(horizons_set),
    )
    return out


# ============================================================================
# CLI
# ============================================================================


def main(argv: Optional[Sequence[str]] = None) -> None:
    """Export issuer×day NEWS / returns panel to CSV for lead–lag analysis."""

    parser = argparse.ArgumentParser(
        description=(
            "Export a panel joining issuer×day NEWS factors with instrument "
            "returns for lead–lag research.",
        ),
    )

    parser.add_argument(
        "--start",
        required=True,
        type=_parse_date,
        help="Inclusive start date (YYYY-MM-DD) for as_of_date.",
    )
    parser.add_argument(
        "--end",
        required=True,
        type=_parse_date,
        help="Inclusive end date (YYYY-MM-DD) for as_of_date.",
    )
    parser.add_argument(
        "--market-id",
        type=str,
        default="US_EQ",
        help="Optional market_id filter for instruments (default: US_EQ).",
    )
    parser.add_argument(
        "--news-model-id",
        type=str,
        default="text-fin-general-v1",
        help="NEWS model_id to use from news_factors_daily (default: text-fin-general-v1).",
    )
    parser.add_argument(
        "--horizons",
        type=int,
        nargs="+",
        default=[1, 5, 21],
        help="Forward return horizons in calendar days (default: 1 5 21).",
    )
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        help="Path to output CSV file.",
    )

    args = parser.parse_args(argv)

    if args.end < args.start:
        parser.error("--end must be >= --start")

    output_path = Path(args.output)

    config = get_config()
    db_manager = DatabaseManager(config)

    logger.info(
        "Exporting NEWS/returns panel for %s→%s (market_id=%s, news_model_id=%s)",
        args.start,
        args.end,
        args.market_id,
        args.news_model_id,
    )

    # 1) instrument_id → issuer_id mapping
    inst_to_issuer = _load_instrument_issuer_mapping(
        db_manager=db_manager,
        market_id=args.market_id,
    )
    if not inst_to_issuer:
        logger.warning("No instrument→issuer mappings loaded; nothing to do")
        return

    # 2) NEWS factors per issuer×day
    news_by_key = _load_news_factors(
        db_manager=db_manager,
        model_id=args.news_model_id,
        start_date=args.start,
        end_date=args.end,
    )
    if not news_by_key:
        logger.warning("No NEWS factors loaded; nothing to do")
        return

    # 3) Returns per instrument×day (we may need a slightly expanded window for forward horizons).
    max_h = max(args.horizons) if args.horizons else 0
    returns_start = args.start
    returns_end = args.end + timedelta(days=max_h)

    returns_by_key = _load_returns(
        db_manager=db_manager,
        start_date=returns_start,
        end_date=returns_end,
        instrument_ids=list(inst_to_issuer.keys()),
    )
    if not returns_by_key:
        logger.warning("No returns loaded; nothing to do")
        return

    # 4) Compute forward returns from daily ret_1d.
    fwd_returns_by_key = _compute_forward_returns(
        returns_by_key,
        horizons=args.horizons,
    )
    if not fwd_returns_by_key:
        logger.warning("No forward returns computed; nothing to do")
        return

    # 5) Build and write CSV panel.
    factor_names: List[str] = sorted({
        name
        for nf in news_by_key.values()
        for name in nf.factors.keys()
    })
    horizon_cols: List[str] = [f"fwd_ret_{h}d" for h in sorted(set(args.horizons))]

    header = [
        "instrument_id",
        "issuer_id",
        "as_of_date",
        *factor_names,
        "ret_1d",
        *horizon_cols,
    ]

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)

        rows_written = 0
        for (inst_id, dt), ret_row in returns_by_key.items():
            issuer_id = inst_to_issuer.get(inst_id)
            if issuer_id is None:
                continue

            nf_key = (issuer_id, dt)
            nf = news_by_key.get(nf_key)
            if nf is None:
                continue

            key = InstrumentKey(instrument_id=inst_id, as_of_date=dt)
            fwd = fwd_returns_by_key.get(key, {})
            if not fwd:
                continue

            factor_vals = [nf.factors.get(name, 0.0) for name in factor_names]
            base_ret_1d = ret_row.ret_1d
            fwd_vals = [fwd.get(h, 0.0) for h in sorted(set(args.horizons))]

            writer.writerow([
                inst_id,
                issuer_id,
                dt.isoformat(),
                *factor_vals,
                base_ret_1d,
                *fwd_vals,
            ])
            rows_written += 1

    logger.info("Exported %d rows to %s", rows_written, output_path)


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
