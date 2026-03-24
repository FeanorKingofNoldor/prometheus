"""Backfill coarse opportunity-density (lambda) metrics per cluster.

This script computes a first-cut estimate of realised "opportunity
density" per (market, sector, soft-target-class) cluster and date, using
historical returns from the ``prices_daily`` table.

For each trading day ``as_of_date`` in the requested range and for each
cluster ``x``, it computes:

- Cross-sectional dispersion of daily returns on ``as_of_date`` within
  the cluster.
- Average realised volatility over a lookback window for instruments in
  the cluster.

It then combines these into a simple scalar ``lambda_value`` which can be
used in offline research to prototype the λ_t(x) target for
opportunity-density models.

The results are written to a CSV file for now; in a later iteration this
can be moved into a dedicated historical DB table
(e.g. ``opportunity_density_history``).

This script is **offline/research** only; it is not part of the live
pipeline.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import List, Optional, Sequence

import numpy as np
import pandas as pd
from apathis.core.config import get_config
from apathis.core.database import DatabaseManager
from apathis.core.logging import get_logger
from apathis.core.time import US_EQ, TradingCalendar, TradingCalendarConfig
from apathis.data.classifications import DEFAULT_CLASSIFICATION_TAXONOMY
from apathis.data.reader import DataReader

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# CLI helpers
# ---------------------------------------------------------------------------


def _parse_date(value: str) -> date:
    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:  # pragma: no cover - CLI validation
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


@dataclass(frozen=True)
class ClusterPoint:
    """Single realised lambda_t(x) observation.

    Attributes:
        as_of_date: Date of observation.
        market_id: Market identifier (e.g. ``"US_EQ"``).
        sector: Sector name (or ``"UNKNOWN"``).
        soft_target_class: Soft-target class label (e.g. ``"STABLE"``).
        num_instruments: Number of instruments in the cluster.
        dispersion: Cross-sectional std dev of same-day returns.
        avg_vol_window: Average realised volatility over the lookback window.
        lambda_value: Combined scalar lambda_t(x) value.
    """

    as_of_date: date
    market_id: str
    sector: str
    soft_target_class: str
    num_instruments: int
    dispersion: float
    avg_vol_window: float
    lambda_value: float


# ---------------------------------------------------------------------------
# Core computation
# ---------------------------------------------------------------------------


def _load_instruments_with_stab(
    db_manager: DatabaseManager,
    *,
    as_of_date: date,
    market_ids: Sequence[str],
    taxonomy: str = DEFAULT_CLASSIFICATION_TAXONOMY,
    allow_legacy_issuer_sector_fallback: bool = False,
) -> pd.DataFrame:
    """Return instruments with sector and soft-target class for as_of_date.

    The result has columns:

    - instrument_id
    - issuer_id
    - sector
    - market_id
    - soft_target_class (may be NULL/None)
    """

    sector_expr = "COALESCE(NULLIF(ic.sector, ''), 'UNKNOWN')"
    joins = ""
    if allow_legacy_issuer_sector_fallback:
        sector_expr = "COALESCE(NULLIF(ic.sector, ''), NULLIF(u.sector, ''), 'UNKNOWN')"
        joins = "LEFT JOIN issuers AS u ON u.issuer_id = i.issuer_id"

    sql = f"""
        SELECT
            i.instrument_id,
            i.issuer_id,
            {sector_expr} AS sector,
            i.market_id,
            st.soft_target_class
        FROM instruments AS i
        LEFT JOIN LATERAL (
            SELECT ic.sector
            FROM issuer_classifications AS ic
            WHERE ic.issuer_id = i.issuer_id
              AND ic.taxonomy = %s
              AND ic.effective_start <= %s
              AND (ic.effective_end IS NULL OR %s < ic.effective_end)
            ORDER BY ic.effective_start DESC
            LIMIT 1
        ) AS ic ON TRUE
        {joins}
        LEFT JOIN soft_target_classes AS st
          ON st.entity_type = 'INSTRUMENT'
         AND st.entity_id = i.instrument_id
         AND st.as_of_date = %s
        WHERE i.market_id = ANY(%s)
          AND i.asset_class = 'EQUITY'
          AND i.status = 'ACTIVE'
          AND i.instrument_id NOT LIKE 'SYNTH%%'
    """

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (taxonomy, as_of_date, as_of_date, as_of_date, list(market_ids)))
            rows = cursor.fetchall()
        finally:
            cursor.close()

    df = pd.DataFrame(
        rows,
        columns=["instrument_id", "issuer_id", "sector", "market_id", "soft_target_class"],
    )
    if df.empty:
        return df

    df["sector"] = df["sector"].astype(str)
    df["market_id"] = df["market_id"].astype(str)
    df["soft_target_class"] = df["soft_target_class"].fillna("UNKNOWN").astype(str)
    return df


def _compute_lambda_for_date(
    db_manager: DatabaseManager,
    data_reader: DataReader,
    calendar: TradingCalendar,
    *,
    as_of_date: date,
    market_ids: Sequence[str],
    lookback_days: int,
    min_cluster_size: int,
) -> List[ClusterPoint]:
    """Compute lambda_t(x) for all clusters on a single as_of_date.

    This function:
    - Loads active equity instruments for the given markets.
    - Attaches sector and soft-target class for as_of_date.
    - Uses prices_daily to compute:
      - Same-day simple returns.
      - Realised volatility over a lookback window of trading days.
    - Aggregates per (market_id, sector, soft_target_class) cluster.
    """

    inst_df = _load_instruments_with_stab(db_manager, as_of_date=as_of_date, market_ids=market_ids)
    if inst_df.empty:
        logger.info("No instruments found for as_of_date=%s; skipping", as_of_date)
        return []

    # Determine lookback window of trading days for the primary market. For
    # v1 we assume a single trading calendar (US_EQ). We search back over
    # a multiple of the lookback window using calendar logic instead of
    # naive year replacement to avoid invalid dates (e.g. leap-day issues).
    search_start = as_of_date - timedelta(days=lookback_days * 3)
    trading_days = calendar.trading_days_between(search_start, as_of_date)
    if len(trading_days) < lookback_days:
        logger.info("Insufficient trading days before %s; skipping", as_of_date)
        return []

    window_days = trading_days[-lookback_days:]
    start_date = window_days[0]

    instrument_ids = sorted(inst_df["instrument_id"].unique().tolist())
    prices = data_reader.read_prices(
        instrument_ids=instrument_ids,
        start_date=start_date,
        end_date=as_of_date,
    )
    if prices.empty:
        logger.info("No prices for instruments on %s; skipping", as_of_date)
        return []

    # Compute simple daily returns per instrument.
    prices = prices[["instrument_id", "trade_date", "close"]].copy()
    prices["close"] = prices["close"].astype(float)
    prices.sort_values(["instrument_id", "trade_date"], inplace=True)

    # Compute simple daily returns per instrument without using groupby.apply
    # to avoid deprecation warnings in newer pandas versions.
    prices["ret"] = (
        prices.groupby("instrument_id")["close"].pct_change()
    )

    # Last-day return per instrument (on as_of_date) and realised volatility
    # over the window.
    latest = prices.groupby("instrument_id").tail(1).copy()
    latest = latest[latest["trade_date"] == as_of_date]

    if latest.empty:
        logger.info("No latest prices matching as_of_date=%s; skipping", as_of_date)
        return []

    # Realised volatility per instrument over the window.
    vol = (
        prices.groupby("instrument_id")["ret"]
        .std(ddof=1)
        .rename("realised_vol_window")
        .reset_index()
    )

    feat = latest.merge(vol, on="instrument_id", how="left")
    feat = feat.merge(inst_df, on="instrument_id", how="left")

    # Drop instruments with missing return or vol.
    feat = feat.dropna(subset=["ret", "realised_vol_window"])
    if feat.empty:
        logger.info("No valid returns/vols for as_of_date=%s; skipping", as_of_date)
        return []

    # Compute cluster-level metrics.
    cluster_points: List[ClusterPoint] = []
    grouped = feat.groupby(["market_id", "sector", "soft_target_class"], dropna=False)

    for (market_id, sector, soft_class), g in grouped:
        n = int(g.shape[0])
        if n < min_cluster_size:
            continue

        disp = float(np.std(g["ret"].to_numpy(), ddof=1))
        avg_vol = float(g["realised_vol_window"].mean())

        # Simple v1 lambda: linear combination; we normalise by a soft
        # factor to avoid crazy magnitudes.
        lambda_value = disp + avg_vol

        cluster_points.append(
            ClusterPoint(
                as_of_date=as_of_date,
                market_id=str(market_id),
                sector=str(sector),
                soft_target_class=str(soft_class),
                num_instruments=n,
                dispersion=disp,
                avg_vol_window=avg_vol,
                lambda_value=lambda_value,
            )
        )

    return cluster_points


# ---------------------------------------------------------------------------
# Main CLI
# ---------------------------------------------------------------------------


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill coarse opportunity-density (lambda_t) metrics per "
            "(market, sector, soft_target_class) cluster into a CSV file."
        ),
    )

    parser.add_argument(
        "--start",
        type=_parse_date,
        required=True,
        help="Start date (YYYY-MM-DD) for as_of_date range",
    )
    parser.add_argument(
        "--end",
        type=_parse_date,
        required=True,
        help="End date (YYYY-MM-DD) for as_of_date range",
    )
    parser.add_argument(
        "--market",
        dest="markets",
        action="append",
        default=None,
        help=(
            "Market_id to include (can be specified multiple times). "
            "Default: US_EQ only."
        ),
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=20,
        help="Number of trading days in lookback window for realised vol (default: 20)",
    )
    parser.add_argument(
        "--min-cluster-size",
        type=int,
        default=5,
        help="Minimum number of instruments per cluster to compute lambda (default: 5)",
    )
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        help="Path to output CSV file for lambda_t(x) observations",
    )

    args = parser.parse_args(argv)

    start_date: date = args.start
    end_date: date = args.end
    if end_date < start_date:
        parser.error("--end must be >= --start")

    markets: List[str]
    if args.markets is None:
        markets = [US_EQ]
    else:
        markets = list(args.markets)

    lookback_days: int = args.lookback_days
    min_cluster_size: int = args.min_cluster_size

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    config = get_config()
    db_manager = DatabaseManager(config)
    data_reader = DataReader(db_manager=db_manager)
    calendar = TradingCalendar(TradingCalendarConfig(market=US_EQ))

    # Enumerate trading days for the primary calendar and restrict to the
    # requested window.
    all_days = calendar.trading_days_between(start_date, end_date)
    if not all_days:
        logger.warning("No trading days between %s and %s; nothing to do", start_date, end_date)
        return

    logger.info(
        "Computing lambda_t(x) for markets=%s, start=%s, end=%s, lookback_days=%d",
        markets,
        start_date,
        end_date,
        lookback_days,
    )

    all_points: List[ClusterPoint] = []
    for d in all_days:
        pts = _compute_lambda_for_date(
            db_manager=db_manager,
            data_reader=data_reader,
            calendar=calendar,
            as_of_date=d,
            market_ids=markets,
            lookback_days=lookback_days,
            min_cluster_size=min_cluster_size,
        )
        if pts:
            all_points.extend(pts)

    if not all_points:
        logger.warning("No lambda_t(x) observations computed; nothing to write")
        return

    df_out = pd.DataFrame(
        [
            {
                "as_of_date": p.as_of_date,
                "market_id": p.market_id,
                "sector": p.sector,
                "soft_target_class": p.soft_target_class,
                "num_instruments": p.num_instruments,
                "dispersion": p.dispersion,
                "avg_vol_window": p.avg_vol_window,
                "lambda_value": p.lambda_value,
            }
            for p in all_points
        ]
    )

    df_out.sort_values(["as_of_date", "market_id", "sector", "soft_target_class"], inplace=True)
    df_out.to_csv(out_path, index=False)

    logger.info(
        "Wrote %d lambda_t(x) observations to %s",
        df_out.shape[0],
        out_path,
    )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
