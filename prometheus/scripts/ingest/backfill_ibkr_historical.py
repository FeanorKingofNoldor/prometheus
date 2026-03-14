"""Backfill historical data from IBKR TWS/Gateway.

Uses :class:`IbkrMarketDataService` to request historical bars for:
- Sector ETFs (XLK, XLF, … XLB) — daily OHLCV
- SPY, QQQ — daily OHLCV
- VIX — daily (Index type)
- HYG — daily OHLCV (credit proxy)
- Historical volatility (HISTORICAL_VOLATILITY) for sector ETFs + SPY
- Implied volatility (OPTION_IMPLIED_VOLATILITY) for sector ETFs + SPY

The rate limiter in IbkrMarketDataService ensures we stay within IBKR's
60-request-per-10-minute limit.

Data is written to ``prices_daily`` in the historical database via
:class:`DataWriter`.

Prerequisites:
- IBKR Gateway or TWS must be running and accepting API connections.

Usage:
    python -m prometheus.scripts.ingest.backfill_ibkr_historical
    python -m prometheus.scripts.ingest.backfill_ibkr_historical --duration "2 Y" --dry-run
    python -m prometheus.scripts.ingest.backfill_ibkr_historical --start-date 1996-12-31
"""

from __future__ import annotations

import argparse
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

from apathis.core.database import get_db_manager
from apathis.core.logging import get_logger
from apathis.data.types import PriceBar
from apathis.data.writer import DataWriter
from prometheus.execution.market_data import (
    BarData,
    IbkrMarketDataService,
    SECTOR_ETF_SYMBOLS,
)

logger = get_logger(__name__)


@dataclass
class BackfillJob:
    """Single backfill request definition."""
    symbol: str
    sec_type: str = "STK"
    exchange: str = "SMART"
    currency: str = "USD"
    data_type: str = "TRADES"
    duration: str = "1 Y"
    bar_size: str = "1 day"
    instrument_id: str = ""  # For writing to DB; defaults to {symbol}.US

    def __post_init__(self) -> None:
        if not self.instrument_id:
            self.instrument_id = f"{self.symbol}.US"


def build_backfill_jobs(duration: str = "1 Y") -> List[BackfillJob]:
    """Build the standard set of historical backfill jobs."""
    jobs: List[BackfillJob] = []

    # ── Sector ETFs: TRADES ──────────────────────────────────────────
    for sym in SECTOR_ETF_SYMBOLS:
        jobs.append(BackfillJob(
            symbol=sym, data_type="TRADES", duration=duration,
            instrument_id=f"{sym}.US",
        ))

    # ── Core indices ─────────────────────────────────────────────────
    jobs.append(BackfillJob(
        symbol="SPY", data_type="TRADES", duration=duration,
        instrument_id="SPY.US",
    ))
    jobs.append(BackfillJob(
        symbol="QQQ", data_type="TRADES", duration=duration,
        instrument_id="QQQ.US",
    ))

    # ── VIX (Index type) ─────────────────────────────────────────────
    jobs.append(BackfillJob(
        symbol="VIX", sec_type="IND", exchange="CBOE",
        data_type="TRADES", duration=duration,
        instrument_id="VIX.INDX",
    ))

    # ── HYG (credit proxy) ───────────────────────────────────────────
    jobs.append(BackfillJob(
        symbol="HYG", data_type="TRADES", duration=duration,
        instrument_id="HYG.US",
    ))

    # ── Historical volatility for key instruments ────────────────────
    for sym in ["SPY"] + SECTOR_ETF_SYMBOLS:
        jobs.append(BackfillJob(
            symbol=sym, data_type="HISTORICAL_VOLATILITY", duration=duration,
            instrument_id=f"{sym}_HVOL.US",
        ))

    # ── Implied volatility for key instruments ───────────────────────
    for sym in ["SPY"] + SECTOR_ETF_SYMBOLS:
        jobs.append(BackfillJob(
            symbol=sym, data_type="OPTION_IMPLIED_VOLATILITY", duration=duration,
            instrument_id=f"{sym}_IV.US",
        ))

    return jobs


# ── Batched year-by-year backfill ─────────────────────────────────────

def _generate_yearly_windows(
    start_date: date,
    end_date: date,
) -> List[Tuple[str, str]]:
    """Generate (endDateTime, duration) pairs walking back 1 year at a time.

    IBKR's ``reqHistoricalData`` accepts at most ~1 Y for daily bars in
    a single call.  We chunk from ``end_date`` backward to ``start_date``.

    Returns
    -------
    List of (end_datetime_str, duration_str) in **reverse-chronological**
    order (most recent window first).
    ``end_datetime_str`` is formatted as ``YYYYMMDD HH:MM:SS``.
    """
    windows: List[Tuple[str, str]] = []
    cursor = end_date

    while cursor > start_date:
        end_str = cursor.strftime("%Y%m%d-23:59:59")
        # Walk back exactly 1 year (or to start_date if closer)
        prev = date(cursor.year - 1, cursor.month, cursor.day)
        if prev < start_date:
            prev = start_date
        # Duration in days for this chunk (IBKR also accepts "1 Y" but
        # explicit days is more precise for the last partial window).
        days = (cursor - prev).days
        if days <= 0:
            break
        if days >= 365:
            dur_str = "1 Y"
        else:
            dur_str = f"{days} D"
        windows.append((end_str, dur_str))
        cursor = prev

    return windows


def _query_head_timestamp(
    ib: "IB",
    job: BackfillJob,
) -> Optional[date]:
    """Ask IBKR for the earliest available data date for a contract.

    Returns None if the query fails (we'll just walk back until 0 bars).
    """
    from prometheus.execution.ib_compat import Stock, Index, Contract

    if job.sec_type == "IND":
        contract = Index(job.symbol, job.exchange, job.currency)
    elif job.sec_type == "STK":
        contract = Stock(job.symbol, job.exchange, job.currency)
    else:
        contract = Contract()
        contract.symbol = job.symbol
        contract.secType = job.sec_type
        contract.exchange = job.exchange
        contract.currency = job.currency

    try:
        qualified = ib.qualifyContracts(contract)
        if qualified:
            contract = qualified[0]
        head = ib.reqHeadTimeStamp(
            contract, whatToShow=job.data_type, useRTH=True,
        )
        if head:
            if isinstance(head, datetime):
                return head.date()
            return date.fromisoformat(str(head)[:10])
    except Exception as exc:
        logger.debug("reqHeadTimeStamp failed for %s/%s: %s",
                     job.symbol, job.data_type, exc)
    return None


def run_batched_backfill(
    ib: "IB",
    jobs: List[BackfillJob],
    start_date: date,
    end_date: date,
    writer: DataWriter,
) -> Tuple[int, int]:
    """Run year-by-year backfill for all jobs back to ``start_date``.

    Returns (total_bars_written, total_failures).
    """
    mds = IbkrMarketDataService(ib)
    total_bars = 0
    total_failures = 0
    total_requests = 0

    for job_idx, job in enumerate(jobs):
        # ── Find earliest available date to avoid pointless requests ──
        head_date = _query_head_timestamp(ib, job)
        effective_start = start_date
        if head_date and head_date > start_date:
            effective_start = head_date
            logger.info(
                "[%d/%d] %s %s: earliest data = %s",
                job_idx + 1, len(jobs), job.symbol, job.data_type,
                head_date.isoformat(),
            )

        windows = _generate_yearly_windows(effective_start, end_date)
        job_bars = 0
        job_failures = 0

        logger.info(
            "[%d/%d] %s %s → %s  (%d year-chunks, from %s)",
            job_idx + 1, len(jobs), job.symbol, job.data_type,
            job.instrument_id, len(windows), effective_start.isoformat(),
        )

        consecutive_empty = 0
        for win_idx, (end_str, dur_str) in enumerate(windows):
            total_requests += 1
            try:
                bars = mds.request_historical_bars(
                    symbol=job.symbol,
                    duration=dur_str,
                    bar_size=job.bar_size,
                    data_type=job.data_type,
                    sec_type=job.sec_type,
                    exchange=job.exchange,
                    currency=job.currency,
                    end_date=end_str,
                )

                if bars:
                    n = write_bars_to_db(bars, job.instrument_id, writer, job.currency)
                    job_bars += n
                    consecutive_empty = 0
                else:
                    consecutive_empty += 1
                    logger.info(
                        "  chunk %d/%d (%s, %s): 0 bars (empty streak: %d)",
                        win_idx + 1, len(windows), end_str[:8], dur_str,
                        consecutive_empty,
                    )
                    # Only stop after 2+ consecutive empty chunks —
                    # the first chunk may land on a weekend/holiday.
                    if consecutive_empty >= 2:
                        logger.info("  → stopping early after %d consecutive empty chunks",
                                    consecutive_empty)
                        break

            except Exception as exc:
                logger.error(
                    "  chunk %d/%d (%s, %s): FAILED — %s",
                    win_idx + 1, len(windows), end_str[:8], dur_str, exc,
                )
                job_failures += 1
                # Continue to next chunk — might be a transient error.

        total_bars += job_bars
        total_failures += job_failures
        logger.info(
            "[%d/%d] %s %s: %d bars written (%d failures)",
            job_idx + 1, len(jobs), job.symbol, job.data_type,
            job_bars, job_failures,
        )

    logger.info(
        "Batched backfill complete: %d bars, %d failures, %d API requests",
        total_bars, total_failures, total_requests,
    )
    return total_bars, total_failures


def write_bars_to_db(
    bars: List[BarData],
    instrument_id: str,
    writer: DataWriter,
    currency: str = "USD",
) -> int:
    """Write BarData list to prices_daily via DataWriter."""
    if not bars:
        return 0

    rows: List[PriceBar] = []
    for bar in bars:
        rows.append(PriceBar(
            instrument_id=instrument_id,
            trade_date=bar.trade_date,
            open=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close,
            adjusted_close=bar.close,  # IBKR data is already adjusted
            volume=bar.volume,
            currency=currency,
            metadata={"source": "ibkr", "data_type": bar.data_type},
        ))

    writer.write_prices(rows)
    return len(rows)


def main(argv: Optional[list[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Backfill historical data from IBKR",
    )
    parser.add_argument("--host", type=str, default="127.0.0.1",
                        help="IBKR Gateway/TWS host")
    parser.add_argument("--port", type=int, default=4001,
                        help="IBKR Gateway/TWS port (4001=paper, 7496=live)")
    parser.add_argument("--client-id", type=int, default=10,
                        help="API client ID (use different from trading client)")
    parser.add_argument("--duration", type=str, default="1 Y",
                        help="IBKR duration string for single-pass mode")
    parser.add_argument("--start-date", type=str, default=None,
                        help="Backfill to this date (YYYY-MM-DD). "
                             "Enables batched year-by-year mode.")
    parser.add_argument("--data-types", type=str, default=None,
                        help="Comma-separated data types to include "
                             "(e.g. TRADES,HISTORICAL_VOLATILITY). "
                             "Default: all.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show jobs without connecting to IBKR")
    args = parser.parse_args(argv)

    batched = args.start_date is not None
    jobs = build_backfill_jobs(duration=args.duration)

    if args.data_types:
        allowed = {dt.strip().upper() for dt in args.data_types.split(",")}
        jobs = [j for j in jobs if j.data_type in allowed]
        logger.info("Filtered to data types %s: %d jobs", allowed, len(jobs))

    if batched:
        start_dt = date.fromisoformat(args.start_date)
        end_dt = date.today()
        years = (end_dt - start_dt).days / 365.25
        logger.info(
            "Batched backfill: %d instruments × ~%.0f years back to %s",
            len(jobs), years, args.start_date,
        )
    else:
        logger.info("Built %d backfill jobs (duration=%s)", len(jobs), args.duration)

    if args.dry_run:
        for i, job in enumerate(jobs):
            print(f"  [{i+1:2d}/{len(jobs)}] {job.symbol:6s} {job.data_type:30s} "
                  f"→ {job.instrument_id}")
        if batched:
            # Rough estimate: 39 instruments, ~avg 20 years each
            est_requests = len(jobs) * int(years)
            est_min = (est_requests // 60) * 10 + 10
            print(f"\nBatched to {args.start_date}: ~{est_requests} requests "
                  f"(~{est_min} min with rate limiting)")
        else:
            print(f"\nTotal: {len(jobs)} requests "
                  f"(~{len(jobs) // 60 * 10 + 10} min with rate limiting)")
        return

    # Connect to IBKR
    from prometheus.execution.ib_compat import IB

    ib = IB()
    logger.info("Connecting to IBKR at %s:%d (client_id=%d)...",
                args.host, args.port, args.client_id)
    ib.connect(host=args.host, port=args.port, clientId=args.client_id)
    logger.info("Connected to IBKR")

    db = get_db_manager()
    writer = DataWriter(db_manager=db)

    try:
        if batched:
            total_bars, failures = run_batched_backfill(
                ib, jobs, start_dt, end_dt, writer,
            )
        else:
            # ── Single-pass mode (original behaviour) ─────────────
            mds = IbkrMarketDataService(ib)
            total_bars = 0
            failures = 0

            for i, job in enumerate(jobs):
                logger.info("[%d/%d] %s %s → %s",
                            i + 1, len(jobs), job.symbol, job.data_type,
                            job.instrument_id)
                try:
                    bars = mds.request_historical_bars(
                        symbol=job.symbol,
                        duration=job.duration,
                        bar_size=job.bar_size,
                        data_type=job.data_type,
                        sec_type=job.sec_type,
                        exchange=job.exchange,
                        currency=job.currency,
                    )

                    if bars:
                        n = write_bars_to_db(bars, job.instrument_id, writer,
                                             job.currency)
                        total_bars += n
                        logger.info("[%d/%d] %s: %d bars written",
                                    i + 1, len(jobs), job.instrument_id, n)
                    else:
                        logger.warning("[%d/%d] %s: no bars returned",
                                       i + 1, len(jobs), job.instrument_id)

                except Exception as exc:
                    logger.error("[%d/%d] %s: FAILED — %s",
                                 i + 1, len(jobs), job.instrument_id, exc)
                    failures += 1

            logger.info("Backfill complete: %d bars written, %d failures out of %d jobs",
                        total_bars, failures, len(jobs))

    finally:
        ib.disconnect()
        logger.info("Disconnected from IBKR")


if __name__ == "__main__":
    main()
