#!/usr/bin/env python3
"""Backfill volatility indices into prices_daily.

Sources:
  - FRED API v1:  VIXCLS (VIX daily, 1990-01-02 → present)
  - CBOE CDN CSV: VIX3M, VIX9D, VIX6M, VIX1Y, SKEW

All series are stored in ``prices_daily`` with instrument_id like
``VIX.INDX``, ``VIX3M.INDX``, etc.  The ``close`` column holds the
index level; OHLCV fields that don't apply are set to the close value
(O=H=L=C) with volume=0.

Usage::

    source venv/bin/activate
    python -m prometheus.scripts.backfill.backfill_vol_indices [--dry-run]
"""

from __future__ import annotations

import argparse
import csv
import io
from datetime import date
from typing import Optional

import requests
from apatheon.core.database import get_db_manager
from apatheon.core.logging import get_logger
from apatheon.data.types import PriceBar
from apatheon.data.writer import DataWriter
from apatheon.data_ingestion.fred_client import FredClient
from dotenv import load_dotenv

logger = get_logger(__name__)

# ── CBOE CSV URLs ────────────────────────────────────────────────────

CBOE_INDICES = {
    "VIX3M.INDX": "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX3M_History.csv",
    "VIX9D.INDX": "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX9D_History.csv",
    "VIX6M.INDX": "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX6M_History.csv",
    "VIX1Y.INDX": "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX1Y_History.csv",
    "SKEW.INDX": "https://cdn.cboe.com/api/global/us_indices/daily_prices/SKEW_History.csv",
}


# ── Helpers ──────────────────────────────────────────────────────────

def _obs_to_pricebar(
    instrument_id: str,
    trade_date: date,
    value: float,
    source: str,
) -> PriceBar:
    """Convert a scalar observation into a PriceBar (O=H=L=C=value)."""
    return PriceBar(
        instrument_id=instrument_id,
        trade_date=trade_date,
        open=value,
        high=value,
        low=value,
        close=value,
        adjusted_close=value,
        volume=0.0,
        currency="USD",
        metadata={"source": source},
    )


def fetch_fred_vix(
    client: FredClient,
    start: Optional[date] = None,
    end: Optional[date] = None,
) -> list[PriceBar]:
    """Fetch VIXCLS from FRED and convert to PriceBars."""
    obs = client.get_series_observations(
        "VIXCLS",
        start_date=start or date(1990, 1, 2),
        end_date=end or date.today(),
    )
    bars = [
        _obs_to_pricebar("VIX.INDX", o.trade_date, o.value, "fred")
        for o in obs
    ]
    logger.info("FRED VIXCLS: %d observations (%s → %s)",
                len(bars),
                bars[0].trade_date if bars else "?",
                bars[-1].trade_date if bars else "?")
    return bars


def fetch_cboe_csv(
    instrument_id: str,
    url: str,
) -> list[PriceBar]:
    """Download a CBOE historical CSV and parse into PriceBars.

    CBOE CSVs typically have columns: DATE, OPEN, HIGH, LOW, CLOSE
    or sometimes: Date, {index_name}
    """
    logger.info("Fetching CBOE CSV for %s: %s", instrument_id, url)
    resp = requests.get(url, timeout=60)
    if resp.status_code != 200:
        logger.error("CBOE CSV download failed: %s → %d", url, resp.status_code)
        return []

    text = resp.text.strip()
    # Some CBOE files have BOM or leading whitespace
    if text.startswith("\ufeff"):
        text = text[1:]

    reader = csv.DictReader(io.StringIO(text))
    bars: list[PriceBar] = []

    for row in reader:
        try:
            # CBOE uses various date column names
            date_str = (
                row.get("DATE")
                or row.get("Date")
                or row.get("date")
                or ""
            ).strip()
            if not date_str:
                continue

            # Parse date: CBOE uses MM/DD/YYYY or YYYY-MM-DD
            if "/" in date_str:
                parts = date_str.split("/")
                trade_date = date(int(parts[2]), int(parts[0]), int(parts[1]))
            else:
                trade_date = date.fromisoformat(date_str)

            # Try CLOSE column, then the second column (some files just
            # have Date + value)
            close_str = (
                row.get("CLOSE")
                or row.get("Close")
                or row.get("close")
            )
            if close_str is None:
                # Fall back to first non-date column
                for k, v in row.items():
                    if k.upper() not in ("DATE",) and v:
                        close_str = v
                        break

            if not close_str or close_str.strip() in ("", "."):
                continue

            value = float(close_str.strip())

            # Build OHLC from available columns or just use close
            open_val = _safe_float(row.get("OPEN") or row.get("Open"), value)
            high_val = _safe_float(row.get("HIGH") or row.get("High"), value)
            low_val = _safe_float(row.get("LOW") or row.get("Low"), value)

            # Sanitise OHLC: CBOE CSVs sometimes have high < low or
            # open/close outside the high/low band.
            true_high = max(open_val, high_val, low_val, value)
            true_low = min(open_val, high_val, low_val, value)
            high_val = true_high
            low_val = true_low

            bars.append(PriceBar(
                instrument_id=instrument_id,
                trade_date=trade_date,
                open=open_val,
                high=high_val,
                low=low_val,
                close=value,
                adjusted_close=value,
                volume=0.0,
                currency="USD",
                metadata={"source": "cboe"},
            ))
        except (ValueError, KeyError, IndexError) as exc:
            logger.debug("Skipping bad CBOE row for %s: %s (%s)",
                         instrument_id, row, exc)

    logger.info("CBOE %s: %d rows (%s → %s)",
                instrument_id,
                len(bars),
                bars[0].trade_date if bars else "?",
                bars[-1].trade_date if bars else "?")
    return bars


def _safe_float(val: Optional[str], default: float) -> float:
    """Parse a float or return default."""
    if val is None:
        return default
    val = val.strip()
    if not val or val == ".":
        return default
    try:
        return float(val)
    except ValueError:
        return default


# ── Main ─────────────────────────────────────────────────────────────

def main() -> None:
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Backfill volatility indices (VIX, VIX3M, VIX9D, VIX6M, VIX1Y, SKEW)"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch data but don't write to DB")
    parser.add_argument("--fred-only", action="store_true",
                        help="Only fetch FRED VIXCLS")
    parser.add_argument("--cboe-only", action="store_true",
                        help="Only fetch CBOE term structure CSVs")
    parser.add_argument("--start", type=str, default=None,
                        help="Start date (YYYY-MM-DD) for FRED fetch")
    args = parser.parse_args()

    start = date.fromisoformat(args.start) if args.start else None

    db = get_db_manager()
    writer = DataWriter(db_manager=db)

    total_written = 0

    # ── FRED: VIX (VIXCLS) ───────────────────────────────────────────
    if not args.cboe_only:
        logger.info("=" * 60)
        logger.info("Fetching VIX from FRED (VIXCLS)...")
        fred = FredClient()
        try:
            vix_bars = fetch_fred_vix(fred, start=start)
            if not args.dry_run and vix_bars:
                writer.write_prices(vix_bars)
                total_written += len(vix_bars)
                logger.info("Wrote %d VIX bars to prices_daily", len(vix_bars))
            else:
                logger.info("[DRY RUN] Would write %d VIX bars", len(vix_bars))
        finally:
            fred.close()

    # ── CBOE: Term structure + SKEW ──────────────────────────────────
    if not args.fred_only:
        for instrument_id, url in CBOE_INDICES.items():
            logger.info("=" * 60)
            logger.info("Fetching %s from CBOE...", instrument_id)
            try:
                bars = fetch_cboe_csv(instrument_id, url)
                if not args.dry_run and bars:
                    writer.write_prices(bars)
                    total_written += len(bars)
                    logger.info("Wrote %d bars for %s", len(bars), instrument_id)
                else:
                    logger.info("[DRY RUN] Would write %d bars for %s",
                                len(bars), instrument_id)
            except Exception:
                logger.exception("Failed to fetch %s", instrument_id)

    # ── Summary ──────────────────────────────────────────────────────
    logger.info("=" * 60)
    if args.dry_run:
        logger.info("DRY RUN complete.  No data written.")
    else:
        logger.info("Backfill complete.  Total rows written: %d", total_written)


if __name__ == "__main__":
    main()
