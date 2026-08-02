"""Prometheus v2 – FX conversion for multi-market execution.

Position sizing, notional risk limits and NAV budgets are expressed in
USD, but non-US instruments trade (and are priced in ``prices_daily``)
in their local currency. This module converts local amounts to USD using
the ``fx_rates_daily`` table in the historical DB, which is populated by
apatheon (:mod:`apatheon.data_ingestion.fx_rates` — daily scheduler job
plus :mod:`prometheus.scripts.backfill.backfill_fx_rates` for history).

Rates are stored in ``XXXUSD`` convention: USD per 1 unit of XXX.

Quote-unit quirk: EODHD quotes LSE equities in **pence (GBX)**, not
pounds — verified empirically 2026-07-03 (AAL.LSE close 3741 = £37.41,
BP.LSE 464.4 = £4.64, HSBA.LSE 1445.2 = £14.45 in both ``prices_daily``
and the live EODHD API). :meth:`FxConverter.price_to_usd` divides by 100
for instruments whose exchange suffix is in :data:`PENCE_QUOTED_SUFFIXES`
before applying the GBP→USD rate.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Dict, Tuple

from apatheon.core.logging import get_logger

logger = get_logger(__name__)

# Exchange suffixes (the part of instrument_id after the last '.') whose
# EODHD price quotes are in 1/100 of the instrument currency. Verified
# 2026-07-03: LSE closes in prices_daily ARE pence/GBX (see module
# docstring for the evidence).
PENCE_QUOTED_SUFFIXES: frozenset[str] = frozenset({"LSE"})

# Currencies whose exchanges quote prices (and ticks) in whole units.
# Used by the order planner to round limit prices; anything not listed
# rounds to 2 decimal places.
CURRENCY_DECIMALS: Dict[str, int] = {"KRW": 0, "JPY": 0}
DEFAULT_CURRENCY_DECIMALS: int = 2

# How far back from ``as_of`` we accept a rate before declaring it stale.
# FX trades ~5 days a week, so 5 calendar days always spans a weekend
# plus a holiday without ever accepting a week-old rate.
MAX_RATE_STALENESS_DAYS: int = 5


class FxRateUnavailable(RuntimeError):
    """Raised when no usable FX rate exists for a (currency, date) pair."""


def _pence_divisor(instrument_id: str) -> float:
    """Return 100.0 when the instrument's exchange quotes in pence, else 1.0."""
    _, _, suffix = instrument_id.rpartition(".")
    return 100.0 if suffix in PENCE_QUOTED_SUFFIXES else 1.0


class FxConverter:
    """Converts local-currency amounts and prices to USD.

    Reads ``fx_rates_daily`` (historical DB) through the supplied
    ``db_manager``. Lookups are memoised per (currency, as_of) for the
    lifetime of the converter, so a converter created per pipeline cycle
    performs at most one query per currency per day.
    """

    def __init__(self, db_manager) -> None:
        self._db_manager = db_manager
        self._cache: Dict[Tuple[str, date], float] = {}

    def usd_rate(self, currency: str, as_of: date) -> float:
        """USD per 1 unit of ``currency`` as of ``as_of``.

        Uses the latest rate with ``trade_date <= as_of`` within the
        :data:`MAX_RATE_STALENESS_DAYS` window. Raises
        :class:`FxRateUnavailable` when no such rate exists — callers
        must treat that as "cannot price this order", never as 1.0.
        """
        currency = currency.upper()
        if currency == "USD":
            return 1.0

        key = (currency, as_of)
        cached = self._cache.get(key)
        if cached is not None:
            return cached

        pair = f"{currency}USD"
        earliest = as_of - timedelta(days=MAX_RATE_STALENESS_DAYS)
        with self._db_manager.get_historical_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    "SELECT rate FROM fx_rates_daily "
                    "WHERE pair = %s AND trade_date <= %s AND trade_date >= %s "
                    "ORDER BY trade_date DESC LIMIT 1",
                    (pair, as_of, earliest),
                )
                row = cur.fetchone()
            finally:
                cur.close()

        if not row or not row[0] or float(row[0]) <= 0:
            raise FxRateUnavailable(
                f"no {pair} rate in fx_rates_daily within {MAX_RATE_STALENESS_DAYS} "
                f"days of {as_of} — run backfill_fx_rates or check the apatheon "
                f"fx_rates_refresh job"
            )

        rate = float(row[0])
        self._cache[key] = rate
        return rate

    def to_usd(self, amount: float, currency: str, as_of: date) -> float:
        """Convert a local-currency amount (already in whole units) to USD."""
        return float(amount) * self.usd_rate(currency, as_of)

    def price_to_usd(
        self, price: float, currency: str, instrument_id: str, as_of: date
    ) -> float:
        """Convert a quoted instrument price to USD.

        Unlike :meth:`to_usd`, this also normalises quote units: LSE
        prices come in pence (GBX), so they are divided by 100 before the
        GBP→USD rate is applied.
        """
        return (float(price) / _pence_divisor(instrument_id)) * self.usd_rate(
            currency, as_of
        )


__all__ = [
    "CURRENCY_DECIMALS",
    "DEFAULT_CURRENCY_DECIMALS",
    "FxConverter",
    "FxRateUnavailable",
    "MAX_RATE_STALENESS_DAYS",
    "PENCE_QUOTED_SUFFIXES",
]
