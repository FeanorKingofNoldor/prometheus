"""IBKR live implied-volatility lookup with fallback chain.

The selection layer (Phase 1+) needs IV per candidate strike to pick
the right delta. Today the codebase uses VIX as a stand-in for sigma
everywhere, which biases strike selection systematically for anything
that is not SPY — and is the documented reason single-name
short-premium was disabled in the legacy backtest (-EV because
single-stock IV is much higher than VIX).

This module returns a per-contract IV with explicit provenance, in
priority order:

1. ``ibkr_live`` — IBKR ``modelGreeks.impliedVol`` from a snapshot
   request (genericTickList=106).
2. ``cache`` — value we successfully resolved earlier in the same
   session, within the TTL.
3. ``realized_vol`` — 30-day historical vol of the *underlying* (also
   from IBKR, tick 104), used as a proxy for the strike's IV. Less
   accurate but still beats VIX for single names.
4. ``vix_fallback`` — final fallback to a caller-provided VIX-as-sigma
   value, with a logged warning so we can audit how often this fires.

The lookup is batchable per underlying so the selection pipeline can
size a chain of candidate strikes in one round-trip.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Protocol

from apatheon.core.logging import get_logger

logger = get_logger(__name__)


IV_SOURCE_LIVE = "ibkr_live"
IV_SOURCE_CACHE = "cache"
IV_SOURCE_REALIZED = "realized_vol"
IV_SOURCE_FALLBACK = "vix_fallback"


@dataclass(frozen=True)
class IvLookupResult:
    """IV value with provenance for the audit log."""

    iv: float
    source: str
    underlying_price: float | None = None
    fetched_at: float = 0.0


class IvLookupLike(Protocol):
    """Structural type satisfied by both the live ``IvLookupService``
    and the backtest harness's ``BacktestIvLookup``. Consumers in
    ``selection`` / ``runner`` accept this protocol so either source
    works without inheritance."""

    def get_iv_batch(
        self, contracts: Iterable[Any], *, fallback_iv: float,
    ) -> dict[str, IvLookupResult]: ...


@dataclass(frozen=True)
class _CacheEntry:
    iv: float
    underlying_price: float | None
    created_at: float
    ttl_sec: float

    @property
    def expired(self) -> bool:
        return (time.monotonic() - self.created_at) > self.ttl_sec


def _contract_key(contract: Any) -> str:
    """Cache key from an IBKR-style contract object."""
    symbol = getattr(contract, "symbol", "?")
    expiry = getattr(contract, "lastTradeDateOrContractMonth", "")
    strike = getattr(contract, "strike", 0)
    right = getattr(contract, "right", "")
    return f"{symbol}:{expiry}:{strike}:{right}"


class IvLookupService:
    """Resolve implied volatility per contract with a fallback chain.

    Parameters
    ----------
    ib
        Connected ``ib_async.IB`` (or compatible) instance. May be
        ``None`` for offline / testing use — in that case every lookup
        falls through to ``vix_fallback``.
    cache_ttl_sec
        Duration to cache a successful IV resolution (default 1h).
    snapshot_wait_sec
        Time to wait for ``reqMktData`` snapshot to populate. IBKR is
        usually <500ms for liquid SPY strikes; bumps up for sector
        ETFs.
    realized_vol_ttl_sec
        Realized-vol values change slowly; cache aggressively
        (default 24h) since they're a fallback anyway.
    """

    def __init__(
        self,
        ib: Any = None,
        *,
        cache_ttl_sec: float = 3600.0,
        snapshot_wait_sec: float = 2.0,
        realized_vol_ttl_sec: float = 86400.0,
    ) -> None:
        self._ib = ib
        self._cache_ttl = cache_ttl_sec
        self._snapshot_wait = snapshot_wait_sec
        self._realized_vol_ttl = realized_vol_ttl_sec

        self._cache: dict[str, _CacheEntry] = {}
        self._realized_vol: dict[str, _CacheEntry] = {}
        self._lock = threading.Lock()

        # Telemetry — useful for "how often are we falling back to VIX?"
        self._counters: dict[str, int] = {
            IV_SOURCE_LIVE: 0,
            IV_SOURCE_CACHE: 0,
            IV_SOURCE_REALIZED: 0,
            IV_SOURCE_FALLBACK: 0,
        }

    # ── Public API ────────────────────────────────────────────────────

    def get_iv(
        self,
        contract: Any,
        *,
        fallback_iv: float,
    ) -> IvLookupResult:
        """Resolve IV for one contract."""
        return self.get_iv_batch([contract], fallback_iv=fallback_iv)[
            _contract_key(contract)
        ]

    def get_iv_batch(
        self,
        contracts: Iterable[Any],
        *,
        fallback_iv: float,
    ) -> dict[str, IvLookupResult]:
        """Resolve IV for many contracts in one round-trip.

        Returns a dict keyed by ``_contract_key(contract)`` so callers
        can look up by the same key the cache uses.

        Cached entries are returned without an IBKR call. Uncached
        entries are batched into a single snapshot request, then any
        that came back blank fall through to realized-vol / VIX.
        """
        contracts = list(contracts)
        if not contracts:
            return {}

        results: dict[str, IvLookupResult] = {}
        to_fetch: list[tuple[Any, str]] = []

        # Step 1: cache.
        for c in contracts:
            key = _contract_key(c)
            cached = self._cached(key)
            if cached is not None:
                results[key] = IvLookupResult(
                    iv=cached.iv,
                    source=IV_SOURCE_CACHE,
                    underlying_price=cached.underlying_price,
                    fetched_at=cached.created_at,
                )
                self._counters[IV_SOURCE_CACHE] += 1
            else:
                to_fetch.append((c, key))

        # Step 2: live IBKR snapshot, batched.
        if to_fetch and self._ib is not None:
            live = self._fetch_live_batch([c for c, _ in to_fetch])
            now = time.monotonic()
            for c, key in to_fetch:
                if key in live:
                    iv, und_price = live[key]
                    self._store_cache(key, iv, und_price)
                    results[key] = IvLookupResult(
                        iv=iv, source=IV_SOURCE_LIVE,
                        underlying_price=und_price, fetched_at=now,
                    )
                    self._counters[IV_SOURCE_LIVE] += 1

        # Step 3: realized-vol fallback (per underlying), then VIX.
        for c, key in to_fetch:
            if key in results:
                continue
            symbol = getattr(c, "symbol", "?")
            rv = self._realized_vol_for(symbol)
            if rv is not None:
                results[key] = IvLookupResult(
                    iv=rv, source=IV_SOURCE_REALIZED,
                    underlying_price=None, fetched_at=time.monotonic(),
                )
                self._counters[IV_SOURCE_REALIZED] += 1
            else:
                logger.warning(
                    "IvLookupService: %s falling back to VIX-as-sigma "
                    "(iv=%.3f) — selection will be biased",
                    key, fallback_iv,
                )
                results[key] = IvLookupResult(
                    iv=fallback_iv, source=IV_SOURCE_FALLBACK,
                    underlying_price=None, fetched_at=time.monotonic(),
                )
                self._counters[IV_SOURCE_FALLBACK] += 1

        return results

    def telemetry(self) -> dict[str, int]:
        """Return counters keyed by source — for monitoring fallback rate."""
        with self._lock:
            return dict(self._counters)

    def clear_cache(self) -> None:
        with self._lock:
            self._cache.clear()
            self._realized_vol.clear()

    # ── Internal: cache plumbing ──────────────────────────────────────

    def _cached(self, key: str) -> _CacheEntry | None:
        with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                return None
            if entry.expired:
                self._cache.pop(key, None)
                return None
            return entry

    def _store_cache(self, key: str, iv: float, underlying_price: float | None) -> None:
        with self._lock:
            self._cache[key] = _CacheEntry(
                iv=iv, underlying_price=underlying_price,
                created_at=time.monotonic(), ttl_sec=self._cache_ttl,
            )

    # ── Internal: IBKR live fetch ─────────────────────────────────────

    def _fetch_live_batch(self, contracts: list[Any]) -> dict[str, tuple[float, float | None]]:
        """Issue a snapshot reqMktData per contract and collect IVs.

        Returns ``{key: (iv, underlying_price)}`` for every contract
        that returned a non-zero IV. Missing keys mean fall through.
        """
        out: dict[str, tuple[float, float | None]] = {}
        tickers: list[tuple[Any, Any, str]] = []

        for c in contracts:
            key = _contract_key(c)
            try:
                ticker = self._ib.reqMktData(c, genericTickList="106", snapshot=True)
            except TypeError:
                # Older ib_async signatures may not accept snapshot kw.
                ticker = self._ib.reqMktData(c, genericTickList="106")
            except Exception as exc:
                logger.debug("reqMktData failed for %s: %s", key, exc)
                continue
            tickers.append((c, ticker, key))

        if not tickers:
            return out

        try:
            self._ib.sleep(self._snapshot_wait)
        except Exception:
            time.sleep(self._snapshot_wait)

        for c, ticker, key in tickers:
            try:
                model = getattr(ticker, "modelGreeks", None)
                iv = float(getattr(model, "impliedVol", 0) or 0) if model else 0.0
                und = float(getattr(model, "undPrice", 0) or 0) if model else 0.0
                if iv > 0:
                    out[key] = (iv, und if und > 0 else None)
            finally:
                try:
                    self._ib.cancelMktData(c)
                except Exception:
                    pass

        return out

    # ── Internal: realized-vol fallback ───────────────────────────────

    def _realized_vol_for(self, symbol: str) -> float | None:
        """Return 30-day realized vol for the underlying, cached."""
        with self._lock:
            entry = self._realized_vol.get(symbol)
            if entry is not None and not entry.expired:
                return entry.iv
            if entry is not None:
                self._realized_vol.pop(symbol, None)

        if self._ib is None:
            return None

        from prometheus.execution.ib_compat import Stock

        contract = Stock(symbol, "SMART", "USD")
        try:
            qualified = self._ib.qualifyContracts(contract)
            if not qualified:
                return None
            contract = qualified[0]
            try:
                ticker = self._ib.reqMktData(contract, genericTickList="104", snapshot=True)
            except TypeError:
                ticker = self._ib.reqMktData(contract, genericTickList="104")
            try:
                self._ib.sleep(self._snapshot_wait)
            except Exception:
                time.sleep(self._snapshot_wait)
            hv = float(getattr(ticker, "histVolatility", 0) or 0)
        except Exception as exc:
            logger.debug("realized vol fetch failed for %s: %s", symbol, exc)
            return None
        finally:
            try:
                self._ib.cancelMktData(contract)
            except Exception:
                pass

        if hv <= 0:
            return None

        with self._lock:
            self._realized_vol[symbol] = _CacheEntry(
                iv=hv, underlying_price=None,
                created_at=time.monotonic(),
                ttl_sec=self._realized_vol_ttl,
            )
        return hv


__all__ = [
    "IvLookupResult",
    "IvLookupLike",
    "IvLookupService",
    "IV_SOURCE_LIVE",
    "IV_SOURCE_CACHE",
    "IV_SOURCE_REALIZED",
    "IV_SOURCE_FALLBACK",
]
