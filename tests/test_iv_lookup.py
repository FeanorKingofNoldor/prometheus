"""Tests for prometheus.derivatives.iv_lookup."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from prometheus.derivatives import iv_lookup

# ── Fake IBKR primitives ─────────────────────────────────────────────


@dataclass
class _FakeContract:
    symbol: str
    strike: float
    right: str
    lastTradeDateOrContractMonth: str = "20260620"


@dataclass
class _FakeModelGreeks:
    impliedVol: float
    undPrice: float = 0.0


@dataclass
class _FakeTicker:
    modelGreeks: Any = None
    histVolatility: float = 0.0


class _FakeIb:
    """In-memory stand-in for ``ib_async.IB`` covering only what the
    lookup uses: ``reqMktData``, ``cancelMktData``, ``sleep``, and
    ``qualifyContracts`` for the realized-vol path."""

    def __init__(self, iv_by_key: dict[str, float] | None = None) -> None:
        # iv_by_key keyed by _contract_key(contract)
        self._iv = iv_by_key or {}
        self.sleep_calls: list[float] = []
        self.req_calls: list[str] = []
        self.cancel_calls: list[str] = []

    def reqMktData(self, contract, genericTickList="", snapshot=False):
        key = iv_lookup._contract_key(contract)
        self.req_calls.append(key)
        iv = self._iv.get(key, 0.0)
        if iv > 0:
            return _FakeTicker(modelGreeks=_FakeModelGreeks(impliedVol=iv, undPrice=500.0))
        return _FakeTicker(modelGreeks=_FakeModelGreeks(impliedVol=0.0))

    def cancelMktData(self, contract) -> None:
        self.cancel_calls.append(iv_lookup._contract_key(contract))

    def sleep(self, sec: float) -> None:
        self.sleep_calls.append(sec)

    def qualifyContracts(self, *contracts):
        return list(contracts)


def _c(strike: float, right: str = "P", symbol: str = "SPY") -> _FakeContract:
    return _FakeContract(symbol=symbol, strike=strike, right=right)


# ── Tests ────────────────────────────────────────────────────────────


def test_offline_lookup_falls_through_to_vix():
    svc = iv_lookup.IvLookupService(ib=None)
    result = svc.get_iv(_c(500), fallback_iv=0.22)
    assert result.iv == 0.22
    assert result.source == iv_lookup.IV_SOURCE_FALLBACK


def test_live_iv_returns_from_ibkr():
    ib = _FakeIb({"SPY:20260620:500:P": 0.27})
    svc = iv_lookup.IvLookupService(ib=ib, snapshot_wait_sec=0.0)
    result = svc.get_iv(_c(500), fallback_iv=0.22)
    assert result.source == iv_lookup.IV_SOURCE_LIVE
    assert result.iv == 0.27
    assert result.underlying_price == 500.0
    assert ib.cancel_calls == ["SPY:20260620:500:P"]


def test_second_call_returns_from_cache():
    ib = _FakeIb({"SPY:20260620:500:P": 0.27})
    svc = iv_lookup.IvLookupService(ib=ib, snapshot_wait_sec=0.0)
    svc.get_iv(_c(500), fallback_iv=0.22)
    ib.req_calls.clear()
    result = svc.get_iv(_c(500), fallback_iv=0.22)
    assert result.source == iv_lookup.IV_SOURCE_CACHE
    assert result.iv == 0.27
    assert ib.req_calls == []


def test_expired_cache_refetches():
    ib = _FakeIb({"SPY:20260620:500:P": 0.27})
    svc = iv_lookup.IvLookupService(ib=ib, snapshot_wait_sec=0.0, cache_ttl_sec=0.01)
    svc.get_iv(_c(500), fallback_iv=0.22)
    time.sleep(0.02)
    ib.req_calls.clear()
    result = svc.get_iv(_c(500), fallback_iv=0.22)
    assert result.source == iv_lookup.IV_SOURCE_LIVE
    assert ib.req_calls == ["SPY:20260620:500:P"]


def test_batch_with_mixed_results():
    ib = _FakeIb({
        "SPY:20260620:500:P": 0.28,
        "SPY:20260620:490:P": 0.30,
        # 480 strike: no IV — falls back
    })
    svc = iv_lookup.IvLookupService(ib=ib, snapshot_wait_sec=0.0)
    res = svc.get_iv_batch(
        [_c(500), _c(490), _c(480)], fallback_iv=0.22,
    )
    assert res["SPY:20260620:500:P"].source == iv_lookup.IV_SOURCE_LIVE
    assert res["SPY:20260620:490:P"].source == iv_lookup.IV_SOURCE_LIVE
    # 480 had zero IV → no realized vol either → falls back to VIX
    assert res["SPY:20260620:480:P"].source == iv_lookup.IV_SOURCE_FALLBACK
    assert res["SPY:20260620:480:P"].iv == 0.22


def test_realized_vol_used_when_iv_blank_but_underlying_vol_cached():
    ib = _FakeIb({})  # No IV available
    svc = iv_lookup.IvLookupService(ib=ib, snapshot_wait_sec=0.0)
    # Pre-populate realized vol for SPY
    svc._realized_vol["SPY"] = iv_lookup._CacheEntry(
        iv=0.18, underlying_price=None,
        created_at=time.monotonic(), ttl_sec=86400.0,
    )
    result = svc.get_iv(_c(500), fallback_iv=0.22)
    assert result.source == iv_lookup.IV_SOURCE_REALIZED
    assert result.iv == 0.18


def test_batch_partially_cached_only_fetches_uncached():
    ib = _FakeIb({
        "SPY:20260620:500:P": 0.28,
        "SPY:20260620:490:P": 0.30,
    })
    svc = iv_lookup.IvLookupService(ib=ib, snapshot_wait_sec=0.0)
    svc.get_iv(_c(500), fallback_iv=0.22)
    ib.req_calls.clear()

    res = svc.get_iv_batch([_c(500), _c(490)], fallback_iv=0.22)
    # Only 490 hit the wire; 500 came from cache.
    assert ib.req_calls == ["SPY:20260620:490:P"]
    assert res["SPY:20260620:500:P"].source == iv_lookup.IV_SOURCE_CACHE
    assert res["SPY:20260620:490:P"].source == iv_lookup.IV_SOURCE_LIVE


def test_empty_batch_returns_empty_dict():
    svc = iv_lookup.IvLookupService(ib=None)
    assert svc.get_iv_batch([], fallback_iv=0.22) == {}


def test_telemetry_counts_each_source():
    ib = _FakeIb({"SPY:20260620:500:P": 0.28})
    svc = iv_lookup.IvLookupService(ib=ib, snapshot_wait_sec=0.0)

    svc.get_iv(_c(500), fallback_iv=0.22)  # live
    svc.get_iv(_c(500), fallback_iv=0.22)  # cache
    svc.get_iv(_c(490), fallback_iv=0.22)  # fallback (no IV, no realized vol)

    t = svc.telemetry()
    assert t[iv_lookup.IV_SOURCE_LIVE] == 1
    assert t[iv_lookup.IV_SOURCE_CACHE] == 1
    assert t[iv_lookup.IV_SOURCE_FALLBACK] == 1


def test_clear_cache_purges_both_caches():
    ib = _FakeIb({"SPY:20260620:500:P": 0.28})
    svc = iv_lookup.IvLookupService(ib=ib, snapshot_wait_sec=0.0)
    svc.get_iv(_c(500), fallback_iv=0.22)
    svc._realized_vol["SPY"] = iv_lookup._CacheEntry(
        iv=0.18, underlying_price=None,
        created_at=time.monotonic(), ttl_sec=86400.0,
    )
    svc.clear_cache()
    assert svc._cache == {}
    assert svc._realized_vol == {}


def test_reqmktdata_typeerror_falls_back_to_legacy_signature():
    """Older ib_async releases reject the snapshot kwarg — the helper
    catches TypeError and retries without it."""
    calls: list[dict[str, Any]] = []

    class _PickyIb(_FakeIb):
        def reqMktData(self, contract, genericTickList="", snapshot=False):
            if snapshot:
                raise TypeError("unexpected keyword 'snapshot'")
            calls.append({"contract": iv_lookup._contract_key(contract)})
            return _FakeTicker(
                modelGreeks=_FakeModelGreeks(impliedVol=0.21, undPrice=500.0)
            )

    svc = iv_lookup.IvLookupService(ib=_PickyIb(), snapshot_wait_sec=0.0)
    result = svc.get_iv(_c(500), fallback_iv=0.22)
    assert result.iv == 0.21
    assert result.source == iv_lookup.IV_SOURCE_LIVE
    assert len(calls) == 1
