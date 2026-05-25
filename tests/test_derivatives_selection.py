"""Tests for prometheus.derivatives.selection.

Uses a stub ``ContractDiscoveryService`` (so we don't need a real IBKR
connection) and the real ``IvLookupService`` / ``LiquidityFilter`` with
fake ``ib`` objects from the existing test infrastructure.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date
from typing import Any

from prometheus.derivatives import iv_lookup, liquidity_filter, selection
from prometheus.execution.contract_discovery import OptionChainParams

# ── Fake plumbing ────────────────────────────────────────────────────


class _StubDiscovery:
    """Replacement for ContractDiscoveryService that returns canned chains."""

    def __init__(self, chains: dict[str, list[OptionChainParams]]) -> None:
        self._chains = chains
        self.calls: list[str] = []

    def discover_option_chain(
        self, symbol: str, *, sec_type: str = "STK",
        exchange: str | None = None, trading_class: str | None = None,
    ) -> list[OptionChainParams]:
        self.calls.append(symbol)
        return self._chains.get(symbol, [])


@dataclass
class _FakeContract:
    symbol: str
    strike: float
    right: str = "P"
    lastTradeDateOrContractMonth: str = ""
    tradingClass: str = ""
    exchange: str = "SMART"


@dataclass
class _FakeTicker:
    bid: float = 0.0
    ask: float = 0.0
    last: float = 0.0
    modelGreeks: Any = None


@dataclass
class _FakeModelGreeks:
    impliedVol: float
    undPrice: float = 0.0


class _FakeIb:
    """Combined IV + liquidity fake.

    Quotes are keyed by ``symbol:expiry:strike:right`` so the same
    instance can satisfy both ``LiquidityFilter`` (bid/ask/last) and
    ``IvLookupService`` (modelGreeks.impliedVol).
    """

    def __init__(
        self,
        quotes: dict[str, dict[str, float]] | None = None,
        ivs: dict[str, float] | None = None,
    ) -> None:
        self._quotes = quotes or {}
        self._ivs = ivs or {}
        self.req_calls: list[tuple[str, str]] = []  # (key, generic_tick)

    def reqMktData(self, contract, genericTickList="", snapshot=False):
        key = liquidity_filter._contract_key(contract)
        self.req_calls.append((key, genericTickList))
        q = self._quotes.get(key, {})
        iv = self._ivs.get(key, 0.0)
        return _FakeTicker(
            bid=q.get("bid", 0.0),
            ask=q.get("ask", 0.0),
            last=q.get("last", 0.0),
            modelGreeks=(
                _FakeModelGreeks(impliedVol=iv, undPrice=500.0) if iv > 0 else None
            ),
        )

    def cancelMktData(self, contract):
        pass

    def sleep(self, _sec: float) -> None:
        pass

    def qualifyContracts(self, *contracts):
        return list(contracts)


def _spy_chain(expirations: list[str], strikes: list[float]) -> OptionChainParams:
    return OptionChainParams(
        exchange="SMART",
        underlying_con_id=12345,
        trading_class="SPY",
        multiplier="100",
        expirations=frozenset(expirations),
        strikes=frozenset(strikes),
    )


def _quotes_for_strikes(
    expiry: str, strikes: list[float], right: str, *,
    base_premium: float = 5.0, premium_step: float = 0.05,
    symbol: str = "SPY",
) -> dict[str, dict[str, float]]:
    """Build a uniform quote dict — all strikes have tight markets."""
    out: dict[str, dict[str, float]] = {}
    for s in strikes:
        # Cheap OTM puts: premium scales with distance from spot — but
        # for tests we just need values that pass liquidity filter.
        mid = base_premium + abs(500 - s) * premium_step
        out[f"{symbol}:{expiry}:{s}:{right}"] = {
            "bid": mid - 0.05, "ask": mid + 0.05, "last": mid,
        }
    return out


def _ivs_for_strikes(
    expiry: str, strikes: list[float], right: str, *, iv: float = 0.25,
    symbol: str = "SPY",
) -> dict[str, float]:
    return {f"{symbol}:{expiry}:{s}:{right}": iv for s in strikes}


# ── Tests ────────────────────────────────────────────────────────────


def test_picks_strike_closest_to_target_delta():
    today = date(2026, 5, 24)
    expiry = "20260620"        # 27 DTE
    strikes = [460.0, 470.0, 480.0, 490.0, 500.0, 510.0]

    discovery = _StubDiscovery({"SPY": [_spy_chain([expiry], strikes)]})
    ib = _FakeIb(
        quotes=_quotes_for_strikes(expiry, strikes, "P"),
        ivs=_ivs_for_strikes(expiry, strikes, "P", iv=0.20),
    )
    iv_svc = iv_lookup.IvLookupService(ib=ib, snapshot_wait_sec=0.0)
    liq_svc = liquidity_filter.LiquidityFilter(ib=ib, snapshot_wait_sec=0.0)

    target = selection.TargetSpec(
        underlying="SPY", right="P", target_delta=0.25,
        min_dte=14, max_dte=60,
    )

    result = selection.select_contract(
        target=target, underlying_price=500.0,
        discovery=discovery, iv_lookup=iv_svc, liquidity=liq_svc,
        fallback_iv=0.30, today=today,
    )

    assert not result.skipped
    assert result.underlying == "SPY"
    assert result.expiry == expiry
    # The 0.25-delta put on SPY @ 500 with 20% IV, 27 DTE, is ~3-5% OTM
    assert 470 <= result.strike <= 495
    assert result.iv == 0.20
    assert result.iv_source == iv_lookup.IV_SOURCE_LIVE


def test_skips_when_no_chain_available():
    discovery = _StubDiscovery({})  # SPY chain missing
    target = selection.TargetSpec(
        underlying="SPY", right="P", target_delta=0.25,
        min_dte=14, max_dte=60,
    )
    result = selection.select_contract(
        target=target, underlying_price=500.0,
        discovery=discovery,
        iv_lookup=iv_lookup.IvLookupService(ib=None),
        liquidity=liquidity_filter.LiquidityFilter(ib=None),
        fallback_iv=0.22, today=date(2026, 5, 24),
    )
    assert result.skipped
    assert result.skipped_reason == "no_chain"


def test_skips_when_no_expiration_in_dte_band():
    today = date(2026, 5, 24)
    # All expirations far out of band
    discovery = _StubDiscovery({"SPY": [_spy_chain(["20271219"], [500.0])]})
    target = selection.TargetSpec(
        underlying="SPY", right="P", target_delta=0.25,
        min_dte=14, max_dte=60,
    )
    result = selection.select_contract(
        target=target, underlying_price=500.0,
        discovery=discovery,
        iv_lookup=iv_lookup.IvLookupService(ib=None),
        liquidity=liquidity_filter.LiquidityFilter(ib=None),
        fallback_iv=0.22, today=today,
    )
    assert result.skipped
    assert result.skipped_reason == "no_expiration_in_dte_band"


def test_skips_when_all_strikes_rejected_by_liquidity():
    today = date(2026, 5, 24)
    expiry = "20260620"
    strikes = [490.0, 500.0]
    discovery = _StubDiscovery({"SPY": [_spy_chain([expiry], strikes)]})
    # All bids zero → all rejected as no_bid
    ib = _FakeIb(
        quotes={
            f"SPY:{expiry}:490.0:P": {"bid": 0, "ask": 0.05, "last": 0},
            f"SPY:{expiry}:500.0:P": {"bid": 0, "ask": 0.05, "last": 0},
        },
        ivs={},
    )
    iv_svc = iv_lookup.IvLookupService(ib=ib, snapshot_wait_sec=0.0)
    liq_svc = liquidity_filter.LiquidityFilter(ib=ib, snapshot_wait_sec=0.0)

    target = selection.TargetSpec(
        underlying="SPY", right="P", target_delta=0.25,
        min_dte=14, max_dte=60,
    )
    result = selection.select_contract(
        target=target, underlying_price=500.0,
        discovery=discovery, iv_lookup=iv_svc, liquidity=liq_svc,
        fallback_iv=0.22, today=today,
    )
    assert result.skipped
    assert result.skipped_reason == "no_liquid_strikes"
    assert result.trace.liquidity_rejections.get(liquidity_filter.REJECT_NO_BID) == 2


def test_trace_records_all_candidates_and_chosen_index():
    today = date(2026, 5, 24)
    expiry = "20260620"
    strikes = [480.0, 490.0, 500.0, 510.0]
    discovery = _StubDiscovery({"SPY": [_spy_chain([expiry], strikes)]})
    ib = _FakeIb(
        quotes=_quotes_for_strikes(expiry, strikes, "P"),
        ivs=_ivs_for_strikes(expiry, strikes, "P", iv=0.20),
    )
    iv_svc = iv_lookup.IvLookupService(ib=ib, snapshot_wait_sec=0.0)
    liq_svc = liquidity_filter.LiquidityFilter(ib=ib, snapshot_wait_sec=0.0)

    target = selection.TargetSpec(
        underlying="SPY", right="P", target_delta=0.25,
        min_dte=14, max_dte=60,
    )
    result = selection.select_contract(
        target=target, underlying_price=500.0,
        discovery=discovery, iv_lookup=iv_svc, liquidity=liq_svc,
        fallback_iv=0.22, today=today,
    )

    assert len(result.trace.candidates) == 4
    assert result.trace.chosen_index == 0
    # The first candidate in the sorted trace is the chosen one
    assert result.trace.candidates[0].strike == result.strike
    # Diffs should be non-decreasing
    diffs = [c.delta_diff for c in result.trace.candidates]
    assert diffs == sorted(diffs)


def test_iv_source_records_per_strike_when_some_fall_back():
    today = date(2026, 5, 24)
    expiry = "20260620"
    strikes = [490.0, 500.0]
    discovery = _StubDiscovery({"SPY": [_spy_chain([expiry], strikes)]})
    # IV available for 490 only; 500 will fall back
    ib = _FakeIb(
        quotes=_quotes_for_strikes(expiry, strikes, "P"),
        ivs={f"SPY:{expiry}:490.0:P": 0.20},
    )
    iv_svc = iv_lookup.IvLookupService(ib=ib, snapshot_wait_sec=0.0)
    liq_svc = liquidity_filter.LiquidityFilter(ib=ib, snapshot_wait_sec=0.0)

    target = selection.TargetSpec(
        underlying="SPY", right="P", target_delta=0.25,
        min_dte=14, max_dte=60,
    )
    result = selection.select_contract(
        target=target, underlying_price=500.0,
        discovery=discovery, iv_lookup=iv_svc, liquidity=liq_svc,
        fallback_iv=0.30, today=today,
    )

    # Trace should record live for 490 and fallback for 500
    sources = {c.strike: c.iv_source for c in result.trace.candidates}
    assert sources[490.0] == iv_lookup.IV_SOURCE_LIVE
    assert sources[500.0] == iv_lookup.IV_SOURCE_FALLBACK


def test_prefers_third_friday_monthly_when_available():
    today = date(2026, 5, 24)
    # 20260605 (1st Fri, weekly), 20260620 (3rd Sat — Fri is 19), 20260619 (3rd Fri, monthly)
    expirations = ["20260605", "20260612", "20260619", "20260626"]
    strikes = [500.0]
    discovery = _StubDiscovery({"SPY": [_spy_chain(expirations, strikes)]})
    ib = _FakeIb(
        quotes=_quotes_for_strikes("20260619", strikes, "P"),
        ivs=_ivs_for_strikes("20260619", strikes, "P"),
    )
    iv_svc = iv_lookup.IvLookupService(ib=ib, snapshot_wait_sec=0.0)
    liq_svc = liquidity_filter.LiquidityFilter(ib=ib, snapshot_wait_sec=0.0)

    target = selection.TargetSpec(
        underlying="SPY", right="P", target_delta=0.50,
        min_dte=7, max_dte=60,
    )
    result = selection.select_contract(
        target=target, underlying_price=500.0,
        discovery=discovery, iv_lookup=iv_svc, liquidity=liq_svc,
        fallback_iv=0.22, today=today,
    )
    assert result.expiry == "20260619"


def test_invalid_underlying_price_skips():
    discovery = _StubDiscovery({"SPY": [_spy_chain(["20260620"], [500.0])]})
    target = selection.TargetSpec(
        underlying="SPY", right="P", target_delta=0.25,
        min_dte=14, max_dte=60,
    )
    result = selection.select_contract(
        target=target, underlying_price=0.0,
        discovery=discovery,
        iv_lookup=iv_lookup.IvLookupService(ib=None),
        liquidity=liquidity_filter.LiquidityFilter(ib=None),
        fallback_iv=0.22,
    )
    assert result.skipped
    assert result.skipped_reason == "invalid_underlying_price"


def test_call_target_delta_picks_positive_delta_strike():
    today = date(2026, 5, 24)
    expiry = "20260620"
    strikes = [500.0, 510.0, 520.0, 530.0]
    discovery = _StubDiscovery({"SPY": [_spy_chain([expiry], strikes)]})
    ib = _FakeIb(
        quotes=_quotes_for_strikes(expiry, strikes, "C"),
        ivs=_ivs_for_strikes(expiry, strikes, "C", iv=0.20),
    )
    iv_svc = iv_lookup.IvLookupService(ib=ib, snapshot_wait_sec=0.0)
    liq_svc = liquidity_filter.LiquidityFilter(ib=ib, snapshot_wait_sec=0.0)

    target = selection.TargetSpec(
        underlying="SPY", right="C", target_delta=0.30,
        min_dte=14, max_dte=60,
    )
    result = selection.select_contract(
        target=target, underlying_price=500.0,
        discovery=discovery, iv_lookup=iv_svc, liquidity=liq_svc,
        fallback_iv=0.22, today=today,
    )
    assert not result.skipped
    assert result.delta > 0
    assert result.right == "C"


def test_premium_per_contract_is_mid_times_100():
    today = date(2026, 5, 24)
    expiry = "20260620"
    strikes = [500.0]
    discovery = _StubDiscovery({"SPY": [_spy_chain([expiry], strikes)]})
    ib = _FakeIb(
        quotes={f"SPY:{expiry}:500.0:P": {"bid": 4.90, "ask": 5.10, "last": 5.0}},
        ivs={f"SPY:{expiry}:500.0:P": 0.20},
    )
    iv_svc = iv_lookup.IvLookupService(ib=ib, snapshot_wait_sec=0.0)
    liq_svc = liquidity_filter.LiquidityFilter(ib=ib, snapshot_wait_sec=0.0)

    target = selection.TargetSpec(
        underlying="SPY", right="P", target_delta=0.50,
        min_dte=14, max_dte=60,
    )
    result = selection.select_contract(
        target=target, underlying_price=500.0,
        discovery=discovery, iv_lookup=iv_svc, liquidity=liq_svc,
        fallback_iv=0.22, today=today,
    )
    assert result.estimated_premium_per_share == 5.0
    assert result.estimated_premium_per_contract == 500.0


# Use `time` import to keep linter quiet — selection uses it implicitly
# via IvLookupService.
_ = time
