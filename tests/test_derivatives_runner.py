"""Tests for prometheus.derivatives.runner.

End-to-end: feed a sleeve config + signals, get back directives or
explained skips. Reuses the fake plumbing patterns from
``test_derivatives_selection``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from prometheus.derivatives import (
    iv_lookup,
    liquidity_filter,
    runner,
    sleeves,
)
from prometheus.execution.contract_discovery import OptionChainParams

# ── Fake plumbing (mirrors test_derivatives_selection) ───────────────


class _StubDiscovery:
    def __init__(self, chains: dict[str, list[OptionChainParams]]) -> None:
        self._chains = chains

    def discover_option_chain(
        self, symbol: str, *, sec_type: str = "STK",
        exchange: str | None = None, trading_class: str | None = None,
    ) -> list[OptionChainParams]:
        return self._chains.get(symbol, [])


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
    def __init__(
        self,
        quotes: dict[str, dict[str, float]] | None = None,
        ivs: dict[str, float] | None = None,
    ) -> None:
        self._quotes = quotes or {}
        self._ivs = ivs or {}

    def reqMktData(self, contract, genericTickList="", snapshot=False):
        key = liquidity_filter._contract_key(contract)
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


def _spy_chain(expirations: list[str], strikes: list[float], symbol: str = "SPY"):
    return OptionChainParams(
        exchange="SMART",
        underlying_con_id=12345,
        trading_class=symbol,
        multiplier="100",
        expirations=frozenset(expirations),
        strikes=frozenset(strikes),
    )


def _populate_market(
    symbol: str, expiry: str, strikes: list[float], right: str,
    iv: float = 0.20,
) -> tuple[dict[str, dict[str, float]], dict[str, float]]:
    quotes = {}
    ivs = {}
    for s in strikes:
        key = f"{symbol}:{expiry}:{s}:{right}"
        # Tight market, premium not too small
        mid = 5.0 + abs(500 - s) * 0.05
        quotes[key] = {"bid": mid - 0.05, "ask": mid + 0.05, "last": mid}
        ivs[key] = iv
    return quotes, ivs


def _setup(symbol: str, expiry: str, strikes: list[float], right: str = "P"):
    chain = _spy_chain([expiry], strikes, symbol=symbol)
    quotes, ivs = _populate_market(symbol, expiry, strikes, right)
    discovery = _StubDiscovery({symbol: [chain]})
    ib = _FakeIb(quotes=quotes, ivs=ivs)
    iv_svc = iv_lookup.IvLookupService(ib=ib, snapshot_wait_sec=0.0)
    liq_svc = liquidity_filter.LiquidityFilter(ib=ib, snapshot_wait_sec=0.0)
    return discovery, iv_svc, liq_svc


# ── Tests ────────────────────────────────────────────────────────────


def test_hedge_sleeve_fires_protective_put_directive_when_mhi_low():
    today = date(2026, 5, 24)
    discovery, iv_svc, liq_svc = _setup(
        "SPY", "20260815", [460.0, 470.0, 480.0, 490.0, 500.0],
    )
    cfg = sleeves.default_sleeves()[sleeves.Sleeve.HEDGE]
    result = runner.run_sleeve(
        cfg,
        signals={"mhi": 0.30},
        nav=200_000.0,
        open_contracts_by_template={},
        underlying_price_fn=lambda _u: 500.0,
        discovery=discovery, iv_lookup=iv_svc, liquidity=liq_svc,
        today=today,
    )
    pps = [d for d in result.directives if d.template_name == "hedge.spy_protective_put"]
    assert len(pps) == 1
    d = pps[0]
    assert d.action == "OPEN"
    assert d.underlying == "SPY"
    assert d.right == "P"
    assert d.quantity > 0
    assert d.iv_source == iv_lookup.IV_SOURCE_LIVE
    assert "mhi=0.30" in d.reason


def test_hedge_sleeve_skips_protective_put_when_mhi_above_threshold():
    today = date(2026, 5, 24)
    discovery, iv_svc, liq_svc = _setup(
        "SPY", "20260815", [490.0, 500.0],
    )
    cfg = sleeves.default_sleeves()[sleeves.Sleeve.HEDGE]
    result = runner.run_sleeve(
        cfg,
        signals={"mhi": 0.60},  # above 0.40 → trigger does not fire
        nav=200_000.0,
        open_contracts_by_template={},
        underlying_price_fn=lambda _u: 500.0,
        discovery=discovery, iv_lookup=iv_svc, liquidity=liq_svc,
        today=today,
    )
    # spy_protective_put should not fire and should appear in skips
    pp_directives = [d for d in result.directives if d.template_name == "hedge.spy_protective_put"]
    pp_skips = [s for s in result.skips if s.template_name == "hedge.spy_protective_put"]
    assert pp_directives == []
    assert len(pp_skips) == 1
    assert pp_skips[0].reason == runner.SKIP_TRIGGER


def test_income_sleeve_emits_negative_quantity_short_position():
    today = date(2026, 5, 24)
    # Income template wants 30-45 DTE; 20260703 = 40 days out
    discovery, iv_svc, liq_svc = _setup(
        "SPY", "20260703", [470.0, 480.0, 490.0, 500.0],
    )
    cfg = sleeves.default_sleeves()[sleeves.Sleeve.INCOME]
    result = runner.run_sleeve(
        cfg,
        signals={"vix_level": 20.0},
        nav=200_000.0,
        open_contracts_by_template={},
        underlying_price_fn=lambda _u: 500.0,
        discovery=discovery, iv_lookup=iv_svc, liquidity=liq_svc,
        today=today,
    )
    assert result.fired == 1
    d = result.directives[0]
    assert d.template_name == "income.spy_short_put"
    assert d.quantity < 0   # income is short premium


def test_capacity_exhausted_skips_with_explanation():
    today = date(2026, 5, 24)
    discovery, iv_svc, liq_svc = _setup(
        "SPY", "20260815", [490.0, 500.0],
    )
    cfg = sleeves.default_sleeves()[sleeves.Sleeve.HEDGE]
    result = runner.run_sleeve(
        cfg,
        signals={"mhi": 0.30},
        nav=200_000.0,
        # Already at max_concurrent (1) — sizing should refuse
        open_contracts_by_template={"hedge.spy_protective_put": 1},
        underlying_price_fn=lambda _u: 500.0,
        discovery=discovery, iv_lookup=iv_svc, liquidity=liq_svc,
        today=today,
    )
    pp_directives = [d for d in result.directives if d.template_name == "hedge.spy_protective_put"]
    pp_skips = [s for s in result.skips if s.template_name == "hedge.spy_protective_put"]
    assert pp_directives == []
    assert len(pp_skips) == 1
    assert pp_skips[0].reason == runner.SKIP_SIZING
    assert "capacity_exhausted" in pp_skips[0].detail


def test_no_underlying_price_skips_with_explanation():
    today = date(2026, 5, 24)
    discovery, iv_svc, liq_svc = _setup(
        "SPY", "20260815", [500.0],
    )
    cfg = sleeves.default_sleeves()[sleeves.Sleeve.HEDGE]
    result = runner.run_sleeve(
        cfg,
        signals={"mhi": 0.30},
        nav=200_000.0,
        open_contracts_by_template={},
        underlying_price_fn=lambda _u: 0.0,   # missing price
        discovery=discovery, iv_lookup=iv_svc, liquidity=liq_svc,
        today=today,
    )
    assert result.fired == 0
    assert result.skips[0].reason == runner.SKIP_NO_PRICE


def test_selection_failure_propagates_as_skip():
    today = date(2026, 5, 24)
    # Chain missing entirely → select_contract returns no_chain
    discovery = _StubDiscovery({})
    iv_svc = iv_lookup.IvLookupService(ib=None)
    liq_svc = liquidity_filter.LiquidityFilter(ib=None)
    cfg = sleeves.default_sleeves()[sleeves.Sleeve.HEDGE]
    result = runner.run_sleeve(
        cfg,
        signals={"mhi": 0.30},
        nav=200_000.0,
        open_contracts_by_template={},
        underlying_price_fn=lambda _u: 500.0,
        discovery=discovery, iv_lookup=iv_svc, liquidity=liq_svc,
        today=today,
    )
    assert result.fired == 0
    skip = result.skips[0]
    assert skip.reason == runner.SKIP_SELECTION
    assert "no_chain" in skip.detail


def test_convex_template_targets_sector_named_by_trigger():
    today = date(2026, 5, 24)
    expiry = "20260703"   # 40 DTE — inside convex 30-60 band
    strikes = [80.0, 90.0, 100.0]
    chain = _spy_chain([expiry], strikes, symbol="XLE")
    quotes, ivs = _populate_market("XLE", expiry, strikes, "P", iv=0.35)
    discovery = _StubDiscovery({"XLE": [chain]})
    ib = _FakeIb(quotes=quotes, ivs=ivs)
    iv_svc = iv_lookup.IvLookupService(ib=ib, snapshot_wait_sec=0.0)
    liq_svc = liquidity_filter.LiquidityFilter(ib=ib, snapshot_wait_sec=0.0)

    cfg = sleeves.default_sleeves()[sleeves.Sleeve.CONVEX]
    result = runner.run_sleeve(
        cfg,
        signals={
            "compound_pressure": {"severity": "CRITICAL", "target_sector_etf": "XLE"},
        },
        nav=200_000.0,
        open_contracts_by_template={},
        underlying_price_fn=lambda _u: 90.0,
        discovery=discovery, iv_lookup=iv_svc, liquidity=liq_svc,
        today=today,
    )
    assert result.fired == 1
    d = result.directives[0]
    assert d.underlying == "XLE"
    assert d.right == "P"
    assert d.quantity > 0
    assert d.trigger_metadata["sector_etf"] == "XLE"


def test_directive_carries_full_trace():
    today = date(2026, 5, 24)
    discovery, iv_svc, liq_svc = _setup(
        "SPY", "20260815", [470.0, 480.0, 490.0, 500.0, 510.0],
    )
    cfg = sleeves.default_sleeves()[sleeves.Sleeve.HEDGE]
    result = runner.run_sleeve(
        cfg,
        signals={"mhi": 0.30},
        nav=200_000.0,
        open_contracts_by_template={},
        underlying_price_fn=lambda _u: 500.0,
        discovery=discovery, iv_lookup=iv_svc, liquidity=liq_svc,
        today=today,
    )
    d = result.directives[0]
    # Selection trace records every candidate considered
    assert len(d.selection_trace.candidates) == 5
    # Sizing flags persist for audit
    assert d.sizing.contracts == abs(d.quantity)
    assert d.sizing.skipped is False
    # Limit price was set from the ask (long position lifts the offer)
    assert d.limit_price == d.selection_trace.candidates[0].quote.ask


def test_sleeve_run_result_per_template_counters():
    today = date(2026, 5, 24)
    discovery, iv_svc, liq_svc = _setup("SPY", "20260815", [490.0, 500.0])
    cfg = sleeves.default_sleeves()[sleeves.Sleeve.HEDGE]

    fired = runner.run_sleeve(
        cfg, signals={"mhi": 0.30}, nav=200_000.0,
        open_contracts_by_template={},
        underlying_price_fn=lambda _u: 500.0,
        discovery=discovery, iv_lookup=iv_svc, liquidity=liq_svc,
        today=today,
    )
    # Every template produces exactly one outcome — directive or skip —
    # regardless of how many we have in the sleeve.
    total = fired.fired + fired.skipped
    assert total == len(cfg.templates)
    pp = [d for d in fired.directives if d.template_name == "hedge.spy_protective_put"]
    assert len(pp) == 1

    skipped = runner.run_sleeve(
        cfg, signals={"mhi": 0.99}, nav=200_000.0,
        open_contracts_by_template={},
        underlying_price_fn=lambda _u: 500.0,
        discovery=discovery, iv_lookup=iv_svc, liquidity=liq_svc,
        today=today,
    )
    # With mhi=0.99 (no hedge signals), no templates fire
    pp_directives = [d for d in skipped.directives if d.template_name == "hedge.spy_protective_put"]
    assert pp_directives == []
