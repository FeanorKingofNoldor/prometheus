"""Tests for prometheus.derivatives.shadow."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date
from typing import Any

from prometheus.derivatives import (
    iv_lookup,
    liquidity_filter,
    runner,
    shadow,
    sleeves,
)
from prometheus.derivatives.selection import SelectionTrace
from prometheus.derivatives.sizing import SizingResult
from prometheus.execution.contract_discovery import OptionChainParams

# ── Fake DB ──────────────────────────────────────────────────────────


class _FakeCursor:
    def __init__(self, db: "_FakeDb") -> None:
        self._db = db

    def execute(self, sql: str, args: Any = ()) -> None:
        self._db.queries.append((sql, args))
        if "INSERT INTO derivatives_shadow_decisions" in sql:
            cols = [
                "run_id", "as_of_date", "sleeve", "template_name", "kind",
                "nav", "vix_level", "mhi",
            ]
            base = dict(zip(cols, args[:8]))
            # Distinguish directive vs skip by argument count
            if len(args) == 27:
                base.update({
                    "underlying": args[8], "right": args[9],
                    "expiry": args[10], "strike": args[11],
                    "quantity": args[12], "limit_price": args[13],
                    "iv_used": args[14], "iv_source": args[15],
                    "delta": args[16], "premium": args[17],
                    "sizing_contracts": args[18],
                    "sizing_capacity_bound": args[19],
                    "sizing_budget_bound": args[20],
                    "trigger_reason": args[21],
                    "reason": args[24],
                })
            else:
                base.update({
                    "trigger_reason": args[8],
                    "reason": args[11],
                    "skip_reason": args[12],
                    "skip_detail": args[13],
                })
            self._db.rows.append(base)

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *_exc: Any) -> None:
        return None


class _FakeConnection:
    def __init__(self, db: "_FakeDb") -> None:
        self._db = db

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self._db)

    def commit(self) -> None:
        return None

    def __enter__(self) -> "_FakeConnection":
        return self

    def __exit__(self, *_exc: Any) -> None:
        return None


class _FakeDb:
    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []
        self.queries: list[tuple] = []

    @contextmanager
    def get_runtime_connection(self):
        yield _FakeConnection(self)


# ── Fake IBKR plumbing (mirrors runner tests) ────────────────────────


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


def _chain(expirations, strikes, symbol="SPY"):
    return OptionChainParams(
        exchange="SMART", underlying_con_id=12345,
        trading_class=symbol, multiplier="100",
        expirations=frozenset(expirations), strikes=frozenset(strikes),
    )


def _market(symbol, expiry, strikes, right, iv=0.20):
    quotes = {}
    ivs = {}
    for s in strikes:
        key = f"{symbol}:{expiry}:{s}:{right}"
        mid = 5.0 + abs(500 - s) * 0.05
        quotes[key] = {"bid": mid - 0.05, "ask": mid + 0.05, "last": mid}
        ivs[key] = iv
    return quotes, ivs


# ── Tests ────────────────────────────────────────────────────────────


def test_record_shadow_result_writes_directive_and_skip_rows():
    db = _FakeDb()

    # Hand-build one directive and one skip
    directive = runner.SleeveDirective(
        sleeve=sleeves.Sleeve.HEDGE,
        template_name="hedge.spy_protective_put",
        action="OPEN", underlying="SPY", right="P",
        expiry="20260815", strike=480.0, quantity=3,
        limit_price=4.20, iv_used=0.20,
        iv_source=iv_lookup.IV_SOURCE_LIVE, delta=-0.27,
        estimated_premium_per_contract=420.0,
        trigger_reason="mhi=0.30 below threshold 0.40",
        trigger_metadata={"mhi": 0.30},
        selection_trace=SelectionTrace(
            underlying="SPY", underlying_price=500.0, expiry="20260815",
            chain_strikes_total=20, chain_strikes_in_window=8,
            liquidity_rejections={}, candidates=[], chosen_index=0,
        ),
        sizing=SizingResult(contracts=3, capacity_bound=True,
                            budget_bound=False, skipped_reason=None),
        reason="hedge.spy_protective_put: mhi=0.30 below threshold 0.40",
    )
    skip = runner.SleeveSkip(
        sleeve=sleeves.Sleeve.INCOME,
        template_name="income.spy_short_put",
        reason=runner.SKIP_TRIGGER,
        detail="vix=12.0 outside 15-30 band",
    )

    result = runner.SleeveRunResult(
        sleeve=sleeves.Sleeve.HEDGE,
        directives=[directive], skips=[skip],
    )

    rows = shadow.record_shadow_result(
        db, run_id="run-2026-05-24", as_of_date=date(2026, 5, 24),
        nav=200_000.0,
        signals={"vix_level": 14.0, "mhi": 0.30},
        sleeve_results=[result],
    )
    assert rows == 2
    assert len(db.rows) == 2

    by_kind = {r["kind"]: r for r in db.rows}
    assert by_kind["DIRECTIVE"]["underlying"] == "SPY"
    assert by_kind["DIRECTIVE"]["strike"] == 480.0
    assert by_kind["DIRECTIVE"]["quantity"] == 3
    assert by_kind["DIRECTIVE"]["nav"] == 200_000.0
    assert by_kind["DIRECTIVE"]["vix_level"] == 14.0
    assert by_kind["DIRECTIVE"]["mhi"] == 0.30

    assert by_kind["SKIP"]["template_name"] == "income.spy_short_put"
    assert by_kind["SKIP"]["skip_reason"] == runner.SKIP_TRIGGER
    assert "vix=12.0" in by_kind["SKIP"]["skip_detail"]


def test_record_shadow_handles_signals_with_missing_keys():
    db = _FakeDb()
    skip = runner.SleeveSkip(
        sleeve=sleeves.Sleeve.HEDGE,
        template_name="hedge.spy_protective_put",
        reason=runner.SKIP_TRIGGER, detail="…",
    )
    result = runner.SleeveRunResult(
        sleeve=sleeves.Sleeve.HEDGE, directives=[], skips=[skip],
    )
    rows = shadow.record_shadow_result(
        db, run_id="r1", as_of_date=date(2026, 5, 24),
        nav=200_000.0, signals={},  # no vix, no mhi
        sleeve_results=[result],
    )
    assert rows == 1


def test_run_shadow_pass_walks_all_sleeves_and_persists():
    today = date(2026, 5, 24)
    # Build market data for SPY (hedge + income templates)
    spy_expiry_hedge = "20260815"  # 83 DTE — hedge 45-90
    spy_expiry_income = "20260703"  # 40 DTE — income 30-45
    strikes = [470.0, 480.0, 490.0, 500.0, 510.0]
    chain = OptionChainParams(
        exchange="SMART", underlying_con_id=12345,
        trading_class="SPY", multiplier="100",
        expirations=frozenset([spy_expiry_hedge, spy_expiry_income]),
        strikes=frozenset(strikes),
    )
    quotes_h, ivs_h = _market("SPY", spy_expiry_hedge, strikes, "P")
    quotes_i, ivs_i = _market("SPY", spy_expiry_income, strikes, "P")
    discovery = _StubDiscovery({"SPY": [chain]})
    ib = _FakeIb(
        quotes={**quotes_h, **quotes_i},
        ivs={**ivs_h, **ivs_i},
    )
    iv_svc = iv_lookup.IvLookupService(ib=ib, snapshot_wait_sec=0.0)
    liq_svc = liquidity_filter.LiquidityFilter(ib=ib, snapshot_wait_sec=0.0)

    db = _FakeDb()
    results, rows = shadow.run_shadow_pass(
        db_manager=db,
        run_id="run-001",
        as_of_date=today,
        nav=200_000.0,
        signals={"mhi": 0.30, "vix_level": 20.0},   # both hedge and income fire
        open_contracts_by_template={},
        underlying_price_fn=lambda _u: 500.0,
        discovery=discovery, iv_lookup=iv_svc, liquidity=liq_svc,
    )

    # Three sleeves; HEDGE + INCOME fire (1 directive each); CONVEX skips
    # because no compound_pressure signal was supplied.
    sleeves_with_directives = [r.sleeve for r in results if r.fired]
    assert sleeves.Sleeve.HEDGE in sleeves_with_directives
    assert sleeves.Sleeve.INCOME in sleeves_with_directives
    assert sleeves.Sleeve.CONVEX not in sleeves_with_directives
    assert rows == sum(len(r.directives) + len(r.skips) for r in results)


def test_run_shadow_pass_uses_default_sleeves_when_unspecified():
    db = _FakeDb()
    results, _ = shadow.run_shadow_pass(
        db_manager=db, run_id="r", as_of_date=date(2026, 5, 24),
        nav=200_000.0, signals={},  # no triggers fire
        open_contracts_by_template={},
        underlying_price_fn=lambda _u: 500.0,
        discovery=_StubDiscovery({}),
        iv_lookup=iv_lookup.IvLookupService(ib=None),
        liquidity=liquidity_filter.LiquidityFilter(ib=None),
    )
    sleeves_seen = {r.sleeve for r in results}
    assert sleeves_seen == set(sleeves.default_sleeves().keys())


def test_commodity_sleeve_walked_by_shadow_pass():
    """The COMMODITY sleeve must be enumerated by the shadow pass and
    produce per-template rows (skips when no intel) — not be silently
    omitted from the fixed sleeve list."""
    db = _FakeDb()
    results, _ = shadow.run_shadow_pass(
        db_manager=db, run_id="r-comm", as_of_date=date(2026, 5, 24),
        nav=200_000.0, signals={},  # no intel → COMMODITY templates skip
        open_contracts_by_template={},
        underlying_price_fn=lambda _u: 75.0,
        discovery=_StubDiscovery({}),
        iv_lookup=iv_lookup.IvLookupService(ib=None),
        liquidity=liquidity_filter.LiquidityFilter(ib=None),
    )
    comm = [r for r in results if r.sleeve is sleeves.Sleeve.COMMODITY]
    assert comm, "COMMODITY sleeve missing from shadow pass results"
    comm_result = comm[0]
    # All four templates evaluated → one skip row each (no intel signals).
    comm_cfg = sleeves.default_sleeves()[sleeves.Sleeve.COMMODITY]
    assert comm_result.skipped == len(comm_cfg.templates) == 4
    skipped_names = {s.template_name for s in comm_result.skips}
    assert skipped_names == {t.name for t in comm_cfg.templates}
    # And the COMMODITY skip rows were persisted to the shadow table.
    comm_rows = [
        row for row in db.rows
        if row["sleeve"] == sleeves.Sleeve.COMMODITY.value
    ]
    assert len(comm_rows) == 4


def test_commodity_templates_in_leg_count_and_open_contracts_maps():
    """Re-fire bug fix: every COMMODITY template must be in the
    leg-count map (single-leg → 1) and recognised by the
    open-contracts lookup even though it has no legacy strategy."""
    from prometheus.scripts.run.run_derivatives_daily import (
        _LEGACY_STRATEGY_TO_TEMPLATE,
        _TEMPLATE_LEG_COUNT,
        _open_contracts_by_template,
    )

    comm_cfg = sleeves.default_sleeves()[sleeves.Sleeve.COMMODITY]
    comm_names = [t.name for t in comm_cfg.templates]

    # (1) all present in the leg-count map, each single-leg.
    for name in comm_names:
        assert _TEMPLATE_LEG_COUNT.get(name) == 1, name

    # (2) COMMODITY has no legacy-strategy correspondence (shadow-only).
    assert not (set(comm_names) & set(_LEGACY_STRATEGY_TO_TEMPLATE.values()))

    # (3) a position tagged with the template name itself registers as
    #     "already open" so the template won't re-fire daily.
    target = comm_names[0]
    open_counts = _open_contracts_by_template(
        [{"strategy": target}, {"strategy": target}],
    )
    assert open_counts.get(target) == 2


# Suppress lint warnings for imports used only via fixture data
_ = field
