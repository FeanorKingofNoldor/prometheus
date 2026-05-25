"""Tests for prometheus.derivatives.liquidity_filter."""

from __future__ import annotations

from dataclasses import dataclass

from prometheus.derivatives import liquidity_filter as lf

# ── Fake IBKR primitives ─────────────────────────────────────────────


@dataclass
class _FakeContract:
    symbol: str
    strike: float
    right: str = "P"
    lastTradeDateOrContractMonth: str = "20260620"


@dataclass
class _FakeTicker:
    bid: float = 0.0
    ask: float = 0.0
    last: float = 0.0


class _FakeIb:
    def __init__(self, quotes_by_key: dict[str, dict[str, float]] | None = None) -> None:
        self._quotes = quotes_by_key or {}
        self.req_calls: list[str] = []
        self.cancel_calls: list[str] = []

    def reqMktData(self, contract, snapshot=False):
        key = lf._contract_key(contract)
        self.req_calls.append(key)
        q = self._quotes.get(key, {})
        return _FakeTicker(
            bid=q.get("bid", 0.0),
            ask=q.get("ask", 0.0),
            last=q.get("last", 0.0),
        )

    def cancelMktData(self, contract):
        self.cancel_calls.append(lf._contract_key(contract))

    def sleep(self, sec: float) -> None:
        return None


def _c(strike: float, symbol: str = "SPY") -> _FakeContract:
    return _FakeContract(symbol=symbol, strike=strike)


# ── Tests ────────────────────────────────────────────────────────────


def test_empty_input_returns_empty_result():
    f = lf.LiquidityFilter(ib=None)
    r = f.filter([])
    assert r.accepted == [] and r.rejected == []


def test_offline_rejects_all_as_no_quote():
    f = lf.LiquidityFilter(ib=None)
    r = f.filter([_c(500), _c(490)])
    assert r.accepted == []
    assert len(r.rejected) == 2
    assert all(rej.reason == lf.REJECT_NO_QUOTE for _, rej in r.rejected)


def test_accepts_tight_market():
    ib = _FakeIb({
        "SPY:20260620:500:P": {"bid": 3.10, "ask": 3.20, "last": 3.15},
    })
    f = lf.LiquidityFilter(ib=ib, snapshot_wait_sec=0.0)
    r = f.filter([_c(500)])
    assert r.accepted_count == 1
    contract, quote = r.accepted[0]
    assert quote.bid == 3.10
    assert quote.ask == 3.20
    assert quote.spread_pct < 0.05


def test_rejects_no_bid():
    ib = _FakeIb({
        "SPY:20260620:500:P": {"bid": 0.0, "ask": 0.05, "last": 0.0},
    })
    f = lf.LiquidityFilter(ib=ib, snapshot_wait_sec=0.0, min_bid=0.05)
    r = f.filter([_c(500)])
    assert r.accepted == []
    assert r.rejected[0][1].reason == lf.REJECT_NO_BID


def test_rejects_wide_spread():
    # Bid 1.00, ask 2.00 → mid 1.50, spread_pct = 1.0/1.5 = 0.667
    ib = _FakeIb({
        "SPY:20260620:500:P": {"bid": 1.00, "ask": 2.00, "last": 1.50},
    })
    f = lf.LiquidityFilter(ib=ib, snapshot_wait_sec=0.0, max_spread_pct=0.30)
    r = f.filter([_c(500)])
    assert r.accepted == []
    assert r.rejected[0][1].reason == lf.REJECT_WIDE_SPREAD


def test_rejects_no_quote_when_all_fields_zero():
    ib = _FakeIb({"SPY:20260620:500:P": {"bid": 0.0, "ask": 0.0, "last": 0.0}})
    f = lf.LiquidityFilter(ib=ib, snapshot_wait_sec=0.0)
    r = f.filter([_c(500)])
    assert r.rejected[0][1].reason == lf.REJECT_NO_QUOTE


def test_negative_quote_fields_treated_as_zero():
    # IBKR uses -1 as a "no quote" sentinel for bid/ask.
    ib = _FakeIb({"SPY:20260620:500:P": {"bid": -1.0, "ask": -1.0, "last": 2.50}})
    f = lf.LiquidityFilter(ib=ib, snapshot_wait_sec=0.0, min_bid=0.05)
    r = f.filter([_c(500)])
    # -1 coerced to 0 → fails min_bid check
    assert r.rejected[0][1].reason == lf.REJECT_NO_BID


def test_mixed_chain_returns_correct_split():
    ib = _FakeIb({
        "SPY:20260620:500:P": {"bid": 3.00, "ask": 3.10, "last": 3.05},   # good
        "SPY:20260620:480:P": {"bid": 0.0,  "ask": 0.10, "last": 0.05},   # no bid
        "SPY:20260620:460:P": {"bid": 0.50, "ask": 2.50, "last": 1.50},   # wide
        "SPY:20260620:440:P": {"bid": 0.0,  "ask": 0.0,  "last": 0.0},    # no quote
    })
    f = lf.LiquidityFilter(ib=ib, snapshot_wait_sec=0.0)
    r = f.filter([_c(500), _c(480), _c(460), _c(440)])

    assert r.accepted_count == 1
    accepted_strikes = {c.strike for c, _ in r.accepted}
    assert accepted_strikes == {500.0}

    reasons = r.reasons()
    assert reasons[lf.REJECT_NO_BID] == 1
    assert reasons[lf.REJECT_WIDE_SPREAD] == 1
    assert reasons[lf.REJECT_NO_QUOTE] == 1


def test_cancel_called_for_every_request():
    ib = _FakeIb({
        "SPY:20260620:500:P": {"bid": 3.0, "ask": 3.1, "last": 3.05},
        "SPY:20260620:490:P": {"bid": 0.0, "ask": 0.0, "last": 0.0},
    })
    f = lf.LiquidityFilter(ib=ib, snapshot_wait_sec=0.0)
    f.filter([_c(500), _c(490)])
    assert sorted(ib.cancel_calls) == sorted(["SPY:20260620:500:P", "SPY:20260620:490:P"])


def test_legacy_reqmktdata_signature_supported():
    """Older ib_async rejects the snapshot kwarg — helper retries."""

    class _PickyIb(_FakeIb):
        def reqMktData(self, contract, snapshot=False):
            if snapshot:
                raise TypeError("unexpected keyword 'snapshot'")
            return _FakeTicker(bid=3.0, ask=3.1, last=3.05)

    f = lf.LiquidityFilter(ib=_PickyIb(), snapshot_wait_sec=0.0)
    r = f.filter([_c(500)])
    assert r.accepted_count == 1


def test_threshold_tunable_per_filter_instance():
    # Spread = 0.20 / 1.00 = 20% — accepted at 25% threshold, rejected at 10%.
    ib = _FakeIb({"SPY:20260620:500:P": {"bid": 0.90, "ask": 1.10, "last": 1.0}})

    f_lax = lf.LiquidityFilter(ib=ib, snapshot_wait_sec=0.0, max_spread_pct=0.25)
    r_lax = f_lax.filter([_c(500)])
    assert r_lax.accepted_count == 1

    ib2 = _FakeIb({"SPY:20260620:500:P": {"bid": 0.90, "ask": 1.10, "last": 1.0}})
    f_strict = lf.LiquidityFilter(ib=ib2, snapshot_wait_sec=0.0, max_spread_pct=0.10)
    r_strict = f_strict.filter([_c(500)])
    assert r_strict.accepted == []
    assert r_strict.rejected[0][1].reason == lf.REJECT_WIDE_SPREAD

