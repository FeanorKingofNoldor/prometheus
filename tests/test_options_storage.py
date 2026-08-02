"""Tests for prometheus.execution.options_storage.

Uses an in-memory fake DB manager that captures SQL statements and
holds rows in dicts, so the storage helpers can be exercised without a
real Postgres connection.
"""

from __future__ import annotations

import re
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from typing import Any

from prometheus.execution import options_storage

# ── Fake DB infrastructure ───────────────────────────────────────────


class _FakeCursor:
    def __init__(self, db: "_FakeDb") -> None:
        self._db = db
        self._result: list[tuple] = []
        self.rowcount: int = 0

    def execute(self, sql: str, args: Any = ()) -> None:
        self._db.queries.append((sql, args))
        sql_norm = " ".join(sql.split()).upper()

        if sql_norm.startswith("SELECT POSITION_ID FROM OPTIONS_POSITIONS"):
            iid, pid, mode = args
            match = next(
                (p for p in self._db.positions.values()
                 if p["instrument_id"] == iid and p["portfolio_id"] == pid and p["mode"] == mode),
                None,
            )
            self._result = [(match["position_id"],)] if match else []
            self.rowcount = len(self._result)
            return

        if sql_norm.startswith("INSERT INTO OPTIONS_POSITIONS"):
            row = {
                "position_id": args[0], "instrument_id": args[1],
                "portfolio_id": args[2], "mode": args[3],
                "sleeve": args[4], "template": args[5], "strategy": args[6],
                "symbol": args[7], "right": args[8], "expiry": args[9],
                "strike": args[10], "multiplier": args[11], "sec_type": args[12],
                "quantity": args[13], "avg_cost": args[14], "opened_at": args[15],
                "opened_decision_id": args[16],
                "delta": args[17], "gamma": args[18], "theta": args[19],
                "vega": args[20], "implied_vol": args[21], "underlying_price": args[22],
                "greeks_updated_at": args[23], "metadata_json": args[24],
                "market_price": None, "market_value": None, "unrealized_pnl": None,
            }
            self._db.positions[args[0]] = row
            self.rowcount = 1
            return

        if sql_norm.startswith("INSERT INTO OPTIONS_POSITION_EVENTS"):
            self._db.events.append({
                "position_id": args[0], "portfolio_id": args[1],
                "mode": args[2], "event_type": args[3],
                "event_at": args[4], "as_of_date": args[5],
                "instrument_id": args[6], "symbol": args[7],
                "right": args[8], "expiry": args[9],
                "strike": args[10], "multiplier": args[11],
                "quantity_delta": args[12], "price": args[13],
                "realized_pnl": args[14],
                "sleeve": args[15], "template": args[16], "strategy": args[17],
                "decision_id": args[18], "order_id": args[19], "fill_id": args[20],
                "greeks_json": args[21], "metadata_json": args[22],
            })
            self.rowcount = 1
            return

        if sql_norm.startswith("SELECT INSTRUMENT_ID, PORTFOLIO_ID, MODE, SYMBOL"):
            pid = args[0]
            row = self._db.positions.get(pid)
            if row is None:
                self._result = []
            elif "QUANTITY" in sql_norm:
                self._result = [(
                    row["instrument_id"], row["portfolio_id"], row["mode"],
                    row["symbol"], row["right"], row["expiry"],
                    row["strike"], row["multiplier"],
                    row["quantity"], row["sleeve"], row["template"], row["strategy"],
                )]
            else:
                self._result = [(
                    row["instrument_id"], row["portfolio_id"], row["mode"],
                    row["symbol"], row["right"], row["expiry"],
                    row["strike"], row["multiplier"],
                    row["sleeve"], row["template"], row["strategy"],
                )]
            self.rowcount = len(self._result)
            return

        if sql_norm.startswith("DELETE FROM OPTIONS_POSITIONS"):
            pid = args[0]
            self._db.positions.pop(pid, None)
            self.rowcount = 1
            return

        if sql_norm.startswith("UPDATE OPTIONS_POSITIONS SET"):
            pid = args[-1]
            row = self._db.positions.get(pid)
            if row is None:
                self.rowcount = 0
                return
            # Parse "col = %s, col = %s" portion to learn what to update.
            set_clause = re.search(r"SET (.+?) WHERE", sql, re.IGNORECASE | re.DOTALL).group(1)
            cols = [c.split("=")[0].strip() for c in set_clause.split(",")]
            for col, val in zip(cols, args[:-1]):
                row[col] = val
            self.rowcount = 1
            return

        if sql_norm.startswith("SELECT INSTRUMENT_ID, STRATEGY FROM OPTIONS_POSITION_EVENTS"):
            pid, mode, event_type, since = args
            rows = [
                e for e in self._db.events
                if e["portfolio_id"] == pid and e["mode"] == mode
                and e["event_type"] == event_type
                and e["as_of_date"] >= since
            ]
            rows.sort(key=lambda e: e["event_at"])
            self._result = [(e["instrument_id"], e["strategy"]) for e in rows]
            self.rowcount = len(self._result)
            return

        if sql_norm.startswith("SELECT POSITION_ID, INSTRUMENT_ID, PORTFOLIO_ID"):
            pid, mode = args
            rows = [
                r for r in self._db.positions.values()
                if r["portfolio_id"] == pid and r["mode"] == mode
            ]
            rows.sort(key=lambda r: r["opened_at"])
            self._result = [
                (
                    r["position_id"], r["instrument_id"], r["portfolio_id"], r["mode"],
                    r["sleeve"], r["template"], r["strategy"],
                    r["symbol"], r["right"], r["expiry"], r["strike"],
                    r["multiplier"], r["sec_type"],
                    r["quantity"], r["avg_cost"], r["opened_at"],
                    r["market_price"], r["market_value"], r["unrealized_pnl"],
                    r["delta"], r["gamma"], r["theta"], r["vega"],
                    r["implied_vol"], r["underlying_price"],
                )
                for r in rows
            ]
            self.rowcount = len(self._result)
            return

        raise AssertionError(f"Unhandled SQL in fake cursor: {sql_norm[:80]}")

    def fetchone(self) -> tuple | None:
        return self._result[0] if self._result else None

    def fetchall(self) -> list[tuple]:
        return list(self._result)

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
        self.positions: dict[str, dict[str, Any]] = {}
        self.events: list[dict[str, Any]] = []
        self.queries: list[tuple] = []

    @contextmanager
    def get_runtime_connection(self):
        yield _FakeConnection(self)


# ── Tests ────────────────────────────────────────────────────────────


def _open_args(**overrides: Any) -> dict[str, Any]:
    args = dict(
        instrument_id="SPY_260620_500P",
        portfolio_id="US_OPTIONS",
        mode="PAPER",
        symbol="SPY",
        right="P",
        expiry="20260620",
        strike=500.0,
        quantity=5,
        avg_cost=3.20,
    )
    args.update(overrides)
    return args


def test_record_position_open_inserts_position_and_event():
    db = _FakeDb()
    pid = options_storage.record_position_open(db, **_open_args())

    assert pid in db.positions
    row = db.positions[pid]
    assert row["symbol"] == "SPY"
    assert row["right"] == "P"
    assert row["quantity"] == 5

    assert len(db.events) == 1
    evt = db.events[0]
    assert evt["event_type"] == options_storage.EVENT_OPEN
    assert evt["position_id"] == pid
    assert evt["quantity_delta"] == 5
    assert evt["price"] == 3.20


def test_record_position_open_is_idempotent_on_dup_key():
    db = _FakeDb()
    pid1 = options_storage.record_position_open(db, **_open_args())
    pid2 = options_storage.record_position_open(db, **_open_args())
    assert pid1 == pid2
    assert len(db.positions) == 1
    # Only one OPEN event written
    assert sum(1 for e in db.events if e["event_type"] == "OPEN") == 1


def test_record_position_open_writes_greeks_snapshot():
    db = _FakeDb()
    greeks = {
        "delta": -0.27, "gamma": 0.012, "theta": -8.50,
        "vega": 18.0, "implied_vol": 0.22, "underlying_price": 510.0,
    }
    pid = options_storage.record_position_open(db, **_open_args(), greeks=greeks)
    row = db.positions[pid]
    assert row["delta"] == -0.27
    assert row["theta"] == -8.50
    assert row["greeks_updated_at"] is not None


def test_record_position_close_partial_updates_quantity():
    db = _FakeDb()
    pid = options_storage.record_position_open(db, **_open_args(quantity=5))

    removed = options_storage.record_position_close(
        db, position_id=pid, quantity_delta=-2, price=4.10,
        realized_pnl=180.0,
    )
    assert removed is False
    assert db.positions[pid]["quantity"] == 3

    close_evt = next(e for e in db.events if e["event_type"] == "CLOSE")
    assert close_evt["quantity_delta"] == -2
    assert close_evt["realized_pnl"] == 180.0


def test_record_position_close_zeroing_removes_row():
    db = _FakeDb()
    pid = options_storage.record_position_open(db, **_open_args(quantity=5))

    removed = options_storage.record_position_close(
        db, position_id=pid, quantity_delta=-5, price=4.10,
        realized_pnl=450.0,
    )
    assert removed is True
    assert pid not in db.positions
    # Event still exists post-removal
    assert any(e["event_type"] == "CLOSE" and e["position_id"] == pid for e in db.events)


def test_record_position_close_unknown_position_logs_and_returns_false():
    db = _FakeDb()
    removed = options_storage.record_position_close(
        db, position_id="does-not-exist", quantity_delta=-1, price=1.0,
    )
    assert removed is False
    assert db.events == []


def test_update_position_mark_writes_price_and_greeks():
    db = _FakeDb()
    pid = options_storage.record_position_open(db, **_open_args())

    options_storage.update_position_mark(
        db, position_id=pid,
        market_price=4.50, market_value=2250.0, unrealized_pnl=650.0,
        greeks={"delta": -0.35, "implied_vol": 0.25},
    )
    row = db.positions[pid]
    assert row["market_price"] == 4.50
    assert row["unrealized_pnl"] == 650.0
    assert row["delta"] == -0.35
    assert row["implied_vol"] == 0.25
    # No MARK event by default
    assert not any(e["event_type"] == "MARK" for e in db.events)


def test_update_position_mark_writes_mark_event_when_requested():
    db = _FakeDb()
    pid = options_storage.record_position_open(db, **_open_args())

    options_storage.update_position_mark(
        db, position_id=pid, market_price=4.50,
        greeks={"delta": -0.35},
        write_mark_event=True,
        as_of_date=date(2026, 5, 24),
    )
    mark_events = [e for e in db.events if e["event_type"] == "MARK"]
    assert len(mark_events) == 1
    assert mark_events[0]["price"] == 4.50
    assert mark_events[0]["as_of_date"] == date(2026, 5, 24)


def test_update_position_mark_no_op_when_no_fields():
    db = _FakeDb()
    pid = options_storage.record_position_open(db, **_open_args())
    qcount_before = len(db.queries)
    options_storage.update_position_mark(db, position_id=pid)
    # The early return must not run UPDATE/SELECT
    assert len(db.queries) == qcount_before


def test_get_open_positions_returns_typed_rows():
    db = _FakeDb()
    pid_a = options_storage.record_position_open(db, **_open_args(
        instrument_id="SPY_260620_500P", strike=500.0, quantity=5,
    ))
    pid_b = options_storage.record_position_open(db, **_open_args(
        instrument_id="SPY_260620_480P", strike=480.0, quantity=3,
    ))
    options_storage.record_position_open(db, **_open_args(
        instrument_id="OTHER", portfolio_id="OTHER_PF", quantity=1,
    ))

    rows = options_storage.get_open_positions(
        db, portfolio_id="US_OPTIONS", mode="PAPER",
    )
    assert {r.position_id for r in rows} == {pid_a, pid_b}
    assert all(isinstance(r, options_storage.OptionPositionRow) for r in rows)
    assert all(r.portfolio_id == "US_OPTIONS" for r in rows)


@dataclass
class _SnapEntry:
    """Stand-in for OptionPositionEntry — duck-typed to what
    reconcile_positions reads."""
    symbol: str
    right: str
    expiry: str
    strike: float
    quantity: int
    multiplier: int = 100
    avg_cost: float = 0.0
    market_price: float | None = None
    market_value: float | None = None
    unrealized_pnl: float | None = None
    strategy: str = ""
    greeks: Any = None


def test_reconcile_inserts_new_positions_with_open_event():
    db = _FakeDb()
    snapshot = {
        "SPY_260620_500P": _SnapEntry(
            symbol="SPY", right="P", expiry="20260620",
            strike=500.0, quantity=5, avg_cost=3.20,
            strategy="protective_put",
        ),
    }
    summary = options_storage.reconcile_positions(
        db, portfolio_id="US_OPTIONS_PAPER", mode="PAPER",
        snapshot=snapshot,
    )
    assert summary.opened == 1
    assert summary.updated == 0
    assert summary.closed == 0
    assert len(db.positions) == 1
    assert any(e["event_type"] == "OPEN" for e in db.events)


def test_reconcile_updates_existing_positions_mark_only_no_event():
    db = _FakeDb()
    snapshot = {
        "SPY_260620_500P": _SnapEntry(
            symbol="SPY", right="P", expiry="20260620",
            strike=500.0, quantity=5, avg_cost=3.20,
            market_price=4.10, market_value=2050.0, unrealized_pnl=450.0,
            strategy="protective_put",
        ),
    }
    options_storage.reconcile_positions(
        db, portfolio_id="US_OPTIONS_PAPER", mode="PAPER",
        snapshot=snapshot,
    )
    open_events_after_first = sum(1 for e in db.events if e["event_type"] == "OPEN")

    snapshot["SPY_260620_500P"].market_price = 4.50
    summary = options_storage.reconcile_positions(
        db, portfolio_id="US_OPTIONS_PAPER", mode="PAPER",
        snapshot=snapshot,
    )
    assert summary.opened == 0
    assert summary.updated == 1
    # No new OPEN event (or any event) from a mark update
    assert sum(1 for e in db.events if e["event_type"] == "OPEN") == open_events_after_first
    assert db.positions[next(iter(db.positions.keys()))]["market_price"] == 4.50


def test_reconcile_logs_close_for_positions_missing_from_snapshot():
    db = _FakeDb()
    # Seed the table with two positions
    options_storage.record_position_open(
        db, instrument_id="SPY_260620_500P",
        portfolio_id="US_OPTIONS_PAPER", mode="PAPER",
        symbol="SPY", right="P", expiry="20260620",
        strike=500.0, quantity=5, avg_cost=3.20,
    )
    options_storage.record_position_open(
        db, instrument_id="SPY_260620_480P",
        portfolio_id="US_OPTIONS_PAPER", mode="PAPER",
        symbol="SPY", right="P", expiry="20260620",
        strike=480.0, quantity=3, avg_cost=2.50,
    )
    assert len(db.positions) == 2

    # Snapshot only contains the 500 put — the 480 must close
    snapshot = {
        "SPY_260620_500P": _SnapEntry(
            symbol="SPY", right="P", expiry="20260620",
            strike=500.0, quantity=5, avg_cost=3.20,
        ),
    }
    summary = options_storage.reconcile_positions(
        db, portfolio_id="US_OPTIONS_PAPER", mode="PAPER",
        snapshot=snapshot,
    )
    assert summary.closed == 1
    assert len(db.positions) == 1
    close_events = [e for e in db.events if e["event_type"] == "CLOSE"]
    assert len(close_events) == 1
    assert "480" in str(close_events[0]["strike"])


def test_reconcile_tags_sleeve_and_template_when_provided():
    db = _FakeDb()
    snapshot = {
        "SPY_260620_480P": _SnapEntry(
            symbol="SPY", right="P", expiry="20260620",
            strike=480.0, quantity=3, avg_cost=4.20,
            strategy="protective_put",
        ),
    }
    options_storage.reconcile_positions(
        db, portfolio_id="US_OPTIONS_PAPER", mode="PAPER",
        snapshot=snapshot,
        sleeve_by_iid={"SPY_260620_480P": "HEDGE"},
        template_by_iid={"SPY_260620_480P": "hedge.spy_protective_put"},
    )
    row = next(iter(db.positions.values()))
    assert row["sleeve"] == "HEDGE"
    assert row["template"] == "hedge.spy_protective_put"


def test_reconcile_with_empty_snapshot_closes_all_existing():
    db = _FakeDb()
    options_storage.record_position_open(
        db, instrument_id="A", portfolio_id="P", mode="PAPER",
        symbol="A", right="P", expiry="20260620",
        strike=100.0, quantity=1, avg_cost=1.0,
    )
    options_storage.record_position_open(
        db, instrument_id="B", portfolio_id="P", mode="PAPER",
        symbol="B", right="P", expiry="20260620",
        strike=100.0, quantity=1, avg_cost=1.0,
    )
    summary = options_storage.reconcile_positions(
        db, portfolio_id="P", mode="PAPER", snapshot={},
    )
    assert summary.closed == 2
    assert db.positions == {}


def test_record_event_carries_sleeve_template_strategy_into_log():
    db = _FakeDb()
    options_storage.record_position_open(
        db, **_open_args(),
        sleeve="HEDGE",
        template="hedge.spy_protective_put",
        strategy="protective_put",
        decision_id="dec-42",
        order_id="ord-77",
        fill_id="fill-99",
    )
    evt = db.events[0]
    assert evt["sleeve"] == "HEDGE"
    assert evt["template"] == "hedge.spy_protective_put"
    assert evt["strategy"] == "protective_put"
    assert evt["decision_id"] == "dec-42"
    assert evt["order_id"] == "ord-77"
    assert evt["fill_id"] == "fill-99"


# ── Order-submission provenance (strategy-tag round trip) ────────────


def _submit_args(**overrides: Any) -> dict[str, Any]:
    args = dict(
        portfolio_id="US_OPTIONS_PAPER",
        mode="PAPER",
        instrument_id="VIX_260818_35C.US",
        symbol="VIX",
        right="C",
        expiry="20260818",
        strike=35.0,
        quantity=150,
        strategy="vix_tail_hedge",
        order_id="oid-1",
        limit_price=0.90,
    )
    args.update(overrides)
    return args


def test_record_order_submission_writes_submit_event():
    db = _FakeDb()
    options_storage.record_order_submission(db, **_submit_args())

    assert len(db.events) == 1
    evt = db.events[0]
    assert evt["event_type"] == options_storage.EVENT_SUBMIT
    assert evt["position_id"] == options_storage.SUBMIT_POSITION_ID
    assert evt["instrument_id"] == "VIX_260818_35C.US"
    assert evt["strategy"] == "vix_tail_hedge"
    assert evt["order_id"] == "oid-1"
    assert evt["quantity_delta"] == 150
    # No position row is created at submission time.
    assert db.positions == {}


def test_load_strategy_tags_merges_submissions_and_position_rows():
    db = _FakeDb()
    # Two submissions on the same contract: the later one wins.
    options_storage.record_order_submission(
        db, **_submit_args(strategy="old_strategy"),
    )
    options_storage.record_order_submission(db, **_submit_args())
    # A second contract that already has a reconciled position row with
    # a strategy — the row wins over any submission.
    options_storage.record_order_submission(
        db, **_submit_args(
            instrument_id="SPY_260918_450P.US", symbol="SPY", right="P",
            expiry="20260918", strike=450.0, strategy="from_submission",
        ),
    )
    options_storage.record_position_open(
        db, instrument_id="SPY_260918_450P.US",
        portfolio_id="US_OPTIONS_PAPER", mode="PAPER",
        symbol="SPY", right="P", expiry="20260918",
        strike=450.0, quantity=2, avg_cost=5.0,
        strategy="protective_put",
    )

    tags = options_storage.load_strategy_tags(
        db, portfolio_id="US_OPTIONS_PAPER", mode="PAPER",
    )
    assert tags["VIX_260818_35C.US"] == "vix_tail_hedge"
    assert tags["SPY_260918_450P.US"] == "protective_put"


def test_load_strategy_tags_scoped_to_portfolio_and_mode():
    db = _FakeDb()
    options_storage.record_order_submission(db, **_submit_args())
    tags = options_storage.load_strategy_tags(
        db, portfolio_id="US_OPTIONS_LIVE", mode="LIVE",
    )
    assert tags == {}


def test_reconcile_backfills_strategy_on_untagged_rows():
    db = _FakeDb()
    # Row persisted before the tag was known (strategy=None).
    options_storage.record_position_open(
        db, instrument_id="VIX_260818_35C.US",
        portfolio_id="US_OPTIONS_PAPER", mode="PAPER",
        symbol="VIX", right="C", expiry="20260818",
        strike=35.0, quantity=150, avg_cost=0.85,
    )
    row = next(iter(db.positions.values()))
    assert row["strategy"] is None

    snapshot = {
        "VIX_260818_35C.US": _SnapEntry(
            symbol="VIX", right="C", expiry="20260818",
            strike=35.0, quantity=150, avg_cost=0.85,
            strategy="vix_tail_hedge",
        ),
    }
    summary = options_storage.reconcile_positions(
        db, portfolio_id="US_OPTIONS_PAPER", mode="PAPER",
        snapshot=snapshot,
    )
    assert summary.updated == 1
    row = next(iter(db.positions.values()))
    assert row["strategy"] == "vix_tail_hedge"


def test_submission_tags_restored_into_strategy_map_after_sync_round_trip():
    """Full defect-A round trip: strategy recorded at submission time is
    restored into OptionsPortfolio._strategy_map after a fresh sync
    (which comes back from the broker with strategy='')."""
    from prometheus.execution.broker_interface import Position
    from prometheus.execution.options_portfolio import OptionsPortfolio

    db = _FakeDb()
    options_storage.record_order_submission(db, **_submit_args())

    portfolio = OptionsPortfolio(ib=None)
    portfolio.sync(broker_positions={
        "VIX_260818_35C.US": Position(
            instrument_id="VIX_260818_35C.US",
            quantity=150, avg_cost=0.85,
            market_value=12750.0, unrealized_pnl=0.0,
        ),
    })
    # Fresh sync: broker knows nothing about strategies.
    assert portfolio.get_position("VIX_260818_35C.US").strategy == ""

    tags = options_storage.load_strategy_tags(
        db, portfolio_id="US_OPTIONS_PAPER", mode="PAPER",
    )
    applied = portfolio.apply_strategy_tags(tags)

    assert applied == 1
    assert portfolio._strategy_map["VIX_260818_35C.US"] == "vix_tail_hedge"
    assert portfolio.get_position("VIX_260818_35C.US").strategy == "vix_tail_hedge"
    assert len(portfolio.get_positions_by_strategy("vix_tail_hedge")) == 1
    # Tag survives the NEXT sync too (the nightly failure mode).
    portfolio.sync(broker_positions={
        "VIX_260818_35C.US": Position(
            instrument_id="VIX_260818_35C.US",
            quantity=150, avg_cost=0.85,
            market_value=13000.0, unrealized_pnl=250.0,
        ),
    })
    assert portfolio.get_position("VIX_260818_35C.US").strategy == "vix_tail_hedge"


def test_apply_strategy_tags_matches_vix_expiry_off_by_one():
    """VIX contracts qualify at IBKR with settlement-minus-one-day
    expiries — the submitted expiry and the synced position expiry can
    differ by one day. The signature fallback must still match."""
    from prometheus.execution.broker_interface import Position
    from prometheus.execution.options_portfolio import OptionsPortfolio

    portfolio = OptionsPortfolio(ib=None)
    portfolio.sync(broker_positions={
        "VIX_260817_35C.US": Position(
            instrument_id="VIX_260817_35C.US",
            quantity=150, avg_cost=0.85,
            market_value=12750.0, unrealized_pnl=0.0,
        ),
    })
    applied = portfolio.apply_strategy_tags(
        {"VIX_260818_35C.US": "vix_tail_hedge"},
    )
    assert applied == 1
    assert portfolio.get_position("VIX_260817_35C.US").strategy == "vix_tail_hedge"
