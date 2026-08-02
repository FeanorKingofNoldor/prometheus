"""Tests for morning fill reconciliation (prometheus.execution.fill_reconciliation).

No real IBKR or database — the client is injected via the ``client_factory``
test seam and the DB manager is faked with a SQL-dispatching cursor.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

from apatheon.core.market_state import MarketState

from prometheus.execution.broker_interface import OrderSide, OrderStatus
from prometheus.execution.fill_reconciliation import (
    _derive_order_status,
    _fill_from_ib,
    _normalize_exec_time,
    reconcile_fills,
)
from prometheus.orchestration.dag import JobPriority, build_market_dag

# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


def _ib_fill(
    exec_id: str,
    order_ref: str,
    *,
    side: str = "BOT",
    shares: float = 10.0,
    price: float = 100.0,
    symbol: str = "AAPL",
    ts: datetime | None = None,
    commission: float = 1.25,
):
    return SimpleNamespace(
        execution=SimpleNamespace(
            execId=exec_id,
            orderRef=order_ref,
            side=side,
            shares=shares,
            price=price,
            time=ts or datetime.now(timezone.utc) - timedelta(hours=1),
            orderId=42,
            exchange="SMART",
        ),
        contract=SimpleNamespace(symbol=symbol, secType="STK"),
        commissionReport=SimpleNamespace(commission=commission),
    )


class _FakeCursor:
    def __init__(self, db: "_FakeDBManager") -> None:
        self._db = db
        self._rows: list = []

    def execute(self, sql: str, params=None) -> None:
        s = " ".join(sql.split()).lower()
        self._db.executed.append((s, params))
        if s.startswith("select order_id, quantity"):
            wanted = set(params[0])
            self._rows = [r for r in self._db.order_rows if r[0] in wanted]
        elif s.startswith("select fill_id from fills"):
            wanted = set(params[0])
            self._rows = [(f,) for f in self._db.existing_fill_ids if f in wanted]
        elif s.startswith("insert into fills"):
            self._db.inserted_fills.append(params)
            self._rows = []
        elif s.startswith("insert into executed_actions"):
            self._db.inserted_actions.append(params)
            self._rows = []
        elif "metadata = coalesce" in s:  # stale-order expiry UPDATE
            self._db.expiry_params = params
            self._rows = [(oid,) for oid in self._db.stale_order_ids]
        elif s.startswith("update orders set status"):
            self._db.status_updates.append(params)
            self._rows = []
        else:
            self._rows = []

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def close(self) -> None:
        pass


class _FakeConn:
    def __init__(self, db: "_FakeDBManager") -> None:
        self._db = db

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self._db)

    def commit(self) -> None:
        pass


class _FakeDBManager:
    def __init__(self, *, order_rows=(), existing_fill_ids=(), stale_order_ids=()) -> None:
        # order_rows: (order_id, quantity, status, portfolio_id, decision_id)
        self.order_rows = list(order_rows)
        self.existing_fill_ids = set(existing_fill_ids)
        self.stale_order_ids = list(stale_order_ids)
        self.executed: list = []
        self.inserted_fills: list = []
        self.inserted_actions: list = []
        self.status_updates: list = []
        self.expiry_params = None
        self._conn = _FakeConn(self)

    @contextmanager
    def get_runtime_connection(self):
        yield self._conn


class _FakeClient:
    def __init__(self, ib_fills) -> None:
        self.ib = SimpleNamespace(reqExecutions=lambda _filter: list(ib_fills))
        self.disconnected = False

    def disconnect(self) -> None:
        self.disconnected = True


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def test_normalize_exec_time_variants():
    aware = datetime(2026, 7, 2, 14, 31, tzinfo=timezone.utc)
    assert _normalize_exec_time(aware) == aware
    naive = datetime(2026, 7, 2, 14, 31)
    assert _normalize_exec_time(naive).tzinfo is timezone.utc
    from_str = _normalize_exec_time("2026-07-02T14:31:00+00:00")
    assert from_str == aware


def test_fill_from_ib_maps_fields():
    fill = _fill_from_ib(_ib_fill("exec-1", "order-1", side="SLD", shares=5, price=42.5))
    assert fill is not None
    assert fill.fill_id == "exec-1"
    assert fill.order_id == "order-1"
    assert fill.instrument_id == "AAPL.US"
    assert fill.side == OrderSide.SELL
    assert fill.quantity == 5.0
    assert fill.price == 42.5
    assert fill.commission == 1.25
    assert fill.metadata["source"] == "fill_reconciliation"


def test_fill_from_ib_ignores_empty_order_ref():
    assert _fill_from_ib(_ib_fill("exec-1", "")) is None
    assert _fill_from_ib(_ib_fill("exec-1", "   ")) is None


def test_derive_order_status():
    assert _derive_order_status(10.0, 10.0) == OrderStatus.FILLED
    assert _derive_order_status(10.0000001, 10.0) == OrderStatus.FILLED
    assert _derive_order_status(4.0, 10.0) == OrderStatus.PARTIALLY_FILLED
    assert _derive_order_status(0.0, 10.0) is None


# ---------------------------------------------------------------------------
# reconcile_fills end-to-end (fakes)
# ---------------------------------------------------------------------------


def test_reconcile_fills_backfills_and_expires():
    db = _FakeDBManager(
        order_rows=[
            ("order-1", 10.0, "SUBMITTED", "IBKR_PAPER", "dec-1"),
            ("order-2", 20.0, "SUBMITTED", "IBKR_PAPER", None),
        ],
        existing_fill_ids={"exec-known"},  # already in fills — must not re-record
        stale_order_ids=["order-stale"],
    )
    ib_fills = [
        _ib_fill("exec-new", "order-1", shares=10.0),        # new → recorded, FILLED
        _ib_fill("exec-known", "order-2", shares=5.0),       # known → status only
        _ib_fill("exec-other", "not-our-order", shares=3.0),  # unknown orderRef → ignored
        _ib_fill("exec-blank", "", shares=3.0),               # empty orderRef → ignored
    ]
    client = _FakeClient(ib_fills)

    summary = reconcile_fills(db, mode="paper", client_factory=lambda _mode: client)

    assert summary["errors"] == []
    assert summary["fills_recorded"] == 1
    assert len(db.inserted_fills) == 1
    assert db.inserted_fills[0][0] == "exec-new"
    # executed_actions only for the genuinely new fill (insert is not idempotent)
    assert len(db.inserted_actions) == 1

    # order-1 fully filled, order-2 partially — both statuses updated
    assert summary["orders_updated"] == 2
    updates = {order_id: status for status, order_id in db.status_updates}
    assert updates["order-1"] == OrderStatus.FILLED.value
    assert updates["order-2"] == OrderStatus.PARTIALLY_FILLED.value

    # stale order expired; orders with executions excluded from the sweep
    assert summary["orders_expired"] == 1
    assert db.expiry_params is not None
    assert db.expiry_params[0] == OrderStatus.CANCELLED.value
    excluded = db.expiry_params[-1]
    assert set(excluded) == {"order-1", "order-2"}

    # client always disconnected
    assert client.disconnected is True


def test_reconcile_fills_idempotent_when_nothing_new():
    db = _FakeDBManager(
        order_rows=[("order-1", 10.0, "FILLED", "IBKR_PAPER", None)],
        existing_fill_ids={"exec-1"},
    )
    client = _FakeClient([_ib_fill("exec-1", "order-1", shares=10.0)])

    summary = reconcile_fills(db, mode="paper", client_factory=lambda _mode: client)

    assert summary["errors"] == []
    assert summary["fills_recorded"] == 0
    assert db.inserted_fills == []
    assert db.inserted_actions == []
    # status already FILLED — no redundant update
    assert summary["orders_updated"] == 0
    assert db.status_updates == []


def test_reconcile_fills_connect_failure_is_reported_not_raised():
    db = _FakeDBManager()

    def _boom(_mode):
        raise RuntimeError("gateway down")

    summary = reconcile_fills(db, mode="paper", client_factory=_boom)
    assert summary["fills_recorded"] == 0
    assert summary["orders_expired"] == 0
    assert len(summary["errors"]) == 1
    assert "gateway down" in summary["errors"][0]


def test_reconcile_fills_rejects_unknown_mode():
    db = _FakeDBManager()
    summary = reconcile_fills(db, mode="dry_run", client_factory=lambda _m: _FakeClient([]))
    assert summary["errors"]
    assert db.executed == []


# ---------------------------------------------------------------------------
# DAG wiring
# ---------------------------------------------------------------------------


def test_market_dag_has_reconcile_fills_job():
    dag = build_market_dag("US_EQ", date(2026, 7, 2))
    job_id = "us_eq_reconcile_fills_2026-07-02"
    assert job_id in dag.jobs
    job = dag.jobs[job_id]
    assert job.job_type == "reconcile_fills"
    assert job.priority == JobPriority.OPTIONAL
    assert job.max_retries == 3
    assert job.dependencies == ()
    # nothing may depend on it — it must never block the pipeline
    assert all(job_id not in j.dependencies for j in dag.jobs.values())


def test_reconcile_fills_runnable_in_pre_open_and_session_only():
    dag = build_market_dag("US_EQ", date(2026, 7, 2))
    job_id = "us_eq_reconcile_fills_2026-07-02"

    def runnable_ids(state: MarketState) -> set[str]:
        return {j.job_id for j in dag.get_runnable_jobs(set(), set(), state)}

    assert job_id in runnable_ids(MarketState.PRE_OPEN)
    assert job_id in runnable_ids(MarketState.SESSION)
    assert job_id not in runnable_ids(MarketState.POST_CLOSE)
    assert job_id not in runnable_ids(MarketState.OVERNIGHT)


def test_market_dag_has_reconcile_fills_eod_before_execution():
    """The EOD pass exists (US only), runs POST_CLOSE, and gates run_execution.

    Same-day capture is the only window where reqExecutions can still see
    the session's executions; run_execution depends on it so order state
    is current before planning.
    """
    dag = build_market_dag("US_EQ", date(2026, 7, 2))
    eod_id = "us_eq_reconcile_fills_eod_2026-07-02"
    assert eod_id in dag.jobs
    job = dag.jobs[eod_id]
    assert job.job_type == "reconcile_fills_eod"
    assert job.priority == JobPriority.STANDARD
    assert job.dependencies == ()
    assert eod_id in dag.jobs["us_eq_run_execution_2026-07-02"].dependencies

    def runnable_ids(state: MarketState) -> set[str]:
        return {j.job_id for j in dag.get_runnable_jobs(set(), set(), state)}

    assert eod_id in runnable_ids(MarketState.POST_CLOSE)
    assert eod_id not in runnable_ids(MarketState.PRE_OPEN)
    assert eod_id not in runnable_ids(MarketState.SESSION)


def test_non_account_global_dag_has_no_eod_reconcile_or_dep():
    dag = build_market_dag("UK_EQ", date(2026, 7, 2))
    assert not any(j.job_type == "reconcile_fills_eod" for j in dag.jobs.values())
    exec_job = dag.jobs["uk_eq_run_execution_2026-07-02"]
    assert exec_job.dependencies == ("uk_eq_run_books_2026-07-02",)


# ---------------------------------------------------------------------------
# Expiry gating (expire_stale / stale_cutoff_utc)
# ---------------------------------------------------------------------------


def test_reconcile_fills_expire_stale_false_never_expires():
    """A capture-only pass must not touch stale orders.

    This is the pre-open/catch-up profile: those passes run when the
    relevant session's executions are no longer (or not yet) visible, so
    expiring there cancels orders that actually filled at IBKR.
    """
    db = _FakeDBManager(stale_order_ids=["stale-1", "stale-2"])
    client = _FakeClient([])

    summary = reconcile_fills(
        db, mode="paper", client_factory=lambda _mode: client, expire_stale=False,
    )

    assert summary["orders_expired"] == 0
    assert db.expiry_params is None
    assert not any("metadata = coalesce" in sql for sql, _ in db.executed)


def test_reconcile_fills_stale_cutoff_override_reaches_update():
    """An explicit stale_cutoff_utc is used verbatim as the expiry cutoff."""
    db = _FakeDBManager(stale_order_ids=["stale-1"])
    client = _FakeClient([])
    cutoff = datetime(2026, 8, 2, 15, 30, tzinfo=timezone.utc)

    summary = reconcile_fills(
        db,
        mode="paper",
        client_factory=lambda _mode: client,
        expire_stale=True,
        stale_cutoff_utc=cutoff,
    )

    assert summary["orders_expired"] == 1
    # params: (CANCELLED, note, mode_db, SUBMITTED, PENDING, cutoff, exclude)
    assert db.expiry_params[5] == cutoff


# ---------------------------------------------------------------------------
# Multi-market stale-order cutoff (_previous_trading_day)
# ---------------------------------------------------------------------------

# 2026-06-29 is a Monday, 2026-06-30 a Tuesday, 2026-06-26 the prior Friday.
_TUESDAY = date(2026, 6, 30)
_MONDAY = date(2026, 6, 29)
_FRIDAY = date(2026, 6, 26)

#: Per-market holiday sets for the fake calendar. HK is closed on the
#: Monday, so its previous session before the Tuesday is the Friday.
_FAKE_HOLIDAYS = {
    "US_EQ": set(),
    "HK_EQ": {_MONDAY},
}


class _FakeTradingCalendar:
    def __init__(self, config, db_manager=None):
        market = config.market
        if market == "XX_EQ":
            raise RuntimeError("no calendar for XX_EQ")
        self._holidays = _FAKE_HOLIDAYS.get(market, set())

    def trading_days_between(self, start, end):
        days = []
        current = start
        while current <= end:
            if current.weekday() < 5 and current not in self._holidays:
                days.append(current)
            current = current + timedelta(days=1)
        return days


def _patch_calendar(monkeypatch):
    import apatheon.core.time as apatheon_time

    monkeypatch.setattr(apatheon_time, "TradingCalendar", _FakeTradingCalendar)


def test_previous_trading_day_returns_oldest_across_markets(monkeypatch):
    from prometheus.execution.fill_reconciliation import _previous_trading_day

    _patch_calendar(monkeypatch)

    # Single-market behaviour: US previous session is the Monday.
    assert _previous_trading_day(_TUESDAY, ["US_EQ"]) == _MONDAY
    # HK skipped Monday (holiday): its previous session is the Friday.
    assert _previous_trading_day(_TUESDAY, ["HK_EQ"]) == _FRIDAY
    # Multi-market takes the OLDEST so HK orders submitted Monday evening
    # are not expired before HK has had its next session.
    assert _previous_trading_day(_TUESDAY, ["US_EQ", "HK_EQ"]) == _FRIDAY
    assert _previous_trading_day(_TUESDAY, ["HK_EQ", "US_EQ"]) == _FRIDAY


def test_previous_trading_day_defaults_from_active_markets_env(monkeypatch):
    from prometheus.execution.fill_reconciliation import (
        _default_market_ids,
        _previous_trading_day,
    )

    _patch_calendar(monkeypatch)

    # Env-driven default: IRIS/INTEL pseudo-markets are filtered out.
    monkeypatch.setenv("PROMETHEUS_ACTIVE_MARKETS", "us_eq, hk_eq ,IRIS,INTEL,")
    assert _default_market_ids() == ("US_EQ", "HK_EQ")
    assert _previous_trading_day(_TUESDAY) == _FRIDAY

    # Unset env falls back to the historical single-market behaviour.
    monkeypatch.delenv("PROMETHEUS_ACTIVE_MARKETS", raising=False)
    assert _default_market_ids() == ("US_EQ",)
    assert _previous_trading_day(_TUESDAY) == _MONDAY

    # An env list of only pseudo-markets also falls back to US_EQ.
    monkeypatch.setenv("PROMETHEUS_ACTIVE_MARKETS", "IRIS,INTEL")
    assert _default_market_ids() == ("US_EQ",)


def test_previous_trading_day_broken_calendar_stays_conservative(monkeypatch):
    from prometheus.execution.fill_reconciliation import _previous_trading_day

    _patch_calendar(monkeypatch)

    # XX_EQ's calendar raises → it contributes the conservative as_of-3d
    # fallback, which is older than the US Monday, so the cutoff cannot
    # move forward just because one market's calendar is unavailable.
    assert _previous_trading_day(_TUESDAY, ["US_EQ", "XX_EQ"]) == _TUESDAY - timedelta(days=3)
    # All calendars broken → pure fallback.
    assert _previous_trading_day(_TUESDAY, ["XX_EQ"]) == _TUESDAY - timedelta(days=3)
