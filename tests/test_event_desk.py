from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from prometheus.event_desk.model import Policy, Quote, entry_signal, evidence_state, exposures, trade_plan
from prometheus.event_desk.runner import lifecycle, observe, submit_leg

NOW = datetime(2026, 10, 12, 15, tzinfo=timezone.utc)


def claim(**kwargs):
    return dict(
        status="applied",
        tense="past",
        extractor_confidence=0.95,
        polarity="asserted",
        modality="asserted",
        source_tier=2,
        url="https://one.example/story",
        source="One",
        quote="The refinery has shut down.",
        **kwargs,
    )


def test_copied_reporting_does_not_become_independent_confirmation():
    a = claim()
    b = {**a, "url": "https://two.example/copy"}
    assert not evidence_state([a, b])["confirmed"]
    b["quote"] = "Operator confirms production has stopped."
    assert evidence_state([a, b])["confirmed"]
    assert evidence_state([{**a, "source_tier": 1}])["confirmed"]
    assert not evidence_state([{**a, "source_tier": 1}, {**b, "modality": "denied"}])["confirmed"]


def test_future_and_rejected_claims_cannot_confirm():
    assert not evidence_state([{**claim(), "tense": "future", "source_tier": 1}])["confirmed"]
    assert not evidence_state([{**claim(), "status": "rejected", "source_tier": 1}])["confirmed"]


def test_resource_shortage_has_opposite_producer_and_consumer_exposure():
    resource = ("RESOURCE", "oil")
    producer = ("COMPANY", "X")
    consumer = ("COMPANY", "Y")
    edges = [
        SimpleNamespace(source=producer, target=resource, relation="produces", weight=0.8),
        SimpleNamespace(source=consumer, target=resource, relation="depends_on", weight=0.5),
    ]
    graph = SimpleNamespace(
        get_edges_to=lambda n: [e for e in edges if e.target == n],
        get_edges_from=lambda n: [e for e in edges if e.source == n],
    )
    result = exposures(graph, resource, -1)
    assert result["X"]["sign"] == 1 and result["Y"]["sign"] == -1
    assert result["Y"]["path"][0]["relation"] == "depends_on"


def test_two_hop_supplier_and_competitor_paths_preserve_signs():
    a = ("COMPANY", "A")
    b = ("COMPANY", "B")
    c = ("COMPANY", "C")
    edges = [
        SimpleNamespace(source=a, target=b, relation="supplies", weight=0.8),
        SimpleNamespace(source=b, target=c, relation="competes_with", weight=0.5),
    ]
    graph = SimpleNamespace(
        get_edges_to=lambda n: [e for e in edges if e.target == n],
        get_edges_from=lambda n: [e for e in edges if e.source == n],
    )
    result = exposures(graph, a, -1)
    assert result["B"]["sign"] == -1 and result["C"]["sign"] == 1
    assert len(result["C"]["path"]) == 2


@pytest.mark.parametrize("sign", [1, -1])
def test_strategies_require_different_evidence_and_price_behavior(sign):
    yes = {"confirmed": True}
    no = {"confirmed": False}
    assert entry_signal("CONTINUATION", sign, yes, 0.005 * sign, 0.003 * sign, 0.005, 20)[0] == sign
    assert entry_signal("CONTINUATION", sign, no, 0.005 * sign, 0.003 * sign, 0.005, 20)[0] is None
    assert entry_signal("CONTINUATION", sign, yes, 0.05 * sign, 0.03 * sign, 0.05, 20)[0] is None
    assert entry_signal("REVERSAL", sign, no, 0.012 * sign, 0.01 * sign, 0.02, 20)[0] == -sign
    assert entry_signal("REVERSAL", sign, yes, 0.012 * sign, 0.01 * sign, 0.02, 20)[0] is None
    assert entry_signal("REVERSAL", sign, no, 0.02 * sign, 0.01 * sign, 0.02, 20)[0] is None


def test_quote_freshness_spread_and_delayed_feed():
    assert Quote(100, 100.1, NOW, 3).valid(NOW)
    for q in [
        Quote(float("nan"), 100, NOW, 1),
        Quote(100, 110, NOW, 1),
        Quote(100, 101, NOW, 4),
        Quote(100, 100.1, NOW - timedelta(minutes=5), 1),
        Quote(100, 99, NOW, 1),
    ]:
        assert not q.valid(NOW)


def test_trade_plan_caps_risk_and_charges_borrow_for_short():
    q = Quote(100, 100.1, NOW, 1)
    long = trade_plan("CONTINUATION", 1, q)
    short = trade_plan("REVERSAL", -1, q)
    assert long["quantity"] * long["entry_limit"] <= 5000
    assert long["quantity"] * long["entry_limit"] * long["stop_fraction"] <= 100
    assert long["stop"] < long["entry_limit"] < long["target"]
    assert short["target"] < short["entry_limit"] < short["stop"]
    assert short["estimated_round_trip_cost"] > long["estimated_round_trip_cost"]
    assert long["expected_return"] is None


def candidate():
    return {
        "candidate_id": "a",
        "event_id": "e",
        "strategy": "CONTINUATION",
        "version": "event-v1",
        "instrument_id": "X.US",
        "state": "ENTRY_PENDING",
        "reason": "",
        "payload": {
            "sign": 1,
            "entry_order": "o",
            "plan": {"side": 1, "quantity": 10, "entry_limit": 100, "hold_hours": 72},
        },
    }


def order(status="CANCELLED", leg="ENTRY", oid="o", quantity=10):
    return {"order_id": oid, "status": status, "quantity": quantity, "timestamp": NOW, "metadata": {"event_leg": leg}}


def fill(qty, px, leg="ENTRY", oid="o"):
    return {"quantity": qty, "price": px, "leg": leg, "order_id": oid, "timestamp": NOW, "commission": 1}


def test_partial_entry_and_exit_use_actual_fills():
    c = candidate()
    lifecycle(c, [fill(4, 100)], [order()], {}, NOW, Policy())
    assert c["state"] == "OPEN" and c["payload"]["remaining_quantity"] == 4
    fs = [fill(4, 100), fill(2, 110, "EXIT", "x")]
    orders = [order(), order(leg="EXIT", oid="x", quantity=4)]
    lifecycle(c, fs, orders, {}, NOW, Policy())
    assert c["state"] == "OPEN" and c["payload"]["remaining_quantity"] == 2
    lifecycle(c, fs + [fill(2, 105, "EXIT", "y")], orders + [order(leg="EXIT", oid="y", quantity=2)], {}, NOW, Policy())
    assert c["state"] == "CLOSED"
    assert c["payload"]["net_pnl"] == 27


def test_missing_execution_or_ambiguous_submission_cannot_be_retried():
    c = candidate()
    lifecycle(c, [], [order("FILLED")], {}, NOW, Policy())
    assert c["state"] == "REVIEW"
    lifecycle(c, [], [order("PENDING")], {}, NOW + timedelta(minutes=11), Policy())
    assert c["state"] == "REVIEW"
    lifecycle(c, [], [], {}, NOW, Policy())
    assert c["state"] == "REVIEW"


def test_over_exit_is_reconciliation_error():
    c = candidate()
    lifecycle(c, [fill(4, 100), fill(5, 100, "EXIT", "x")], [], {}, NOW, Policy())
    assert c["state"] == "REVIEW"


def test_forward_horizons_cannot_be_backfilled_from_late_quotes():
    c = candidate()
    c["payload"]["baseline"] = {"price": 100, "benchmark": 100, "at": NOW.isoformat(), "quote": {"market_data_type": 3}}
    store = Mock()
    observe(
        store,
        c,
        Quote(109.9, 110.1, NOW, 3),
        Quote(101.9, 102.1, NOW, 3),
        NOW + timedelta(hours=4),
    )
    calls = [x for x in store.query.call_args_list if "event_desk_outcomes" in x.args[0]]
    assert len(calls) == 1 and calls[0].args[1][1] == 4
    data = calls[0].args[1][3].adapted
    assert data["evaluation_kind"] == "forward_research"
    assert data["signed_excess_return"] == pytest.approx(0.08)


def test_submission_persists_before_broker_and_does_not_retry_exception(monkeypatch):
    c = candidate()
    store = Mock()
    session = Mock()
    sequence = []
    store.save.side_effect = lambda *args: sequence.append("intent")
    monkeypatch.setattr("prometheus.execution.storage.record_orders", lambda *args, **kw: sequence.append("order"))

    def fail(order):
        sequence.append("broker")
        raise TimeoutError("ambiguous submission")

    session.submit.side_effect = fail
    with pytest.raises(TimeoutError):
        submit_leg(store, session, c, {"side": 1, "quantity": 4, "entry_limit": 100}, "ENTRY", NOW)
    assert sequence == ["intent", "order", "broker"]
    assert c["state"] == "ENTRY_PENDING"
    assert session.submit.call_count == 1


def test_event_dispatch_uses_shared_broker_token_and_two_minute_bucket(monkeypatch):
    from prometheus.orchestration import market_aware_daemon as m

    monkeypatch.setenv("PROMETHEUS_EVENT_DESK_ENABLED", "1")
    daemon = object.__new__(m.MarketAwareDaemon)
    daemon.lanes = {}
    daemon._ibkr_job_holder = "wheel"
    daemon.db_manager = Mock()
    daemon._start_job = Mock()
    m.MarketAwareDaemon._dispatch_event_desk(daemon, NOW)
    daemon._start_job.assert_not_called()
    daemon._ibkr_job_holder = None
    monkeypatch.setattr(m, "get_latest_job_execution", lambda *a: None)
    monkeypatch.setattr(m, "create_job_execution", lambda *a: Mock())
    m.MarketAwareDaemon._dispatch_event_desk(daemon, NOW)
    assert daemon._start_job.call_args.args[1].job_type == "event_desk_tick"
    assert "event_desk_tick" in m.IBKR_EXCLUSIVE_JOB_TYPES
    daemon._start_job.reset_mock()
    monkeypatch.setattr(m, "get_latest_job_execution", lambda *a: Mock())
    m.MarketAwareDaemon._dispatch_event_desk(daemon, NOW)
    daemon._start_job.assert_not_called()


class MemoryStore:
    db = None

    def __init__(self, candidates):
        self.rows = candidates
        self.orders = []
        self.executions = []

    def candidates(self, *args):
        return self.rows

    def save(self, c, now):
        c["updated_at"] = now

    def query(self, sql, params=(), write=False):
        return [{"peak": 0}] if "max(" in sql else []

    def order_rows(self, cid):
        return [o for o in self.orders if o["metadata"]["candidate_id"] == cid]

    def fills(self, cid):
        ids = {o["order_id"] for o in self.order_rows(cid)}
        return [f for f in self.executions if f["order_id"] in ids]

    def liquid_instruments(self, symbols, now):
        return set(symbols)

    def sync_order_states(self, states):
        for o in self.orders:
            if o["order_id"] in states:
                o["status"] = states[o["order_id"]]


def watcher(cid="a", instrument="X.US"):
    return {
        "candidate_id": cid,
        "event_id": "e",
        "strategy": "CONTINUATION",
        "version": "event-v1",
        "instrument_id": instrument,
        "state": "WATCHING",
        "reason": "",
        "created_at": NOW - timedelta(minutes=30),
        "updated_at": NOW,
        "first_seen": NOW - timedelta(minutes=30),
        "event": {"evidence_state": {"confirmed": True, "contradicted": False}},
        "payload": {
            "sign": 1,
            "baseline": {
                "price": 100,
                "benchmark": 100,
                "at": (NOW - timedelta(minutes=30)).isoformat(),
                "quote": {"market_data_type": 3},
            },
        },
    }


def test_complete_paper_cycle_enters_once_and_exits_only_owned_fills(monkeypatch):
    from prometheus.event_desk import runner
    from prometheus.execution.broker_interface import OrderStatus

    c = watcher()
    duplicate = watcher("b")
    store = MemoryStore([c, duplicate])
    monkeypatch.setattr(runner, "utcnow", lambda: NOW)

    def persist(db, *, orders, **kwargs):
        for o in orders:
            store.orders.append(
                {
                    "order_id": o.order_id,
                    "metadata": o.metadata,
                    "quantity": o.quantity,
                    "status": "PENDING",
                    "timestamp": NOW,
                }
            )

    def statuses(db, *, statuses):
        for o in store.orders:
            if o["order_id"] in statuses:
                o["status"] = statuses[o["order_id"]].value

    monkeypatch.setattr("prometheus.execution.storage.record_orders", persist)
    monkeypatch.setattr("prometheus.execution.storage.update_order_statuses", statuses)

    class Session:
        price = 100.5
        owned = 0
        submits = 0

        def order_states(self):
            return {o["order_id"]: o["status"] for o in store.orders}

        def quotes(self, symbols):
            return {
                s: Quote(99.95, 100.05, NOW, 3)
                if s == "SPY.US"
                else Quote(self.price - 0.05, self.price + 0.05, NOW, 3, 1000)
                for s in symbols
            }

        def positions(self):
            return {"X.US": SimpleNamespace(quantity=self.owned)}

        def reconcile(self, db):
            pass

        def submit(self, o):
            self.submits += 1
            self.owned += o.quantity * (1 if o.side.value == "BUY" else -1)
            store.executions.append(fill(o.quantity, o.limit_price, o.metadata["event_leg"], o.order_id))
            return OrderStatus.FILLED

    session = Session()
    result = runner.process(store, session, NOW, Policy())
    assert result["submissions"] == 1 and session.submits == 1
    assert c["state"] == "OPEN" and duplicate["state"] == "WATCHING"
    assert "already held" in duplicate["reason"]
    runner.process(store, session, NOW, Policy())
    assert session.submits == 1
    session.price = 97
    runner.process(store, session, NOW, Policy(), allow_entries=False)
    assert c["state"] == "CLOSED" and session.owned == 0 and session.submits == 2
    assert c["payload"]["net_pnl"] < 0
    assert store.orders[0]["quantity"] == store.orders[1]["quantity"]


def test_pending_entry_reserves_budget_and_blocks_other_entries(monkeypatch):
    from prometheus.event_desk import runner

    a = candidate()
    a.update(created_at=NOW, updated_at=NOW, first_seen=NOW, event={})
    b = watcher("b", "Y.US")
    store = MemoryStore([a, b])
    store.orders = [{**order("PENDING"), "metadata": {"candidate_id": "a", "event_leg": "ENTRY"}}]
    monkeypatch.setattr(runner, "utcnow", lambda: NOW)
    session = SimpleNamespace(
        order_states=lambda: {},
        positions=lambda: {},
        quotes=lambda ids: {
            s: Quote(100.45, 100.55, NOW, 3) if s != "SPY.US" else Quote(99.95, 100.05, NOW, 3) for s in ids
        },
    )
    result = runner.process(store, session, NOW, Policy())
    assert result["gross_reserved"] == 1000
    assert result["submissions"] == 0
    assert b["reason"] == "Unresolved order blocks new entries"


def test_live_gateway_cannot_be_used_by_paper_session(monkeypatch):
    from prometheus.event_desk.broker import PaperSession
    from prometheus.execution import ibkr_config

    monkeypatch.setattr(
        ibkr_config, "create_connection_config", lambda *a, **kw: SimpleNamespace(port=4001, account_id="U123")
    )
    with pytest.raises(RuntimeError, match="paper account"):
        PaperSession()


def test_country_sanctions_cannot_become_specific_resource_trades():
    graph = Mock()
    assert exposures(graph, ("SOVEREIGN", "RUS"), -1) == {}
    graph.get_edges_to.assert_not_called()


def test_aggregator_urls_are_not_independent_corroboration():
    a = {**claim(), "url": "https://news.google.com/rss/articles/opaque", "source_tier": 1}
    b = {**claim(), "url": "https://t.me/channel/123", "quote": "Another version of the headline"}
    assert not evidence_state([a, b])["confirmed"]
    assert evidence_state([a, b])["sources"] == 0
