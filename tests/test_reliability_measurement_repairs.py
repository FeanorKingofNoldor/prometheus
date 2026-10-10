"""Regression cases from the production reliability/measurement review."""
from datetime import date, timedelta
from types import SimpleNamespace
import math
import threading

import pytest
from prometheus.decisions.evaluator import OutcomeEvaluator


def evaluator(weights_prices):
    ev = object.__new__(OutcomeEvaluator)
    ev.calendar = SimpleNamespace(trading_days_between=lambda **kw: sorted(weights_prices))
    ev._horizon_in_future = lambda *args: False
    ev._get_prices_for_instruments = lambda ids, day: weights_prices[day]
    ev._resolve_portfolio_nav = lambda day: 500_000
    return ev


def test_cash_is_retained_and_daily_returns_match_horizon():
    day = date(2026, 1, 1)
    ev = evaluator({day: {'X': 100}, day + timedelta(days=1): {'X': 105}, day + timedelta(days=2): {'X': 110}})
    result = ev.evaluate_portfolio_decision_outcome(decision_id='test', decision_as_of_date=day,
        horizon_days=2, target_weights={'X': .4})
    assert result.realized_return == pytest.approx(.04)
    assert result.realized_pnl == pytest.approx(20_000)
    assert result.metadata['cash_weight'] == .6
    assert result.metadata['evaluation_kind'] == 'target_portfolio'
    returns = ev._compute_daily_portfolio_returns({'X': .4}, sorted(ev.calendar.trading_days_between()))
    assert math.prod(1 + r for r in returns) - 1 == pytest.approx(.04)


def test_incomplete_prices_do_not_reweight_or_grade():
    day = date(2026, 1, 1)
    ev = evaluator({day: {'X': 100, 'Y': 100}, day + timedelta(days=1): {'X': 110}})
    result = ev.evaluate_portfolio_decision_outcome(decision_id='test', decision_as_of_date=day,
        horizon_days=1, target_weights={'X': .4, 'Y': .2})
    assert result is None


def test_short_sign_and_unavailable_nav_are_preserved():
    day = date(2026, 1, 1)
    ev = evaluator({day: {'X': 100}, day + timedelta(days=1): {'X': 110}})
    ev._resolve_portfolio_nav = lambda day: None
    result = ev.evaluate_portfolio_decision_outcome(decision_id='test', decision_as_of_date=day,
        horizon_days=1, target_weights={'X': -.4})
    assert result.realized_return == pytest.approx(-.04)
    assert result.realized_pnl is None


def test_deliberate_disconnect_does_not_reconnect():
    from prometheus.execution.ibkr_client_impl import IbkrClientImpl
    client = object.__new__(IbkrClientImpl)
    client._conn_mgr = None
    client._connected = True
    client._reconnect_attempts = 0
    client._max_reconnect_attempts = 5
    calls = []
    client._stop_heartbeat = lambda: None
    client._attempt_reconnect = lambda: calls.append('reconnected')
    client._ib = SimpleNamespace(isConnected=lambda: True, disconnect=client._on_disconnected)
    client.disconnect()
    client._on_disconnected()  # queued duplicate event must also stay closed
    assert calls == []
    assert not client._connected


def test_unexpected_disconnect_still_attempts_recovery():
    from prometheus.execution.ibkr_client_impl import IbkrClientImpl
    client = object.__new__(IbkrClientImpl)
    client._conn_mgr = None
    client._closing = False
    client._reconnect_attempts = 0
    client._max_reconnect_attempts = 5
    calls = []
    client._attempt_reconnect = lambda: calls.append(True)
    client._on_disconnected()
    assert calls == [True]


def test_shutdown_during_retry_delay_prevents_reconnect(monkeypatch):
    from prometheus.execution.ibkr_client_impl import IbkrClientImpl
    client = object.__new__(IbkrClientImpl)
    client._closing = False
    client._reconnect_attempts = 0
    client._max_reconnect_attempts = 5
    client._reconnect_delay_sec = 1
    calls = []
    client.connect = lambda: calls.append(True)
    monkeypatch.setattr('prometheus.execution.ibkr_client_impl.time.sleep', lambda _: setattr(client, '_closing', True))
    client._attempt_reconnect()
    assert calls == []


def test_ib_client_has_event_loop_in_worker_thread(monkeypatch):
    import asyncio
    import prometheus.execution.ibkr_client_impl as impl
    from prometheus.execution.ibkr_client import IbkrConnectionConfig
    found, errors = [], []
    def fake_ib():
        found.append(asyncio.get_event_loop())
        return SimpleNamespace()
    monkeypatch.setattr(impl, 'IB', fake_ib)
    monkeypatch.setattr(impl.IbkrClientImpl, '_wire_events', lambda *a: None)
    def worker():
        try:
            impl.IbkrClientImpl(config=IbkrConnectionConfig(), mapper=SimpleNamespace())
        except Exception as exc: errors.append(exc)
        finally:
            if found: found[0].close()
    thread = threading.Thread(target=worker)
    thread.start(); thread.join()
    assert not errors
    assert found


def test_tied_ranks_are_averaged_and_constant_signal_is_undefined():
    from prometheus.decisions.live_performance import _rank, _spearman_rho, _LIVE_GUARD
    assert _rank([2, 1, 1]) == [3, 1.5, 1.5]
    assert math.isnan(_spearman_rho([1,1,1], [1,2,3]))
    assert _spearman_rho([1,1,2], [1,2,3]) == pytest.approx(math.sqrt(3)/2)
    assert 'executed_fills' in _LIVE_GUARD


def test_peak_uses_scoped_history_instead_of_current_value():
    from prometheus.wheel.runner import _peak_nav
    statements = []
    class Cursor:
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def execute(self, sql, params=None): statements.append((sql, params))
        def fetchone(self):
            return (date(2026, 8, 5), 500_000) if len(statements)==1 else (600_000,)
    class Conn:
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def cursor(self): return Cursor()
    db = SimpleNamespace(get_runtime_connection=Conn)
    assert _peak_nav(db, 550_000, 'MY_PAPER') == 600_000
    assert 'MAX(equity)' in statements[1][0]
    assert statements[1][1] == ('MY_PAPER', date(2026, 8, 5))


def test_wheel_feedback_records_observations_without_fabricating_alpha():
    class Cursor:
        sql = ''
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def execute(self, sql, params): self.sql=sql
        def fetchone(self):
            if 'engine_decisions' in self.sql: return ({'shadow':False,'order_outcomes':[]},)
            return (0,)
        def fetchall(self): return [(date(2026,1,1),500000),(date(2026,1,6),505000)]
    class Conn:
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def cursor(self): return Cursor()
    ev = object.__new__(OutcomeEvaluator)
    ev.db_manager=SimpleNamespace(get_runtime_connection=Conn)
    ev._horizon_in_future=lambda *a:False
    outcome=ev.evaluate_wheel_observation('wheel',date(2026,1,1),5)
    assert outcome.realized_return is None and outcome.realized_pnl is None
    assert outcome.metadata['observed_nav_change']==pytest.approx(.01)
    assert outcome.metadata['graded'] is False


def test_wheel_decisions_are_dispatched_by_batch_evaluator():
    from prometheus.meta.types import DecisionOutcome
    ev=object.__new__(OutcomeEvaluator)
    ev.find_pending_decisions=lambda **kw:[('wheel',date(2026,1,1),5,'WHEEL')]
    ev._batch_load_decision_weights=lambda ids:{}
    ev._batch_load_decision_orders=lambda ids:{}
    ev._batch_load_universe_instruments=lambda ids:{}
    ev._preload_prices=lambda *a:None
    calls=[]
    ev._storage=SimpleNamespace(save_decision_outcome=lambda outcome:calls.append(outcome))
    ev.evaluate_wheel_observation=lambda *a:DecisionOutcome(a[0],a[2],metadata={'graded':False})
    assert ev.evaluate_pending_outcomes(as_of_date=date(2026,1,6))==1
    assert calls[0].decision_id=='wheel'
