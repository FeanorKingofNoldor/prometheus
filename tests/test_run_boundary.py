"""Tests for prometheus.decisions.run_boundary (account-reset scoping)."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, List

from prometheus.decisions.run_boundary import clamp_window_start, current_run_start

# ── Fake DB plumbing ─────────────────────────────────────────────────


class _FakeCursor:
    def __init__(self, rows: List[tuple], raise_on_execute: bool = False) -> None:
        self._rows = rows
        self._raise = raise_on_execute
        self.executed: List[str] = []

    def execute(self, sql: str, params: Any = None) -> None:
        self.executed.append(sql)
        if self._raise:
            raise RuntimeError("relation account_resets does not exist")

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def close(self) -> None:
        pass


class _FakeConn:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> _FakeCursor:
        return self._cursor

    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, *a: Any) -> bool:
        return False


class _FakeDB:
    def __init__(self, rows: List[tuple], raise_on_execute: bool = False) -> None:
        self.cursor = _FakeCursor(rows, raise_on_execute)

    def get_runtime_connection(self) -> _FakeConn:
        return _FakeConn(self.cursor)


# ── current_run_start ────────────────────────────────────────────────


def test_current_run_start_returns_latest_reset_date():
    db = _FakeDB([(date(2026, 5, 13),)])
    assert current_run_start(db) == date(2026, 5, 13)
    assert "account_resets" in db.cursor.executed[0]


def test_current_run_start_coerces_datetime_to_date():
    db = _FakeDB([(datetime(2026, 5, 13, 18, 12, tzinfo=timezone.utc),)])
    assert current_run_start(db) == date(2026, 5, 13)


def test_current_run_start_none_when_table_empty():
    # MAX() over an empty table returns a NULL row.
    db = _FakeDB([(None,)])
    assert current_run_start(db) is None


def test_current_run_start_none_when_no_row():
    db = _FakeDB([])
    assert current_run_start(db) is None


def test_current_run_start_none_on_db_error():
    db = _FakeDB([], raise_on_execute=True)
    assert current_run_start(db) is None


def test_current_run_start_none_on_garbage_value():
    # Defensive: a mis-stubbed DB returning a non-date must not propagate.
    db = _FakeDB([(0.42,)])
    assert current_run_start(db) is None


# ── clamp_window_start ───────────────────────────────────────────────


def test_clamp_uses_run_start_when_later():
    assert clamp_window_start(date(2026, 1, 1), date(2026, 5, 13)) == date(2026, 5, 13)


def test_clamp_keeps_window_start_when_later():
    assert clamp_window_start(date(2026, 6, 1), date(2026, 5, 13)) == date(2026, 6, 1)


def test_clamp_noop_without_run_start():
    assert clamp_window_start(date(2026, 1, 1), None) == date(2026, 1, 1)
