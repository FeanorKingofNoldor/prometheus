"""Integration tests for prometheus.execution.options_storage against real Postgres.

The ``options_positions`` / ``options_position_events`` tables have a
column named ``right`` — a reserved keyword in PostgreSQL that must be
quoted in every SQL statement. The unit tests in
``test_options_storage.py`` use a fake cursor and therefore never catch
quoting bugs, which is how the unquoted-``right`` regression shipped and
crashed the daily derivatives job. This module exercises the real
INSERT + SELECT + DELETE paths end-to-end against the runtime database
(PgBouncer, port 6432, env from ``.env``).

Run with:

    set -a; source .env; set +a
    pytest tests/test_options_storage_postgres.py -v

Skipped automatically when the runtime DB is unreachable.
"""

from __future__ import annotations

import pytest

pytest.importorskip("apatheon.core.database")

from apatheon.core.database import DatabaseManager, get_db_manager

from prometheus.execution import options_storage

pytestmark = pytest.mark.integration

TEST_PORTFOLIO_ID = "TEST_OPTIONS_STORAGE"
TEST_INSTRUMENT_ID = "SPY_TEST"
TEST_MODE = "TEST"


def _cleanup(db_manager: DatabaseManager) -> None:
    """Remove every row this test suite may have left behind."""
    with db_manager.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM options_position_events WHERE portfolio_id = %s",
                (TEST_PORTFOLIO_ID,),
            )
            cur.execute(
                "DELETE FROM options_positions WHERE portfolio_id = %s",
                (TEST_PORTFOLIO_ID,),
            )
        conn.commit()


@pytest.fixture(scope="module")
def db_manager() -> DatabaseManager:
    try:
        db = get_db_manager()
        with db.get_runtime_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
    except Exception as exc:  # pragma: no cover - environment-dependent
        pytest.skip(f"runtime DB unreachable: {exc}")
    return db


def test_open_read_close_roundtrip(db_manager: DatabaseManager) -> None:
    """Open a position, read it back, close it — the previously-broken paths."""

    _cleanup(db_manager)
    try:
        # INSERT (options_positions + OPEN event) — crashed pre-fix on
        # the unquoted ``right`` column.
        position_id = options_storage.record_position_open(
            db_manager,
            instrument_id=TEST_INSTRUMENT_ID,
            portfolio_id=TEST_PORTFOLIO_ID,
            mode=TEST_MODE,
            symbol="SPY",
            right="P",
            expiry="20261218",
            strike=500.0,
            quantity=2,
            avg_cost=3.50,
            sleeve="HEDGE",
            template="hedge.test_template",
            strategy="integration_test",
            greeks={"delta": -0.30, "implied_vol": 0.22},
        )
        assert position_id

        # Idempotency on (instrument_id, portfolio_id, mode).
        again = options_storage.record_position_open(
            db_manager,
            instrument_id=TEST_INSTRUMENT_ID,
            portfolio_id=TEST_PORTFOLIO_ID,
            mode=TEST_MODE,
            symbol="SPY",
            right="P",
            expiry="20261218",
            strike=500.0,
            quantity=2,
            avg_cost=3.50,
        )
        assert again == position_id

        # SELECT list with ``right`` — crashed pre-fix.
        rows = options_storage.get_open_positions(
            db_manager, portfolio_id=TEST_PORTFOLIO_ID, mode=TEST_MODE,
        )
        assert len(rows) == 1
        row = rows[0]
        assert row.position_id == position_id
        assert row.instrument_id == TEST_INSTRUMENT_ID
        assert row.symbol == "SPY"
        assert row.right == "P"
        assert row.expiry == "20261218"
        assert row.strike == 500.0
        assert row.quantity == 2
        assert row.avg_cost == 3.50
        assert row.sleeve == "HEDGE"

        # Mark update (dynamic UPDATE SET; ``right`` only in read-back).
        options_storage.update_position_mark(
            db_manager,
            position_id=position_id,
            market_price=2.10,
            market_value=420.0,
            unrealized_pnl=-280.0,
            greeks={"delta": -0.25},
            write_mark_event=True,
        )

        # Full close — SELECT with ``right`` + DELETE + CLOSE event.
        removed = options_storage.record_position_close(
            db_manager,
            position_id=position_id,
            quantity_delta=-2,
            price=2.10,
            realized_pnl=-280.0,
        )
        assert removed is True

        assert options_storage.get_open_positions(
            db_manager, portfolio_id=TEST_PORTFOLIO_ID, mode=TEST_MODE,
        ) == []

        # Event log survives closure; quoted ``right`` reads back too.
        with db_manager.get_runtime_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT event_type, "right"
                    FROM options_position_events
                    WHERE portfolio_id = %s AND position_id = %s
                    ORDER BY event_at
                    """,
                    (TEST_PORTFOLIO_ID, position_id),
                )
                events = cur.fetchall()
        assert [e[0] for e in events] == ["OPEN", "MARK", "CLOSE"]
        assert all(e[1] == "P" for e in events)
    finally:
        _cleanup(db_manager)
