"""Tests for pipeline state machine (prometheus.pipeline.state)."""

from __future__ import annotations

from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pytest

from prometheus.pipeline.state import (
    EngineRun,
    EngineRunStateError,
    RunPhase,
    _row_to_engine_run,
    _validate_transition,
    get_or_create_run,
    load_run,
    update_phase,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_run(phase: RunPhase = RunPhase.WAITING_FOR_DATA) -> EngineRun:
    now = datetime(2025, 6, 30, 12, 0, 0)
    return EngineRun(
        run_id="run-001",
        as_of_date=date(2025, 6, 30),
        region="US_EQ",
        phase=phase,
        error={},
        created_at=now,
        updated_at=now,
        phase_started_at=now,
        phase_completed_at=None,
    )


def _make_row(
    run_id: str = "run-001",
    as_of_date: date = date(2025, 6, 30),
    region: str = "US_EQ",
    phase: str = "WAITING_FOR_DATA",
) -> tuple:
    now = datetime(2025, 6, 30, 12, 0, 0)
    return (run_id, as_of_date, region, phase, {}, now, now, now, None)


# ---------------------------------------------------------------------------
# RunPhase ordering tests
# ---------------------------------------------------------------------------


class TestRunPhaseOrdering:
    """Tests for RunPhase comparison operators."""

    def test_waiting_lt_data_ready(self):
        assert RunPhase.WAITING_FOR_DATA < RunPhase.DATA_READY

    def test_data_ready_lt_signals_done(self):
        assert RunPhase.DATA_READY < RunPhase.SIGNALS_DONE

    def test_signals_done_lt_universes_done(self):
        assert RunPhase.SIGNALS_DONE < RunPhase.UNIVERSES_DONE

    def test_universes_done_lt_books_done(self):
        assert RunPhase.UNIVERSES_DONE < RunPhase.BOOKS_DONE

    def test_books_done_lt_execution_done(self):
        assert RunPhase.BOOKS_DONE < RunPhase.EXECUTION_DONE

    def test_completed_not_lt_waiting(self):
        assert not (RunPhase.COMPLETED < RunPhase.WAITING_FOR_DATA)

    def test_same_phase_not_lt(self):
        assert not (RunPhase.DATA_READY < RunPhase.DATA_READY)

    def test_le_same(self):
        assert RunPhase.DATA_READY <= RunPhase.DATA_READY

    def test_le_less(self):
        assert RunPhase.DATA_READY <= RunPhase.SIGNALS_DONE

    def test_gt(self):
        assert RunPhase.COMPLETED > RunPhase.WAITING_FOR_DATA

    def test_ge_same(self):
        assert RunPhase.DATA_READY >= RunPhase.DATA_READY

    def test_ge_greater(self):
        assert RunPhase.SIGNALS_DONE >= RunPhase.DATA_READY

    def test_comparison_with_non_phase_returns_not_implemented(self):
        assert RunPhase.DATA_READY.__lt__("not_a_phase") is NotImplemented
        assert RunPhase.DATA_READY.__le__("not_a_phase") is NotImplemented
        assert RunPhase.DATA_READY.__gt__("not_a_phase") is NotImplemented
        assert RunPhase.DATA_READY.__ge__("not_a_phase") is NotImplemented


# ---------------------------------------------------------------------------
# _validate_transition tests
# ---------------------------------------------------------------------------


class TestValidateTransition:
    """Tests for the _validate_transition function."""

    def test_same_phase_is_allowed(self):
        """Transitioning to the same phase is a no-op."""
        _validate_transition(RunPhase.DATA_READY, RunPhase.DATA_READY)

    def test_valid_linear_transitions(self):
        """All sequential forward transitions should be allowed."""
        valid_pairs = [
            (RunPhase.WAITING_FOR_DATA, RunPhase.DATA_READY),
            (RunPhase.DATA_READY, RunPhase.SIGNALS_DONE),
            (RunPhase.SIGNALS_DONE, RunPhase.UNIVERSES_DONE),
            (RunPhase.UNIVERSES_DONE, RunPhase.BOOKS_DONE),
            (RunPhase.BOOKS_DONE, RunPhase.EXECUTION_DONE),
            (RunPhase.EXECUTION_DONE, RunPhase.OPTIONS_DONE),
            (RunPhase.OPTIONS_DONE, RunPhase.COMPLETED),
        ]
        for current, new in valid_pairs:
            _validate_transition(current, new)  # Should not raise

    def test_books_done_can_skip_to_completed(self):
        """BOOKS_DONE can skip EXECUTION_DONE and go directly to COMPLETED."""
        _validate_transition(RunPhase.BOOKS_DONE, RunPhase.COMPLETED)

    def test_books_done_can_skip_to_options_done(self):
        """BOOKS_DONE can skip EXECUTION_DONE and go to OPTIONS_DONE."""
        _validate_transition(RunPhase.BOOKS_DONE, RunPhase.OPTIONS_DONE)

    def test_execution_done_can_skip_to_completed(self):
        """EXECUTION_DONE can skip OPTIONS_DONE and go to COMPLETED."""
        _validate_transition(RunPhase.EXECUTION_DONE, RunPhase.COMPLETED)

    def test_any_phase_can_fail(self):
        """Every non-terminal phase can transition to FAILED."""
        for phase in RunPhase:
            if phase in (RunPhase.FAILED, RunPhase.COMPLETED):
                continue
            _validate_transition(phase, RunPhase.FAILED)

    def test_failed_can_only_retry_to_waiting(self):
        """FAILED → WAITING_FOR_DATA is allowed (retry); all other targets forbidden."""
        # Retry path is intentionally permitted so a stuck run can be re-driven.
        _validate_transition(RunPhase.FAILED, RunPhase.WAITING_FOR_DATA)
        # All other targets must raise.
        for phase in RunPhase:
            if phase in (RunPhase.FAILED, RunPhase.WAITING_FOR_DATA):
                continue
            with pytest.raises(EngineRunStateError, match="Cannot transition from FAILED"):
                _validate_transition(RunPhase.FAILED, phase)

    def test_completed_cannot_transition(self):
        """COMPLETED phase cannot transition forward."""
        for phase in RunPhase:
            if phase == RunPhase.COMPLETED:
                continue
            with pytest.raises(EngineRunStateError, match="Invalid transition"):
                _validate_transition(RunPhase.COMPLETED, phase)

    def test_backward_transition_raises(self):
        """Going backward raises EngineRunStateError."""
        with pytest.raises(EngineRunStateError, match="Invalid transition"):
            _validate_transition(RunPhase.SIGNALS_DONE, RunPhase.DATA_READY)

    def test_skipping_phases_raises(self):
        """Skipping non-optional phases raises EngineRunStateError."""
        with pytest.raises(EngineRunStateError, match="Invalid transition"):
            _validate_transition(RunPhase.WAITING_FOR_DATA, RunPhase.SIGNALS_DONE)

    def test_data_ready_to_universes_done_raises(self):
        """Cannot skip from DATA_READY directly to UNIVERSES_DONE."""
        with pytest.raises(EngineRunStateError):
            _validate_transition(RunPhase.DATA_READY, RunPhase.UNIVERSES_DONE)


# ---------------------------------------------------------------------------
# _row_to_engine_run tests
# ---------------------------------------------------------------------------


class TestRowToEngineRun:
    """Tests for the _row_to_engine_run helper."""

    def test_converts_valid_row(self):
        row = _make_row()
        run = _row_to_engine_run(row)
        assert run.run_id == "run-001"
        assert run.phase == RunPhase.WAITING_FOR_DATA
        assert run.region == "US_EQ"

    def test_converts_phase_string_to_enum(self):
        row = _make_row(phase="SIGNALS_DONE")
        run = _row_to_engine_run(row)
        assert run.phase == RunPhase.SIGNALS_DONE

    def test_invalid_phase_raises(self):
        row = _make_row(phase="INVALID_PHASE")
        with pytest.raises(ValueError):
            _row_to_engine_run(row)


# ---------------------------------------------------------------------------
# load_run tests (DB mocked)
# ---------------------------------------------------------------------------


class TestLoadRun:
    """Tests for load_run with mocked DatabaseManager."""

    def test_load_existing_run(self):
        row = _make_row()
        cursor = MagicMock()
        cursor.fetchone.return_value = row
        conn = MagicMock()
        conn.cursor.return_value = cursor
        conn.__enter__ = MagicMock(return_value=conn)
        conn.__exit__ = MagicMock(return_value=False)

        db = MagicMock()
        db.get_runtime_connection.return_value = conn

        run = load_run(db, "run-001")
        assert run.run_id == "run-001"
        assert run.phase == RunPhase.WAITING_FOR_DATA

    def test_load_missing_run_raises(self):
        cursor = MagicMock()
        cursor.fetchone.return_value = None
        conn = MagicMock()
        conn.cursor.return_value = cursor
        conn.__enter__ = MagicMock(return_value=conn)
        conn.__exit__ = MagicMock(return_value=False)

        db = MagicMock()
        db.get_runtime_connection.return_value = conn

        with pytest.raises(EngineRunStateError, match="not found"):
            load_run(db, "nonexistent")


# ---------------------------------------------------------------------------
# get_or_create_run tests (DB mocked)
# ---------------------------------------------------------------------------


class TestGetOrCreateRun:
    """Tests for get_or_create_run with mocked DB.

    The implementation now uses a single ``INSERT ... ON CONFLICT DO
    UPDATE ... RETURNING *`` upsert. The old SELECT-then-INSERT pattern
    was vulnerable to two daemon threads racing on the same
    (as_of_date, region) tuple.
    """

    def test_returns_existing_run(self):
        # When the row already exists, the upsert collapses to a no-op
        # update and returns the existing row in one round trip.
        row = _make_row()
        cursor = MagicMock()
        cursor.fetchone.return_value = row
        conn = MagicMock()
        conn.cursor.return_value = cursor
        conn.__enter__ = MagicMock(return_value=conn)
        conn.__exit__ = MagicMock(return_value=False)

        db = MagicMock()
        db.get_runtime_connection.return_value = conn

        run = get_or_create_run(db, date(2025, 6, 30), "US_EQ")
        assert run.run_id == "run-001"
        # One atomic upsert + commit — no separate SELECT round trip.
        assert cursor.execute.call_count == 1
        conn.commit.assert_called_once()

    def test_creates_new_run_when_none_exists(self):
        row = _make_row(run_id="new-run-id")
        cursor = MagicMock()
        cursor.fetchone.return_value = row
        conn = MagicMock()
        conn.cursor.return_value = cursor
        conn.__enter__ = MagicMock(return_value=conn)
        conn.__exit__ = MagicMock(return_value=False)

        db = MagicMock()
        db.get_runtime_connection.return_value = conn

        with patch("prometheus.pipeline.state.generate_uuid", return_value="new-run-id"):
            run = get_or_create_run(db, date(2025, 6, 30), "US_EQ")

        assert run.run_id == "new-run-id"
        assert run.phase == RunPhase.WAITING_FOR_DATA
        # Single atomic upsert (no SELECT-then-INSERT).
        assert cursor.execute.call_count == 1
        conn.commit.assert_called_once()


# ---------------------------------------------------------------------------
# EngineRun dataclass tests
# ---------------------------------------------------------------------------


class TestEngineRun:
    """Tests for EngineRun dataclass."""

    def test_frozen_dataclass(self):
        run = _make_run()
        with pytest.raises(AttributeError):
            run.phase = RunPhase.DATA_READY  # type: ignore[misc]

    def test_fields_accessible(self):
        run = _make_run(RunPhase.SIGNALS_DONE)
        assert run.run_id == "run-001"
        assert run.phase == RunPhase.SIGNALS_DONE
        assert run.as_of_date == date(2025, 6, 30)
        assert run.region == "US_EQ"
        assert run.error == {}
        assert run.phase_completed_at is None
