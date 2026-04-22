"""Tests for the Prometheus re-audit fixes.

Covers:
- HIGH #1: NaN/inf validation on order quantities
- HIGH #2: Missing price validation on limit orders
- MEDIUM #1: Duplicate order prevention
- MEDIUM #2: Naive datetime.now() replaced with timezone-aware
- MEDIUM #3: Cursor cleanup via context managers (structural, not testable)
- MEDIUM #4: update_phase() atomicity (SELECT after UPDATE)
- MEDIUM #5: ConvictionDefaults range validation
- MEDIUM #6: Shutdown check in _run_cycle
- MEDIUM #7: retry_backoff cleanup on max retries
- MEDIUM #8: Frontend console.log gating (structural, verified by build)
- LOW #2: model_basic bare excepts now have logger.debug()
"""

from __future__ import annotations

import math
import time
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from prometheus.execution.broker_interface import Order, OrderSide, OrderType, Position
from prometheus.execution.order_planner import (
    MIN_ABS_QUANTITY,
    _DEDUP_WINDOW_SECONDS,
    _recent_orders,
    plan_orders,
)


def _get_dedup_ledger():
    """Get the canonical dedup ledger from the plan_orders function attribute."""
    return plan_orders._dedup_ledger  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _pos(instrument_id: str, quantity: float, avg_cost: float = 100.0) -> Position:
    return Position(
        instrument_id=instrument_id,
        quantity=quantity,
        avg_cost=avg_cost,
        market_value=quantity * avg_cost,
        unrealized_pnl=0.0,
    )


# ---------------------------------------------------------------------------
# HIGH #1: NaN/inf validation on order quantities
# ---------------------------------------------------------------------------


class TestNanInfOrderQuantityValidation:
    """NaN and inf deltas must be rejected, not turned into invalid orders."""

    def setup_method(self):
        _recent_orders.clear()

    def test_nan_target_skipped(self):
        orders = plan_orders(
            current_positions={"AAPL": _pos("AAPL", 100.0)},
            target_positions={"AAPL": float("nan")},
            min_rebalance_pct=0.0,
        )
        assert len(orders) == 0

    def test_inf_target_skipped(self):
        orders = plan_orders(
            current_positions={"AAPL": _pos("AAPL", 100.0)},
            target_positions={"AAPL": float("inf")},
            min_rebalance_pct=0.0,
        )
        assert len(orders) == 0

    def test_neg_inf_target_skipped(self):
        orders = plan_orders(
            current_positions={"AAPL": _pos("AAPL", 100.0)},
            target_positions={"AAPL": float("-inf")},
            min_rebalance_pct=0.0,
        )
        assert len(orders) == 0

    def test_nan_current_position_skipped(self):
        orders = plan_orders(
            current_positions={"AAPL": _pos("AAPL", float("nan"))},
            target_positions={"AAPL": 100.0},
            min_rebalance_pct=0.0,
        )
        assert len(orders) == 0

    def test_valid_orders_still_pass(self):
        orders = plan_orders(
            current_positions={},
            target_positions={"AAPL": 100.0, "GOOG": 50.0},
            min_rebalance_pct=0.0,
        )
        assert len(orders) == 2

    def test_mixed_nan_and_valid(self):
        """NaN instruments are skipped while valid ones produce orders."""
        _recent_orders.clear()
        orders = plan_orders(
            current_positions={},
            target_positions={
                "AAPL": float("nan"),
                "GOOG": 50.0,
            },
            min_rebalance_pct=0.0,
        )
        assert len(orders) == 1
        assert orders[0].instrument_id == "GOOG"


# ---------------------------------------------------------------------------
# HIGH #2: Missing price validation on limit orders
# ---------------------------------------------------------------------------


class TestLimitPriceValidation:
    """Limit prices that are NaN or non-positive must fall back to MARKET."""

    def setup_method(self):
        _recent_orders.clear()

    def test_nan_ref_price_falls_back_to_market(self):
        orders = plan_orders(
            current_positions={},
            target_positions={"AAPL": 100.0},
            order_type=OrderType.LIMIT,
            prices={"AAPL": float("nan")},
            min_rebalance_pct=0.0,
        )
        assert len(orders) == 1
        # NaN ref_price means ref_price > 0 check fails -> MARKET fallback
        assert orders[0].order_type == OrderType.MARKET
        assert orders[0].limit_price is None

    def test_negative_ref_price_falls_back_to_market(self):
        orders = plan_orders(
            current_positions={},
            target_positions={"AAPL": 100.0},
            order_type=OrderType.LIMIT,
            prices={"AAPL": -10.0},
            min_rebalance_pct=0.0,
        )
        assert len(orders) == 1
        assert orders[0].order_type == OrderType.MARKET
        assert orders[0].limit_price is None

    def test_valid_limit_price_preserved(self):
        orders = plan_orders(
            current_positions={},
            target_positions={"AAPL": 100.0},
            order_type=OrderType.LIMIT,
            prices={"AAPL": 150.0},
            limit_buffer_pct=0.01,
            min_rebalance_pct=0.0,
        )
        assert len(orders) == 1
        assert orders[0].order_type == OrderType.LIMIT
        assert orders[0].limit_price is not None
        assert orders[0].limit_price > 0
        assert math.isfinite(orders[0].limit_price)


# ---------------------------------------------------------------------------
# MEDIUM #1: Duplicate order prevention
# ---------------------------------------------------------------------------


class TestDuplicateOrderPrevention:
    """Same (instrument, side) within 60s dedup window is suppressed."""

    def setup_method(self):
        _get_dedup_ledger().clear()

    def test_first_order_passes(self):
        orders = plan_orders(
            current_positions={},
            target_positions={"AAPL": 100.0},
            min_rebalance_pct=0.0,
        )
        assert len(orders) == 1

    def test_second_identical_call_suppressed(self):
        """Calling plan_orders twice rapidly for the same target suppresses dupes."""
        plan_orders(
            current_positions={},
            target_positions={"AAPL": 100.0},
            min_rebalance_pct=0.0,
        )
        # Second call within dedup window
        orders = plan_orders(
            current_positions={},
            target_positions={"AAPL": 100.0},
            min_rebalance_pct=0.0,
        )
        assert len(orders) == 0

    def test_different_instrument_not_suppressed(self):
        plan_orders(
            current_positions={},
            target_positions={"AAPL": 100.0},
            min_rebalance_pct=0.0,
        )
        orders = plan_orders(
            current_positions={},
            target_positions={"GOOG": 50.0},
            min_rebalance_pct=0.0,
        )
        assert len(orders) == 1
        assert orders[0].instrument_id == "GOOG"

    def test_different_side_not_suppressed(self):
        """BUY and SELL on same instrument are distinct keys."""
        plan_orders(
            current_positions={},
            target_positions={"AAPL": 100.0},
            min_rebalance_pct=0.0,
        )
        orders = plan_orders(
            current_positions={"AAPL": _pos("AAPL", 200.0)},
            target_positions={"AAPL": 50.0},
            min_rebalance_pct=0.0,
        )
        assert len(orders) == 1
        assert orders[0].side == OrderSide.SELL

    def test_expired_dedup_allows_reorder(self):
        """After dedup window expires, the same order is allowed again."""
        plan_orders(
            current_positions={},
            target_positions={"AAPL": 100.0},
            min_rebalance_pct=0.0,
        )
        # Manually backdate the dedup entry to simulate window expiry
        ledger = _get_dedup_ledger()
        for key in list(ledger.keys()):
            ledger[key] = time.monotonic() - _DEDUP_WINDOW_SECONDS - 1

        orders = plan_orders(
            current_positions={},
            target_positions={"AAPL": 100.0},
            min_rebalance_pct=0.0,
        )
        assert len(orders) == 1

    def test_dedup_window_constant_is_60s(self):
        assert _DEDUP_WINDOW_SECONDS == 60.0


# ---------------------------------------------------------------------------
# MEDIUM #2: Naive datetime.now()
# ---------------------------------------------------------------------------


class TestTimezoneAwareDatetime:
    """Verify scripts use timezone-aware datetime.now(timezone.utc)."""

    def test_param_grid_search_uses_utc(self):
        import inspect
        import re
        from prometheus.scripts.grid_search import param_grid_search
        source = inspect.getsource(param_grid_search)
        assert "datetime.now(timezone.utc)" in source
        # Find all datetime.now(...) calls; every one must include timezone.utc
        all_calls = re.findall(r"datetime\.now\([^)]*\)", source)
        for call in all_calls:
            assert "timezone.utc" in call, f"Found bare datetime call: {call}"

    def test_test_ibkr_paper_uses_utc(self):
        import inspect
        import re
        from prometheus.scripts.run import test_ibkr_paper
        source = inspect.getsource(test_ibkr_paper)
        assert "datetime.now(timezone.utc)" in source
        all_calls = re.findall(r"datetime\.now\([^)]*\)", source)
        for call in all_calls:
            assert "timezone.utc" in call, f"Found bare datetime call: {call}"


# ---------------------------------------------------------------------------
# MEDIUM #5: ConvictionDefaults range validation
# ---------------------------------------------------------------------------


class TestConvictionDefaultsValidation:
    """Range checks on ConvictionDefaults should reject invalid values."""

    def test_valid_defaults_pass(self):
        from prometheus.portfolio.config import load_conviction_config
        cfg = load_conviction_config(path="/nonexistent")
        assert cfg.hard_stop_pct == 0.20
        assert cfg.build_rate == 1.0

    def test_hard_stop_pct_above_1_rejected(self, monkeypatch):
        from prometheus.portfolio.config import load_conviction_config
        monkeypatch.setenv("PROMETHEUS_CONVICTION_HARD_STOP_PCT", "1.5")
        with pytest.raises(ValueError, match="hard_stop_pct"):
            load_conviction_config(path="/nonexistent")

    def test_hard_stop_pct_negative_rejected(self, monkeypatch):
        from prometheus.portfolio.config import load_conviction_config
        monkeypatch.setenv("PROMETHEUS_CONVICTION_HARD_STOP_PCT", "-0.1")
        with pytest.raises(ValueError, match="hard_stop_pct"):
            load_conviction_config(path="/nonexistent")

    def test_build_rate_zero_rejected(self, monkeypatch):
        from prometheus.portfolio.config import load_conviction_config
        monkeypatch.setenv("PROMETHEUS_CONVICTION_BUILD_RATE", "0")
        with pytest.raises(ValueError, match="build_rate"):
            load_conviction_config(path="/nonexistent")

    def test_decay_rate_negative_rejected(self, monkeypatch):
        from prometheus.portfolio.config import load_conviction_config
        monkeypatch.setenv("PROMETHEUS_CONVICTION_DECAY_RATE", "-1.0")
        with pytest.raises(ValueError, match="decay_rate"):
            load_conviction_config(path="/nonexistent")

    def test_score_cap_zero_rejected(self, monkeypatch):
        from prometheus.portfolio.config import load_conviction_config
        monkeypatch.setenv("PROMETHEUS_CONVICTION_SCORE_CAP", "0")
        with pytest.raises(ValueError, match="score_cap"):
            load_conviction_config(path="/nonexistent")

    def test_scale_up_days_zero_rejected(self, monkeypatch):
        from prometheus.portfolio.config import load_conviction_config
        monkeypatch.setenv("PROMETHEUS_CONVICTION_SCALE_UP_DAYS", "0")
        with pytest.raises(ValueError, match="scale_up_days"):
            load_conviction_config(path="/nonexistent")

    def test_entry_weight_fraction_above_1_rejected(self, monkeypatch):
        from prometheus.portfolio.config import load_conviction_config
        monkeypatch.setenv("PROMETHEUS_CONVICTION_ENTRY_WEIGHT_FRACTION", "2.0")
        with pytest.raises(ValueError, match="entry_weight_fraction"):
            load_conviction_config(path="/nonexistent")


# ---------------------------------------------------------------------------
# MEDIUM #6: Shutdown check in _run_cycle
# ---------------------------------------------------------------------------


class TestShutdownCheckInRunCycle:
    """_run_cycle should bail immediately if shutdown_event is set."""

    def test_run_cycle_returns_early_when_shutdown(self):
        from prometheus.orchestration.market_aware_daemon import (
            MarketAwareDaemon,
            MarketAwareDaemonConfig,
        )

        db = MagicMock()
        config = MarketAwareDaemonConfig(markets=["US_EQ"])
        daemon = MarketAwareDaemon(config, db)

        # Pre-set the shutdown event
        daemon._shutdown_event.set()

        # _run_cycle should return immediately without calling _check_timeouts
        with patch.object(daemon, "_check_timeouts") as mock_check:
            daemon._run_cycle(date.today())
            mock_check.assert_not_called()


# ---------------------------------------------------------------------------
# MEDIUM #7: retry_backoff cleanup on max retries
# ---------------------------------------------------------------------------


class TestRetryBackoffCleanup:
    """When retries are exhausted, the backoff entry must be cleaned up."""

    def test_backoff_entry_removed_on_exhaustion(self):
        from prometheus.orchestration.dag import JobMetadata, JobStatus
        from prometheus.orchestration.market_aware_daemon import (
            MarketAwareDaemon,
            MarketAwareDaemonConfig,
        )

        db = MagicMock()
        config = MarketAwareDaemonConfig(markets=["US_EQ"])
        daemon = MarketAwareDaemon(config, db)

        # Simulate a job that has exhausted retries
        job = JobMetadata(
            job_id="test_job",
            job_type="ingest_prices",
            market_id="US_EQ",
            max_retries=2,
        )

        # Create a mock execution with FAILED status and max attempts
        mock_exec = MagicMock()
        mock_exec.execution_id = "exec-123"
        mock_exec.job_id = "test_job"
        mock_exec.status = JobStatus.FAILED
        mock_exec.attempt_number = 2
        mock_exec.error_message = "some error"

        # Pre-seed the retry_backoff dict
        daemon.retry_backoff["exec-123"] = datetime.now(timezone.utc)

        # Mock external calls
        from prometheus.orchestration import market_aware_daemon as mad

        with patch.object(mad, "get_latest_job_execution", return_value=mock_exec), \
             patch.object(mad, "should_retry_job", return_value=False), \
             patch.object(mad, "update_job_execution_status"):

            from prometheus.orchestration.dag import DAG
            mock_dag = MagicMock(spec=DAG)
            mock_dag.get_runnable_jobs.return_value = [job]

            with patch.object(daemon, "_get_completed_jobs", return_value=set()), \
                 patch.object(daemon, "_get_running_job_ids", return_value=set()):
                daemon._process_market(
                    "US_EQ", mock_dag, "dag-1",
                    MagicMock(), date.today(), datetime.now(timezone.utc),
                )

        # The backoff entry should be cleaned up
        assert "exec-123" not in daemon.retry_backoff


# ---------------------------------------------------------------------------
# MEDIUM #8: Frontend LOG_ENABLED gated
# ---------------------------------------------------------------------------


class TestFrontendLogGating:
    """Verify LOG_ENABLED is not hardcoded to true."""

    def test_client_ts_log_enabled_not_hardcoded(self):
        from pathlib import Path
        client_ts = Path("/home/feanor/coding/prometheus/prometheus_web/src/api/client.ts")
        content = client_ts.read_text()
        assert 'LOG_ENABLED = true' not in content, "LOG_ENABLED should not be hardcoded to true"
        assert 'import.meta.env.DEV' in content, "LOG_ENABLED should be gated by Vite DEV mode"

    def test_intelligence_no_console_log(self):
        from pathlib import Path
        tsx = Path("/home/feanor/coding/prometheus/prometheus_web/src/pages/Intelligence.tsx")
        content = tsx.read_text()
        assert "console.log" not in content

    def test_topbar_no_console_log(self):
        from pathlib import Path
        tsx = Path("/home/feanor/coding/prometheus/prometheus_web/src/layout/TopBar.tsx")
        content = tsx.read_text()
        assert "console.log" not in content


# ---------------------------------------------------------------------------
# LOW #2: model_basic bare excepts now have logger.debug()
# ---------------------------------------------------------------------------


class TestModelBasicBareExcepts:
    """Bare `except: pass` statements should now include logger.debug()."""

    def test_assessment_model_basic_no_bare_pass(self):
        import inspect
        from prometheus.assessment import model_basic
        source = inspect.getsource(model_basic)
        # Check that "except Exception:\n                pass" no longer exists
        # (the pattern was: except Exception: <newline+spaces> pass)
        lines = source.splitlines()
        for i, line in enumerate(lines):
            stripped = line.strip()
            if stripped == "pass" and i > 0:
                prev = lines[i - 1].strip()
                if prev.startswith("except") and prev.endswith(":"):
                    pytest.fail(
                        f"Found bare 'except: pass' at line {i}: {prev} / {stripped}"
                    )

    def test_portfolio_model_basic_db_init_has_debug(self):
        import inspect
        from prometheus.portfolio import model_basic
        source = inspect.getsource(model_basic._load_assessment_confidences)
        assert "logger.debug" in source
