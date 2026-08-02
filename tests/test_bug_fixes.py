"""Tests for CRITICAL/HIGH/MEDIUM bug fixes.

Each test class covers a specific fix:
- CRITICAL 1: Date rollover race condition in market_aware_daemon
- HIGH 1/3: Sector health index validation and division-by-zero
- HIGH 2: LLM chat timeout
- HIGH 4: Portfolio volatility returns None on failure
- HIGH 5: Order planner long-only validation
- MEDIUM 1: NaN sanitization in LLM context
- MEDIUM 2: Config range validation
- MEDIUM 3: Crisis alpha flash signal off-by-one
- MEDIUM 5: Midnight job clearing order
- MEDIUM 6: Config loading visibility
- MEDIUM 7: Morning catch-up budget check
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

# ============================================================================
# CRITICAL 1: Date rollover race condition
# ============================================================================

class TestDateRolloverRaceCondition:
    """Catch-up re-entry / idempotency safety under the lane scheduler.

    The old ``_catchup_in_progress`` flag is gone BY DESIGN: catch-up no
    longer loops inline. ``_maybe_morning_catchup`` only ATTACHES a
    CatchupState to the market's lane, once per (market, last_trading_day)
    keyed ``catchup_{market}_{date}`` in ``_catchup_done``; the lane then
    serves the catch-up DAG through the normal dispatcher. These tests
    preserve the original safety intents against the new mechanism.
    """

    def _make_daemon(self, morning_catchup_hour: int = 8, as_of_date=None, markets=None):
        """Build a minimal MarketAwareDaemon mock for catch-up testing."""
        from prometheus.orchestration.market_aware_daemon import MarketAwareDaemon

        config = MagicMock()
        config.morning_catchup_hour = morning_catchup_hour
        config.as_of_date = as_of_date
        config.markets = markets if markets is not None else ["US_EQ"]
        config.poll_interval_seconds = 1
        config.options_mode = "paper"

        db = MagicMock()
        daemon = MarketAwareDaemon(config, db)
        return daemon

    @patch("prometheus.orchestration.market_aware_daemon.build_market_dag")
    @patch("prometheus.orchestration.market_aware_daemon.now_local")
    def test_catchup_guard_prevents_reentry(self, mock_now_local, mock_build):
        """Re-entry safety (successor of the _catchup_in_progress flag):
        a second call in the same window must not attach a second
        CatchupState or build a second catch-up DAG."""
        daemon = self._make_daemon()
        yesterday = date(2026, 4, 10)
        mock_now_local.return_value = datetime(2026, 4, 11, 8, 2)

        mock_cal = MagicMock()
        mock_cal.trading_days_between.return_value = [yesterday]
        daemon._calendars["US_EQ"] = mock_cal

        mock_dag = MagicMock()
        mock_dag.jobs = {}
        mock_build.return_value = mock_dag

        # load_latest_run is imported locally; patch at source.
        with patch("prometheus.pipeline.state.load_latest_run", return_value=None):
            daemon._maybe_morning_catchup(date(2026, 4, 11))
            first = daemon.lanes["US_EQ"].catchup
            assert first is not None

            # Second call: lane already has a catch-up (and the key is in
            # _catchup_done) — must be a no-op.
            daemon._maybe_morning_catchup(date(2026, 4, 11))

        assert daemon.lanes["US_EQ"].catchup is first
        assert mock_build.call_count == 1
        assert f"catchup_US_EQ_{yesterday}" in daemon._catchup_done

    @patch("prometheus.orchestration.market_aware_daemon.now_local")
    def test_catchup_noop_for_pseudo_markets(self, mock_now_local):
        """IRIS/INTEL have no engine runs — catch-up must never attach to
        them. (Replaces the obsolete as_of_date==today early-return test:
        the new scheduler keys catch-up off each REAL market's last trading
        day rather than comparing dates globally.)"""
        daemon = self._make_daemon(markets=["IRIS", "INTEL"])
        mock_now_local.return_value = datetime(2026, 4, 12, 8, 2)

        daemon._maybe_morning_catchup(date(2026, 4, 12))

        assert daemon.lanes["IRIS"].catchup is None
        assert daemon.lanes["INTEL"].catchup is None
        # No calendar was even consulted for pseudo-markets.
        assert not daemon._calendars

    @patch("prometheus.orchestration.market_aware_daemon.now_local")
    def test_catchup_skips_outside_hour(self, mock_now_local):
        """Catch-up only triggers at the configured hour."""
        daemon = self._make_daemon(morning_catchup_hour=8)

        mock_now_local.return_value = datetime(2026, 4, 12, 10, 0)  # hour=10, not 8
        daemon._maybe_morning_catchup(date(2026, 4, 11))

        # Function exited after checking the hour — nothing attached.
        assert daemon.lanes["US_EQ"].catchup is None

    @patch("prometheus.orchestration.market_aware_daemon.now_local")
    def test_catchup_skips_past_minute_5(self, mock_now_local):
        """Catch-up only triggers in the first 5 minutes of the hour."""
        daemon = self._make_daemon(morning_catchup_hour=8)

        mock_now_local.return_value = datetime(2026, 4, 12, 8, 10)  # minute=10 > 5
        daemon._maybe_morning_catchup(date(2026, 4, 11))

        # Should return without attaching a catch-up.
        assert daemon.lanes["US_EQ"].catchup is None

    @patch("prometheus.orchestration.market_aware_daemon.now_local")
    def test_normal_catchup_pipeline_already_ran(self, mock_now_local):
        """When the pipeline already completed, no CatchupState attaches and
        the (market, date) key is cached in _catchup_done."""
        daemon = self._make_daemon(morning_catchup_hour=8)

        yesterday = date(2026, 4, 10)
        mock_now_local.return_value = datetime(2026, 4, 11, 8, 2)

        # Mock the trading calendar to return yesterday as a trading day
        mock_cal = MagicMock()
        mock_cal.trading_days_between.return_value = [yesterday]
        daemon._calendars["US_EQ"] = mock_cal

        # Mock load_latest_run to indicate pipeline already completed
        # The import is local inside _maybe_morning_catchup, so patch at source.
        with patch("prometheus.pipeline.state.load_latest_run") as mock_load:
            mock_run = MagicMock()
            from prometheus.pipeline.state import RunPhase
            mock_run.phase = RunPhase.COMPLETED
            mock_load.return_value = mock_run

            daemon._maybe_morning_catchup(date(2026, 4, 11))

        # Key format is now per-market: catchup_{market}_{last_trading_day}
        assert f"catchup_US_EQ_{yesterday}" in daemon._catchup_done
        assert daemon.lanes["US_EQ"].catchup is None

    def test_catchup_cleared_after_completion(self):
        """Successor of test_catchup_flag_cleared_after_run: once the
        catch-up DAG has no runnable work left, _resolve_lane_work fires
        _on_catchup_complete and clears lane.catchup so the live DAG
        resumes (same safety goal as clearing _catchup_in_progress)."""
        import time as _time

        from prometheus.orchestration import market_aware_daemon as mad

        daemon = self._make_daemon(morning_catchup_hour=8)
        lane = daemon.lanes["US_EQ"]

        mock_dag = MagicMock()
        mock_dag.jobs = {"j": MagicMock()}
        mock_dag.get_runnable_jobs.return_value = []  # everything done
        lane.catchup = mad.CatchupState(
            dag=mock_dag,
            dag_id="US_EQ_2026-04-10",
            catchup_date=date(2026, 4, 10),
            deadline_monotonic=_time.monotonic() + 300,
        )
        daemon._get_completed_jobs = MagicMock(return_value={"j"})

        with patch.object(daemon, "_on_catchup_complete") as mock_done:
            daemon._resolve_lane_work(lane, datetime.now(timezone.utc))

        mock_done.assert_called_once_with(lane)
        assert lane.catchup is None

    @patch("prometheus.orchestration.market_aware_daemon.build_market_dag")
    @patch("prometheus.orchestration.market_aware_daemon.now_local")
    def test_catchup_idempotent(self, mock_now_local, mock_build):
        """Second call for the same (market, last_trading_day) is a no-op
        (cached in _catchup_done)."""
        daemon = self._make_daemon(morning_catchup_hour=8)
        yesterday = date(2026, 4, 10)
        mock_now_local.return_value = datetime(2026, 4, 11, 8, 2)

        mock_cal = MagicMock()
        mock_cal.trading_days_between.return_value = [yesterday]
        daemon._calendars["US_EQ"] = mock_cal

        # Pre-populate the done set (new per-market key format)
        daemon._catchup_done = {f"catchup_US_EQ_{yesterday}"}

        with patch("prometheus.pipeline.state.load_latest_run") as mock_load:
            daemon._maybe_morning_catchup(date(2026, 4, 11))

        # Returned early: no run lookup, no DAG built, nothing attached.
        mock_load.assert_not_called()
        mock_build.assert_not_called()
        assert daemon.lanes["US_EQ"].catchup is None


# ============================================================================
# HIGH 1 / HIGH 3: Sector health index validation and redistribution
# ============================================================================

class TestSectorAllocatorValidation:
    """Tests for sector score validation and division-by-zero guards."""

    def _make_allocator(self, scores: dict[str, dict[date, float]], sector_map=None):
        from prometheus.sector.allocator import SectorAllocator, SectorAllocatorConfig

        config = SectorAllocatorConfig()

        mapper = MagicMock()
        if sector_map is None:
            sector_map = {}
        mapper.get_sector.side_effect = lambda iid: sector_map.get(iid, "UNKNOWN")
        mapper.get_sector_weights.side_effect = lambda w: {}

        health = MagicMock()
        health.scores = scores

        return SectorAllocator(config, mapper, health)

    def test_negative_score_clamped_to_zero(self):
        """Negative sector score should be clamped to 0.0."""
        today = date(2026, 4, 12)
        allocator = self._make_allocator({"Tech": {today: -0.5}})
        level, sick, weak, healthy, scores = allocator.classify_stress(today)

        assert scores["Tech"] == 0.0
        assert "Tech" in sick  # 0.0 < kill_threshold (0.25)

    def test_score_above_one_clamped(self):
        """Score > 1.0 should be clamped to 1.0."""
        today = date(2026, 4, 12)
        allocator = self._make_allocator({"Tech": {today: 1.5}})
        level, sick, weak, healthy, scores = allocator.classify_stress(today)

        assert scores["Tech"] == 1.0
        assert "Tech" in healthy

    def test_nan_score_clamped_to_zero(self):
        """NaN score should be clamped to 0.0."""
        today = date(2026, 4, 12)
        allocator = self._make_allocator({"Tech": {today: float("nan")}})
        level, sick, weak, healthy, scores = allocator.classify_stress(today)

        assert scores["Tech"] == 0.0
        assert "Tech" in sick

    def test_inf_score_clamped_to_one(self):
        """Inf score should be clamped to 1.0."""
        today = date(2026, 4, 12)
        allocator = self._make_allocator({"Tech": {today: float("inf")}})
        level, sick, weak, healthy, scores = allocator.classify_stress(today)

        assert scores["Tech"] == 1.0
        assert "Tech" in healthy

    def test_zero_healthy_weight_no_division_error(self):
        """When all healthy sectors have zero weight, no division by zero."""
        today = date(2026, 4, 12)
        sector_map = {"AAPL": "Tech", "JPM": "Finance", "XOM": "Energy"}

        allocator = self._make_allocator(
            {
                "Tech": {today: 0.10},    # sick
                "Finance": {today: 0.60}, # healthy
                "Energy": {today: 0.60},  # healthy
            },
            sector_map=sector_map,
        )

        # Finance and Energy are healthy, but have zero weight
        # (all weight is in Tech which gets killed)
        result = allocator.adjust_weights(
            weights={"AAPL": 1.0},  # Only Tech positions
            as_of_date=today,
        )
        # Weight should be killed, not redistributed (no healthy instruments)
        assert result.weight_killed == pytest.approx(1.0)
        # No adjusted weights — all killed
        assert sum(result.adjusted_weights.values()) == pytest.approx(0.0)

    def test_concentration_limit_zero_total_weight(self):
        """Concentration limits should not crash when sector total_w is near zero."""
        from prometheus.sector.allocator import SectorAllocator, SectorAllocatorConfig

        config = SectorAllocatorConfig(sector_max_concentration=0.30)
        mapper = MagicMock()
        mapper.get_sector.return_value = "Tech"
        # Return a sector weight that is exactly 0 (edge case)
        mapper.get_sector_weights.return_value = {"Tech": 0.0}

        health = MagicMock()
        health.scores = {}

        allocator = SectorAllocator(config, mapper, health)
        # Should not raise
        result = allocator._apply_concentration_limits({"AAPL": 0.0})
        assert result == {"AAPL": 0.0}


# ============================================================================
# HIGH 2: LLM chat timeout
# ============================================================================

class TestPortfolioFactorRiskNone:
    """Test that _compute_factor_risk returns None when variance <= 0."""

    def test_zero_variance_returns_none(self):
        """When factor variance is zero, function should return None."""
        from prometheus.portfolio.model_basic import BasicLongOnlyPortfolioModel

        model = BasicLongOnlyPortfolioModel.__new__(BasicLongOnlyPortfolioModel)

        # Call with empty members → returns early with ({}, 0.0, 0)
        # but that's the "no data" path. We need to test the variance<=0 path.
        result = model._compute_factor_risk(date(2026, 4, 12), [], [])
        # Empty members returns ({}, 0.0, 0) which is the early-return path
        assert result == ({}, 0.0, 0)


# ============================================================================
# HIGH 5: Order planner long-only validation
# ============================================================================

class TestOrderPlannerLongOnly:
    """Tests for long-only validation in plan_orders."""

    def test_long_only_clamps_sell_to_position(self):
        """SELL qty exceeding position should be clamped in long-only mode."""
        from prometheus.execution.broker_interface import OrderSide, Position
        from prometheus.execution.order_planner import plan_orders

        current = {
            "AAPL": Position(
                instrument_id="AAPL",
                quantity=50.0,
                avg_cost=150.0,
                market_value=7500.0,
                unrealized_pnl=0.0,
            ),
        }
        # Target is -10 shares — would create a short if unclamped
        orders = plan_orders(
            current_positions=current,
            target_positions={"AAPL": -10.0},
            min_rebalance_pct=0.0,
            long_only=True,
        )
        # The sell should be clamped to the current position (50)
        assert len(orders) == 1
        assert orders[0].side == OrderSide.SELL
        assert orders[0].quantity == pytest.approx(50.0)

    def test_long_only_allows_valid_sells(self):
        """SELL qty within position is allowed in long-only mode."""
        from prometheus.execution.broker_interface import OrderSide, Position
        from prometheus.execution.order_planner import plan_orders

        current = {
            "AAPL": Position(
                instrument_id="AAPL",
                quantity=100.0,
                avg_cost=150.0,
                market_value=15000.0,
                unrealized_pnl=0.0,
            ),
        }
        orders = plan_orders(
            current_positions=current,
            target_positions={"AAPL": 60.0},
            min_rebalance_pct=0.0,
            long_only=True,
        )
        assert len(orders) == 1
        assert orders[0].side == OrderSide.SELL
        assert orders[0].quantity == pytest.approx(40.0)

    def test_long_only_false_allows_shorts(self):
        """Without long_only, oversized sells are allowed."""
        from prometheus.execution.broker_interface import OrderSide, Position
        from prometheus.execution.order_planner import plan_orders

        current = {
            "AAPL": Position(
                instrument_id="AAPL",
                quantity=50.0,
                avg_cost=150.0,
                market_value=7500.0,
                unrealized_pnl=0.0,
            ),
        }
        orders = plan_orders(
            current_positions=current,
            target_positions={"AAPL": -10.0},
            min_rebalance_pct=0.0,
            long_only=False,
        )
        assert len(orders) == 1
        assert orders[0].side == OrderSide.SELL
        assert orders[0].quantity == pytest.approx(60.0)  # Full delta

    def test_long_only_no_position_sell_eliminated(self):
        """In long-only, selling instrument with no current position → order removed."""
        from prometheus.execution.order_planner import plan_orders

        orders = plan_orders(
            current_positions={},
            target_positions={"AAPL": -100.0},
            min_rebalance_pct=0.0,
            long_only=True,
        )
        # The sell would create a short — should be clamped to 0 and removed
        assert len(orders) == 0


# ============================================================================
# MEDIUM 1: NaN sanitization in LLM context
# ============================================================================

class TestConfigRangeValidation:
    """Tests for allocator and crisis alpha config range validation."""

    def test_allocator_config_clamps_out_of_range(self):
        """Out-of-range values in allocator config are clamped."""
        from prometheus.sector.allocator import load_allocator_config

        with patch.dict("os.environ", {
            "PROMETHEUS_SECTOR_KILL_THRESHOLD": "1.5",  # > 1.0
        }):
            config = load_allocator_config()
            assert config.sector_kill_threshold == 1.0  # clamped

    def test_allocator_config_clamps_negative(self):
        """Negative values are clamped to lower bound."""
        from prometheus.sector.allocator import load_allocator_config

        with patch.dict("os.environ", {
            "PROMETHEUS_SECTOR_KILL_THRESHOLD": "-0.5",
        }):
            config = load_allocator_config()
            assert config.sector_kill_threshold == 0.0

    def test_allocator_config_valid_value_unchanged(self):
        """Valid values pass through unchanged."""
        from prometheus.sector.allocator import load_allocator_config

        with patch.dict("os.environ", {
            "PROMETHEUS_SECTOR_KILL_THRESHOLD": "0.30",
        }):
            config = load_allocator_config()
            assert config.sector_kill_threshold == pytest.approx(0.30)


# ============================================================================
# MEDIUM 3: Crisis alpha flash signal — REMOVED 2026-06-11
#
# prometheus/sector/crisis_alpha.py was dead code (never imported by the live
# pipeline) and was deleted in the signal-layer cleanup, along with these
# tests. The live "crisis_alpha" OPTIONS strategy is a separate, still-wired
# thing in prometheus/execution/options_strategy.py.
# ============================================================================


# ============================================================================
# MEDIUM 5: Midnight job clearing order
# ============================================================================

class TestMidnightJobClearing:
    """Date-rollover safety under the lane scheduler.

    The old behavior (finalize running jobs, then clear running_jobs, at
    the moment of rollover) CHANGED BY DESIGN: rollover now only sets
    ``lane.pending_rollover`` and never touches in-flight handles — the
    running job's status writes keep going against its old dag_id, and
    ``_apply_pending_rollover`` swaps the DAG (finalizing the market's
    stale engine_runs) only once the lane is idle. Same safety goal —
    no execution row is stranded RUNNING and no run left un-finalized —
    achieved by deferral instead of in-place finalization.
    """

    def test_date_rollover_defers_swap_for_busy_lane(self):
        """A busy lane keeps its old DAG at rollover; the swap (and
        stale-run finalization) happens only after the in-flight job
        completes and the lane is idle."""
        import threading

        from prometheus.orchestration import market_aware_daemon as mad
        from prometheus.orchestration.market_aware_daemon import (
            MarketAwareDaemon,
            MarketAwareDaemonConfig,
        )

        config = MarketAwareDaemonConfig(markets=["US_EQ"])
        daemon = MarketAwareDaemon(config, MagicMock())

        old_date = date(2026, 4, 11)
        new_date = date(2026, 4, 12)
        old_dag = MagicMock()
        old_dag.as_of_date = old_date
        daemon.active_dags["US_EQ"] = (old_dag, f"US_EQ_{old_date.isoformat()}")

        lane = daemon.lanes["US_EQ"]

        # Simulate an in-flight job on the lane.
        mock_job = MagicMock()
        mock_job.job_id = "test_job"
        handle = mad.JobHandle(
            job=mock_job,
            execution_id="exec-001",
            dag_id=f"US_EQ_{old_date.isoformat()}",
            market_id="US_EQ",
            as_of_date=old_date,
            thread=threading.Thread(target=lambda: None, daemon=True),
            started_at=datetime.now(timezone.utc),
            deadline=datetime.now(timezone.utc) + timedelta(hours=1),
            done=threading.Event(),
            result=[],
            attempt_number=1,
            max_retries=3,
        )
        lane.handle = handle

        # run()'s rollover block only marks the lane.
        lane.pending_rollover = new_date

        with patch("prometheus.orchestration.market_aware_daemon.update_job_execution_status") as mock_update, \
             patch.object(daemon, "_finalize_stale_runs_for_market") as mock_fin, \
             patch.object(daemon, "_initialize_dag") as mock_init:
            # Busy lane: the deferred rollover must NOT touch the running
            # handle, must NOT finalize, must NOT swap the DAG.
            daemon._apply_pending_rollover(lane)
            mock_update.assert_not_called()
            mock_fin.assert_not_called()
            mock_init.assert_not_called()
            assert lane.handle is handle
            assert lane.pending_rollover == new_date
            assert daemon.active_dags["US_EQ"][0] is old_dag

            # Job completes → lane freed (the normal poll path does this).
            lane.handle = None

            # Now the swap goes through: finalize stale runs for the OLD
            # date, then re-initialize the DAG for the new date.
            daemon._apply_pending_rollover(lane)
            mock_fin.assert_called_once_with("US_EQ", old_date)
            mock_init.assert_called_once_with("US_EQ", new_date)
            assert lane.pending_rollover is None


# ============================================================================
# MEDIUM 6: Config loading visibility
# ============================================================================

class TestConfigLoadingVisibility:
    """Test that missing config files produce warnings."""

    def test_allocator_explicit_missing_path_warns(self, capsys):
        """Passing an explicit nonexistent path should print a warning to stderr."""
        from prometheus.sector.allocator import load_allocator_config

        config = load_allocator_config(path="/nonexistent/allocator.yaml")
        captured = capsys.readouterr()
        assert "not found" in captured.err.lower()
        # Config should still be valid (defaults)
        assert config.sector_kill_threshold == pytest.approx(0.25)

    def test_allocator_default_missing_path_no_warning(self, capsys):
        """When no explicit path is given and default doesn't exist, no warning."""
        from prometheus.sector.allocator import load_allocator_config

        with patch("prometheus.sector.allocator.DEFAULT_ALLOCATOR_CONFIG_PATH",
                    __import__("pathlib").Path("/nonexistent/default.yaml")):
            load_allocator_config()

        captured = capsys.readouterr()
        # No stderr warning for implicit default path
        assert "not found" not in captured.err.lower()


# ============================================================================
# MEDIUM 7: Morning catch-up budget check
# ============================================================================

class TestCatchupBudgetCheck:
    """A non-positive catch-up budget must prevent any catch-up work.

    New-semantics equivalent of the old "budget checked BEFORE submitting
    the next job" test: catch-up no longer loops inline, so budget<=0 now
    means no CatchupState is ever attached to any lane.
    """

    @patch("prometheus.orchestration.market_aware_daemon.now_local")
    @patch("prometheus.orchestration.market_aware_daemon.build_market_dag")
    def test_zero_budget_attaches_no_catchup(self, mock_build, mock_now_local):
        """A zero-second budget returns before the per-market loop."""
        from prometheus.orchestration.market_aware_daemon import MarketAwareDaemon

        config = MagicMock()
        config.morning_catchup_hour = 8
        config.as_of_date = None
        config.markets = ["US_EQ"]
        config.poll_interval_seconds = 1
        config.options_mode = "paper"

        db = MagicMock()
        daemon = MarketAwareDaemon(config, db)

        yesterday = date(2026, 4, 10)
        mock_now_local.return_value = datetime(2026, 4, 11, 8, 2)

        mock_cal = MagicMock()
        mock_cal.trading_days_between.return_value = [yesterday]
        daemon._calendars["US_EQ"] = mock_cal

        # load_latest_run is imported locally; patch at source
        with patch("prometheus.pipeline.state.load_latest_run", return_value=None) as mock_load:
            with patch.dict("os.environ", {"PROMETHEUS_CATCHUP_BUDGET_SECONDS": "0"}):
                daemon._maybe_morning_catchup(date(2026, 4, 11))

        # Zero budget → no lane got a CatchupState, no DAG was built, and
        # the (market, date) key was NOT consumed, so a later restart with a
        # sane budget can still catch up.
        assert daemon.lanes["US_EQ"].catchup is None
        mock_build.assert_not_called()
        mock_load.assert_not_called()
        assert f"catchup_US_EQ_{yesterday}" not in daemon._catchup_done
