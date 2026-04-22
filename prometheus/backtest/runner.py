"""Prometheus v2 – Sleeve-level backtest runner.

This module provides :class:`BacktestRunner`, a small orchestration
helper that simulates a sleeve/book over a historical period using the
execution layer's :class:`~prometheus.execution.backtest_broker.BacktestBroker`.

The runner is intentionally narrow in scope for the first iteration:

* It operates at end-of-day frequency using :class:`TimeMachine`.
* Target positions are supplied via a user-provided callback function;
  higher-level orchestration that wires Assessment/Universe/Portfolio
  engines can be layered on top.
* It records results into the ``backtest_runs``, ``backtest_trades``, and
  ``backtest_daily_equity`` tables defined by migration 0003.
"""

from __future__ import annotations

import multiprocessing
import os
import sys
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime
from math import sqrt
from typing import Callable, Dict, List, Sequence

from apathis.core.database import DatabaseManager
from apathis.core.ids import generate_uuid
from apathis.core.logging import get_logger
from apathis.core.markets import infer_region_from_market_id
from apathis.fragility.storage import FragilityStorage
from apathis.regime.storage import RegimeStorage
from apathis.regime.types import RegimeLabel
from psycopg2.extras import Json

from prometheus.backtest.analyzers import EquityCurveAnalyzer, EquityCurvePoint
from prometheus.backtest.config import SleeveConfig
from prometheus.execution.api import apply_execution_plan
from prometheus.execution.backtest_broker import BacktestBroker
from prometheus.execution.broker_interface import Fill
from prometheus.execution.executed_actions import (
    ExecutedActionContext,
    record_executed_actions_for_fills,
)
from prometheus.meta.market_situation import (
    MarketSituation,
    MarketSituationConfig,
    classify_market_situation,
)
from prometheus.meta.storage import MetaStorage
from prometheus.meta.types import DecisionOutcome, EngineDecision

logger = get_logger(__name__)


TargetPositionsFn = Callable[[date], Dict[str, float]]


@dataclass
class BacktestRunner:
    """Run a simple sleeve-level backtest over a date range.

    The runner is parameterised by a :class:`BacktestBroker`, an equity
    curve analyzer, and a callback that produces per-date target
    positions. It is agnostic to how those targets are computed (they may
    come from Assessment/Universe/Portfolio engines or any other logic).
    """

    db_manager: DatabaseManager
    broker: BacktestBroker
    equity_analyzer: EquityCurveAnalyzer
    target_positions_fn: TargetPositionsFn
    # Optional callback producing per-date exposure metrics (e.g. lambda
    # and state-aware diagnostics) to be stored alongside daily equity in
    # ``backtest_daily_equity.exposure_metrics_json``. When omitted or if
    # the callback fails, an empty dict is stored.
    exposure_metrics_fn: Callable[[date], Dict[str, float]] | None = None

    def run_sleeve(self, config: SleeveConfig, start_date: date, end_date: date) -> str:
        """Run a backtest for ``config`` between ``start_date`` and ``end_date``.

        Returns the generated ``run_id`` from ``backtest_runs``.
        """

        if end_date < start_date:
            raise ValueError("end_date must be >= start_date")

        run_id = generate_uuid()
        # Single meta-level decision identifier for this sleeve backtest
        # run. This id is propagated to orders (via apply_execution_plan),
        # executed_actions, and engine_decisions/decision_outcomes so the
        # Meta-Orchestrator can join everything together cheaply.
        decision_id = generate_uuid()

        self._insert_backtest_run_initial(run_id, config, start_date, end_date)

        # Allow target/exposure callables to bind to the run context if they
        # support it (e.g. sleeve pipeline decision logging).
        for fn in (self.target_positions_fn, self.exposure_metrics_fn):
            if fn is None:
                continue
            setter = getattr(fn, "set_run_context", None)
            if callable(setter):
                try:
                    setter(run_id=run_id, decision_id=decision_id)
                except Exception:  # pragma: no cover - defensive
                    logger.exception(
                        "BacktestRunner.run_sleeve: failed to set run context on callable for run_id=%s",
                        run_id,
                    )

        time_machine = self.broker.time_machine
        equity_curve: List[EquityCurvePoint] = []
        exposure_by_date: Dict[date, Dict[str, float]] = {}
        peak_equity: float | None = None
        last_fill_ts: datetime | None = None

        # Build the concrete list of trading days once so we can display an
        # accurate completion percentage.
        trading_days = [
            d for d in time_machine.iter_trading_days() if start_date <= d <= end_date
        ]
        total_steps = len(trading_days)

        def _parse_bool_env(name: str) -> bool | None:
            raw = os.getenv(name)
            if raw is None:
                return None
            val = raw.strip().lower()
            if val in ("1", "true", "yes", "y", "on"):
                return True
            if val in ("0", "false", "no", "n", "off", ""):
                return False
            return None

        def _should_show_progress() -> tuple[bool, bool]:
            """Return (enabled, interactive).

            - Enabled by default only on TTY *and* in the main process.
            - Can be forced on/off via BACKTEST_PROGRESS.
            """

            forced = _parse_bool_env("BACKTEST_PROGRESS")
            is_tty = sys.stderr.isatty()
            is_main = multiprocessing.current_process().name == "MainProcess"

            if forced is False:
                return (False, False)
            if forced is True:
                return (True, is_tty)

            # Default behaviour: only show an interactive progress bar when
            # the user is running in a terminal and we're not a worker.
            if is_tty and is_main:
                return (True, True)
            return (False, False)

        show_progress, interactive_progress = _should_show_progress()

        def _fmt_hhmmss(seconds: float) -> str:
            seconds = max(0.0, float(seconds))
            h = int(seconds // 3600)
            m = int((seconds % 3600) // 60)
            s = int(seconds % 60)
            if h > 0:
                return f"{h:d}:{m:02d}:{s:02d}"
            return f"{m:02d}:{s:02d}"

        progress_lock = threading.Lock()

        def _render_progress(
            step: int,
            total: int,
            as_of: date,
            *,
            t0: float,
            phase: str | None = None,
        ) -> None:
            if not show_progress:
                return
            if total <= 0:
                return

            pct = step * 100.0 / float(total)
            bar_width = 28
            filled = int(bar_width * step / float(total))
            filled = max(0, min(bar_width, filled))
            bar = "#" * filled + "-" * (bar_width - filled)

            elapsed = time.perf_counter() - t0
            eta = 0.0
            if step > 0 and total > step:
                eta = (elapsed / float(step)) * float(total - step)

            phase_part = f" phase={phase}" if phase else ""

            msg = (
                f"Backtest {config.sleeve_id} {as_of} "
                f"[{bar}] {step}/{total} ({pct:5.1f}%) "
                f"elapsed={_fmt_hhmmss(elapsed)} eta={_fmt_hhmmss(eta)}"
                f"{phase_part}"
            )

            with progress_lock:
                if interactive_progress:
                    # Clear-to-EOL avoids leftovers when line length changes.
                    sys.stderr.write("\r" + msg + "\x1b[K")
                    sys.stderr.flush()
                else:
                    sys.stderr.write(msg + "\n")
                    sys.stderr.flush()

        t0 = time.perf_counter()

        progress_state: dict[str, object] = {
            "step": 0,
            "as_of": None,
            "phase": None,
            "done": False,
        }

        def _progress_heartbeat() -> None:
            """Periodically refresh the progress line so long steps look alive."""

            last = 0.0
            while True:
                if bool(progress_state.get("done")):
                    return
                if not (show_progress and interactive_progress):
                    return

                now = time.perf_counter()
                if now - last >= 1.0:
                    step = int(progress_state.get("step") or 0)
                    as_of_val = progress_state.get("as_of")
                    phase_val = progress_state.get("phase")
                    if step > 0 and isinstance(as_of_val, date):
                        _render_progress(
                            step,
                            total_steps,
                            as_of_val,
                            t0=t0,
                            phase=str(phase_val) if phase_val is not None else None,
                        )
                    last = now

                time.sleep(0.1)

        heartbeat_thread: threading.Thread | None = None
        if show_progress and interactive_progress:
            heartbeat_thread = threading.Thread(target=_progress_heartbeat, daemon=True)
            heartbeat_thread.start()

        for step_idx, as_of in enumerate(trading_days, start=1):
            progress_state["step"] = step_idx
            progress_state["as_of"] = as_of
            progress_state["phase"] = "targets"
            _render_progress(step_idx, total_steps, as_of, t0=t0, phase="targets")

            # Log progress every 5% of the *trading-day* window.
            if total_steps > 0:
                pct_int = int(step_idx * 100 / total_steps)
                prev_pct_int = int((step_idx - 1) * 100 / total_steps)
                if pct_int % 5 == 0 and pct_int != prev_pct_int:
                    logger.info(
                        "BacktestRunner.run_sleeve progress: sleeve=%s strategy=%s as_of=%s %d%%",
                        config.sleeve_id,
                        config.strategy_id,
                        as_of,
                        pct_int,
                    )

            # Advance the TimeMachine and synchronise broker state.
            time_machine.set_date(as_of)

            target_positions = self.target_positions_fn(as_of)
            target_num_positions = len(target_positions)

            progress_state["phase"] = "execution"
            _render_progress(step_idx, total_steps, as_of, t0=t0, phase="execution")

            # Apply execution plan via the unified execution API. This
            # function is responsible for computing orders from current
            # vs target positions, submitting them via the broker, and in
            # BACKTEST mode generating fills via BacktestBroker +
            # MarketSimulator. It also persists orders, fills, and (optionally)
            # a positions snapshot into the runtime DB.
            apply_execution_plan(
                db_manager=self.db_manager,
                broker=self.broker,
                portfolio_id=config.portfolio_id,
                target_positions=target_positions,
                mode="BACKTEST",
                as_of_date=as_of,
                decision_id=decision_id,
                record_positions=True,
            )

            # After execution, inspect updated account state.
            account_state = self.broker.get_account_state()
            equity = float(account_state.get("equity", 0.0))

            if peak_equity is None or equity > peak_equity:
                peak_equity = equity
            drawdown = 0.0
            if peak_equity and peak_equity > 0.0:
                drawdown = equity / peak_equity - 1.0

            # Optional per-date exposure metrics (e.g. lambda/state-aware
            # diagnostics) that we want to persist alongside the equity
            # curve. Failures here should never abort the backtest.
            exposure_metrics: Dict[str, float] = {}
            if self.exposure_metrics_fn is not None:
                try:
                    raw = self.exposure_metrics_fn(as_of) or {}
                    exposure_metrics = {str(k): float(v) for k, v in raw.items()}
                except Exception:  # pragma: no cover - defensive
                    logger.exception(
                        "BacktestRunner.run_sleeve: exposure_metrics_fn failed for %s; using empty metrics",
                        as_of,
                    )
                    exposure_metrics = {}

            # Always attach a small set of universally useful diagnostics.
            exposure_metrics["target_num_positions"] = float(target_num_positions)

            exposure_by_date[as_of] = exposure_metrics

            equity_curve.append(EquityCurvePoint(date=as_of, equity=equity))
            self._insert_daily_equity(run_id, as_of, equity, drawdown, exposure_metrics)

            # Record trades for any new fills since the previous step.
            fills = self.broker.get_fills(since=last_fill_ts)
            if fills:
                last_fill_ts = max(f.timestamp for f in fills)
                self._insert_trades_for_fills(run_id, fills, config)

                # Also mirror fills into executed_actions so that the
                # Meta-Orchestrator and monitoring layers can analyse
                # realised trades in a unified schema across modes.
                record_executed_actions_for_fills(
                    db_manager=self.db_manager,
                    fills=fills,
                    context=ExecutedActionContext(
                        run_id=run_id,
                        portfolio_id=config.portfolio_id,
                        decision_id=decision_id,
                        mode="BACKTEST",
                    ),
                )

        # Stop heartbeat and finalise interactive progress bar.
        progress_state["done"] = True
        if heartbeat_thread is not None:
            heartbeat_thread.join(timeout=2.0)

        if show_progress and interactive_progress:
            sys.stderr.write("\n")
            sys.stderr.flush()

        metrics = self.equity_analyzer.compute_metrics(equity_curve)

        # Optionally augment metrics with run-level summaries derived from
        # per-date exposure diagnostics (lambda and state-aware context).
        if exposure_by_date:
            exposure_summary = self._compute_exposure_aggregates(
                equity_curve=equity_curve,
                exposure_by_date=exposure_by_date,
            )
            if exposure_summary:
                metrics.update(exposure_summary)

        # Crisis-lead diagnostics: warning signals before CRISIS regime.
        try:
            crisis_metrics = self._compute_crisis_lead_metrics(
                config=config,
                equity_curve=equity_curve,
                exposure_by_date=exposure_by_date,
                start_date=start_date,
                end_date=end_date,
            )
            if crisis_metrics:
                metrics.update(crisis_metrics)
        except Exception:  # pragma: no cover - defensive enrichment only
            logger.exception(
                "BacktestRunner.run_sleeve: failed to compute crisis lead metrics for run_id=%s",
                run_id,
            )

        # Situation-conditional performance buckets (MarketSituation labels).
        try:
            situation_metrics = self._compute_situation_bucket_metrics(
                config=config,
                equity_curve=equity_curve,
                start_date=start_date,
                end_date=end_date,
            )
            if situation_metrics:
                metrics.update(situation_metrics)
        except Exception:  # pragma: no cover - defensive enrichment only
            logger.exception(
                "BacktestRunner.run_sleeve: failed to compute situation bucket metrics for run_id=%s",
                run_id,
            )

        self._update_backtest_run_metrics(run_id, metrics)

        # Record a Meta-Orchestrator friendly decision and outcome for this
        # backtest run so that it becomes visible in engine_decisions and
        # decision_outcomes.
        self._record_meta_decision_and_outcome(
            run_id=run_id,
            decision_id=decision_id,
            config=config,
            start_date=start_date,
            end_date=end_date,
            metrics=metrics,
        )

        logger.info(
            "BacktestRunner.run_sleeve: run_id=%s sleeve=%s strategy=%s start=%s end=%s cumulative_return=%.4f",
            run_id,
            config.sleeve_id,
            config.strategy_id,
            start_date,
            end_date,
            float(metrics.get("cumulative_return", 0.0)),
        )

        return run_id

    # ------------------------------------------------------------------
    # Persistence helpers
    # ------------------------------------------------------------------

    def _insert_backtest_run_initial(
        self,
        run_id: str,
        config: SleeveConfig,
        start_date: date,
        end_date: date,
    ) -> None:
        """Insert an initial row into ``backtest_runs`` before equity curve is computed."""

        payload = {
            "sleeve_id": config.sleeve_id,
            "strategy_id": config.strategy_id,
            "market_id": config.market_id,
            "universe_id": config.universe_id,
            "portfolio_id": config.portfolio_id,
            "assessment_strategy_id": config.assessment_strategy_id,
            "assessment_horizon_days": config.assessment_horizon_days,
            # Selection / portfolio sizing knobs (useful for sweep analysis).
            "universe_max_size": getattr(config, "universe_max_size", None),
            "universe_sector_max_names": getattr(config, "universe_sector_max_names", None),
            "portfolio_max_names": getattr(config, "portfolio_max_names", None),
            "portfolio_hysteresis_buffer": getattr(config, "portfolio_hysteresis_buffer", None),
            "portfolio_per_instrument_max_weight": getattr(
                config, "portfolio_per_instrument_max_weight", None
            ),
            "lambda_score_weight": getattr(config, "lambda_score_weight", None),
            "lambda_score_weight_selection": getattr(config, "lambda_score_weight_selection", None),
            "lambda_score_weight_portfolio": getattr(config, "lambda_score_weight_portfolio", None),
            # Assessment configuration.
            "assessment_backend": getattr(config, "assessment_backend", None),
            "assessment_model_id": getattr(config, "assessment_model_id", None),
            "assessment_use_joint_context": getattr(config, "assessment_use_joint_context", None),
            "assessment_context_model_id": getattr(config, "assessment_context_model_id", None),
            # Risk configuration.
            "stability_risk_alpha": getattr(config, "stability_risk_alpha", None),
            "stability_risk_horizon_steps": getattr(config, "stability_risk_horizon_steps", None),
            "regime_risk_alpha": getattr(config, "regime_risk_alpha", None),
            "scenario_risk_set_id": getattr(config, "scenario_risk_set_id", None),
            # Regime detector + Meta budget knobs.
            "hazard_profile": getattr(config, "hazard_profile", None),
            "meta_budget_enabled": getattr(config, "meta_budget_enabled", None),
            "meta_budget_alpha": getattr(config, "meta_budget_alpha", None),
            "meta_budget_min": getattr(config, "meta_budget_min", None),
            "meta_budget_horizon_steps": getattr(config, "meta_budget_horizon_steps", None),
            "meta_budget_region": getattr(config, "meta_budget_region", None),
            "apply_fragility_overlay": getattr(config, "apply_fragility_overlay", None),
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        }

        sql = """
            INSERT INTO backtest_runs (
                run_id,
                strategy_id,
                config_json,
                start_date,
                end_date,
                universe_id,
                metrics_json,
                created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, NULL, NOW())
        """

        with self.db_manager.get_runtime_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    sql,
                    (
                        run_id,
                        config.strategy_id,
                        Json(payload),
                        start_date,
                        end_date,
                        config.universe_id,
                    ),
                )
                conn.commit()

    def _insert_daily_equity(
        self,
        run_id: str,
        as_of_date: date,
        equity: float,
        drawdown: float,
        exposure_metrics: Dict[str, float],
    ) -> None:
        """Insert a row into ``backtest_daily_equity`` for a given date."""

        sql = """
            INSERT INTO backtest_daily_equity (
                run_id,
                date,
                equity_curve_value,
                drawdown,
                exposure_metrics_json
            ) VALUES (%s, %s, %s, %s, %s)
        """

        with self.db_manager.get_runtime_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    sql,
                    (
                        run_id,
                        as_of_date,
                        float(equity),
                        float(drawdown),
                        Json(exposure_metrics or {}),
                    ),
                )
                conn.commit()

    def _insert_trades_for_fills(
        self,
        run_id: str,
        fills: Sequence[Fill],
        config: SleeveConfig,
    ) -> None:
        """Insert ``backtest_trades`` rows corresponding to fills."""

        if not fills:
            return

        sql = """
            INSERT INTO backtest_trades (
                run_id,
                trade_date,
                ticker,
                direction,
                size,
                price,
                regime_id,
                universe_id,
                profile_version_id,
                decision_metadata_json
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        with self.db_manager.get_runtime_connection() as conn:
            with conn.cursor() as cursor:
                for fill in fills:
                    metadata = Json(
                        {
                            "sleeve_id": config.sleeve_id,
                            "strategy_id": config.strategy_id,
                        }
                    )
                    cursor.execute(
                        sql,
                        (
                            run_id,
                            fill.timestamp.date(),
                            fill.instrument_id,
                            fill.side.value,
                            float(fill.quantity),
                            float(fill.price),
                            None,  # regime_id (optional, not wired yet)
                            config.universe_id,
                            None,  # profile_version_id (optional, not wired yet)
                            metadata,
                        ),
                    )
                conn.commit()

    def _update_backtest_run_metrics(self, run_id: str, metrics: Dict[str, float]) -> None:
        """Update ``backtest_runs.metrics_json`` for ``run_id``."""

        sql = """
            UPDATE backtest_runs
               SET metrics_json = %s
             WHERE run_id = %s
        """

        with self.db_manager.get_runtime_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (Json(metrics), run_id))
                conn.commit()

    def _compute_exposure_aggregates(
        self,
        *,
        equity_curve: Sequence[EquityCurvePoint],
        exposure_by_date: Dict[date, Dict[str, float]],
    ) -> Dict[str, float]:
        """Compute run-level aggregates from per-date exposure diagnostics.

        The input exposures are the same metrics persisted into
        ``backtest_daily_equity.exposure_metrics_json``. This helper derives
        a small number of summary statistics suitable for Meta-level
        analysis, such as average lambda exposure and performance in
        low/medium/high lambda regimes.
        """

        if not equity_curve or not exposure_by_date:
            return {}

        # Build daily returns keyed by date from the equity curve.
        curve_sorted = sorted(equity_curve, key=lambda p: p.date)
        returns_by_date: Dict[date, float] = {}
        if len(curve_sorted) >= 2:
            prev = curve_sorted[0]
            for point in curve_sorted[1:]:
                prev_eq = float(prev.equity)
                if prev_eq > 0.0:
                    returns_by_date[point.date] = float(point.equity) / prev_eq - 1.0
                prev = point

        metrics: Dict[str, float] = {}

        def _time_mean(key: str) -> float | None:
            vals: List[float] = []
            for exp in exposure_by_date.values():
                val = exp.get(key)
                if isinstance(val, (int, float)):
                    vals.append(float(val))
            if not vals:
                return None
            return float(sum(vals) / len(vals))

        # Simple time-averaged exposures.
        for src_key, dst_key in [
            ("lambda_score_mean", "lambda_score_mean_over_run"),
            ("lambda_score_coverage", "lambda_score_coverage_over_run"),
            ("stab_risk_score_mean", "stab_risk_score_mean_over_run"),
            ("stab_p_worsen_any_mean", "stab_p_worsen_any_mean_over_run"),
            ("regime_risk_score", "regime_risk_score_mean_over_run"),
            ("regime_p_change_any", "regime_p_change_any_mean_over_run"),
            ("down_risk", "down_risk_mean_over_run"),
            ("up_risk", "up_risk_mean_over_run"),
            ("universe_size", "universe_size_mean_over_run"),
            ("target_num_positions", "target_num_positions_mean_over_run"),
        ]:
            mean_val = _time_mean(src_key)
            if mean_val is not None:
                metrics[dst_key] = mean_val

        # Lambda bucketed performance: low / mid / high lambda days.
        lambda_obs: List[tuple[float, float]] = []
        for d, exp in exposure_by_date.items():
            lam = exp.get("lambda_score_mean")
            ret = returns_by_date.get(d)
            if isinstance(lam, (int, float)) and isinstance(ret, (int, float)):
                lambda_obs.append((float(lam), float(ret)))

        if lambda_obs:
            metrics["lambda_bucket_total_num_days"] = float(len(lambda_obs))

        def _bucket_metrics(prefix: str, obs: List[tuple[float, float]]) -> None:
            if len(obs) < 3:
                return
            obs.sort(key=lambda x: x[0])
            n = len(obs)
            third = max(n // 3, 1)

            low = obs[:third]
            mid = obs[third : 2 * third]
            high = obs[2 * third :]
            if not high:
                high = obs[-third:]

            def _mean_ret(pairs: List[tuple[float, float]]) -> float | None:
                if not pairs:
                    return None
                return float(sum(r for _x, r in pairs) / len(pairs))

            bucket_info = [
                ("low", low),
                ("mid", mid),
                ("high", high),
            ]
            for name, pairs in bucket_info:
                mean_ret = _mean_ret(pairs)
                if mean_ret is not None:
                    metrics[f"{prefix}_bucket_{name}_mean_daily_return"] = mean_ret
                metrics[f"{prefix}_bucket_{name}_num_days"] = float(len(pairs))

            low_mean = metrics.get(f"{prefix}_bucket_low_mean_daily_return")
            high_mean = metrics.get(f"{prefix}_bucket_high_mean_daily_return")
            if isinstance(low_mean, (int, float)) and isinstance(high_mean, (int, float)):
                metrics[f"{prefix}_bucket_high_minus_low_return_diff"] = float(high_mean - low_mean)

        if len(lambda_obs) >= 3:
            _bucket_metrics("lambda", lambda_obs)

        # Hazard-signal bucketed performance (market-proxy hazard inputs to the regime detector).
        down_obs: List[tuple[float, float]] = []
        up_obs: List[tuple[float, float]] = []
        for d, exp in exposure_by_date.items():
            ret = returns_by_date.get(d)
            if not isinstance(ret, (int, float)):
                continue

            dr = exp.get("down_risk")
            if isinstance(dr, (int, float)):
                down_obs.append((float(dr), float(ret)))

            ur = exp.get("up_risk")
            if isinstance(ur, (int, float)):
                up_obs.append((float(ur), float(ret)))

        if down_obs:
            metrics["down_risk_bucket_total_num_days"] = float(len(down_obs))
            _bucket_metrics("down_risk", down_obs)

        if up_obs:
            metrics["up_risk_bucket_total_num_days"] = float(len(up_obs))
            _bucket_metrics("up_risk", up_obs)

        return metrics

    def _compute_crisis_lead_metrics(
        self,
        *,
        config: SleeveConfig,
        equity_curve: Sequence[EquityCurvePoint],
        exposure_by_date: Dict[date, Dict[str, float]],
        start_date: date,
        end_date: date,
        warning_lookback_days: int = 30,
        down_risk_threshold: float = 0.7,
        regime_risk_threshold: float = 0.6,
        lambda_score_threshold: float = 0.5,
    ) -> Dict[str, float]:
        """Compute crisis-lead diagnostics for backtest runs.

        This helper identifies CRISIS regime transitions during the backtest
        window and analyzes warning signals that preceded each crisis. The
        goal is to measure how effectively tuning knobs (portfolio parameters)
        affect early warning quality and financial returns before crises.

        Warning signals are defined using daily diagnostics from
        ``exposure_metrics_json``:
        - ``down_risk >= down_risk_threshold`` (default 0.7)
        - ``regime_risk_score >= regime_risk_threshold`` (default 0.6)
        - ``lambda_score_mean >= lambda_score_threshold`` (default 0.5)

        For each crisis transition, we scan backward up to
        ``warning_lookback_days`` (default 30) to find the first warning day
        and compute:
        - Lead time (days from first warning to crisis start)
        - Returns during the warning period
        - Max drawdown during the warning period

        Returns
        -------
        Dict[str, float]
            A dictionary of crisis-lead metrics suitable for storage in
            ``backtest_runs.metrics_json``, including:
            - ``crisis_transitions_count``: Number of CRISIS regime entries
            - ``crisis_warnings_found_count``: Crises with at least one warning
            - ``warning_to_crisis_days_mean``: Mean lead time (days)
            - ``warning_to_crisis_days_median``: Median lead time (days)
            - ``pre_crisis_return_mean``: Mean return during warning period
            - ``max_drawdown_pre_crisis``: Largest drawdown before crisis
            - ``warning_coverage_pct``: Fraction of crises with warnings
        """

        if not equity_curve or not exposure_by_date:
            return {}

        market_id = getattr(config, "market_id", None)
        if not isinstance(market_id, str) or not market_id.strip():
            return {}
        market_id = market_id.strip()

        region = infer_region_from_market_id(market_id)
        if region is None:
            return {}

        # Load regime history for the full backtest window.
        regimes: list[tuple[date, RegimeLabel]] = []
        try:
            regime_storage = RegimeStorage(db_manager=self.db_manager)
            prev = regime_storage.get_latest_regime(region, as_of_date=start_date, inclusive=False)
            if prev is not None:
                regimes.append((prev.as_of_date, prev.regime_label))

            for state in regime_storage.get_history(region, start_date, end_date):
                regimes.append((state.as_of_date, state.regime_label))

            regimes.sort(key=lambda x: x[0])
        except Exception:
            logger.exception(
                "BacktestRunner._compute_crisis_lead_metrics: failed to load regime history for region=%s",
                region,
            )
            return {}

        if not regimes:
            return {}

        # Build a mapping from date to regime label for fast lookups.
        regime_by_date: Dict[date, RegimeLabel] = {}
        current_label = regimes[0][1]
        for d in sorted(exposure_by_date.keys()):
            while regimes and regimes[0][0] <= d:
                current_label = regimes[0][1]
                regimes.pop(0)
            regime_by_date[d] = current_label

        # Build daily returns from equity curve.
        curve_sorted = sorted(equity_curve, key=lambda p: p.date)
        returns_by_date: Dict[date, float] = {}
        if len(curve_sorted) >= 2:
            prev = curve_sorted[0]
            for point in curve_sorted[1:]:
                prev_eq = float(prev.equity)
                if prev_eq > 0.0:
                    returns_by_date[point.date] = float(point.equity) / prev_eq - 1.0
                prev = point

        # Identify CRISIS transitions (entry into CRISIS regime).
        dates_sorted = sorted(regime_by_date.keys())
        crisis_transitions: list[date] = []
        prev_regime = None
        for d in dates_sorted:
            curr_regime = regime_by_date[d]
            if curr_regime == RegimeLabel.CRISIS and prev_regime != RegimeLabel.CRISIS:
                crisis_transitions.append(d)
            prev_regime = curr_regime

        if not crisis_transitions:
            return {
                "crisis_transitions_count": 0.0,
                "crisis_warnings_found_count": 0.0,
                "warning_to_crisis_days_mean": 0.0,
                "warning_to_crisis_days_median": 0.0,
                "pre_crisis_return_mean": 0.0,
                "max_drawdown_pre_crisis": 0.0,
                "warning_coverage_pct": 0.0,
            }

        # For each crisis transition, scan backward to find warning signals.
        def _is_warning_day(d: date) -> bool:
            """Check if date has any warning signal above thresholds."""
            exp = exposure_by_date.get(d)
            if not exp:
                return False
            down_risk = exp.get("down_risk")
            regime_risk = exp.get("regime_risk_score")
            lambda_score = exp.get("lambda_score_mean")
            return (
                (isinstance(down_risk, (int, float)) and float(down_risk) >= down_risk_threshold)
                or (isinstance(regime_risk, (int, float)) and float(regime_risk) >= regime_risk_threshold)
                or (isinstance(lambda_score, (int, float)) and float(lambda_score) >= lambda_score_threshold)
            )

        lead_times: list[int] = []
        warning_period_returns: list[float] = []
        pre_crisis_drawdowns: list[float] = []
        warnings_found = 0

        for crisis_date in crisis_transitions:
            # Find all dates within lookback window before crisis.
            lookback_dates = [
                d for d in dates_sorted if d < crisis_date and (crisis_date - d).days <= warning_lookback_days
            ]
            lookback_dates.sort()

            if not lookback_dates:
                continue

            # Find first warning day in the lookback window.
            first_warning_date: date | None = None
            for d in lookback_dates:
                if _is_warning_day(d):
                    first_warning_date = d
                    break

            if first_warning_date is None:
                continue

            warnings_found += 1
            lead_days = (crisis_date - first_warning_date).days
            lead_times.append(lead_days)

            # Compute returns during the warning period (first_warning_date to crisis_date).
            warning_period_dates = [d for d in lookback_dates if first_warning_date <= d < crisis_date]
            if warning_period_dates:
                period_returns = [returns_by_date.get(d, 0.0) for d in warning_period_dates if d in returns_by_date]
                if period_returns:
                    cumulative_return = 1.0
                    for r in period_returns:
                        cumulative_return *= 1.0 + float(r)
                    warning_period_returns.append(cumulative_return - 1.0)

                    # Compute max drawdown during warning period.
                    equity_local = 1.0
                    peak_local = 1.0
                    max_dd_local = 0.0
                    for r in period_returns:
                        equity_local *= 1.0 + float(r)
                        if equity_local > peak_local:
                            peak_local = equity_local
                        dd = 0.0
                        if peak_local > 0.0:
                            dd = equity_local / peak_local - 1.0
                        if dd < max_dd_local:
                            max_dd_local = dd
                    pre_crisis_drawdowns.append(max_dd_local)

        # Aggregate metrics.
        num_crises = len(crisis_transitions)
        coverage_pct = float(warnings_found) / float(num_crises) if num_crises > 0 else 0.0

        def _safe_mean(vals: list[float]) -> float:
            return float(sum(vals) / len(vals)) if vals else 0.0

        def _safe_median(vals: list[int] | list[float]) -> float:
            if not vals:
                return 0.0
            sorted_vals = sorted(vals)
            n = len(sorted_vals)
            if n % 2 == 1:
                return float(sorted_vals[n // 2])
            else:
                return float((sorted_vals[n // 2 - 1] + sorted_vals[n // 2]) / 2.0)

        return {
            "crisis_transitions_count": float(num_crises),
            "crisis_warnings_found_count": float(warnings_found),
            "warning_to_crisis_days_mean": _safe_mean([float(x) for x in lead_times]),
            "warning_to_crisis_days_median": _safe_median(lead_times),
            "pre_crisis_return_mean": _safe_mean(warning_period_returns),
            "max_drawdown_pre_crisis": min(pre_crisis_drawdowns) if pre_crisis_drawdowns else 0.0,
            "warning_coverage_pct": coverage_pct,
        }

    def _compute_situation_bucket_metrics(
        self,
        *,
        config: SleeveConfig,
        equity_curve: Sequence[EquityCurvePoint],
        start_date: date,
        end_date: date,
        situation_config: MarketSituationConfig = MarketSituationConfig(),
        trading_days_per_year: int = 252,
        min_bucket_days: int = 3,
    ) -> Dict[str, float]:
        """Compute MarketSituation bucketed performance metrics.

        We classify each trading day in ``equity_curve`` into a
        :class:`~prometheus.meta.market_situation.MarketSituation` using
        the *same* close[t] inputs as the daily pipeline:

        - Region-level regime label from ``regimes``
        - Market-level fragility score from ``fragility_measures``

        We then bucket daily returns by situation and compute a small set
        of per-situation summary statistics. All returned values are
        floats so they can be stored into ``backtest_runs.metrics_json``
        without breaking downstream consumers.

        Notes
        - Buckets may be sparse for short backtests; we still emit keys for
          all situations with 0.0 values.
        - The bucketed cumulative return and drawdown are computed on the
          *subset* of daily returns observed in that situation (i.e. not a
          contiguous sub-curve in calendar time).
        """

        if not equity_curve or len(equity_curve) < 2:
            return {}

        market_id = getattr(config, "market_id", None)
        if not isinstance(market_id, str) or not market_id.strip():
            return {}
        market_id = market_id.strip()

        region = infer_region_from_market_id(market_id)

        # --------------------------------------------------------------
        # Load regime + fragility history once for the full window.
        # --------------------------------------------------------------

        regimes: list[tuple[date, object]] = []
        if region is not None:
            try:
                regime_storage = RegimeStorage(db_manager=self.db_manager)
                prev = regime_storage.get_latest_regime(region, as_of_date=start_date, inclusive=False)
                if prev is not None:
                    regimes.append((prev.as_of_date, prev.regime_label))

                for state in regime_storage.get_history(region, start_date, end_date):
                    regimes.append((state.as_of_date, state.regime_label))

                regimes.sort(key=lambda x: x[0])
            except Exception:  # pragma: no cover - defensive
                regimes = []

        frags: list[tuple[date, float]] = []
        try:
            frag_storage = FragilityStorage(db_manager=self.db_manager)
            prev_f = frag_storage.get_latest_measure(
                "MARKET",
                market_id,
                as_of_date=start_date,
                inclusive=False,
            )
            if prev_f is not None:
                frags.append((prev_f.as_of_date, float(prev_f.fragility_score)))

            for m in frag_storage.get_history("MARKET", market_id, start_date, end_date):
                frags.append((m.as_of_date, float(m.fragility_score)))

            frags.sort(key=lambda x: x[0])
        except Exception:  # pragma: no cover - defensive
            frags = []

        # --------------------------------------------------------------
        # Classify situation per date (close[t] inputs).
        # --------------------------------------------------------------

        curve_sorted = sorted(equity_curve, key=lambda p: p.date)
        dates = [p.date for p in curve_sorted]

        situations_by_date: dict[date, MarketSituation] = {}

        reg_idx = 0
        current_regime = None

        frag_idx = 0
        current_fragility: float | None = None

        for d in dates:
            prev_regime = current_regime

            while reg_idx < len(regimes) and regimes[reg_idx][0] <= d:
                current_regime = regimes[reg_idx][1]
                reg_idx += 1

            while frag_idx < len(frags) and frags[frag_idx][0] <= d:
                current_fragility = float(frags[frag_idx][1])
                frag_idx += 1

            try:
                sit = classify_market_situation(
                    regime_label=current_regime,
                    prev_regime_label=prev_regime,
                    fragility_score=current_fragility,
                    config=situation_config,
                )
            except Exception:  # pragma: no cover - defensive
                sit = MarketSituation.NEUTRAL

            situations_by_date[d] = sit

        # --------------------------------------------------------------
        # Build daily returns and bucket by situation.
        # --------------------------------------------------------------

        returns_by_date: Dict[date, float] = {}
        prev = curve_sorted[0]
        for point in curve_sorted[1:]:
            prev_eq = float(prev.equity)
            if prev_eq > 0.0:
                returns_by_date[point.date] = float(point.equity) / prev_eq - 1.0
            prev = point

        bucket_returns: dict[MarketSituation, list[float]] = {s: [] for s in MarketSituation}
        for d, ret in returns_by_date.items():
            sit = situations_by_date.get(d, MarketSituation.NEUTRAL)
            bucket_returns[sit].append(float(ret))

        def _stats_for_returns(returns: list[float]) -> dict[str, float]:
            n = len(returns)
            if n <= 0:
                return {
                    "num_days": 0.0,
                    "mean_daily_return": 0.0,
                    "win_rate": 0.0,
                    "annualised_vol": 0.0,
                    "annualised_sharpe": 0.0,
                    "cumulative_return": 0.0,
                    "max_drawdown": 0.0,
                }

            mean_daily = float(sum(returns) / n)
            wins = sum(1 for r in returns if r > 0.0)
            win_rate = float(wins) / float(n)

            if n > 1:
                var = sum((r - mean_daily) ** 2 for r in returns) / float(n - 1)
                vol_daily = sqrt(max(0.0, float(var)))
            else:
                vol_daily = 0.0

            annualised_vol = float(vol_daily) * sqrt(float(trading_days_per_year))
            if annualised_vol > 0.0:
                annualised_sharpe = mean_daily * float(trading_days_per_year) / annualised_vol
            else:
                annualised_sharpe = 0.0

            # Cumulative return on the subset of returns.
            equity = 1.0
            peak = 1.0
            max_dd = 0.0
            for r in returns:
                equity *= 1.0 + float(r)
                if equity > peak:
                    peak = equity
                dd = 0.0
                if peak > 0.0:
                    dd = equity / peak - 1.0
                if dd < max_dd:
                    max_dd = dd

            cumulative_return = equity - 1.0

            return {
                "num_days": float(n),
                "mean_daily_return": float(mean_daily),
                "win_rate": float(win_rate),
                "annualised_vol": float(annualised_vol),
                "annualised_sharpe": float(annualised_sharpe),
                "cumulative_return": float(cumulative_return),
                "max_drawdown": float(max_dd),
            }

        metrics: Dict[str, float] = {
            "situation_bucket_total_num_days": float(len(returns_by_date)),
        }

        for sit in MarketSituation:
            key = sit.value
            returns = bucket_returns.get(sit, [])
            stats = _stats_for_returns(returns)

            metrics[f"situation_bucket_{key}_num_days"] = stats["num_days"]
            metrics[f"situation_bucket_{key}_mean_daily_return"] = stats["mean_daily_return"]
            metrics[f"situation_bucket_{key}_win_rate"] = stats["win_rate"]
            metrics[f"situation_bucket_{key}_annualised_vol"] = stats["annualised_vol"]
            metrics[f"situation_bucket_{key}_annualised_sharpe"] = stats["annualised_sharpe"]
            metrics[f"situation_bucket_{key}_cumulative_return"] = stats["cumulative_return"]
            metrics[f"situation_bucket_{key}_max_drawdown"] = stats["max_drawdown"]

            # Convenience key for eligibility filtering.
            metrics[f"situation_bucket_{key}_eligible"] = 1.0 if len(returns) >= min_bucket_days else 0.0

        return metrics

    def _record_meta_decision_and_outcome(
        self,
        *,
        run_id: str,
        decision_id: str,
        config: SleeveConfig,
        start_date: date,
        end_date: date,
        metrics: Dict[str, float],
    ) -> None:
        """Record engine_decisions and decision_outcomes for a sleeve run.

        This creates a single logical Meta-Orchestrator decision for the
        backtest run and an associated outcome record for the full
        backtest window. It is intentionally minimal and focuses on
        making backtests visible to the Meta layer; more granular
        per-horizon outcomes can be added later.
        """

        # Defensive: if strategy_id is missing we still record a decision
        # with a synthetic engine_name.
        strategy_id = config.strategy_id or "UNKNOWN_STRATEGY"
        market_id = config.market_id

        # Compute an approximate horizon in calendar days; this is
        # sufficient for distinguishing short vs long backtests and can
        # be refined later.
        horizon_days = max((end_date - start_date).days, 1)

        storage = MetaStorage(db_manager=self.db_manager)

        decision = EngineDecision(
            decision_id=decision_id,
            engine_name="BACKTEST_SLEEVE_RUNNER",
            run_id=run_id,
            strategy_id=strategy_id,
            market_id=market_id,
            as_of_date=end_date,
            config_id=config.sleeve_id,
            input_refs={
                "run_id": run_id,
                "sleeve_id": config.sleeve_id,
                "portfolio_id": config.portfolio_id,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
            },
            output_refs={
                "metrics": metrics,
            },
            metadata={},
        )
        storage.save_engine_decision(decision)

        outcome = DecisionOutcome(
            decision_id=decision_id,
            horizon_days=horizon_days,
            realized_return=float(metrics.get("cumulative_return", 0.0)),
            realized_pnl=float(metrics.get("final_pnl", 0.0)) if "final_pnl" in metrics else None,
            realized_drawdown=float(metrics.get("max_drawdown", 0.0)),
            realized_vol=float(metrics.get("annualised_vol", 0.0)) if "annualised_vol" in metrics else None,
            metadata={
                "annualised_sharpe": float(metrics.get("annualised_sharpe", 0.0)),
            },
        )
        storage.save_decision_outcome(outcome)
