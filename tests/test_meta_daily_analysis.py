"""Tests for prometheus.meta.daily_analysis."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from typing import Any
from unittest.mock import patch

from prometheus.meta import daily_analysis

# ── Fake DB ──────────────────────────────────────────────────────────


class _FakeCursor:
    def __init__(self, db: "_FakeDb") -> None:
        self._db = db
        self._result: list[tuple] = []

    def execute(self, sql: str, args: Any = ()) -> None:
        norm = " ".join(sql.split()).upper()
        if norm.startswith("SELECT DISTINCT STRATEGY_ID"):
            self._result = [(s,) for s in self._db.strategies]
            return
        if norm.startswith("INSERT INTO META_FEEDBACK_INSIGHTS"):
            self._db.feedback_insights.append(args)
            return
        if norm.startswith("INSERT INTO META_SIGNAL_VALIDATIONS"):
            self._db.signal_validations.append(args)
            return
        if norm.startswith("INSERT INTO META_DIAGNOSTIC_REPORTS"):
            self._db.diagnostic_reports.append(args)
            return
        raise AssertionError(f"unhandled SQL: {norm[:80]}")

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

    def __enter__(self) -> "_FakeConnection":
        return self

    def __exit__(self, *_exc: Any) -> None:
        return None


class _FakeDb:
    def __init__(self, strategies: list[str] | None = None) -> None:
        self.strategies: list[str] = strategies or []
        self.feedback_insights: list[tuple] = []
        self.signal_validations: list[tuple] = []
        self.diagnostic_reports: list[tuple] = []

    @contextmanager
    def get_runtime_connection(self):
        yield _FakeConnection(self)


# ── Fake analysis modules ────────────────────────────────────────────


@dataclass
class _FakeInsight:
    category: str = "portfolio_quality"
    severity: str = "warning"
    message: str = "test insight"
    metric_name: str = "test_metric"
    metric_value: float = 0.5
    benchmark: float = 0.6
    deviation: float = -0.1


@dataclass
class _FakeFeedbackReport:
    insights: list[_FakeInsight]


def _stub_feedback_with_insights(insights: list[_FakeInsight]):
    def _stub(db, as_of, lookback_days=63):
        return _FakeFeedbackReport(insights=list(insights))
    return _stub


def _stub_feedback_empty(db, as_of, lookback_days=63):
    return _FakeFeedbackReport(insights=[])


class _StubLivePerf:
    """Fakes LivePerformanceTracker with predictable outputs."""

    def __init__(self, db_manager):
        self.db_manager = db_manager

    def validate_fragility_signal(self, **_kw):
        return {"n": 50, "spearman_rho": -0.35, "verdict": "SIGNAL_VALID"}

    def compute_hedge_effectiveness(self, **_kw):
        return {"n_dates": 40, "pearson_r": -0.25, "verdict": "HEDGE_EFFECTIVE",
                "options_pnl_total": 1000.0, "portfolio_pnl_total": -5000.0}

    def compute_rolling_performance(self, **_kw):
        return {"n": 60, "sharpe": 1.2, "win_rate": 0.55, "max_drawdown": 0.08,
                "avg_return": 0.012, "total_pnl": 50000.0, "by_strategy": []}


@dataclass
class _FakePerformanceStats:
    sharpe: float = 1.0
    cumulative_return: float = 0.15
    max_drawdown: float = -0.08
    volatility: float = 0.18
    n_observations: int = 100


@dataclass
class _FakeRegimePerformance:
    regime: str = "RISK_ON"
    n_observations: int = 30
    sharpe: float = 1.1


@dataclass
class _FakeConfigComparison:
    config_key: str = "max_names"
    baseline_value: int = 20
    alternative_value: int = 30
    sample_count: int = 50
    sharpe_delta: float = 0.2
    return_delta: float = 0.01
    risk_delta: float = -0.02


@dataclass
class _FakeDiagnosticReport:
    strategy_id: str
    overall_performance: _FakePerformanceStats
    regime_breakdown: list[_FakeRegimePerformance]
    config_comparisons: list[_FakeConfigComparison]
    underperforming_configs: list[dict]
    high_risk_configs: list[dict]
    sample_metadata: dict


class _StubDiagnostics:
    def __init__(self, db_manager):
        self.db_manager = db_manager

    def analyze_strategy(self, strategy_id, min_sample_size=5):
        return _FakeDiagnosticReport(
            strategy_id=strategy_id,
            overall_performance=_FakePerformanceStats(),
            regime_breakdown=[_FakeRegimePerformance()],
            config_comparisons=[_FakeConfigComparison()],
            underperforming_configs=[],
            high_risk_configs=[],
            sample_metadata={"total_runs": 10, "analysis_timestamp": "2026-05-25"},
        )

    def compute_confidence_score(self, **_kw):
        return 0.6


class _StubDiagnosticsInsufficient(_StubDiagnostics):
    def analyze_strategy(self, strategy_id, min_sample_size=5):
        raise ValueError(f"Insufficient data: 2 runs available, need {min_sample_size}")


class _StubProposalGenerator:
    def __init__(self, db_manager, diagnostics_engine, **_kw):
        self.db_manager = db_manager

    def generate_proposals(self, strategy_id, auto_save=True):
        return ["fake_proposal_1", "fake_proposal_2"]


# ── Tests ────────────────────────────────────────────────────────────


def test_run_daily_meta_analysis_persists_feedback_insights():
    db = _FakeDb()
    with patch.object(daily_analysis, "compute_feedback_report",
                      _stub_feedback_with_insights([_FakeInsight(), _FakeInsight()])):
        with patch.object(daily_analysis, "LivePerformanceTracker", _StubLivePerf):
            result = daily_analysis.run_daily_meta_analysis(
                db, date(2026, 5, 25), strategies=[],
            )

    # 2 insights persisted
    assert len(db.feedback_insights) == 2
    feedback_step = next(s for s in result.steps if s.name == "feedback_report")
    assert feedback_step.rows_persisted == 2


def test_feedback_step_skipped_when_no_insights():
    db = _FakeDb()
    with patch.object(daily_analysis, "compute_feedback_report", _stub_feedback_empty):
        with patch.object(daily_analysis, "LivePerformanceTracker", _StubLivePerf):
            result = daily_analysis.run_daily_meta_analysis(
                db, date(2026, 5, 25), strategies=[],
            )

    feedback_step = next(s for s in result.steps if s.name == "feedback_report")
    assert feedback_step.skipped_reason == "empty_insights"
    assert db.feedback_insights == []


def test_signal_validations_persist_three_rows_per_run():
    db = _FakeDb()
    with patch.object(daily_analysis, "compute_feedback_report", _stub_feedback_empty):
        with patch.object(daily_analysis, "LivePerformanceTracker", _StubLivePerf):
            daily_analysis.run_daily_meta_analysis(
                db, date(2026, 5, 25), strategies=[],
            )

    # fragility, hedge, rolling sharpe → 3 validations
    assert len(db.signal_validations) == 3
    signal_names = {row[1] for row in db.signal_validations}
    assert "fragility_vs_portfolio_return" in signal_names
    assert "hedge_options_vs_portfolio_pnl" in signal_names
    assert "rolling_portfolio_sharpe" in signal_names


def test_diagnostics_persists_per_strategy():
    db = _FakeDb()
    with patch.object(daily_analysis, "compute_feedback_report", _stub_feedback_empty):
        with patch.object(daily_analysis, "LivePerformanceTracker", _StubLivePerf):
            with patch.object(daily_analysis, "DiagnosticsEngine", _StubDiagnostics):
                with patch.object(daily_analysis, "ProposalGenerator",
                                  _StubProposalGenerator):
                    daily_analysis.run_daily_meta_analysis(
                        db, date(2026, 5, 25),
                        strategies=["US_EQ_CORE_LONG_EQ", "US_OPTIONS"],
                    )

    # 2 strategies → 2 diagnostic reports persisted
    assert len(db.diagnostic_reports) == 2
    strategy_ids = {row[1] for row in db.diagnostic_reports}
    assert strategy_ids == {"US_EQ_CORE_LONG_EQ", "US_OPTIONS"}


def test_proposals_generated_after_successful_diagnostics():
    db = _FakeDb()
    with patch.object(daily_analysis, "compute_feedback_report", _stub_feedback_empty):
        with patch.object(daily_analysis, "LivePerformanceTracker", _StubLivePerf):
            with patch.object(daily_analysis, "DiagnosticsEngine", _StubDiagnostics):
                with patch.object(daily_analysis, "ProposalGenerator",
                                  _StubProposalGenerator):
                    result = daily_analysis.run_daily_meta_analysis(
                        db, date(2026, 5, 25),
                        strategies=["US_EQ_CORE_LONG_EQ"],
                    )

    proposal_step = next(
        s for s in result.steps if s.name.startswith("proposals[")
    )
    assert proposal_step.rows_persisted == 2  # _StubProposalGenerator returns 2


def test_diagnostics_skipped_when_insufficient_data_blocks_proposals():
    db = _FakeDb()
    with patch.object(daily_analysis, "compute_feedback_report", _stub_feedback_empty):
        with patch.object(daily_analysis, "LivePerformanceTracker", _StubLivePerf):
            with patch.object(daily_analysis, "DiagnosticsEngine",
                              _StubDiagnosticsInsufficient):
                result = daily_analysis.run_daily_meta_analysis(
                    db, date(2026, 5, 25),
                    strategies=["US_EQ_CORE_LONG_EQ"],
                )

    diag_step = next(s for s in result.steps if s.name.startswith("diagnostics["))
    assert diag_step.skipped_reason is not None
    assert "Insufficient" in diag_step.skipped_reason
    # No proposal step emitted because diagnostics didn't succeed
    assert not any(s.name.startswith("proposals[") for s in result.steps)


def test_step_failure_does_not_block_other_steps():
    db = _FakeDb()

    def _broken_feedback(*_a, **_kw):
        raise RuntimeError("intentional test failure")

    with patch.object(daily_analysis, "compute_feedback_report", _broken_feedback):
        with patch.object(daily_analysis, "LivePerformanceTracker", _StubLivePerf):
            result = daily_analysis.run_daily_meta_analysis(
                db, date(2026, 5, 25), strategies=[],
            )

    feedback_step = next(s for s in result.steps if "feedback" in s.name.lower())
    assert feedback_step.error is not None
    assert "intentional" in feedback_step.error

    # But signal validations still ran
    signal_step = next(s for s in result.steps if "signal" in s.name.lower())
    assert signal_step.rows_persisted == 3


def test_strategy_discovery_uses_backtest_runs_table():
    db = _FakeDb(strategies=["DISCOVERED_STRAT"])
    with patch.object(daily_analysis, "compute_feedback_report", _stub_feedback_empty):
        with patch.object(daily_analysis, "LivePerformanceTracker", _StubLivePerf):
            with patch.object(daily_analysis, "DiagnosticsEngine", _StubDiagnostics):
                with patch.object(daily_analysis, "ProposalGenerator",
                                  _StubProposalGenerator):
                    daily_analysis.run_daily_meta_analysis(
                        db, date(2026, 5, 25),
                        # strategies=None → discover from DB
                    )

    assert len(db.diagnostic_reports) == 1
    assert db.diagnostic_reports[0][1] == "DISCOVERED_STRAT"


def test_explicit_strategies_overrides_discovery():
    db = _FakeDb(strategies=["WOULD_BE_DISCOVERED"])
    with patch.object(daily_analysis, "compute_feedback_report", _stub_feedback_empty):
        with patch.object(daily_analysis, "LivePerformanceTracker", _StubLivePerf):
            with patch.object(daily_analysis, "DiagnosticsEngine", _StubDiagnostics):
                with patch.object(daily_analysis, "ProposalGenerator",
                                  _StubProposalGenerator):
                    daily_analysis.run_daily_meta_analysis(
                        db, date(2026, 5, 25),
                        strategies=["EXPLICIT_STRAT"],
                    )

    strategy_ids = {row[1] for row in db.diagnostic_reports}
    assert strategy_ids == {"EXPLICIT_STRAT"}


def test_safe_float_handles_nan_inf_and_none():
    assert daily_analysis._safe_float(None) is None
    assert daily_analysis._safe_float(float("nan")) is None
    assert daily_analysis._safe_float(float("inf")) is None
    assert daily_analysis._safe_float("not a number") is None
    assert daily_analysis._safe_float(1.5) == 1.5
    assert daily_analysis._safe_float("2.5") == 2.5


def test_safe_json_round_trips_dates_and_floats():
    # Production path: callers asdict() any dataclass before calling
    # _safe_json. The job of _safe_json is to stringify dates/datetimes
    # and reject NaN/Inf.
    out = daily_analysis._safe_json({
        "as_of": date(2026, 5, 25),
        "score": 1.5,
        "nested": {"verdict": "VALID"},
    })
    assert out["as_of"] == "2026-05-25"
    assert out["score"] == 1.5
    assert out["nested"]["verdict"] == "VALID"


def test_safe_json_replaces_nan_with_none():
    # NaN gets scrubbed before json.dumps so the call succeeds.
    out = daily_analysis._safe_json({"v": float("nan"), "ok": 1.5})
    assert out["v"] is None
    assert out["ok"] == 1.5


def test_safe_json_recursively_scrubs_nested_nan():
    out = daily_analysis._safe_json({
        "nested": {"a": float("inf"), "b": [float("nan"), 1.0]},
    })
    assert out["nested"]["a"] is None
    assert out["nested"]["b"] == [None, 1.0]


def test_result_aggregates_totals_and_error_flag():
    db = _FakeDb()
    with patch.object(daily_analysis, "compute_feedback_report",
                      _stub_feedback_with_insights([_FakeInsight()])):
        with patch.object(daily_analysis, "LivePerformanceTracker", _StubLivePerf):
            result = daily_analysis.run_daily_meta_analysis(
                db, date(2026, 5, 25), strategies=[],
            )

    # 1 insight + 3 signal validations = 4 total
    assert result.total_persisted == 4
    assert result.any_errors is False
    assert result.as_of_date == date(2026, 5, 25)


@dataclass
class _FakeTradeRecord:
    realized_pnl: float | None


@dataclass
class _FakeWeeklyReport:
    period_start: date
    period_end: date
    period_return_pct: float = 0.025
    period_sharpe: float = 1.4
    ytd_return_pct: float = 0.18
    current_nav: float = 200_000.0
    max_drawdown_period: float = -0.04
    n_positions: int = 18
    n_entries: int = 5
    n_exits: int = 3
    turnover_pct: float = 0.12
    top_winners: list = None
    top_losers: list = None
    closed_trades: list = None
    sector_pnl: dict = None
    regime_label: str = "NEUTRAL"
    forward_signal: str = "GREEN"
    portfolio_hit_rate: float | None = 0.58
    assessment_accuracy: float | None = 0.012
    anomalies: list = None
    proposals: list = None

    def __post_init__(self):
        if self.top_winners is None:
            self.top_winners = []
        if self.top_losers is None:
            self.top_losers = []
        if self.closed_trades is None:
            self.closed_trades = [
                _FakeTradeRecord(realized_pnl=500.0),
                _FakeTradeRecord(realized_pnl=-200.0),
                _FakeTradeRecord(realized_pnl=300.0),
            ]
        if self.sector_pnl is None:
            self.sector_pnl = {}
        if self.anomalies is None:
            self.anomalies = []
        if self.proposals is None:
            self.proposals = []


class _FakeWeeklyDb(_FakeDb):
    def __init__(self) -> None:
        super().__init__()
        self.weekly_reports: list[dict[str, Any]] = []
        self.weekly_deletes: list[tuple] = []


class _FakeWeeklyCursor(_FakeCursor):
    def execute(self, sql: str, args: Any = ()) -> None:
        norm = " ".join(sql.split()).upper()
        if norm.startswith("DELETE FROM WEEKLY_REPORTS"):
            self._db.weekly_deletes.append(args)
            return
        if norm.startswith("INSERT INTO WEEKLY_REPORTS"):
            # NULL-strategy path inlines NULL in the SQL → args is
            # 2 elements shorter (no strategy_id slot at position 2).
            if "NULL," in sql or " NULL\n" in sql or " NULL " in sql:
                self._db.weekly_reports.append({
                    "week_start": args[0], "week_end": args[1],
                    "strategy_id": None,
                    "period_return": args[2], "period_sharpe": args[3],
                    "n_trades": args[5], "n_winners": args[6], "n_losers": args[7],
                })
            else:
                self._db.weekly_reports.append({
                    "week_start": args[0], "week_end": args[1],
                    "strategy_id": args[2],
                    "period_return": args[3], "period_sharpe": args[4],
                    "n_trades": args[6], "n_winners": args[7], "n_losers": args[8],
                })
            return
        super().execute(sql, args)


class _FakeWeeklyConnection(_FakeConnection):
    def cursor(self) -> _FakeWeeklyCursor:
        return _FakeWeeklyCursor(self._db)


class _WeeklyDb(_FakeWeeklyDb):
    @contextmanager
    def get_runtime_connection(self):
        yield _FakeWeeklyConnection(self)


def test_weekly_report_persists_summary_row():
    db = _WeeklyDb()
    fake_report = _FakeWeeklyReport(
        period_start=date(2026, 5, 18),
        period_end=date(2026, 5, 22),
    )
    with patch.object(daily_analysis, "compute_weekly_report",
                      lambda _db, _d: fake_report):
        with patch.object(daily_analysis, "format_weekly_report",
                          lambda _r: "# Weekly Report\n..."):
            result = daily_analysis.run_weekly_report(
                db, date(2026, 5, 25), strategy_id="US_EQ_CORE_LONG_EQ",
            )

    assert result.rows_persisted == 1
    assert len(db.weekly_reports) == 1
    row = db.weekly_reports[0]
    assert row["strategy_id"] == "US_EQ_CORE_LONG_EQ"
    assert row["n_winners"] == 2  # 500 + 300 positive
    assert row["n_losers"] == 1   # -200 negative
    assert row["n_trades"] == 3


def test_weekly_report_with_null_strategy_uses_delete_then_insert():
    db = _WeeklyDb()
    fake_report = _FakeWeeklyReport(
        period_start=date(2026, 5, 18),
        period_end=date(2026, 5, 22),
    )
    with patch.object(daily_analysis, "compute_weekly_report",
                      lambda _db, _d: fake_report):
        with patch.object(daily_analysis, "format_weekly_report",
                          lambda _r: ""):
            daily_analysis.run_weekly_report(
                db, date(2026, 5, 25), strategy_id=None,
            )

    assert len(db.weekly_deletes) == 1
    assert len(db.weekly_reports) == 1
    assert db.weekly_reports[0]["strategy_id"] is None


def test_weekly_report_returns_error_when_compute_raises():
    db = _WeeklyDb()

    def _broken(*_a, **_kw):
        raise RuntimeError("compute exploded")

    with patch.object(daily_analysis, "compute_weekly_report", _broken):
        result = daily_analysis.run_weekly_report(
            db, date(2026, 5, 25), strategy_id=None,
        )

    assert result.error is not None
    assert "compute exploded" in result.error
    assert db.weekly_reports == []


def test_signal_validations_capture_verdict_strings():
    db = _FakeDb()

    class _BadLivePerf(_StubLivePerf):
        def validate_fragility_signal(self, **_kw):
            return {"n": 2, "spearman_rho": float("nan"),
                    "verdict": "INSUFFICIENT_DATA"}

        def compute_hedge_effectiveness(self, **_kw):
            return {"n_dates": 0, "pearson_r": float("nan"),
                    "verdict": "INSUFFICIENT_DATA",
                    "options_pnl_total": 0, "portfolio_pnl_total": 0}

        def compute_rolling_performance(self, **_kw):
            return {"n": 0, "sharpe": float("nan"), "win_rate": float("nan"),
                    "max_drawdown": float("nan"), "avg_return": float("nan"),
                    "total_pnl": 0.0, "by_strategy": []}

    with patch.object(daily_analysis, "compute_feedback_report", _stub_feedback_empty):
        with patch.object(daily_analysis, "LivePerformanceTracker", _BadLivePerf):
            daily_analysis.run_daily_meta_analysis(
                db, date(2026, 5, 25), strategies=[],
            )

    verdicts = {row[2] for row in db.signal_validations}
    assert "INSUFFICIENT_DATA" in verdicts
    # NaN metric values get coerced to None
    metric_values = [row[3] for row in db.signal_validations]
    assert all(v is None or isinstance(v, float) for v in metric_values)
