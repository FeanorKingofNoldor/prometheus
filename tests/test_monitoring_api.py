"""Tests for the Prometheus monitoring/meta/control FastAPI endpoints.

These tests exercise the business logic (P&L calculation, capital-flow
filtering, alias mapping, config formatting) by mocking the database
layer so no real Postgres connection is needed.
"""

from __future__ import annotations

import math
from contextlib import contextmanager
from datetime import date, datetime, timezone
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# Mock DB infrastructure
# ---------------------------------------------------------------------------

class _MockCursor:
    """Tracks execute() calls and returns canned data from a dispatch table."""

    def __init__(self, dispatch: Dict[str, Any] | None = None):
        self._dispatch = dispatch or {}
        self._result: Any = None
        self.last_sql: str | None = None

    def execute(self, sql: str, params: Any = None) -> None:
        self.last_sql = sql
        # Walk through dispatch keys; if the key appears as a substring in the
        # SQL, use its value.
        for key, value in self._dispatch.items():
            if key in sql:
                self._result = value
                return
        self._result = []

    def fetchone(self) -> Any:
        if isinstance(self._result, list):
            return self._result[0] if self._result else None
        return self._result

    def fetchall(self) -> list:
        if isinstance(self._result, list):
            return self._result
        return [self._result] if self._result is not None else []

    def close(self) -> None:
        pass


class _MockConnection:
    def __init__(self, cursor: _MockCursor):
        self._cursor = cursor

    def cursor(self) -> _MockCursor:
        return self._cursor


def _build_mock_db(runtime_dispatch: Dict[str, Any] | None = None,
                   historical_dispatch: Dict[str, Any] | None = None) -> MagicMock:
    """Return a mock db_manager whose connections yield cursors backed by *dispatch*."""
    runtime_cursor = _MockCursor(runtime_dispatch or {})
    hist_cursor = _MockCursor(historical_dispatch or {})

    runtime_conn = _MockConnection(runtime_cursor)
    hist_conn = _MockConnection(hist_cursor)

    db = MagicMock()

    # Context managers must be reentrant: each endpoint may open multiple
    # connections (e.g. get_status_overview opens ~4 separate blocks).
    @contextmanager
    def _rt():
        yield runtime_conn

    @contextmanager
    def _ht():
        yield hist_conn

    db.get_runtime_connection = _rt
    db.get_historical_connection = _ht
    return db


# ---------------------------------------------------------------------------
# Fixture: patched FastAPI app
# ---------------------------------------------------------------------------

@pytest.fixture()
def client_and_db():
    """Yield (TestClient, mock_db_setter) where mock_db_setter(dispatch) swaps
    the database dispatch table between requests."""

    # We need to patch get_db_manager everywhere it's imported.
    _db_holder: Dict[str, Any] = {"db": _build_mock_db()}

    def _get_db():
        return _db_holder["db"]

    # Force-import modules so patch targets resolve
    import prometheus.monitoring.api  # noqa: F401
    import prometheus.monitoring.meta_api  # noqa: F401
    import prometheus.monitoring.control_api  # noqa: F401

    patches = [
        patch("prometheus.monitoring.api.get_db_manager", _get_db),
        patch("prometheus.monitoring.meta_api.get_db_manager", _get_db),
        patch("prometheus.monitoring.control_api.get_db_manager", _get_db),
        # Also patch the top-level import used by meta_api.get_performance
        patch("apathis.core.database.get_db_manager", _get_db),
    ]
    for p in patches:
        p.start()

    # Patch the scheduler lock and startup tasks to avoid side effects
    with patch("prometheus.monitoring.app._acquire_scheduler_lock", return_value=True), \
         patch("prometheus.monitoring.app._start_internal_schedulers"):
        from prometheus.monitoring.app import app
        client = TestClient(app, raise_server_exceptions=False)

        def set_db(runtime_dispatch=None, historical_dispatch=None):
            _db_holder["db"] = _build_mock_db(runtime_dispatch, historical_dispatch)

        yield client, set_db

    for p in patches:
        p.stop()


# ============================================================================
# /status/overview
# ============================================================================


class TestStatusOverview:
    """Tests for GET /api/status/overview."""

    def test_returns_expected_fields(self, client_and_db):
        client, set_db = client_and_db

        today = date.today()
        year_start = today.replace(month=1, day=1)

        set_db(runtime_dispatch={
            "MAX(as_of_date) FROM portfolio_risk_reports": (today,),
            "AVG(net_exposure)": (0.65, 0.80, 1.05),
            "MAX(as_of_date) FROM stability_vectors": (today,),
            "AVG(overall_score)": (25.0,),
            "regime_label, confidence": (f"STABLE_EXPANSION", 0.90),
            "latest_per_day": [
                (today, 100_000.0),
            ],
        })

        resp = client.get("/api/status/overview")
        assert resp.status_code == 200
        data = resp.json()

        # Required top-level KPI fields
        for field in ("pnl_today", "pnl_mtd", "pnl_ytd", "max_drawdown",
                      "net_exposure", "gross_exposure", "leverage",
                      "global_stability_index", "regimes", "alerts"):
            assert field in data, f"Missing field: {field}"

    def test_capital_flow_filtering_in_pnl(self, client_and_db):
        """P&L must exclude days where NLV jumps >15% (capital flows)."""
        client, set_db = client_and_db

        today = date.today()
        d0 = date(today.year, 1, 2)
        d1 = date(today.year, 1, 3)
        d2 = date(today.year, 1, 6)  # deposit day — NLV jumps 50%
        d3 = date(today.year, 1, 7)

        # NLV series: 100k, 101k, 151k (deposit), 152k
        nlv_rows = [
            (d0, 100_000.0),
            (d1, 101_000.0),
            (d2, 151_000.0),  # >15% jump = capital flow
            (d3, 152_000.0),
        ]

        set_db(runtime_dispatch={
            "MAX(as_of_date) FROM portfolio_risk_reports": (None,),
            "AVG(net_exposure)": None,
            "MAX(as_of_date) FROM stability_vectors": (None,),
            "AVG(overall_score)": None,
            "regime_label, confidence": None,
            "latest_per_day": nlv_rows,
        })

        resp = client.get("/api/status/overview")
        assert resp.status_code == 200
        data = resp.json()

        # The YTD P&L should exclude the d2 jump day.
        # d0->d1 = +1000, d2 is flow (skipped), d2->d3 = +1000 (daily pct ~0.66%, included)
        # Expected YTD = 1000 + 1000 = 2000
        assert data["pnl_ytd"] == pytest.approx(2000.0, abs=1.0)


# ============================================================================
# /status/portfolio
# ============================================================================


class TestStatusPortfolio:
    """Tests for GET /api/status/portfolio."""

    def test_returns_positions(self, client_and_db):
        client, set_db = client_and_db

        today = date.today()

        set_db(runtime_dispatch={
            # eff_date from risk reports
            "MAX(as_of_date) FROM portfolio_risk_reports": (today,),
            # target_portfolios
            "target_positions": ({"weights": {"AAPL.US": 0.25, "MSFT.US": 0.35}},),
            # risk report row
            "risk_metrics, exposures_by_sector": (
                {"net_liquidation": 500_000, "total_cash": 25_000},
                {"Technology": 0.60},
                {"momentum": 0.10},
                {},
            ),
            # positions_snapshots max timestamp
            "MAX(timestamp) FROM positions_snapshots": (datetime(2026, 1, 15, 16, 0, 0),),
            # positions from snapshot
            "instrument_id, quantity, avg_cost, market_value": [
                ("AAPL.US", 100, 150.0, 17500.0, 2500.0, "PAPER"),
                ("MSFT.US", 50, 300.0, 16000.0, 1000.0, "PAPER"),
            ],
            # P&L NLV rows
            "latest_per_day": [
                (today, 33500.0),
            ],
        })

        resp = client.get("/api/status/portfolio?portfolio_id=IBKR_PAPER")
        assert resp.status_code == 200
        data = resp.json()

        assert data["portfolio_id"] == "IBKR_PAPER"
        assert len(data["positions"]) >= 1
        # Each position should have instrument_id and market_value
        for pos in data["positions"]:
            assert "instrument_id" in pos
            assert "market_value" in pos


# ============================================================================
# /status/execution — IBKR_PAPER alias mapping
# ============================================================================


class TestStatusExecution:
    """Tests for GET /api/status/execution."""

    def test_ibkr_paper_alias_mapping(self, client_and_db):
        """IBKR_PAPER should query both IBKR_PAPER and US_EQ_ALLOCATOR."""
        client, set_db = client_and_db

        set_db(runtime_dispatch={
            # Orders — the SQL uses IN (IBKR_PAPER, US_EQ_ALLOCATOR)
            "FROM orders": [
                ("ord-1", datetime(2026, 1, 15, 10, 0), "AAPL.US", "BUY", "MARKET", 100, "FILLED", "PAPER", "dec-1"),
                ("ord-2", datetime(2026, 1, 15, 10, 1), "MSFT.US", "SELL", "LIMIT", 50, "PENDING", "PAPER", None),
            ],
            # Fills
            "FROM fills": [],
            # Positions
            "MAX(timestamp) FROM positions_snapshots": (None,),
        })

        resp = client.get("/api/status/execution?portfolio_id=IBKR_PAPER")
        assert resp.status_code == 200
        data = resp.json()

        assert data["portfolio_id"] == "IBKR_PAPER"
        assert len(data["orders"]) == 2
        assert data["orders"][0]["instrument_id"] == "AAPL.US"
        assert data["orders"][0]["status"] == "FILLED"

    def test_non_ibkr_portfolio_no_alias(self, client_and_db):
        """A non-IBKR portfolio should only query its own portfolio_id."""
        client, set_db = client_and_db

        set_db(runtime_dispatch={
            "FROM orders": [],
            "FROM fills": [],
            "MAX(timestamp) FROM positions_snapshots": (None,),
        })

        resp = client.get("/api/status/execution?portfolio_id=MY_BACKTEST")
        assert resp.status_code == 200
        data = resp.json()
        assert data["portfolio_id"] == "MY_BACKTEST"
        assert data["orders"] == []


# ============================================================================
# /meta/configs
# ============================================================================


class TestMetaConfigs:
    """Tests for GET /api/meta/configs."""

    @patch("prometheus.monitoring.meta_api.load_meta_policy_artifact")
    @patch("prometheus.monitoring.meta_api.get_config")
    @patch("prometheus.monitoring.meta_api.load_execution_policy_artifact")
    @patch("prometheus.monitoring.meta_api._load_daily_portfolio_risk_config")
    @patch("prometheus.monitoring.meta_api._load_daily_universe_lambda_config")
    def test_returns_config_rows(self, mock_univ, mock_port, mock_exec, mock_cfg, mock_meta,
                                  client_and_db):
        client, _ = client_and_db

        # Universe config mock
        univ_cfg = MagicMock()
        univ_cfg.score_weight = 0.5
        univ_cfg.experiment_id = "exp-001"
        univ_cfg.predictions_csv = "/data/preds.csv"
        mock_univ.return_value = univ_cfg

        # Portfolio config mock
        port_cfg = MagicMock()
        port_cfg.hazard_profile = "MODERATE"
        port_cfg.meta_budget_enabled = True
        port_cfg.meta_budget_alpha = 0.3
        port_cfg.meta_budget_min = 0.1
        mock_port.return_value = port_cfg

        # Execution policy mock
        exec_artifact = MagicMock()
        turnover = MagicMock()
        turnover.one_way_limit = 0.15
        exec_artifact.policy.turnover = turnover
        exec_artifact.policy.no_trade_band_bps = 50
        exec_artifact.policy.cash_buffer_weight = 0.02
        mock_exec.return_value = exec_artifact

        # Execution risk mock
        exec_risk = MagicMock()
        exec_risk.max_order_notional = 0.0
        exec_risk.max_position_notional = 50000.0
        exec_risk.max_leverage = 1.5
        mock_cfg.return_value = MagicMock(execution_risk=exec_risk)

        # Meta policy mock — no policy for US_EQ
        meta_artifact = MagicMock()
        meta_artifact.policies = {}
        mock_meta.return_value = meta_artifact

        resp = client.get("/api/meta/configs")
        assert resp.status_code == 200
        data = resp.json()

        # Response must be a list of ConfigRow dicts
        assert isinstance(data, list)
        assert len(data) > 0

        # Each row must have the ConfigRow shape
        for row in data:
            assert "key" in row
            assert "value" in row
            assert "section" in row
            assert "editable" in row

        # Verify specific values propagated correctly
        sections = {r["section"] for r in data}
        assert "Universe" in sections
        assert "Execution" in sections
        assert "Execution Risk" in sections

        # Check that unconstrained renders properly
        risk_rows = {r["key"]: r["value"] for r in data if r["section"] == "Execution Risk"}
        assert risk_rows["risk.max_order_notional"] == "unconstrained"
        assert risk_rows["risk.max_position_notional"] == "50000.0"


# ============================================================================
# /meta/performance
# ============================================================================


class TestMetaPerformance:
    """Tests for GET /api/meta/performance."""

    def test_returns_flat_dict(self, client_and_db):
        client, set_db = client_and_db

        set_db(runtime_dispatch={
            # Backtest run
            "FROM backtest_runs": (
                {"annualised_sharpe": 1.5, "cumulative_return": 0.25, "max_drawdown": 0.08, "win_rate": 0.55},
                "strat-1",
                "2025-01-01",
                "2025-12-31",
            ),
            # NLV series for live Sharpe
            "latest_per_day": [
                (date(2026, 1, 2), 100_000.0),
                (date(2026, 1, 3), 100_500.0),
                (date(2026, 1, 6), 101_200.0),
                (date(2026, 1, 7), 100_800.0),
                (date(2026, 1, 8), 101_500.0),
            ],
        })

        resp = client.get("/api/meta/performance")
        assert resp.status_code == 200
        data = resp.json()

        # Must be flat dict (not nested)
        assert isinstance(data, dict)
        for v in data.values():
            assert not isinstance(v, dict), "performance response must be flat, not nested"

        # Backtest metrics present
        assert "backtest_sharpe" in data
        assert data["backtest_sharpe"] == 1.5

        # Live metrics present
        assert "live_sharpe" in data
        assert "live_ann_vol" in data
        assert "live_days" in data

    def test_capital_flow_excluded_from_live_sharpe(self, client_and_db):
        """NLV jumps >15% should be excluded from Sharpe calculation."""
        client, set_db = client_and_db

        set_db(runtime_dispatch={
            "FROM backtest_runs": None,
            "latest_per_day": [
                (date(2026, 1, 2), 100_000.0),
                (date(2026, 1, 3), 101_000.0),  # +1%
                (date(2026, 1, 6), 150_000.0),  # +48.5% = capital flow, excluded
                (date(2026, 1, 7), 151_500.0),  # +1%
                (date(2026, 1, 8), 152_500.0),  # +0.66%
            ],
        })

        resp = client.get("/api/meta/performance")
        assert resp.status_code == 200
        data = resp.json()

        # Only 3 daily returns should be used (d2 flow excluded)
        assert data.get("live_days") == 3


# ============================================================================
# /meta/engine_parameters
# ============================================================================


class TestMetaEngineParameters:
    """Tests for GET /api/meta/engine_parameters."""

    @patch("prometheus.monitoring.meta_api.load_book_registry")
    @patch("prometheus.monitoring.meta_api.load_meta_policy_artifact")
    @patch("prometheus.monitoring.meta_api.get_config")
    @patch("prometheus.monitoring.meta_api.load_execution_policy_artifact")
    @patch("prometheus.monitoring.meta_api._load_daily_portfolio_risk_config")
    @patch("prometheus.monitoring.meta_api._load_daily_universe_lambda_config")
    def test_includes_assessment_engine(self, mock_univ, mock_port, mock_exec, mock_cfg,
                                         mock_meta, mock_book_reg, client_and_db):
        client, _ = client_and_db

        # Minimal mocks so the endpoint doesn't crash
        univ_cfg = MagicMock()
        univ_cfg.predictions_csv = "/data/preds.csv"
        univ_cfg.experiment_id = "exp-1"
        univ_cfg.score_weight = 0.5
        mock_univ.return_value = univ_cfg

        port_cfg = MagicMock()
        port_cfg.hazard_profile = "MODERATE"
        port_cfg.scenario_risk_set_id = "scen-1"
        port_cfg.stab_scenario_set_id = "stab-1"
        port_cfg.meta_budget_enabled = True
        port_cfg.meta_budget_alpha = 0.3
        port_cfg.meta_budget_min = 0.1
        mock_port.return_value = port_cfg

        exec_artifact = MagicMock()
        turnover = MagicMock()
        turnover.one_way_limit = 0.15
        exec_artifact.policy.turnover = turnover
        exec_artifact.policy.no_trade_band_bps = 50
        exec_artifact.policy.cash_buffer_weight = 0.02
        mock_exec.return_value = exec_artifact

        exec_risk = MagicMock()
        exec_risk.max_order_notional = 0.0
        exec_risk.max_position_notional = 0.0
        exec_risk.max_leverage = 0.0
        mock_cfg.return_value = MagicMock(execution_risk=exec_risk)

        meta_artifact = MagicMock()
        meta_artifact.policies = {}
        mock_meta.return_value = meta_artifact

        mock_book_reg.return_value = MagicMock()

        # Patch the BasicAssessmentModel dataclass fields
        import dataclasses

        @dataclasses.dataclass
        class _FakeAssessment:
            momentum_window_days: int = 126
            momentum_ref: float = 0.20
            fragility_penalty_weight: float = 0.15
            strong_buy_threshold: float = 0.03
            sell_threshold: float = 0.01
            max_workers: int = 1

        with patch("prometheus.monitoring.meta_api.BasicAssessmentModel", _FakeAssessment):
            resp = client.get("/api/meta/engine_parameters")

        assert resp.status_code == 200
        data = resp.json()

        # Must have the EngineParametersResponse shape
        assert "generated_at" in data
        assert "engines" in data
        assert isinstance(data["engines"], list)

        # Find the Assessment Engine group
        engine_ids = [e["engine_id"] for e in data["engines"]]
        assert "ASSESSMENT_ENGINE" in engine_ids

        ae = next(e for e in data["engines"] if e["engine_id"] == "ASSESSMENT_ENGINE")
        param_keys = [p["key"] for p in ae["parameters"]]
        assert "momentum_window_days" in param_keys
        assert "fragility_penalty_weight" in param_keys

        # Each parameter should have key, value, source, detrimental_reason
        for p in ae["parameters"]:
            assert "key" in p
            assert "value" in p
            assert "source" in p
            assert "detrimental_reason" in p


# ============================================================================
# /control/ibkr_status
# ============================================================================


class TestIbkrStatus:
    """Tests for GET /api/control/ibkr_status."""

    @patch("prometheus.monitoring.control_api._tcp_probe")
    def test_connected_when_gateway_up(self, mock_probe, client_and_db):
        client, _ = client_and_db

        # Gateway reachable, TWS not
        mock_probe.side_effect = [
            (True, 1.2, ""),     # Gateway (Paper)
            (False, 2000.0, "Connection refused"),  # TWS (Paper)
        ]

        resp = client.get("/api/control/ibkr_status")
        assert resp.status_code == 200
        data = resp.json()

        assert data["status"] == "connected"
        assert data["mode"] == "PAPER"
        assert len(data["endpoints"]) == 2

        gw = next(ep for ep in data["endpoints"] if "Gateway" in ep["label"])
        assert gw["reachable"] is True

    @patch("prometheus.monitoring.control_api._tcp_probe")
    def test_disconnected_when_nothing_up(self, mock_probe, client_and_db):
        client, _ = client_and_db

        mock_probe.side_effect = [
            (False, 2000.0, "Connection refused"),
            (False, 2000.0, "Connection refused"),
        ]

        resp = client.get("/api/control/ibkr_status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "disconnected"

    @patch("prometheus.monitoring.control_api._tcp_probe")
    def test_connected_when_tws_up(self, mock_probe, client_and_db):
        """TWS alone should also yield connected status."""
        client, _ = client_and_db

        mock_probe.side_effect = [
            (False, 2000.0, "Connection refused"),  # Gateway
            (True, 0.8, ""),                          # TWS
        ]

        resp = client.get("/api/control/ibkr_status")
        assert resp.status_code == 200
        assert resp.json()["status"] == "connected"


# ============================================================================
# /status/portfolio_equity — flow-adjusted equity chart
# ============================================================================


class TestPortfolioEquity:
    """Tests for GET /api/status/portfolio_equity."""

    def test_flow_adjusted_equity_chart(self, client_and_db):
        """Equity chart must rebase on capital-flow days (>15% NLV jump)."""
        client, set_db = client_and_db

        set_db(
            runtime_dispatch={
                "positions_snapshots": [
                    (date(2026, 1, 2), 100_000.0),
                    (date(2026, 1, 3), 101_000.0),   # +1%
                    (date(2026, 1, 6), 151_000.0),   # deposit (+49.5%)
                    (date(2026, 1, 7), 152_510.0),   # +1%
                ],
            },
            historical_dispatch={
                "prices_daily": [
                    (date(2026, 1, 2), 450.0),
                    (date(2026, 1, 3), 454.5),
                    (date(2026, 1, 6), 459.0),
                    (date(2026, 1, 7), 463.6),
                ],
            },
        )

        resp = client.get("/api/status/portfolio_equity?portfolio_id=TEST&benchmark=SPY.US")
        assert resp.status_code == 200
        data = resp.json()

        if data:
            # The flow-adjusted equity should not show the ~50% jump
            # Day 0: 100000, Day 1: 101000 (+1%), Day 2: rebased (flat),
            # Day 3: 101000*(1+1%) = 102010
            equities = [pt["portfolio"] for pt in data if pt["portfolio"] is not None]
            if len(equities) >= 3:
                # The jump from day 1 to day 2 should be flattened
                day1_to_day2_ret = (equities[2] - equities[1]) / equities[1]
                assert abs(day1_to_day2_ret) < 0.01, (
                    f"Flow day should be flat; got {day1_to_day2_ret:.2%} return"
                )


# ============================================================================
# Health and root endpoints
# ============================================================================


class TestHealthEndpoints:
    """Basic health/root endpoint tests."""

    def test_root(self, client_and_db):
        client, _ = client_and_db
        resp = client.get("/")
        assert resp.status_code == 200
        data = resp.json()
        assert data["service"] == "Prometheus C2 Backend"
        assert data["status"] == "operational"

    def test_health(self, client_and_db):
        client, _ = client_and_db
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "healthy"
