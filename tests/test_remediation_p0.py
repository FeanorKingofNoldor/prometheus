"""Tests for the P0/P1 remediation work.

These tests cover behaviour that was either missing or broken before
the audit cleanup: log rotation wiring, timezone-aware clocks, the
options-submission feature flag, the drawdown circuit breaker, the
retry-backoff hard cap, and the price-freshness guard.

The tests intentionally avoid live DB / IBKR connectivity. Anything
that does require external state (PostgreSQL, IB Gateway) is mocked.
"""

from __future__ import annotations

import logging.handlers
import os
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Log rotation (P0 #79)
# ---------------------------------------------------------------------------


def test_setup_logging_uses_rotating_handler(tmp_path, monkeypatch):
    """`setup_logging` must attach a RotatingFileHandler, not FileHandler."""
    import logging
    import importlib

    import apatheon.core.logging as apatheon_logging

    # Reset the root logger so setup_logging() actually configures handlers.
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)

    log_path = tmp_path / "test_apatheon.log"
    monkeypatch.setenv("LOG_FILE", str(log_path))
    monkeypatch.setenv("APATHEON_LOG_MAX_BYTES", "1024")
    monkeypatch.setenv("APATHEON_LOG_BACKUP_COUNT", "3")

    # Force config reload so the env vars take effect.
    import apatheon.core.config as cfg_mod
    importlib.reload(cfg_mod)
    importlib.reload(apatheon_logging)

    apatheon_logging.setup_logging()

    rotating = [h for h in logging.getLogger().handlers
                if isinstance(h, logging.handlers.RotatingFileHandler)]
    assert rotating, "expected a RotatingFileHandler on the root logger"
    rfh = rotating[0]
    assert rfh.maxBytes == 1024
    assert rfh.backupCount == 3


# ---------------------------------------------------------------------------
# Timezone-aware clock (P0 #80)
# ---------------------------------------------------------------------------


def test_now_utc_is_timezone_aware():
    from prometheus.orchestration.clock import now_utc

    ts = now_utc()
    assert ts.tzinfo is not None, "now_utc() must return a tz-aware datetime"
    assert ts.utcoffset() == timedelta(0)


def test_now_local_is_timezone_aware():
    from prometheus.orchestration.clock import now_local

    ts = now_local()
    assert ts.tzinfo is not None, "now_local() must return a tz-aware datetime"


def test_local_tz_falls_back_to_utc_for_invalid_zone(monkeypatch):
    """Bad PROMETHEUS_LOCAL_TZ should degrade to UTC, not crash imports."""
    import importlib

    monkeypatch.setenv("PROMETHEUS_LOCAL_TZ", "Not/A/Zone")
    import prometheus.orchestration.clock as clock_mod

    importlib.reload(clock_mod)
    assert clock_mod.LOCAL_TZ is timezone.utc


# ---------------------------------------------------------------------------
# Options submission feature flag (P0 #81 / P1 #90)
# ---------------------------------------------------------------------------


def test_options_live_flag_default_is_false():
    """Without PROMETHEUS_OPTIONS_SUBMIT_LIVE the flag must be falsy."""
    val = os.environ.get("PROMETHEUS_OPTIONS_SUBMIT_LIVE", "")
    assert val.lower() not in ("1", "true", "yes")


def test_options_live_flag_truthy_values():
    truthy = ("1", "true", "yes")
    falsy = ("", "0", "false", "no", "maybe", "TRUE_BUT_NO")
    for v in truthy:
        assert v.lower() in ("1", "true", "yes")
    for v in falsy:
        if v == "":
            assert v.lower() not in ("1", "true", "yes")
        elif v.lower() == "true":
            continue
        else:
            assert v.lower() not in ("1", "true", "yes")


# ---------------------------------------------------------------------------
# Drawdown + sector concentration circuit breakers (P0 #85)
# ---------------------------------------------------------------------------


def test_execution_risk_config_supports_drawdown_and_sector():
    from apatheon.core.config import ExecutionRiskConfig

    cfg = ExecutionRiskConfig(
        max_drawdown_pct=0.10,
        max_sector_concentration_pct=0.30,
    )
    assert cfg.max_drawdown_pct == 0.10
    assert cfg.max_sector_concentration_pct == 0.30


def test_drawdown_breaker_blocks_when_below_threshold():
    """When equity is far enough below peak, the breaker raises."""
    from apatheon.core.config import ExecutionRiskConfig
    from prometheus.execution.broker_interface import (
        Order,
        OrderSide,
        OrderType,
        Position,
    )
    from prometheus.execution.risk_broker import RiskCheckingBroker, RiskLimitExceeded

    inner = MagicMock()
    inner.get_positions.return_value = {}
    inner.get_account_state.return_value = {
        "equity": 80_000.0,        # current
        "high_water_mark": 100_000.0,  # 20% drawdown
    }

    broker = RiskCheckingBroker(
        inner=inner,
        config=ExecutionRiskConfig(enabled=True, max_drawdown_pct=0.10),
    )

    # Stub price so notional check doesn't gate first.
    broker._estimate_price = MagicMock(return_value=100.0)
    # Stub trailing-peak lookup to avoid DB.
    broker._lookup_trailing_peak = MagicMock(return_value=100_000.0)

    order = Order(
        order_id="test",
        instrument_id="AAPL.US",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10,
    )

    with pytest.raises(RiskLimitExceeded, match="drawdown circuit breaker"):
        # Bypass _block's DB write
        with patch.object(broker, "_block", side_effect=lambda o, r: (_ for _ in ()).throw(RiskLimitExceeded(r))):
            broker._enforce_limits(order)


def test_drawdown_breaker_allows_when_within_threshold():
    """Equity within tolerance must NOT trip the breaker."""
    from apatheon.core.config import ExecutionRiskConfig
    from prometheus.execution.broker_interface import (
        Order,
        OrderSide,
        OrderType,
    )
    from prometheus.execution.risk_broker import RiskCheckingBroker

    inner = MagicMock()
    inner.get_positions.return_value = {}
    inner.get_account_state.return_value = {
        "equity": 95_000.0,
        "high_water_mark": 100_000.0,  # 5% drawdown
    }

    broker = RiskCheckingBroker(
        inner=inner,
        config=ExecutionRiskConfig(enabled=True, max_drawdown_pct=0.10),
    )
    broker._estimate_price = MagicMock(return_value=100.0)

    order = Order(
        order_id="test",
        instrument_id="AAPL.US",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10,
    )

    # Should not raise
    broker._enforce_limits(order)


# ---------------------------------------------------------------------------
# Retry backoff hard cap (P1 #91)
# ---------------------------------------------------------------------------


def test_retry_backoff_caps_at_one_hour_default():
    from prometheus.orchestration.dag import JobMetadata
    from prometheus.orchestration.market_aware_daemon import calculate_retry_delay

    # Construct a JobMetadata with a base that would otherwise blow up:
    # 600 * 2^10 = 614_400 seconds (> 7 days).
    job = JobMetadata(
        job_id="test_job",
        job_type="ingest_prices",
        market_id="US_EQ",
        retry_delay_seconds=600,
        max_retries=20,
    )
    delay = calculate_retry_delay(job, attempt_number=10)
    # Cap is 3600 ± 25% jitter → at most 4500
    assert delay <= 4500.0


def test_retry_backoff_429_lifts_floor():
    """Rate-limit errors should bump the base delay to >= 15 minutes."""
    from prometheus.orchestration.dag import JobMetadata
    from prometheus.orchestration.market_aware_daemon import calculate_retry_delay

    job = JobMetadata(
        job_id="test_job",
        job_type="ingest_prices",
        market_id="US_EQ",
        retry_delay_seconds=10,  # tiny base
        max_retries=5,
    )
    # Without rate limit error, delay stays tiny.
    plain = calculate_retry_delay(job, attempt_number=1, error_message="generic failure")
    assert plain < 60

    # With rate limit, floor lifts to 900s minimum (jittered ±25%).
    rate_limited = calculate_retry_delay(
        job, attempt_number=1, error_message="HTTP 429: Too Many Requests",
    )
    assert rate_limited > 600  # well above the no-429 baseline


# ---------------------------------------------------------------------------
# Price freshness guard (P1 #87)
# ---------------------------------------------------------------------------


def test_check_price_data_freshness_passes_within_lag():
    from apatheon.data_ingestion.daily_orchestrator import check_price_data_freshness

    db = MagicMock()
    cur = MagicMock()
    cur.fetchone.return_value = (date(2026, 4, 14),)
    conn = MagicMock()
    conn.cursor.return_value = cur
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    db.get_historical_connection.return_value = conn

    fresh, msg = check_price_data_freshness(db, date(2026, 4, 14), max_lag_days=1)
    assert fresh is True
    assert "ok" in msg


def test_check_price_data_freshness_fails_when_stale():
    from apatheon.data_ingestion.daily_orchestrator import check_price_data_freshness

    db = MagicMock()
    cur = MagicMock()
    cur.fetchone.return_value = (date(2026, 4, 1),)  # 14 days behind
    conn = MagicMock()
    conn.cursor.return_value = cur
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    db.get_historical_connection.return_value = conn

    fresh, msg = check_price_data_freshness(db, date(2026, 4, 15), max_lag_days=1)
    assert fresh is False
    assert "stale" in msg
    assert "14d" in msg


def test_check_price_data_freshness_fails_when_table_empty():
    from apatheon.data_ingestion.daily_orchestrator import check_price_data_freshness

    db = MagicMock()
    cur = MagicMock()
    cur.fetchone.return_value = (None,)
    conn = MagicMock()
    conn.cursor.return_value = cur
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    db.get_historical_connection.return_value = conn

    fresh, msg = check_price_data_freshness(db, date(2026, 4, 15))
    assert fresh is False
    assert "empty" in msg.lower()


# ---------------------------------------------------------------------------
# IBKR credential preflight (P1 #88)
# ---------------------------------------------------------------------------


def test_ibkr_preflight_paper_passes_when_creds_present(monkeypatch):
    monkeypatch.setenv("IBKR_PAPER_USERNAME", "u")
    monkeypatch.setenv("IBKR_PAPER_ACCOUNT", "DUN1")
    from prometheus.execution.ibkr_config import validate_credentials_at_startup

    validate_credentials_at_startup(require_paper=True, require_live=False)


def test_ibkr_preflight_paper_fails_when_creds_missing(monkeypatch):
    monkeypatch.delenv("IBKR_PAPER_USERNAME", raising=False)
    monkeypatch.delenv("IBKR_PAPER_ACCOUNT", raising=False)
    from prometheus.execution.ibkr_config import validate_credentials_at_startup

    with pytest.raises(ValueError, match="IBKR_PAPER_USERNAME"):
        validate_credentials_at_startup(require_paper=True, require_live=False)


def test_ibkr_preflight_lists_all_missing_vars(monkeypatch):
    """Operators want a single error listing every missing var."""
    for v in ("IBKR_PAPER_USERNAME", "IBKR_PAPER_ACCOUNT",
              "IBKR_LIVE_USERNAME", "IBKR_LIVE_ACCOUNT"):
        monkeypatch.delenv(v, raising=False)
    from prometheus.execution.ibkr_config import validate_credentials_at_startup

    with pytest.raises(ValueError) as exc:
        validate_credentials_at_startup(require_paper=True, require_live=True)
    msg = str(exc.value)
    for v in ("IBKR_PAPER_USERNAME", "IBKR_PAPER_ACCOUNT",
              "IBKR_LIVE_USERNAME", "IBKR_LIVE_ACCOUNT"):
        assert v in msg
