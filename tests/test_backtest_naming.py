"""Tests for the backtest namespace guard (prometheus/backtest/naming.py)."""

from __future__ import annotations

import pytest

from prometheus.backtest.naming import BacktestNamespaceError, assert_backtest_namespace


def test_bt_prefixed_ids_pass():
    assert_backtest_namespace("BT_US_EQ_LONG", "LAMBDA_FACT_PORT_L5", "CPP_UI_PARITY_X")


def test_live_ids_rejected_even_with_override(monkeypatch):
    monkeypatch.setenv("PROMETHEUS_ALLOW_LIVE_NAMESPACE_BACKTEST", "1")
    with pytest.raises(BacktestNamespaceError, match="LIVE id"):
        assert_backtest_namespace("US_CORE_LONG_EQ")
    with pytest.raises(BacktestNamespaceError, match="LIVE id"):
        assert_backtest_namespace("us_eq_long_v12")


def test_unprefixed_id_rejected():
    with pytest.raises(BacktestNamespaceError, match="lacks a backtest prefix"):
        assert_backtest_namespace("MY_EXPERIMENT")


def test_unprefixed_id_allowed_with_override(monkeypatch):
    monkeypatch.setenv("PROMETHEUS_ALLOW_LIVE_NAMESPACE_BACKTEST", "1")
    assert_backtest_namespace("MY_EXPERIMENT")


def test_none_and_empty_skipped():
    assert_backtest_namespace(None, "", "BT_OK")
