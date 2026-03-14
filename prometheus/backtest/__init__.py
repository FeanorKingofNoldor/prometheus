"""Prometheus v2 – Backtesting public API.

This package exposes the core backtesting types used by integration
tests and higher-level orchestration, including the sleeve-level
BacktestRunner and helpers for building sleeve pipelines and running
multi-sleeve campaigns.
"""

from __future__ import annotations

from prometheus.backtest.analyzers import EquityCurveAnalyzer, EquityCurvePoint
from prometheus.backtest.campaign import SleeveRunSummary, run_backtest_campaign
from prometheus.backtest.config import SleeveConfig
from prometheus.backtest.runner import BacktestRunner

__all__ = [
    "BacktestRunner",
    "EquityCurveAnalyzer",
    "EquityCurvePoint",
    "SleeveConfig",
    "SleeveRunSummary",
    "run_backtest_campaign",
]
