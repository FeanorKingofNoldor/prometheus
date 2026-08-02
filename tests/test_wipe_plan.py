"""Tests for the extended wipe plan in wipe_paper_trading."""

from __future__ import annotations

from datetime import date

from prometheus.scripts.maintenance.wipe_paper_trading import (
    LIVE_PORTFOLIO_IDS,
    _build_plan,
)


def _plan_labels(**kw) -> list[str]:
    return [label for label, _sel, _del, _params in _build_plan(date(2026, 3, 1), **kw)]


def test_live_portfolio_ids_constant():
    # US book + account portfolio + the five regional books (multi-market
    # build 2026-07). The wipe must cover every live book's state.
    assert LIVE_PORTFOLIO_IDS == [
        "US_EQ_LONG_V12",
        "IBKR_PAPER",
        "UK_EQ_LONG_V1",
        "EU_EQ_LONG_V1",
        "HK_EQ_LONG_V1",
        "KR_EQ_LONG_V1",
        "AU_EQ_LONG_V1",
    ]


def test_plan_includes_new_feedback_tables():
    labels = _plan_labels()
    for expected in (
        "position_convictions",
        "target_portfolios",
        "trade_journal",
        "options_positions",
        "options_position_events",
        "derivatives_shadow_decisions",
        "meta_signal_validations",
        "backtest_live_drift",
        "weekly_reports",
        "meta_feedback_insights",
    ):
        assert expected in labels, f"missing wipe entry: {expected}"


def test_equity_history_only_included_when_flagged():
    assert "portfolio_equity_history" not in _plan_labels()
    assert "portfolio_equity_history" in _plan_labels(include_equity_history=True)


def test_option_events_deleted_before_positions():
    labels = _plan_labels()
    assert labels.index("options_position_events") < labels.index("options_positions")


def test_portfolio_scoped_entries_use_live_ids():
    plan = {label: (sel, del_, params)
            for label, sel, del_, params in _build_plan(date(2026, 3, 1))}
    for label in ("position_convictions", "target_portfolios"):
        sel, del_, params = plan[label]
        assert "portfolio_id = ANY(%s)" in sel
        assert "portfolio_id = ANY(%s)" in del_
        assert params == (LIVE_PORTFOLIO_IDS,)
    for label in ("options_positions", "options_position_events"):
        sel, del_, params = plan[label]
        assert "mode='PAPER' OR portfolio_id = ANY(%s)" in sel
        assert params == (LIVE_PORTFOLIO_IDS,)


def test_backup_and_delete_share_params():
    # The archive-before-delete pattern relies on the same params tuple
    # working for both statements — placeholder counts must match.
    for label, sel, del_, params in _build_plan(date(2026, 3, 1),
                                                include_equity_history=True):
        assert sel.count("%s") == del_.count("%s"), label
        assert sel.count("%s") == len(params), label
