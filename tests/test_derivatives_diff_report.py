"""Tests for prometheus.derivatives.diff_report."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date
from typing import Any

from prometheus.derivatives import diff_report
from prometheus.derivatives.backtest import LegacyOption

# ── Fake DB ──────────────────────────────────────────────────────────


class _FakeCursor:
    def __init__(self, db: "_FakeDb") -> None:
        self._db = db
        self._result: list[tuple] = []

    def execute(self, sql: str, args: Any = ()) -> None:
        sql_norm = " ".join(sql.split()).upper()
        if "FROM DERIVATIVES_SHADOW_DECISIONS" in sql_norm:
            d = args[0]
            self._result = [
                tuple(r[c] for c in (
                    "decision_id", "as_of_date", "sleeve", "template_name", "kind",
                    "underlying", "right", "strike", "expiry", "quantity",
                    "limit_price", "iv_used", "iv_source", "delta",
                    "reason", "skip_reason",
                ))
                for r in self._db.shadow_rows if r["as_of_date"] == d
            ]
        elif "FROM ENGINE_DECISIONS" in sql_norm:
            d = args[0]
            self._result = [
                (r["output_refs"],)
                for r in self._db.engine_decisions
                if r["as_of_date"] == d
            ]
        else:
            raise AssertionError(f"unhandled SQL: {sql_norm[:80]}")

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
    def __init__(self) -> None:
        self.shadow_rows: list[dict[str, Any]] = []
        self.engine_decisions: list[dict[str, Any]] = []

    @contextmanager
    def get_runtime_connection(self):
        yield _FakeConnection(self)


def _mk_shadow_row(
    *,
    as_of_date: date,
    sleeve: str = "HEDGE",
    template_name: str = "hedge.spy_protective_put",
    kind: str = "DIRECTIVE",
    underlying: str | None = "SPY",
    right: str | None = "P",
    strike: float | None = 480.0,
    expiry: str | None = "20260815",
    quantity: int | None = 3,
    iv_source: str | None = "ibkr_live",
    skip_reason: str | None = None,
    decision_id: int = 1,
) -> dict[str, Any]:
    return {
        "decision_id": decision_id, "as_of_date": as_of_date,
        "sleeve": sleeve, "template_name": template_name, "kind": kind,
        "underlying": underlying, "right": right,
        "strike": strike, "expiry": expiry, "quantity": quantity,
        "limit_price": 4.20, "iv_used": 0.22, "iv_source": iv_source,
        "delta": -0.27, "reason": "test", "skip_reason": skip_reason,
    }


def _mk_engine_decision(
    *,
    as_of_date: date,
    orders: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "as_of_date": as_of_date,
        "output_refs": {"orders": orders},
    }


# ── Loader tests ─────────────────────────────────────────────────────


def test_load_shadow_decisions_returns_typed_rows():
    db = _FakeDb()
    today = date(2026, 5, 24)
    db.shadow_rows.append(_mk_shadow_row(as_of_date=today))
    rows = diff_report.load_shadow_decisions(db, today)
    assert len(rows) == 1
    assert isinstance(rows[0], diff_report.ShadowDecisionRow)
    assert rows[0].template_name == "hedge.spy_protective_put"


def test_load_shadow_decisions_filters_by_date():
    db = _FakeDb()
    today = date(2026, 5, 24)
    other = date(2026, 5, 20)
    db.shadow_rows.append(_mk_shadow_row(as_of_date=today))
    db.shadow_rows.append(_mk_shadow_row(as_of_date=other))
    assert len(diff_report.load_shadow_decisions(db, today)) == 1
    assert len(diff_report.load_shadow_decisions(db, other)) == 1


def test_load_legacy_options_decisions_explodes_orders():
    db = _FakeDb()
    today = date(2026, 5, 24)
    db.engine_decisions.append(_mk_engine_decision(
        as_of_date=today,
        orders=[
            {"symbol": "SPY", "right": "P", "strike": 480.0,
             "expiry": "20260815", "quantity": 3, "action": "BUY",
             "strategy": "protective_put"},
            {"symbol": "VIX", "right": "C", "strike": 30.0,
             "expiry": "20260619", "quantity": 5, "action": "BUY",
             "strategy": "vix_tail_hedge"},
        ],
    ))
    legacy = diff_report.load_legacy_options_decisions(db, today)
    assert len(legacy) == 2
    assert {lo.strategy for lo in legacy} == {"protective_put", "vix_tail_hedge"}


def test_load_legacy_options_decisions_sell_action_flips_quantity_sign():
    db = _FakeDb()
    today = date(2026, 5, 24)
    db.engine_decisions.append(_mk_engine_decision(
        as_of_date=today,
        orders=[
            {"symbol": "SPY", "right": "P", "strike": 470.0,
             "expiry": "20260815", "quantity": 3, "action": "SELL",
             "strategy": "short_put"},
        ],
    ))
    legacy = diff_report.load_legacy_options_decisions(db, today)
    assert legacy[0].quantity == -3


# ── diff_decisions tests ─────────────────────────────────────────────


def test_diff_pairs_matching_shadow_and_legacy():
    today = date(2026, 5, 24)
    shadow = [diff_report.ShadowDecisionRow(
        decision_id=1, as_of_date=today, sleeve="HEDGE",
        template_name="hedge.spy_protective_put", kind="DIRECTIVE",
        underlying="SPY", right="P", strike=480.0, expiry="20260815",
        quantity=3, limit_price=4.20, iv_used=0.20,
        iv_source="ibkr_live", delta=-0.27, reason="…", skip_reason=None,
    )]
    legacy = [LegacyOption(
        as_of_date=today, symbol="SPY", right="P", strike=485.0,
        expiry="20260815", quantity=3, strategy="protective_put",
    )]
    summary = diff_report.diff_decisions(shadow=shadow, legacy=legacy)
    assert summary.by_kind == {"both": 1}
    assert summary.strike_divergence_count == 1


def test_diff_classifies_new_only_when_legacy_silent():
    today = date(2026, 5, 24)
    shadow = [diff_report.ShadowDecisionRow(
        decision_id=1, as_of_date=today, sleeve="HEDGE",
        template_name="hedge.spy_protective_put", kind="DIRECTIVE",
        underlying="SPY", right="P", strike=480.0, expiry="20260815",
        quantity=3, limit_price=4.20, iv_used=0.20,
        iv_source="ibkr_live", delta=-0.27, reason="…", skip_reason=None,
    )]
    summary = diff_report.diff_decisions(shadow=shadow, legacy=[])
    assert summary.by_kind == {"new_only": 1}


def test_diff_classifies_legacy_only_when_shadow_silent():
    today = date(2026, 5, 24)
    legacy = [LegacyOption(
        as_of_date=today, symbol="SPY", right="P", strike=485.0,
        expiry="20260815", quantity=3, strategy="protective_put",
    )]
    summary = diff_report.diff_decisions(shadow=[], legacy=legacy)
    assert summary.by_kind == {"legacy_only": 1}


def test_diff_skips_shadow_skip_rows():
    """Shadow SKIP rows should not pair with legacy or count as
    new_only — they're informational only."""
    today = date(2026, 5, 24)
    shadow = [diff_report.ShadowDecisionRow(
        decision_id=1, as_of_date=today, sleeve="HEDGE",
        template_name="hedge.spy_protective_put", kind="SKIP",
        underlying=None, right=None, strike=None, expiry=None,
        quantity=None, limit_price=None, iv_used=None,
        iv_source=None, delta=None,
        reason="SKIP[trigger_not_fired]", skip_reason="trigger_not_fired",
    )]
    legacy = [LegacyOption(
        as_of_date=today, symbol="SPY", right="P", strike=485.0,
        expiry="20260815", quantity=3, strategy="protective_put",
    )]
    summary = diff_report.diff_decisions(shadow=shadow, legacy=legacy)
    # The shadow SKIP is ignored; the legacy still appears as legacy_only.
    assert summary.by_kind == {"legacy_only": 1}


# ── format_diff_report tests ─────────────────────────────────────────


def test_format_report_includes_summary_and_per_entry_table():
    today = date(2026, 5, 24)
    shadow = [diff_report.ShadowDecisionRow(
        decision_id=1, as_of_date=today, sleeve="HEDGE",
        template_name="hedge.spy_protective_put", kind="DIRECTIVE",
        underlying="SPY", right="P", strike=480.0, expiry="20260815",
        quantity=3, limit_price=4.20, iv_used=0.20,
        iv_source="ibkr_live", delta=-0.27, reason="…", skip_reason=None,
    )]
    legacy = [LegacyOption(
        as_of_date=today, symbol="SPY", right="P", strike=485.0,
        expiry="20260815", quantity=3, strategy="protective_put",
    )]
    md = diff_report.format_diff_report(
        as_of_date=today, shadow_rows=shadow, legacy_decisions=legacy,
    )
    assert "# Derivatives shadow-vs-legacy diff — 2026-05-24" in md
    assert "Shadow directives" in md
    assert "hedge.spy_protective_put" in md
    assert "Strike divergence" in md
    # The single fired template should appear in the per-entry table
    assert "BOTH" in md


def test_format_report_handles_empty_day_gracefully():
    today = date(2026, 5, 24)
    md = diff_report.format_diff_report(
        as_of_date=today, shadow_rows=[], legacy_decisions=[],
    )
    assert "No entries today" in md


def test_format_report_lists_iv_sources_distribution():
    today = date(2026, 5, 24)
    shadow = [
        diff_report.ShadowDecisionRow(
            decision_id=i, as_of_date=today, sleeve="HEDGE",
            template_name="t", kind="DIRECTIVE",
            underlying="SPY", right="P", strike=480.0, expiry="20260815",
            quantity=1, limit_price=1.0, iv_used=0.2, iv_source=src,
            delta=-0.27, reason="…", skip_reason=None,
        )
        for i, src in enumerate(["ibkr_live", "ibkr_live", "vix_fallback"])
    ]
    md = diff_report.format_diff_report(
        as_of_date=today, shadow_rows=shadow, legacy_decisions=[],
    )
    assert "IV source distribution" in md
    assert "`ibkr_live`: 2" in md
    assert "`vix_fallback`: 1" in md


def test_run_daily_diff_end_to_end():
    db = _FakeDb()
    today = date(2026, 5, 24)
    db.shadow_rows.append(_mk_shadow_row(
        as_of_date=today, template_name="hedge.spy_protective_put",
    ))
    db.engine_decisions.append(_mk_engine_decision(
        as_of_date=today,
        orders=[{
            "symbol": "SPY", "right": "P", "strike": 480.0,
            "expiry": "20260815", "quantity": 3, "action": "BUY",
            "strategy": "protective_put",
        }],
    ))
    md = diff_report.run_daily_diff(db, today)
    assert "BOTH" in md
    assert "Strike divergence (both fired, different strikes): **0**" in md
