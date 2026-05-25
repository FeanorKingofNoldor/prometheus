"""Tests for prometheus.meta.notifications."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from typing import Any

from prometheus.meta import notifications

# ── Fake DB ──────────────────────────────────────────────────────────


class _FakeCursor:
    def __init__(self, db: "_FakeDb") -> None:
        self._db = db
        self._result: list[tuple] = []
        self.rowcount: int = 0

    def execute(self, sql: str, args: Any = ()) -> None:
        norm = " ".join(sql.split()).upper()

        if norm.startswith("INSERT INTO NOTIFICATIONS"):
            (as_of, kind, severity, title, body,
             src_table, src_id, link, md) = args
            key = (as_of, kind, src_id)
            if key in self._db._seen_keys:
                self.rowcount = 0
                return
            self._db._seen_keys.add(key)
            self._db.notifications.append({
                "as_of_date": as_of, "kind": kind, "severity": severity,
                "title": title, "body": body,
                "source_table": src_table, "source_id": src_id,
                "link_path": link, "metadata_json": md,
                "read_at": None, "dismissed_at": None,
                "notification_id": len(self._db.notifications) + 1,
                "created_at": datetime.now(timezone.utc),
            })
            self.rowcount = 1
            return

        if "FROM META_CONFIG_PROPOSALS" in norm:
            as_of, min_conf = args
            self._result = [
                (p["proposal_id"], p["strategy_id"], p["proposal_type"],
                 p["target_component"], p["confidence_score"],
                 p["expected_sharpe_improvement"], p["rationale"])
                for p in self._db.proposals
                if p["created_at"].date() == as_of
                and float(p["confidence_score"]) >= float(min_conf)
            ]
            return

        if "FROM META_FEEDBACK_INSIGHTS" in norm:
            as_of = args[0]
            self._result = [
                (i["insight_id"], i["category"], i["message"],
                 i["metric_name"], i["metric_value"], i["benchmark"])
                for i in self._db.feedback_insights
                if i["as_of_date"] == as_of and i["severity"] == "critical"
            ]
            return

        if "FROM META_SIGNAL_VALIDATIONS" in norm:
            start, end, bad_list, min_bad = args
            grouped: dict[str, list[str]] = {}
            for v in self._db.signal_validations:
                if start <= v["as_of_date"] <= end and v["verdict"] in bad_list:
                    grouped.setdefault(v["signal_name"], []).append(v["verdict"])
            self._result = [
                (name, len(verdicts), verdicts[-1])
                for name, verdicts in grouped.items()
                if len(verdicts) >= int(min_bad)
            ]
            return

        if "FROM META_DIAGNOSTIC_REPORTS" in norm:
            as_of = args[0]
            self._result = [
                (r["report_id"], r["strategy_id"],
                 r["has_underperformers"], r["has_high_risk"],
                 r["num_runs_analysed"])
                for r in self._db.diagnostic_reports
                if r["as_of_date"] == as_of
                and (r["has_underperformers"] or r["has_high_risk"])
            ]
            return

        if "FROM BACKTEST_LIVE_DRIFT" in norm:
            as_of = args[0]
            self._result = [
                (r.get("drift_id", 0), r.get("strategy_id"),
                 r.get("horizon_days"), r.get("sharpe_delta"),
                 r.get("live_sharpe"), r.get("backtest_sharpe"),
                 r.get("severity"), r.get("notes"))
                for r in getattr(self._db, "drift_rows", [])
                if r.get("as_of_date") == as_of
                and r.get("severity") in ("warning", "critical")
            ]
            return

        if "FROM NOTIFICATIONS" in norm:
            (limit,) = args
            unread = [
                (n["notification_id"], n["created_at"], n["as_of_date"],
                 n["kind"], n["severity"], n["title"], n["body"],
                 n["source_table"], n["source_id"], n["link_path"],
                 n["metadata_json"])
                for n in self._db.notifications
                if n["read_at"] is None and n["dismissed_at"] is None
            ]
            unread.sort(key=lambda r: r[1], reverse=True)
            self._result = unread[:limit]
            return

        if "UPDATE NOTIFICATIONS SET READ_AT" in norm:
            (nid,) = args
            for n in self._db.notifications:
                if n["notification_id"] == int(nid) and n["read_at"] is None:
                    n["read_at"] = datetime.now(timezone.utc)
                    self.rowcount = 1
                    return
            self.rowcount = 0
            return

        if "UPDATE NOTIFICATIONS SET DISMISSED_AT" in norm:
            (nid,) = args
            for n in self._db.notifications:
                if n["notification_id"] == int(nid) and n["dismissed_at"] is None:
                    n["dismissed_at"] = datetime.now(timezone.utc)
                    self.rowcount = 1
                    return
            self.rowcount = 0
            return

        raise AssertionError(f"unhandled SQL: {norm[:80]}")

    def fetchone(self) -> tuple | None:
        return self._result[0] if self._result else None

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
        self.proposals: list[dict[str, Any]] = []
        self.feedback_insights: list[dict[str, Any]] = []
        self.signal_validations: list[dict[str, Any]] = []
        self.diagnostic_reports: list[dict[str, Any]] = []
        self.notifications: list[dict[str, Any]] = []
        self._seen_keys: set[tuple] = set()

    @contextmanager
    def get_runtime_connection(self):
        yield _FakeConnection(self)


# ── record_notification ─────────────────────────────────────────────


def test_record_notification_inserts_one_row():
    db = _FakeDb()
    inserted = notifications.record_notification(
        db, as_of_date=date(2026, 5, 25),
        kind="proposal_pending", severity="warning",
        title="Test notification", source_id="abc",
    )
    assert inserted is True
    assert len(db.notifications) == 1


def test_record_notification_is_idempotent_on_same_key():
    db = _FakeDb()
    inserted_1 = notifications.record_notification(
        db, as_of_date=date(2026, 5, 25), kind="x",
        severity="info", title="t", source_id="abc",
    )
    inserted_2 = notifications.record_notification(
        db, as_of_date=date(2026, 5, 25), kind="x",
        severity="info", title="t", source_id="abc",
    )
    assert inserted_1 is True
    assert inserted_2 is False
    assert len(db.notifications) == 1


def test_record_notification_different_dates_both_insert():
    db = _FakeDb()
    notifications.record_notification(
        db, as_of_date=date(2026, 5, 25), kind="x",
        severity="info", title="t", source_id="abc",
    )
    notifications.record_notification(
        db, as_of_date=date(2026, 5, 26), kind="x",
        severity="info", title="t", source_id="abc",
    )
    assert len(db.notifications) == 2


# ── Rule: proposal_pending ──────────────────────────────────────────


def test_proposal_pending_records_one_per_proposal_above_threshold():
    db = _FakeDb()
    today = date(2026, 5, 25)
    db.proposals.append({
        "proposal_id": "prop-1", "strategy_id": "US_EQ_CORE_LONG_EQ",
        "proposal_type": "universe_adjustment",
        "target_component": "max_names",
        "confidence_score": 0.6,
        "expected_sharpe_improvement": 0.15,
        "rationale": "Top-30 names beat top-20 over 50-run sample",
        "created_at": datetime(2026, 5, 25, 18, 0, tzinfo=timezone.utc),
    })
    result = notifications.evaluate_daily_alerts(db, today)
    proposal_eval = next(
        e for e in result.evaluations if e.kind == "proposal_pending"
    )
    assert proposal_eval.recorded == 1
    n = db.notifications[0]
    assert n["kind"] == "proposal_pending"
    assert n["severity"] == "warning"  # 0.6 is in warning band
    assert "max_names" in n["title"]


def test_proposal_pending_severity_critical_at_high_confidence():
    db = _FakeDb()
    today = date(2026, 5, 25)
    db.proposals.append({
        "proposal_id": "p", "strategy_id": "S",
        "proposal_type": "x", "target_component": "y",
        "confidence_score": 0.85,
        "expected_sharpe_improvement": 0.4,
        "rationale": "",
        "created_at": datetime(2026, 5, 25, 18, 0, tzinfo=timezone.utc),
    })
    notifications.evaluate_daily_alerts(db, today)
    assert db.notifications[0]["severity"] == "critical"


def test_proposal_pending_skips_below_threshold():
    db = _FakeDb()
    today = date(2026, 5, 25)
    db.proposals.append({
        "proposal_id": "p", "strategy_id": "S",
        "proposal_type": "x", "target_component": "y",
        "confidence_score": 0.25,  # below default 0.4
        "expected_sharpe_improvement": 0.15,
        "rationale": "",
        "created_at": datetime(2026, 5, 25, 18, 0, tzinfo=timezone.utc),
    })
    notifications.evaluate_daily_alerts(db, today)
    assert db.notifications == []


# ── Rule: critical_insight ──────────────────────────────────────────


def test_critical_insight_records_one_per_critical_row():
    db = _FakeDb()
    today = date(2026, 5, 25)
    db.feedback_insights.append({
        "insight_id": 1, "as_of_date": today,
        "category": "portfolio_quality", "severity": "critical",
        "message": "Hit rate below 45%", "metric_name": "portfolio_hit_rate",
        "metric_value": 0.40, "benchmark": 0.55,
    })
    db.feedback_insights.append({
        "insight_id": 2, "as_of_date": today,
        "category": "summary", "severity": "info",
        "message": "All ok", "metric_name": "overall_status",
        "metric_value": 1.0, "benchmark": 1.0,
    })
    notifications.evaluate_daily_alerts(db, today)
    critical_notifs = [
        n for n in db.notifications if n["kind"] == "critical_insight"
    ]
    assert len(critical_notifs) == 1
    assert "portfolio_quality" in critical_notifs[0]["title"]


# ── Rule: signal_degradation ────────────────────────────────────────


def test_signal_degradation_fires_when_bad_threshold_exceeded():
    db = _FakeDb()
    today = date(2026, 5, 25)
    # Fragility signal bad on 6 of last 10 days
    for i in range(6):
        db.signal_validations.append({
            "as_of_date": today - timedelta(days=i),
            "signal_name": "fragility_vs_portfolio_return",
            "verdict": "SIGNAL_INVERTED",
        })
    # And 4 good days mixed in
    for i in range(6, 10):
        db.signal_validations.append({
            "as_of_date": today - timedelta(days=i),
            "signal_name": "fragility_vs_portfolio_return",
            "verdict": "SIGNAL_VALID",
        })
    notifications.evaluate_daily_alerts(db, today)
    deg_notifs = [
        n for n in db.notifications if n["kind"] == "signal_degradation"
    ]
    assert len(deg_notifs) == 1
    assert "fragility_vs_portfolio_return" in deg_notifs[0]["title"]


def test_signal_degradation_skips_when_below_threshold():
    db = _FakeDb()
    today = date(2026, 5, 25)
    # Only 3 bad days, default min_bad is 5
    for i in range(3):
        db.signal_validations.append({
            "as_of_date": today - timedelta(days=i),
            "signal_name": "x",
            "verdict": "HEDGE_INEFFECTIVE",
        })
    notifications.evaluate_daily_alerts(db, today)
    assert all(n["kind"] != "signal_degradation" for n in db.notifications)


# ── Rule: diagnostic_warning ────────────────────────────────────────


def test_diagnostic_warning_fires_on_underperformers_or_high_risk():
    db = _FakeDb()
    today = date(2026, 5, 25)
    db.diagnostic_reports.append({
        "report_id": 1, "as_of_date": today,
        "strategy_id": "US_EQ_CORE_LONG_EQ",
        "has_underperformers": True, "has_high_risk": False,
        "num_runs_analysed": 12,
    })
    db.diagnostic_reports.append({
        "report_id": 2, "as_of_date": today,
        "strategy_id": "OTHER", "has_underperformers": False,
        "has_high_risk": True, "num_runs_analysed": 8,
    })
    db.diagnostic_reports.append({
        "report_id": 3, "as_of_date": today,
        "strategy_id": "CLEAN", "has_underperformers": False,
        "has_high_risk": False, "num_runs_analysed": 20,
    })
    notifications.evaluate_daily_alerts(db, today)
    diag_notifs = [
        n for n in db.notifications if n["kind"] == "diagnostic_warning"
    ]
    assert len(diag_notifs) == 2  # CLEAN doesn't fire
    strategies = {n["metadata_json"].adapted["strategy_id"] for n in diag_notifs}
    assert strategies == {"US_EQ_CORE_LONG_EQ", "OTHER"}


# ── End-to-end ──────────────────────────────────────────────────────


def test_evaluate_daily_alerts_returns_aggregate_with_counts():
    db = _FakeDb()
    today = date(2026, 5, 25)
    db.proposals.append({
        "proposal_id": "p", "strategy_id": "S",
        "proposal_type": "x", "target_component": "y",
        "confidence_score": 0.6,
        "expected_sharpe_improvement": 0.15,
        "rationale": "test",
        "created_at": datetime(2026, 5, 25, 18, 0, tzinfo=timezone.utc),
    })
    result = notifications.evaluate_daily_alerts(db, today)
    assert result.as_of_date == today
    assert len(result.evaluations) == 5  # 5 rules (added drift_alert)
    assert result.total_recorded == 1
    assert result.any_errors is False


def test_idempotent_when_evaluate_runs_twice():
    db = _FakeDb()
    today = date(2026, 5, 25)
    db.proposals.append({
        "proposal_id": "p", "strategy_id": "S",
        "proposal_type": "x", "target_component": "y",
        "confidence_score": 0.6,
        "expected_sharpe_improvement": 0.15,
        "rationale": "",
        "created_at": datetime(2026, 5, 25, 18, 0, tzinfo=timezone.utc),
    })
    first = notifications.evaluate_daily_alerts(db, today)
    second = notifications.evaluate_daily_alerts(db, today)
    assert first.total_recorded == 1
    assert second.total_recorded == 0  # all dedup'd
    assert len(db.notifications) == 1


# ── Inbox helpers ───────────────────────────────────────────────────


def test_list_unread_orders_newest_first():
    db = _FakeDb()
    # Insert via record so timestamps are real
    notifications.record_notification(
        db, as_of_date=date(2026, 5, 20), kind="a",
        severity="info", title="older", source_id="1",
    )
    notifications.record_notification(
        db, as_of_date=date(2026, 5, 25), kind="b",
        severity="info", title="newer", source_id="2",
    )
    rows = notifications.list_unread_notifications(db)
    assert len(rows) == 2
    # Newer first
    assert rows[0]["title"] == "newer"


def test_mark_read_excludes_from_unread_list():
    db = _FakeDb()
    notifications.record_notification(
        db, as_of_date=date(2026, 5, 25), kind="a",
        severity="info", title="t", source_id="x",
    )
    nid = db.notifications[0]["notification_id"]
    assert notifications.mark_read(db, nid) is True
    assert notifications.mark_read(db, nid) is False  # idempotent
    assert notifications.list_unread_notifications(db) == []


def test_dismiss_excludes_from_unread_list():
    db = _FakeDb()
    notifications.record_notification(
        db, as_of_date=date(2026, 5, 25), kind="a",
        severity="info", title="t", source_id="x",
    )
    nid = db.notifications[0]["notification_id"]
    assert notifications.dismiss(db, nid) is True
    assert notifications.list_unread_notifications(db) == []


# ── Failure isolation ───────────────────────────────────────────────


def test_rule_failure_does_not_break_other_rules():
    """An exception inside one rule yields error=str(exc) on its
    evaluation but doesn't abort the run."""
    db = _FakeDb()
    today = date(2026, 5, 25)

    # Critical insight rule will succeed (no critical rows → 0 recorded)
    # Inject a broken cursor for the proposal_pending query
    original_execute = _FakeCursor.execute

    def _broken_execute(self, sql, args=()):
        norm = " ".join(sql.split()).upper()
        if "FROM META_CONFIG_PROPOSALS" in norm:
            raise RuntimeError("simulated proposal query failure")
        return original_execute(self, sql, args)

    try:
        _FakeCursor.execute = _broken_execute  # type: ignore[method-assign]
        result = notifications.evaluate_daily_alerts(db, today)
    finally:
        _FakeCursor.execute = original_execute  # type: ignore[method-assign]

    prop_eval = next(e for e in result.evaluations if e.kind == "proposal_pending")
    assert prop_eval.error is not None
    # The other 4 rules still ran (critical_insight, signal_degradation,
    # diagnostic_warning, drift_alert).
    assert result.any_errors is True
    other_kinds = [e.kind for e in result.evaluations if e.kind != "proposal_pending"]
    assert len(other_kinds) == 4
