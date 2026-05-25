"""Meta-analysis persistence tables for the autopilot feedback loop.

Revision ID: 0099_meta_analysis_tables
Revises: 0098_derivatives_shadow_decisions
Create Date: 2026-05-25

Four tables that turn the existing meta analysis modules from "log
and forget" into "log, persist, surface in frontend":

- meta_feedback_insights: one row per insight from the daily
  compute_feedback_report() — categories like portfolio_quality,
  assessment_accuracy, risk_override_rate, regime_timing.
- meta_signal_validations: verdicts from LivePerformanceTracker.
  Tracks the rolling Spearman ρ for fragility, the Pearson r for
  hedge effectiveness, and the per-regime hit-rate breakdown.
- meta_diagnostic_reports: per-strategy DiagnosticReport from
  DiagnosticsEngine — regime breakdown + config comparisons +
  underperformer detection.
- weekly_reports: rolled-up weekly performance report. One row
  per week; both structured JSON and pre-rendered markdown for
  the frontend's weekly digest page.

All tables are append-only. Daily analysis writes here; the
frontend reads here; nothing mutates rows after insert.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0099_meta_analysis_tables"
down_revision: Union[str, None] = "0098_derivatives_shadow_decisions"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── meta_feedback_insights ──────────────────────────────────────
    op.create_table(
        "meta_feedback_insights",
        sa.Column("insight_id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("as_of_date", sa.Date, nullable=False),
        sa.Column("category", sa.String(length=64), nullable=False),
        sa.Column("severity", sa.String(length=16), nullable=False),
        sa.Column("message", sa.String(length=500), nullable=False),
        sa.Column("metric_name", sa.String(length=64), nullable=False),
        sa.Column("metric_value", sa.Float, nullable=False),
        sa.Column("benchmark", sa.Float, nullable=False),
        sa.Column("deviation", sa.Float, nullable=False),
        sa.Column("lookback_days", sa.Integer, nullable=False, server_default="63"),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
    )
    op.create_index(
        "idx_meta_feedback_date_severity",
        "meta_feedback_insights",
        ["as_of_date", "severity"],
    )
    op.create_index(
        "idx_meta_feedback_category",
        "meta_feedback_insights",
        ["category", "as_of_date"],
    )

    # ── meta_signal_validations ─────────────────────────────────────
    op.create_table(
        "meta_signal_validations",
        sa.Column("validation_id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("as_of_date", sa.Date, nullable=False),
        sa.Column("signal_name", sa.String(length=64), nullable=False),
        sa.Column("verdict", sa.String(length=32), nullable=False),
        sa.Column("metric_value", sa.Float, nullable=True),
        sa.Column("threshold", sa.Float, nullable=True),
        sa.Column("sample_size", sa.Integer, nullable=True),
        sa.Column("lookback_days", sa.Integer, nullable=False, server_default="63"),
        sa.Column("details_json", postgresql.JSONB, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
    )
    op.create_index(
        "idx_meta_signal_date",
        "meta_signal_validations",
        ["as_of_date", "signal_name"],
    )
    op.create_index(
        "idx_meta_signal_name_date",
        "meta_signal_validations",
        ["signal_name", "as_of_date"],
    )

    # ── meta_diagnostic_reports ─────────────────────────────────────
    op.create_table(
        "meta_diagnostic_reports",
        sa.Column("report_id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("as_of_date", sa.Date, nullable=False),
        sa.Column("strategy_id", sa.String(length=64), nullable=False),
        sa.Column("has_underperformers", sa.Boolean, nullable=False, server_default=sa.text("false")),
        sa.Column("has_high_risk", sa.Boolean, nullable=False, server_default=sa.text("false")),
        sa.Column("num_runs_analysed", sa.Integer, nullable=False, server_default="0"),
        sa.Column("report_json", postgresql.JSONB, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
    )
    op.create_index(
        "idx_meta_diag_strategy_date",
        "meta_diagnostic_reports",
        ["strategy_id", "as_of_date"],
    )
    op.create_index(
        "idx_meta_diag_date",
        "meta_diagnostic_reports",
        ["as_of_date"],
    )

    # ── weekly_reports ──────────────────────────────────────────────
    op.create_table(
        "weekly_reports",
        sa.Column("report_id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("week_start", sa.Date, nullable=False),
        sa.Column("week_end", sa.Date, nullable=False),
        sa.Column("strategy_id", sa.String(length=64), nullable=True),
        sa.Column("period_return", sa.Float, nullable=True),
        sa.Column("period_sharpe", sa.Float, nullable=True),
        sa.Column("period_max_drawdown", sa.Float, nullable=True),
        sa.Column("n_trades", sa.Integer, nullable=True),
        sa.Column("n_winners", sa.Integer, nullable=True),
        sa.Column("n_losers", sa.Integer, nullable=True),
        sa.Column("report_json", postgresql.JSONB, nullable=False),
        sa.Column("markdown", sa.Text, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
    )
    op.create_index(
        "ux_weekly_reports_week_strategy",
        "weekly_reports",
        ["week_start", "strategy_id"],
        unique=True,
    )
    op.create_index(
        "idx_weekly_reports_week_desc",
        "weekly_reports",
        ["week_start"],
    )


def downgrade() -> None:
    op.drop_index("idx_weekly_reports_week_desc", table_name="weekly_reports")
    op.drop_index("ux_weekly_reports_week_strategy", table_name="weekly_reports")
    op.drop_table("weekly_reports")

    op.drop_index("idx_meta_diag_date", table_name="meta_diagnostic_reports")
    op.drop_index("idx_meta_diag_strategy_date", table_name="meta_diagnostic_reports")
    op.drop_table("meta_diagnostic_reports")

    op.drop_index("idx_meta_signal_name_date", table_name="meta_signal_validations")
    op.drop_index("idx_meta_signal_date", table_name="meta_signal_validations")
    op.drop_table("meta_signal_validations")

    op.drop_index("idx_meta_feedback_category", table_name="meta_feedback_insights")
    op.drop_index("idx_meta_feedback_date_severity", table_name="meta_feedback_insights")
    op.drop_table("meta_feedback_insights")
