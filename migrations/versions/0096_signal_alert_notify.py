"""Add NOTIFY triggers for high-severity intel signals.

Revision ID: 0096_signal_alert_notify
Revises: 0095_scenario_branches
Create Date: 2026-05-05

Wires Postgres ``NOTIFY`` channels so the Prometheus daemon can react
event-driven rather than polling every 60s.  Two channels:

* ``prometheus_signal_alert`` — fires on EXTREME divergence and CRITICAL
  compound-pressure rows.  Daemon listener triggers an immediate
  options re-evaluation and a flash entry into ``engine_decisions``.

The payload is a small JSON blob naming the channel, the table, and
the entity so the listener can route the notification without a second
DB round-trip.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "0096_signal_alert_notify"
down_revision: Union[str, None] = "0095_scenario_branches"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_DIVERGENCE_FN = """
CREATE OR REPLACE FUNCTION prometheus_notify_divergence_signal()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.severity = 'EXTREME' THEN
        PERFORM pg_notify(
            'prometheus_signal_alert',
            json_build_object(
                'source', 'divergence',
                'severity', NEW.severity,
                'entity_type', NEW.entity_type,
                'entity_id', NEW.entity_id,
                'as_of_date', NEW.as_of_date::text,
                'trading_signal', NEW.trading_signal,
                'decision_id', NEW.decision_id
            )::text
        );
    END IF;
    RETURN NEW;
END;
$$;
"""

_DIVERGENCE_TRIGGER = """
DROP TRIGGER IF EXISTS trg_prometheus_notify_divergence_signal ON divergence_signals;
CREATE TRIGGER trg_prometheus_notify_divergence_signal
AFTER INSERT OR UPDATE ON divergence_signals
FOR EACH ROW EXECUTE FUNCTION prometheus_notify_divergence_signal();
"""

_COMPOUND_FN = """
CREATE OR REPLACE FUNCTION prometheus_notify_compound_pressure()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.severity = 'CRITICAL' THEN
        PERFORM pg_notify(
            'prometheus_signal_alert',
            json_build_object(
                'source', 'compound_pressure',
                'severity', NEW.severity,
                'entity_type', NEW.target_entity_type,
                'entity_id', NEW.target_entity_id,
                'as_of_date', NEW.as_of_date::text,
                'encirclement_score', NEW.encirclement_score,
                'decision_id', NEW.decision_id
            )::text
        );
    END IF;
    RETURN NEW;
END;
$$;
"""

_COMPOUND_TRIGGER = """
DROP TRIGGER IF EXISTS trg_prometheus_notify_compound_pressure ON compound_pressure_alerts;
CREATE TRIGGER trg_prometheus_notify_compound_pressure
AFTER INSERT OR UPDATE ON compound_pressure_alerts
FOR EACH ROW EXECUTE FUNCTION prometheus_notify_compound_pressure();
"""


def upgrade() -> None:
    op.execute(_DIVERGENCE_FN)
    op.execute(_DIVERGENCE_TRIGGER)
    op.execute(_COMPOUND_FN)
    op.execute(_COMPOUND_TRIGGER)


def downgrade() -> None:
    op.execute(
        "DROP TRIGGER IF EXISTS trg_prometheus_notify_compound_pressure "
        "ON compound_pressure_alerts;"
    )
    op.execute("DROP FUNCTION IF EXISTS prometheus_notify_compound_pressure();")
    op.execute(
        "DROP TRIGGER IF EXISTS trg_prometheus_notify_divergence_signal "
        "ON divergence_signals;"
    )
    op.execute("DROP FUNCTION IF EXISTS prometheus_notify_divergence_signal();")
