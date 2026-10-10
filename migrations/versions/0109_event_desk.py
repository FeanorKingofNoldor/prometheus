"""Forward event observations, paper trade lifecycle and horizon measurements."""

from alembic import op

revision = "0109_event_desk"
down_revision = "0108_outcome_accounting_v2"
branch_labels = depends_on = None


def upgrade():
    op.execute("""
    CREATE TABLE event_desk_events (
      event_id text PRIMARY KEY, signature text NOT NULL, first_seen timestamptz NOT NULL,
      published_at timestamptz NOT NULL, updated_at timestamptz NOT NULL, payload jsonb NOT NULL);
    CREATE INDEX ix_event_desk_signature ON event_desk_events(signature, first_seen DESC);
    CREATE TABLE event_desk_candidates (
      candidate_id text PRIMARY KEY, event_id text NOT NULL REFERENCES event_desk_events(event_id),
      strategy text NOT NULL CHECK(strategy IN ('CONTINUATION','REVERSAL')),
      version text NOT NULL, instrument_id text NOT NULL, created_at timestamptz NOT NULL,
      updated_at timestamptz NOT NULL, state text NOT NULL, reason text NOT NULL,
      payload jsonb NOT NULL, UNIQUE(event_id,strategy,version,instrument_id));
    CREATE INDEX ix_event_desk_state ON event_desk_candidates(state,updated_at);
    CREATE TABLE event_desk_marks (
      candidate_id text NOT NULL REFERENCES event_desk_candidates(candidate_id),
      observed_at timestamptz NOT NULL, payload jsonb NOT NULL,
      PRIMARY KEY(candidate_id,observed_at));
    CREATE TABLE event_desk_outcomes (
      candidate_id text NOT NULL REFERENCES event_desk_candidates(candidate_id),
      horizon_hours integer NOT NULL, observed_at timestamptz NOT NULL,
      payload jsonb NOT NULL, PRIMARY KEY(candidate_id,horizon_hours));
    CREATE TABLE event_desk_runs (
      started_at timestamptz PRIMARY KEY, completed_at timestamptz, payload jsonb NOT NULL);
    """)


def downgrade():
    op.execute(
        "DROP TABLE event_desk_runs,event_desk_outcomes,event_desk_marks,event_desk_candidates,event_desk_events"
    )
