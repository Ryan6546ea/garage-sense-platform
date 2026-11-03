"""Allow expired telemetry deletion within an explicit retention transaction.

Revision ID: 0013
Revises: 0012
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0013"
down_revision: str | None = "0012"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("""
        CREATE OR REPLACE FUNCTION reject_telemetry_event_mutation() RETURNS trigger AS $$
        DECLARE cutoff timestamptz;
        BEGIN
          cutoff := NULLIF(current_setting('garage.retention_cutoff', true), '')::timestamptz;
          IF TG_OP = 'DELETE' AND cutoff IS NOT NULL
             AND cutoff <= CURRENT_TIMESTAMP - INTERVAL '1 day'
             AND OLD.received_at < cutoff THEN
            RETURN OLD;
          END IF;
          RAISE EXCEPTION 'telemetry events are append-only outside expired-data retention';
        END;
        $$ LANGUAGE plpgsql
    """)


def downgrade() -> None:
    op.execute("""
        CREATE OR REPLACE FUNCTION reject_telemetry_event_mutation() RETURNS trigger AS $$
        BEGIN
          RAISE EXCEPTION 'typed telemetry events are append-only';
        END;
        $$ LANGUAGE plpgsql
    """)
