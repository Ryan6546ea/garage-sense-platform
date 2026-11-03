"""Create M6.1 shared execution lease and historical replay tables.

Revision ID: 0004
Revises: 0003
Create Date: 2026-07-31
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "environment_execution_leases",
        sa.Column("scope", sa.Text(), nullable=False),
        sa.Column("holder_type", sa.Text(), nullable=False),
        sa.Column("holder_id", sa.Uuid(), nullable=False),
        sa.Column("acquired_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("version", sa.Integer(), server_default="1", nullable=False),
        sa.CheckConstraint("scope = 'G001'", name="ck_execution_leases_scope"),
        sa.CheckConstraint(
            "holder_type IN ('random_run','replay_run')",
            name="ck_execution_leases_holder_type",
        ),
        sa.PrimaryKeyConstraint("scope"),
    )
    op.create_table(
        "replay_runs",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("source_type", sa.Text(), nullable=False),
        sa.Column("source_run_id", sa.Uuid()),
        sa.Column("selection", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("speed_multiplier", sa.Integer(), nullable=False),
        sa.Column("source_time_mode", sa.Text(), nullable=False),
        sa.Column("created_by", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("paused_at", sa.DateTime(timezone=True)),
        sa.Column("resumed_at", sa.DateTime(timezone=True)),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.Column("total_paused_seconds", sa.Float(), server_default="0", nullable=False),
        sa.Column("item_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("published_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("failed_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("current_sequence_number", sa.Integer(), server_default="0", nullable=False),
        sa.Column("first_original_received_at", sa.DateTime(timezone=True)),
        sa.Column("last_original_received_at", sa.DateTime(timezone=True)),
        sa.Column("simulator_instance_id", sa.Text()),
        sa.Column("simulator_suspension_token", sa.Text()),
        sa.Column("reserved_seq_start", sa.BigInteger()),
        sa.Column("reserved_seq_end", sa.BigInteger()),
        sa.Column("failure_reason", sa.Text()),
        sa.Column("version", sa.Integer(), server_default="1", nullable=False),
        sa.CheckConstraint(
            "status IN ('pending','running','paused','completed','stopped','failed','interrupted')",
            name="ck_replay_runs_status",
        ),
        sa.CheckConstraint(
            "source_type IN ('test_run','time_range')",
            name="ck_replay_runs_source_type",
        ),
        sa.CheckConstraint(
            "source_type <> 'test_run' OR source_run_id IS NOT NULL",
            name="ck_replay_runs_test_source_run",
        ),
        sa.CheckConstraint(
            "source_type <> 'time_range' OR "
            "(selection ? 'receivedFrom' AND selection ? 'receivedTo')",
            name="ck_replay_runs_time_range_selection",
        ),
        sa.CheckConstraint("speed_multiplier IN (1,2,5,10)", name="ck_replay_runs_speed"),
        sa.CheckConstraint(
            "source_time_mode IN ('shifted','preserve')",
            name="ck_replay_runs_source_time_mode",
        ),
        sa.CheckConstraint(
            "total_paused_seconds >= 0 AND item_count >= 0 AND published_count >= 0 "
            "AND failed_count >= 0 AND current_sequence_number >= 0",
            name="ck_replay_runs_nonnegative_counts",
        ),
        sa.ForeignKeyConstraint(
            ["source_run_id"], ["simulation_runs.id"], ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_replay_runs_status_created", "replay_runs", ["status", "created_at"])
    op.create_table(
        "replay_items",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("replay_run_id", sa.Uuid(), nullable=False),
        sa.Column("sequence_number", sa.Integer(), nullable=False),
        sa.Column("source_event_id", sa.BigInteger(), nullable=False),
        sa.Column("original_message_id", sa.Text(), nullable=False),
        sa.Column("original_source_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("original_received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("original_seq", sa.BigInteger(), nullable=False),
        sa.Column("topic", sa.Text(), nullable=False),
        sa.Column("planned_offset_seconds", sa.Float(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("replay_message_id", sa.Text()),
        sa.Column("replay_seq", sa.BigInteger()),
        sa.Column("transformed_source_time", sa.DateTime(timezone=True)),
        sa.Column("published_at", sa.DateTime(timezone=True)),
        sa.Column("attempt_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("last_error", sa.Text()),
        sa.CheckConstraint(
            "status IN ('pending','publishing','published','failed','cancelled')",
            name="ck_replay_items_status",
        ),
        sa.ForeignKeyConstraint(["replay_run_id"], ["replay_runs.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["source_event_id"], ["telemetry_events.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "replay_run_id",
            "sequence_number",
            name="uq_replay_items_run_sequence",
        ),
    )
    op.create_index("ix_replay_items_run_status", "replay_items", ["replay_run_id", "status"])
    op.create_index("ix_replay_items_source_event", "replay_items", ["source_event_id"])
    op.create_index(
        "ux_replay_items_replay_message_id",
        "replay_items",
        ["replay_message_id"],
        unique=True,
    )
    op.create_table(
        "replay_events",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("replay_run_id", sa.Uuid(), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("sequence_number", sa.BigInteger(), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.ForeignKeyConstraint(["replay_run_id"], ["replay_runs.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "replay_run_id",
            "sequence_number",
            name="uq_replay_events_run_sequence",
        ),
    )
    op.create_index("ix_replay_events_occurred_at", "replay_events", ["occurred_at"])
    op.execute(
        """
        CREATE FUNCTION reject_replay_event_mutation() RETURNS trigger AS $$
        BEGIN RAISE EXCEPTION 'replay_events is append-only'; END;
        $$ LANGUAGE plpgsql
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_replay_events_append_only BEFORE UPDATE OR DELETE ON replay_events
        FOR EACH ROW EXECUTE FUNCTION reject_replay_event_mutation()
        """
    )
    op.execute(
        """
        CREATE FUNCTION reject_replay_item_source_mutation() RETURNS trigger AS $$
        BEGIN
          IF OLD.replay_run_id IS DISTINCT FROM NEW.replay_run_id
             OR OLD.sequence_number IS DISTINCT FROM NEW.sequence_number
             OR OLD.source_event_id IS DISTINCT FROM NEW.source_event_id
             OR OLD.original_message_id IS DISTINCT FROM NEW.original_message_id
             OR OLD.original_source_time IS DISTINCT FROM NEW.original_source_time
             OR OLD.original_received_at IS DISTINCT FROM NEW.original_received_at
             OR OLD.original_seq IS DISTINCT FROM NEW.original_seq
             OR OLD.topic IS DISTINCT FROM NEW.topic
             OR OLD.planned_offset_seconds IS DISTINCT FROM NEW.planned_offset_seconds THEN
            RAISE EXCEPTION 'replay item source identity is immutable';
          END IF;
          RETURN NEW;
        END;
        $$ LANGUAGE plpgsql
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_replay_items_source_immutable BEFORE UPDATE ON replay_items
        FOR EACH ROW EXECUTE FUNCTION reject_replay_item_source_mutation()
        """
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER trg_replay_items_source_immutable ON replay_items")
    op.execute("DROP FUNCTION reject_replay_item_source_mutation()")
    op.execute("DROP TRIGGER trg_replay_events_append_only ON replay_events")
    op.execute("DROP FUNCTION reject_replay_event_mutation()")
    op.drop_index("ix_replay_events_occurred_at", table_name="replay_events")
    op.drop_table("replay_events")
    op.drop_index("ux_replay_items_replay_message_id", table_name="replay_items")
    op.drop_index("ix_replay_items_source_event", table_name="replay_items")
    op.drop_index("ix_replay_items_run_status", table_name="replay_items")
    op.drop_table("replay_items")
    op.drop_index("ix_replay_runs_status_created", table_name="replay_runs")
    op.drop_table("replay_runs")
    op.drop_table("environment_execution_leases")
