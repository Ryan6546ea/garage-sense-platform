"""Create M5 scenario-control run, activation, and immutable event tables.

Revision ID: 0003
Revises: 0002
Create Date: 2026-07-31
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create only the M5 control-domain persistence structures."""

    op.create_table(
        "simulation_runs",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("run_type", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("seed", sa.BigInteger()),
        sa.Column("requested_duration_seconds", sa.Float(), nullable=False),
        sa.Column("abnormal_probability", sa.Float(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("paused_at", sa.DateTime(timezone=True)),
        sa.Column("resumed_at", sa.DateTime(timezone=True)),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.Column("total_paused_seconds", sa.Float(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_by", sa.Text(), nullable=False),
        sa.Column("failure_reason", sa.Text()),
        sa.Column("configuration", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False, server_default="1"),
        sa.CheckConstraint(
            "run_type IN ('manual', 'random')",
            name="ck_simulation_runs_run_type",
        ),
        sa.CheckConstraint(
            "status IN ('pending','running','paused','completed','stopped','failed','interrupted')",
            name="ck_simulation_runs_status",
        ),
        sa.CheckConstraint(
            "requested_duration_seconds > 0",
            name="ck_simulation_runs_duration_positive",
        ),
        sa.CheckConstraint(
            "abnormal_probability >= 0 AND abnormal_probability <= 1",
            name="ck_simulation_runs_probability",
        ),
        sa.CheckConstraint(
            "run_type <> 'random' OR seed IS NOT NULL",
            name="ck_simulation_runs_random_seed",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_simulation_runs_status_created",
        "simulation_runs",
        ["status", "created_at"],
    )
    op.create_index(
        "ux_simulation_runs_one_active_random",
        "simulation_runs",
        ["run_type"],
        unique=True,
        postgresql_where=sa.text(
            "run_type = 'random' AND status IN ('running', 'paused')"
        ),
    )
    op.create_table(
        "scenario_activations",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid()),
        sa.Column("request_id", sa.Text(), nullable=False),
        sa.Column("target_id", sa.Text(), nullable=False),
        sa.Column("scenario_id", sa.Text(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("requested_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("dispatched_at", sa.DateTime(timezone=True)),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column("intended_duration_seconds", sa.Float()),
        sa.Column("expires_at", sa.DateTime(timezone=True)),
        sa.Column("simulator_activation_id", sa.Text()),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error", sa.Text()),
        sa.Column("response_payload", postgresql.JSONB(astext_type=sa.Text())),
        sa.Column("sequence_number", sa.BigInteger()),
        sa.CheckConstraint(
            "source IN ('manual', 'random')",
            name="ck_scenario_activations_source",
        ),
        sa.CheckConstraint(
            "status IN ('pending','dispatching','succeeded','failed','cancelled')",
            name="ck_scenario_activations_status",
        ),
        sa.CheckConstraint(
            "intended_duration_seconds IS NULL OR intended_duration_seconds > 0",
            name="ck_scenario_activations_duration_positive",
        ),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["simulation_runs.id"],
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "run_id",
            "sequence_number",
            name="uq_scenario_activations_run_sequence",
        ),
    )
    op.create_index(
        "ux_scenario_activations_request_id",
        "scenario_activations",
        ["request_id"],
        unique=True,
    )
    op.create_index(
        "ix_scenario_activations_run_sequence",
        "scenario_activations",
        ["run_id", "sequence_number"],
    )
    op.create_index(
        "ix_scenario_activations_target_requested",
        "scenario_activations",
        ["target_id", "requested_at"],
    )
    op.create_table(
        "run_events",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("sequence_number", sa.BigInteger(), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["simulation_runs.id"],
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "run_id",
            "sequence_number",
            name="uq_run_events_run_sequence",
        ),
    )
    op.create_index("ix_run_events_occurred_at", "run_events", ["occurred_at"])
    op.execute(
        """
        CREATE FUNCTION reject_run_event_mutation() RETURNS trigger AS $$
        BEGIN
          RAISE EXCEPTION 'run_events is append-only';
        END;
        $$ LANGUAGE plpgsql
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_run_events_append_only
        BEFORE UPDATE OR DELETE ON run_events
        FOR EACH ROW EXECUTE FUNCTION reject_run_event_mutation()
        """
    )


def downgrade() -> None:
    """Remove only the M5 control-domain persistence structures."""

    op.execute("DROP TRIGGER trg_run_events_append_only ON run_events")
    op.execute("DROP FUNCTION reject_run_event_mutation()")
    op.drop_index("ix_run_events_occurred_at", table_name="run_events")
    op.drop_table("run_events")
    op.drop_index(
        "ix_scenario_activations_target_requested",
        table_name="scenario_activations",
    )
    op.drop_index(
        "ix_scenario_activations_run_sequence",
        table_name="scenario_activations",
    )
    op.drop_index(
        "ux_scenario_activations_request_id",
        table_name="scenario_activations",
    )
    op.drop_table("scenario_activations")
    op.drop_index(
        "ux_simulation_runs_one_active_random",
        table_name="simulation_runs",
    )
    op.drop_index(
        "ix_simulation_runs_status_created",
        table_name="simulation_runs",
    )
    op.drop_table("simulation_runs")
