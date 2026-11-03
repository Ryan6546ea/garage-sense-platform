"""Create virtual command execution, feedback, and audit tables.

Revision ID: 0007
Revises: 0006
Create Date: 2026-08-01
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0007"
down_revision: str | None = "0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "virtual_controller_state",
        sa.Column("controller_id", sa.Text(), primary_key=True),
        sa.Column("scenario_id", sa.Text(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False, server_default="1"),
        sa.CheckConstraint("version > 0", name="ck_virtual_controller_state_version"),
    )
    op.create_table(
        "controller_scenario_requests",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("request_id", sa.Text(), nullable=False),
        sa.Column("target_id", sa.Text(), nullable=False),
        sa.Column("scenario_id", sa.Text(), nullable=False),
        sa.Column("requested_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("response_payload", postgresql.JSONB(astext_type=sa.Text())),
        sa.Column("error_detail", sa.Text()),
        sa.CheckConstraint(
            "status IN ('pending','succeeded','failed')",
            name="ck_controller_scenario_request_status",
        ),
    )
    op.create_index(
        "ux_controller_scenario_requests_request_id",
        "controller_scenario_requests",
        ["request_id"],
        unique=True,
    )
    op.create_table(
        "controller_execution_jobs",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("execution_id", sa.Text(), nullable=False),
        sa.Column("request_id", sa.Text(), nullable=False),
        sa.Column("command_id", sa.Text(), nullable=False),
        sa.Column("command_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("scenario_id", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column("failure_reason", sa.Text()),
        sa.Column("version", sa.Integer(), nullable=False, server_default="1"),
        sa.CheckConstraint(
            "status IN ('accepted','scheduled','completed','failed')",
            name="ck_controller_execution_job_status",
        ),
        sa.CheckConstraint("version > 0", name="ck_controller_execution_job_version"),
        sa.UniqueConstraint("execution_id", name="uq_controller_execution_jobs_execution_id"),
        sa.UniqueConstraint("request_id", name="uq_controller_execution_jobs_request_id"),
        sa.UniqueConstraint("command_id", name="uq_controller_execution_jobs_command_id"),
    )
    op.create_table(
        "controller_feedback_outbox",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column(
            "execution_job_id",
            sa.Uuid(),
            sa.ForeignKey("controller_execution_jobs.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("command_id", sa.Text(), nullable=False),
        sa.Column("feedback_id", sa.Text(), nullable=False),
        sa.Column("feedback_sequence", sa.Integer(), nullable=False),
        sa.Column("due_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True)),
        sa.Column("last_error", sa.Text()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("feedback_sequence > 0", name="ck_controller_outbox_feedback_sequence"),
        sa.CheckConstraint("attempt_count >= 0", name="ck_controller_outbox_attempt_count"),
        sa.CheckConstraint(
            "status IN ('pending','publishing','published','retry','failed')",
            name="ck_controller_outbox_status",
        ),
        sa.CheckConstraint(
            "status <> 'published' OR published_at IS NOT NULL",
            name="ck_controller_outbox_published_at",
        ),
        sa.UniqueConstraint("feedback_id", name="uq_controller_outbox_feedback_id"),
        sa.UniqueConstraint(
            "command_id", "feedback_sequence", name="uq_controller_outbox_command_sequence"
        ),
    )
    op.create_index(
        "ix_controller_outbox_claim",
        "controller_feedback_outbox",
        ["status", "next_attempt_at", "due_at"],
    )

    op.create_table(
        "virtual_commands",
        sa.Column("command_id", sa.Text(), primary_key=True),
        sa.Column("schema_version", sa.Text(), nullable=False),
        sa.Column("garage_id", sa.Text(), nullable=False),
        sa.Column("target_device_id", sa.Text(), nullable=False),
        sa.Column("action", sa.Text(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("issued_by", sa.Text(), nullable=False),
        sa.Column("source_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("parameters", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("command_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("request_id", sa.Text(), nullable=False),
        sa.Column("execution_id", sa.Text()),
        sa.Column("accepted_at", sa.DateTime(timezone=True)),
        sa.Column("dispatched_at", sa.DateTime(timezone=True)),
        sa.Column("feedback_deadline", sa.DateTime(timezone=True)),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column("latest_result", sa.Text()),
        sa.Column("latest_message", sa.Text()),
        sa.Column(
            "pending_extension_used", sa.Boolean(), nullable=False, server_default=sa.false()
        ),
        sa.Column("version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "status IN ('accepted','dispatching','awaiting_feedback','pending',"
            "'succeeded','failed','rejected','timed_out','dispatch_failed')",
            name="ck_virtual_commands_status",
        ),
        sa.CheckConstraint("version > 0", name="ck_virtual_commands_version"),
        sa.UniqueConstraint("request_id", name="uq_virtual_commands_request_id"),
        sa.UniqueConstraint("execution_id", name="uq_virtual_commands_execution_id"),
    )
    op.create_index(
        "ix_virtual_commands_status_created", "virtual_commands", ["status", "created_at"]
    )
    op.create_index(
        "ix_virtual_commands_action_created", "virtual_commands", ["action", "created_at"]
    )
    op.create_index(
        "ix_virtual_commands_issued_created", "virtual_commands", ["issued_by", "created_at"]
    )
    op.create_index("ix_virtual_commands_source_time", "virtual_commands", ["source_time"])
    op.create_index(
        "ix_virtual_commands_feedback_deadline", "virtual_commands", ["feedback_deadline"]
    )
    op.create_table(
        "command_feedbacks",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column("feedback_id", sa.Text(), nullable=False),
        sa.Column(
            "command_id",
            sa.Text(),
            sa.ForeignKey("virtual_commands.command_id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("feedback_sequence", sa.Integer(), nullable=False),
        sa.Column("result", sa.Text(), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("emitted_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("executed_at", sa.DateTime(timezone=True)),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("topic", sa.Text(), nullable=False),
        sa.Column("qos", sa.Integer(), nullable=False),
        sa.Column("retain", sa.Boolean(), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.UniqueConstraint("feedback_id", name="uq_command_feedbacks_feedback_id"),
        sa.UniqueConstraint(
            "command_id", "feedback_sequence", name="uq_command_feedbacks_command_sequence"
        ),
    )
    op.create_table(
        "command_events",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column(
            "command_id",
            sa.Text(),
            sa.ForeignKey("virtual_commands.command_id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("sequence_number", sa.Integer(), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.UniqueConstraint(
            "command_id", "sequence_number", name="uq_command_events_command_sequence"
        ),
    )
    op.create_index(
        "ix_command_events_command", "command_events", ["command_id", "sequence_number"]
    )
    op.create_table(
        "command_ingest_failures",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column("topic", sa.Text(), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("error_type", sa.Text(), nullable=False),
        sa.Column("error_detail", sa.Text(), nullable=False),
        sa.Column("original_payload", sa.LargeBinary(), nullable=False),
        sa.Column("original_payload_size", sa.Integer(), nullable=False),
        sa.Column("payload_truncated", sa.Boolean(), nullable=False),
        sa.Column("payload_sha256", sa.Text(), nullable=False),
    )
    op.create_index(
        "ix_command_ingest_failures_received", "command_ingest_failures", ["received_at"]
    )

    op.execute(
        "INSERT INTO virtual_controller_state "
        "(controller_id,scenario_id,updated_at,version) VALUES "
        "('G001-VIRTUAL-CONTROLLER','VIRTUAL_COMMAND_SUCCESS',CURRENT_TIMESTAMP,1)"
    )
    op.execute("""
        CREATE FUNCTION reject_m8_append_only_mutation() RETURNS trigger AS $$
        BEGIN RAISE EXCEPTION 'M8 audit rows are append-only'; END;
        $$ LANGUAGE plpgsql
    """)
    for table in ("command_feedbacks", "command_events"):
        op.execute(
            f"CREATE TRIGGER trg_{table}_append_only BEFORE UPDATE OR DELETE ON {table} "
            "FOR EACH ROW EXECUTE FUNCTION reject_m8_append_only_mutation()"
        )
    op.execute("""
        CREATE FUNCTION reject_controller_outbox_payload_change() RETURNS trigger AS $$
        BEGIN
          IF NEW.payload IS DISTINCT FROM OLD.payload
             OR NEW.feedback_id IS DISTINCT FROM OLD.feedback_id THEN
            RAISE EXCEPTION 'outbox payload and feedback_id are immutable';
          END IF;
          RETURN NEW;
        END;
        $$ LANGUAGE plpgsql
    """)
    op.execute(
        "CREATE TRIGGER trg_controller_outbox_payload_immutable BEFORE UPDATE ON "
        "controller_feedback_outbox FOR EACH ROW EXECUTE FUNCTION "
        "reject_controller_outbox_payload_change()"
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER trg_controller_outbox_payload_immutable ON controller_feedback_outbox")
    op.execute("DROP FUNCTION reject_controller_outbox_payload_change()")
    for table in ("command_events", "command_feedbacks"):
        op.execute(f"DROP TRIGGER trg_{table}_append_only ON {table}")
    op.execute("DROP FUNCTION reject_m8_append_only_mutation()")
    op.drop_index("ix_command_ingest_failures_received", table_name="command_ingest_failures")
    op.drop_table("command_ingest_failures")
    op.drop_index("ix_command_events_command", table_name="command_events")
    op.drop_table("command_events")
    op.drop_table("command_feedbacks")
    for name in (
        "ix_virtual_commands_feedback_deadline",
        "ix_virtual_commands_source_time",
        "ix_virtual_commands_issued_created",
        "ix_virtual_commands_action_created",
        "ix_virtual_commands_status_created",
    ):
        op.drop_index(name, table_name="virtual_commands")
    op.drop_table("virtual_commands")
    op.drop_index("ix_controller_outbox_claim", table_name="controller_feedback_outbox")
    op.drop_table("controller_feedback_outbox")
    op.drop_table("controller_execution_jobs")
    op.drop_index(
        "ux_controller_scenario_requests_request_id", table_name="controller_scenario_requests"
    )
    op.drop_table("controller_scenario_requests")
    op.drop_table("virtual_controller_state")
