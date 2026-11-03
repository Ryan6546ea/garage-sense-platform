"""Create asynchronous export job, manifest, and audit tables.

Revision ID: 0005
Revises: 0004
Create Date: 2026-07-31
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0005"
down_revision: str | None = "0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "export_jobs",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("export_type", sa.Text(), nullable=False),
        sa.Column("format", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("source_type", sa.Text(), nullable=False),
        sa.Column("source_reference_id", sa.Uuid()),
        sa.Column("selection", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("include_replay_outputs", sa.Boolean(), nullable=False),
        sa.Column("include_ingest_failures", sa.Boolean(), nullable=False),
        sa.Column("include_run_audit", sa.Boolean(), nullable=False),
        sa.Column("created_by", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("item_count", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("processed_count", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("failed_count", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("output_filename", sa.Text()),
        sa.Column("output_media_type", sa.Text()),
        sa.Column("output_size_bytes", sa.BigInteger()),
        sa.Column("output_sha256", sa.Text()),
        sa.Column("storage_relative_path", sa.Text()),
        sa.Column("failure_reason", sa.Text()),
        sa.Column(
            "cancellation_requested", sa.Boolean(), server_default=sa.false(), nullable=False
        ),
        sa.Column("version", sa.Integer(), server_default="1", nullable=False),
        sa.CheckConstraint(
            "export_type IN ('telemetry','run_audit','combined_bundle')",
            name="ck_export_jobs_type",
        ),
        sa.CheckConstraint(
            "format IN ('jsonl','csv','zip')",
            name="ck_export_jobs_format",
        ),
        sa.CheckConstraint(
            "status IN ('pending','running','completed','failed','cancelled',"
            "'expired','interrupted')",
            name="ck_export_jobs_status",
        ),
        sa.CheckConstraint(
            "source_type IN ('time_range','test_run','replay_run')",
            name="ck_export_jobs_source_type",
        ),
        sa.CheckConstraint(
            "source_type = 'time_range' OR source_reference_id IS NOT NULL",
            name="ck_export_jobs_source_reference",
        ),
        sa.CheckConstraint(
            "(export_type = 'telemetry' AND format IN ('jsonl','csv')) OR "
            "(export_type = 'run_audit' AND format IN ('jsonl','zip')) OR "
            "(export_type = 'combined_bundle' AND format = 'zip')",
            name="ck_export_jobs_type_format",
        ),
        sa.CheckConstraint(
            "item_count >= 0 AND processed_count >= 0 AND failed_count >= 0",
            name="ck_export_jobs_nonnegative_counts",
        ),
        sa.CheckConstraint("expires_at > created_at", name="ck_export_jobs_expiry"),
        sa.CheckConstraint(
            "status <> 'completed' OR "
            "(storage_relative_path IS NOT NULL AND output_filename IS NOT NULL "
            "AND output_media_type IS NOT NULL AND output_size_bytes >= 0 "
            "AND output_sha256 ~ '^[0-9a-f]{64}$')",
            name="ck_export_jobs_completed_file",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_export_jobs_status_created", "export_jobs", ["status", "created_at"])
    op.create_index("ix_export_jobs_expires_at", "export_jobs", ["expires_at"])
    op.create_index(
        "ix_export_jobs_source",
        "export_jobs",
        ["source_type", "source_reference_id"],
    )
    op.create_index(
        "ix_export_jobs_creator_created",
        "export_jobs",
        ["created_by", "created_at"],
    )

    op.create_table(
        "export_items",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("export_job_id", sa.Uuid(), nullable=False),
        sa.Column("sequence_number", sa.BigInteger(), nullable=False),
        sa.Column("source_event_id", sa.BigInteger(), nullable=False),
        sa.Column("original_message_id", sa.Text(), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("device_id", sa.Text(), nullable=False),
        sa.Column("sensor_type", sa.Text(), nullable=False),
        sa.ForeignKeyConstraint(["export_job_id"], ["export_jobs.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["source_event_id"], ["telemetry_events.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "export_job_id", "sequence_number", name="uq_export_items_job_sequence"
        ),
        sa.UniqueConstraint("export_job_id", "source_event_id", name="uq_export_items_job_source"),
    )
    op.create_index(
        "ix_export_items_job_sequence",
        "export_items",
        ["export_job_id", "sequence_number"],
    )
    op.create_index("ix_export_items_source_event", "export_items", ["source_event_id"])
    op.create_index(
        "ix_export_items_device_received",
        "export_items",
        ["device_id", "received_at"],
    )
    op.create_index(
        "ix_export_items_sensor_received",
        "export_items",
        ["sensor_type", "received_at"],
    )

    op.create_table(
        "export_events",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("export_job_id", sa.Uuid(), nullable=False),
        sa.Column("sequence_number", sa.BigInteger(), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.ForeignKeyConstraint(["export_job_id"], ["export_jobs.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "export_job_id", "sequence_number", name="uq_export_events_job_sequence"
        ),
    )
    op.create_index("ix_export_events_occurred_at", "export_events", ["occurred_at"])

    op.execute(
        """
        CREATE FUNCTION reject_export_event_mutation() RETURNS trigger AS $$
        BEGIN RAISE EXCEPTION 'export_events is append-only'; END;
        $$ LANGUAGE plpgsql
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_export_events_append_only BEFORE UPDATE OR DELETE ON export_events
        FOR EACH ROW EXECUTE FUNCTION reject_export_event_mutation()
        """
    )
    op.execute(
        """
        CREATE FUNCTION reject_export_item_mutation() RETURNS trigger AS $$
        BEGIN RAISE EXCEPTION 'export item source identity is immutable'; END;
        $$ LANGUAGE plpgsql
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_export_items_immutable BEFORE UPDATE ON export_items
        FOR EACH ROW EXECUTE FUNCTION reject_export_item_mutation()
        """
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER trg_export_items_immutable ON export_items")
    op.execute("DROP FUNCTION reject_export_item_mutation()")
    op.execute("DROP TRIGGER trg_export_events_append_only ON export_events")
    op.execute("DROP FUNCTION reject_export_event_mutation()")
    op.drop_index("ix_export_events_occurred_at", table_name="export_events")
    op.drop_table("export_events")
    op.drop_index("ix_export_items_sensor_received", table_name="export_items")
    op.drop_index("ix_export_items_device_received", table_name="export_items")
    op.drop_index("ix_export_items_source_event", table_name="export_items")
    op.drop_index("ix_export_items_job_sequence", table_name="export_items")
    op.drop_table("export_items")
    op.drop_index("ix_export_jobs_creator_created", table_name="export_jobs")
    op.drop_index("ix_export_jobs_source", table_name="export_jobs")
    op.drop_index("ix_export_jobs_expires_at", table_name="export_jobs")
    op.drop_index("ix_export_jobs_status_created", table_name="export_jobs")
    op.drop_table("export_jobs")
