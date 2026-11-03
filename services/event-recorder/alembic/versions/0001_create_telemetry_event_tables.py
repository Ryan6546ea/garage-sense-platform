"""Create append-only telemetry event and ingest failure tables.

Revision ID: 0001
Revises:
Create Date: 2026-07-31
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create M1.2 append-only storage."""

    op.create_table(
        "telemetry_events",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("message_id", sa.Text(), nullable=False),
        sa.Column("topic", sa.Text(), nullable=False),
        sa.Column("qos", sa.SmallInteger(), nullable=False),
        sa.Column("retain", sa.Boolean(), nullable=False),
        sa.Column("garage_id", sa.Text(), nullable=False),
        sa.Column("area_id", sa.Text(), nullable=False),
        sa.Column("device_id", sa.Text(), nullable=False),
        sa.Column("sensor_id", sa.Text(), nullable=False),
        sa.Column("point_code", sa.Text(), nullable=False),
        sa.Column("sensor_type", sa.Text(), nullable=False),
        sa.Column("scenario_id", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("quality", sa.Text(), nullable=False),
        sa.Column("source_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("seq", sa.BigInteger(), nullable=False),
        sa.Column(
            "payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ux_telemetry_events_message_id",
        "telemetry_events",
        ["message_id"],
        unique=True,
    )
    op.create_index(
        "ix_telemetry_events_device_source",
        "telemetry_events",
        ["device_id", "source_time"],
    )
    op.create_index(
        "ix_telemetry_events_garage_source",
        "telemetry_events",
        ["garage_id", "source_time"],
    )
    op.create_index(
        "ix_telemetry_events_sensor_type_source",
        "telemetry_events",
        ["sensor_type", "source_time"],
    )
    op.create_index(
        "ix_telemetry_events_received_at",
        "telemetry_events",
        ["received_at"],
    )

    op.create_table(
        "telemetry_ingest_failures",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("topic", sa.Text(), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("payload", sa.LargeBinary(), nullable=False),
        sa.Column("error_type", sa.Text(), nullable=False),
        sa.Column("error_detail", sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_telemetry_ingest_failures_received_at",
        "telemetry_ingest_failures",
        ["received_at"],
    )


def downgrade() -> None:
    """Remove M1.2 storage."""

    op.drop_index(
        "ix_telemetry_ingest_failures_received_at",
        table_name="telemetry_ingest_failures",
    )
    op.drop_table("telemetry_ingest_failures")
    op.drop_index("ix_telemetry_events_received_at", table_name="telemetry_events")
    op.drop_index(
        "ix_telemetry_events_sensor_type_source",
        table_name="telemetry_events",
    )
    op.drop_index(
        "ix_telemetry_events_garage_source",
        table_name="telemetry_events",
    )
    op.drop_index(
        "ix_telemetry_events_device_source",
        table_name="telemetry_events",
    )
    op.drop_index(
        "ux_telemetry_events_message_id",
        table_name="telemetry_events",
    )
    op.drop_table("telemetry_events")
