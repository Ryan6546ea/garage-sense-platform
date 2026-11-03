"""Create camera mock runtime, idempotency, and immutable audit tables.

Revision ID: 0006
Revises: 0005
Create Date: 2026-08-01
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0006"
down_revision: str | None = "0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "camera_runtime_states",
        sa.Column("camera_id", sa.Text(), nullable=False),
        sa.Column("camera_name", sa.Text(), nullable=False),
        sa.Column("garage_id", sa.Text(), nullable=False),
        sa.Column("area_id", sa.Text(), nullable=False),
        sa.Column("stream_path", sa.Text(), nullable=False),
        sa.Column("scenario_id", sa.Text(), nullable=False),
        sa.Column("online", sa.Boolean(), nullable=False),
        sa.Column("preview_url_available", sa.Boolean(), nullable=False),
        sa.Column("stream_desired", sa.Text(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("version", sa.Integer(), server_default="1", nullable=False),
        sa.CheckConstraint(
            "stream_desired IN ('running','stopped')",
            name="ck_camera_runtime_stream_desired",
        ),
        sa.CheckConstraint("version > 0", name="ck_camera_runtime_version"),
        sa.PrimaryKeyConstraint("camera_id"),
        sa.UniqueConstraint("stream_path", name="uq_camera_runtime_stream_path"),
    )
    op.create_table(
        "hikvision_adapter_state",
        sa.Column("adapter_id", sa.Text(), nullable=False),
        sa.Column("scenario_id", sa.Text(), nullable=False),
        sa.Column("auth_available", sa.Boolean(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("version", sa.Integer(), server_default="1", nullable=False),
        sa.CheckConstraint("version > 0", name="ck_hikvision_adapter_version"),
        sa.PrimaryKeyConstraint("adapter_id"),
    )
    op.create_table(
        "camera_scenario_requests",
        sa.Column("id", sa.Uuid(), nullable=False),
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
            name="ck_camera_scenario_requests_status",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ux_camera_scenario_requests_request_id",
        "camera_scenario_requests",
        ["request_id"],
        unique=True,
    )
    op.create_table(
        "camera_events",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("target_id", sa.Text(), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("scenario_id", sa.Text(), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("request_id", sa.Text(), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_camera_events_request_id", "camera_events", ["request_id"])
    op.create_index(
        "ix_camera_events_target_occurred",
        "camera_events",
        ["target_id", "occurred_at"],
    )
    op.execute(
        """
        INSERT INTO camera_runtime_states
          (camera_id,camera_name,garage_id,area_id,stream_path,scenario_id,
           online,preview_url_available,stream_desired,updated_at,version)
        VALUES
          ('G001-CAM-001','升降区域摄像头','G001','A-LIFT','cam001',
           'CAMERA_ONLINE',true,true,'running',CURRENT_TIMESTAMP,1),
          ('G001-CAM-002','横移及车辆检测区域摄像头','G001','A-SHIFT','cam002',
           'CAMERA_ONLINE',true,true,'running',CURRENT_TIMESTAMP,1)
        """
    )
    op.execute(
        """
        INSERT INTO hikvision_adapter_state
          (adapter_id,scenario_id,auth_available,updated_at,version)
        VALUES
          ('HIKVISION-MOCK-AUTH','HIKVISION_AUTH_RECOVER',true,CURRENT_TIMESTAMP,1)
        """
    )
    op.execute(
        """
        CREATE FUNCTION reject_camera_event_mutation() RETURNS trigger AS $$
        BEGIN RAISE EXCEPTION 'camera_events is append-only'; END;
        $$ LANGUAGE plpgsql
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_camera_events_append_only
        BEFORE UPDATE OR DELETE ON camera_events
        FOR EACH ROW EXECUTE FUNCTION reject_camera_event_mutation()
        """
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER trg_camera_events_append_only ON camera_events")
    op.execute("DROP FUNCTION reject_camera_event_mutation()")
    op.drop_index("ix_camera_events_target_occurred", table_name="camera_events")
    op.drop_index("ix_camera_events_request_id", table_name="camera_events")
    op.drop_table("camera_events")
    op.drop_index(
        "ux_camera_scenario_requests_request_id",
        table_name="camera_scenario_requests",
    )
    op.drop_table("camera_scenario_requests")
    op.drop_table("hikvision_adapter_state")
    op.drop_table("camera_runtime_states")
