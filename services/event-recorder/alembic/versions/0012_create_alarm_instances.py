"""Add standalone alarm instance storage to the shared migration chain.

Revision ID: 0012
Revises: 0011
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0012"
down_revision: str | None = "0011"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "alarm_instances",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("garage_id", sa.Text(), nullable=False),
        sa.Column("device_id", sa.Text(), nullable=False),
        sa.Column("point_code", sa.Text(), nullable=False),
        sa.Column("condition_key", sa.Text(), nullable=False),
        sa.Column("alarm_code", sa.Text(), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("priority", sa.Integer(), nullable=False),
        sa.Column("rule_code", sa.Text(), nullable=False),
        sa.Column("rule_version", sa.Integer(), nullable=False),
        sa.Column("rule_snapshot", postgresql.JSONB(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("activated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("returned_to_normal_at", sa.DateTime(timezone=True)),
        sa.Column("acked_at", sa.DateTime(timezone=True)),
        sa.Column("acked_by", sa.Text()),
        sa.Column("closed_at", sa.DateTime(timezone=True)),
        sa.Column("trigger_message_id", sa.Text()),
        sa.Column("recovery_message_id", sa.Text()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("version", sa.Integer(), nullable=False, server_default=sa.text("1")),
        sa.CheckConstraint("priority BETWEEN 1 AND 4", name="ck_alarm_instances_priority"),
        sa.CheckConstraint("rule_version > 0", name="ck_alarm_instances_rule_version"),
        sa.CheckConstraint("version > 0", name="ck_alarm_instances_version"),
        sa.CheckConstraint(
            "jsonb_typeof(rule_snapshot) = 'object'", name="ck_alarm_instances_rule_snapshot",
        ),
        sa.CheckConstraint(
            "(is_active AND returned_to_normal_at IS NULL) OR "
            "(NOT is_active AND returned_to_normal_at IS NOT NULL)",
            name="ck_alarm_instances_recovery",
        ),
        sa.CheckConstraint(
            "(acked_at IS NULL AND acked_by IS NULL) OR "
            "(acked_at IS NOT NULL AND acked_by IS NOT NULL AND length(trim(acked_by)) > 0)",
            name="ck_alarm_instances_ack",
        ),
        sa.CheckConstraint(
            "closed_at IS NULL OR (NOT is_active AND acked_at IS NOT NULL "
            "AND closed_at >= returned_to_normal_at AND closed_at >= acked_at)",
            name="ck_alarm_instances_close",
        ),
        sa.CheckConstraint(
            "last_observed_at >= activated_at "
            "AND (returned_to_normal_at IS NULL OR "
            "returned_to_normal_at BETWEEN activated_at AND last_observed_at) "
            "AND (acked_at IS NULL OR acked_at >= activated_at) "
            "AND updated_at >= created_at",
            name="ck_alarm_instances_times",
        ),
    )
    op.create_index(
        "ux_alarm_instances_active_condition", "alarm_instances",
        ["garage_id", "device_id", "condition_key"],
        unique=True, postgresql_where=sa.text("is_active"),
    )
    op.create_index(
        "ix_alarm_instances_device_history", "alarm_instances",
        ["garage_id", "device_id", "activated_at"],
    )
    op.create_index(
        "ix_alarm_instances_open", "alarm_instances", ["garage_id", "priority", "activated_at"],
        postgresql_where=sa.text("closed_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_table("alarm_instances")
