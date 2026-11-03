"""Add append-only alarm handling records and the closing operator.

Revision ID: 0014
Revises: 0013
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0014"
down_revision: str | None = "0013"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("alarm_instances", sa.Column("closed_by", sa.Text()))
    # An explicit unknown marker preserves old closures without inventing a person.
    op.execute(
        "UPDATE alarm_instances SET closed_by = 'legacy:unknown' WHERE closed_at IS NOT NULL"
    )
    op.create_check_constraint(
        "ck_alarm_instances_closed_by", "alarm_instances",
        "(closed_at IS NULL AND closed_by IS NULL) OR "
        "(closed_at IS NOT NULL AND closed_by IS NOT NULL AND length(trim(closed_by)) > 0)",
    )
    op.create_table(
        "alarm_handling_records",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("alarm_instance_id", sa.Uuid(),
                  sa.ForeignKey("alarm_instances.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("action", sa.Text(), nullable=False),
        sa.Column("operator", sa.Text(), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("remarks", sa.Text()),
        sa.Column("measures", sa.Text()),
        sa.Column("before_state", postgresql.JSONB(), nullable=False),
        sa.Column("after_state", postgresql.JSONB(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.CheckConstraint(
            "action IN ('activated', 'recovered', 'acknowledged', "
            "'measures_added', 'remark_added', 'closed')",
            name="ck_alarm_handling_records_action",
        ),
        sa.CheckConstraint("length(trim(operator)) > 0", name="ck_alarm_handling_records_operator"),
        sa.CheckConstraint(
            "jsonb_typeof(before_state) = 'object' AND jsonb_typeof(after_state) = 'object'",
            name="ck_alarm_handling_records_states",
        ),
        sa.CheckConstraint(
            "action <> 'measures_added' OR (measures IS NOT NULL AND length(trim(measures)) > 0)",
            name="ck_alarm_handling_records_measures",
        ),
        sa.CheckConstraint(
            "action <> 'remark_added' OR (remarks IS NOT NULL AND length(trim(remarks)) > 0)",
            name="ck_alarm_handling_records_remarks",
        ),
    )
    op.create_index(
        "ix_alarm_handling_records_history", "alarm_handling_records",
        ["alarm_instance_id", "occurred_at", "id"],
    )
    op.execute("""
        CREATE FUNCTION reject_alarm_handling_record_mutation() RETURNS trigger AS $$
        BEGIN
          RAISE EXCEPTION 'alarm handling records are append-only';
        END;
        $$ LANGUAGE plpgsql
    """)
    op.execute("""
        CREATE TRIGGER trg_alarm_handling_records_append_only
        BEFORE UPDATE OR DELETE ON alarm_handling_records
        FOR EACH ROW EXECUTE FUNCTION reject_alarm_handling_record_mutation()
    """)
    op.execute("""
        CREATE TRIGGER trg_alarm_handling_records_no_truncate
        BEFORE TRUNCATE ON alarm_handling_records
        FOR EACH STATEMENT EXECUTE FUNCTION reject_alarm_handling_record_mutation()
    """)


def downgrade() -> None:
    op.drop_table("alarm_handling_records")
    op.execute("DROP FUNCTION reject_alarm_handling_record_mutation()")
    op.drop_constraint("ck_alarm_instances_closed_by", "alarm_instances", type_="check")
    op.drop_column("alarm_instances", "closed_by")
