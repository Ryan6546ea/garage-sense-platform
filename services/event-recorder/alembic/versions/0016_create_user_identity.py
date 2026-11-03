"""Accounts, revocable sessions, login throttling and immutable identity audit."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0016"
down_revision: str | None = "0015"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "users",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("username", sa.String(64), nullable=False, unique=True),
        sa.Column("display_name", sa.String(80), nullable=False),
        sa.Column("password_hash", sa.String(255), nullable=False),
        sa.Column("role", sa.String(20), nullable=False),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("garage_ids", sa.JSON(), nullable=False),
        sa.Column("log_scope", sa.String(20), nullable=False),
        sa.Column("log_export", sa.Boolean(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("role IN ('viewer','operator','admin')", name="ck_users_role"),
        sa.CheckConstraint("status IN ('pending','active','disabled')", name="ck_users_status"),
        sa.CheckConstraint("log_scope IN ('own','garage','all')", name="ck_users_log_scope"),
        sa.CheckConstraint("username = lower(username)", name="ck_users_username_lower"),
        sa.CheckConstraint("version > 0", name="ck_users_version"),
    )
    op.create_table(
        "user_sessions",
        sa.Column("token_hash", sa.String(64), primary_key=True),
        sa.Column("user_id", sa.String(36), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("csrf_token", sa.String(64), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_user_sessions_user_id", "user_sessions", ["user_id"])
    op.create_index("ix_user_sessions_expires_at", "user_sessions", ["expires_at"])
    op.create_table(
        "auth_throttles",
        sa.Column("key", sa.String(64), primary_key=True),
        sa.Column("count", sa.Integer(), nullable=False),
        sa.Column("reset_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "user_audit_events",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("actor_id", sa.String(36)),
        sa.Column("actor", sa.String(64), nullable=False),
        sa.Column("action", sa.String(120), nullable=False),
        sa.Column("target", sa.String(500), nullable=False),
        sa.Column("garage_ids", sa.JSON(), nullable=False),
        sa.Column("outcome", sa.String(40), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("details", sa.JSON(), nullable=False),
    )
    op.create_index("ix_user_audit_events_actor_id", "user_audit_events", ["actor_id"])
    op.create_index("ix_user_audit_events_occurred_at", "user_audit_events", ["occurred_at"])
    op.execute("""
        CREATE FUNCTION reject_user_audit_mutation() RETURNS trigger AS $$
        BEGIN
          RAISE EXCEPTION 'user audit events are append-only';
        END;
        $$ LANGUAGE plpgsql
    """)
    op.execute("""
        CREATE TRIGGER user_audit_append_only BEFORE UPDATE OR DELETE ON user_audit_events
        FOR EACH ROW EXECUTE FUNCTION reject_user_audit_mutation()
    """)
    op.execute("""
        CREATE TRIGGER user_audit_no_truncate BEFORE TRUNCATE ON user_audit_events
        FOR EACH STATEMENT EXECUTE FUNCTION reject_user_audit_mutation()
    """)


def downgrade() -> None:
    op.drop_table("user_audit_events")
    op.execute("DROP FUNCTION reject_user_audit_mutation()")
    op.drop_table("auth_throttles")
    op.drop_table("user_sessions")
    op.drop_table("users")
