"""Add the covering order index used by the system overview.

Revision ID: 0009
Revises: 0008
Create Date: 2026-08-26
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0009"
down_revision: str | None = "0008"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

INDEX_NAME = "ix_telemetry_events_device_received_latest"


def upgrade() -> None:
    op.execute(
        f"CREATE INDEX {INDEX_NAME} "
        "ON telemetry_events (device_id, received_at DESC, id DESC)"
    )


def downgrade() -> None:
    op.drop_index(INDEX_NAME, table_name="telemetry_events")
