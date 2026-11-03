"""Add bounded ingest-failure payload audit metadata.

Revision ID: 0002
Revises: 0001
Create Date: 2026-07-31
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add size, truncation, and SHA-256 audit columns without rewriting 0001."""

    op.add_column(
        "telemetry_ingest_failures",
        sa.Column(
            "original_payload_size",
            sa.BigInteger(),
            nullable=False,
            server_default="0",
        ),
    )
    op.add_column(
        "telemetry_ingest_failures",
        sa.Column(
            "payload_truncated",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )
    op.add_column(
        "telemetry_ingest_failures",
        sa.Column(
            "payload_sha256",
            sa.Text(),
            nullable=False,
            server_default="",
        ),
    )
    op.execute(
        "UPDATE telemetry_ingest_failures "
        "SET original_payload_size = octet_length(payload)"
    )
    op.alter_column(
        "telemetry_ingest_failures",
        "original_payload_size",
        server_default=None,
    )
    op.alter_column(
        "telemetry_ingest_failures",
        "payload_truncated",
        server_default=None,
    )
    op.alter_column(
        "telemetry_ingest_failures",
        "payload_sha256",
        server_default=None,
    )


def downgrade() -> None:
    """Remove only the M3 ingest-failure audit columns."""

    op.drop_column("telemetry_ingest_failures", "payload_sha256")
    op.drop_column("telemetry_ingest_failures", "payload_truncated")
    op.drop_column("telemetry_ingest_failures", "original_payload_size")
