"""Seed the second synthetic garage camera registry.

Revision ID: 0008
Revises: 0007
Create Date: 2026-08-26
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0008"
down_revision: str | None = "0007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        INSERT INTO camera_runtime_states
          (camera_id,camera_name,garage_id,area_id,stream_path,scenario_id,
           online,preview_url_available,stream_desired,updated_at,version)
        VALUES
          ('G002-CAM-001','高海拔升降区域摄像头','G002','A-LIFT','cam003',
           'CAMERA_ONLINE',true,true,'running',CURRENT_TIMESTAMP,1),
          ('G002-CAM-002','高海拔横移及车辆检测区域摄像头','G002','A-SHIFT','cam004',
           'CAMERA_ONLINE',true,true,'running',CURRENT_TIMESTAMP,1)
        ON CONFLICT (camera_id) DO NOTHING
        """
    )


def downgrade() -> None:
    op.execute(
        "DELETE FROM camera_runtime_states WHERE camera_id IN ('G002-CAM-001','G002-CAM-002')"
    )
