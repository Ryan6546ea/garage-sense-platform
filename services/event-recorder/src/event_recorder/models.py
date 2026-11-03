"""SQLAlchemy models for typed telemetry history and ingest failures."""

from datetime import datetime
from typing import Any

from sqlalchemy import BigInteger, Boolean, DateTime, Index, LargeBinary, SmallInteger, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Declarative metadata root used by Alembic."""


class TelemetryEvent(Base):
    """Read-only row projected by the six-table telemetry union view."""

    __tablename__ = "telemetry_all_events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    message_id: Mapped[str] = mapped_column(Text, nullable=False)
    topic: Mapped[str] = mapped_column(Text, nullable=False)
    qos: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    retain: Mapped[bool] = mapped_column(Boolean, nullable=False)
    garage_id: Mapped[str] = mapped_column(Text, nullable=False)
    area_id: Mapped[str] = mapped_column(Text, nullable=False)
    device_id: Mapped[str] = mapped_column(Text, nullable=False)
    sensor_id: Mapped[str] = mapped_column(Text, nullable=False)
    point_code: Mapped[str] = mapped_column(Text, nullable=False)
    sensor_type: Mapped[str] = mapped_column(Text, nullable=False)
    scenario_id: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    quality: Mapped[str] = mapped_column(Text, nullable=False)
    source_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    seq: Mapped[int] = mapped_column(BigInteger, nullable=False)
    action_stage: Mapped[str] = mapped_column(Text, nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)


class TelemetryIngestFailure(Base):
    """One isolated invalid MQTT payload."""

    __tablename__ = "telemetry_ingest_failures"
    __table_args__ = (Index("ix_telemetry_ingest_failures_received_at", "received_at"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    topic: Mapped[str] = mapped_column(Text, nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    payload: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    error_type: Mapped[str] = mapped_column(Text, nullable=False)
    error_detail: Mapped[str] = mapped_column(Text, nullable=False)
    original_payload_size: Mapped[int] = mapped_column(BigInteger, nullable=False)
    payload_truncated: Mapped[bool] = mapped_column(Boolean, nullable=False)
    payload_sha256: Mapped[str] = mapped_column(Text, nullable=False)
