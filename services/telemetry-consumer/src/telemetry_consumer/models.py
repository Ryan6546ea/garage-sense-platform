"""SQLAlchemy models for sensor-type-specific telemetry tables."""

from datetime import datetime
from typing import Any

from sqlalchemy import (
    ARRAY,
    BigInteger,
    Boolean,
    DateTime,
    Double,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    SmallInteger,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Telemetry consumer declarative base."""


class TelemetryEventIdentity(Base):
    """Global identity and idempotency key shared by every typed event table."""

    __tablename__ = "telemetry_event_identities"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    message_id: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    sensor_type: Mapped[str] = mapped_column(Text, nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class TelemetryIngestFailure(Base):
    """Bounded invalid delivery retained for diagnostics."""

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


class TelemetryEventBase(Base):
    """Common envelope fields shared by every typed telemetry table."""

    __abstract__ = True

    id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("telemetry_event_identities.id", ondelete="RESTRICT"),
        primary_key=True,
        autoincrement=False,
    )
    message_id: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    topic: Mapped[str] = mapped_column(Text, nullable=False)
    qos: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    retain: Mapped[bool] = mapped_column(Boolean, nullable=False)
    garage_id: Mapped[str] = mapped_column(Text, nullable=False)
    area_id: Mapped[str] = mapped_column(Text, nullable=False)
    device_id: Mapped[str] = mapped_column(Text, nullable=False)
    sensor_id: Mapped[str] = mapped_column(Text, nullable=False)
    point_code: Mapped[str] = mapped_column(Text, nullable=False)
    source_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    seq: Mapped[int] = mapped_column(BigInteger, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    quality: Mapped[str] = mapped_column(Text, nullable=False)
    action_stage: Mapped[str] = mapped_column(Text, nullable=False)
    scenario_id: Mapped[str] = mapped_column(Text, nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)


class LaserDistanceTelemetryEvent(TelemetryEventBase):
    __tablename__ = "telemetry_laser_distance_events"

    vehicle_present: Mapped[bool] = mapped_column(Boolean, nullable=False)
    distance_m: Mapped[float | None] = mapped_column(Double)
    displacement_m: Mapped[float | None] = mapped_column(Double)
    position_status: Mapped[str] = mapped_column(Text, nullable=False)


class CurrentTelemetryEvent(TelemetryEventBase):
    __tablename__ = "telemetry_current_events"

    unit: Mapped[str] = mapped_column(Text, nullable=False)
    phase: Mapped[str] = mapped_column(Text, nullable=False)
    sample_rate_hz: Mapped[int] = mapped_column(Integer, nullable=False)
    sample_count: Mapped[int] = mapped_column(Integer, nullable=False)
    window_start_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    window_end_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    current_samples: Mapped[list[float]] = mapped_column(ARRAY(Double), nullable=False)


class VibrationTelemetryEvent(TelemetryEventBase):
    __tablename__ = "telemetry_vibration_events"

    unit: Mapped[str] = mapped_column(Text, nullable=False)
    sample_rate_hz: Mapped[int] = mapped_column(Integer, nullable=False)
    sample_count: Mapped[int] = mapped_column(Integer, nullable=False)
    samples: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False)
    features: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)


class EnvironmentTelemetryEvent(TelemetryEventBase):
    __tablename__ = "telemetry_environment_events"

    temperature: Mapped[float | None] = mapped_column(Double)
    temperature_unit: Mapped[str] = mapped_column(Text, nullable=False)
    humidity: Mapped[float | None] = mapped_column(Double)
    humidity_unit: Mapped[str] = mapped_column(Text, nullable=False)
    pressure_kpa: Mapped[float | None] = mapped_column(Double)
    altitude_meters: Mapped[float | None] = mapped_column(Double)
    condensation_risk: Mapped[bool | None] = mapped_column(Boolean)


class VisionEventTelemetryEvent(TelemetryEventBase):
    __tablename__ = "telemetry_vision_events"

    camera_id: Mapped[str] = mapped_column(Text, nullable=False)
    person_detected: Mapped[bool] = mapped_column(Boolean, nullable=False)
    vehicle_detected: Mapped[bool] = mapped_column(Boolean, nullable=False)
    vehicle_position: Mapped[str] = mapped_column(Text, nullable=False)
    confidence: Mapped[float] = mapped_column(Double, nullable=False)


class MechanismStateTelemetryEvent(TelemetryEventBase):
    __tablename__ = "telemetry_mechanism_state_events"

    plc_state: Mapped[str] = mapped_column(Text, nullable=False)
    at_position: Mapped[bool | None] = mapped_column(Boolean)
    lift_limit_active: Mapped[bool | None] = mapped_column(Boolean)
    shift_limit_active: Mapped[bool | None] = mapped_column(Boolean)
    interlock_closed: Mapped[bool | None] = mapped_column(Boolean)
    emergency_stop: Mapped[bool | None] = mapped_column(Boolean)
    load_kg: Mapped[float | None] = mapped_column(Double)
