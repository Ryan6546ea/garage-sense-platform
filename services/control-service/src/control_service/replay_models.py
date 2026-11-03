"""SQLAlchemy persistence models for the M6.1 replay domain."""

from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    Text,
    UniqueConstraint,
    Uuid,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from control_service.models import Base


class LeaseHolderType(StrEnum):
    RANDOM_RUN = "random_run"
    REPLAY_RUN = "replay_run"


class ReplayStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    STOPPED = "stopped"
    FAILED = "failed"
    INTERRUPTED = "interrupted"


REPLAY_TERMINAL_STATUSES = frozenset(
    {
        ReplayStatus.COMPLETED,
        ReplayStatus.STOPPED,
        ReplayStatus.FAILED,
        ReplayStatus.INTERRUPTED,
    }
)


class ReplayItemStatus(StrEnum):
    PENDING = "pending"
    PUBLISHING = "publishing"
    PUBLISHED = "published"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ExecutionLease(Base):
    __tablename__ = "environment_execution_leases"
    __table_args__ = (
        CheckConstraint("scope = 'G001'", name="ck_execution_leases_scope"),
        CheckConstraint(
            "holder_type IN ('random_run','replay_run')",
            name="ck_execution_leases_holder_type",
        ),
    )

    scope: Mapped[str] = mapped_column(Text, primary_key=True)
    holder_type: Mapped[str] = mapped_column(Text, nullable=False)
    holder_id: Mapped[UUID] = mapped_column(Uuid, nullable=False)
    acquired_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    heartbeat_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)


class ReplayRun(Base):
    __tablename__ = "replay_runs"
    __table_args__ = (
        CheckConstraint(
            "status IN ('pending','running','paused','completed','stopped','failed','interrupted')",
            name="ck_replay_runs_status",
        ),
        CheckConstraint(
            "source_type IN ('test_run','time_range')",
            name="ck_replay_runs_source_type",
        ),
        CheckConstraint(
            "source_type <> 'test_run' OR source_run_id IS NOT NULL",
            name="ck_replay_runs_test_source_run",
        ),
        CheckConstraint(
            "source_type <> 'time_range' OR "
            "(selection ? 'receivedFrom' AND selection ? 'receivedTo')",
            name="ck_replay_runs_time_range_selection",
        ),
        CheckConstraint(
            "speed_multiplier IN (1,2,5,10)",
            name="ck_replay_runs_speed",
        ),
        CheckConstraint(
            "source_time_mode IN ('shifted','preserve')",
            name="ck_replay_runs_source_time_mode",
        ),
        CheckConstraint(
            "total_paused_seconds >= 0 AND item_count >= 0 AND published_count >= 0 "
            "AND failed_count >= 0 AND current_sequence_number >= 0",
            name="ck_replay_runs_nonnegative_counts",
        ),
        Index("ix_replay_runs_status_created", "status", "created_at"),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    source_type: Mapped[str] = mapped_column(Text, nullable=False)
    source_run_id: Mapped[UUID | None] = mapped_column(
        Uuid, ForeignKey("simulation_runs.id", ondelete="RESTRICT")
    )
    selection: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    speed_multiplier: Mapped[int] = mapped_column(Integer, nullable=False)
    source_time_mode: Mapped[str] = mapped_column(Text, nullable=False)
    created_by: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    paused_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    resumed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    total_paused_seconds: Mapped[float] = mapped_column(Float, nullable=False, default=0)
    item_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    published_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failed_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    current_sequence_number: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    first_original_received_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_original_received_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    simulator_instance_id: Mapped[str | None] = mapped_column(Text)
    simulator_suspension_token: Mapped[str | None] = mapped_column(Text)
    reserved_seq_start: Mapped[int | None] = mapped_column(BigInteger)
    reserved_seq_end: Mapped[int | None] = mapped_column(BigInteger)
    failure_reason: Mapped[str | None] = mapped_column(Text)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)


class TelemetrySourceEvent(Base):
    """Read-only union of typed telemetry rows used to build replay manifests."""

    __tablename__ = "telemetry_all_events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    message_id: Mapped[str] = mapped_column(Text, nullable=False)
    topic: Mapped[str] = mapped_column(Text, nullable=False)
    qos: Mapped[int] = mapped_column(Integer, nullable=False)
    retain: Mapped[bool] = mapped_column(Boolean, nullable=False)
    sensor_id: Mapped[str] = mapped_column(Text, nullable=False)
    garage_id: Mapped[str] = mapped_column(Text, nullable=False)
    device_id: Mapped[str] = mapped_column(Text, nullable=False)
    sensor_type: Mapped[str] = mapped_column(Text, nullable=False)
    scenario_id: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    quality: Mapped[str] = mapped_column(Text, nullable=False)
    source_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    seq: Mapped[int] = mapped_column(BigInteger, nullable=False)
    action_stage: Mapped[str] = mapped_column(Text, nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)


class ReplayItem(Base):
    __tablename__ = "replay_items"
    __table_args__ = (
        CheckConstraint(
            "status IN ('pending','publishing','published','failed','cancelled')",
            name="ck_replay_items_status",
        ),
        UniqueConstraint("replay_run_id", "sequence_number", name="uq_replay_items_run_sequence"),
        Index("ix_replay_items_run_status", "replay_run_id", "status"),
        Index("ix_replay_items_source_event", "source_event_id"),
        Index("ux_replay_items_replay_message_id", "replay_message_id", unique=True),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    replay_run_id: Mapped[UUID] = mapped_column(
        Uuid, ForeignKey("replay_runs.id", ondelete="RESTRICT"), nullable=False
    )
    sequence_number: Mapped[int] = mapped_column(Integer, nullable=False)
    source_event_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    original_message_id: Mapped[str] = mapped_column(Text, nullable=False)
    original_source_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    original_received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    original_seq: Mapped[int] = mapped_column(BigInteger, nullable=False)
    topic: Mapped[str] = mapped_column(Text, nullable=False)
    planned_offset_seconds: Mapped[float] = mapped_column(Float, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    replay_message_id: Mapped[str | None] = mapped_column(Text)
    replay_seq: Mapped[int | None] = mapped_column(BigInteger)
    transformed_source_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[str | None] = mapped_column(Text)


class ReplayEvent(Base):
    __tablename__ = "replay_events"
    __table_args__ = (
        UniqueConstraint("replay_run_id", "sequence_number", name="uq_replay_events_run_sequence"),
        Index("ix_replay_events_occurred_at", "occurred_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    replay_run_id: Mapped[UUID] = mapped_column(
        Uuid, ForeignKey("replay_runs.id", ondelete="RESTRICT"), nullable=False
    )
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    sequence_number: Mapped[int] = mapped_column(BigInteger, nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
