"""SQLAlchemy models for M5 run state, activations, and append-only audit events."""

from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    Text,
    UniqueConstraint,
    Uuid,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class RunType(StrEnum):
    MANUAL = "manual"
    RANDOM = "random"


class RunStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    STOPPED = "stopped"
    FAILED = "failed"
    INTERRUPTED = "interrupted"


class ActivationSource(StrEnum):
    MANUAL = "manual"
    RANDOM = "random"


class ActivationStatus(StrEnum):
    PENDING = "pending"
    DISPATCHING = "dispatching"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class RunEventType(StrEnum):
    RUN_CREATED = "run_created"
    RUN_STARTED = "run_started"
    SCENARIO_SELECTED = "scenario_selected"
    SCENARIO_DISPATCH_STARTED = "scenario_dispatch_started"
    SCENARIO_ACTIVATED = "scenario_activated"
    SCENARIO_FAILED = "scenario_failed"
    RUN_PAUSED = "run_paused"
    RUN_RESUMED = "run_resumed"
    RUN_STOPPED = "run_stopped"
    RUN_COMPLETED = "run_completed"
    RUN_FAILED = "run_failed"
    RUN_INTERRUPTED = "run_interrupted"


ACTIVE_RUN_STATUSES = (RunStatus.RUNNING.value, RunStatus.PAUSED.value)
TERMINAL_RUN_STATUSES = frozenset(
    {
        RunStatus.COMPLETED,
        RunStatus.STOPPED,
        RunStatus.FAILED,
        RunStatus.INTERRUPTED,
    }
)


class Base(DeclarativeBase):
    """Control-service metadata root."""


class SimulationRun(Base):
    """One manual or deterministic random simulation run."""

    __tablename__ = "simulation_runs"
    __table_args__ = (
        CheckConstraint(
            "run_type IN ('manual', 'random')",
            name="ck_simulation_runs_run_type",
        ),
        CheckConstraint(
            "status IN ('pending','running','paused','completed','stopped','failed','interrupted')",
            name="ck_simulation_runs_status",
        ),
        CheckConstraint(
            "requested_duration_seconds > 0",
            name="ck_simulation_runs_duration_positive",
        ),
        CheckConstraint(
            "abnormal_probability >= 0 AND abnormal_probability <= 1",
            name="ck_simulation_runs_probability",
        ),
        CheckConstraint(
            "run_type <> 'random' OR seed IS NOT NULL",
            name="ck_simulation_runs_random_seed",
        ),
        Index("ix_simulation_runs_status_created", "status", "created_at"),
        Index(
            "ux_simulation_runs_one_active_random",
            "run_type",
            unique=True,
            postgresql_where=text("run_type = 'random' AND status IN ('running', 'paused')"),
        ),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    run_type: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    seed: Mapped[int | None] = mapped_column(BigInteger)
    requested_duration_seconds: Mapped[float] = mapped_column(Float, nullable=False)
    abnormal_probability: Mapped[float] = mapped_column(Float, nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    paused_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    resumed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    total_paused_seconds: Mapped[float] = mapped_column(
        Float,
        nullable=False,
        default=0,
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_by: Mapped[str] = mapped_column(Text, nullable=False)
    failure_reason: Mapped[str | None] = mapped_column(Text)
    configuration: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)


class ScenarioActivation(Base):
    """One persisted call to the telemetry-simulator internal API."""

    __tablename__ = "simulation_scenario_activations"
    __table_args__ = (
        CheckConstraint(
            "source IN ('manual', 'random')",
            name="ck_simulation_scenario_activations_source",
        ),
        CheckConstraint(
            "status IN ('pending','dispatching','succeeded','failed','cancelled')",
            name="ck_simulation_scenario_activations_status",
        ),
        CheckConstraint(
            "intended_duration_seconds IS NULL OR intended_duration_seconds > 0",
            name="ck_simulation_scenario_activations_duration_positive",
        ),
        UniqueConstraint(
            "run_id",
            "sequence_number",
            name="uq_simulation_scenario_activations_run_sequence",
        ),
        Index(
            "ux_simulation_scenario_activations_request_id",
            "request_id",
            unique=True,
        ),
        Index(
            "ix_simulation_scenario_activations_run_sequence",
            "run_id",
            "sequence_number",
        ),
        Index(
            "ix_simulation_scenario_activations_target_requested",
            "target_id",
            "requested_at",
        ),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    run_id: Mapped[UUID | None] = mapped_column(
        Uuid,
        ForeignKey("simulation_runs.id", ondelete="RESTRICT"),
    )
    request_id: Mapped[str] = mapped_column(Text, nullable=False)
    target_id: Mapped[str] = mapped_column(Text, nullable=False)
    scenario_id: Mapped[str] = mapped_column(Text, nullable=False)
    source: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    requested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    dispatched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    intended_duration_seconds: Mapped[float | None] = mapped_column(Float)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    simulator_activation_id: Mapped[str | None] = mapped_column(Text)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[str | None] = mapped_column(Text)
    response_payload: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    sequence_number: Mapped[int | None] = mapped_column(BigInteger)


class RunEvent(Base):
    """One immutable event in a run-local strictly ordered stream."""

    __tablename__ = "simulation_run_events"
    __table_args__ = (
        UniqueConstraint(
            "run_id",
            "sequence_number",
            name="uq_simulation_run_events_run_sequence",
        ),
        Index("ix_simulation_run_events_occurred_at", "occurred_at"),
    )

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )
    run_id: Mapped[UUID] = mapped_column(
        Uuid,
        ForeignKey("simulation_runs.id", ondelete="RESTRICT"),
        nullable=False,
    )
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    sequence_number: Mapped[int] = mapped_column(BigInteger, nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
