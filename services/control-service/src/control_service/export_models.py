"""SQLAlchemy models for asynchronous export jobs and frozen manifests."""

from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    Text,
    UniqueConstraint,
    Uuid,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from control_service.models import Base


class ExportType(StrEnum):
    TELEMETRY = "telemetry"
    RUN_AUDIT = "run_audit"
    COMBINED_BUNDLE = "combined_bundle"


class ExportFormat(StrEnum):
    JSONL = "jsonl"
    CSV = "csv"
    ZIP = "zip"


class ExportStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    EXPIRED = "expired"
    INTERRUPTED = "interrupted"


EXPORT_TERMINAL_STATUSES = frozenset(
    {
        ExportStatus.COMPLETED,
        ExportStatus.FAILED,
        ExportStatus.CANCELLED,
        ExportStatus.EXPIRED,
        ExportStatus.INTERRUPTED,
    }
)


class ExportJob(Base):
    __tablename__ = "export_jobs"
    __table_args__ = (
        CheckConstraint(
            "export_type IN ('telemetry','run_audit','combined_bundle')",
            name="ck_export_jobs_type",
        ),
        CheckConstraint("format IN ('jsonl','csv','zip')", name="ck_export_jobs_format"),
        CheckConstraint(
            "status IN ('pending','running','completed','failed','cancelled',"
            "'expired','interrupted')",
            name="ck_export_jobs_status",
        ),
        CheckConstraint(
            "source_type IN ('time_range','test_run','replay_run')",
            name="ck_export_jobs_source_type",
        ),
        CheckConstraint(
            "source_type = 'time_range' OR source_reference_id IS NOT NULL",
            name="ck_export_jobs_source_reference",
        ),
        CheckConstraint(
            "(export_type = 'telemetry' AND format IN ('jsonl','csv')) OR "
            "(export_type = 'run_audit' AND format IN ('jsonl','zip')) OR "
            "(export_type = 'combined_bundle' AND format = 'zip')",
            name="ck_export_jobs_type_format",
        ),
        CheckConstraint(
            "item_count >= 0 AND processed_count >= 0 AND failed_count >= 0",
            name="ck_export_jobs_nonnegative_counts",
        ),
        CheckConstraint("expires_at > created_at", name="ck_export_jobs_expiry"),
        CheckConstraint(
            "status <> 'completed' OR "
            "(storage_relative_path IS NOT NULL AND output_filename IS NOT NULL "
            "AND output_media_type IS NOT NULL AND output_size_bytes >= 0 "
            "AND output_sha256 ~ '^[0-9a-f]{64}$')",
            name="ck_export_jobs_completed_file",
        ),
        Index("ix_export_jobs_status_created", "status", "created_at"),
        Index("ix_export_jobs_expires_at", "expires_at"),
        Index("ix_export_jobs_source", "source_type", "source_reference_id"),
        Index("ix_export_jobs_creator_created", "created_by", "created_at"),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    export_type: Mapped[str] = mapped_column(Text, nullable=False)
    format: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    source_type: Mapped[str] = mapped_column(Text, nullable=False)
    source_reference_id: Mapped[UUID | None] = mapped_column(Uuid)
    selection: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    include_replay_outputs: Mapped[bool] = mapped_column(Boolean, nullable=False)
    include_ingest_failures: Mapped[bool] = mapped_column(Boolean, nullable=False)
    include_run_audit: Mapped[bool] = mapped_column(Boolean, nullable=False)
    created_by: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    item_count: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    processed_count: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    failed_count: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    output_filename: Mapped[str | None] = mapped_column(Text)
    output_media_type: Mapped[str | None] = mapped_column(Text)
    output_size_bytes: Mapped[int | None] = mapped_column(BigInteger)
    output_sha256: Mapped[str | None] = mapped_column(Text)
    storage_relative_path: Mapped[str | None] = mapped_column(Text)
    failure_reason: Mapped[str | None] = mapped_column(Text)
    cancellation_requested: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)


class ExportItem(Base):
    __tablename__ = "export_items"
    __table_args__ = (
        UniqueConstraint("export_job_id", "sequence_number", name="uq_export_items_job_sequence"),
        UniqueConstraint("export_job_id", "source_event_id", name="uq_export_items_job_source"),
        Index("ix_export_items_job_sequence", "export_job_id", "sequence_number"),
        Index("ix_export_items_source_event", "source_event_id"),
        Index("ix_export_items_device_received", "device_id", "received_at"),
        Index("ix_export_items_sensor_received", "sensor_type", "received_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    export_job_id: Mapped[UUID] = mapped_column(
        Uuid, ForeignKey("export_jobs.id", ondelete="RESTRICT"), nullable=False
    )
    sequence_number: Mapped[int] = mapped_column(BigInteger, nullable=False)
    source_event_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    original_message_id: Mapped[str] = mapped_column(Text, nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    device_id: Mapped[str] = mapped_column(Text, nullable=False)
    sensor_type: Mapped[str] = mapped_column(Text, nullable=False)


class ExportEvent(Base):
    __tablename__ = "export_events"
    __table_args__ = (
        UniqueConstraint("export_job_id", "sequence_number", name="uq_export_events_job_sequence"),
        Index("ix_export_events_occurred_at", "occurred_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    export_job_id: Mapped[UUID] = mapped_column(
        Uuid, ForeignKey("export_jobs.id", ondelete="RESTRICT"), nullable=False
    )
    sequence_number: Mapped[int] = mapped_column(BigInteger, nullable=False)
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)


class TelemetryIngestFailureSource(Base):
    """Read-only mapping for optional failure export."""

    __tablename__ = "telemetry_ingest_failures"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    topic: Mapped[str] = mapped_column(Text, nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    payload: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    error_type: Mapped[str] = mapped_column(Text, nullable=False)
    error_detail: Mapped[str] = mapped_column(Text, nullable=False)
    original_payload_size: Mapped[int] = mapped_column(BigInteger, nullable=False)
    payload_truncated: Mapped[bool] = mapped_column(Boolean, nullable=False)
    payload_sha256: Mapped[str] = mapped_column(Text, nullable=False)
