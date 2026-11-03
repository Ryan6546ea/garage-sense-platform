"""PostgreSQL boundary for frozen export manifests and job state."""

from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import Select, and_, func, literal, or_, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from control_service.export_domain import (
    CreateExportRequest,
    ReplayRunExportSource,
    TestRunExportSource,
    TimeRangeExportSource,
    source_reference,
)
from control_service.export_models import ExportEvent, ExportItem, ExportJob, ExportStatus
from control_service.models import (
    TERMINAL_RUN_STATUSES,
    RunEvent,
    ScenarioActivation,
    SimulationRun,
)
from control_service.replay_models import (
    REPLAY_TERMINAL_STATUSES,
    ReplayEvent,
    ReplayItem,
    ReplayRun,
    TelemetrySourceEvent,
)


class ExportValidationError(RuntimeError):
    """The requested source cannot be frozen into a valid manifest."""


class ExportTooLargeError(ExportValidationError):
    def __init__(self, actual: int, maximum: int) -> None:
        self.actual = actual
        self.maximum = maximum
        super().__init__(f"export contains {actual} items; maximum is {maximum}")


class ExportConflictError(RuntimeError):
    """The requested export state transition is not allowed."""


class ExportRepository:
    """Transactionally freezes sources and coordinates a bounded worker pool."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    @staticmethod
    async def _append_event(
        session: AsyncSession,
        job_id: UUID,
        event_type: str,
        now: datetime,
        payload: dict[str, Any] | None = None,
    ) -> None:
        sequence = await session.scalar(
            select(func.coalesce(func.max(ExportEvent.sequence_number), 0) + 1).where(
                ExportEvent.export_job_id == job_id
            )
        )
        session.add(
            ExportEvent(
                export_job_id=job_id,
                sequence_number=int(sequence or 1),
                event_type=event_type,
                occurred_at=now,
                payload=payload or {},
            )
        )

    @staticmethod
    def _exclude_replay_outputs(
        statement: Select[tuple[TelemetrySourceEvent]],
    ) -> Select[tuple[TelemetrySourceEvent]]:
        replay_ids = select(ReplayItem.replay_message_id).where(
            ReplayItem.replay_message_id.is_not(None)
        )
        return statement.where(TelemetrySourceEvent.message_id.not_in(replay_ids))

    async def _source_statement(
        self,
        session: AsyncSession,
        request: CreateExportRequest,
    ) -> tuple[Select[tuple[TelemetrySourceEvent]], datetime, datetime]:
        source = request.source
        statement = select(TelemetrySourceEvent)
        if isinstance(source, TimeRangeExportSource):
            start, end = source.received_from, source.received_to
            statement = statement.where(
                TelemetrySourceEvent.received_at >= start,
                TelemetrySourceEvent.received_at < end,
            )
            if source.sensor_ids:
                statement = statement.where(TelemetrySourceEvent.sensor_id.in_(source.sensor_ids))
            if source.garage_ids:
                statement = statement.where(TelemetrySourceEvent.garage_id.in_(source.garage_ids))
            if source.device_ids:
                statement = statement.where(TelemetrySourceEvent.device_id.in_(source.device_ids))
            if source.sensor_types:
                statement = statement.where(
                    TelemetrySourceEvent.sensor_type.in_(source.sensor_types)
                )
            if source.scenario_ids:
                statement = statement.where(
                    TelemetrySourceEvent.scenario_id.in_(source.scenario_ids)
                )
            if source.statuses:
                statement = statement.where(
                    TelemetrySourceEvent.status.in_([item.value for item in source.statuses])
                )
            if source.qualities:
                statement = statement.where(
                    TelemetrySourceEvent.quality.in_([item.value for item in source.qualities])
                )
        elif isinstance(source, TestRunExportSource):
            run = await session.get(SimulationRun, source.run_id)
            if run is None:
                raise ExportValidationError("test run not found")
            if run.status not in {item.value for item in TERMINAL_RUN_STATUSES}:
                raise ExportValidationError("test run must be terminal before export")
            if run.started_at is None or run.finished_at is None:
                raise ExportValidationError("test run has no complete telemetry window")
            registered = run.configuration.get("registeredSensors")
            if registered is not None:
                statement = statement.where(
                    TelemetrySourceEvent.sensor_id.in_([item["sensorId"] for item in registered])
                )
            start, end = run.started_at, run.finished_at
            statement = statement.where(
                TelemetrySourceEvent.received_at >= start,
                TelemetrySourceEvent.received_at <= end,
            )
        else:
            replay_run = await session.get(ReplayRun, source.replay_id)
            if replay_run is None:
                raise ExportValidationError("replay run not found")
            if replay_run.status not in {item.value for item in REPLAY_TERMINAL_STATUSES}:
                raise ExportValidationError("replay run must be terminal before export")
            if replay_run.started_at is None or replay_run.finished_at is None:
                raise ExportValidationError("replay run has no complete telemetry window")
            start, end = replay_run.started_at, replay_run.finished_at
            original_ids = select(ReplayItem.source_event_id).where(
                ReplayItem.replay_run_id == source.replay_id
            )
            if request.include_replay_outputs:
                replay_ids = select(ReplayItem.replay_message_id).where(
                    ReplayItem.replay_run_id == source.replay_id,
                    ReplayItem.replay_message_id.is_not(None),
                )
                statement = statement.where(
                    or_(
                        TelemetrySourceEvent.id.in_(original_ids),
                        TelemetrySourceEvent.message_id.in_(replay_ids),
                    )
                )
            else:
                statement = statement.where(TelemetrySourceEvent.id.in_(original_ids))
        if not request.include_replay_outputs and not isinstance(source, ReplayRunExportSource):
            statement = self._exclude_replay_outputs(statement)
        return (
            statement.order_by(TelemetrySourceEvent.received_at, TelemetrySourceEvent.id),
            start,
            end,
        )

    async def create(
        self,
        request: CreateExportRequest,
        *,
        now: datetime,
        retention_seconds: int,
        max_items: int,
    ) -> ExportJob:
        """Freeze the ordered identity manifest in the job creation transaction."""

        job_id = uuid4()
        async with self._sessions.begin() as session:
            statement, start, end = await self._source_statement(session, request)
            selected_count = int(
                await session.scalar(
                    select(func.count()).select_from(statement.order_by(None).subquery())
                )
                or 0
            )
            if selected_count == 0:
                raise ExportValidationError("export selection contains no telemetry")
            if selected_count > max_items:
                raise ExportTooLargeError(selected_count, max_items)
            selection = {
                "source": request.source.model_dump(mode="json", by_alias=True),
                "resolvedReceivedFrom": start.isoformat(),
                "resolvedReceivedTo": end.isoformat(),
            }
            job = ExportJob(
                id=job_id,
                name=request.name,
                export_type=request.export_type.value,
                format=request.format.value,
                status=ExportStatus.PENDING.value,
                source_type=request.source.type,
                source_reference_id=source_reference(request.source),
                selection=selection,
                include_replay_outputs=request.include_replay_outputs,
                include_ingest_failures=request.include_ingest_failures,
                include_run_audit=request.include_run_audit,
                created_by=request.created_by,
                created_at=now,
                expires_at=now + timedelta(seconds=retention_seconds),
                # The manifest insert below is the source of truth. Under PostgreSQL's
                # READ COMMITTED isolation, a count and a later INSERT ... SELECT may
                # observe different committed telemetry rows.
                item_count=0,
                processed_count=0,
                failed_count=0,
                cancellation_requested=False,
                version=1,
            )
            session.add(job)
            await session.flush()
            ordered = statement.limit(max_items + 1).subquery()
            manifest = select(
                literal(job_id, type_=ExportItem.export_job_id.type),
                func.row_number().over(order_by=(ordered.c.received_at, ordered.c.id)),
                ordered.c.id,
                ordered.c.message_id,
                ordered.c.received_at,
                ordered.c.device_id,
                ordered.c.sensor_type,
            ).select_from(ordered)
            await session.execute(
                insert(ExportItem).from_select(
                    [
                        ExportItem.export_job_id,
                        ExportItem.sequence_number,
                        ExportItem.source_event_id,
                        ExportItem.original_message_id,
                        ExportItem.received_at,
                        ExportItem.device_id,
                        ExportItem.sensor_type,
                    ],
                    manifest,
                )
            )
            frozen_count = int(
                await session.scalar(select(func.count()).where(ExportItem.export_job_id == job_id))
                or 0
            )
            if frozen_count == 0:
                raise ExportValidationError("export selection contains no telemetry")
            if frozen_count > max_items:
                raise ExportTooLargeError(max(selected_count, frozen_count), max_items)
            job.item_count = frozen_count
            await self._append_event(
                session,
                job_id,
                "export_created",
                now,
                {"itemCount": frozen_count},
            )
            await self._append_event(
                session,
                job_id,
                "export_manifest_created",
                now,
                {"itemCount": frozen_count, "order": ["received_at", "id"]},
            )
            return job

    async def get(self, job_id: UUID) -> ExportJob | None:
        async with self._sessions() as session:
            return await session.get(ExportJob, job_id)

    async def list_jobs(
        self,
        *,
        limit: int,
        status: str | None = None,
        export_type: str | None = None,
        format: str | None = None,
        created_by: str | None = None,
        created_from: datetime | None = None,
        created_to: datetime | None = None,
        cursor: tuple[datetime, UUID] | None = None,
    ) -> Sequence[ExportJob]:
        statement = select(ExportJob)
        for condition in (
            ExportJob.status == status if status else None,
            ExportJob.export_type == export_type if export_type else None,
            ExportJob.format == format if format else None,
            ExportJob.created_by == created_by if created_by else None,
            ExportJob.created_at >= created_from if created_from else None,
            ExportJob.created_at <= created_to if created_to else None,
        ):
            if condition is not None:
                statement = statement.where(condition)
        if cursor:
            created, identifier = cursor
            statement = statement.where(
                or_(
                    ExportJob.created_at < created,
                    and_(ExportJob.created_at == created, ExportJob.id < identifier),
                )
            )
        async with self._sessions() as session:
            return (
                await session.scalars(
                    statement.order_by(ExportJob.created_at.desc(), ExportJob.id.desc()).limit(
                        limit
                    )
                )
            ).all()

    async def list_events(
        self, job_id: UUID, *, after_sequence: int | None, limit: int
    ) -> Sequence[ExportEvent]:
        statement = select(ExportEvent).where(ExportEvent.export_job_id == job_id)
        if after_sequence is not None:
            statement = statement.where(ExportEvent.sequence_number > after_sequence)
        async with self._sessions() as session:
            return (
                await session.scalars(statement.order_by(ExportEvent.sequence_number).limit(limit))
            ).all()

    async def claim(self, *, now: datetime) -> ExportJob | None:
        async with self._sessions.begin() as session:
            job = (
                await session.scalars(
                    select(ExportJob)
                    .where(ExportJob.status == ExportStatus.PENDING.value)
                    .order_by(ExportJob.created_at, ExportJob.id)
                    .with_for_update(skip_locked=True)
                    .limit(1)
                )
            ).one_or_none()
            if job is None:
                return None
            job.status = ExportStatus.RUNNING.value
            job.started_at = now
            job.version += 1
            await self._append_event(session, job.id, "export_started", now)
            return job

    async def telemetry_batch(
        self, job_id: UUID, *, after_sequence: int, limit: int
    ) -> Sequence[tuple[ExportItem, TelemetrySourceEvent]]:
        async with self._sessions() as session:
            rows = await session.execute(
                select(ExportItem, TelemetrySourceEvent)
                .join(TelemetrySourceEvent, TelemetrySourceEvent.id == ExportItem.source_event_id)
                .where(
                    ExportItem.export_job_id == job_id,
                    ExportItem.sequence_number > after_sequence,
                )
                .order_by(ExportItem.sequence_number)
                .limit(limit)
            )
            return [(row[0], row[1]) for row in rows.all()]

    async def update_progress(self, job_id: UUID, processed: int, *, now: datetime) -> bool:
        async with self._sessions.begin() as session:
            job = await session.get(ExportJob, job_id, with_for_update=True)
            if job is None or job.status != ExportStatus.RUNNING.value:
                return False
            job.processed_count = processed
            job.version += 1
            await self._append_event(
                session,
                job_id,
                "export_progress",
                now,
                {"processedCount": processed, "itemCount": job.item_count},
            )
            return not job.cancellation_requested

    async def cancellation_requested(self, job_id: UUID) -> bool:
        async with self._sessions() as session:
            value = await session.scalar(
                select(ExportJob.cancellation_requested).where(ExportJob.id == job_id)
            )
            return bool(value)

    async def request_cancel(self, job_id: UUID, *, now: datetime) -> ExportJob:
        async with self._sessions.begin() as session:
            job = await session.get(ExportJob, job_id, with_for_update=True)
            if job is None:
                raise KeyError(job_id)
            if job.status == ExportStatus.CANCELLED.value:
                return job
            if job.status not in {ExportStatus.PENDING.value, ExportStatus.RUNNING.value}:
                raise ExportConflictError(f"{job.status} export cannot be cancelled")
            if not job.cancellation_requested:
                job.cancellation_requested = True
                job.version += 1
                await self._append_event(session, job_id, "export_cancel_requested", now)
            if job.status == ExportStatus.PENDING.value:
                job.status = ExportStatus.CANCELLED.value
                job.finished_at = now
                job.version += 1
                await self._append_event(session, job_id, "export_cancelled", now)
            return job

    async def finalize_cancel(self, job_id: UUID, *, now: datetime) -> None:
        await self._terminal(job_id, ExportStatus.CANCELLED, now=now, event="export_cancelled")

    async def fail(self, job_id: UUID, reason: str, *, now: datetime) -> None:
        await self._terminal(
            job_id, ExportStatus.FAILED, now=now, event="export_failed", reason=reason[:2000]
        )

    async def interrupt(self, job_id: UUID, *, now: datetime) -> None:
        await self._terminal(job_id, ExportStatus.INTERRUPTED, now=now, event="export_interrupted")

    async def _terminal(
        self,
        job_id: UUID,
        status: ExportStatus,
        *,
        now: datetime,
        event: str,
        reason: str | None = None,
    ) -> None:
        async with self._sessions.begin() as session:
            job = await session.get(ExportJob, job_id, with_for_update=True)
            if job is None or job.status != ExportStatus.RUNNING.value:
                return
            job.status = status.value
            job.finished_at = now
            job.failure_reason = reason
            job.version += 1
            await self._append_event(
                session, job_id, event, now, {"reason": reason} if reason else {}
            )

    async def complete(
        self,
        job_id: UUID,
        *,
        filename: str,
        media_type: str,
        size: int,
        sha256: str,
        relative_path: str,
        now: datetime,
    ) -> None:
        async with self._sessions.begin() as session:
            job = await session.get(ExportJob, job_id, with_for_update=True)
            if job is None or job.status != ExportStatus.RUNNING.value:
                raise ExportConflictError("export is no longer running")
            job.output_filename = filename
            job.output_media_type = media_type
            job.output_size_bytes = size
            job.output_sha256 = sha256
            job.storage_relative_path = relative_path
            job.processed_count = job.item_count
            job.status = ExportStatus.COMPLETED.value
            job.finished_at = now
            job.version += 1
            await self._append_event(
                session, job_id, "export_file_finalized", now, {"sizeBytes": size, "sha256": sha256}
            )
            await self._append_event(session, job_id, "export_completed", now)

    async def interrupt_running(self, *, now: datetime) -> list[UUID]:
        async with self._sessions.begin() as session:
            jobs = (
                await session.scalars(
                    select(ExportJob)
                    .where(ExportJob.status == ExportStatus.RUNNING.value)
                    .with_for_update(skip_locked=True)
                )
            ).all()
            for job in jobs:
                job.status = ExportStatus.INTERRUPTED.value
                job.finished_at = now
                job.version += 1
                await self._append_event(session, job.id, "export_interrupted", now)
            return [job.id for job in jobs]

    async def expired_completed(self, *, now: datetime, limit: int = 100) -> Sequence[ExportJob]:
        async with self._sessions() as session:
            return (
                await session.scalars(
                    select(ExportJob)
                    .where(
                        ExportJob.status == ExportStatus.COMPLETED.value,
                        ExportJob.expires_at <= now,
                    )
                    .order_by(ExportJob.expires_at)
                    .limit(limit)
                )
            ).all()

    async def mark_expired(self, job_id: UUID, *, now: datetime, deleted: bool) -> None:
        async with self._sessions.begin() as session:
            job = await session.get(ExportJob, job_id, with_for_update=True)
            if job is None or job.status != ExportStatus.COMPLETED.value:
                return
            if deleted:
                await self._append_event(session, job_id, "export_file_deleted", now)
            job.status = ExportStatus.EXPIRED.value
            job.version += 1
            await self._append_event(
                session, job_id, "export_expired", now, {"fileExisted": deleted}
            )

    async def record_event(
        self, job_id: UUID, event: str, *, now: datetime, payload: dict[str, Any]
    ) -> None:
        async with self._sessions.begin() as session:
            await session.execute(
                select(ExportJob.id).where(ExportJob.id == job_id).with_for_update()
            )
            await self._append_event(session, job_id, event, now, payload)

    async def random_run(self, run_id: UUID) -> SimulationRun:
        async with self._sessions() as session:
            run = await session.get(SimulationRun, run_id)
            if run is None:
                raise ExportValidationError("test run not found")
            return run

    async def replay_run(self, replay_id: UUID) -> ReplayRun:
        async with self._sessions() as session:
            run = await session.get(ReplayRun, replay_id)
            if run is None:
                raise ExportValidationError("replay run not found")
            return run

    async def activation_stream(
        self, run_id: UUID, *, batch_size: int
    ) -> AsyncIterator[ScenarioActivation]:
        statement = (
            select(ScenarioActivation)
            .where(ScenarioActivation.run_id == run_id)
            .order_by(ScenarioActivation.sequence_number, ScenarioActivation.id)
            .execution_options(yield_per=batch_size)
        )
        async with self._sessions() as session:
            result = await session.stream_scalars(statement)
            async for row in result:
                yield row

    async def run_event_stream(self, run_id: UUID, *, batch_size: int) -> AsyncIterator[RunEvent]:
        statement = (
            select(RunEvent)
            .where(RunEvent.run_id == run_id)
            .order_by(RunEvent.sequence_number)
            .execution_options(yield_per=batch_size)
        )
        async with self._sessions() as session:
            result = await session.stream_scalars(statement)
            async for row in result:
                yield row

    async def replay_item_stream(
        self, replay_id: UUID, *, batch_size: int
    ) -> AsyncIterator[ReplayItem]:
        statement = (
            select(ReplayItem)
            .where(ReplayItem.replay_run_id == replay_id)
            .order_by(ReplayItem.sequence_number)
            .execution_options(yield_per=batch_size)
        )
        async with self._sessions() as session:
            result = await session.stream_scalars(statement)
            async for row in result:
                yield row

    async def replay_event_stream(
        self, replay_id: UUID, *, batch_size: int
    ) -> AsyncIterator[ReplayEvent]:
        statement = (
            select(ReplayEvent)
            .where(ReplayEvent.replay_run_id == replay_id)
            .order_by(ReplayEvent.sequence_number)
            .execution_options(yield_per=batch_size)
        )
        async with self._sessions() as session:
            result = await session.stream_scalars(statement)
            async for row in result:
                yield row

    async def failure_batch(
        self, *, start: datetime, end: datetime, after_id: int, limit: int
    ) -> Sequence[Any]:
        from control_service.export_models import TelemetryIngestFailureSource

        async with self._sessions() as session:
            return (
                await session.scalars(
                    select(TelemetryIngestFailureSource)
                    .where(
                        TelemetryIngestFailureSource.received_at >= start,
                        TelemetryIngestFailureSource.received_at <= end,
                        TelemetryIngestFailureSource.id > after_id,
                    )
                    .order_by(TelemetryIngestFailureSource.id)
                    .limit(limit)
                )
            ).all()

    async def ping(self) -> bool:
        try:
            async with self._sessions() as session:
                await session.execute(text("SELECT 1"))
        except Exception:
            return False
        return True


def utc_now() -> datetime:
    return datetime.now(UTC)
