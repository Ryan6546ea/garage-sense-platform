"""Async PostgreSQL repository with optimistic run transitions and ordered events."""

from collections.abc import Mapping, Sequence
from contextlib import suppress
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from sim_core import DeviceSemanticError, DeviceSemanticRegistry
from sqlalchemy import and_, delete, func, or_, select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from control_service.config import ControlSettings
from control_service.domain import ActivationCursor, RandomRunConfiguration, RunCursor
from control_service.models import (
    ACTIVE_RUN_STATUSES,
    ActivationSource,
    ActivationStatus,
    RunEvent,
    RunEventType,
    RunStatus,
    RunType,
    ScenarioActivation,
    SimulationRun,
)
from control_service.replay_models import ExecutionLease, LeaseHolderType


class RunConflictError(RuntimeError):
    """A requested state transition conflicts with persisted state."""


class ActiveRunConflictError(RunConflictError):
    """The database already contains a running or paused random run."""

    def __init__(self, run_id: UUID | None = None) -> None:
        self.run_id = run_id
        detail = (
            f"another random run is active: {run_id}"
            if run_id is not None
            else "another random run is active"
        )
        super().__init__(detail)


class ExecutionLeaseConflictError(RunConflictError):
    """The garage-wide execution lease belongs to another operation."""

    def __init__(self, lease: ExecutionLease) -> None:
        self.scope = lease.scope
        self.holder_type = lease.holder_type
        self.holder_id = lease.holder_id
        super().__init__(f"{lease.scope} is leased by {lease.holder_type} {lease.holder_id}")


class Database:
    """Own the control-service async engine and session factory."""

    def __init__(self, settings: ControlSettings) -> None:
        self.engine: AsyncEngine = create_async_engine(
            settings.database_url,
            pool_pre_ping=True,
        )
        self.sessions = async_sessionmaker(
            self.engine,
            expire_on_commit=False,
            class_=AsyncSession,
        )

    async def dispose(self) -> None:
        await self.engine.dispose()


class ControlRepository:
    """Persistence boundary for run state, activations, and append-only events."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions
        self.registry_enabled = False

    async def ping(self) -> bool:
        try:
            async with self._sessions() as session:
                await session.execute(text("SELECT 1"))
        except Exception:
            return False
        return True

    async def acquire_execution_lease(
        self,
        *,
        holder_type: LeaseHolderType,
        holder_id: UUID,
        now: datetime,
        scope: str = "G001",
    ) -> ExecutionLease:
        """Acquire the cross-run lease transactionally or report its owner."""

        statement = (
            insert(ExecutionLease)
            .values(
                scope=scope,
                holder_type=holder_type.value,
                holder_id=holder_id,
                acquired_at=now,
                heartbeat_at=now,
                version=1,
            )
            .on_conflict_do_nothing(index_elements=[ExecutionLease.scope])
            .returning(ExecutionLease)
        )
        async with self._sessions.begin() as session:
            await session.execute(text("SELECT pg_advisory_xact_lock(7150015)"))
            lease = (await session.scalars(statement)).one_or_none()
            if lease is None:
                lease = (
                    await session.scalars(
                        select(ExecutionLease)
                        .where(ExecutionLease.scope == scope)
                        .with_for_update()
                    )
                ).one()
                if lease.holder_type != holder_type.value or lease.holder_id != holder_id:
                    raise ExecutionLeaseConflictError(lease)
            return lease

    async def current_execution_lease(self, scope: str = "G001") -> ExecutionLease | None:
        async with self._sessions() as session:
            return await session.get(ExecutionLease, scope)

    async def heartbeat_execution_lease(
        self,
        *,
        holder_type: LeaseHolderType,
        holder_id: UUID,
        now: datetime,
        scope: str = "G001",
    ) -> None:
        statement = (
            update(ExecutionLease)
            .where(
                ExecutionLease.scope == scope,
                ExecutionLease.holder_type == holder_type.value,
                ExecutionLease.holder_id == holder_id,
            )
            .values(heartbeat_at=now, version=ExecutionLease.version + 1)
            .returning(ExecutionLease.scope)
        )
        async with self._sessions.begin() as session:
            updated = (await session.scalars(statement)).one_or_none()
            if updated is None:
                raise RunConflictError("execution lease ownership was lost")

    async def release_execution_lease(
        self,
        *,
        holder_type: LeaseHolderType,
        holder_id: UUID,
        scope: str = "G001",
    ) -> bool:
        statement = (
            delete(ExecutionLease)
            .where(
                ExecutionLease.scope == scope,
                ExecutionLease.holder_type == holder_type.value,
                ExecutionLease.holder_id == holder_id,
            )
            .returning(ExecutionLease.scope)
        )
        async with self._sessions.begin() as session:
            return (await session.scalars(statement)).one_or_none() is not None

    async def remove_execution_lease(self, scope: str = "G001") -> bool:
        """Startup-only stale-lease cleanup after persisted owner inspection."""

        async with self._sessions.begin() as session:
            deleted = await session.scalar(
                delete(ExecutionLease)
                .where(ExecutionLease.scope == scope)
                .returning(ExecutionLease.scope)
            )
            return deleted is not None

    async def create_run(
        self,
        *,
        name: str,
        created_by: str,
        configuration: RandomRunConfiguration,
        now: datetime,
        run_id: UUID | None = None,
    ) -> SimulationRun:
        row = SimulationRun(
            id=run_id or uuid4(),
            run_type=RunType.RANDOM.value,
            status=RunStatus.PENDING.value,
            name=name,
            seed=configuration.seed,
            requested_duration_seconds=configuration.duration_seconds,
            abnormal_probability=configuration.abnormal_probability,
            total_paused_seconds=0,
            created_at=now,
            updated_at=now,
            created_by=created_by,
            configuration=configuration.model_dump(by_alias=True),
            version=1,
        )
        async with self._sessions.begin() as session:
            if self.registry_enabled:
                from sim_core.catalog import RegisteredSensor

                await session.execute(text("SELECT pg_advisory_xact_lock(7150015)"))
                records = (
                    (
                        await session.execute(
                            text(
                                "SELECT sensor_id,device_id,garage_id,area_id,sensor_type,n"
                                "ame,point_code,role,"
                                "camera_id,topic,publish_interval_seconds,seed,parameters,d"
                                "esired_enabled,"
                                "archived,config_version FROM sensor_instances ORDER BY sensor_id"
                            )
                        )
                    )
                    .mappings()
                    .all()
                )
                sensors = [RegisteredSensor.model_validate(dict(item)) for item in records]
                selected = [
                    sensor
                    for sensor in sensors
                    if sensor.definition().target_id in configuration.allowed_targets
                    or sensor.device_id in configuration.allowed_targets
                ]
                values = configuration.model_dump(by_alias=True)
                values["registeredSensors"] = [
                    sensor.model_dump(by_alias=True) for sensor in selected
                ]
                try:
                    configuration = RandomRunConfiguration.model_validate(values)
                except ValueError as exc:
                    raise RunConflictError(
                        "registration changed; reload available targets"
                    ) from exc
                configuration = configuration.model_copy(
                    update={
                        "allowed_targets": [
                            configuration.definition(target).target_id
                            for target in configuration.allowed_targets
                        ]
                    }
                )
                row.configuration = configuration.model_dump(by_alias=True)
            session.add(row)
            await session.flush()
            await self._append_event_in_session(
                session,
                row.id,
                RunEventType.RUN_CREATED,
                now,
                {
                    "name": name,
                    "seed": configuration.seed,
                    "configuration": configuration.model_dump(by_alias=True),
                },
            )
        return row

    async def get_run(self, run_id: UUID) -> SimulationRun | None:
        async with self._sessions() as session:
            return await session.get(SimulationRun, run_id)

    async def active_random_run(self) -> SimulationRun | None:
        statement = select(SimulationRun).where(
            SimulationRun.run_type == RunType.RANDOM.value,
            SimulationRun.status.in_(ACTIVE_RUN_STATUSES),
        )
        async with self._sessions() as session:
            return (await session.scalars(statement)).one_or_none()

    async def list_runs(
        self,
        *,
        limit: int,
        cursor: RunCursor | None = None,
    ) -> Sequence[SimulationRun]:
        statement = select(SimulationRun)
        if cursor is not None:
            cursor_id = UUID(cursor.run_id)
            statement = statement.where(
                or_(
                    SimulationRun.created_at < cursor.created_at,
                    and_(
                        SimulationRun.created_at == cursor.created_at,
                        SimulationRun.id < cursor_id,
                    ),
                )
            )
        statement = statement.order_by(
            SimulationRun.created_at.desc(),
            SimulationRun.id.desc(),
        ).limit(limit)
        async with self._sessions() as session:
            return (await session.scalars(statement)).all()

    async def transition_run(
        self,
        run: SimulationRun,
        *,
        allowed: frozenset[RunStatus],
        target: RunStatus,
        now: datetime,
        fields: Mapping[str, object] | None = None,
    ) -> SimulationRun:
        if RunStatus(run.status) not in allowed:
            raise RunConflictError(
                f"run {run.id} cannot transition from {run.status} to {target.value}"
            )
        values: dict[str, object] = {
            "status": target.value,
            "updated_at": now,
            "version": run.version + 1,
        }
        if fields:
            values.update(fields)
        statement = (
            update(SimulationRun)
            .where(
                SimulationRun.id == run.id,
                SimulationRun.version == run.version,
                SimulationRun.status.in_([item.value for item in allowed]),
            )
            .values(**values)
            .returning(SimulationRun)
        )
        try:
            async with self._sessions.begin() as session:
                updated = (await session.scalars(statement)).one_or_none()
        except IntegrityError as exc:
            active = await self.active_random_run()
            raise ActiveRunConflictError(active.id if active else None) from exc
        if updated is None:
            raise RunConflictError(f"run {run.id} version conflict")
        return updated

    async def append_event(
        self,
        run_id: UUID,
        event_type: RunEventType,
        occurred_at: datetime,
        payload: dict[str, Any],
    ) -> RunEvent:
        async with self._sessions.begin() as session:
            return await self._append_event_in_session(
                session,
                run_id,
                event_type,
                occurred_at,
                payload,
            )

    async def _append_event_in_session(
        self,
        session: AsyncSession,
        run_id: UUID,
        event_type: RunEventType,
        occurred_at: datetime,
        payload: dict[str, Any],
    ) -> RunEvent:
        await session.execute(
            select(SimulationRun.id).where(SimulationRun.id == run_id).with_for_update()
        )
        maximum = await session.scalar(
            select(func.max(RunEvent.sequence_number)).where(RunEvent.run_id == run_id)
        )
        event = RunEvent(
            run_id=run_id,
            event_type=event_type.value,
            occurred_at=occurred_at,
            sequence_number=(maximum or 0) + 1,
            payload=payload,
        )
        session.add(event)
        await session.flush()
        return event

    async def list_events(
        self,
        run_id: UUID,
        *,
        limit: int,
        after_sequence: int | None = None,
    ) -> Sequence[RunEvent]:
        statement = select(RunEvent).where(RunEvent.run_id == run_id)
        if after_sequence is not None:
            statement = statement.where(RunEvent.sequence_number > after_sequence)
        statement = statement.order_by(RunEvent.sequence_number.asc()).limit(limit)
        async with self._sessions() as session:
            return (await session.scalars(statement)).all()

    async def create_activation(
        self,
        *,
        run_id: UUID | None,
        request_id: str,
        target_id: str,
        scenario_id: str,
        source: ActivationSource,
        duration_seconds: float | None,
        requested_at: datetime,
        expires_at: datetime | None,
        activation_id: UUID | None = None,
    ) -> ScenarioActivation:
        async with self._sessions.begin() as session:
            if self.registry_enabled:
                await session.execute(text("SELECT pg_advisory_xact_lock(7150015)"))
                if target_id.startswith("sensor:"):
                    available = (
                        await session.execute(
                            text(
                                "SELECT EXISTS(SELECT 1 FROM sensor_instances "
                                "WHERE sensor_id=:sensor "
                                "AND desired_enabled AND NOT archived)"
                            ),
                            {"sensor": target_id.removeprefix("sensor:")},
                        )
                    ).scalar_one()
                    if not available:
                        raise RunConflictError("sensor is disabled or archived")
            sequence: int | None = None
            if run_id is not None:
                await session.execute(
                    select(SimulationRun.id).where(SimulationRun.id == run_id).with_for_update()
                )
                maximum = await session.scalar(
                    select(func.max(ScenarioActivation.sequence_number)).where(
                        ScenarioActivation.run_id == run_id
                    )
                )
                sequence = (maximum or 0) + 1
            row = ScenarioActivation(
                id=activation_id or uuid4(),
                run_id=run_id,
                request_id=request_id,
                target_id=target_id,
                scenario_id=scenario_id,
                source=source.value,
                status=ActivationStatus.PENDING.value,
                requested_at=requested_at,
                intended_duration_seconds=duration_seconds,
                expires_at=expires_at,
                attempt_count=0,
                sequence_number=sequence,
            )
            session.add(row)
            await session.flush()
            return row

    async def get_activation(self, activation_id: UUID) -> ScenarioActivation | None:
        async with self._sessions() as session:
            return await session.get(ScenarioActivation, activation_id)

    async def update_activation(
        self,
        activation_id: UUID,
        **values: object,
    ) -> ScenarioActivation:
        statement = (
            update(ScenarioActivation)
            .where(ScenarioActivation.id == activation_id)
            .values(**values)
            .returning(ScenarioActivation)
        )
        async with self._sessions.begin() as session:
            row = (await session.scalars(statement)).one()
        return row

    async def list_activations(
        self,
        *,
        target_id: str | None = None,
        scenario_id: str | None = None,
        source: ActivationSource | None = None,
        status: ActivationStatus | None = None,
        requested_from: datetime | None = None,
        requested_to: datetime | None = None,
        run_id: UUID | None = None,
        limit: int,
        cursor: ActivationCursor | None = None,
    ) -> Sequence[ScenarioActivation]:
        statement = select(ScenarioActivation)
        if target_id is not None:
            aliases = {target_id}
            with suppress(DeviceSemanticError):
                aliases.add(DeviceSemanticRegistry().by_device(target_id).target_id)
            statement = statement.where(ScenarioActivation.target_id.in_(aliases))
        if scenario_id is not None:
            statement = statement.where(ScenarioActivation.scenario_id == scenario_id)
        if source is not None:
            statement = statement.where(ScenarioActivation.source == source.value)
        if status is not None:
            statement = statement.where(ScenarioActivation.status == status.value)
        if requested_from is not None:
            statement = statement.where(ScenarioActivation.requested_at >= requested_from)
        if requested_to is not None:
            statement = statement.where(ScenarioActivation.requested_at <= requested_to)
        if run_id is not None:
            statement = statement.where(ScenarioActivation.run_id == run_id)
        if cursor is not None:
            cursor_id = UUID(cursor.activation_id)
            statement = statement.where(
                or_(
                    ScenarioActivation.requested_at < cursor.requested_at,
                    and_(
                        ScenarioActivation.requested_at == cursor.requested_at,
                        ScenarioActivation.id < cursor_id,
                    ),
                )
            )
        statement = statement.order_by(
            ScenarioActivation.requested_at.desc(),
            ScenarioActivation.id.desc(),
        ).limit(limit)
        async with self._sessions() as session:
            return (await session.scalars(statement)).all()

    async def activation_counts(self, run_id: UUID) -> tuple[int, int]:
        statement = (
            select(ScenarioActivation.status, func.count())
            .where(ScenarioActivation.run_id == run_id)
            .group_by(ScenarioActivation.status)
        )
        async with self._sessions() as session:
            rows = (await session.execute(statement)).tuples().all()
            values: dict[str, int] = {
                str(activation_status): int(count) for activation_status, count in rows
            }
        return (
            int(values.get(ActivationStatus.SUCCEEDED.value, 0)),
            int(values.get(ActivationStatus.FAILED.value, 0)),
        )

    async def current_scenarios(self, run_id: UUID) -> dict[str, str]:
        ranked = (
            select(
                ScenarioActivation.target_id,
                ScenarioActivation.scenario_id,
                func.row_number()
                .over(
                    partition_by=ScenarioActivation.target_id,
                    order_by=ScenarioActivation.sequence_number.desc(),
                )
                .label("position"),
            )
            .where(
                ScenarioActivation.run_id == run_id,
                ScenarioActivation.status == ActivationStatus.SUCCEEDED.value,
            )
            .subquery()
        )
        statement = select(ranked.c.target_id, ranked.c.scenario_id).where(ranked.c.position == 1)
        async with self._sessions() as session:
            rows = (await session.execute(statement)).tuples().all()
            return {str(target_id): str(scenario_id) for target_id, scenario_id in rows}

    async def interrupt_active_runs(
        self,
        *,
        now: datetime,
        reason: str,
    ) -> list[UUID]:
        interrupted: list[UUID] = []
        async with self._sessions.begin() as session:
            rows = (
                await session.scalars(
                    select(SimulationRun)
                    .where(SimulationRun.status.in_(ACTIVE_RUN_STATUSES))
                    .with_for_update()
                )
            ).all()
            for row in rows:
                row.status = RunStatus.INTERRUPTED.value
                row.finished_at = now
                row.updated_at = now
                row.failure_reason = reason
                row.version += 1
                await self._append_event_in_session(
                    session,
                    row.id,
                    RunEventType.RUN_INTERRUPTED,
                    now,
                    {"reason": reason, "serviceStartedAt": now.isoformat()},
                )
                interrupted.append(row.id)
        return interrupted


def utc_now() -> datetime:
    """Production wall clock kept as an injectable callable."""

    return datetime.now(UTC)
