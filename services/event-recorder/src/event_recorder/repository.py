"""Async PostgreSQL persistence and keyset history queries."""

import hashlib
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime

from sim_core import DataQuality, TelemetryStatus
from sqlalchemy import and_, func, or_, select, text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from event_recorder.config import RecorderSettings
from event_recorder.models import TelemetryEvent, TelemetryIngestFailure


@dataclass(frozen=True, slots=True)
class EventCursor:
    """Stable keyset position for source-time ordered history."""

    source_time: datetime
    event_id: int


@dataclass(frozen=True, slots=True)
class EventFilters:
    """Optional history query filters."""

    garage_id: str | None = None
    sensor_id: str | None = None
    device_id: str | None = None
    sensor_type: str | None = None
    scenario_id: str | None = None
    status: TelemetryStatus | None = None
    quality: DataQuality | None = None
    source_time_from: datetime | None = None
    source_time_to: datetime | None = None


class Database:
    """Own the async SQLAlchemy engine and sessions."""

    def __init__(self, settings: RecorderSettings) -> None:
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
        """Close pooled PostgreSQL connections."""

        await self.engine.dispose()


class EventRepository:
    """Append-only writes and read-only event queries."""

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        *,
        max_failure_payload_bytes: int = 65_536,
    ) -> None:
        self._sessions = sessions
        self._max_failure_payload_bytes = max_failure_payload_bytes

    async def ping(self) -> bool:
        """Return whether PostgreSQL can execute a trivial query."""

        try:
            async with self._sessions() as session:
                await session.execute(text("SELECT 1"))
        except Exception:
            return False
        return True

    async def insert_failure(
        self,
        *,
        topic: str,
        payload: bytes,
        received_at: datetime,
        error_type: str,
        error_detail: str,
    ) -> None:
        """Persist a bounded prefix plus size and hash for exact audit identity."""

        stored_payload = payload[: self._max_failure_payload_bytes]
        async with self._sessions.begin() as session:
            session.add(
                TelemetryIngestFailure(
                    topic=topic,
                    payload=stored_payload,
                    received_at=received_at,
                    error_type=error_type,
                    error_detail=error_detail,
                    original_payload_size=len(payload),
                    payload_truncated=len(stored_payload) != len(payload),
                    payload_sha256=hashlib.sha256(payload).hexdigest(),
                )
            )

    async def list_events(
        self,
        filters: EventFilters,
        *,
        limit: int,
        cursor: EventCursor | None,
    ) -> Sequence[TelemetryEvent]:
        """Return source-time/id ordered events using keyset pagination."""

        statement = select(TelemetryEvent)
        if filters.garage_id is not None:
            statement = statement.where(TelemetryEvent.garage_id == filters.garage_id)
        if filters.sensor_id is not None:
            statement = statement.where(TelemetryEvent.sensor_id == filters.sensor_id)
        if filters.device_id is not None:
            statement = statement.where(TelemetryEvent.device_id == filters.device_id)
        if filters.sensor_type is not None:
            statement = statement.where(TelemetryEvent.sensor_type == filters.sensor_type)
        if filters.scenario_id is not None:
            statement = statement.where(TelemetryEvent.scenario_id == filters.scenario_id)
        if filters.status is not None:
            statement = statement.where(TelemetryEvent.status == filters.status.value)
        if filters.quality is not None:
            statement = statement.where(TelemetryEvent.quality == filters.quality.value)
        if filters.source_time_from is not None:
            statement = statement.where(TelemetryEvent.source_time >= filters.source_time_from)
        if filters.source_time_to is not None:
            statement = statement.where(TelemetryEvent.source_time <= filters.source_time_to)
        if cursor is not None:
            statement = statement.where(
                or_(
                    TelemetryEvent.source_time > cursor.source_time,
                    and_(
                        TelemetryEvent.source_time == cursor.source_time,
                        TelemetryEvent.id > cursor.event_id,
                    ),
                )
            )
        statement = statement.order_by(
            TelemetryEvent.source_time.asc(),
            TelemetryEvent.id.asc(),
        ).limit(limit)
        async with self._sessions() as session:
            return (await session.scalars(statement)).all()

    async def get_event(self, message_id: str) -> TelemetryEvent | None:
        """Look up one event by its globally unique messageId."""

        statement = select(TelemetryEvent).where(TelemetryEvent.message_id == message_id)
        async with self._sessions() as session:
            return (await session.scalars(statement)).one_or_none()

    async def list_failures(self, *, limit: int) -> Sequence[TelemetryIngestFailure]:
        """Return the newest isolated failures for test diagnostics."""

        statement = (
            select(TelemetryIngestFailure)
            .order_by(
                TelemetryIngestFailure.received_at.desc(),
                TelemetryIngestFailure.id.desc(),
            )
            .limit(limit)
        )
        async with self._sessions() as session:
            return (await session.scalars(statement)).all()

    async def count_message_id(self, message_id: str) -> int:
        """Count one messageId for integration and smoke verification."""

        statement = (
            select(func.count())
            .select_from(TelemetryEvent)
            .where(TelemetryEvent.message_id == message_id)
        )
        async with self._sessions() as session:
            return (await session.execute(statement)).scalar_one()

    async def count_failures(self) -> int:
        """Count isolated failures for smoke verification."""

        statement = select(func.count()).select_from(TelemetryIngestFailure)
        async with self._sessions() as session:
            return (await session.execute(statement)).scalar_one()
