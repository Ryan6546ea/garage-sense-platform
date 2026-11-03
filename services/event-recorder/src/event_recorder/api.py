"""FastAPI health and read-only telemetry history endpoints."""

import base64
import json
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Annotated, Any, Protocol

from fastapi import FastAPI, HTTPException, Query, status
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field
from sim_core import DataQuality, TelemetryStatus
from sim_core.identity_api import AuthSettings
from sim_core.identity_guard import install_identity
from sim_core.identity_store import IdentityStore

from event_recorder.config import RecorderSettings
from event_recorder.models import TelemetryEvent, TelemetryIngestFailure
from event_recorder.repository import Database, EventCursor, EventFilters, EventRepository


class ApiModel(BaseModel):
    """Strict camelCase API base model."""

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
        serialize_by_alias=True,
    )


class EventItem(ApiModel):
    """Stored telemetry plus recorder metadata."""

    message: dict[str, Any]
    received_at: datetime = Field(alias="receivedAt")
    topic: str
    qos: int
    retain: bool


class EventPage(ApiModel):
    """One stable keyset page."""

    items: list[EventItem]
    next_cursor: str | None = Field(alias="nextCursor")


class IngestFailureItem(ApiModel):
    """Recent isolated payload for test diagnostics."""

    id: int
    topic: str
    received_at: datetime = Field(alias="receivedAt")
    raw_payload_base64: str = Field(alias="rawPayloadBase64")
    error_type: str = Field(alias="errorType")
    error_detail: str = Field(alias="errorDetail")
    original_payload_size: int = Field(alias="originalPayloadSize")
    payload_truncated: bool = Field(alias="payloadTruncated")
    payload_sha256: str = Field(alias="payloadSha256")


class IngestFailurePage(ApiModel):
    """Newest failure records."""

    items: list[IngestFailureItem]


class Repository(Protocol):
    """Persistence operations used by the HTTP layer."""

    async def ping(self) -> bool:
        """Return whether PostgreSQL is queryable."""

    async def list_events(
        self,
        filters: EventFilters,
        *,
        limit: int,
        cursor: EventCursor | None,
    ) -> Sequence[TelemetryEvent]:
        """Return event history."""

    async def get_event(self, message_id: str) -> TelemetryEvent | None:
        """Return one event."""

    async def list_failures(self, *, limit: int) -> Sequence[TelemetryIngestFailure]:
        """Return recent failures."""


class ConsumerHealth(Protocol):
    """Consumer lifecycle and readiness surface."""

    @property
    def connected(self) -> bool:
        """Return MQTT connection state."""

    @property
    def task_alive(self) -> bool:
        """Return consume-task state."""

    @property
    def db_write_healthy(self) -> bool:
        """Return latest write state."""

    async def start(self) -> None:
        """Start consuming."""

    async def stop(self) -> None:
        """Stop consuming."""


def encode_cursor(event: TelemetryEvent) -> str:
    """Encode stable source_time/id keys as an opaque URL-safe cursor."""

    raw = json.dumps(
        [event.source_time.isoformat(), event.id],
        separators=(",", ":"),
    ).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def decode_cursor(value: str) -> EventCursor:
    """Decode and strictly validate an opaque history cursor."""

    try:
        padded = value + "=" * (-len(value) % 4)
        decoded = json.loads(base64.urlsafe_b64decode(padded))
        if (
            not isinstance(decoded, list)
            or len(decoded) != 2
            or not isinstance(decoded[0], str)
            or not isinstance(decoded[1], int)
            or decoded[1] < 1
        ):
            raise ValueError("cursor payload has invalid shape")
        source_time = datetime.fromisoformat(decoded[0])
        if source_time.utcoffset() is None:
            raise ValueError("cursor sourceTime must include a UTC offset")
    except (ValueError, TypeError, json.JSONDecodeError) as exc:
        raise ValueError("cursor is invalid") from exc
    return EventCursor(source_time=source_time, event_id=decoded[1])


def event_item(record: TelemetryEvent) -> EventItem:
    """Return immutable stored JSON, including historical pre-2.0 events."""

    return EventItem(
        message=record.payload,
        received_at=record.received_at,
        topic=record.topic,
        qos=record.qos,
        retain=record.retain,
    )


def failure_item(record: TelemetryIngestFailure) -> IngestFailureItem:
    """Encode exact invalid bytes safely for JSON transport."""

    return IngestFailureItem(
        id=record.id,
        topic=record.topic,
        received_at=record.received_at,
        raw_payload_base64=base64.b64encode(record.payload).decode(),
        error_type=record.error_type,
        error_detail=record.error_detail,
        original_payload_size=record.original_payload_size,
        payload_truncated=record.payload_truncated,
        payload_sha256=record.payload_sha256,
    )


def create_app(
    settings: RecorderSettings,
    *,
    repository: Repository | None = None,
    consumer: ConsumerHealth | None = None,
    database: Database | None = None,
) -> FastAPI:
    """Create the recorder with injectable infrastructure for focused tests."""

    managed_database = database
    managed_repository = repository
    if managed_repository is None:
        managed_database = managed_database or Database(settings)
        managed_repository = EventRepository(
            managed_database.sessions,
            max_failure_payload_bytes=settings.max_failure_payload_bytes,
        )
    managed_consumer = consumer

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        if managed_consumer is not None:
            await managed_consumer.start()
        try:
            yield
        finally:
            if managed_consumer is not None:
                await managed_consumer.stop()
            if managed_database is not None:
                await managed_database.dispose()

    app = FastAPI(title="Event Recorder", version="0.1.0", lifespan=lifespan)
    if AuthSettings().enabled:
        managed_database = managed_database or Database(settings)
        install_identity(app, IdentityStore(managed_database.sessions))

    @app.get("/health/live")
    async def live() -> dict[str, str]:
        return {"status": "live"}

    @app.get("/health/ready")
    async def ready() -> dict[str, str]:
        database_ready = await managed_repository.ping()
        consumer_ready = managed_consumer is None or (
            managed_consumer.connected
            and managed_consumer.task_alive
            and managed_consumer.db_write_healthy
        )
        if not (database_ready and consumer_ready):
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="PostgreSQL history reader is not ready",
            )
        return {"status": "ready"}

    @app.get("/api/v1/telemetry/events", response_model=EventPage)
    async def list_events(
        garage_id: Annotated[str | None, Query(alias="garageId", min_length=1)] = None,
        sensor_id: Annotated[str | None, Query(alias="sensorId", min_length=1)] = None,
        device_id: Annotated[str | None, Query(alias="deviceId", min_length=1)] = None,
        sensor_type: Annotated[
            str | None,
            Query(alias="sensorType", min_length=1),
        ] = None,
        scenario_id: Annotated[
            str | None,
            Query(alias="scenarioId", min_length=1),
        ] = None,
        event_status: Annotated[
            TelemetryStatus | None,
            Query(alias="status"),
        ] = None,
        quality: DataQuality | None = None,
        source_time_from: Annotated[
            AwareDatetime | None,
            Query(alias="sourceTimeFrom"),
        ] = None,
        source_time_to: Annotated[
            AwareDatetime | None,
            Query(alias="sourceTimeTo"),
        ] = None,
        limit: Annotated[int, Query(ge=1, le=1000)] = 100,
        cursor: str | None = None,
    ) -> EventPage:
        if (
            source_time_from is not None
            and source_time_to is not None
            and source_time_from > source_time_to
        ):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="sourceTimeFrom must be less than or equal to sourceTimeTo",
            )
        try:
            decoded_cursor = decode_cursor(cursor) if cursor is not None else None
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=str(exc),
            ) from exc
        records = await managed_repository.list_events(
            EventFilters(
                sensor_id=sensor_id,
                garage_id=garage_id,
                device_id=device_id,
                sensor_type=sensor_type,
                scenario_id=scenario_id,
                status=event_status,
                quality=quality,
                source_time_from=source_time_from,
                source_time_to=source_time_to,
            ),
            limit=limit + 1,
            cursor=decoded_cursor,
        )
        has_more = len(records) > limit
        page_records = records[:limit]
        next_cursor = encode_cursor(page_records[-1]) if has_more else None
        return EventPage(
            items=[event_item(record) for record in page_records],
            next_cursor=next_cursor,
        )

    @app.get(
        "/api/v1/telemetry/events/{message_id}",
        response_model=EventItem,
    )
    async def get_event(message_id: str) -> EventItem:
        record = await managed_repository.get_event(message_id)
        if record is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="telemetry event not found",
            )
        return event_item(record)

    @app.get(
        "/api/v1/telemetry/ingest-failures",
        response_model=IngestFailurePage,
    )
    async def list_ingest_failures(
        limit: Annotated[int, Query(ge=1, le=100)] = 100,
    ) -> IngestFailurePage:
        if settings.environment != "test":
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
        records = await managed_repository.list_failures(limit=limit)
        return IngestFailurePage(items=[failure_item(record) for record in records])

    return app
