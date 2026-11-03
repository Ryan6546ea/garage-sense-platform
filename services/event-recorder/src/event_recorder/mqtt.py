"""Persistent-session MQTT consumer with capped reconnect backoff."""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

import aiomqtt
from sim_core import TelemetryMessage

from event_recorder.config import RecorderSettings
from event_recorder.validation import ContractValidator, JsonObject, TelemetryValidationError

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class InboundMessage:
    """Transport-neutral MQTT message passed to ingestion."""

    topic: str
    payload: bytes
    qos: int
    retain: bool


class MqttSession(Protocol):
    """Subset of aiomqtt used by the reconnect loop."""

    @property
    def messages(self) -> aiomqtt.MessagesIterator:
        """Yield inbound messages."""

    async def subscribe(self, topic: str, qos: int = 0) -> object:
        """Subscribe to a topic filter."""


class SessionFactory(Protocol):
    """Create one reconnectable MQTT context manager."""

    def __call__(self) -> AbstractAsyncContextManager[MqttSession]:
        """Return a disconnected MQTT session."""


class IngestRepository(Protocol):
    """Append-only writes used by the consumer."""

    async def insert_event(
        self,
        *,
        topic: str,
        qos: int,
        retain: bool,
        message: TelemetryMessage,
        payload: JsonObject,
        received_at: datetime,
    ) -> bool:
        """Insert a valid event once."""

    async def insert_failure(
        self,
        *,
        topic: str,
        payload: bytes,
        received_at: datetime,
        error_type: str,
        error_detail: str,
    ) -> None:
        """Insert an invalid payload."""


class MqttConsumer:
    """Consume telemetry forever while isolating individual message failures."""

    def __init__(
        self,
        settings: RecorderSettings,
        repository: IngestRepository,
        validator: ContractValidator,
        *,
        session_factory: SessionFactory | None = None,
        retry_wait: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        self._settings = settings
        self._repository = repository
        self._validator = validator
        self._session_factory = session_factory or self._new_session
        self._retry_wait = retry_wait or self._wait_for_stop
        self._connected = asyncio.Event()
        self._stop_requested = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._db_write_healthy = True
        self._duplicate_count = 0

    @property
    def connected(self) -> bool:
        """Return whether the persistent MQTT session is connected."""

        return self._connected.is_set()

    @property
    def task_alive(self) -> bool:
        """Return whether the background consume loop is alive."""

        return self._task is not None and not self._task.done()

    @property
    def db_write_healthy(self) -> bool:
        """Return whether the latest attempted database write succeeded."""

        return self._db_write_healthy

    @property
    def duplicate_count(self) -> int:
        """Return the number of idempotently ignored deliveries."""

        return self._duplicate_count

    async def start(self) -> None:
        """Start the consume loop once."""

        if self._task is None:
            self._task = asyncio.create_task(
                self._run(),
                name="event-recorder-mqtt-consumer",
            )

    async def stop(self) -> None:
        """Cancel the consumer and leave the broker session persistent."""

        self._stop_requested.set()
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None
        self._connected.clear()

    async def ingest(self, inbound: InboundMessage) -> None:
        """Validate and persist one delivery without raising for bad payloads."""

        received_at = datetime.now(UTC)
        try:
            validated = self._validator.validate(inbound.payload, inbound.topic)
        except TelemetryValidationError as exc:
            try:
                await self._repository.insert_failure(
                    topic=inbound.topic,
                    payload=inbound.payload,
                    received_at=received_at,
                    error_type=exc.error_type,
                    error_detail=exc.detail,
                )
            except Exception as db_exc:
                self._db_write_healthy = False
                LOGGER.error(
                    "ingest failure persistence failed",
                    extra={"exception_type": type(db_exc).__name__},
                )
            else:
                self._db_write_healthy = True
            return

        try:
            inserted = await self._repository.insert_event(
                topic=inbound.topic,
                qos=inbound.qos,
                retain=inbound.retain,
                message=validated.message,
                payload=validated.payload,
                received_at=received_at,
            )
        except Exception as exc:
            self._db_write_healthy = False
            LOGGER.error(
                "telemetry event persistence failed",
                extra={"exception_type": type(exc).__name__},
            )
            return

        self._db_write_healthy = True
        if not inserted:
            self._duplicate_count += 1
            LOGGER.info(
                "duplicate telemetry ignored",
                extra={
                    "message_id": validated.message.message_id,
                    "duplicate_count": self._duplicate_count,
                },
            )

    def _new_session(self) -> AbstractAsyncContextManager[MqttSession]:
        return aiomqtt.Client(
            hostname=self._settings.mqtt_host,
            port=self._settings.mqtt_port,
            username=self._settings.mqtt_username,
            password=self._settings.mqtt_password,
            identifier=self._settings.mqtt_client_id,
            clean_session=False,
            keepalive=self._settings.mqtt_keepalive_seconds,
        )

    async def _run(self) -> None:
        delay = self._settings.mqtt_reconnect_initial_seconds
        while not self._stop_requested.is_set():
            try:
                async with self._session_factory() as client:
                    await client.subscribe(self._settings.mqtt_topic, qos=1)
                    self._connected.set()
                    delay = self._settings.mqtt_reconnect_initial_seconds
                    LOGGER.info(
                        "connected persistent MQTT consumer",
                        extra={
                            "client_id": self._settings.mqtt_client_id,
                            "topic": self._settings.mqtt_topic,
                        },
                    )
                    async for message in client.messages:
                        if self._stop_requested.is_set():
                            break
                        try:
                            inbound = InboundMessage(
                                topic=str(message.topic),
                                payload=bytes(message.payload),
                                qos=int(message.qos),
                                retain=bool(message.retain),
                            )
                            await self.ingest(inbound)
                        except Exception as exc:
                            LOGGER.error(
                                "single MQTT message handling failed",
                                extra={"exception_type": type(exc).__name__},
                            )
            except asyncio.CancelledError:
                raise
            except aiomqtt.MqttError as exc:
                LOGGER.warning(
                    "MQTT consumer connection failed",
                    extra={"exception_type": type(exc).__name__},
                )
            finally:
                self._connected.clear()

            if self._stop_requested.is_set():
                break
            LOGGER.info("retrying MQTT consumer connection in %.2f seconds", delay)
            await self._retry_wait(delay)
            delay = min(delay * 2, self._settings.mqtt_reconnect_max_seconds)

    async def _wait_for_stop(self, delay: float) -> None:
        try:
            await asyncio.wait_for(self._stop_requested.wait(), timeout=delay)
        except TimeoutError:
            return
