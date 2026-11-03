"""Independent per-sensor MQTT subscribers, bounded queues, and workers."""

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import UTC, datetime
from time import monotonic
from typing import Protocol, cast

import aiomqtt
from jsonschema.exceptions import ValidationError as JsonSchemaValidationError
from pydantic import ValidationError as PydanticValidationError
from sim_core import (
    DeviceSemanticError,
    DeviceSemanticRegistry,
    TelemetryContractRegistry,
    TelemetryMessage,
    UnsupportedSensorTypeError,
)

from telemetry_consumer.config import ConsumerSettings
from telemetry_consumer.metrics import ConsumerMetrics
from telemetry_consumer.repository import JsonObject

LOGGER = logging.getLogger(__name__)
SENSOR_TYPES = (
    "laser_distance",
    "current",
    "vibration",
    "environment",
    "vision_event",
    "mechanism_state",
)
TOPIC_SEGMENT_BY_SENSOR_TYPE = {
    "laser_distance": "laser",
    "current": "current",
    "vibration": "vibration",
    "environment": "environment",
    "vision_event": "vision",
    "mechanism_state": "state",
}
SENSOR_TYPE_BY_TOPIC_SEGMENT = {
    topic_segment: sensor_type
    for sensor_type, topic_segment in TOPIC_SEGMENT_BY_SENSOR_TYPE.items()
}


@dataclass(frozen=True, slots=True)
class InboundMessage:
    """One MQTT delivery waiting for a sensor-specific worker."""

    topic: str
    payload: bytes
    qos: int
    retain: bool
    enqueued_at: float


class MqttSession(Protocol):
    @property
    def messages(self) -> aiomqtt.MessagesIterator: ...

    async def subscribe(self, topic: str, qos: int = 0) -> object: ...


class SessionFactory(Protocol):
    def __call__(self, sensor_type: str) -> AbstractAsyncContextManager[MqttSession]: ...


class TelemetryStore(Protocol):
    async def insert_message(
        self,
        *,
        topic: str,
        qos: int,
        retain: bool,
        message: TelemetryMessage,
        payload: JsonObject,
        received_at: datetime,
    ) -> bool: ...

    async def insert_failure(
        self,
        *,
        topic: str,
        payload: bytes,
        received_at: datetime,
        error_type: str,
        error_detail: str,
    ) -> None: ...


class TelemetryConsumer:
    """Dispatch all subscribed topics into isolated sensor-type workers."""

    def __init__(
        self,
        settings: ConsumerSettings,
        repository: TelemetryStore,
        contracts: TelemetryContractRegistry,
        semantics: DeviceSemanticRegistry,
        *,
        session_factory: SessionFactory | None = None,
        retry_wait: Callable[[float], Awaitable[None]] | None = None,
        clock: Callable[[], float] = monotonic,
        metrics: ConsumerMetrics | None = None,
    ) -> None:
        self._settings = settings
        self._repository = repository
        self._contracts = contracts
        self._semantics = semantics
        self._session_factory = session_factory or self._new_session
        self._retry_wait = retry_wait or self._wait_for_stop
        self._clock = clock
        self.metrics = metrics or ConsumerMetrics()
        self._queues = {
            sensor_type: asyncio.Queue[InboundMessage](maxsize=settings.queue_capacity)
            for sensor_type in SENSOR_TYPES
        }
        self._connected: set[str] = set()
        self._stop_requested = asyncio.Event()
        self._mqtt_tasks: dict[str, asyncio.Task[None]] = {}
        self._worker_tasks: dict[str, asyncio.Task[None]] = {}
        self._unhealthy_writers: set[str] = set()
        for sensor_type in SENSOR_TYPES:
            self.metrics.queue_length.labels(sensor_type=sensor_type).set(0)

    @property
    def connected(self) -> bool:
        return self._connected == set(SENSOR_TYPES)

    @property
    def task_alive(self) -> bool:
        return (
            len(self._mqtt_tasks) == len(SENSOR_TYPES)
            and all(not task.done() for task in self._mqtt_tasks.values())
            and len(self._worker_tasks) == len(SENSOR_TYPES)
            and all(not task.done() for task in self._worker_tasks.values())
        )

    @property
    def db_write_healthy(self) -> bool:
        return not self._unhealthy_writers

    @property
    def subscription_topics(self) -> tuple[str, ...]:
        """Return one explicit topic filter for each supported sensor type."""

        prefix = self._settings.mqtt_topic_prefix
        return tuple(
            f"{prefix}/{TOPIC_SEGMENT_BY_SENSOR_TYPE[sensor_type]}/+"
            for sensor_type in SENSOR_TYPES
        )

    def queue_size(self, sensor_type: str) -> int:
        """Expose one queue depth for readiness diagnostics and tests."""

        return self._queues[sensor_type].qsize()

    async def start(self) -> None:
        if self._mqtt_tasks:
            return
        self._stop_requested.clear()
        self._worker_tasks = {
            sensor_type: asyncio.create_task(
                self._worker(sensor_type),
                name=f"telemetry-consumer-{sensor_type}-worker",
            )
            for sensor_type in SENSOR_TYPES
        }
        self._mqtt_tasks = {
            sensor_type: asyncio.create_task(
                self._connection_loop(sensor_type),
                name=f"telemetry-consumer-{sensor_type}-mqtt",
            )
            for sensor_type in SENSOR_TYPES
        }

    async def stop(self) -> None:
        self._stop_requested.set()
        for task in self._mqtt_tasks.values():
            task.cancel()
        await asyncio.gather(*self._mqtt_tasks.values(), return_exceptions=True)
        self._mqtt_tasks.clear()
        try:
            async with asyncio.timeout(self._settings.shutdown_drain_timeout_seconds):
                await asyncio.gather(*(queue.join() for queue in self._queues.values()))
        except TimeoutError:
            LOGGER.warning("timed out draining telemetry worker queues")
        for task in self._worker_tasks.values():
            task.cancel()
        await asyncio.gather(*self._worker_tasks.values(), return_exceptions=True)
        self._worker_tasks.clear()
        self._connected.clear()

    async def enqueue(
        self,
        *,
        topic: str,
        payload: bytes,
        qos: int = 1,
        retain: bool = False,
    ) -> bool:
        """Route one MQTT delivery without letting a full type queue block the others."""

        sensor_type = self._sensor_type_from_topic(topic)
        if sensor_type is None:
            self.metrics.failures.labels(sensor_type="unknown", reason="invalid_topic").inc()
            LOGGER.warning("telemetry message has an unsupported topic", extra={"topic": topic})
            return False
        self.metrics.received.labels(sensor_type=sensor_type).inc()
        queue = self._queues[sensor_type]
        try:
            queue.put_nowait(
                InboundMessage(
                    topic=topic,
                    payload=payload,
                    qos=qos,
                    retain=retain,
                    enqueued_at=self._clock(),
                )
            )
        except asyncio.QueueFull:
            self.metrics.failures.labels(sensor_type=sensor_type, reason="queue_full").inc()
            LOGGER.error(
                "sensor telemetry queue is full; message rejected",
                extra={"sensor_type": sensor_type, "topic": topic},
            )
            return False
        self.metrics.queue_length.labels(sensor_type=sensor_type).set(queue.qsize())
        return True

    async def _worker(self, sensor_type: str) -> None:
        queue = self._queues[sensor_type]
        while True:
            inbound = await queue.get()
            self.metrics.queue_length.labels(sensor_type=sensor_type).set(queue.qsize())
            self.metrics.queue_wait_seconds.labels(sensor_type=sensor_type).observe(
                max(0.0, self._clock() - inbound.enqueued_at)
            )
            started_at = self._clock()
            try:
                async with asyncio.timeout(self._settings.processing_timeout_seconds):
                    await self._process(sensor_type, inbound)
            except TimeoutError:
                self._unhealthy_writers.add(sensor_type)
                self.metrics.failures.labels(
                    sensor_type=sensor_type,
                    reason="processing_timeout",
                ).inc()
                LOGGER.error(
                    "telemetry message processing timed out",
                    extra={"sensor_type": sensor_type, "topic": inbound.topic},
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._unhealthy_writers.add(sensor_type)
                self.metrics.failures.labels(sensor_type=sensor_type, reason="internal_error").inc()
                LOGGER.exception(
                    "unexpected telemetry worker failure",
                    extra={"sensor_type": sensor_type, "exception_type": type(exc).__name__},
                )
            finally:
                self.metrics.processing_seconds.labels(sensor_type=sensor_type).observe(
                    max(0.0, self._clock() - started_at)
                )
                queue.task_done()

    async def _process(self, sensor_type: str, inbound: InboundMessage) -> None:
        try:
            if len(inbound.payload) > self._settings.max_message_bytes:
                raise ClassifiedMessageError(
                    "message_too_large",
                    f"payload is {len(inbound.payload)} bytes; maximum is "
                    f"{self._settings.max_message_bytes}",
                )
            try:
                value = json.loads(inbound.payload)
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                raise ClassifiedMessageError("invalid_json", str(exc)) from exc
            if not isinstance(value, dict):
                raise ClassifiedMessageError(
                    "pydantic_validation", "payload must be a JSON object"
                )
            payload = cast(JsonObject, value)
            try:
                message = self._contracts.validate(payload)
            except UnsupportedSensorTypeError as exc:
                raise ClassifiedMessageError("unsupported_sensor_type", str(exc)) from exc
            except PydanticValidationError as exc:
                raise ClassifiedMessageError("pydantic_validation", str(exc)) from exc
            except JsonSchemaValidationError as exc:
                raise ClassifiedMessageError("json_schema_validation", exc.message) from exc
            try:
                self._semantics.validate_topic_payload(inbound.topic, message)
            except DeviceSemanticError as exc:
                raise ClassifiedMessageError(exc.error_type, exc.detail) from exc
            if message.sensor_type != sensor_type:
                raise ClassifiedMessageError(
                    "topic_payload_mismatch",
                    "worker sensor type does not match payload",
                )
        except Exception as exc:
            error_type = (
                exc.error_type if isinstance(exc, ClassifiedMessageError) else "invalid_message"
            )
            self.metrics.failures.labels(
                sensor_type=sensor_type,
                reason=error_type,
            ).inc()
            LOGGER.warning(
                "invalid telemetry message rejected",
                extra={
                    "sensor_type": sensor_type,
                    "topic": inbound.topic,
                    "exception_type": type(exc).__name__,
                },
            )
            try:
                await self._repository.insert_failure(
                    topic=inbound.topic,
                    payload=inbound.payload,
                    received_at=datetime.now(UTC),
                    error_type=error_type,
                    error_detail=str(exc),
                )
            except Exception as failure_exc:
                self._unhealthy_writers.add(sensor_type)
                self.metrics.failures.labels(
                    sensor_type=sensor_type,
                    reason="failure_persistence_error",
                ).inc()
                LOGGER.error(
                    "telemetry failure persistence failed",
                    extra={"exception_type": type(failure_exc).__name__},
                )
            else:
                self._unhealthy_writers.discard(sensor_type)
            return

        try:
            inserted = await self._repository.insert_message(
                topic=inbound.topic,
                qos=inbound.qos,
                retain=inbound.retain,
                message=message,
                payload=payload,
                received_at=datetime.now(UTC),
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._unhealthy_writers.add(sensor_type)
            self.metrics.failures.labels(sensor_type=sensor_type, reason="database_error").inc()
            LOGGER.error(
                "typed telemetry persistence failed",
                extra={
                    "sensor_type": sensor_type,
                    "exception_type": type(exc).__name__,
                },
            )
            return

        self._unhealthy_writers.discard(sensor_type)
        metric = self.metrics.persisted if inserted else self.metrics.duplicates
        metric.labels(sensor_type=sensor_type).inc()

    def _new_session(self, sensor_type: str) -> AbstractAsyncContextManager[MqttSession]:
        return aiomqtt.Client(
            hostname=self._settings.mqtt_host,
            port=self._settings.mqtt_port,
            username=self._settings.mqtt_username,
            password=self._settings.mqtt_password,
            identifier=f"{self._settings.mqtt_client_id}-{sensor_type}",
            clean_session=False,
            keepalive=self._settings.mqtt_keepalive_seconds,
        )

    async def _connection_loop(self, sensor_type: str) -> None:
        topic = (
            f"{self._settings.mqtt_topic_prefix}/"
            f"{TOPIC_SEGMENT_BY_SENSOR_TYPE[sensor_type]}/+"
        )
        delay = self._settings.mqtt_reconnect_initial_seconds
        while not self._stop_requested.is_set():
            try:
                async with self._session_factory(sensor_type) as client:
                    await client.subscribe(topic, qos=1)
                    self._connected.add(sensor_type)
                    delay = self._settings.mqtt_reconnect_initial_seconds
                    async for mqtt_message in client.messages:
                        if self._stop_requested.is_set():
                            break
                        await self.enqueue(
                            topic=str(mqtt_message.topic),
                            payload=bytes(mqtt_message.payload),
                            qos=int(mqtt_message.qos),
                            retain=bool(mqtt_message.retain),
                        )
            except asyncio.CancelledError:
                raise
            except aiomqtt.MqttError as exc:
                LOGGER.warning(
                    "telemetry consumer MQTT connection failed",
                    extra={"sensor_type": sensor_type, "exception_type": type(exc).__name__},
                )
            finally:
                self._connected.discard(sensor_type)

            if self._stop_requested.is_set():
                break
            await self._retry_wait(delay)
            delay = min(delay * 2, self._settings.mqtt_reconnect_max_seconds)

    def _sensor_type_from_topic(self, topic: str) -> str | None:
        parts = topic.split("/")
        if (
            len(parts) != 5
            or parts[0] != "garage"
            or not parts[1]
            or parts[2] != "telemetry"
            or parts[3] not in SENSOR_TYPE_BY_TOPIC_SEGMENT
            or not parts[4]
        ):
            return None
        return SENSOR_TYPE_BY_TOPIC_SEGMENT[parts[3]]

    async def _wait_for_stop(self, delay: float) -> None:
        try:
            await asyncio.wait_for(self._stop_requested.wait(), timeout=delay)
        except TimeoutError:
            return


class ClassifiedMessageError(ValueError):
    """Invalid telemetry with a stable diagnostics classification."""

    def __init__(self, error_type: str, detail: str) -> None:
        super().__init__(detail)
        self.error_type = error_type
