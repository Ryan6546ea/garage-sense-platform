"""PostgreSQL persistence for typed telemetry events and invalid deliveries."""

import hashlib
from datetime import datetime

from sim_core import (
    CurrentTelemetryMessage,
    EnvironmentTelemetryMessage,
    LaserTelemetryMessage,
    MechanismStateTelemetryMessage,
    TelemetryMessage,
    VibrationTelemetryMessage,
    VisionTelemetryMessage,
)
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.sql.dml import ReturningInsert

from telemetry_consumer.config import ConsumerSettings
from telemetry_consumer.models import (
    CurrentTelemetryEvent,
    EnvironmentTelemetryEvent,
    LaserDistanceTelemetryEvent,
    MechanismStateTelemetryEvent,
    TelemetryEventBase,
    TelemetryEventIdentity,
    TelemetryIngestFailure,
    VibrationTelemetryEvent,
    VisionEventTelemetryEvent,
)

type JsonObject = dict[str, object]


class Database:
    """Own the telemetry consumer database pool."""

    def __init__(self, settings: ConsumerSettings) -> None:
        self.engine: AsyncEngine = create_async_engine(settings.database_url, pool_pre_ping=True)
        self.sessions = async_sessionmaker(
            self.engine,
            expire_on_commit=False,
            class_=AsyncSession,
        )

    async def dispose(self) -> None:
        await self.engine.dispose()


class TelemetryRepository:
    """Persist each sensor type into its dedicated append-only table."""

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        *,
        max_failure_payload_bytes: int = 65_536,
    ) -> None:
        self._sessions = sessions
        self._max_failure_payload_bytes = max_failure_payload_bytes

    async def ping(self) -> bool:
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
        """Persist a bounded invalid payload with its full-content hash."""

        stored_payload = payload[: self._max_failure_payload_bytes]
        async with self._sessions.begin() as session:
            session.add(
                TelemetryIngestFailure(
                    topic=topic,
                    received_at=received_at,
                    payload=stored_payload,
                    error_type=error_type,
                    error_detail=error_detail,
                    original_payload_size=len(payload),
                    payload_truncated=len(stored_payload) != len(payload),
                    payload_sha256=hashlib.sha256(payload).hexdigest(),
                )
            )

    async def insert_message(
        self,
        *,
        topic: str,
        qos: int,
        retain: bool,
        message: TelemetryMessage,
        payload: JsonObject,
        received_at: datetime,
    ) -> bool:
        """Insert a typed telemetry row; return false for duplicate messageId."""

        identity_statement = (
            insert(TelemetryEventIdentity)
            .values(
                message_id=message.message_id,
                sensor_type=message.sensor_type,
                received_at=received_at,
            )
            .on_conflict_do_nothing(index_elements=[TelemetryEventIdentity.message_id])
            .returning(TelemetryEventIdentity.id)
        )
        async with self._sessions.begin() as session:
            identity_id = (await session.execute(identity_statement)).scalar_one_or_none()
            if identity_id is None:
                return False
            await session.execute(
                self._telemetry_insert(
                    identity_id,
                    topic,
                    qos,
                    retain,
                    message,
                    payload,
                    received_at,
                )
            )
        return True

    @staticmethod
    def _telemetry_insert(
        identity_id: int,
        topic: str,
        qos: int,
        retain: bool,
        message: TelemetryMessage,
        payload: JsonObject,
        received_at: datetime,
    ) -> ReturningInsert[tuple[int]]:
        common = {
            "id": identity_id,
            "message_id": message.message_id,
            "topic": topic,
            "qos": qos,
            "retain": retain,
            "garage_id": message.garage_id,
            "area_id": message.area_id,
            "device_id": message.device_id,
            "sensor_id": message.sensor_id,
            "point_code": message.point_code,
            "source_time": message.source_time,
            "received_at": received_at,
            "seq": message.seq,
            "status": message.status.value,
            "quality": message.quality.value,
            "action_stage": message.action_stage.value,
            "scenario_id": message.scenario_id,
            "payload": payload,
        }
        model: type[TelemetryEventBase]
        specific: dict[str, object]
        if isinstance(message, LaserTelemetryMessage):
            model = LaserDistanceTelemetryEvent
            specific = {
                "vehicle_present": message.data.vehicle_present,
                "distance_m": message.data.distance_m,
                "displacement_m": message.data.displacement_m,
                "position_status": message.data.position_status.value,
            }
        elif isinstance(message, CurrentTelemetryMessage):
            model = CurrentTelemetryEvent
            specific = {
                "unit": message.data.unit,
                "phase": message.data.phase.value,
                "sample_rate_hz": message.data.sample_rate_hz,
                "sample_count": message.data.sample_count,
                "window_start_time": message.data.window_start_time,
                "window_end_time": message.data.window_end_time,
                "current_samples": message.data.current_samples,
            }
        elif isinstance(message, VibrationTelemetryMessage):
            model = VibrationTelemetryEvent
            specific = {
                "unit": message.data.unit,
                "sample_rate_hz": message.data.sample_rate_hz,
                "sample_count": message.data.sample_count,
                "samples": [sample.model_dump(mode="json") for sample in message.data.samples],
                "features": message.data.features.model_dump(mode="json", by_alias=True),
            }
        elif isinstance(message, EnvironmentTelemetryMessage):
            model = EnvironmentTelemetryEvent
            specific = {
                "temperature": message.data.temperature,
                "temperature_unit": message.data.temperature_unit,
                "humidity": message.data.humidity,
                "humidity_unit": message.data.humidity_unit,
                "pressure_kpa": message.data.pressure_kpa,
                "altitude_meters": message.data.altitude_meters,
                "condensation_risk": message.data.condensation_risk,
            }
        elif isinstance(message, VisionTelemetryMessage):
            model = VisionEventTelemetryEvent
            specific = {
                "camera_id": message.data.camera_id,
                "person_detected": message.data.person_detected,
                "vehicle_detected": message.data.vehicle_detected,
                "vehicle_position": message.data.vehicle_position.value,
                "confidence": message.data.confidence,
            }
        elif isinstance(message, MechanismStateTelemetryMessage):
            model = MechanismStateTelemetryEvent
            specific = {
                "plc_state": message.data.plc_state,
                "at_position": message.data.at_position,
                "lift_limit_active": message.data.lift_limit_active,
                "shift_limit_active": message.data.shift_limit_active,
                "interlock_closed": message.data.interlock_closed,
                "emergency_stop": message.data.emergency_stop,
                "load_kg": message.data.load_kg,
            }
        else:
            raise TypeError(f"unsupported telemetry model: {type(message).__name__}")

        return insert(model).values(**common, **specific).returning(model.id)
