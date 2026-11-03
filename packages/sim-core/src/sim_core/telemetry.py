"""Pydantic models for the unified telemetry message contract."""

import math
from datetime import datetime
from enum import StrEnum
from typing import Annotated, Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    FiniteFloat,
    StrictBool,
    field_validator,
    model_validator,
)

from sim_core.environment import calculate_condensation_risk

NonEmptyString = Annotated[str, Field(min_length=1)]
MessageId = Annotated[
    str,
    Field(
        pattern=(
            r"^MSG-[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-"
            r"[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
        )
    ),
]


class TelemetryStatus(StrEnum):
    """Allowed telemetry states."""

    NORMAL = "normal"
    WARNING = "warning"
    CRITICAL = "critical"
    FAULT = "fault"
    OFFLINE = "offline"
    UNKNOWN = "unknown"


class DataQuality(StrEnum):
    """Allowed telemetry data-quality states."""

    VALID = "valid"
    INVALID = "invalid"
    MISSING = "missing"
    DELAYED = "delayed"
    DUPLICATED = "duplicated"
    OUT_OF_RANGE = "out_of_range"


class ActionStage(StrEnum):
    """Garage mechanism stage shared by every correlated telemetry message."""

    IDLE = "idle"
    STARTING = "starting"
    LIFTING = "lifting"
    SHIFTING = "shifting"
    ARRIVING = "arriving"
    STOPPED = "stopped"
    FAULT = "fault"


class ContractModel(BaseModel):
    """Strict base model shared by externally visible contracts."""

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
        str_strip_whitespace=True,
    )


class TelemetryEnvelope[PayloadT: BaseModel](ContractModel):
    """Unified envelope used by every simulator telemetry message."""

    schema_version: Literal["2.0"] = Field(alias="schemaVersion")
    message_id: MessageId = Field(alias="messageId")
    garage_id: NonEmptyString = Field(alias="garageId")
    area_id: NonEmptyString = Field(alias="areaId")
    device_id: NonEmptyString = Field(alias="deviceId")
    sensor_id: NonEmptyString = Field(alias="sensorId")
    point_code: NonEmptyString = Field(alias="pointCode")
    sensor_type: NonEmptyString = Field(alias="sensorType")
    source_time: datetime = Field(alias="sourceTime")
    seq: int = Field(ge=0, strict=True)
    status: TelemetryStatus
    quality: DataQuality
    action_stage: ActionStage = Field(alias="actionStage")
    source: Literal["simulator"]
    scenario_id: NonEmptyString = Field(alias="scenarioId")
    data: PayloadT

    @field_validator("source_time")
    @classmethod
    def source_time_must_have_utc_offset(cls, value: datetime) -> datetime:
        """Reject local/naive timestamps because replay ordering depends on offsets."""

        if value.utcoffset() is None:
            raise ValueError("sourceTime must include a UTC offset")
        return value


class VehiclePositionStatus(StrEnum):
    """Coarse position state suitable for simulated vision/laser correlation."""

    UNKNOWN = "unknown"
    CLEAR = "clear"
    ALIGNED = "aligned"
    MISALIGNED = "misaligned"


class LaserPresenceData(ContractModel):
    """Ranging payload retained under the historical Python class name."""

    vehicle_present: bool = Field(alias="vehiclePresent", strict=True)
    distance_m: FiniteFloat | None = Field(alias="distanceM", ge=0)
    displacement_m: FiniteFloat | None = Field(alias="displacementM")
    position_status: VehiclePositionStatus = Field(alias="positionStatus")


class LaserTelemetryMessage(TelemetryEnvelope[LaserPresenceData]):
    """Concrete unified envelope for laser distance and position telemetry."""

    point_code: Literal["VEHICLE_DISTANCE"] = Field(alias="pointCode")
    sensor_type: Literal["laser_distance"] = Field(alias="sensorType")

    @model_validator(mode="after")
    def missing_laser_values_must_match_quality(self) -> Self:
        missing = self.status is TelemetryStatus.OFFLINE or self.quality is DataQuality.MISSING
        values = (self.data.distance_m, self.data.displacement_m)
        if missing:
            if (
                self.status is not TelemetryStatus.OFFLINE
                or self.quality is not DataQuality.MISSING
            ):
                raise ValueError("offline status and missing quality must be paired")
            if any(value is not None for value in values):
                raise ValueError("offline laser measurements must be null")
        elif any(value is None for value in values):
            raise ValueError("online laser measurements must not be null")
        return self


class CurrentPhase(StrEnum):
    """Supported motor-current operating phases."""

    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    OVERLOAD = "overload"
    STALLED = "stalled"
    OFFLINE = "offline"


class CurrentTelemetryData(ContractModel):
    """One batched motor-current payload."""

    unit: Literal["A"] = "A"
    sample_rate_hz: int = Field(default=10, alias="sampleRateHz", gt=0, strict=True)
    sample_count: int = Field(alias="sampleCount", ge=0, strict=True)
    window_start_time: datetime = Field(alias="windowStartTime")
    window_end_time: datetime = Field(alias="windowEndTime")
    phase: CurrentPhase
    current_samples: list[FiniteFloat] = Field(alias="currentSamples")

    @model_validator(mode="after")
    def sampling_window_must_match_samples(self) -> Self:
        """Keep count, timestamps, and the uniformly sampled batch consistent."""

        if self.window_start_time.utcoffset() is None:
            raise ValueError("windowStartTime must include a UTC offset")
        if self.window_end_time.utcoffset() is None:
            raise ValueError("windowEndTime must include a UTC offset")
        if self.window_end_time < self.window_start_time:
            raise ValueError("windowEndTime must not precede windowStartTime")
        if len(self.current_samples) != self.sample_count:
            raise ValueError("currentSamples length must equal sampleCount")
        expected_seconds = self.sample_count / self.sample_rate_hz
        actual_seconds = (self.window_end_time - self.window_start_time).total_seconds()
        if not math.isclose(actual_seconds, expected_seconds, rel_tol=0, abs_tol=1e-6):
            raise ValueError(
                "sampling window duration must equal sampleCount divided by sampleRateHz"
            )
        return self


class CurrentTelemetryMessage(TelemetryEnvelope[CurrentTelemetryData]):
    """Concrete unified envelope for batched motor-current telemetry."""

    point_code: Literal["LIFT_MOTOR_CURRENT", "SHIFT_MOTOR_CURRENT"] = Field(alias="pointCode")
    sensor_type: Literal["current"] = Field(alias="sensorType")


NonNegativeFiniteFloat = Annotated[FiniteFloat, Field(ge=0)]


class VibrationSample(ContractModel):
    """One simultaneous three-axis acceleration sample."""

    x: FiniteFloat
    y: FiniteFloat
    z: FiniteFloat


class VibrationFeatures(ContractModel):
    """Features derived from all samples in one vibration batch."""

    x_rms: NonNegativeFiniteFloat = Field(alias="xRms")
    y_rms: NonNegativeFiniteFloat = Field(alias="yRms")
    z_rms: NonNegativeFiniteFloat = Field(alias="zRms")
    peak: NonNegativeFiniteFloat


class VibrationTelemetryData(ContractModel):
    """One batched three-axis vibration payload with verified features."""

    unit: Literal["m/s2"] = "m/s2"
    sample_rate_hz: int = Field(default=100, alias="sampleRateHz", gt=0, strict=True)
    sample_count: int = Field(default=100, alias="sampleCount", gt=0, strict=True)
    samples: list[VibrationSample]
    features: VibrationFeatures

    @model_validator(mode="after")
    def samples_and_features_must_be_consistent(self) -> Self:
        """Reject length mismatches and independently fabricated features."""

        if len(self.samples) != self.sample_count:
            raise ValueError("samples length must equal sampleCount")
        axes = {
            "xRms": [sample.x for sample in self.samples],
            "yRms": [sample.y for sample in self.samples],
            "zRms": [sample.z for sample in self.samples],
        }
        expected = {
            name: math.sqrt(sum(value * value for value in values) / self.sample_count)
            for name, values in axes.items()
        }
        expected["peak"] = max(
            abs(value) for sample in self.samples for value in (sample.x, sample.y, sample.z)
        )
        actual = {
            "xRms": self.features.x_rms,
            "yRms": self.features.y_rms,
            "zRms": self.features.z_rms,
            "peak": self.features.peak,
        }
        for name, expected_value in expected.items():
            if not math.isclose(
                actual[name],
                expected_value,
                rel_tol=1e-9,
                abs_tol=1e-12,
            ):
                raise ValueError(f"features.{name} must be calculated from samples")
        return self


class VibrationTelemetryMessage(TelemetryEnvelope[VibrationTelemetryData]):
    """Concrete unified envelope for batched three-axis vibration telemetry."""

    point_code: Literal["LIFT_MECHANISM_VIBRATION"] = Field(alias="pointCode")
    sensor_type: Literal["vibration"] = Field(alias="sensorType")


class VisionEventData(ContractModel):
    """Structured recognition result; no real computer-vision inference is performed."""

    camera_id: NonEmptyString = Field(alias="cameraId")
    person_detected: StrictBool = Field(alias="personDetected")
    vehicle_detected: StrictBool = Field(alias="vehicleDetected")
    vehicle_position: VehiclePositionStatus = Field(alias="vehiclePosition")
    confidence: FiniteFloat = Field(ge=0, le=1)


class VisionTelemetryMessage(TelemetryEnvelope[VisionEventData]):
    """MQTT metadata derived synthetically alongside an independent RTSP stream."""

    point_code: Literal["VISION_SAFETY_STATE"] = Field(alias="pointCode")
    sensor_type: Literal["vision_event"] = Field(alias="sensorType")


class MechanismStateData(ContractModel):
    """Optional PLC/switch/load observations used to correlate an action cycle."""

    plc_state: Literal["idle", "running", "fault", "unavailable"] = Field(alias="plcState")
    at_position: StrictBool | None = Field(alias="atPosition")
    lift_limit_active: StrictBool | None = Field(alias="liftLimitActive")
    shift_limit_active: StrictBool | None = Field(alias="shiftLimitActive")
    interlock_closed: StrictBool | None = Field(alias="interlockClosed")
    emergency_stop: StrictBool | None = Field(alias="emergencyStop")
    load_kg: FiniteFloat | None = Field(alias="loadKg", ge=0)


class MechanismStateTelemetryMessage(TelemetryEnvelope[MechanismStateData]):
    """Configured auxiliary mechanism/PLC state telemetry."""

    point_code: Literal["MECHANISM_STATE"] = Field(alias="pointCode")
    sensor_type: Literal["mechanism_state"] = Field(alias="sensorType")


class EnvironmentTelemetryData(ContractModel):
    """Temperature, humidity, pressure, altitude, and derived condensation result."""

    temperature: FiniteFloat | None
    temperature_unit: Literal["C"] = Field(alias="temperatureUnit")
    humidity: FiniteFloat | None
    humidity_unit: Literal["%RH"] = Field(alias="humidityUnit")
    pressure_kpa: FiniteFloat | None = Field(alias="pressureKPa", gt=0)
    altitude_meters: FiniteFloat | None = Field(alias="altitudeMeters", ge=0)
    condensation_risk: StrictBool | None = Field(alias="condensationRisk")


class EnvironmentTelemetryMessage(TelemetryEnvelope[EnvironmentTelemetryData]):
    """Concrete unified envelope for temperature and humidity telemetry."""

    point_code: Literal["GARAGE_TEMP_HUMIDITY"] = Field(alias="pointCode")
    sensor_type: Literal["environment"] = Field(alias="sensorType")

    @model_validator(mode="after")
    def nullability_range_and_risk_must_match_envelope(self) -> Self:
        """Enforce offline nulls and recalculate condensation from measurements."""

        is_offline = self.status is TelemetryStatus.OFFLINE
        is_missing = self.quality is DataQuality.MISSING
        values = (
            self.data.temperature,
            self.data.humidity,
            self.data.pressure_kpa,
            self.data.altitude_meters,
            self.data.condensation_risk,
        )
        if is_offline or is_missing:
            if not (is_offline and is_missing):
                raise ValueError("offline status and missing quality must be paired")
            if any(value is not None for value in values):
                raise ValueError("offline/missing environment data must contain null values")
            return self
        if any(value is None for value in values):
            raise ValueError("online environment data must not contain null values")

        temperature = self.data.temperature
        humidity = self.data.humidity
        risk = self.data.condensation_risk
        if temperature is None or humidity is None or risk is None:
            raise AssertionError("validated online values unexpectedly contain null")
        if self.scenario_id != "ENVIRONMENT_OUT_OF_RANGE" and not 0 <= humidity <= 100:
            raise ValueError("humidity must be between 0 and 100")
        expected = calculate_condensation_risk(temperature, humidity)
        if risk is not expected:
            raise ValueError("condensationRisk must be calculated from temperature and humidity")
        return self
