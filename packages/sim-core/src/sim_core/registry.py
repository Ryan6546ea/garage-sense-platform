"""Process-start contract registry shared by telemetry producers and consumers."""

import contextlib
import json
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import cast

from jsonschema import Draft202012Validator, FormatChecker
from pydantic import ValidationError as PydanticValidationError

from sim_core.telemetry import (
    CurrentTelemetryMessage,
    EnvironmentTelemetryMessage,
    LaserTelemetryMessage,
    MechanismStateTelemetryMessage,
    VibrationTelemetryMessage,
    VisionTelemetryMessage,
)

type JsonObject = dict[str, object]
type TelemetryMessage = (
    LaserTelemetryMessage
    | CurrentTelemetryMessage
    | VibrationTelemetryMessage
    | EnvironmentTelemetryMessage
    | VisionTelemetryMessage
    | MechanismStateTelemetryMessage
)
type TelemetryModel = (
    type[LaserTelemetryMessage]
    | type[CurrentTelemetryMessage]
    | type[VibrationTelemetryMessage]
    | type[EnvironmentTelemetryMessage]
    | type[VisionTelemetryMessage]
    | type[MechanismStateTelemetryMessage]
)


class UnsupportedSensorTypeError(ValueError):
    """Raised when no registered contract exists for a sensor type."""


class DeviceSemanticError(ValueError):
    """Stable classified device/topic semantic validation error."""

    def __init__(self, error_type: str, detail: str) -> None:
        super().__init__(detail)
        self.error_type = error_type
        self.detail = detail


@dataclass(frozen=True, slots=True)
class DeviceDefinition:
    """Fixed cross-field and MQTT mapping for one supported device."""

    garage_id: str
    area_id: str
    device_id: str
    sensor_id: str
    sensor_type: str
    point_code: str
    topic_type: str
    topic: str
    scenario_prefix: str
    recovery_scenario: str
    allowed_scenarios: frozenset[str]
    control_id: str | None = None

    @property
    def target_id(self) -> str:
        return self.control_id or self.device_id


LASER_SCENARIOS = frozenset(
    {
        "LASER_NO_VEHICLE",
        "LASER_VEHICLE_PRESENT",
        "LASER_VEHICLE_MISALIGNED",
        "LASER_INVALID",
        "LASER_DELAYED",
        "LASER_OFFLINE",
        "LASER_NORMAL",
    }
)
ENVIRONMENT_SCENARIOS = frozenset(
    f"ENVIRONMENT_{name}"
    for name in (
        "NORMAL",
        "HIGH_TEMPERATURE",
        "LOW_TEMPERATURE",
        "HIGH_HUMIDITY",
        "LOW_HUMIDITY",
        "LOW_PRESSURE",
        "PRESSURE_VARIATION",
        "CONDENSATION_RISK",
        "OUT_OF_RANGE",
        "INVALID",
        "DELAYED",
        "OFFLINE",
        "RECOVER",
    )
)
CURRENT_SUFFIXES = (
    "STOPPED",
    "STARTING",
    "RUNNING",
    "OVERLOAD",
    "STALLED",
    "OUT_OF_RANGE",
    "DELAYED",
    "OFFLINE",
    "NORMAL",
)
VIBRATION_SCENARIOS = frozenset(
    f"VIBRATION_{name}"
    for name in (
        "NORMAL",
        "ELEVATED",
        "IMPULSE",
        "OUT_OF_RANGE",
        "DELAYED",
        "OFFLINE",
        "RECOVER",
    )
)
VISION_SCENARIOS = frozenset(
    f"VISION_{name}"
    for name in (
        "NORMAL",
        "PERSON_INTRUSION",
        "VEHICLE_MISALIGNED",
        "INVALID",
        "DELAYED",
        "OFFLINE",
        "RECOVER",
    )
)
MECHANISM_SCENARIOS = frozenset(
    f"MECHANISM_{name}"
    for name in (
        "NORMAL",
        "CYCLE",
        "INTERLOCK_FAILURE",
        "LIMIT_FAILURE",
        "EMERGENCY_STOP",
        "DATA_CONFLICT",
        "OFFLINE",
        "RECOVER",
    )
)
GARAGE_SCENARIOS = frozenset(
    f"GARAGE_{name}"
    for name in (
        "NORMAL_CYCLE",
        "PERSON_INTRUSION",
        "VEHICLE_MISALIGNED",
        "MOTOR_STALL",
        "ABNORMAL_VIBRATION",
        "INTERLOCK_FAILURE",
        "SENSOR_CONFLICT",
        "DATA_MISSING",
        "HIGH_ALTITUDE_VARIATION",
        "RECOVER",
    )
)


def _garage_devices(garage_id: str) -> tuple[DeviceDefinition, ...]:
    """Build the checked-in default profile for one synthetic garage."""

    prefix = garage_id
    return (
        DeviceDefinition(
            garage_id,
            "A-PRESENCE",
            f"{prefix}-LASER-001",
            f"{prefix}-LASER-001",
            "laser_distance",
            "VEHICLE_DISTANCE",
            "laser",
            f"garage/{garage_id}/telemetry/laser/{prefix}-LASER-001",
            "LASER_",
            "LASER_NORMAL",
            LASER_SCENARIOS,
        ),
        DeviceDefinition(
            garage_id,
            "A-ENV",
            f"{prefix}-TH-001",
            f"{prefix}-TH-001",
            "environment",
            "GARAGE_TEMP_HUMIDITY",
            "environment",
            f"garage/{garage_id}/telemetry/environment/{prefix}-TH-001",
            "ENVIRONMENT_",
            "ENVIRONMENT_RECOVER",
            ENVIRONMENT_SCENARIOS,
        ),
        *(
            DeviceDefinition(
                garage_id,
                f"A-{role}",
                f"{prefix}-MOTOR-{role}-001",
                f"{prefix}-CUR-{role}-001",
                "current",
                f"{role}_MOTOR_CURRENT",
                "current",
                f"garage/{garage_id}/telemetry/current/{prefix}-MOTOR-{role}-001",
                f"{role}_CURRENT_",
                f"{role}_CURRENT_NORMAL",
                frozenset(f"{role}_CURRENT_{name}" for name in CURRENT_SUFFIXES),
            )
            for role in ("LIFT", "SHIFT")
        ),
        DeviceDefinition(
            garage_id,
            "A-LIFT",
            f"{prefix}-VIB-001",
            f"{prefix}-VIB-001",
            "vibration",
            "LIFT_MECHANISM_VIBRATION",
            "vibration",
            f"garage/{garage_id}/telemetry/vibration/{prefix}-VIB-001",
            "VIBRATION_",
            "VIBRATION_RECOVER",
            VIBRATION_SCENARIOS,
        ),
        DeviceDefinition(
            garage_id,
            "A-SAFETY",
            f"{prefix}-VISION-001",
            f"{prefix}-VISION-001",
            "vision_event",
            "VISION_SAFETY_STATE",
            "vision",
            f"garage/{garage_id}/telemetry/vision/{prefix}-VISION-001",
            "VISION_",
            "VISION_RECOVER",
            VISION_SCENARIOS,
        ),
        DeviceDefinition(
            garage_id,
            "A-MECHANISM",
            f"{prefix}-STATE-001",
            f"{prefix}-STATE-001",
            "mechanism_state",
            "MECHANISM_STATE",
            "state",
            f"garage/{garage_id}/telemetry/state/{prefix}-STATE-001",
            "MECHANISM_",
            "MECHANISM_RECOVER",
            MECHANISM_SCENARIOS,
        ),
    )


DEVICE_DEFINITIONS = (*_garage_devices("G001"), *_garage_devices("G002"))

# Replaced atomically by each service after loading a committed catalog snapshot.
_catalog_devices: tuple[DeviceDefinition, ...] | None = None
_catalog_garages: tuple[str, ...] = ()
_catalog_enabled: frozenset[str] = frozenset()
_catalog_by_sensor: dict[str, DeviceDefinition] = {}
_catalog_by_topic: dict[str, DeviceDefinition] = {}
_catalog_by_target: dict[str, DeviceDefinition] = {}
_catalog_by_device: dict[str, tuple[DeviceDefinition, ...]] = {}


def install_catalog(
    devices: tuple[DeviceDefinition, ...] | None,
    garages: tuple[str, ...] = (),
    enabled: frozenset[str] = frozenset(),
) -> None:
    global _catalog_devices, _catalog_garages, _catalog_enabled
    global _catalog_by_sensor, _catalog_by_topic, _catalog_by_target, _catalog_by_device
    grouped: dict[str, list[DeviceDefinition]] = {}
    for item in devices or ():
        grouped.setdefault(item.device_id, []).append(item)
    _catalog_by_sensor = {item.sensor_id: item for item in devices or ()}
    _catalog_by_topic = {item.topic: item for item in devices or ()}
    _catalog_by_target = {item.target_id: item for item in devices or ()}
    _catalog_by_device = {key: tuple(values) for key, values in grouped.items()}
    _catalog_devices, _catalog_garages, _catalog_enabled = devices, garages, enabled


def registered_devices() -> tuple[DeviceDefinition, ...]:
    return DEVICE_DEFINITIONS if _catalog_devices is None else _catalog_devices


def registered_garages() -> tuple[str, ...]:
    return ("G001", "G002") if _catalog_devices is None else _catalog_garages


class ControlTargetKind(StrEnum):
    """Kinds exposed by the shared scenario-control surface."""

    TELEMETRY = "telemetry"
    CAMERA = "camera"
    ADAPTER_AUTH = "adapter_auth"
    VIRTUAL_CONTROLLER = "virtual_controller"
    GARAGE_SCENARIO = "garage_scenario"


class DispatcherType(StrEnum):
    """Internal service responsible for a registered control target."""

    TELEMETRY_SIMULATOR = "telemetry_simulator"
    HIKVISION_MOCK = "hikvision_mock"
    VIRTUAL_CONTROLLER_MOCK = "virtual_controller_mock"


@dataclass(frozen=True, slots=True)
class ControlTargetDefinition:
    """Scenario allow-list and dispatch metadata for one control target."""

    target_id: str
    target_kind: ControlTargetKind
    allowed_scenarios: frozenset[str]
    recovery_scenario: str
    dispatcher_type: DispatcherType


@dataclass(frozen=True, slots=True)
class CameraDefinition:
    """Stable camera identity and safe RTSP path mapping."""

    camera_id: str
    camera_name: str
    garage_id: str
    area_id: str
    stream_path: str


CAMERA_DEFINITIONS = (
    CameraDefinition("G001-CAM-001", "升降区域摄像头", "G001", "A-LIFT", "cam001"),
    CameraDefinition(
        "G001-CAM-002",
        "横移及车辆检测区域摄像头",
        "G001",
        "A-SHIFT",
        "cam002",
    ),
    CameraDefinition("G002-CAM-001", "高海拔升降区域摄像头", "G002", "A-LIFT", "cam003"),
    CameraDefinition(
        "G002-CAM-002",
        "高海拔横移及车辆检测区域摄像头",
        "G002",
        "A-SHIFT",
        "cam004",
    ),
)


CAMERA_SCENARIOS = frozenset(
    {
        "CAMERA_ONLINE",
        "CAMERA_OFFLINE",
        "CAMERA_PREVIEW_URL_FAILURE",
        "CAMERA_STREAM_INTERRUPTED",
        "CAMERA_RECOVER",
    }
)
AUTH_SCENARIOS = frozenset({"HIKVISION_AUTH_FAILURE", "HIKVISION_AUTH_RECOVER"})
VIRTUAL_CONTROLLER_SCENARIOS = frozenset(
    {
        "VIRTUAL_COMMAND_SUCCESS",
        "VIRTUAL_COMMAND_FAILED",
        "VIRTUAL_COMMAND_REJECTED",
        "VIRTUAL_COMMAND_TIMEOUT",
        "VIRTUAL_COMMAND_PENDING",
        "VIRTUAL_COMMAND_RECOVER",
    }
)
CONTROL_TARGET_DEFINITIONS = (
    *(
        ControlTargetDefinition(
            target_id=item.device_id,
            target_kind=ControlTargetKind.TELEMETRY,
            allowed_scenarios=item.allowed_scenarios,
            recovery_scenario=item.recovery_scenario,
            dispatcher_type=DispatcherType.TELEMETRY_SIMULATOR,
        )
        for item in DEVICE_DEFINITIONS
    ),
    *(
        ControlTargetDefinition(
            target_id=item.camera_id,
            target_kind=ControlTargetKind.CAMERA,
            allowed_scenarios=CAMERA_SCENARIOS,
            recovery_scenario="CAMERA_RECOVER",
            dispatcher_type=DispatcherType.HIKVISION_MOCK,
        )
        for item in CAMERA_DEFINITIONS
    ),
    *(
        ControlTargetDefinition(
            target_id=f"{garage_id}-SIMULATION",
            target_kind=ControlTargetKind.GARAGE_SCENARIO,
            allowed_scenarios=GARAGE_SCENARIOS,
            recovery_scenario="GARAGE_RECOVER",
            dispatcher_type=DispatcherType.TELEMETRY_SIMULATOR,
        )
        for garage_id in ("G001", "G002")
    ),
    ControlTargetDefinition(
        target_id="HIKVISION-MOCK-AUTH",
        target_kind=ControlTargetKind.ADAPTER_AUTH,
        allowed_scenarios=AUTH_SCENARIOS,
        recovery_scenario="HIKVISION_AUTH_RECOVER",
        dispatcher_type=DispatcherType.HIKVISION_MOCK,
    ),
    ControlTargetDefinition(
        target_id="G001-VIRTUAL-CONTROLLER",
        target_kind=ControlTargetKind.VIRTUAL_CONTROLLER,
        allowed_scenarios=VIRTUAL_CONTROLLER_SCENARIOS,
        recovery_scenario="VIRTUAL_COMMAND_RECOVER",
        dispatcher_type=DispatcherType.VIRTUAL_CONTROLLER_MOCK,
    ),
)


class ControlTargetRegistry:
    """Single target registry shared by control-service dispatchers."""

    def __init__(self) -> None:
        self._by_target = {item.target_id: item for item in CONTROL_TARGET_DEFINITIONS}

    @property
    def targets(self) -> tuple[ControlTargetDefinition, ...]:
        if _catalog_devices is None:
            return CONTROL_TARGET_DEFINITIONS
        return (
            *(
                ControlTargetDefinition(
                    target_id=item.target_id,
                    target_kind=ControlTargetKind.TELEMETRY,
                    allowed_scenarios=item.allowed_scenarios,
                    recovery_scenario=item.recovery_scenario,
                    dispatcher_type=DispatcherType.TELEMETRY_SIMULATOR,
                )
                for item in _catalog_devices
                if item.sensor_id in _catalog_enabled
            ),
            *(
                ControlTargetDefinition(
                    target_id=f"{garage}-SIMULATION",
                    target_kind=ControlTargetKind.GARAGE_SCENARIO,
                    allowed_scenarios=GARAGE_SCENARIOS,
                    recovery_scenario="GARAGE_RECOVER",
                    dispatcher_type=DispatcherType.TELEMETRY_SIMULATOR,
                )
                for garage in _catalog_garages
            ),
            *(
                item
                for item in CONTROL_TARGET_DEFINITIONS
                if item.target_kind
                not in {ControlTargetKind.TELEMETRY, ControlTargetKind.GARAGE_SCENARIO}
            ),
        )

    def by_target(self, target_id: str) -> ControlTargetDefinition:
        if _catalog_devices is not None:
            targets = {item.target_id: item for item in self.targets}
            if target_id not in targets:
                with contextlib.suppress(DeviceSemanticError):
                    target_id = DeviceSemanticRegistry().by_device(target_id).target_id
            if target_id in targets:
                return targets[target_id]
            raise DeviceSemanticError("unknown_control_target", f"unknown targetId: {target_id}")
        try:
            return self._by_target[target_id]
        except KeyError as exc:
            raise DeviceSemanticError(
                "unknown_control_target", f"unknown targetId: {target_id}"
            ) from exc

    def validate_activation(self, target_id: str, scenario_id: str) -> ControlTargetDefinition:
        definition = self.by_target(target_id)
        if scenario_id not in definition.allowed_scenarios:
            raise DeviceSemanticError(
                "scenario_target_mismatch",
                f"scenarioId {scenario_id!r} is not allowed for {target_id}",
            )
        return definition


class DeviceSemanticRegistry:
    """Single source of truth for device, topic, and scenario relationships."""

    def __init__(self) -> None:
        self._by_device = {item.device_id: item for item in DEVICE_DEFINITIONS}
        self._by_topic = {item.topic: item for item in DEVICE_DEFINITIONS}

    @property
    def devices(self) -> tuple[DeviceDefinition, ...]:
        """Return the stable registered device definitions."""

        return registered_devices()

    @property
    def active_devices(self) -> tuple[DeviceDefinition, ...]:
        return tuple(
            item
            for item in self.devices
            if _catalog_devices is None or item.sensor_id in _catalog_enabled
        )

    def by_device(self, device_id: str) -> DeviceDefinition:
        if _catalog_devices is not None:
            direct = _catalog_by_target.get(device_id)
            if direct is not None:
                return direct
            matches = _catalog_by_device.get(device_id, ())
            if len(matches) == 1:
                return matches[0]
            raise DeviceSemanticError(
                "unknown_or_ambiguous_device", f"use sensor:<sensorId> for target: {device_id}"
            )
        try:
            return self._by_device[device_id]
        except KeyError as exc:
            raise DeviceSemanticError("unknown_device", f"unknown deviceId: {device_id}") from exc

    def by_topic(self, topic: str) -> DeviceDefinition:
        if _catalog_devices is not None:
            definition = _catalog_by_topic.get(topic)
            if definition is not None:
                return definition
            raise DeviceSemanticError("unknown_topic", f"unknown telemetry topic: {topic}")
        try:
            return self._by_topic[topic]
        except KeyError as exc:
            raise DeviceSemanticError("unknown_topic", f"unknown telemetry topic: {topic}") from exc

    def validate_message(self, message: TelemetryMessage) -> DeviceDefinition:
        if _catalog_devices is None:
            definition = self.by_device(message.device_id)
        else:
            found = _catalog_by_sensor.get(message.sensor_id)
            if found is None:
                raise DeviceSemanticError(
                    "unknown_sensor", f"unknown sensorId: {message.sensor_id}"
                )
            definition = found
        fields = {
            "deviceId": (message.device_id, definition.device_id),
            "garageId": (message.garage_id, definition.garage_id),
            "areaId": (message.area_id, definition.area_id),
            "sensorId": (message.sensor_id, definition.sensor_id),
            "sensorType": (message.sensor_type, definition.sensor_type),
            "pointCode": (message.point_code, definition.point_code),
        }
        mismatches = [name for name, values in fields.items() if values[0] != values[1]]
        if mismatches:
            raise DeviceSemanticError(
                "device_semantic_mismatch",
                f"device fields do not match {definition.device_id}: {mismatches}",
            )
        if message.scenario_id not in definition.allowed_scenarios:
            raise DeviceSemanticError(
                "scenario_device_mismatch",
                f"scenarioId {message.scenario_id!r} is not allowed for {definition.device_id}",
            )
        return definition

    def validate_topic_payload(self, topic: str, message: TelemetryMessage) -> DeviceDefinition:
        topic_definition = self.by_topic(topic)
        payload_definition = self.validate_message(message)
        if topic_definition != payload_definition:
            raise DeviceSemanticError(
                "topic_payload_mismatch",
                f"topic targets {topic_definition.device_id}, payload targets {message.device_id}",
            )
        return payload_definition

    def validate_activation(self, target_id: str, scenario_id: str) -> DeviceDefinition:
        definition = self.by_device(target_id)
        if scenario_id not in definition.allowed_scenarios:
            raise DeviceSemanticError(
                "scenario_device_mismatch",
                f"scenarioId {scenario_id!r} is not allowed for {target_id}",
            )
        return definition


@dataclass(frozen=True, slots=True)
class ContractDefinition:
    """Static model and schema mapping for one sensor type."""

    sensor_type: str
    model: TelemetryModel
    schema_filename: str


@dataclass(frozen=True, slots=True)
class CompiledContract:
    """One startup-loaded Pydantic and JSON Schema contract."""

    definition: ContractDefinition
    schema: Draft202012Validator


CONTRACT_DEFINITIONS = (
    ContractDefinition(
        sensor_type="laser_distance",
        model=LaserTelemetryMessage,
        schema_filename="laser-telemetry-envelope.schema.json",
    ),
    ContractDefinition(
        sensor_type="current",
        model=CurrentTelemetryMessage,
        schema_filename="current-telemetry-envelope.schema.json",
    ),
    ContractDefinition(
        sensor_type="vibration",
        model=VibrationTelemetryMessage,
        schema_filename="vibration-telemetry-envelope.schema.json",
    ),
    ContractDefinition(
        sensor_type="environment",
        model=EnvironmentTelemetryMessage,
        schema_filename="environment-telemetry-envelope.schema.json",
    ),
    ContractDefinition(
        sensor_type="vision_event",
        model=VisionTelemetryMessage,
        schema_filename="vision-telemetry-envelope.schema.json",
    ),
    ContractDefinition(
        sensor_type="mechanism_state",
        model=MechanismStateTelemetryMessage,
        schema_filename="mechanism-state-telemetry-envelope.schema.json",
    ),
)


class TelemetryContractRegistry:
    """Select and validate telemetry contracts without per-message file I/O."""

    def __init__(self, schema_directory: Path) -> None:
        contracts: dict[str, CompiledContract] = {}
        for definition in CONTRACT_DEFINITIONS:
            path = schema_directory / definition.schema_filename
            value = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(value, dict):
                raise ValueError(f"JSON Schema must be an object: {path}")
            schema = cast(JsonObject, value)
            Draft202012Validator.check_schema(schema)
            contracts[definition.sensor_type] = CompiledContract(
                definition=definition,
                schema=Draft202012Validator(
                    schema,
                    format_checker=FormatChecker(),
                ),
            )
        self._contracts = contracts

    @property
    def sensor_types(self) -> frozenset[str]:
        """Return all registered sensor types."""

        return frozenset(self._contracts)

    def contract_for(self, sensor_type: object) -> CompiledContract:
        """Return one compiled contract or reject a stable unsupported type."""

        if not isinstance(sensor_type, str) or sensor_type not in self._contracts:
            raise UnsupportedSensorTypeError(f"unsupported sensorType: {sensor_type!r}")
        return self._contracts[sensor_type]

    def validate(self, value: object) -> TelemetryMessage:
        """Validate one decoded JSON value against its selected dual contract."""

        if not isinstance(value, dict):
            raise PydanticValidationError.from_exception_data(
                "TelemetryMessage",
                [],
            )
        contract = self.contract_for(value.get("sensorType"))
        message = contract.definition.model.model_validate(value)
        contract.schema.validate(value)
        return message
