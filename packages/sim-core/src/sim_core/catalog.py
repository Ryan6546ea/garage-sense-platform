"""Strict registration contracts for the six supported telemetry families."""

from typing import Annotated, Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, GetJsonSchemaHandler, model_validator
from pydantic.json_schema import JsonSchemaValue
from pydantic_core import CoreSchema

from sim_core.registry import (
    CURRENT_SUFFIXES,
    ENVIRONMENT_SCENARIOS,
    LASER_SCENARIOS,
    MECHANISM_SCENARIOS,
    VIBRATION_SCENARIOS,
    VISION_SCENARIOS,
    DeviceDefinition,
)

Identifier = Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,99}$")]
SensorType = Literal[
    "laser_distance", "current", "vibration", "environment", "vision_event", "mechanism_state"
]


class CatalogModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid", populate_by_name=True, allow_inf_nan=False, strict=True
    )


class LaserParameters(CatalogModel):
    vehicle_distance_m: float = Field(alias="vehicleDistanceM", ge=0, le=1000)
    clear_distance_m: float = Field(alias="clearDistanceM", ge=0, le=1000)


class CurrentParameters(CatalogModel):
    batch_samples: int = Field(alias="batchSamples", ge=1, le=1000, strict=True)
    sample_rate_hz: int = Field(alias="sampleRateHz", ge=1, le=100000, strict=True)


class VibrationParameters(CatalogModel):
    sample_count: int = Field(alias="sampleCount", ge=1, le=10000, strict=True)
    sample_rate_hz: int = Field(alias="sampleRateHz", ge=1, le=100000, strict=True)
    normal_amplitude: float = Field(alias="normalAmplitude", ge=0, le=1000)


class EnvironmentParameters(CatalogModel):
    temperature_c: float = Field(alias="temperatureC", ge=-80, le=80)
    humidity_rh: float = Field(alias="humidityRh", ge=1, le=99)
    pressure_k_pa: float = Field(alias="pressureKPa", ge=10, le=120)
    altitude_meters: float = Field(alias="altitudeMeters", ge=0, le=10000)


class MechanismParameters(CatalogModel):
    load_kg: float = Field(alias="loadKg", ge=0, le=100000)


PARAMETER_MODELS: dict[str, type[CatalogModel]] = {
    "laser_distance": LaserParameters,
    "current": CurrentParameters,
    "vibration": VibrationParameters,
    "environment": EnvironmentParameters,
    "vision_event": CatalogModel,
    "mechanism_state": MechanismParameters,
}
TYPE_RULES = {
    "laser_distance": ("laser", "VEHICLE_DISTANCE", "LASER_NORMAL", LASER_SCENARIOS),
    "vibration": (
        "vibration",
        "LIFT_MECHANISM_VIBRATION",
        "VIBRATION_RECOVER",
        VIBRATION_SCENARIOS,
    ),
    "environment": (
        "environment",
        "GARAGE_TEMP_HUMIDITY",
        "ENVIRONMENT_RECOVER",
        ENVIRONMENT_SCENARIOS,
    ),
    "vision_event": ("vision", "VISION_SAFETY_STATE", "VISION_RECOVER", VISION_SCENARIOS),
    "mechanism_state": ("state", "MECHANISM_STATE", "MECHANISM_RECOVER", MECHANISM_SCENARIOS),
}


class GarageCreate(CatalogModel):
    garage_id: Identifier = Field(alias="garageId")
    name: str = Field(min_length=1, max_length=200)
    altitude_meters: float = Field(alias="altitudeMeters", ge=0, le=10000)


class AreaCreate(CatalogModel):
    area_id: Identifier = Field(alias="areaId")
    name: str = Field(min_length=1, max_length=200)


class DeviceCreate(CatalogModel):
    device_id: Identifier = Field(alias="deviceId")
    area_id: Identifier = Field(alias="areaId")
    name: str = Field(min_length=1, max_length=200)


class SensorCreate(CatalogModel):
    sensor_id: Identifier = Field(alias="sensorId")
    device_id: Identifier = Field(alias="deviceId")
    sensor_type: SensorType = Field(alias="sensorType")
    name: str = Field(min_length=1, max_length=200)
    point_code: str = Field(alias="pointCode", min_length=1, max_length=100)
    role: Literal["LIFT", "SHIFT"] | None = None
    camera_id: Identifier | None = Field(default=None, alias="cameraId")
    publish_interval_seconds: float = Field(alias="publishIntervalSeconds", ge=0.1, le=86400)
    seed: int = Field(ge=0, le=2147483647, strict=True)
    parameters: dict[str, Any]
    desired_enabled: bool = Field(alias="desiredEnabled", strict=True)

    @classmethod
    def __get_pydantic_json_schema__(
        cls,
        core_schema: CoreSchema,
        handler: GetJsonSchemaHandler,
    ) -> JsonSchemaValue:
        schema = handler(core_schema)
        rules = []
        for kind, parameters_model in PARAMETER_MODELS.items():
            properties: dict[str, Any] = {
                "parameters": parameters_model.model_json_schema(by_alias=True),
            }
            required = []
            if kind == "current":
                properties["role"] = {"enum": ["LIFT", "SHIFT"]}
                properties["pointCode"] = {"enum": ["LIFT_MOTOR_CURRENT", "SHIFT_MOTOR_CURRENT"]}
                required.append("role")
            else:
                properties["pointCode"] = {"const": TYPE_RULES[kind][1]}
                properties["role"] = {"type": "null"}
            if kind == "vision_event":
                properties["cameraId"] = {"type": "string", "minLength": 1}
                required.append("cameraId")
            else:
                properties["cameraId"] = {"type": "null"}
            rules.append(
                {
                    "if": {"properties": {"sensorType": {"const": kind}}},
                    "then": {"properties": properties, "required": required},
                }
            )
        for role in ("LIFT", "SHIFT"):
            rules.append(
                {
                    "if": {
                        "properties": {"sensorType": {"const": "current"}, "role": {"const": role}}
                    },
                    "then": {"properties": {"pointCode": {"const": f"{role}_MOTOR_CURRENT"}}},
                }
            )
        schema["allOf"] = rules
        return schema

    @model_validator(mode="after")
    def validate_type_configuration(self) -> Self:
        PARAMETER_MODELS[self.sensor_type].model_validate(self.parameters)
        if self.sensor_type == "current":
            if self.role is None or self.point_code != f"{self.role}_MOTOR_CURRENT":
                raise ValueError("current requires LIFT/SHIFT role and matching pointCode")
        else:
            if self.role is not None:
                raise ValueError("role is only supported for current")
            if self.point_code != TYPE_RULES[self.sensor_type][1]:
                raise ValueError("pointCode does not match sensorType")
        if (self.camera_id is not None) != (self.sensor_type == "vision_event"):
            raise ValueError("cameraId is required only for synthetic vision_event sources")
        return self


class SensorPatch(CatalogModel):
    config_version: int = Field(alias="configVersion", gt=0)
    name: str | None = Field(default=None, min_length=1, max_length=200)
    publish_interval_seconds: float | None = Field(
        default=None, alias="publishIntervalSeconds", ge=0.1, le=86400
    )
    seed: int | None = Field(default=None, ge=0, le=2147483647, strict=True)
    parameters: dict[str, Any] | None = None

    @classmethod
    def __get_pydantic_json_schema__(
        cls,
        core_schema: CoreSchema,
        handler: GetJsonSchemaHandler,
    ) -> JsonSchemaValue:
        schema = handler(core_schema)
        for field in schema.get("properties", {}).values():
            if "anyOf" in field:
                field["anyOf"] = [item for item in field["anyOf"] if item.get("type") != "null"]
                field.pop("default", None)
        return schema

    @model_validator(mode="after")
    def no_explicit_null(self) -> Self:
        if any(getattr(self, name) is None for name in self.model_fields_set):
            raise ValueError("explicit null cannot clear required configuration")
        return self


class VersionRequest(CatalogModel):
    config_version: int = Field(alias="configVersion", gt=0)


class RegisteredSensor(SensorCreate):
    garage_id: Identifier = Field(alias="garageId")
    area_id: Identifier = Field(alias="areaId")
    topic: str = Field(min_length=1)
    config_version: int = Field(alias="configVersion", gt=0)
    archived: bool

    def definition(self) -> DeviceDefinition:
        if self.sensor_type == "current":
            topic_type = "current"
            recovery = f"{self.role}_CURRENT_NORMAL"
            scenarios = frozenset(f"{self.role}_CURRENT_{name}" for name in CURRENT_SUFFIXES)
        else:
            topic_type, _, recovery, scenarios = TYPE_RULES[self.sensor_type]
        return DeviceDefinition(
            self.garage_id,
            self.area_id,
            self.device_id,
            self.sensor_id,
            self.sensor_type,
            self.point_code,
            topic_type,
            self.topic,
            recovery.rsplit("_", 1)[0] + "_",
            recovery,
            scenarios,
            control_id=f"sensor:{self.sensor_id}",
        )

    def profile(self) -> dict[str, Any]:
        return self.model_dump(
            by_alias=True,
            exclude={"name", "desired_enabled", "config_version", "archived"},
        )


class CatalogSnapshot(CatalogModel):
    revision: int = Field(ge=0)
    garages: list[GarageCreate]
    sensors: list[RegisteredSensor]

    @model_validator(mode="after")
    def unique_identities(self) -> Self:
        for values in (
            [garage.garage_id for garage in self.garages],
            [sensor.sensor_id for sensor in self.sensors],
            [sensor.topic for sensor in self.sensors],
        ):
            if len(values) != len(set(values)):
                raise ValueError("catalog contains duplicate identities")
        garages = {garage.garage_id for garage in self.garages}
        if any(sensor.garage_id not in garages for sensor in self.sensors):
            raise ValueError("sensor references an unknown garage")
        return self


class RuntimeReport(CatalogModel):
    service: Literal["simulator", "consumer"]
    instance_id: str = Field(alias="instanceId", min_length=1, max_length=100)
    applied_revision: int = Field(alias="appliedRevision", ge=0)
    error: str | None = Field(default=None, max_length=2000)
