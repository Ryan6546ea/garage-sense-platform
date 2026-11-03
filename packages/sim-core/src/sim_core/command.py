"""Strict virtual-command contracts, separate from telemetry envelopes."""

import json
import math
import re
from enum import StrEnum
from pathlib import Path
from typing import Any, Self, cast
from uuid import UUID

from jsonschema import Draft202012Validator, FormatChecker
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator

COMMAND_SCHEMA_VERSION = "1.0"
COMMAND_TARGET_ID = "G001-VIRTUAL-CONTROLLER"
COMMAND_FEEDBACK_TOPIC = "garage/G001/command/feedback"
COMMAND_AREAS = frozenset({"A-LIFT", "A-SHIFT", "A-PRESENCE", "A-ENV"})
COMMAND_ID_PATTERN = re.compile(
    r"^CMD-[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)
FEEDBACK_ID_PATTERN = re.compile(
    r"^FDB-[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)


class CommandAction(StrEnum):
    ALARM_BUZZER_ON = "ALARM_BUZZER_ON"
    ALARM_BUZZER_OFF = "ALARM_BUZZER_OFF"
    STOP_REQUEST = "STOP_REQUEST"
    DISABLE_AREA = "DISABLE_AREA"
    ENABLE_AREA = "ENABLE_AREA"


class CommandResult(StrEnum):
    SUCCESS = "success"
    FAILED = "failed"
    REJECTED = "rejected"
    TIMEOUT = "timeout"
    PENDING = "pending"


type JsonValue = bool | int | float | str | list[JsonValue] | dict[str, JsonValue] | None


def _validate_prefixed_uuid(value: str, prefix: str, pattern: re.Pattern[str]) -> str:
    if not pattern.fullmatch(value):
        raise ValueError(f"identifier must use {prefix}<canonical UUIDv4>")
    parsed = UUID(value[len(prefix) :])
    if parsed.version != 4:
        raise ValueError(f"identifier must use {prefix}<canonical UUIDv4>")
    return value


def _validate_json(value: JsonValue) -> None:
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("parameters must not contain NaN or Infinity")
    if isinstance(value, list):
        for item in value:
            _validate_json(item)
    elif isinstance(value, dict):
        for item in value.values():
            _validate_json(item)
    json.dumps(value, allow_nan=False)


class CommandModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
        serialize_by_alias=True,
        str_strip_whitespace=True,
    )


class VirtualCommandRequest(CommandModel):
    schema_version: str = Field(alias="schemaVersion", pattern=r"^1\.0$")
    command_id: str = Field(alias="commandId")
    garage_id: str = Field(alias="garageId", pattern=r"^G001$")
    target_device_id: str = Field(alias="targetDeviceId", pattern=r"^G001-VIRTUAL-CONTROLLER$")
    action: CommandAction
    reason: str = Field(min_length=1, max_length=1000)
    issued_by: str = Field(alias="issuedBy", min_length=1, max_length=200)
    source_time: AwareDatetime = Field(alias="sourceTime")
    parameters: dict[str, JsonValue]

    @field_validator("command_id")
    @classmethod
    def validate_command_id(cls, value: str) -> str:
        return _validate_prefixed_uuid(value, "CMD-", COMMAND_ID_PATTERN)

    @model_validator(mode="after")
    def validate_action_parameters(self) -> Self:
        _validate_json(cast(JsonValue, self.parameters))
        keys = set(self.parameters)
        if self.action in {CommandAction.ALARM_BUZZER_ON, CommandAction.ALARM_BUZZER_OFF}:
            if keys:
                raise ValueError("buzzer actions require empty parameters")
        elif self.action is CommandAction.STOP_REQUEST:
            if keys - {"scope", "areaId"}:
                raise ValueError("STOP_REQUEST accepts only scope and areaId")
            scope = self.parameters.get("scope")
            area = self.parameters.get("areaId")
            if scope == "garage" and area is not None:
                raise ValueError("garage STOP_REQUEST must not include areaId")
            if scope == "area" and area not in COMMAND_AREAS:
                raise ValueError("area STOP_REQUEST requires a registered areaId")
            if scope not in {"garage", "area"}:
                raise ValueError("STOP_REQUEST scope must be garage or area")
        else:
            if keys != {"areaId"} or self.parameters.get("areaId") not in COMMAND_AREAS:
                raise ValueError("area actions require one registered areaId")
        return self


class VirtualCommandFeedback(CommandModel):
    schema_version: str = Field(alias="schemaVersion", pattern=r"^1\.0$")
    feedback_id: str = Field(alias="feedbackId")
    command_id: str = Field(alias="commandId")
    garage_id: str = Field(alias="garageId", pattern=r"^G001$")
    target_device_id: str = Field(alias="targetDeviceId", pattern=r"^G001-VIRTUAL-CONTROLLER$")
    action: CommandAction
    result: CommandResult
    message: str = Field(min_length=1, max_length=1000)
    feedback_sequence: int = Field(alias="feedbackSequence", ge=1)
    emitted_at: AwareDatetime = Field(alias="emittedAt")
    executed_at: AwareDatetime | None = Field(alias="executedAt")
    source: str = Field(pattern=r"^virtual-controller-mock$")

    @field_validator("feedback_id")
    @classmethod
    def validate_feedback_id(cls, value: str) -> str:
        return _validate_prefixed_uuid(value, "FDB-", FEEDBACK_ID_PATTERN)

    @field_validator("command_id")
    @classmethod
    def validate_command_id(cls, value: str) -> str:
        return _validate_prefixed_uuid(value, "CMD-", COMMAND_ID_PATTERN)

    @model_validator(mode="after")
    def validate_execution_time(self) -> Self:
        if self.result is CommandResult.PENDING and self.executed_at is not None:
            raise ValueError("pending feedback must have executedAt=null")
        if self.result is not CommandResult.PENDING and self.executed_at is None:
            raise ValueError("terminal feedback requires executedAt")
        return self

    def validate_for(self, command: VirtualCommandRequest) -> None:
        for name, actual, expected in (
            ("commandId", self.command_id, command.command_id),
            ("garageId", self.garage_id, command.garage_id),
            ("targetDeviceId", self.target_device_id, command.target_device_id),
            ("action", self.action, command.action),
        ):
            if actual != expected:
                raise ValueError(f"feedback {name} does not match the original command")


class CommandContractRegistry:
    """Startup-loaded command and feedback JSON Schemas."""

    def __init__(self, schema_directory: Path) -> None:
        self.command = self._load(schema_directory / "virtual-command.schema.json")
        self.feedback = self._load(schema_directory / "virtual-command-feedback.schema.json")

    @staticmethod
    def _load(path: Path) -> Draft202012Validator:
        value: Any = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise ValueError(f"JSON Schema must be an object: {path}")
        Draft202012Validator.check_schema(value)
        return Draft202012Validator(value, format_checker=FormatChecker())

    def validate_command(self, value: object) -> VirtualCommandRequest:
        command = VirtualCommandRequest.model_validate(value)
        self.command.validate(value)
        return command

    def validate_feedback(self, value: object) -> VirtualCommandFeedback:
        feedback = VirtualCommandFeedback.model_validate(value)
        self.feedback.validate(value)
        return feedback
