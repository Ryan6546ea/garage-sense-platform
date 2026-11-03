"""Pydantic and JSON Schema validation for incoming telemetry."""

import json
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from jsonschema.exceptions import ValidationError as JsonSchemaValidationError
from pydantic import ValidationError as PydanticValidationError
from sim_core import (
    DeviceSemanticError,
    DeviceSemanticRegistry,
    TelemetryContractRegistry,
    TelemetryMessage,
    UnsupportedSensorTypeError,
)

type JsonObject = dict[str, object]


@dataclass(frozen=True, slots=True)
class ValidatedTelemetry:
    """A payload accepted by both checked-in contracts."""

    message: TelemetryMessage
    payload: JsonObject


class TelemetryValidationError(ValueError):
    """An invalid inbound message with a persistence-safe classification."""

    def __init__(self, error_type: str, detail: str) -> None:
        super().__init__(detail)
        self.error_type = error_type
        self.detail = detail


class ContractValidator:
    """Validate raw MQTT payloads without duplicating the sim-core model."""

    def __init__(
        self,
        schema_directory: Path,
        *,
        max_message_bytes: int = 65_536,
    ) -> None:
        self.registry = TelemetryContractRegistry(schema_directory)
        self.semantics = DeviceSemanticRegistry()
        self.max_message_bytes = max_message_bytes

    def validate(self, raw_payload: bytes, topic: str | None = None) -> ValidatedTelemetry:
        """Return the dual-contract-valid message or a classified error."""

        if len(raw_payload) > self.max_message_bytes:
            raise TelemetryValidationError(
                "message_too_large",
                f"payload is {len(raw_payload)} bytes; maximum is {self.max_message_bytes}",
            )
        try:
            value = json.loads(raw_payload)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise TelemetryValidationError("invalid_json", str(exc)) from exc

        if not isinstance(value, dict):
            raise TelemetryValidationError("pydantic_validation", "payload must be an object")
        try:
            message = self.registry.validate(value)
        except UnsupportedSensorTypeError as exc:
            raise TelemetryValidationError("unsupported_sensor_type", str(exc)) from exc
        except PydanticValidationError as exc:
            raise TelemetryValidationError(
                "pydantic_validation",
                str(exc),
            ) from exc
        except JsonSchemaValidationError as exc:
            raise TelemetryValidationError(
                "json_schema_validation",
                exc.message,
            ) from exc
        try:
            if topic is None:
                self.semantics.validate_message(message)
            else:
                self.semantics.validate_topic_payload(topic, message)
        except DeviceSemanticError as exc:
            raise TelemetryValidationError(exc.error_type, exc.detail) from exc

        return ValidatedTelemetry(message=message, payload=cast(JsonObject, value))
