"""Deterministic serialization primitives for M6.2 export files."""

import csv
import json
from collections.abc import Iterable
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Protocol, TextIO, cast

from sqlalchemy.inspection import inspect

from control_service.export_models import ExportItem
from control_service.replay_models import TelemetrySourceEvent

CSV_HEADER = [
    "eventId",
    "messageId",
    "schemaVersion",
    "garageId",
    "areaId",
    "deviceId",
    "sensorId",
    "pointCode",
    "sensorType",
    "sourceTime",
    "receivedAt",
    "seq",
    "status",
    "quality",
    "actionStage",
    "source",
    "scenarioId",
    "topic",
    "qos",
    "retain",
    "dataJson",
]


def iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    result = value.astimezone(UTC).isoformat(timespec="microseconds")
    return result.replace("+00:00", "Z")


def json_default(value: object) -> object:
    if isinstance(value, datetime):
        return iso(value)
    if isinstance(value, Enum):
        return value.value
    return str(value)


def canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
        default=json_default,
    )


def telemetry_json(item: ExportItem, event: TelemetrySourceEvent) -> dict[str, Any]:
    return {
        "eventId": event.id,
        "topic": event.topic,
        "qos": event.qos,
        "retain": event.retain,
        "receivedAt": iso(event.received_at),
        "telemetry": event.payload,
    }


def telemetry_csv_row(item: ExportItem, event: TelemetrySourceEvent) -> list[object]:
    payload = event.payload
    return [
        event.id,
        payload.get("messageId", ""),
        payload.get("schemaVersion", ""),
        payload.get("garageId", ""),
        payload.get("areaId", ""),
        payload.get("deviceId", ""),
        payload.get("sensorId", ""),
        payload.get("pointCode", ""),
        payload.get("sensorType", ""),
        payload.get("sourceTime", ""),
        iso(event.received_at),
        payload.get("seq", ""),
        payload.get("status", ""),
        payload.get("quality", ""),
        payload.get("actionStage", ""),
        payload.get("source", ""),
        payload.get("scenarioId", ""),
        event.topic,
        event.qos,
        "true" if event.retain else "false",
        canonical_json(payload.get("data")),
    ]


def write_jsonl(stream: TextIO, value: object) -> None:
    stream.write(canonical_json(value))
    stream.write("\n")


class CsvWriter(Protocol):
    def writerow(self, row: Iterable[Any]) -> Any: ...


def csv_writer(stream: TextIO) -> CsvWriter:
    writer = csv.writer(stream, dialect="excel", lineterminator="\r\n")
    writer.writerow(CSV_HEADER)
    return writer


def model_dict(value: object) -> dict[str, Any]:
    mapper = cast(Any, inspect(value)).mapper
    return {attribute.key: getattr(value, attribute.key) for attribute in mapper.column_attrs}


def file_metadata(path: Path, *, relative_to: Path) -> dict[str, object]:
    from control_service.export_storage import ExportStorage

    return {
        "path": path.relative_to(relative_to).as_posix(),
        "sizeBytes": path.stat().st_size,
        "sha256": ExportStorage.sha256(path),
    }


def sanitize_detail(value: str) -> str:
    return "".join(
        character if character >= " " or character in "\t" else "?" for character in value
    )[:2000]
