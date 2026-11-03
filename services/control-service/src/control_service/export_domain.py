"""Strict export request contracts and source selection validation."""

from datetime import datetime
from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import AwareDatetime, Field, model_validator
from sim_core import CONTRACT_DEFINITIONS, DataQuality, TelemetryStatus
from sim_core.registry import registered_devices, registered_garages

from control_service.domain import ApiModel
from control_service.export_models import ExportFormat, ExportType


class TimeRangeExportSource(ApiModel):
    type: Literal["time_range"]
    received_from: AwareDatetime = Field(alias="receivedFrom")
    received_to: AwareDatetime = Field(alias="receivedTo")
    sensor_ids: list[str] = Field(default_factory=list, alias="sensorIds", max_length=10000)
    garage_ids: list[str] = Field(default_factory=list, alias="garageIds", max_length=10000)
    device_ids: list[str] = Field(default_factory=list, alias="deviceIds")
    sensor_types: list[str] = Field(default_factory=list, alias="sensorTypes")
    scenario_ids: list[str] = Field(default_factory=list, alias="scenarioIds")
    statuses: list[TelemetryStatus] = Field(default_factory=list)
    qualities: list[DataQuality] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_range_and_whitelists(self) -> Self:
        if self.received_to <= self.received_from:
            raise ValueError("receivedTo must be later than receivedFrom")
        known_devices = {item.device_id for item in registered_devices()}
        known_sensors = {item.sensor_type for item in CONTRACT_DEFINITIONS}
        known_scenarios = {
            scenario for item in registered_devices() for scenario in item.allowed_scenarios
        }
        for name, values, allowed in (
            ("sensorIds", self.sensor_ids, {item.sensor_id for item in registered_devices()}),
            ("garageIds", self.garage_ids, set(registered_garages())),
            ("deviceIds", self.device_ids, known_devices),
            ("sensorTypes", self.sensor_types, known_sensors),
            ("scenarioIds", self.scenario_ids, known_scenarios),
        ):
            unknown = sorted(set(values) - allowed)
            if unknown:
                raise ValueError(f"{name} contains unsupported values: {unknown}")
            if len(values) != len(set(values)):
                raise ValueError(f"{name} must not contain duplicates")
        return self


class TestRunExportSource(ApiModel):
    type: Literal["test_run"]
    run_id: UUID = Field(alias="runId")


class ReplayRunExportSource(ApiModel):
    type: Literal["replay_run"]
    replay_id: UUID = Field(alias="replayId")


ExportSource = Annotated[
    TimeRangeExportSource | TestRunExportSource | ReplayRunExportSource,
    Field(discriminator="type"),
]


class CreateExportRequest(ApiModel):
    name: str = Field(min_length=1, max_length=200)
    export_type: ExportType = Field(alias="exportType")
    format: ExportFormat
    source: ExportSource
    include_replay_outputs: bool = Field(default=False, alias="includeReplayOutputs")
    include_ingest_failures: bool = Field(default=False, alias="includeIngestFailures")
    include_run_audit: bool = Field(default=False, alias="includeRunAudit")
    created_by: str = Field(alias="createdBy", min_length=1, max_length=200)

    @model_validator(mode="after")
    def validate_combination(self) -> Self:
        valid = {
            (ExportType.TELEMETRY, ExportFormat.JSONL),
            (ExportType.TELEMETRY, ExportFormat.CSV),
            (ExportType.RUN_AUDIT, ExportFormat.JSONL),
            (ExportType.RUN_AUDIT, ExportFormat.ZIP),
            (ExportType.COMBINED_BUNDLE, ExportFormat.ZIP),
        }
        if (self.export_type, self.format) not in valid:
            raise ValueError("unsupported exportType/format combination")
        if self.export_type is ExportType.COMBINED_BUNDLE and not self.include_run_audit:
            raise ValueError("combined_bundle requires includeRunAudit=true")
        if (
            isinstance(self.source, TimeRangeExportSource)
            and self.export_type is not ExportType.TELEMETRY
        ):
            raise ValueError("time_range source only supports telemetry exports")
        if self.include_ingest_failures and self.format is not ExportFormat.ZIP:
            raise ValueError("includeIngestFailures requires ZIP format")
        return self


def source_reference(source: ExportSource) -> UUID | None:
    if isinstance(source, TestRunExportSource):
        return source.run_id
    if isinstance(source, ReplayRunExportSource):
        return source.replay_id
    return None


def receive_window(source: ExportSource) -> tuple[datetime, datetime] | None:
    if isinstance(source, TimeRangeExportSource):
        return source.received_from, source.received_to
    return None
