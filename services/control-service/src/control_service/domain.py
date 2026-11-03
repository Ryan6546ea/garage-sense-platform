"""Strict API-domain models and deterministic random selection."""

import random
from dataclasses import dataclass
from datetime import datetime
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, model_validator
from sim_core import DeviceSemanticError, DeviceSemanticRegistry
from sim_core.catalog import RegisteredSensor
from sim_core.registry import DeviceDefinition


class ApiModel(BaseModel):
    """Strict camelCase model used by control-service APIs and persisted config."""

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
        serialize_by_alias=True,
    )


class ScenarioDuration(ApiModel):
    minimum_seconds: int = Field(alias="minimumSeconds", gt=0, le=86_400)
    maximum_seconds: int = Field(alias="maximumSeconds", gt=0, le=86_400)

    @model_validator(mode="after")
    def maximum_must_cover_minimum(self) -> Self:
        if self.maximum_seconds < self.minimum_seconds:
            raise ValueError("maximumSeconds must be greater than or equal to minimumSeconds")
        return self


class RandomRunConfiguration(ApiModel):
    """Persisted, reproducible random scheduler input."""

    duration_seconds: float = Field(alias="durationSeconds", gt=0, le=7 * 86_400)
    registered_sensors: list[RegisteredSensor] | None = Field(
        default=None, alias="registeredSensors"
    )
    abnormal_probability: float = Field(
        alias="abnormalProbability",
        ge=0,
        le=1,
        allow_inf_nan=False,
    )
    seed: int
    allowed_targets: list[str] = Field(alias="allowedTargets", min_length=1)
    allowed_scenarios: list[str] = Field(alias="allowedScenarios", min_length=1)
    scenario_duration: ScenarioDuration = Field(alias="scenarioDuration")
    selection_interval_seconds: float = Field(
        alias="selectionIntervalSeconds",
        gt=0,
        le=86_400,
        allow_inf_nan=False,
    )

    @model_validator(mode="after")
    def device_scenario_combinations_must_be_registered(self) -> Self:
        if len(set(self.allowed_targets)) != len(self.allowed_targets):
            raise ValueError("allowedTargets must not contain duplicates")
        if len(set(self.allowed_scenarios)) != len(self.allowed_scenarios):
            raise ValueError("allowedScenarios must not contain duplicates")
        definitions = []
        for target in self.allowed_targets:
            try:
                definitions.append(self.definition(target))
            except DeviceSemanticError as exc:
                raise ValueError(exc.detail) from exc
        for scenario in self.allowed_scenarios:
            if not any(scenario in item.allowed_scenarios for item in definitions):
                raise ValueError(f"scenarioId {scenario!r} is not valid for any allowed target")
        if self.abnormal_probability > 0:
            for definition in definitions:
                abnormal = definition.allowed_scenarios.intersection(
                    self.allowed_scenarios
                ).difference({definition.recovery_scenario})
                if not abnormal:
                    raise ValueError(
                        f"allowed target {definition.device_id} has no abnormal scenario"
                    )
        return self

    def definition(self, target: str) -> DeviceDefinition:
        if self.registered_sensors is None:
            return DeviceSemanticRegistry().by_device(target)
        matches = [
            sensor
            for sensor in self.registered_sensors
            if target in {sensor.definition().target_id, sensor.device_id}
        ]
        if len(matches) != 1 or not matches[0].desired_enabled or matches[0].archived:
            raise ValueError(f"target is unavailable or ambiguous in frozen registration: {target}")
        return matches[0].definition()


class CreateRunRequest(RandomRunConfiguration):
    registered_sensors: None = Field(default=None, alias="registeredSensors")
    name: str = Field(min_length=1, max_length=200)
    created_by: str = Field(alias="createdBy", min_length=1, max_length=200)

    @model_validator(mode="after")
    def snapshot_is_server_owned(self) -> Self:
        if self.registered_sensors is not None:
            raise ValueError("registeredSensors is server-owned; do not supply a snapshot")
        return self

    def persisted_configuration(self) -> RandomRunConfiguration:
        """Return only scheduler configuration, excluding run metadata."""

        return RandomRunConfiguration.model_validate(
            self.model_dump(
                include={
                    "duration_seconds",
                    "abnormal_probability",
                    "seed",
                    "allowed_targets",
                    "allowed_scenarios",
                    "scenario_duration",
                    "selection_interval_seconds",
                }
            )
        )


class ManualActivationRequest(ApiModel):
    target_id: str = Field(alias="targetId", min_length=1)
    scenario_id: str = Field(alias="scenarioId", min_length=1)
    duration_seconds: float | None = Field(
        default=None,
        alias="durationSeconds",
        gt=0,
        le=86_400,
        allow_inf_nan=False,
    )
    issued_by: str = Field(alias="issuedBy", min_length=1, max_length=200)


@dataclass(frozen=True, slots=True)
class RandomSelection:
    """One deterministic scheduler decision, excluding IDs and wall time."""

    target_id: str
    scenario_id: str
    duration_seconds: int


class RandomScenarioSelector:
    """Run-local deterministic selector with stable candidate ordering."""

    def __init__(
        self,
        configuration: RandomRunConfiguration,
        *,
        generator: random.Random | None = None,
    ) -> None:
        self.configuration = configuration
        self._random = generator or random.Random(configuration.seed)
        self._targets = sorted(configuration.allowed_targets)
        self._definitions = {target: configuration.definition(target) for target in self._targets}
        allowed = frozenset(configuration.allowed_scenarios)
        self._abnormal = {
            target: sorted(
                definition.allowed_scenarios.intersection(allowed).difference(
                    {definition.recovery_scenario}
                )
            )
            for target, definition in self._definitions.items()
        }

    def next(self) -> RandomSelection:
        """Draw target, normal/abnormal decision, scenario, then duration."""

        target = self._random.choice(self._targets)
        definition = self._definitions[target]
        candidates = self._abnormal[target]
        abnormal = (
            bool(candidates) and self._random.random() < self.configuration.abnormal_probability
        )
        scenario = self._random.choice(candidates) if abnormal else definition.recovery_scenario
        duration = self._random.randint(
            self.configuration.scenario_duration.minimum_seconds,
            self.configuration.scenario_duration.maximum_seconds,
        )
        return RandomSelection(target, scenario, duration)


@dataclass(frozen=True, slots=True)
class RunCursor:
    created_at: datetime
    run_id: str


@dataclass(frozen=True, slots=True)
class ActivationCursor:
    requested_at: datetime
    activation_id: str
