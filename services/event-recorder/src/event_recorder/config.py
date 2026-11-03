"""Strict environment configuration for the event recorder."""

from pathlib import Path
from typing import Literal, Self

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import URL

TELEMETRY_SUBSCRIPTION = "garage/+/telemetry/+/+"


class RecorderSettings(BaseSettings):
    """All runtime configuration loaded from EVENT_RECORDER_* variables."""

    model_config = SettingsConfigDict(
        env_prefix="EVENT_RECORDER_",
        env_file=".env",
        case_sensitive=False,
        extra="ignore",
        populate_by_name=True,
        str_strip_whitespace=True,
    )

    environment: Literal["test", "development", "production"]
    api_host: str = Field(min_length=1)
    api_port: int = Field(ge=1, le=65535)
    database_host: str = Field(min_length=1)
    database_port: int = Field(ge=1, le=65535)
    database_name: str = Field(min_length=1)
    database_user: str = Field(min_length=1)
    database_password: str = Field(min_length=1)
    mqtt_host: str = Field(min_length=1)
    mqtt_port: int = Field(ge=1, le=65535)
    mqtt_username: str = Field(min_length=1)
    mqtt_password: str = Field(min_length=1)
    mqtt_client_id: str = Field(min_length=1)
    mqtt_topic: Literal["garage/+/telemetry/+/+"]
    mqtt_keepalive_seconds: int = Field(ge=5, le=65535)
    mqtt_reconnect_initial_seconds: float = Field(gt=0)
    mqtt_reconnect_max_seconds: float = Field(gt=0)
    schema_directory: Path
    max_message_bytes: int = Field(ge=1024, le=16 * 1024 * 1024)
    max_failure_payload_bytes: int = Field(ge=1024, le=16 * 1024 * 1024)

    @property
    def database_url(self) -> URL:
        """Build an async SQLAlchemy URL without interpolating credentials."""

        return URL.create(
            "postgresql+asyncpg",
            username=self.database_user,
            password=self.database_password,
            host=self.database_host,
            port=self.database_port,
            database=self.database_name,
        )

    @model_validator(mode="after")
    def validate_related_settings(self) -> Self:
        """Reject invalid backoff caps and missing schema files at startup."""

        if self.mqtt_reconnect_max_seconds < self.mqtt_reconnect_initial_seconds:
            raise ValueError(
                "mqtt_reconnect_max_seconds must be greater than or equal to "
                "mqtt_reconnect_initial_seconds"
            )
        if not self.schema_directory.is_dir():
            raise ValueError(f"schema_directory does not exist: {self.schema_directory}")
        return self
