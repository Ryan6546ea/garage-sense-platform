"""Fail-fast telemetry-consumer environment configuration."""

from pathlib import Path
from typing import Literal, Self

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import URL


class ConsumerSettings(BaseSettings):
    """Runtime configuration loaded only from TELEMETRY_CONSUMER_* variables."""

    model_config = SettingsConfigDict(
        env_prefix="TELEMETRY_CONSUMER_",
        env_file=".env",
        case_sensitive=False,
        extra="ignore",
        populate_by_name=True,
        str_strip_whitespace=True,
    )

    environment: Literal["test", "development", "production"]
    registry_base_url: str | None = Field(default=None, pattern=r"^https?://[^/]+(?:/.*)?$")

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
    mqtt_client_id: str = Field(min_length=1, max_length=200)
    mqtt_topic_prefix: Literal["garage/+/telemetry"]
    mqtt_keepalive_seconds: int = Field(ge=5, le=65535)
    mqtt_reconnect_initial_seconds: float = Field(gt=0, le=60)
    mqtt_reconnect_max_seconds: float = Field(gt=0, le=300)
    schema_directory: Path
    max_message_bytes: int = Field(ge=1024, le=16 * 1024 * 1024)
    max_failure_payload_bytes: int = Field(ge=256, le=16 * 1024 * 1024)
    queue_capacity: int = Field(ge=1, le=1_000_000)
    processing_timeout_seconds: float = Field(gt=0, le=300)
    shutdown_drain_timeout_seconds: float = Field(gt=0, le=300)

    @property
    def database_url(self) -> URL:
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
        if self.mqtt_reconnect_max_seconds < self.mqtt_reconnect_initial_seconds:
            raise ValueError(
                "mqtt_reconnect_max_seconds must be greater than or equal to "
                "mqtt_reconnect_initial_seconds"
            )
        if not self.schema_directory.is_dir():
            raise ValueError(f"schema_directory does not exist: {self.schema_directory}")
        return self
