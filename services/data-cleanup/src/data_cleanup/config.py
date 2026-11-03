"""Strict TOML retention policy; credentials are environment-only."""

import tomllib
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import URL


class CleanupPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    enabled: bool = True
    interval_seconds: int = Field(default=86400, ge=1, le=31536000)
    sensor_retention_days: int = Field(default=365, ge=1, le=36500)
    ingest_failure_retention_days: int | None = Field(default=None, ge=1, le=36500)
    batch_size: int = Field(default=1000, ge=1, le=10000)
    max_batches_per_table: int = Field(default=100, ge=1, le=10000)
    statement_timeout_seconds: int = Field(default=30, ge=1, le=300)


def load_policy(path: Path) -> CleanupPolicy:
    with path.open("rb") as source:
        return CleanupPolicy.model_validate(tomllib.load(source))


class CleanupSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DATA_CLEANUP_", extra="ignore")

    config_path: Path = Path("config/data-cleanup.toml")
    database_host: str = Field(min_length=1)
    database_port: int = Field(ge=1, le=65535)
    database_name: str = Field(min_length=1)
    database_user: str = Field(min_length=1)
    database_password: SecretStr = Field(min_length=1)

    @property
    def database_url(self) -> URL:
        return URL.create(
            "postgresql+asyncpg",
            username=self.database_user,
            password=self.database_password.get_secret_value(),
            host=self.database_host,
            port=self.database_port,
            database=self.database_name,
        )
