"""Strict environment configuration for the control service."""
from pathlib import Path
from typing import Literal
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import URL

class ControlSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix='CONTROL_', env_file='.env', case_sensitive=False, extra='ignore', populate_by_name=True, str_strip_whitespace=True)
    environment: Literal['test', 'development', 'production']
    api_host: str = Field(min_length=1)
    api_port: int = Field(ge=1, le=65535)
    database_host: str = Field(min_length=1)
    database_port: int = Field(ge=1, le=65535)
    database_name: str = Field(min_length=1)
    database_user: str = Field(min_length=1)
    database_password: str = Field(min_length=1)
    export_storage_root: Path
    export_retention_seconds: int = Field(ge=1, le=31536000)
    export_max_items: int = Field(ge=1, le=10000000)
    export_worker_concurrency: int = Field(ge=1, le=8)
    export_progress_batch_size: int = Field(ge=1, le=100000)
    export_zip_compression_level: int = Field(ge=0, le=9)
    export_max_file_size_bytes: int = Field(ge=1024)
    export_cleanup_interval_seconds: float = Field(gt=0, le=86400)
    export_tmp_max_age_seconds: float = Field(gt=0, le=604800)
    export_worker_poll_seconds: float = Field(default=0.2, gt=0, le=60)
    export_shutdown_grace_seconds: float = Field(default=5, gt=0, le=300)
    export_generator_version: str = Field(default='m6.2', min_length=1, max_length=100)
    export_max_failure_payload_bytes: int = Field(default=65536, ge=0)

    @property
    def database_url(self) -> URL:
        """Build an async URL without string-interpolating credentials."""
        return URL.create('postgresql+asyncpg', username=self.database_user, password=self.database_password, host=self.database_host, port=self.database_port, database=self.database_name)
