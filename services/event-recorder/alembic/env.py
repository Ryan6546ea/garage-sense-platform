"""Alembic environment for async PostgreSQL migrations."""

import asyncio
import platform
import subprocess
import sys
from logging.config import fileConfig
from pathlib import Path

from alembic import context
from event_recorder.config import RecorderSettings
from event_recorder.models import Base
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata

if platform.system() == "Windows":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


def do_run_migrations(connection: Connection) -> None:
    """Configure Alembic and run one transactional migration pass."""

    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_offline() -> None:
    """Render SQL without opening a database connection."""

    url = RecorderSettings().database_url.render_as_string(hide_password=False)
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    """Run migrations using the configured asyncpg connection."""

    connectable = create_async_engine(
        RecorderSettings().database_url,
        pool_pre_ping=True,
    )
    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await connectable.dispose()


def run_migrations_online() -> None:
    """Bridge Alembic's synchronous entry point to async SQLAlchemy."""

    if platform.system() == "Windows":
        if config.config_file_name is None:
            raise RuntimeError("Windows container migration requires alembic.ini")
        project_root = Path(config.config_file_name).resolve().parent
        container_args = sys.argv[1:]
        subprocess.run(
            [
                "docker",
                "compose",
                "--env-file",
                ".env",
                "run",
                "--rm",
                "event-recorder",
                "alembic",
                "-c",
                "/app/alembic.ini",
                *container_args,
            ],
            cwd=project_root,
            check=True,
        )
        return
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
