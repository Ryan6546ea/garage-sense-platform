"""Health and Prometheus endpoints with managed consumer lifecycle."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Response, status
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from sim_core import DeviceSemanticRegistry, TelemetryContractRegistry
from sim_core.catalog_client import CatalogClient

from telemetry_consumer.config import ConsumerSettings
from telemetry_consumer.consumer import TelemetryConsumer
from telemetry_consumer.repository import Database, TelemetryRepository


def create_app(settings: ConsumerSettings) -> FastAPI:
    """Build the typed telemetry consumer service."""

    database = Database(settings)
    repository = TelemetryRepository(
        database.sessions,
        max_failure_payload_bytes=settings.max_failure_payload_bytes,
    )
    consumer = TelemetryConsumer(
        settings,
        repository,
        TelemetryContractRegistry(settings.schema_directory),
        DeviceSemanticRegistry(),
    )

    catalog = (
        CatalogClient(settings.registry_base_url, "consumer")
        if settings.registry_base_url
        else None
    )

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        if catalog is not None:
            await catalog.initialise()
            await catalog.start()
        await consumer.start()
        try:
            yield
        finally:
            await consumer.stop()
            if catalog is not None:
                await catalog.stop()
            await database.dispose()

    app = FastAPI(title="Telemetry Consumer", version="0.1.0", lifespan=lifespan)
    app.state.consumer = consumer

    @app.get("/health/live")
    async def live() -> dict[str, str]:
        return {"status": "live"}

    @app.get("/health/ready")
    async def ready() -> dict[str, str]:
        database_ready = await repository.ping()
        if not (
            database_ready
            and (catalog is None or catalog.healthy)
            and consumer.connected
            and consumer.task_alive
            and consumer.db_write_healthy
        ):
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="telemetry consumer PostgreSQL, MQTT, worker, or writer is not ready",
            )
        return {"status": "ready"}

    @app.get("/metrics", include_in_schema=False)
    async def metrics() -> Response:
        return Response(
            content=generate_latest(consumer.metrics.registry),
            media_type=CONTENT_TYPE_LATEST,
        )

    return app
