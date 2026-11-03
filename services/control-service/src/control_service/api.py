"""Standalone business API without simulator clients or simulation workers."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from sim_core.identity_api import AuthSettings
from sim_core.identity_guard import install_identity
from sim_core.identity_store import IdentityStore

from control_service.catalog_api import CatalogService, create_catalog_router
from control_service.catalog_repository import CatalogRepository
from control_service.config import ControlSettings
from control_service.export_api import create_export_router
from control_service.export_orchestrator import ExportOrchestrator
from control_service.export_repository import ExportRepository
from control_service.export_storage import ExportStorage
from control_service.repository import Database


def create_app(settings: ControlSettings) -> FastAPI:
    database = Database(settings)
    catalog = CatalogService(CatalogRepository(database.sessions))
    repository = ExportRepository(database.sessions)
    exports = ExportOrchestrator(
        settings,
        repository,
        ExportStorage(settings.export_storage_root, max_file_size=settings.export_max_file_size_bytes),
    )
    auth = AuthSettings()
    if auth.enabled and len(auth.service_token) < 32:
        raise ValueError("AUTH_SERVICE_TOKEN must contain at least 32 characters")

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        try:
            await catalog.refresh()
            await exports.startup()
            try:
                yield
            finally:
                await exports.shutdown()
        finally:
            await database.dispose()

    app = FastAPI(title="Garage Backend", version="0.1.0", lifespan=lifespan)
    install_identity(app, IdentityStore(database.sessions), accounts=True, settings=auth)
    app.include_router(create_catalog_router(catalog))
    app.include_router(create_export_router(repository, exports))

    @app.get("/health/live")
    async def live() -> dict[str, str]:
        return {"status": "live"}

    @app.get("/health/ready")
    async def ready() -> dict[str, str]:
        if not await exports.ready():
            raise HTTPException(503, "Database or export workers are not ready")
        return {"status": "ready"}

    return app
