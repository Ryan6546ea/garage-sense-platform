"""Local registration API; no external authentication or vendor integration."""

import asyncio
from collections.abc import Awaitable
from typing import Annotated, Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import ValidationError
from sim_core.catalog import (
    PARAMETER_MODELS,
    AreaCreate,
    CatalogSnapshot,
    DeviceCreate,
    GarageCreate,
    RuntimeReport,
    SensorCreate,
    SensorPatch,
    VersionRequest,
)
from sqlalchemy.exc import IntegrityError

from control_service.catalog_repository import (
    CatalogConflict,
    CatalogNotFound,
    CatalogRepository,
    install_snapshot,
)


class CatalogService:
    def __init__(self, repository: CatalogRepository) -> None:
        self.repository = repository
        self._refresh_lock = asyncio.Lock()

    async def refresh(self) -> CatalogSnapshot:
        async with self._refresh_lock:
            snapshot = await self.repository.snapshot()
            install_snapshot(snapshot)
            return snapshot

    async def mutate(self, operation: Awaitable[Any]) -> Any:
        try:
            result = await operation
        except CatalogNotFound as exc:
            raise HTTPException(404, str(exc)) from exc
        except CatalogConflict as exc:
            raise HTTPException(409, str(exc)) from exc
        except IntegrityError as exc:
            raise HTTPException(409, "duplicate identity or invalid parent reference") from exc
        except ValidationError as exc:
            raise HTTPException(422, str(exc)) from exc
        await self.refresh()
        return result


def create_catalog_router(service: CatalogService) -> APIRouter:
    router = APIRouter(prefix="/api/v1", tags=["registration"])
    repository = service.repository

    @router.get("/sensor-types")
    async def sensor_types() -> dict[str, Any]:
        return {
            "items": [
                {"sensorType": kind, "parametersSchema": model.model_json_schema(by_alias=True)}
                for kind, model in PARAMETER_MODELS.items()
            ]
        }

    @router.post("/garages", status_code=201)
    async def create_garage(request: GarageCreate) -> Any:
        return await service.mutate(repository.create_parent(request))

    @router.get("/garages")
    async def garages() -> dict[str, Any]:
        snapshot = await service.refresh()
        return {"items": [item.model_dump(by_alias=True) for item in snapshot.garages]}

    @router.post("/garages/{garage_id}/areas", status_code=201)
    async def create_area(garage_id: str, request: AreaCreate) -> Any:
        return await service.mutate(repository.create_parent(request, garage_id))

    @router.get("/garages/{garage_id}/areas")
    async def areas(garage_id: str) -> dict[str, Any]:
        return {"items": await repository.parents("areas", garage_id)}

    @router.post("/garages/{garage_id}/devices", status_code=201)
    async def create_device(garage_id: str, request: DeviceCreate) -> Any:
        return await service.mutate(repository.create_parent(request, garage_id))

    @router.get("/garages/{garage_id}/devices")
    async def devices(garage_id: str) -> dict[str, Any]:
        return {"items": await repository.parents("devices", garage_id)}

    @router.post("/garages/{garage_id}/sensors", status_code=201)
    async def create_sensor(garage_id: str, request: SensorCreate) -> Any:
        return await service.mutate(repository.create_sensor(garage_id, request))

    @router.get("/garages/{garage_id}/sensors")
    async def sensors(
        garage_id: str,
        sensor_type: Annotated[str | None, Query(alias="sensorType")] = None,
        archived: bool | None = None,
        after: str = "",
        limit: Annotated[int, Query(ge=1, le=200)] = 100,
    ) -> dict[str, Any]:
        snapshot = await service.refresh()
        selected = [
            item
            for item in snapshot.sensors
            if item.garage_id == garage_id
            and item.sensor_id > after
            and (sensor_type is None or item.sensor_type == sensor_type)
            and (archived is None or item.archived == archived)
        ]
        page = selected[:limit]
        return {
            "items": [
                item.model_dump(by_alias=True) | {"targetId": item.definition().target_id}
                for item in page
            ],
            "nextCursor": page[-1].sensor_id if len(selected) > limit else None,
        }

    @router.get("/sensors/{sensor_id}")
    async def sensor(sensor_id: str) -> dict[str, Any]:
        snapshot = await service.refresh()
        item = next((item for item in snapshot.sensors if item.sensor_id == sensor_id), None)
        if item is None:
            raise HTTPException(404, "sensor does not exist")
        runtime = await repository.runtime()
        return item.model_dump(by_alias=True) | {
            "targetId": item.definition().target_id,
            "desiredRevision": snapshot.revision,
            "runtime": runtime,
            "applied": len(runtime) == 2
            and all(
                row["fresh"]
                and row["error"] is None
                and row["appliedRevision"] >= snapshot.revision
                for row in runtime
            ),
        }

    @router.patch("/sensors/{sensor_id}")
    async def patch_sensor(sensor_id: str, request: SensorPatch) -> Any:
        return await service.mutate(
            repository.change_sensor(sensor_id, request.config_version, "configure", request)
        )

    @router.post("/sensors/{sensor_id}/enable", status_code=202)
    async def enable(sensor_id: str, request: VersionRequest) -> Any:
        return await service.mutate(
            repository.change_sensor(sensor_id, request.config_version, "enable")
        )

    @router.post("/sensors/{sensor_id}/disable", status_code=202)
    async def disable(sensor_id: str, request: VersionRequest) -> Any:
        return await service.mutate(
            repository.change_sensor(sensor_id, request.config_version, "disable")
        )

    @router.post("/sensors/{sensor_id}/archive", status_code=202)
    async def archive(sensor_id: str, request: VersionRequest) -> Any:
        return await service.mutate(
            repository.change_sensor(sensor_id, request.config_version, "archive")
        )

    @router.get("/sensors/{sensor_id}/registration-events")
    async def audit(
        sensor_id: str, after: int = 0, limit: Annotated[int, Query(ge=1, le=200)] = 100
    ) -> dict[str, Any]:
        return {"items": await repository.audit(sensor_id, after, limit)}

    @router.get("/internal/registry/snapshot", response_model=CatalogSnapshot)
    async def snapshot() -> CatalogSnapshot:
        return await service.refresh()

    @router.get("/internal/registry/revision")
    async def revision() -> dict[str, int]:
        return {"revision": await repository.revision()}

    @router.get("/internal/registry/runtime")
    async def runtime() -> dict[str, Any]:
        return {"items": await repository.runtime()}

    @router.post("/internal/registry/runtime", status_code=204)
    async def report(request: RuntimeReport) -> None:
        await repository.report(request)

    return router
