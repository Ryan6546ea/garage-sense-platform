"""Server-side authorization for existing simulator API boundaries.

Global aggregate endpoints require access to every garage. Object endpoints resolve
their persisted scope, never trusting caller-supplied garage IDs to authorize objects.
"""

import json
import secrets
from typing import Any
from urllib.parse import urlsplit

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from sqlalchemy import text
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import JSONResponse, Response

from sim_core.identity_api import COOKIE, AuthSettings, create_identity_router
from sim_core.identity_store import IdentityStore, User, audit
from sim_core.registry import CAMERA_DEFINITIONS, registered_devices, registered_garages

READ_METHODS = {"GET", "HEAD", "OPTIONS"}
PUBLIC = {"/api/v1/auth/login", "/api/v1/auth/register"}


def target_garages(target: str) -> set[str]:
    for item in registered_devices():
        if target in {item.device_id, item.sensor_id, item.target_id}:
            return {item.garage_id}
    for camera in CAMERA_DEFINITIONS:
        if target == camera.camera_id:
            return {camera.garage_id}
    if target in registered_garages():
        return {target}
    # Garage/global controller targets must not accidentally be treated as single-device.
    return set(registered_garages())


class IdentityGuard(BaseHTTPMiddleware):
    def __init__(self, app: Any, *, store: IdentityStore, settings: AuthSettings) -> None:
        super().__init__(app)
        self.store = store
        self.settings = settings

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        path = request.url.path.rstrip("/")
        user: User | None = None
        garages: set[str] = set()
        mutating = request.method not in READ_METHODS
        try:
            if path in {"/health/live", "/health/ready"}:
                return await call_next(request)
            if path.startswith("/api/v1/internal/"):
                supplied = request.headers.get("X-Service-Token", "")
                if not self.settings.service_token or not secrets.compare_digest(
                    supplied, self.settings.service_token
                ):
                    raise HTTPException(403, "需要服务身份")
                return await call_next(request)
            if mutating:
                chunks: list[bytes] = []
                size = 0
                async for chunk in request.stream():
                    size += len(chunk)
                    if size > 2 * 1024 * 1024:
                        raise HTTPException(413, "请求过大")
                    chunks.append(chunk)
                request._body = b"".join(chunks)
                origin = request.headers.get("origin")
                if origin and urlsplit(origin).netloc != request.headers.get("host"):
                    raise HTTPException(403, "跨站请求被拒绝")
                if request.headers.get("sec-fetch-site") == "cross-site":
                    raise HTTPException(403, "跨站请求被拒绝")
                if path in PUBLIC and request.headers.get("content-type", "").split(";")[0] != (
                    "application/json"
                ):
                    raise HTTPException(415, "需要 application/json")
            if path in PUBLIC and request.method == "POST":
                return await self._response(request, call_next)
            user, session = await self.store.authenticate(request.cookies.get(COOKIE))
            request.state.identity = user
            request.state.identity_session = session
            if mutating and not secrets.compare_digest(
                request.headers.get("X-CSRF-Token", ""), session.csrf_token
            ):
                raise HTTPException(403, "请求验证已失效, 请刷新页面")
            data: dict[str, Any] = {}
            if mutating:
                body = await request.body()
                if len(body) > 2 * 1024 * 1024:
                    raise HTTPException(413, "请求过大")
                if body:
                    try:
                        parsed = json.loads(body)
                    except (ValueError, UnicodeDecodeError) as exc:
                        raise HTTPException(422, "无效 JSON") from exc
                    if isinstance(parsed, dict):
                        data = parsed
            identity_path = path.startswith(
                ("/api/v1/auth/", "/api/v1/users", "/api/v1/audit-logs")
            )
            if not identity_path:
                garages = await self.authorize(user, path, request, data)
                if mutating:
                    # Bind legacy actor fields to the authenticated identity.
                    for key in ("issuedBy", "createdBy", "issued_by", "created_by", "operator"):
                        if key in data:
                            data[key] = user.username
                    request._body = json.dumps(data).encode() if data else await request.body()
                    async with self.store.sessions.begin() as db:
                        audit(
                            db,
                            user,
                            f"{request.method} requested",
                            path,
                            garages=sorted(garages),
                            outcome="accepted",
                        )
            response = await self._response(request, call_next)
            if mutating and not identity_path:
                async with self.store.sessions.begin() as db:
                    audit(
                        db,
                        user,
                        request.method,
                        path,
                        garages=sorted(garages),
                        outcome="success" if response.status_code < 400 else "failed",
                        details={"statusCode": response.status_code},
                    )
            return response
        except HTTPException as exc:
            if user and mutating:
                async with self.store.sessions.begin() as db:
                    audit(
                        db,
                        user,
                        request.method,
                        path,
                        garages=sorted(garages),
                        outcome="denied",
                        details={"statusCode": exc.status_code},
                    )
            return JSONResponse(
                {"detail": exc.detail},
                status_code=exc.status_code,
                headers={"Cache-Control": "no-store", **(exc.headers or {})},
            )

    async def _response(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        response = await call_next(request)
        response.headers["Cache-Control"] = "no-store"
        return response

    async def row(self, table: str, key: str, value: str) -> dict[str, Any]:
        # Table/key names come only from the constants below; values remain bound parameters.
        async with self.store.sessions() as session:
            result = (
                (
                    await session.execute(
                        text(f"SELECT * FROM {table} WHERE CAST({key} AS TEXT) = :id"),
                        {"id": value},
                    )
                )
                .mappings()
                .first()
            )
            if result is None:
                raise HTTPException(404, "资源不存在")
            return dict(result)

    async def authorize(
        self, user: User, path: str, request: Request, data: dict[str, Any]
    ) -> set[str]:
        mutating = request.method not in READ_METHODS
        preview = path.startswith("/api/v1/cameras/") and path.endswith("/preview-url")
        catalog_write = path.startswith(("/api/v1/garages", "/api/v1/sensors")) and mutating
        if catalog_write and user.role != "admin":
            raise HTTPException(403, "配置管理需要管理员权限")
        if mutating and not preview and user.role not in {"operator", "admin"}:
            raise HTTPException(403, "没有操作权限")
        # Existing orchestration audit endpoints contain all operators for their resource.
        is_audit = (
            path.endswith(("/events", "/registration-events", "/ingest-failures"))
            and path != "/api/v1/telemetry/events"
        )
        if is_audit and user.log_scope == "own":
            raise HTTPException(403, "请使用本人操作日志; 此接口需要车库或全系统日志权限")
        if (
            path.startswith("/api/v1/exports")
            and (
                data.get("includeRunAudit")
                or data.get("include_run_audit")
                or data.get("includeIngestFailures")
                or data.get("include_ingest_failures")
                or data.get("exportType", data.get("export_type", "telemetry")) != "telemetry"
            )
            and (not user.log_export or user.log_scope != "all")
        ):
            raise HTTPException(403, "此导出需要全系统日志查看及导出权限")
        garages = await self.scope(path, request, data)
        if user.role != "admin" and (not garages or not garages.issubset(user.garage_ids)):
            raise HTTPException(403, "没有所需车库权限; 跨车库汇总需要全部相关车库授权")
        if path.startswith("/api/v1/exports/"):
            export_id = path.split("/")[4]
            row = await self.row("export_jobs", "id", export_id)
            if (
                row["include_run_audit"]
                or row["include_ingest_failures"]
                or (row["export_type"] != "telemetry")
            ) and (user.log_scope != "all" or (path.endswith("/download") and not user.log_export)):
                raise HTTPException(403, "没有该日志导出文件的访问权限")
        return garages

    async def scope(self, path: str, request: Request, data: dict[str, Any]) -> set[str]:
        parts = path.split("/")[3:]
        all_garages = set(registered_garages())
        if not parts:
            return all_garages
        resource = parts[0]
        identifier = parts[1] if len(parts) > 1 else ""
        if resource == "telemetry" and identifier == "events":
            if len(parts) > 2:
                row = await self.row("telemetry_all_events", "message_id", parts[2])
                return {row["garage_id"]}
            garage = request.query_params.get("garageId")
            return {garage} if garage else all_garages
        if resource == "garages" and identifier:
            return {identifier}
        if resource == "sensors" and identifier:
            row = await self.row("sensor_instances", "sensor_id", identifier)
            return {row["garage_id"]}
        if resource == "cameras" and identifier:
            return target_garages(identifier)
        if resource == "scenarios" and identifier == "activate":
            return target_garages(str(data.get("targetId", "")))
        if resource == "scenarios" and identifier == "activations":
            if len(parts) > 2:
                row = await self.row("simulation_scenario_activations", "id", parts[2])
                return target_garages(row["target_id"])
            target = request.query_params.get("targetId")
            return target_garages(target) if target else all_garages
        if resource == "commands":
            if identifier and identifier != "ingest-failures":
                row = await self.row("virtual_commands", "command_id", identifier)
                return {row["garage_id"]}
            if request.method == "POST":
                return {str(data.get("garageId", ""))} | target_garages(
                    str(data.get("targetDeviceId", ""))
                )
        # Random runs restore all devices and replay suspends the shared simulator.
        # These existing global operations deliberately require all garages, even if
        # the submitted source selection names a single garage.
        return all_garages


def install_identity(
    app: FastAPI,
    store: IdentityStore,
    *,
    accounts: bool = False,
    settings: AuthSettings | None = None,
) -> None:
    config = settings or AuthSettings()
    if not config.enabled:
        return
    app.state.identity_store = store

    @app.exception_handler(RequestValidationError)
    async def validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
        # Pydantic's default error includes raw input (including passwords).
        return JSONResponse(
            status_code=422,
            content={
                "detail": [
                    {"loc": list(error["loc"]), "msg": error["msg"], "type": error["type"]}
                    for error in exc.errors()
                ]
            },
        )

    app.add_middleware(IdentityGuard, store=store, settings=config)
    if accounts:
        app.include_router(create_identity_router(store, config))
