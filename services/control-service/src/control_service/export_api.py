"""HTTP API for asynchronous telemetry and run-audit exports."""

import base64
import json
from datetime import UTC, datetime
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Query, Request, Response, status
from fastapi.responses import FileResponse
from pydantic import AwareDatetime, Field

from control_service.domain import ApiModel
from control_service.export_domain import CreateExportRequest
from control_service.export_models import (
    ExportFormat,
    ExportJob,
    ExportStatus,
    ExportType,
)
from control_service.export_orchestrator import ExportOrchestrator
from control_service.export_repository import (
    ExportConflictError,
    ExportRepository,
    ExportValidationError,
)
from control_service.export_storage import ExportStorageError


class ExportItemResponse(ApiModel):
    export_id: UUID = Field(alias="exportId")
    name: str
    status: str
    progress: float
    item_count: int = Field(alias="itemCount")
    processed_count: int = Field(alias="processedCount")
    failed_count: int = Field(alias="failedCount")
    format: str
    export_type: str = Field(alias="exportType")
    file_name: str | None = Field(alias="fileName")
    media_type: str | None = Field(alias="mediaType")
    size_bytes: int | None = Field(alias="sizeBytes")
    sha256: str | None
    created_at: datetime = Field(alias="createdAt")
    started_at: datetime | None = Field(alias="startedAt")
    finished_at: datetime | None = Field(alias="finishedAt")
    expires_at: datetime = Field(alias="expiresAt")
    failure_reason: str | None = Field(alias="failureReason")
    download_available: bool = Field(alias="downloadAvailable")


class ExportPage(ApiModel):
    items: list[ExportItemResponse]
    next_cursor: str | None = Field(alias="nextCursor")


class ExportEventResponse(ApiModel):
    event_type: str = Field(alias="eventType")
    occurred_at: datetime = Field(alias="occurredAt")
    sequence_number: int = Field(alias="sequenceNumber")
    payload: dict[str, Any]


class ExportEventPage(ApiModel):
    items: list[ExportEventResponse]
    next_cursor: str | None = Field(alias="nextCursor")


def export_response(job: ExportJob, *, now: datetime | None = None) -> ExportItemResponse:
    current = now or datetime.now(UTC)
    return ExportItemResponse(
        export_id=job.id,
        name=job.name,
        status=job.status,
        progress=(job.processed_count / job.item_count if job.item_count else 0),
        item_count=job.item_count,
        processed_count=job.processed_count,
        failed_count=job.failed_count,
        format=job.format,
        export_type=job.export_type,
        file_name=job.output_filename,
        media_type=job.output_media_type,
        size_bytes=job.output_size_bytes,
        sha256=job.output_sha256,
        created_at=job.created_at,
        started_at=job.started_at,
        finished_at=job.finished_at,
        expires_at=job.expires_at,
        failure_reason=job.failure_reason,
        download_available=(
            job.status == ExportStatus.COMPLETED.value and job.expires_at > current
        ),
    )


def encode_job_cursor(created_at: datetime, job_id: UUID) -> str:
    raw = json.dumps([created_at.isoformat(), str(job_id)], separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def decode_job_cursor(value: str) -> tuple[datetime, UUID]:
    try:
        padded = value + "=" * (-len(value) % 4)
        raw = json.loads(base64.urlsafe_b64decode(padded))
        created = datetime.fromisoformat(raw[0])
        identifier = UUID(raw[1])
        if created.utcoffset() is None:
            raise ValueError
    except (ValueError, TypeError, KeyError, IndexError, json.JSONDecodeError) as exc:
        raise ValueError("export cursor is invalid") from exc
    return created, identifier


def encode_event_cursor(sequence: int) -> str:
    return base64.urlsafe_b64encode(str(sequence).encode()).decode().rstrip("=")


def decode_event_cursor(value: str) -> int:
    try:
        result = int(base64.urlsafe_b64decode(value + "=" * (-len(value) % 4)))
        if result < 1:
            raise ValueError
        return result
    except (ValueError, TypeError) as exc:
        raise ValueError("export event cursor is invalid") from exc


def create_export_router(
    repository: ExportRepository,
    orchestrator: ExportOrchestrator,
) -> APIRouter:
    router = APIRouter(prefix="/api/v1/exports", tags=["exports"])

    @router.post("", response_model=ExportItemResponse, status_code=status.HTTP_202_ACCEPTED)
    async def create_export(request: CreateExportRequest) -> ExportItemResponse:
        try:
            job = await repository.create(
                request,
                now=orchestrator._clock(),
                retention_seconds=orchestrator.settings.export_retention_seconds,
                max_items=orchestrator.settings.export_max_items,
            )
        except ExportValidationError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return export_response(job, now=orchestrator._clock())

    @router.get("", response_model=ExportPage)
    async def list_exports(
        export_status: Annotated[ExportStatus | None, Query(alias="status")] = None,
        export_type: Annotated[ExportType | None, Query(alias="exportType")] = None,
        export_format: Annotated[ExportFormat | None, Query(alias="format")] = None,
        created_by: Annotated[str | None, Query(alias="createdBy")] = None,
        created_from: Annotated[AwareDatetime | None, Query(alias="createdFrom")] = None,
        created_to: Annotated[AwareDatetime | None, Query(alias="createdTo")] = None,
        limit: Annotated[int, Query(ge=1, le=1000)] = 100,
        cursor: str | None = None,
    ) -> ExportPage:
        if created_from and created_to and created_from > created_to:
            raise HTTPException(status_code=422, detail="createdFrom must not exceed createdTo")
        decoded = None
        if cursor:
            try:
                decoded = decode_job_cursor(cursor)
            except ValueError as exc:
                raise HTTPException(status_code=422, detail=str(exc)) from exc
        rows = await repository.list_jobs(
            limit=limit + 1,
            status=export_status.value if export_status else None,
            export_type=export_type.value if export_type else None,
            format=export_format.value if export_format else None,
            created_by=created_by,
            created_from=created_from,
            created_to=created_to,
            cursor=decoded,
        )
        page = rows[:limit]
        next_cursor = (
            encode_job_cursor(page[-1].created_at, page[-1].id) if len(rows) > limit else None
        )
        return ExportPage(
            items=[export_response(row, now=orchestrator._clock()) for row in page],
            next_cursor=next_cursor,
        )

    @router.get("/{export_id}", response_model=ExportItemResponse)
    async def get_export(export_id: UUID) -> ExportItemResponse:
        job = await repository.get(export_id)
        if job is None:
            raise HTTPException(status_code=404, detail="export not found")
        return export_response(job, now=orchestrator._clock())

    @router.get("/{export_id}/events", response_model=ExportEventPage)
    async def list_export_events(
        export_id: UUID,
        limit: Annotated[int, Query(ge=1, le=1000)] = 100,
        cursor: str | None = None,
    ) -> ExportEventPage:
        if await repository.get(export_id) is None:
            raise HTTPException(status_code=404, detail="export not found")
        after = None
        if cursor:
            try:
                after = decode_event_cursor(cursor)
            except ValueError as exc:
                raise HTTPException(status_code=422, detail=str(exc)) from exc
        rows = await repository.list_events(export_id, after_sequence=after, limit=limit + 1)
        page = rows[:limit]
        return ExportEventPage(
            items=[
                ExportEventResponse(
                    event_type=row.event_type,
                    occurred_at=row.occurred_at,
                    sequence_number=row.sequence_number,
                    payload=row.payload,
                )
                for row in page
            ],
            next_cursor=(
                encode_event_cursor(page[-1].sequence_number) if len(rows) > limit else None
            ),
        )

    @router.post("/{export_id}/cancel", response_model=ExportItemResponse)
    async def cancel_export(export_id: UUID) -> ExportItemResponse:
        try:
            job = await repository.request_cancel(export_id, now=orchestrator._clock())
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="export not found") from exc
        except ExportConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return export_response(job, now=orchestrator._clock())

    async def download_response(
        export_id: UUID,
        request: Request,
        if_none_match: str | None,
        *,
        head: bool,
    ) -> Response:
        job = await repository.get(export_id)
        if job is None:
            raise HTTPException(status_code=404, detail="export not found")
        now = orchestrator._clock()
        if job.status == ExportStatus.EXPIRED.value or job.expires_at <= now:
            raise HTTPException(status_code=410, detail="export has expired")
        if job.status != ExportStatus.COMPLETED.value:
            raise HTTPException(status_code=409, detail=f"{job.status} export is not downloadable")
        if not all(
            [
                job.storage_relative_path,
                job.output_filename,
                job.output_media_type,
                job.output_sha256,
                job.output_size_bytes is not None,
            ]
        ):
            raise HTTPException(status_code=500, detail="completed export metadata is incomplete")
        try:
            path = orchestrator.storage.resolve(str(job.storage_relative_path))
        except ExportStorageError as exc:
            await repository.record_event(
                job.id,
                "export_file_validation_failed",
                now=now,
                payload={"reason": "unsafe stored path"},
            )
            raise HTTPException(status_code=500, detail="stored export path is invalid") from exc
        if not path.is_file() or path.is_symlink():
            await repository.record_event(
                job.id,
                "export_file_validation_failed",
                now=now,
                payload={"reason": "file missing"},
            )
            raise HTTPException(status_code=500, detail="export file is missing")
        digest = orchestrator.storage.sha256(path)
        if digest != job.output_sha256:
            await repository.record_event(
                job.id,
                "export_file_validation_failed",
                now=now,
                payload={"reason": "sha256 mismatch"},
            )
            raise HTTPException(status_code=500, detail="export file checksum mismatch")
        etag = f'"{digest}"'
        headers = {
            "ETag": etag,
            "Content-Length": str(job.output_size_bytes),
            "Content-Disposition": f'attachment; filename="{job.output_filename}"',
            "Accept-Ranges": "none",
        }
        if if_none_match == etag:
            return Response(status_code=304, headers={"ETag": etag})
        if head or request.method == "HEAD":
            return Response(
                status_code=200,
                media_type=job.output_media_type,
                headers=headers,
            )
        return FileResponse(
            path,
            media_type=job.output_media_type,
            filename=job.output_filename,
            headers=headers,
        )

    @router.get("/{export_id}/download")
    async def download_export(
        export_id: UUID,
        request: Request,
        if_none_match: Annotated[str | None, Header(alias="If-None-Match")] = None,
    ) -> Response:
        return await download_response(export_id, request, if_none_match, head=False)

    @router.head("/{export_id}/download")
    async def head_export(
        export_id: UUID,
        request: Request,
        if_none_match: Annotated[str | None, Header(alias="If-None-Match")] = None,
    ) -> Response:
        return await download_response(export_id, request, if_none_match, head=True)

    return router
