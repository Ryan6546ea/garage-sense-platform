"""Bounded asynchronous export workers, atomic files, and retention cleanup."""

import asyncio
import base64
import logging
import zipfile
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TextIO
from uuid import UUID

from control_service.config import ControlSettings
from control_service.export_formats import (
    canonical_json,
    csv_writer,
    file_metadata,
    iso,
    model_dict,
    sanitize_detail,
    telemetry_csv_row,
    telemetry_json,
    write_jsonl,
)
from control_service.export_models import ExportFormat, ExportJob, ExportType
from control_service.export_repository import ExportRepository
from control_service.export_storage import ExportStorage

LOGGER = logging.getLogger(__name__)

README = """Garage simulation export

This data comes from an isolated test simulation environment and does not represent
real-device thresholds. sourceTime is the simulated device observation time;
receivedAt is the recorder ingestion time. When quality is not valid, measurement
values must not be treated as current valid readings. Replay messages may use a
different messageId, seq, and sourceTime from their originals. Condensation-risk
rules exist only for functional testing. HTTP Range is not implemented in M6.2.
"""


class ExportCancelled(RuntimeError):
    pass


class ExportOrchestrator:
    """Run a fixed worker pool without coupling individual failures to HTTP."""

    def __init__(
        self,
        settings: ControlSettings,
        repository: ExportRepository,
        storage: ExportStorage,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.settings = settings
        self.repository = repository
        self.storage = storage
        self._clock = clock or (lambda: datetime.now(UTC))
        self._stop = asyncio.Event()
        self._workers: list[asyncio.Task[None]] = []
        self._cleanup: asyncio.Task[None] | None = None
        self._running_jobs: set[UUID] = set()

    async def startup(self) -> None:
        self.storage.initialize()
        now = self._clock()
        await self.repository.interrupt_running(now=now)
        self.storage.clean_stale_parts(
            now=now, max_age_seconds=self.settings.export_tmp_max_age_seconds
        )
        self._stop.clear()
        self._workers = [
            asyncio.create_task(self._worker(index), name=f"export-worker-{index}")
            for index in range(self.settings.export_worker_concurrency)
        ]
        self._cleanup = asyncio.create_task(self._cleanup_loop(), name="export-cleanup")

    async def shutdown(self) -> None:
        self._stop.set()
        tasks = [*self._workers, *([self._cleanup] if self._cleanup else [])]
        if tasks:
            _done, pending = await asyncio.wait(
                tasks, timeout=self.settings.export_shutdown_grace_seconds
            )
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
        now = self._clock()
        for job_id in tuple(self._running_jobs):
            await self.repository.interrupt(job_id, now=now)
        self.storage.clean_stale_parts(now=now, max_age_seconds=0)
        self._workers.clear()
        self._cleanup = None

    async def ready(self) -> bool:
        tasks_alive = bool(self._workers) and all(not task.done() for task in self._workers)
        cleanup_alive = self._cleanup is not None and not self._cleanup.done()
        return (
            tasks_alive
            and cleanup_alive
            and self.storage.available()
            and await self.repository.ping()
        )

    async def _worker(self, index: int) -> None:
        while not self._stop.is_set():
            try:
                job = await self.repository.claim(now=self._clock())
                if job is None:
                    await asyncio.wait_for(
                        self._stop.wait(), timeout=self.settings.export_worker_poll_seconds
                    )
                    continue
                self._running_jobs.add(job.id)
                try:
                    await self._execute(job)
                finally:
                    self._running_jobs.discard(job.id)
            except TimeoutError:
                continue
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception("export worker failed", extra={"worker": index})
                await asyncio.sleep(min(self.settings.export_worker_poll_seconds, 1))

    async def _execute(self, job: ExportJob) -> None:
        final: Path | None = None
        temporary: Path | None = None
        work: Path | None = None
        try:
            suffix = job.format
            final, temporary = self.storage.final_paths(job.id, suffix)
            self.storage.remove(temporary)
            if job.export_type == ExportType.TELEMETRY.value:
                await self._write_telemetry(job, temporary)
            elif job.export_type == ExportType.RUN_AUDIT.value:
                await self._write_audit_export(job, temporary)
            else:
                work = self.storage.work_directory(job.id)
                await self._write_bundle(job, work, temporary)
            if await self.repository.cancellation_requested(job.id):
                raise ExportCancelled
            self.storage.enforce_size(temporary)
            temporary.replace(final)
            size = final.stat().st_size
            digest = self.storage.sha256(final)
            media = {
                "jsonl": "application/x-ndjson",
                "csv": "text/csv; charset=utf-8",
                "zip": "application/zip",
            }[job.format]
            await self.repository.complete(
                job.id,
                filename=final.name,
                media_type=media,
                size=size,
                sha256=digest,
                relative_path=final.name,
                now=self._clock(),
            )
        except ExportCancelled:
            if temporary:
                self.storage.remove(temporary)
            await self.repository.finalize_cancel(job.id, now=self._clock())
        except asyncio.CancelledError:
            if temporary:
                self.storage.remove(temporary)
            await self.repository.interrupt(job.id, now=self._clock())
            raise
        except Exception as exc:
            if temporary:
                self.storage.remove(temporary)
            await self.repository.fail(job.id, str(exc), now=self._clock())
            LOGGER.exception("export job failed", extra={"export_id": str(job.id)})
        finally:
            if work:
                self.storage.remove(work)

    async def _write_telemetry(self, job: ExportJob, path: Path) -> None:
        processed = 0
        if job.format == ExportFormat.JSONL.value:
            with path.open("w", encoding="utf-8", newline="\n") as stream:
                while processed < job.item_count:
                    rows = await self.repository.telemetry_batch(
                        job.id,
                        after_sequence=processed,
                        limit=self.settings.export_progress_batch_size,
                    )
                    if not rows:
                        break
                    for item, event in rows:
                        write_jsonl(stream, telemetry_json(item, event))
                    stream.flush()
                    processed = rows[-1][0].sequence_number
                    self.storage.enforce_size(path)
                    await self._progress(job, processed)
        else:
            with path.open("w", encoding="utf-8-sig", newline="") as stream:
                writer = csv_writer(stream)
                while processed < job.item_count:
                    rows = await self.repository.telemetry_batch(
                        job.id,
                        after_sequence=processed,
                        limit=self.settings.export_progress_batch_size,
                    )
                    if not rows:
                        break
                    for item, event in rows:
                        writer.writerow(telemetry_csv_row(item, event))
                    stream.flush()
                    processed = rows[-1][0].sequence_number
                    self.storage.enforce_size(path)
                    await self._progress(job, processed)
        if processed != job.item_count:
            raise RuntimeError(
                f"frozen manifest expected {job.item_count} items but read {processed}"
            )

    async def _progress(self, job: ExportJob, processed: int) -> None:
        if not await self.repository.update_progress(job.id, processed, now=self._clock()):
            raise ExportCancelled
        if self._stop.is_set():
            raise asyncio.CancelledError

    async def _write_audit_export(self, job: ExportJob, path: Path) -> None:
        if job.format == ExportFormat.JSONL.value:
            with path.open("w", encoding="utf-8", newline="\n") as stream:
                await self._write_audit_jsonl(job, stream)
            self.storage.enforce_size(path)
            await self._progress(job, job.item_count)
            return
        work = self.storage.work_directory(job.id)
        try:
            await self._write_audit_files(job, work / "audit")
            await self._archive(work, path)
        finally:
            self.storage.remove(work)
        await self._progress(job, job.item_count)

    async def _write_audit_jsonl(self, job: ExportJob, stream: TextIO) -> None:
        reference = job.source_reference_id
        if reference is None:
            raise RuntimeError("run audit requires a run reference")
        if job.source_type == "test_run":
            random_run = await self.repository.random_run(reference)
            write_jsonl(stream, {"recordType": "run", "record": model_dict(random_run)})
            async for activation in self.repository.activation_stream(
                reference, batch_size=self.settings.export_progress_batch_size
            ):
                write_jsonl(
                    stream,
                    {
                        "recordType": "scenario_activation",
                        "record": model_dict(activation),
                    },
                )
            async for run_event in self.repository.run_event_stream(
                reference, batch_size=self.settings.export_progress_batch_size
            ):
                write_jsonl(
                    stream,
                    {"recordType": "run_event", "record": model_dict(run_event)},
                )
        else:
            replay_run = await self.repository.replay_run(reference)
            write_jsonl(stream, {"recordType": "replay", "record": model_dict(replay_run)})
            async for replay_item in self.repository.replay_item_stream(
                reference, batch_size=self.settings.export_progress_batch_size
            ):
                write_jsonl(
                    stream,
                    {"recordType": "replay_item", "record": model_dict(replay_item)},
                )
            async for replay_event in self.repository.replay_event_stream(
                reference, batch_size=self.settings.export_progress_batch_size
            ):
                write_jsonl(
                    stream,
                    {"recordType": "replay_event", "record": model_dict(replay_event)},
                )
        write_jsonl(
            stream,
            {"recordType": "telemetry_manifest_summary", "itemCount": job.item_count},
        )

    async def _write_audit_files(self, job: ExportJob, directory: Path) -> None:
        directory.mkdir(parents=True, exist_ok=True)
        reference = job.source_reference_id
        if reference is None:
            raise RuntimeError("combined audit requires a run reference")
        if job.source_type == "test_run":
            random_run = await self.repository.random_run(reference)
            (directory / "run.json").write_text(
                canonical_json(model_dict(random_run)) + "\n",
                encoding="utf-8",
                newline="\n",
            )
            with (directory / "scenario-activations.jsonl").open(
                "w", encoding="utf-8", newline="\n"
            ) as stream:
                async for activation in self.repository.activation_stream(
                    reference,
                    batch_size=self.settings.export_progress_batch_size,
                ):
                    write_jsonl(stream, model_dict(activation))
            with (directory / "run-events.jsonl").open(
                "w", encoding="utf-8", newline="\n"
            ) as stream:
                async for run_event in self.repository.run_event_stream(
                    reference,
                    batch_size=self.settings.export_progress_batch_size,
                ):
                    write_jsonl(stream, model_dict(run_event))
        else:
            replay_run = await self.repository.replay_run(reference)
            (directory / "replay.json").write_text(
                canonical_json(model_dict(replay_run)) + "\n",
                encoding="utf-8",
                newline="\n",
            )
            with (directory / "replay-items.jsonl").open(
                "w", encoding="utf-8", newline="\n"
            ) as stream:
                async for replay_item in self.repository.replay_item_stream(
                    reference,
                    batch_size=self.settings.export_progress_batch_size,
                ):
                    write_jsonl(stream, model_dict(replay_item))
            with (directory / "replay-events.jsonl").open(
                "w", encoding="utf-8", newline="\n"
            ) as stream:
                async for replay_event in self.repository.replay_event_stream(
                    reference,
                    batch_size=self.settings.export_progress_batch_size,
                ):
                    write_jsonl(stream, model_dict(replay_event))

    async def _write_bundle(self, job: ExportJob, work: Path, output: Path) -> None:
        telemetry = work / "telemetry"
        telemetry.mkdir()
        jsonl = telemetry / "telemetry.jsonl"
        csv_path = telemetry / "telemetry.csv"
        processed = 0
        with (
            jsonl.open("w", encoding="utf-8", newline="\n") as json_stream,
            csv_path.open("w", encoding="utf-8-sig", newline="") as csv_stream,
        ):
            writer = csv_writer(csv_stream)
            while processed < job.item_count:
                rows = await self.repository.telemetry_batch(
                    job.id,
                    after_sequence=processed,
                    limit=self.settings.export_progress_batch_size,
                )
                if not rows:
                    break
                for item, event in rows:
                    write_jsonl(json_stream, telemetry_json(item, event))
                    writer.writerow(telemetry_csv_row(item, event))
                json_stream.flush()
                csv_stream.flush()
                processed = rows[-1][0].sequence_number
                self.storage.enforce_size(jsonl)
                self.storage.enforce_size(csv_path)
                await self._progress(job, processed)
        if processed != job.item_count:
            raise RuntimeError("frozen manifest changed while bundle was generated")
        if job.include_run_audit:
            await self._write_audit_files(job, work / "audit")
        failure_truncated = False
        if job.include_ingest_failures:
            failure_truncated = await self._write_failures(job, work / "quality")
        (work / "README.txt").write_text(README, encoding="utf-8", newline="\n")
        payload_files = sorted(
            [path for path in work.rglob("*") if path.is_file() and not path.is_symlink()],
            key=lambda path: path.relative_to(work).as_posix(),
        )
        metadata = [file_metadata(path, relative_to=work) for path in payload_files]
        manifest = {
            "exportId": str(job.id),
            "name": job.name,
            "exportType": job.export_type,
            "format": job.format,
            "source": job.selection["source"],
            "createdAt": iso(job.created_at),
            "generatedAt": iso(self._clock()),
            "expiresAt": iso(job.expires_at),
            "itemCount": job.item_count,
            "includeReplayOutputs": job.include_replay_outputs,
            "includeRunAudit": job.include_run_audit,
            "includeIngestFailures": job.include_ingest_failures,
            "ingestFailurePayloadTruncated": failure_truncated,
            "files": metadata,
            "schemaVersion": "2.0",
            "generatorVersion": self.settings.export_generator_version,
            "timeSemantics": {
                "timezone": "UTC",
                "sourceTime": "simulated device observation time",
                "receivedAt": "event-recorder ingestion time",
            },
            "filters": job.selection,
        }
        (work / "manifest.json").write_text(
            canonical_json(manifest) + "\n", encoding="utf-8", newline="\n"
        )
        checksum_files = sorted(
            [
                path
                for path in work.rglob("*")
                if path.is_file() and path.name != "checksums.sha256" and not path.is_symlink()
            ],
            key=lambda path: path.relative_to(work).as_posix(),
        )
        with (work / "checksums.sha256").open("w", encoding="ascii", newline="\n") as stream:
            for path in checksum_files:
                stream.write(f"{self.storage.sha256(path)}  {path.relative_to(work).as_posix()}\n")
        await self._archive(work, output)

    async def _write_failures(self, job: ExportJob, directory: Path) -> bool:
        directory.mkdir(parents=True, exist_ok=True)
        start = datetime.fromisoformat(job.selection["resolvedReceivedFrom"])
        end = datetime.fromisoformat(job.selection["resolvedReceivedTo"])
        after = 0
        truncated = False
        with (directory / "ingest-failures.jsonl").open(
            "w", encoding="utf-8", newline="\n"
        ) as stream:
            while True:
                rows: Sequence[Any] = await self.repository.failure_batch(
                    start=start,
                    end=end,
                    after_id=after,
                    limit=self.settings.export_progress_batch_size,
                )
                if not rows:
                    break
                for row in rows:
                    can_include = (
                        not row.payload_truncated
                        and row.original_payload_size
                        <= self.settings.export_max_failure_payload_bytes
                    )
                    raw = (
                        row.payload
                        if can_include
                        else row.payload[: self.settings.export_max_failure_payload_bytes]
                    )
                    truncated = truncated or not can_include
                    write_jsonl(
                        stream,
                        {
                            "id": row.id,
                            "topic": row.topic,
                            "receivedAt": iso(row.received_at),
                            "errorType": row.error_type,
                            "errorDetail": sanitize_detail(row.error_detail),
                            "originalPayloadSize": row.original_payload_size,
                            "payloadTruncated": row.payload_truncated or not can_include,
                            "payloadSha256": row.payload_sha256,
                            "rawPayloadBase64": base64.b64encode(raw).decode("ascii"),
                        },
                    )
                    after = row.id
                stream.flush()
        return truncated

    async def _archive(self, directory: Path, output: Path) -> None:
        files = sorted(
            [path for path in directory.rglob("*") if path.is_file()],
            key=lambda path: path.relative_to(directory).as_posix(),
        )
        for path in files:
            if path.is_symlink():
                raise RuntimeError("symbolic links are forbidden in export bundles")
            name = path.relative_to(directory).as_posix()
            if name.startswith("/") or ".." in Path(name).parts:
                raise RuntimeError("unsafe ZIP member path")
        with zipfile.ZipFile(
            output,
            "w",
            compression=zipfile.ZIP_DEFLATED,
            compresslevel=self.settings.export_zip_compression_level,
        ) as archive:
            for path in files:
                archive.write(path, path.relative_to(directory).as_posix())
        self.storage.enforce_size(output)

    async def cleanup_once(self) -> None:
        now = self._clock()
        jobs = await self.repository.expired_completed(now=now)
        for job in jobs:
            try:
                deleted = False
                if job.storage_relative_path:
                    deleted = self.storage.remove(self.storage.resolve(job.storage_relative_path))
                await self.repository.mark_expired(job.id, now=now, deleted=deleted)
            except Exception:
                LOGGER.exception("export cleanup failed", extra={"export_id": str(job.id)})
        self.storage.clean_stale_parts(
            now=now, max_age_seconds=self.settings.export_tmp_max_age_seconds
        )

    async def _cleanup_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await self.cleanup_once()
                await asyncio.wait_for(
                    self._stop.wait(),
                    timeout=self.settings.export_cleanup_interval_seconds,
                )
            except TimeoutError:
                continue
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception("export cleanup iteration failed")
                await asyncio.sleep(min(self.settings.export_cleanup_interval_seconds, 1))
