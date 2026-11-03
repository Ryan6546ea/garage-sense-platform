"""Safe bounded filesystem operations for export artifacts."""

import hashlib
import os
import shutil
from datetime import datetime
from pathlib import Path
from uuid import UUID


class ExportStorageError(RuntimeError):
    pass


class ExportFileTooLargeError(ExportStorageError):
    pass


class ExportStorage:
    def __init__(self, root: Path, *, max_file_size: int) -> None:
        self.root = root.resolve()
        self.max_file_size = max_file_size

    def initialize(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        if not self.available():
            raise ExportStorageError(f"export storage is not writable: {self.root}")

    def available(self) -> bool:
        return self.root.is_dir() and os.access(self.root, os.R_OK | os.W_OK | os.X_OK)

    def resolve(self, relative: str) -> Path:
        value = Path(relative)
        if value.is_absolute() or ".." in value.parts:
            raise ExportStorageError("unsafe export path")
        resolved = (self.root / value).resolve()
        if not resolved.is_relative_to(self.root):
            raise ExportStorageError("export path escapes storage root")
        return resolved

    def final_paths(self, job_id: UUID, suffix: str) -> tuple[Path, Path]:
        final = self.resolve(f"{job_id}.{suffix}")
        return final, self.resolve(f"{job_id}.{suffix}.part")

    def work_directory(self, job_id: UUID) -> Path:
        path = self.resolve(f"{job_id}.work.part")
        path.mkdir(mode=0o700, exist_ok=False)
        return path

    def enforce_size(self, path: Path) -> None:
        if path.stat().st_size > self.max_file_size:
            raise ExportFileTooLargeError(
                f"export exceeded maximum file size {self.max_file_size} bytes"
            )

    @staticmethod
    def sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    @staticmethod
    def remove(path: Path) -> bool:
        if not path.exists() and not path.is_symlink():
            return False
        if path.is_symlink():
            path.unlink()
        elif path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
        return True

    def clean_stale_parts(self, *, now: datetime, max_age_seconds: float) -> int:
        removed = 0
        if not self.root.is_dir():
            return removed
        cutoff = now.timestamp() - max_age_seconds
        for path in self.root.iterdir():
            if path.name.endswith(".part") and path.lstat().st_mtime <= cutoff:
                removed += int(self.remove(path))
        return removed
