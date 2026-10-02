"""Job lifecycle and queue management for the Telegram media processor."""

from __future__ import annotations

import asyncio
import json
import logging
import shutil
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from uuid import uuid4

logger = logging.getLogger("app.job_manager")


class JobStatus:
    RECEIVED = "RECEIVED"
    QUEUED = "QUEUED"
    DOWNLOADING = "DOWNLOADING"
    EXTRACTING = "EXTRACTING"
    SCANNING = "SCANNING"
    UPLOADING = "UPLOADING"
    COMPLETED = "COMPLETED"
    COMPLETED_WITH_ERRORS = "COMPLETED_WITH_ERRORS"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


TERMINAL_STATES = {
    JobStatus.COMPLETED,
    JobStatus.COMPLETED_WITH_ERRORS,
    JobStatus.FAILED,
    JobStatus.CANCELLED,
}


ALLOWED_TRANSITIONS: dict[str, set[str]] = {
    JobStatus.RECEIVED: {
        JobStatus.QUEUED,
        JobStatus.CANCELLED,
        JobStatus.FAILED,
    },
    JobStatus.QUEUED: {
        JobStatus.DOWNLOADING,
        JobStatus.CANCELLED,
        JobStatus.FAILED,
    },
    JobStatus.DOWNLOADING: {
        JobStatus.EXTRACTING,
        JobStatus.CANCELLED,
        JobStatus.FAILED,
    },
    JobStatus.EXTRACTING: {
        JobStatus.SCANNING,
        JobStatus.CANCELLED,
        JobStatus.FAILED,
    },
    JobStatus.SCANNING: {
        JobStatus.UPLOADING,
        JobStatus.COMPLETED,
        JobStatus.COMPLETED_WITH_ERRORS,
        JobStatus.CANCELLED,
        JobStatus.FAILED,
    },
    JobStatus.UPLOADING: {
        JobStatus.COMPLETED,
        JobStatus.COMPLETED_WITH_ERRORS,
        JobStatus.CANCELLED,
        JobStatus.FAILED,
    },
    JobStatus.COMPLETED: set(),
    JobStatus.COMPLETED_WITH_ERRORS: set(),
    JobStatus.FAILED: set(),
    JobStatus.CANCELLED: set(),
}


@dataclass
class UploadResult:
    """Successful upload/result information."""

    media_type: str
    filename: str
    provider: str
    url: str
    display_url: Optional[str] = None
    provider_id: Optional[str] = None
    size_bytes: Optional[int] = None
    width: Optional[int] = None
    height: Optional[int] = None
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class UploadFailure:
    """Information about an upload that failed."""

    media_type: str
    filename: str
    provider: str
    error: str
    attempts: Optional[int] = None
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class Job:
    """A single media-processing job."""

    job_id: str
    user_id: int
    chat_id: int
    archive_name: str

    status: str = JobStatus.RECEIVED

    created_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    updated_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    source_message_id: Optional[int] = None
    status_message_id: Optional[int] = None
    sender_id: Optional[int] = None
    message_id: Optional[int] = None

    archive_size_bytes: Optional[int] = None

    archive_path: Optional[str] = None
    extract_dir: Optional[str] = None

    image_count: int = 0
    video_count: int = 0
    ignored_count: int = 0

    extracted_file_count: int = 0
    extracted_size_bytes: int = 0

    error: Optional[str] = None

    upload_results: list[UploadResult] = field(default_factory=list)
    upload_failures: list[UploadFailure] = field(default_factory=list)

    metadata: dict[str, Any] = field(default_factory=dict)

    # Runtime-only fields.
    _telegram_message: Any = field(
        default=None,
        repr=False,
        compare=False,
    )

    cancel_requested: bool = False

    def touch(self) -> None:
        self.updated_at = datetime.now(timezone.utc)

    def is_terminal(self) -> bool:
        return self.status in TERMINAL_STATES

    def to_dict(self) -> dict[str, Any]:
        """Convert job to a JSON-safe dictionary."""

        def serialize(value: Any) -> Any:
            if isinstance(value, datetime):
                return value.isoformat()

            if isinstance(value, Path):
                return str(value)

            if isinstance(value, UploadResult):
                return value.to_dict()

            if isinstance(value, UploadFailure):
                return value.to_dict()

            if isinstance(value, dict):
                return {
                    str(k): serialize(v)
                    for k, v in value.items()
                }

            if isinstance(value, (list, tuple)):
                return [serialize(v) for v in value]

            if hasattr(value, "to_dict"):
                return serialize(value.to_dict())

            if isinstance(value, (str, int, float, bool)) or value is None:
                return value

            return str(value)

        data = {
            "job_id": self.job_id,
            "user_id": self.user_id,
            "chat_id": self.chat_id,
            "archive_name": self.archive_name,
            "status": self.status,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "source_message_id": self.source_message_id,
            "status_message_id": self.status_message_id,
            "sender_id": self.sender_id,
            "message_id": self.message_id,
            "archive_size_bytes": self.archive_size_bytes,
            "archive_path": self.archive_path,
            "extract_dir": self.extract_dir,
            "image_count": self.image_count,
            "video_count": self.video_count,
            "ignored_count": self.ignored_count,
            "extracted_file_count": self.extracted_file_count,
            "extracted_size_bytes": self.extracted_size_bytes,
            "error": self.error,
            "upload_results": self.upload_results,
            "upload_failures": self.upload_failures,
            "metadata": self.metadata,
            "cancel_requested": self.cancel_requested,
        }

        return serialize(data)


class JobManager:
    """Manage job state, persistence metadata and queue bookkeeping."""

    def __init__(self, job_dir: str | Path) -> None:
        self.job_dir = Path(job_dir)
        self.job_dir.mkdir(parents=True, exist_ok=True)

        self._jobs: dict[str, Job] = {}

        # This queue is bookkeeping for jobs waiting to enter the pipeline.
        self._queue: asyncio.Queue[str] = asyncio.Queue()

        # Prevent duplicate queue entries.
        self._queued_ids: set[str] = set()

    # ------------------------------------------------------------------
    # Job ID
    # ------------------------------------------------------------------

    @staticmethod
    def generate_job_id() -> str:
        """
        Generate IDs similar to:

        JOB-20261002-170612-4681
        """

        now = datetime.now()

        # First four digits of microseconds keep IDs compact.
        suffix = f"{now.microsecond:06d}"[:4]

        return (
            f"JOB-{now:%Y%m%d-%H%M%S}-{suffix}"
        )

    # ------------------------------------------------------------------
    # Creation / retrieval
    # ------------------------------------------------------------------

    def create_job(
        self,
        job_id: Optional[str] = None,
        user_id: Optional[int] = None,
        chat_id: int = 0,
        archive_name: str = "archive.zip",
        message_id: Optional[int] = None,
        source_message_id: Optional[int] = None,
        sender_id: Optional[int] = None,
        archive_size_bytes: Optional[int] = None,
    ) -> Job:
        """
        Create a new job.

        The arguments intentionally support both the current API and
        older Part-1 calling conventions.
        """

        if job_id is None:
            job_id = self.generate_job_id()

        if user_id is None:
            user_id = sender_id

        if user_id is None:
            raise ValueError("user_id/sender_id is required")

        if source_message_id is None:
            source_message_id = message_id

        if sender_id is None:
            sender_id = user_id

        job_root = self.job_dir / job_id
        archive_dir = job_root / "archive"
        extract_dir = job_root / "extracted"

        archive_dir.mkdir(parents=True, exist_ok=True)
        extract_dir.mkdir(parents=True, exist_ok=True)

        archive_path = archive_dir / archive_name

        job = Job(
            job_id=job_id,
            user_id=int(user_id),
            chat_id=int(chat_id),
            archive_name=archive_name,
            source_message_id=source_message_id,
            message_id=message_id,
            sender_id=sender_id,
            archive_size_bytes=archive_size_bytes,
            archive_path=str(archive_path),
            extract_dir=str(extract_dir),
        )

        self._jobs[job_id] = job

        logger.info(
            "Created job %s (archive=%s)",
            job.job_id,
            job.archive_name,
        )

        return job

    def get_job(self, job_id: str) -> Optional[Job]:
        return self._jobs.get(job_id)

    def require_job(self, job_id: str) -> Job:
        job = self.get_job(job_id)

        if job is None:
            raise KeyError(f"Job not found: {job_id}")

        return job

    def list_jobs(self) -> list[Job]:
        return sorted(
            self._jobs.values(),
            key=lambda job: job.created_at,
        )

    # Backward-compatible alias.
    def all_jobs(self) -> list[Job]:
        return self.list_jobs()

    # ------------------------------------------------------------------
    # Queue
    # ------------------------------------------------------------------

    async def enqueue(self, job_id: str) -> None:
        job = self.require_job(job_id)

        if job.is_terminal():
            return

        if job_id in self._queued_ids:
            return

        self._queued_ids.add(job_id)

        if job.status == JobStatus.RECEIVED:
            self.set_status(job_id, JobStatus.QUEUED)

        await self._queue.put(job_id)

    async def get_next_job(self) -> Job:
        job_id = await self._queue.get()

        self._queued_ids.discard(job_id)

        return self.require_job(job_id)

    def task_done(self, job_id: Optional[str] = None) -> None:
        """
        Mark a queue item as processed.

        job_id is accepted for compatibility, but asyncio.Queue itself
        tracks completion independently of the ID.
        """

        self._queue.task_done()

        if job_id:
            self._queued_ids.discard(job_id)

    def queue_size(self) -> int:
        return self._queue.qsize()

    def remove_from_queue(self, job_id: str) -> None:
        """
        Remove bookkeeping for a job.

        asyncio.Queue does not safely support arbitrary removal, so the
        actual queue item is allowed to drain while this bookkeeping
        prevents duplicate scheduling.
        """

        self._queued_ids.discard(job_id)

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------

    def set_status(
        self,
        job_id: str,
        new_status: str,
        *,
        force: bool = False,
    ) -> Job:
        job = self.require_job(job_id)

        old_status = job.status

        if old_status == new_status:
            job.touch()
            return job

        if job.is_terminal() and not force:
            raise RuntimeError(
                f"Cannot change terminal job {job_id}: "
                f"{old_status} -> {new_status}"
            )

        if not force:
            allowed = ALLOWED_TRANSITIONS.get(old_status, set())

            if new_status not in allowed:
                raise RuntimeError(
                    f"Invalid job transition for {job_id}: "
                    f"{old_status} -> {new_status}"
                )

        job.status = new_status
        job.touch()

        logger.info(
            "Job %s status %s -> %s",
            job_id,
            old_status,
            new_status,
        )

        return job

    def set_status_message(
        self,
        job_id: str,
        message_id: int,
    ) -> Job:
        job = self.require_job(job_id)

        job.status_message_id = int(message_id)
        job.touch()

        return job

    # ------------------------------------------------------------------
    # Error / cancellation
    # ------------------------------------------------------------------

    def set_error(
        self,
        job_id: str,
        error: str,
        *,
        failed: bool = False,
    ) -> Job:
        job = self.require_job(job_id)

        job.error = str(error)
        job.touch()

        if failed and not job.is_terminal():
            self.set_status(
                job_id,
                JobStatus.FAILED,
            )

        logger.error(
            "Job %s error: %s",
            job_id,
            error,
        )

        return job

    def fail(
        self,
        job_id: str,
        error: str,
    ) -> Job:
        return self.set_error(
            job_id,
            error,
            failed=True,
        )

    def cancel(self, job_id: str) -> Job:
        job = self.require_job(job_id)

        job.cancel_requested = True
        job.touch()

        if not job.is_terminal():
            self.set_status(
                job_id,
                JobStatus.CANCELLED,
            )

        self.remove_from_queue(job_id)

        logger.info(
            "Job %s cancelled",
            job_id,
        )

        return job

    # Backward-compatible alias.
    def request_cancel(self, job_id: str) -> Job:
        return self.cancel(job_id)

    def is_cancel_requested(self, job_id: str) -> bool:
        return self.require_job(job_id).cancel_requested

    # ------------------------------------------------------------------
    # Media / extraction stats
    # ------------------------------------------------------------------

    def set_media_counts(
        self,
        job_id: str,
        *,
        image_count: int = 0,
        video_count: int = 0,
        ignored_count: int = 0,
    ) -> Job:
        job = self.require_job(job_id)

        job.image_count = int(image_count)
        job.video_count = int(video_count)
        job.ignored_count = int(ignored_count)
        job.touch()

        return job

    def set_extraction_stats(
        self,
        job_id: str,
        *,
        file_count: int = 0,
        total_size_bytes: int = 0,
    ) -> Job:
        job = self.require_job(job_id)

        job.extracted_file_count = int(file_count)
        job.extracted_size_bytes = int(total_size_bytes)
        job.touch()

        return job

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    def set_metadata(
        self,
        job_id: str,
        key: str,
        value: Any,
    ) -> Job:
        job = self.require_job(job_id)

        job.metadata[str(key)] = value
        job.touch()

        return job

    def update_metadata(
        self,
        job_id: str,
        values: dict[str, Any],
    ) -> Job:
        job = self.require_job(job_id)

        job.metadata.update(values)
        job.touch()

        return job

    # ------------------------------------------------------------------
    # Upload results
    # ------------------------------------------------------------------

    def add_upload_result(
        self,
        job_id: str,
        result: UploadResult | dict[str, Any],
    ) -> Job:
        job = self.require_job(job_id)

        if isinstance(result, dict):
            result = UploadResult(
                media_type=str(
                    result.get("media_type", "unknown")
                ),
                filename=str(
                    result.get("filename", "")
                ),
                provider=str(
                    result.get("provider", "")
                ),
                url=str(
                    result.get("url", "")
                ),
                display_url=result.get("display_url"),
                provider_id=result.get("provider_id")
                or result.get("id"),
                size_bytes=result.get("size_bytes")
                or result.get("size"),
                width=result.get("width"),
                height=result.get("height"),
                extra=result.get("extra", {}),
            )

        job.upload_results.append(result)
        job.touch()

        return job

    def set_upload_result(
        self,
        job_id: str,
        result: UploadResult | dict[str, Any],
    ) -> Job:
        return self.add_upload_result(
            job_id,
            result,
        )

    def add_upload_failure(
        self,
        job_id: str,
        failure: UploadFailure | dict[str, Any],
    ) -> Job:
        job = self.require_job(job_id)

        if isinstance(failure, dict):
            failure = UploadFailure(
                media_type=str(
                    failure.get("media_type", "unknown")
                ),
                filename=str(
                    failure.get("filename", "")
                ),
                provider=str(
                    failure.get("provider", "")
                ),
                error=str(
                    failure.get("error", "Unknown error")
                ),
                attempts=failure.get("attempts"),
                extra=failure.get("extra", {}),
            )

        job.upload_failures.append(failure)
        job.touch()

        return job

    def set_upload_failure(
        self,
        job_id: str,
        failure: UploadFailure | dict[str, Any],
    ) -> Job:
        return self.add_upload_failure(
            job_id,
            failure,
        )

    # ------------------------------------------------------------------
    # Completion
    # ------------------------------------------------------------------

    def complete(self, job_id: str) -> Job:
        job = self.require_job(job_id)

        if job.is_terminal():
            return job

        self.set_status(
            job_id,
            JobStatus.COMPLETED,
        )

        return job

    def complete_with_errors(self, job_id: str) -> Job:
        job = self.require_job(job_id)

        if job.is_terminal():
            return job

        self.set_status(
            job_id,
            JobStatus.COMPLETED_WITH_ERRORS,
        )

        return job

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

    def save_job_json(self, job_id: str) -> Path:
        """
        Save a JSON snapshot inside the job directory.

        This is only metadata persistence; Telegram session persistence
        remains handled separately by MongoDB.
        """

        job = self.require_job(job_id)

        job_root = self.job_dir / job.job_id
        job_root.mkdir(parents=True, exist_ok=True)

        path = job_root / "job.json"

        path.write_text(
            json.dumps(
                job.to_dict(),
                indent=2,
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        return path

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def cleanup_job_files(self, job_id: str) -> None:
        job = self.require_job(job_id)

        job_root = self.job_dir / job.job_id

        if not job_root.exists():
            return

        # Safety check: never delete outside configured job_dir.
        try:
            resolved_root = job_root.resolve()
            resolved_base = self.job_dir.resolve()

            resolved_root.relative_to(resolved_base)
        except ValueError:
            logger.error(
                "Refusing to cleanup unsafe job path: %s",
                job_root,
            )
            return

        try:
            shutil.rmtree(resolved_root)

            logger.info(
                "Cleaned job %s files",
                job_id,
            )

        except FileNotFoundError:
            pass

        except Exception:
            logger.exception(
                "Failed to cleanup job %s",
                job_id,
            )

    def cleanup(self, job_id: str) -> None:
        self.cleanup_job_files(job_id)

    def remove_job(self, job_id: str) -> Optional[Job]:
        job = self._jobs.pop(job_id, None)

        self.remove_from_queue(job_id)

        return job

    # ------------------------------------------------------------------
    # Convenience helpers
    # ------------------------------------------------------------------

    def active_jobs(self) -> list[Job]:
        return [
            job
            for job in self._jobs.values()
            if not job.is_terminal()
        ]

    def completed_jobs(self) -> list[Job]:
        return [
            job
            for job in self._jobs.values()
            if job.status
            in {
                JobStatus.COMPLETED,
                JobStatus.COMPLETED_WITH_ERRORS,
            }
        ]

    def has_job(self, job_id: str) -> bool:
        return job_id in self._jobs
