"""Job lifecycle and queue management for the Telegram media processor."""

from __future__ import annotations

import asyncio
import logging
import shutil
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Optional


logger = logging.getLogger("app.job_manager")


class JobStatus(str, Enum):
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


# Allowed state transitions.
ALLOWED_TRANSITIONS: dict[JobStatus, set[JobStatus]] = {
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
    """Result returned by an uploader."""

    provider: str
    media_type: str
    filename: str
    path: str
    url: str
    display_url: Optional[str] = None
    provider_id: Optional[str] = None
    size_bytes: Optional[int] = None
    width: Optional[int] = None
    height: Optional[int] = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "UploadResult":
        return cls(
            provider=str(data.get("provider", "")),
            media_type=str(data.get("media_type", "")),
            filename=str(data.get("filename", "")),
            path=str(data.get("path", "")),
            url=str(data.get("url", "")),
            display_url=data.get("display_url"),
            provider_id=data.get("provider_id"),
            size_bytes=data.get("size_bytes"),
            width=data.get("width"),
            height=data.get("height"),
            metadata=dict(data.get("metadata") or {}),
        )


@dataclass
class UploadFailure:
    """A failed upload attempt."""

    provider: str
    media_type: str
    filename: str
    path: str
    error: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "UploadFailure":
        return cls(
            provider=str(data.get("provider", "")),
            media_type=str(data.get("media_type", "")),
            filename=str(data.get("filename", "")),
            path=str(data.get("path", "")),
            error=str(data.get("error", "")),
        )


@dataclass
class Job:
    """Represents one archive-processing job."""

    job_id: str
    user_id: int
    chat_id: int

    archive_name: str
    archive_path: Optional[str] = None
    extract_dir: Optional[str] = None

    status: JobStatus = JobStatus.RECEIVED

    created_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    updated_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    message_id: Optional[int] = None
    status_message_id: Optional[int] = None

    error: Optional[str] = None

    image_count: int = 0
    video_count: int = 0
    ignored_count: int = 0

    extracted_files: int = 0
    extracted_bytes: int = 0

    upload_results: list[UploadResult] = field(default_factory=list)
    upload_failures: list[UploadFailure] = field(default_factory=list)

    metadata: dict[str, Any] = field(default_factory=dict)

    def touch(self) -> None:
        self.updated_at = datetime.now(timezone.utc)


class JobManager:
    """Thread/task-safe in-memory job manager."""

    def __init__(
        self,
        job_root: Path,
        max_jobs: int = 100,
    ) -> None:
        self.job_root = Path(job_root)
        self.job_root.mkdir(parents=True, exist_ok=True)

        self.max_jobs = max_jobs

        self._jobs: dict[str, Job] = {}
        self._queue: asyncio.Queue[str] = asyncio.Queue()

        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Job creation / lookup
    # ------------------------------------------------------------------

    async def create_job(
        self,
        job_id: str,
        user_id: int,
        chat_id: int,
        archive_name: str,
        message_id: Optional[int] = None,
    ) -> Job:
        async with self._lock:
            if len(self._jobs) >= self.max_jobs:
                raise RuntimeError(
                    "Maximum number of active jobs has been reached"
                )

            if job_id in self._jobs:
                raise ValueError(f"Job already exists: {job_id}")

            job_dir = self.job_root / job_id
            archive_dir = job_dir / "archive"
            extracted_dir = job_dir / "extracted"

            archive_dir.mkdir(parents=True, exist_ok=True)
            extracted_dir.mkdir(parents=True, exist_ok=True)

            job = Job(
                job_id=job_id,
                user_id=user_id,
                chat_id=chat_id,
                archive_name=archive_name,
                archive_path=str(
                    archive_dir / archive_name
                ),
                extract_dir=str(extracted_dir),
                message_id=message_id,
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
            raise KeyError(f"Unknown job: {job_id}")

        return job

    def list_jobs(self) -> list[Job]:
        return list(self._jobs.values())

    # ------------------------------------------------------------------
    # Queue
    # ------------------------------------------------------------------

    async def enqueue(self, job_id: str) -> None:
        job = self.require_job(job_id)

        if job.status == JobStatus.RECEIVED:
            self.set_status(job_id, JobStatus.QUEUED)

        await self._queue.put(job_id)

    async def get_next_job(self) -> str:
        return await self._queue.get()

    def task_done(self) -> None:
        self._queue.task_done()

    def queue_size(self) -> int:
        return self._queue.qsize()

    def remove_from_queue(self, job_id: str) -> None:
        """
        Remove a job from the bookkeeping queue.

        The actual asyncio.Queue cannot safely remove arbitrary items,
        so this method is intentionally a no-op for queue storage.

        It exists as an explicit lifecycle hook so callers can mark
        that processing has taken ownership of the job and prevent
        stale queue bookkeeping in higher-level logic.
        """
        logger.debug(
            "Queue ownership transferred for job %s",
            job_id,
        )

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------

    def set_status(
        self,
        job_id: str,
        new_status: JobStatus,
    ) -> Job:
        job = self.require_job(job_id)

        if isinstance(new_status, str):
            new_status = JobStatus(new_status)

        old_status = job.status

        if old_status == new_status:
            job.touch()
            return job

        if old_status in TERMINAL_STATES:
            raise RuntimeError(
                f"Cannot change terminal job {job_id}: "
                f"{old_status.value} -> {new_status.value}"
            )

        allowed = ALLOWED_TRANSITIONS.get(old_status, set())

        if new_status not in allowed:
            raise RuntimeError(
                f"Invalid job status transition for {job_id}: "
                f"{old_status.value} -> {new_status.value}"
            )

        job.status = new_status
        job.touch()

        logger.info(
            "Job %s status %s -> %s",
            job_id,
            old_status.value,
            new_status.value,
        )

        return job

    def is_terminal(self, job_id: str) -> bool:
        job = self.require_job(job_id)
        return job.status in TERMINAL_STATES

    # ------------------------------------------------------------------
    # Status message
    # ------------------------------------------------------------------

    def set_status_message(
        self,
        job_id: str,
        message_id: int,
    ) -> Job:
        job = self.require_job(job_id)
        job.status_message_id = message_id
        job.touch()
        return job

    # ------------------------------------------------------------------
    # Errors
    # ------------------------------------------------------------------

    def set_error(
        self,
        job_id: str,
        error: str,
        *,
        failed: bool = True,
    ) -> Job:
        job = self.require_job(job_id)

        job.error = str(error)
        job.touch()

        if failed and job.status not in TERMINAL_STATES:
            self.set_status(job_id, JobStatus.FAILED)

        logger.error(
            "Job %s error: %s",
            job_id,
            error,
        )

        return job

    # ------------------------------------------------------------------
    # Media statistics
    # ------------------------------------------------------------------

    def set_media_counts(
        self,
        job_id: str,
        *,
        images: int,
        videos: int,
        ignored: int,
    ) -> Job:
        job = self.require_job(job_id)

        job.image_count = int(images)
        job.video_count = int(videos)
        job.ignored_count = int(ignored)
        job.touch()

        return job

    def set_extraction_stats(
        self,
        job_id: str,
        *,
        files: int,
        bytes_total: int,
    ) -> Job:
        job = self.require_job(job_id)

        job.extracted_files = int(files)
        job.extracted_bytes = int(bytes_total)
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
            result = UploadResult.from_dict(result)

        job.upload_results.append(result)
        job.touch()

        logger.info(
            "Job %s upload successful: provider=%s "
            "media_type=%s filename=%s",
            job_id,
            result.provider,
            result.media_type,
            result.filename,
        )

        return job

    def add_upload_failure(
        self,
        job_id: str,
        failure: UploadFailure | dict[str, Any],
    ) -> Job:
        job = self.require_job(job_id)

        if isinstance(failure, dict):
            failure = UploadFailure.from_dict(failure)

        job.upload_failures.append(failure)
        job.touch()

        logger.warning(
            "Job %s upload failed: provider=%s "
            "media_type=%s filename=%s error=%s",
            job_id,
            failure.provider,
            failure.media_type,
            failure.filename,
            failure.error,
        )

        return job

    def clear_upload_results(self, job_id: str) -> Job:
        job = self.require_job(job_id)

        job.upload_results.clear()
        job.upload_failures.clear()
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
    # Completion
    # ------------------------------------------------------------------

    def complete(
        self,
        job_id: str,
    ) -> Job:
        job = self.require_job(job_id)

        if job.upload_failures:
            return self.complete_with_errors(job_id)

        self.set_status(job_id, JobStatus.COMPLETED)

        return job

    def complete_with_errors(
        self,
        job_id: str,
    ) -> Job:
        job = self.require_job(job_id)

        self.set_status(
            job_id,
            JobStatus.COMPLETED_WITH_ERRORS,
        )

        return job

    def cancel(
        self,
        job_id: str,
        reason: Optional[str] = None,
    ) -> Job:
        job = self.require_job(job_id)

        if job.status in TERMINAL_STATES:
            return job

        if reason:
            job.error = reason

        self.set_status(
            job_id,
            JobStatus.CANCELLED,
        )

        return job

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

    def serialize_job(
        self,
        job_id: str,
    ) -> dict[str, Any]:
        job = self.require_job(job_id)

        data = asdict(job)

        data["status"] = job.status.value

        data["created_at"] = job.created_at.isoformat()
        data["updated_at"] = job.updated_at.isoformat()

        data["upload_results"] = [
            result.to_dict()
            for result in job.upload_results
        ]

        data["upload_failures"] = [
            failure.to_dict()
            for failure in job.upload_failures
        ]

        return data

    def deserialize_job(
        self,
        data: dict[str, Any],
    ) -> Job:
        created_at = data.get("created_at")
        updated_at = data.get("updated_at")

        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at)

        if isinstance(updated_at, str):
            updated_at = datetime.fromisoformat(updated_at)

        job = Job(
            job_id=str(data["job_id"]),
            user_id=int(data["user_id"]),
            chat_id=int(data["chat_id"]),
            archive_name=str(data["archive_name"]),
            archive_path=data.get("archive_path"),
            extract_dir=data.get("extract_dir"),
            status=JobStatus(data.get("status", JobStatus.RECEIVED.value)),
            created_at=created_at
            or datetime.now(timezone.utc),
            updated_at=updated_at
            or datetime.now(timezone.utc),
            message_id=data.get("message_id"),
            status_message_id=data.get("status_message_id"),
            error=data.get("error"),
            image_count=int(data.get("image_count", 0)),
            video_count=int(data.get("video_count", 0)),
            ignored_count=int(data.get("ignored_count", 0)),
            extracted_files=int(data.get("extracted_files", 0)),
            extracted_bytes=int(data.get("extracted_bytes", 0)),
            upload_results=[
                UploadResult.from_dict(item)
                for item in data.get("upload_results", [])
            ],
            upload_failures=[
                UploadFailure.from_dict(item)
                for item in data.get("upload_failures", [])
            ],
            metadata=dict(data.get("metadata") or {}),
        )

        return job

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def cleanup_job_files(
        self,
        job_id: str,
    ) -> None:
        job = self.get_job(job_id)

        if job is None:
            return

        job_dir = self.job_root / job_id

        if not job_dir.exists():
            return

        try:
            shutil.rmtree(job_dir)

            logger.info(
                "Cleaned job %s (removed: archive, extracted)",
                job_id,
            )

        except FileNotFoundError:
            pass

        except Exception:
            logger.exception(
                "Failed to clean job files for %s",
                job_id,
            )

    def remove_job(
        self,
        job_id: str,
        *,
        cleanup_files: bool = True,
    ) -> Optional[Job]:
        job = self._jobs.pop(job_id, None)

        if job is None:
            return None

        if cleanup_files:
            self.cleanup_job_files(job_id)

        logger.debug(
            "Removed job %s from job manager",
            job_id,
        )

        return job

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def active_jobs(self) -> list[Job]:
        return [
            job
            for job in self._jobs.values()
            if job.status not in TERMINAL_STATES
        ]

    def active_job_count(self) -> int:
        return len(self.active_jobs())

    def completed_job_count(self) -> int:
        return sum(
            1
            for job in self._jobs.values()
            if job.status in TERMINAL_STATES
        )

    def get_upload_urls(
        self,
        job_id: str,
    ) -> list[str]:
        job = self.require_job(job_id)

        return [
            result.url
            for result in job.upload_results
            if result.url
        ]

    def get_upload_results(
        self,
        job_id: str,
    ) -> list[UploadResult]:
        job = self.require_job(job_id)

        return list(job.upload_results)

    def get_upload_failures(
        self,
        job_id: str,
    ) -> list[UploadFailure]:
        job = self.require_job(job_id)

        return list(job.upload_failures)
