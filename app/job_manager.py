"""Job state and queue management."""

from __future__ import annotations

import json
import logging
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Dict, Iterator, List, Optional

logger = logging.getLogger("app.job_manager")

__all__ = [
    "Job",
    "JobManager",
    "JobStateError",
    "JobStatus",
]


def _utc_now_iso() -> str:
    """Return current UTC time as an ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()


class JobStateError(RuntimeError):
    """Raised when a job state operation is invalid."""


class JobStatus(str, Enum):
    """Supported job states."""

    RECEIVED = "RECEIVED"
    QUEUED = "QUEUED"
    DOWNLOADING = "DOWNLOADING"
    EXTRACTING = "EXTRACTING"
    SCANNING = "SCANNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


TERMINAL_STATUSES = frozenset(
    {
        JobStatus.COMPLETED,
        JobStatus.FAILED,
        JobStatus.CANCELLED,
    }
)


ALLOWED_TRANSITIONS = {
    JobStatus.RECEIVED: frozenset(
        {
            JobStatus.QUEUED,
            JobStatus.CANCELLED,
            JobStatus.FAILED,
        }
    ),
    JobStatus.QUEUED: frozenset(
        {
            JobStatus.DOWNLOADING,
            JobStatus.CANCELLED,
            JobStatus.FAILED,
        }
    ),
    JobStatus.DOWNLOADING: frozenset(
        {
            JobStatus.EXTRACTING,
            JobStatus.CANCELLED,
            JobStatus.FAILED,
        }
    ),
    JobStatus.EXTRACTING: frozenset(
        {
            JobStatus.SCANNING,
            JobStatus.CANCELLED,
            JobStatus.FAILED,
        }
    ),
    JobStatus.SCANNING: frozenset(
        {
            JobStatus.COMPLETED,
            JobStatus.CANCELLED,
            JobStatus.FAILED,
        }
    ),
    JobStatus.COMPLETED: frozenset(),
    JobStatus.FAILED: frozenset(),
    JobStatus.CANCELLED: frozenset(),
}


def generate_job_id() -> str:
    """Generate a readable unique job identifier."""
    timestamp = datetime.now(timezone.utc).strftime(
        "%Y%m%d-%H%M%S"
    )

    import secrets

    suffix = secrets.token_hex(2).upper()

    return f"JOB-{timestamp}-{suffix}"


@dataclass
class Job:
    """Represent one media-processing job."""

    job_id: str
    source_message_id: Optional[int]
    chat_id: Optional[int]
    sender_id: Optional[int]
    archive_name: str
    archive_size_bytes: int
    job_directory: Path
    status: JobStatus = JobStatus.RECEIVED

    created_at: str = field(
        default_factory=_utc_now_iso
    )
    updated_at: str = field(
        default_factory=_utc_now_iso
    )

    started_at: Optional[str] = None
    finished_at: Optional[str] = None

    status_message_id: Optional[int] = None

    total_files: int = 0
    image_files: int = 0
    video_files: int = 0
    ignored_files: int = 0

    error: Optional[str] = None
    cancel_requested: bool = False

    @property
    def archive_dir(self) -> Path:
        """Return directory containing the downloaded archive."""
        return self.job_directory / "archive"

    @property
    def archive_path(self) -> Path:
        """Return expected archive path."""
        return self.archive_dir / self.archive_name

    @property
    def extracted_dir(self) -> Path:
        """Return extraction directory."""
        return self.job_directory / "extracted"

    @property
    def metadata_path(self) -> Path:
        """Return metadata file path."""
        return self.job_directory / "job.json"

    @property
    def is_terminal(self) -> bool:
        """Return whether this job has reached a terminal state."""
        return self.status in TERMINAL_STATUSES

    def to_dict(self) -> dict:
        """Serialize job metadata."""
        return {
            "job_id": self.job_id,
            "source_message_id": self.source_message_id,
            "chat_id": self.chat_id,
            "sender_id": self.sender_id,
            "archive_name": self.archive_name,
            "archive_size_bytes": self.archive_size_bytes,
            "job_directory": str(self.job_directory),
            "status": self.status.value,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "status_message_id": self.status_message_id,
            "total_files": self.total_files,
            "image_files": self.image_files,
            "video_files": self.video_files,
            "ignored_files": self.ignored_files,
            "error": self.error,
            "cancel_requested": self.cancel_requested,
        }

    def write_metadata(self) -> None:
        """Persist job metadata to disk."""
        self.job_directory.mkdir(
            parents=True,
            exist_ok=True,
        )

        temporary_path = self.metadata_path.with_suffix(
            ".json.tmp"
        )

        temporary_path.write_text(
            json.dumps(
                self.to_dict(),
                indent=2,
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        temporary_path.replace(self.metadata_path)


class JobManager:
    """Manage jobs and their pending queue."""

    def __init__(
        self,
        base_dir: Path,
        *,
        keep_files: bool = False,
    ) -> None:
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        self.keep_files = keep_files

        self._jobs: Dict[str, Job] = {}
        self._queue: List[str] = []

    # -- creation ----------------------------------------------------------- #

    def create_job(
        self,
        *,
        archive_name: str,
        source_message_id: Optional[int] = None,
        chat_id: Optional[int] = None,
        sender_id: Optional[int] = None,
        archive_size_bytes: int = 0,
    ) -> Job:
        """Create and register a new job."""
        job_id = generate_job_id()

        while job_id in self._jobs:
            job_id = generate_job_id()

        job_directory = self.base_dir / job_id

        (
            job_directory / "archive"
        ).mkdir(
            parents=True,
            exist_ok=True,
        )

        (
            job_directory / "extracted"
        ).mkdir(
            parents=True,
            exist_ok=True,
        )

        job = Job(
            job_id=job_id,
            source_message_id=source_message_id,
            chat_id=chat_id,
            sender_id=sender_id,
            archive_name=Path(
                archive_name or "archive.zip"
            ).name,
            archive_size_bytes=archive_size_bytes,
            job_directory=job_directory,
            status=JobStatus.RECEIVED,
        )

        self._jobs[job_id] = job

        job.write_metadata()

        logger.info(
            "Created job %s (archive=%s)",
            job_id,
            job.archive_name,
        )

        return job

    # -- access ------------------------------------------------------------- #

    def get(self, job_id: str) -> Job:
        """Return a job or raise if it does not exist."""
        try:
            return self._jobs[job_id]

        except KeyError as exc:
            raise JobStateError(
                f"unknown job id: {job_id}"
            ) from exc

    def find(self, job_id: str) -> Optional[Job]:
        """Return a job or None."""
        return self._jobs.get(job_id)

    def all_jobs(self) -> List[Job]:
        """Return all jobs ordered by creation time."""
        return sorted(
            self._jobs.values(),
            key=lambda job: job.created_at,
        )

    def __iter__(self) -> Iterator[Job]:
        return iter(self.all_jobs())

    def __len__(self) -> int:
        return len(self._jobs)

    def active_jobs(self) -> List[Job]:
        """Return all non-terminal jobs."""
        return [
            job
            for job in self.all_jobs()
            if not job.is_terminal
        ]

    def active_job(self) -> Optional[Job]:
        """Return the newest active job."""
        for job in reversed(self.all_jobs()):
            if not job.is_terminal:
                return job

        return None

    def latest_job(self) -> Optional[Job]:
        """Return the newest job."""
        jobs = self.all_jobs()

        return jobs[-1] if jobs else None

    # -- state machine ------------------------------------------------------ #

    def can_transition(
        self,
        current: JobStatus,
        new: JobStatus,
    ) -> bool:
        """Check whether a state transition is allowed."""
        return new in ALLOWED_TRANSITIONS.get(
            current,
            frozenset(),
        )

    def set_status(
        self,
        job_id: str,
        new_status: JobStatus,
        *,
        error: Optional[str] = None,
    ) -> Job:
        """Change a job's status."""
        job = self.get(job_id)

        new_status = JobStatus(new_status)

        if new_status == job.status:
            job.updated_at = _utc_now_iso()
            return job

        if not self.can_transition(
            job.status,
            new_status,
        ):
            raise JobStateError(
                f"illegal transition for {job_id}: "
                f"{job.status.value} -> {new_status.value}"
            )

        previous = job.status

        job.status = new_status
        job.updated_at = _utc_now_iso()

        if (
            new_status == JobStatus.DOWNLOADING
            and job.started_at is None
        ):
            job.started_at = job.updated_at

        if new_status in TERMINAL_STATUSES:
            job.finished_at = job.updated_at

        if error is not None:
            job.error = error

        job.write_metadata()

        logger.info(
            "Job %s status %s -> %s",
            job_id,
            previous.value,
            new_status.value,
        )

        return job

    def fail(
        self,
        job_id: str,
        error: str,
    ) -> Job:
        """Mark a job as failed."""
        return self.set_status(
            job_id,
            JobStatus.FAILED,
            error=error,
        )

    def cancel(self, job_id: str) -> Job:
        """Cancel a job immediately."""
        job = self.get(job_id)

        if job.is_terminal:
            return job

        return self.set_status(
            job_id,
            JobStatus.CANCELLED,
        )

    def request_cancel(
        self,
        job_id: str,
    ) -> bool:
        """Request cancellation for a running job."""
        job = self.find(job_id)

        if job is None or job.is_terminal:
            return False

        job.cancel_requested = True
        job.updated_at = _utc_now_iso()
        job.write_metadata()

        return True

    def update_counts(
        self,
        job_id: str,
        counts: Dict[str, int],
    ) -> Job:
        """Update scan counters."""
        job = self.get(job_id)

        job.total_files = int(
            counts.get(
                "total_files",
                job.total_files,
            )
        )

        job.image_files = int(
            counts.get(
                "image_files",
                job.image_files,
            )
        )

        job.video_files = int(
            counts.get(
                "video_files",
                job.video_files,
            )
        )

        job.ignored_files = int(
            counts.get(
                "ignored_files",
                job.ignored_files,
            )
        )

        job.updated_at = _utc_now_iso()

        job.write_metadata()

        return job

    def set_status_message(
        self,
        job_id: str,
        message_id: Optional[int],
    ) -> Job:
        """Store Telegram status message ID."""
        job = self.get(job_id)

        job.status_message_id = message_id

        job.write_metadata()

        return job

    # -- queue -------------------------------------------------------------- #

    def enqueue(self, job_id: str) -> None:
        """Add a job to the pending queue."""
        job = self.get(job_id)

        if job.status == JobStatus.RECEIVED:
            self.set_status(
                job_id,
                JobStatus.QUEUED,
            )

        if job_id not in self._queue:
            self._queue.append(job_id)

    def dequeue(self) -> Optional[str]:
        """Remove and return the next non-terminal queued job."""
        while self._queue:
            job_id = self._queue.pop(0)

            job = self.find(job_id)

            if job is not None and not job.is_terminal:
                return job_id

        return None

    def remove_from_queue(
        self,
        job_id: str,
    ) -> None:
        """Remove a specific job from the pending queue."""
        try:
            self._queue.remove(job_id)

        except ValueError:
            pass

    def queued_jobs(self) -> List[Job]:
        """Return jobs currently waiting in the queue."""
        return [
            self._jobs[job_id]
            for job_id in self._queue
            if job_id in self._jobs
        ]

    def queue_size(self) -> int:
        """Return number of jobs currently waiting in the queue."""
        return len(self._queue)

    # -- cleanup ------------------------------------------------------------ #

    def cleanup_job_files(
        self,
        job_id: str,
    ) -> None:
        """Remove temporary archive/extracted files."""
        job = self.get(job_id)

        if self.keep_files:
            logger.info(
                "Keeping job files for %s",
                job_id,
            )
            return

        removed = []

        for directory in (
            job.archive_dir,
            job.extracted_dir,
        ):
            if directory.exists():
                shutil.rmtree(
                    directory,
                    ignore_errors=True,
                )

                removed.append(
                    directory.name
                )

        logger.info(
            "Cleaned job %s (removed: %s)",
            job_id,
            ", ".join(removed) if removed else "nothing",
        )

    def forget(self, job_id: str) -> None:
        """Forget an in-memory job record."""
        self._queue = [
            queued_id
            for queued_id in self._queue
            if queued_id != job_id
        ]

        self._jobs.pop(
            job_id,
            None,
        )
