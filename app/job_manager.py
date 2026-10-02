"""Job management, state tracking, queue bookkeeping and cleanup."""

from __future__ import annotations

import json
import logging
import shutil
import uuid
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger("app.job_manager")

__all__ = [
    "Job",
    "JobManager",
    "JobStatus",
    "JobStateError",
    "ALLOWED_TRANSITIONS",
    "generate_job_id",
]


class JobStateError(RuntimeError):
    """Raised when a job does not exist or has an invalid state change."""


class JobStatus(str, Enum):
    """Lifecycle states for a processing job."""

    RECEIVED = "RECEIVED"
    QUEUED = "QUEUED"
    DOWNLOADING = "DOWNLOADING"
    EXTRACTING = "EXTRACTING"
    SCANNING = "SCANNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


TERMINAL_STATES = {
    JobStatus.COMPLETED,
    JobStatus.FAILED,
    JobStatus.CANCELLED,
}


ALLOWED_TRANSITIONS = {
    JobStatus.RECEIVED: {
        JobStatus.QUEUED,
        JobStatus.DOWNLOADING,
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
        JobStatus.COMPLETED,
        JobStatus.CANCELLED,
        JobStatus.FAILED,
    },
    JobStatus.COMPLETED: set(),
    JobStatus.FAILED: set(),
    JobStatus.CANCELLED: set(),
}


def _utc_now() -> datetime:
    """Return the current timezone-aware UTC timestamp."""
    return datetime.now(timezone.utc)


def _timestamp() -> str:
    """Return an ISO-8601 UTC timestamp."""
    return _utc_now().isoformat()


def generate_job_id() -> str:
    """Generate a short human-readable unique job ID."""
    now = _utc_now().strftime("%Y%m%d-%H%M%S")
    suffix = uuid.uuid4().hex[:4].upper()

    return f"JOB-{now}-{suffix}"


def _safe_archive_name(name: str) -> str:
    """Return a filesystem-safe archive filename."""
    name = Path(str(name or "archive.zip")).name

    if not name:
        name = "archive.zip"

    return name


@dataclass
class Job:
    """Persistent representation of one archive-processing job."""

    job_id: str
    archive_name: str

    source_message_id: Optional[int] = None
    chat_id: Optional[int] = None
    sender_id: Optional[int] = None

    archive_size_bytes: int = 0

    status: JobStatus = JobStatus.RECEIVED

    created_at: str = field(default_factory=_timestamp)
    started_at: Optional[str] = None
    finished_at: Optional[str] = None

    error: Optional[str] = None

    cancel_requested: bool = False

    total_files: int = 0
    image_files: int = 0
    video_files: int = 0
    ignored_files: int = 0

    status_message_id: Optional[int] = None

    # Runtime-only Telegram message.
    #
    # This is intentionally excluded from metadata persistence because a
    # Telethon Message object is not JSON serializable and may contain
    # unnecessary runtime state.
    _telegram_message: Any = field(
        default=None,
        repr=False,
        compare=False,
    )

    # Runtime-only job directory. This is restored/calculated by JobManager.
    _job_root: Optional[Path] = field(
        default=None,
        repr=False,
        compare=False,
    )

    @property
    def is_terminal(self) -> bool:
        """Whether this job has reached a final state."""
        return self.status in TERMINAL_STATES

    @property
    def job_directory(self) -> Path:
        """Root directory belonging exclusively to this job."""
        if self._job_root is None:
            raise RuntimeError(
                "Job directory is not attached to a JobManager"
            )

        return self._job_root

    @property
    def job_dir(self) -> Path:
        """Compatibility alias for the job directory."""
        return self.job_directory

    @property
    def archive_dir(self) -> Path:
        """Directory containing the downloaded archive."""
        return self.job_directory / "archive"

    @property
    def extracted_dir(self) -> Path:
        """Directory containing extracted files."""
        return self.job_directory / "extracted"

    @property
    def metadata_path(self) -> Path:
        """Persistent metadata file."""
        return self.job_directory / "metadata.json"

    @property
    def archive_path(self) -> Path:
        """Expected path of the downloaded archive."""
        return self.archive_dir / self.archive_name

    def write_metadata(self) -> None:
        """Persist safe job metadata to disk."""
        self.job_directory.mkdir(
            parents=True,
            exist_ok=True,
        )

        payload = {
            "job_id": self.job_id,
            "archive_name": self.archive_name,
            "source_message_id": self.source_message_id,
            "chat_id": self.chat_id,
            "sender_id": self.sender_id,
            "archive_size_bytes": self.archive_size_bytes,
            "status": self.status.value,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "error": self.error,
            "cancel_requested": self.cancel_requested,
            "total_files": self.total_files,
            "image_files": self.image_files,
            "video_files": self.video_files,
            "ignored_files": self.ignored_files,
            "status_message_id": self.status_message_id,
        }

        self.metadata_path.write_text(
            json.dumps(
                payload,
                indent=2,
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

    @classmethod
    def read_metadata(
        cls,
        metadata_path: Path,
    ) -> "Job":
        """Restore a Job from its metadata JSON."""
        metadata_path = Path(metadata_path)

        try:
            payload = json.loads(
                metadata_path.read_text(
                    encoding="utf-8",
                )
            )
        except (OSError, json.JSONDecodeError) as exc:
            raise JobStateError(
                f"Could not read job metadata: {metadata_path}"
            ) from exc

        try:
            status = JobStatus(payload["status"])
        except (KeyError, ValueError) as exc:
            raise JobStateError(
                f"Invalid job status in metadata: {metadata_path}"
            ) from exc

        job = cls(
            job_id=str(payload["job_id"]),
            archive_name=_safe_archive_name(
                payload.get("archive_name", "archive.zip")
            ),
            source_message_id=payload.get("source_message_id"),
            chat_id=payload.get("chat_id"),
            sender_id=payload.get("sender_id"),
            archive_size_bytes=int(
                payload.get("archive_size_bytes", 0) or 0
            ),
            status=status,
            created_at=payload.get(
                "created_at",
                _timestamp(),
            ),
            started_at=payload.get("started_at"),
            finished_at=payload.get("finished_at"),
            error=payload.get("error"),
            cancel_requested=bool(
                payload.get("cancel_requested", False)
            ),
            total_files=int(
                payload.get("total_files", 0) or 0
            ),
            image_files=int(
                payload.get("image_files", 0) or 0
            ),
            video_files=int(
                payload.get("video_files", 0) or 0
            ),
            ignored_files=int(
                payload.get("ignored_files", 0) or 0
            ),
            status_message_id=payload.get(
                "status_message_id"
            ),
        )

        job._job_root = metadata_path.parent

        return job


class JobManager:
    """Manage jobs, their state transitions and persistent files."""

    def __init__(
        self,
        job_dir: Path | str = "./data/jobs",
        *,
        keep_files: bool = False,
    ) -> None:
        self.job_dir = Path(job_dir)
        self.keep_files = bool(keep_files)

        self.job_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        self._jobs: dict[str, Job] = {}
        self._queue: deque[str] = deque()

        self._load_existing_jobs()

    # ------------------------------------------------------------------ #
    # Loading
    # ------------------------------------------------------------------ #

    def _load_existing_jobs(self) -> None:
        """Load previously persisted job metadata."""
        if not self.job_dir.exists():
            return

        for metadata_path in self.job_dir.glob(
            "*/metadata.json"
        ):
            try:
                job = Job.read_metadata(metadata_path)
                job._job_root = metadata_path.parent
                self._jobs[job.job_id] = job

            except Exception:
                logger.exception(
                    "Failed to load job metadata: %s",
                    metadata_path,
                )

    # ------------------------------------------------------------------ #
    # Creation / lookup
    # ------------------------------------------------------------------ #

    def create_job(
        self,
        archive_name: str = "archive.zip",
        source_message_id: Optional[int] = None,
        chat_id: Optional[int] = None,
        sender_id: Optional[int] = None,
        archive_size_bytes: int = 0,
    ) -> Job:
        """Create and persist a new job."""
        job_id = generate_job_id()

        while job_id in self._jobs:
            job_id = generate_job_id()

        safe_name = _safe_archive_name(
            archive_name
        )

        root = self.job_dir / job_id

        job = Job(
            job_id=job_id,
            archive_name=safe_name,
            source_message_id=source_message_id,
            chat_id=chat_id,
            sender_id=sender_id,
            archive_size_bytes=int(
                archive_size_bytes or 0
            ),
        )

        job._job_root = root

        job.archive_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        job.extracted_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        job.write_metadata()

        self._jobs[job.job_id] = job

        logger.info(
            "Created job %s (archive=%s)",
            job.job_id,
            job.archive_name,
        )

        return job

    def get(self, job_id: str) -> Job:
        """Return a job or raise JobStateError."""
        job = self._jobs.get(job_id)

        if job is None:
            raise JobStateError(
                f"Unknown job: {job_id}"
            )

        return job

    def all_jobs(self) -> list[Job]:
        """Return all jobs ordered by creation order."""
        return list(
            sorted(
                self._jobs.values(),
                key=lambda job: job.created_at,
            )
        )

    def latest_job(self) -> Optional[Job]:
        """Return the most recently created job."""
        jobs = self.all_jobs()

        if not jobs:
            return None

        return jobs[-1]

    # ------------------------------------------------------------------ #
    # Queue
    # ------------------------------------------------------------------ #

    def enqueue(self, job_id: str) -> None:
        """Add a job to the JobManager bookkeeping queue."""
        job = self.get(job_id)

        if job.is_terminal:
            raise JobStateError(
                f"Cannot enqueue terminal job: {job_id}"
            )

        if job_id not in self._queue:
            self._queue.append(job_id)

        if job.status is JobStatus.RECEIVED:
            self.set_status(
                job_id,
                JobStatus.QUEUED,
            )

    def dequeue(self) -> Optional[str]:
        """Remove and return the oldest queued job."""
        if not self._queue:
            return None

        return self._queue.popleft()

    def remove_from_queue(self, job_id: str) -> None:
        """
        Remove one specific job from the bookkeeping queue.

        IMPORTANT:
        This must NOT use dequeue(), because the pipeline worker has its
        own asyncio queue. Calling dequeue() here could remove a different
        pending job.
        """
        try:
            self._queue.remove(job_id)

        except ValueError:
            pass

    def queue_size(self) -> int:
        """Return the number of jobs waiting in bookkeeping queue."""
        return len(self._queue)

    # ------------------------------------------------------------------ #
    # Status
    # ------------------------------------------------------------------ #

    def set_status(
        self,
        job_id: str,
        new_status: JobStatus,
    ) -> Job:
        """Perform a validated state transition."""
        job = self.get(job_id)

        if not isinstance(new_status, JobStatus):
            try:
                new_status = JobStatus(new_status)
            except ValueError as exc:
                raise JobStateError(
                    f"Invalid job status: {new_status!r}"
                ) from exc

        if job.status is new_status:
            return job

        allowed = ALLOWED_TRANSITIONS.get(
            job.status,
            set(),
        )

        if new_status not in allowed:
            raise JobStateError(
                f"Invalid transition for {job_id}: "
                f"{job.status.value} -> {new_status.value}"
            )

        old_status = job.status

        job.status = new_status

        if (
            new_status is JobStatus.DOWNLOADING
            and job.started_at is None
        ):
            job.started_at = _timestamp()

        if new_status in TERMINAL_STATES:
            job.finished_at = _timestamp()

        job.write_metadata()

        logger.info(
            "Job %s status %s -> %s",
            job_id,
            old_status.value,
            new_status.value,
        )

        return job

    # Compatibility aliases used by some older code.

    def update_status(
        self,
        job_id: str,
        status: JobStatus,
    ) -> Job:
        return self.set_status(
            job_id,
            status,
        )

    def mark_failed(
        self,
        job_id: str,
        reason: str,
    ) -> Job:
        return self.fail(
            job_id,
            reason,
        )

    # ------------------------------------------------------------------ #
    # Cancellation
    # ------------------------------------------------------------------ #

    def request_cancel(
        self,
        job_id: str,
    ) -> bool:
        """Request cancellation without immediately stopping the worker."""
        job = self.get(job_id)

        if job.is_terminal:
            return False

        job.cancel_requested = True
        job.write_metadata()

        return True

    def is_cancelled(
        self,
        job_id: str,
    ) -> bool:
        """Return whether cancellation was requested."""
        return self.get(job_id).cancel_requested

    def cancel(
        self,
        job_id: str,
    ) -> Job:
        """Mark a non-terminal job as cancelled."""
        job = self.get(job_id)

        if job.is_terminal:
            if job.status is JobStatus.CANCELLED:
                return job

            raise JobStateError(
                f"Cannot cancel terminal job: {job_id}"
            )

        return self.set_status(
            job_id,
            JobStatus.CANCELLED,
        )

    # ------------------------------------------------------------------ #
    # Failure
    # ------------------------------------------------------------------ #

    def fail(
        self,
        job_id: str,
        reason: str,
    ) -> Job:
        """Mark a non-terminal job as failed."""
        job = self.get(job_id)

        if job.is_terminal:
            return job

        job.error = str(reason)

        return self.set_status(
            job_id,
            JobStatus.FAILED,
        )

    # ------------------------------------------------------------------ #
    # Counts / progress
    # ------------------------------------------------------------------ #

    def update_counts(
        self,
        job_id: str,
        counts: Optional[dict[str, int]] = None,
        *,
        images: Optional[int] = None,
        videos: Optional[int] = None,
        ignored: Optional[int] = None,
    ) -> Job:
        """
        Update media counts.

        Supports both:

            update_counts(job_id, {"image_files": 2, ...})

        and:

            update_counts(job_id, images=2, videos=1, ignored=0)
        """
        job = self.get(job_id)

        if counts is not None:
            total = counts.get(
                "total_files",
                counts.get("total", None),
            )

            image_count = counts.get(
                "image_files",
                counts.get("images", None),
            )

            video_count = counts.get(
                "video_files",
                counts.get("videos", None),
            )

            ignored_count = counts.get(
                "ignored_files",
                counts.get("ignored", None),
            )

            if total is not None:
                job.total_files = int(total)

            if image_count is not None:
                job.image_files = int(image_count)

            if video_count is not None:
                job.video_files = int(video_count)

            if ignored_count is not None:
                job.ignored_files = int(ignored_count)

        if images is not None:
            job.image_files = int(images)

        if videos is not None:
            job.video_files = int(videos)

        if ignored is not None:
            job.ignored_files = int(ignored)

        if counts is not None and "total_files" not in counts:
            job.total_files = (
                job.image_files
                + job.video_files
                + job.ignored_files
            )

        job.write_metadata()

        return job

    def set_status_message(
        self,
        job_id: str,
        message_id: int,
    ) -> Job:
        """Store the Telegram status message ID."""
        job = self.get(job_id)

        job.status_message_id = int(message_id)
        job.write_metadata()

        return job

    # ------------------------------------------------------------------ #
    # Active jobs
    # ------------------------------------------------------------------ #

    def active_jobs(self) -> list[Job]:
        """Return all non-terminal jobs."""
        return [
            job
            for job in self.all_jobs()
            if not job.is_terminal
        ]

    def active_job(self) -> Optional[Job]:
        """Return the first active job, if any."""
        jobs = self.active_jobs()

        if not jobs:
            return None

        return jobs[0]

    # ------------------------------------------------------------------ #
    # Cleanup
    # ------------------------------------------------------------------ #

    def cleanup_job_files(
        self,
        job_id: str,
        *,
        force: bool = False,
    ) -> list[str]:
        """
        Remove downloaded archive and extracted files.

        Metadata is intentionally preserved.
        """
        job = self.get(job_id)

        if self.keep_files and not force:
            return []

        removed: list[str] = []

        targets = (
            ("archive", job.archive_dir),
            ("extracted", job.extracted_dir),
        )

        for name, path in targets:
            if not path.exists():
                continue

            try:
                shutil.rmtree(path)
                removed.append(name)

            except OSError:
                logger.exception(
                    "Failed to remove %s for job %s",
                    name,
                    job_id,
                )
                raise

        if removed:
            logger.info(
                "Cleaned job %s (removed: %s)",
                job_id,
                ", ".join(removed),
            )

        return removed
