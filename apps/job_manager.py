"""Job model and in-memory job manager.

PART 1 keeps job state in memory only - a database is deliberately not added.
The interface (``create``, ``get``, ``set_status``, ``list_jobs``, ``cleanup``)
is written so a SQLite-backed implementation can replace ``JobManager`` later
without changing any caller.
"""

from __future__ import annotations

import json
import logging
import shutil
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Dict, Iterator, List, Optional

from .utils import is_within_directory

__all__ = [
    "JobStatus",
    "JobStateError",
    "Job",
    "JobManager",
    "generate_job_id",
    "new_job_id",
    "ALLOWED_TRANSITIONS",
]

logger = logging.getLogger("app.job_manager")


class JobStatus(str, Enum):
    """Explicit job lifecycle states."""

    RECEIVED = "RECEIVED"
    QUEUED = "QUEUED"
    DOWNLOADING = "DOWNLOADING"
    EXTRACTING = "EXTRACTING"
    SCANNING = "SCANNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


TERMINAL_STATUSES = frozenset({JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED})

ALLOWED_TRANSITIONS: Dict[JobStatus, frozenset] = {
    JobStatus.RECEIVED: frozenset(
        {JobStatus.QUEUED, JobStatus.DOWNLOADING, JobStatus.FAILED, JobStatus.CANCELLED}
    ),
    JobStatus.QUEUED: frozenset(
        {JobStatus.DOWNLOADING, JobStatus.FAILED, JobStatus.CANCELLED}
    ),
    JobStatus.DOWNLOADING: frozenset(
        {JobStatus.EXTRACTING, JobStatus.FAILED, JobStatus.CANCELLED}
    ),
    JobStatus.EXTRACTING: frozenset(
        {JobStatus.SCANNING, JobStatus.FAILED, JobStatus.CANCELLED}
    ),
    JobStatus.SCANNING: frozenset(
        {JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED}
    ),
    JobStatus.COMPLETED: frozenset(),
    JobStatus.FAILED: frozenset(),
    JobStatus.CANCELLED: frozenset(),
}


class JobStateError(RuntimeError):
    """Raised on an illegal status transition or unknown job id."""


def generate_job_id(now: Optional[datetime] = None) -> str:
    """Build a unique, sortable job id: ``JOB-YYYYMMDD-HHMMSS-XXXX``."""
    moment = now or datetime.now(timezone.utc)
    suffix = uuid.uuid4().hex[:4].upper()
    return f"JOB-{moment.strftime('%Y%m%d-%H%M%S')}-{suffix}"


# Backwards-friendly alias
new_job_id = generate_job_id


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass
class Job:
    """State of a single archive-processing job."""

    job_id: str
    source_message_id: Optional[int] = None
    chat_id: Optional[int] = None
    sender_id: Optional[int] = None
    archive_name: str = ""
    archive_size_bytes: int = 0
    job_directory: Path = field(default_factory=Path)
    status: JobStatus = JobStatus.RECEIVED
    created_at: str = field(default_factory=_utc_now_iso)
    updated_at: str = field(default_factory=_utc_now_iso)
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    status_message_id: Optional[int] = None

    total_files: int = 0
    image_files: int = 0
    video_files: int = 0
    ignored_files: int = 0

    error: Optional[str] = None
    cancel_requested: bool = False

    # -- derived paths ------------------------------------------------------ #
    @property
    def archive_path(self) -> Path:
        return Path(self.job_directory) / "archive" / (self.archive_name or "archive.zip")

    @property
    def archive_dir(self) -> Path:
        return Path(self.job_directory) / "archive"

    @property
    def extracted_dir(self) -> Path:
        return Path(self.job_directory) / "extracted"

    @property
    def metadata_path(self) -> Path:
        return Path(self.job_directory) / "metadata.json"

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL_STATUSES

    # -- serialisation ------------------------------------------------------ #
    def to_dict(self) -> Dict[str, object]:
        data = asdict(self)
        data["job_directory"] = str(self.job_directory)
        data["status"] = self.status.value
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, object]) -> "Job":
        payload = dict(data)
        payload["job_directory"] = Path(str(payload.get("job_directory", ".")))
        payload["status"] = JobStatus(str(payload.get("status", JobStatus.RECEIVED.value)))
        allowed = {f for f in cls.__dataclass_fields__}  # type: ignore[attr-defined]
        return cls(**{k: v for k, v in payload.items() if k in allowed})

    def write_metadata(self) -> None:
        """Persist job metadata to ``<job>/metadata.json`` (never any secrets)."""
        path = self.metadata_path
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(self.to_dict(), handle, indent=2, ensure_ascii=False)

    @classmethod
    def read_metadata(cls, path: Path) -> "Job":
        with open(path, "r", encoding="utf-8") as handle:
            return cls.from_dict(json.load(handle))


class JobManager:
    """In-memory registry of jobs with guarded status transitions."""

    def __init__(self, base_dir: Path, *, keep_files: bool = False) -> None:
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
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
        job_id = generate_job_id()
        while job_id in self._jobs:  # pragma: no cover - collision is ~impossible
            job_id = generate_job_id()

        job_directory = self.base_dir / job_id
        (job_directory / "archive").mkdir(parents=True, exist_ok=True)
        (job_directory / "extracted").mkdir(parents=True, exist_ok=True)

        job = Job(
            job_id=job_id,
            source_message_id=source_message_id,
            chat_id=chat_id,
            sender_id=sender_id,
            archive_name=Path(archive_name or "archive.zip").name,
            archive_size_bytes=archive_size_bytes,
            job_directory=job_directory,
            status=JobStatus.RECEIVED,
        )
        self._jobs[job_id] = job
        job.write_metadata()
        logger.info("Created job %s (archive=%s)", job_id, job.archive_name)
        return job

    # -- access ------------------------------------------------------------- #
    def get(self, job_id: str) -> Job:
        try:
            return self._jobs[job_id]
        except KeyError as exc:
            raise JobStateError(f"unknown job id: {job_id}") from exc

    def find(self, job_id: str) -> Optional[Job]:
        return self._jobs.get(job_id)

    def all_jobs(self) -> List[Job]:
        return sorted(self._jobs.values(), key=lambda j: j.created_at)

    def __iter__(self) -> Iterator[Job]:
        return iter(self.all_jobs())

    def __len__(self) -> int:
        return len(self._jobs)

    def active_jobs(self) -> List[Job]:
        return [job for job in self.all_jobs() if not job.is_terminal]

    def active_job(self) -> Optional[Job]:
        for job in reversed(self.all_jobs()):
            if not job.is_terminal:
                return job
        return None

    def latest_job(self) -> Optional[Job]:
        jobs = self.all_jobs()
        return jobs[-1] if jobs else None

    # -- state machine ------------------------------------------------------ #
    def can_transition(self, current: JobStatus, new: JobStatus) -> bool:
        return new in ALLOWED_TRANSITIONS.get(current, frozenset())

    def set_status(self, job_id: str, new_status: JobStatus, *, error: Optional[str] = None) -> Job:
        job = self.get(job_id)
        new_status = JobStatus(new_status)

        if new_status == job.status:
            job.updated_at = _utc_now_iso()
            return job

        if not self.can_transition(job.status, new_status):
            raise JobStateError(
                f"illegal transition for {job_id}: {job.status.value} -> {new_status.value}"
            )

        previous = job.status
        job.status = new_status
        job.updated_at = _utc_now_iso()

        if new_status == JobStatus.DOWNLOADING and job.started_at is None:
            job.started_at = job.updated_at
        if new_status in TERMINAL_STATUSES:
            job.finished_at = job.updated_at
        if error is not None:
            job.error = error

        job.write_metadata()
        logger.info("Job %s status %s -> %s", job_id, previous.value, new_status.value)
        return job

    def fail(self, job_id: str, error: str) -> Job:
        return self.set_status(job_id, JobStatus.FAILED, error=error)

    def cancel(self, job_id: str) -> Job:
        job = self.get(job_id)
        if job.is_terminal:
            return job
        return self.set_status(job_id, JobStatus.CANCELLED)

    def request_cancel(self, job_id: str) -> bool:
        """Mark a running job as cancellation-requested (checked by workers)."""
        job = self.find(job_id)
        if job is None or job.is_terminal:
            return False
        job.cancel_requested = True
        job.updated_at = _utc_now_iso()
        job.write_metadata()
        return True

    def update_counts(self, job_id: str, counts: Dict[str, int]) -> Job:
        job = self.get(job_id)
        job.total_files = int(counts.get("total_files", job.total_files))
        job.image_files = int(counts.get("image_files", job.image_files))
        job.video_files = int(counts.get("video_files", job.video_files))
        job.ignored_files = int(counts.get("ignored_files", job.ignored_files))
        job.updated_at = _utc_now_iso()
        job.write_metadata()
        return job

    def set_status_message(self, job_id: str, message_id: Optional[int]) -> Job:
        job = self.get(job_id)
        job.status_message_id = message_id
        job.write_metadata()
        return job

    # -- queue -------------------------------------------------------------- #
    def enqueue(self, job_id: str) -> None:
        job = self.get(job_id)
        if job.status == JobStatus.RECEIVED:
            self.set_status(job_id, JobStatus.QUEUED)
        if job_id not in self._queue:
            self._queue.append(job_id)

    def dequeue(self) -> Optional[str]:
        while self._queue:
            job_id = self._queue.pop(0)
            job = self.find(job_id)
            if job is not None and not job.is_terminal:
                return job_id
        return None

    def queued_jobs(self) -> List[Job]:
        return [self._jobs[j] for j in self._queue if j in self._jobs]

    def queue_size(self) -> int:
        return len(self._queue)

    # -- cleanup ------------------------------------------------------------ #
    def cleanup_job_files(self, job_id: str, *, force: bool = False) -> List[str]:
        """Remove the job's ``archive/`` and ``extracted/`` directories.

        ``metadata.json`` is always kept. Nothing outside the job directory can
        ever be touched. Returns the list of removed subdirectory names.
        """
        job = self.get(job_id)
        if self.keep_files and not force:
            logger.info("KEEP_JOB_FILES=true - retaining files for job %s", job_id)
            return []

        removed: List[str] = []
        for name in ("archive", "extracted"):
            target = Path(job.job_directory) / name
            if not is_within_directory(self.base_dir, target) or target == Path(self.base_dir):
                logger.error("Refusing to clean path outside job root: %s", target)
                continue
            if target.exists():
                shutil.rmtree(target, ignore_errors=True)
                removed.append(name)
        if removed:
            logger.info("Cleaned job %s (removed: %s)", job_id, ", ".join(removed))
        return removed

    def forget(self, job_id: str) -> None:
        self._jobs.pop(job_id, None)
        if job_id in self._queue:
            self._queue.remove(job_id)
