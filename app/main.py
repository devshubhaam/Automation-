"""Pipeline worker + application entry point (PART 1).

Pipeline stages implemented here:

    QUEUED -> DOWNLOADING -> EXTRACTING -> SCANNING -> COMPLETED

and on error/cancel -> FAILED / CANCELLED with safe cleanup.

Nothing from PART 2+ (ImgBB, Telegraph, video bot) exists here by design: the
pipeline stops right after media detection and reporting.
"""

from __future__ import annotations

import asyncio
import logging
import signal
import sys
from pathlib import Path
from typing import Optional

from .archive_processor import (
    ArchiveError,
    ExtractionCancelledError,
    safe_extract,
    validate_archive,
)
from .config import ConfigError, Settings
from .job_manager import JobManager, JobStatus
from .logging_config import setup_logging
from .media_scanner import scan_directory
from .progress import render_progress
from .telegram_client import TelegramUserbot

__all__ = ["PipelineWorker", "main", "run"]

logger = logging.getLogger("app.main")


class PipelineWorker:
    """Sequential async worker processing one job at a time."""

    def __init__(self, settings: Settings, job_manager: JobManager, userbot: TelegramUserbot) -> None:
        self.settings = settings
        self.job_manager = job_manager
        self.userbot = userbot
        self._queue: "asyncio.Queue[str]" = asyncio.Queue()
        self._tasks: list[asyncio.Task] = []
        self._current: Optional[asyncio.Task] = None
        self._current_job_id: Optional[str] = None
        self._stopping = False

    # ------------------------------------------------------------------ #
    # Queue plumbing
    # ------------------------------------------------------------------ #
    async def submit(self, job) -> None:  # noqa: ANN001
        await self._queue.put(job.job_id)
        logger.info("Job %s submitted to queue (size=%d)", job.job_id, self._queue.qsize())

    async def start_workers(self) -> None:
        for index in range(max(1, self.settings.worker_count)):
            task = asyncio.create_task(self._worker_loop(index), name=f"worker-{index}")
            self._tasks.append(task)
        logger.info("Started %d worker(s)", len(self._tasks))

    async def _worker_loop(self, index: int) -> None:
        while not self._stopping:
            try:
                job_id = await self._queue.get()
            except asyncio.CancelledError:  # pragma: no cover
                break
            job = self.job_manager.find(job_id)
            if job is None:
                self._queue.task_done()
                continue
            self._current_job_id = job_id
            self._current = asyncio.current_task()
            try:
                await self._process(job)
            except asyncio.CancelledError:
                logger.warning("Job %s worker task cancelled", job_id)
                self._finalize_cancelled(job)
                raise
            except Exception as exc:  # noqa: BLE001 - top-level guard
                logger.exception("Unexpected failure while processing job %s", job_id)
                self._finalize_failed(job, exc, stage="processing")
            finally:
                self._queue.task_done()
                self._current_job_id = None
                self._current = None

    async def _stop_workers(self) -> None:
        self._stopping = True
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

    # ------------------------------------------------------------------ #
    # Cancellation helpers
    # ------------------------------------------------------------------ #
    def _is_cancelled(self, job) -> bool:  # noqa: ANN001
        fresh = self.job_manager.find(job.job_id)
        if fresh is None:
            return True
        return fresh.cancel_requested or fresh.status == JobStatus.CANCELLED

    def _finalize_cancelled(self, job) -> None:  # noqa: ANN001
        fresh = self.job_manager.find(job.job_id)
        if fresh is None or fresh.is_terminal:
            return
        self.job_manager.cancel(job.job_id)
        self.job_manager.cleanup_job_files(job.job_id, force=True)

    def _finalize_failed(self, job, exc: BaseException, *, stage: str) -> None:  # noqa: ANN001
        fresh = self.job_manager.find(job.job_id)
        if fresh is None or fresh.is_terminal:
            return
        if isinstance(exc, ExtractionCancelledError):
            self._finalize_cancelled(job)
            return
        reason = str(exc) or exc.__class__.__name__
        logger.error("Job %s failed at stage %s: %s", job.job_id, stage, reason)
        try:
            self.job_manager.fail(job.job_id, reason)
        except Exception:  # pragma: no cover - defensive
            logger.exception("Could not mark job %s as FAILED", job.job_id)
        self.job_manager.cleanup_job_files(job.job_id, force=True)
        asyncio.create_task(self._notify_failure(job, reason, stage))

    async def _notify_failure(self, job, reason: str, stage: str) -> None:  # noqa: ANN001
        try:
            fresh = self.job_manager.find(job.job_id)
            if fresh is None or fresh.status_message_id is None:
                return
            await self.userbot.edit_message(
                fresh.status_message_id, render_progress(fresh, detail=stage)
            )
        except Exception as exc:  # pragma: no cover - network side
            logger.warning("Could not send failure message: %s", exc)

    # ------------------------------------------------------------------ #
    # The pipeline itself
    # ------------------------------------------------------------------ #
    async def _process(self, job) -> None:  # noqa: ANN001
        reporter = _JobReporter(self.userbot, job)

        # ---- DOWNLOADING -------------------------------------------------- #
        if self._is_cancelled(job):
            self._finalize_cancelled(job)
            return

        self.job_manager.set_status(job.job_id, JobStatus.DOWNLOADING)
        await reporter.update(job, detail="Saving ZIP into the job directory...")

        archive_path = job.archive_path
        archive_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            await self._download(job, archive_path)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            self._finalize_failed(job, exc, stage="ZIP download")
            return

        if self._is_cancelled(job):
            self._finalize_cancelled(job)
            return
        logger.info("ZIP downloaded: %s", archive_path.name)
        await reporter.update(job, detail="ZIP downloaded. 📥")

        # ---- EXTRACTING --------------------------------------------------- #
        self.job_manager.set_status(job.job_id, JobStatus.EXTRACTING)
        await reporter.update(job, detail="Validating archive safety...")
        logger.info("ZIP extraction started: %s", job.archive_name)

        try:
            info = validate_archive(archive_path, self.settings)
            logger.info(
                "Archive validated (members=%d, files=%d, declared=%d bytes)",
                info.member_count,
                info.file_count,
                info.total_uncompressed_bytes,
            )
            await reporter.update(job, detail="Extracting archive safely...")
            result = safe_extract(
                archive_path,
                job.extracted_dir,
                self.settings,
                should_cancel=lambda: self._is_cancelled(job),
            )
        except ExtractionCancelledError:
            self._finalize_cancelled(job)
            return
        except ArchiveError as exc:
            self._finalize_failed(job, exc, stage=getattr(exc, "stage", "ZIP extraction"))
            return
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            self._finalize_failed(job, exc, stage="ZIP extraction")
            return

        logger.info("ZIP extraction completed (%d files)", result.extracted_files)
        if self._is_cancelled(job):
            self._finalize_cancelled(job)
            return

        # ---- SCANNING ----------------------------------------------------- #
        self.job_manager.set_status(job.job_id, JobStatus.SCANNING)
        await reporter.update(job, detail="Recursively scanning extracted files...")
        logger.info("Media scan started")

        try:
            scan = await asyncio.to_thread(
                scan_directory, job.extracted_dir, scan_root=job.extracted_dir
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            self._finalize_failed(job, exc, stage="media scan")
            return

        if self._is_cancelled(job):
            self._finalize_cancelled(job)
            return

        counts = scan.counts()
        self.job_manager.update_counts(job.job_id, counts)
        logger.info(
            "Found %d images and %d videos (%d ignored)",
            scan.image_count,
            scan.video_count,
            scan.ignored_count,
        )

        # ---- REPORT ------------------------------------------------------- #
        await reporter.update(job, detail="📊 Generating report...")
        self.job_manager.set_status(job.job_id, JobStatus.COMPLETED)
        await reporter.finish(
            job,
            images=[f.relative_path for f in scan.images],
            videos=[f.relative_path for f in scan.videos],
            ignored=scan.relative_ignored(),
        )
        logger.info("Job completed: %s", job.job_id)

        # ---- CLEANUP ------------------------------------------------------ #
        if not self.settings.keep_job_files:
            await reporter.update(job, footer="🧹 Cleaning up temporary files...")
        removed = self.job_manager.cleanup_job_files(job.job_id)
        if removed:
            logger.info("Cleaned up %s for job %s", ", ".join(removed), job.job_id)
            await reporter.update(job, footer="🧹 Temporary files cleaned up.")

    # ------------------------------------------------------------------ #
    # Download
    # ------------------------------------------------------------------ #
    async def _download(self, job, destination: Path) -> None:  # noqa: ANN001
        client = self.userbot.client
        entity = job.chat_id if job.chat_id is not None else "me"

        def _progress(received: int, total: int) -> None:
            # Telethon callback runs in the event loop; keep it cheap and silent.
            return None

        await client.download_media(
            await client.get_messages(entity, ids=job.source_message_id),
            file=str(destination),
            progress_callback=_progress,
        )

        if not destination.exists() or destination.stat().st_size == 0:
            raise ArchiveError("downloaded ZIP is missing or empty")
        job.archive_size_bytes = destination.stat().st_size
        job.write_metadata()


class _JobReporter:
    """Small helper that edits a job's existing status message."""

    def __init__(self, userbot: TelegramUserbot, job) -> None:  # noqa: ANN001
        self.userbot = userbot
        self.job = job

    async def update(self, job, **kwargs) -> None:  # noqa: ANN001
        message_id = self.job.status_message_id
        if message_id is None:
            return
        text = render_progress(job, **kwargs)
        try:
            await self.userbot.edit_message(message_id, text)
        except Exception as exc:  # pragma: no cover - network side
            logger.warning("Progress update failed: %s", exc)

    async def finish(self, job, **kwargs) -> None:  # noqa: ANN001
        await self.update(job, **kwargs)


# ---------------------------------------------------------------------- #
# Entry point
# ---------------------------------------------------------------------- #
async def run(settings: Settings) -> None:
    """Wire everything together and run until interrupted."""
    settings.ensure_directories()
    job_manager = JobManager(settings.job_dir, keep_files=settings.keep_job_files)
    userbot = TelegramUserbot(settings, job_manager)
    pipeline = PipelineWorker(settings, job_manager, userbot)
    userbot.pipeline = pipeline

    await userbot.start()
    await pipeline.start_workers()

    logger.info("Userbot is running. Send a ZIP archive or use /ping, /status, /cancel.")

    stop_event = asyncio.Event()

    def _request_stop(*_args) -> None:
        logger.info("Shutdown requested")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_stop)
        except (NotImplementedError, RuntimeError):  # pragma: no cover - Windows
            pass

    try:
        await stop_event.wait()
    finally:
        await pipeline._stop_workers()
        await userbot.stop()
        logger.info("Shutdown complete")


def main(argv: Optional[list[str]] = None) -> int:
    """Synchronous entry point used by ``python -m app.main``."""
    try:
        settings = Settings.from_env()
    except ConfigError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        print("Copy .env.example to .env and fill in API_ID / API_HASH.", file=sys.stderr)
        return 2

    setup_logging(settings)
    logger.info("Starting Telegram Media Processor (PART 1)")

    try:
        asyncio.run(run(settings))
    except KeyboardInterrupt:  # pragma: no cover
        logger.info("Interrupted by user")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
