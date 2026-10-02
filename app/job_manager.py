from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Optional

from .archive_processor import (
    ArchiveError,
    extract_archive,
    validate_archive,
)
from .config import settings
from .job_manager import Job, JobManager, JobStatus
from .logging_config import setup_logging
from .media_scanner import scan_directory
from .telegram_client import TelegramUserbot

try:
    from .login_bot import LoginBot
except ImportError:
    LoginBot = None


logger = logging.getLogger(__name__)


class HealthServer:
    """Minimal HTTP health server for Koyeb Web Service."""

    def __init__(self, host: str = "0.0.0.0", port: int = 8000):
        self.host = host
        self.port = port
        self.server: Optional[asyncio.AbstractServer] = None

    async def start(self) -> None:
        self.server = await asyncio.start_server(
            self._handle_client,
            self.host,
            self.port,
        )

        logger.info(
            "Health server listening on %s:%s",
            self.host,
            self.port,
        )

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            await reader.read(4096)

            response_body = b"OK\n"

            response = (
                b"HTTP/1.1 200 OK\r\n"
                b"Content-Type: text/plain; charset=utf-8\r\n"
                b"Content-Length: "
                + str(len(response_body)).encode()
                + b"\r\n"
                b"Connection: close\r\n"
                b"\r\n"
                + response_body
            )

            writer.write(response)
            await writer.drain()

        except Exception:
            logger.exception("Health server request failed")

        finally:
            writer.close()

            try:
                await writer.wait_closed()
            except Exception:
                pass

    async def stop(self) -> None:
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
            self.server = None


class PipelineWorker:
    """
    Background worker responsible for processing ZIP jobs.

    Pipeline:
        QUEUED
          ↓
        DOWNLOADING
          ↓
        EXTRACTING
          ↓
        SCANNING
          ↓
        COMPLETED
    """

    def __init__(
        self,
        job_manager: JobManager,
        telegram_client: TelegramUserbot,
    ):
        self.job_manager = job_manager
        self.telegram_client = telegram_client

        self._queue: asyncio.Queue[Job] = asyncio.Queue()
        self._workers: list[asyncio.Task] = []

        self._running = False

    async def start(self, worker_count: int = 1) -> None:
        if self._running:
            return

        self._running = True

        for index in range(worker_count):
            task = asyncio.create_task(
                self._worker_loop(index + 1),
                name=f"pipeline-worker-{index + 1}",
            )

            self._workers.append(task)

            logger.info(
                "Pipeline worker %s started",
                index + 1,
            )

    async def stop(self) -> None:
        if not self._running:
            return

        self._running = False

        for task in self._workers:
            task.cancel()

        if self._workers:
            await asyncio.gather(
                *self._workers,
                return_exceptions=True,
            )

        self._workers.clear()

        logger.info("Pipeline workers stopped")

    async def submit(self, job: Job) -> None:
        await self._queue.put(job)

    async def _worker_loop(self, worker_id: int) -> None:
        logger.info(
            "Pipeline worker %s started",
            worker_id,
        )

        while self._running:
            job: Optional[Job] = None

            try:
                job = await self._queue.get()

                logger.debug(
                    "Worker %s picked job %s",
                    worker_id,
                    job.job_id,
                )

                await self._process(job)

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.exception(
                    "Pipeline worker %s failed while processing job",
                    worker_id,
                )

            finally:
                if job is not None:
                    self._queue.task_done()

    async def _process(self, job: Job) -> None:
        job_id = job.job_id

        try:
            # ---------------------------------------------------------
            # Cancellation check
            # ---------------------------------------------------------
            if self.job_manager.is_cancelled(job_id):
                logger.info(
                    "Job %s cancelled before processing",
                    job_id,
                )

                self.job_manager.update_status(
                    job_id,
                    JobStatus.CANCELLED,
                )

                return

            # ---------------------------------------------------------
            # DOWNLOAD
            # ---------------------------------------------------------
            self.job_manager.update_status(
                job_id,
                JobStatus.DOWNLOADING,
            )

            logger.info(
                "Job %s: downloading archive",
                job_id,
            )

            archive_path = await self._download(job)

            logger.info(
                "Job %s: download completed: %s",
                job_id,
                archive_path,
            )

            # ---------------------------------------------------------
            # Validate downloaded archive
            # ---------------------------------------------------------
            validate_archive(
                archive_path,
                max_archive_size_mb=settings.max_archive_size_mb,
                max_files=settings.max_files_per_archive,
            )

            # ---------------------------------------------------------
            # Cancellation check
            # ---------------------------------------------------------
            if self.job_manager.is_cancelled(job_id):
                logger.info(
                    "Job %s cancelled after download",
                    job_id,
                )

                self.job_manager.update_status(
                    job_id,
                    JobStatus.CANCELLED,
                )

                return

            # ---------------------------------------------------------
            # EXTRACT
            # ---------------------------------------------------------
            self.job_manager.update_status(
                job_id,
                JobStatus.EXTRACTING,
            )

            logger.info(
                "Job %s: extracting archive",
                job_id,
            )

            extracted_dir, extracted_files, extracted_bytes = (
                extract_archive(
                    archive_path,
                    max_extracted_size_mb=settings.max_extracted_size_mb,
                    max_files=settings.max_files_per_archive,
                )
            )

            logger.info(
                "Job %s: extracted %s files (%s bytes)",
                job_id,
                extracted_files,
                extracted_bytes,
            )

            # ---------------------------------------------------------
            # Cancellation check
            # ---------------------------------------------------------
            if self.job_manager.is_cancelled(job_id):
                logger.info(
                    "Job %s cancelled after extraction",
                    job_id,
                )

                self.job_manager.update_status(
                    job_id,
                    JobStatus.CANCELLED,
                )

                return

            # ---------------------------------------------------------
            # SCAN
            # ---------------------------------------------------------
            self.job_manager.update_status(
                job_id,
                JobStatus.SCANNING,
            )

            logger.info(
                "Job %s: scanning media",
                job_id,
            )

            scan_result = scan_directory(
                Path(extracted_dir),
            )

            images = getattr(
                scan_result,
                "images",
                [],
            )

            videos = getattr(
                scan_result,
                "videos",
                [],
            )

            ignored = getattr(
                scan_result,
                "ignored",
                [],
            )

            self.job_manager.update_counts(
                job_id,
                images=len(images),
                videos=len(videos),
                ignored=len(ignored),
            )

            # ---------------------------------------------------------
            # COMPLETED
            # ---------------------------------------------------------
            self.job_manager.update_status(
                job_id,
                JobStatus.COMPLETED,
            )

            logger.info(
                "Job %s: processing completed "
                "(images=%s videos=%s ignored=%s)",
                job_id,
                len(images),
                len(videos),
                len(ignored),
            )

        except asyncio.CancelledError:
            logger.warning(
                "Job %s processing task cancelled",
                job_id,
            )

            try:
                self.job_manager.update_status(
                    job_id,
                    JobStatus.CANCELLED,
                )
            except Exception:
                logger.exception(
                    "Job %s: failed to mark CANCELLED",
                    job_id,
                )

            raise

        except ArchiveError as exc:
            logger.warning(
                "Job %s archive error: %s",
                job_id,
                exc,
            )

            try:
                self.job_manager.mark_failed(
                    job_id,
                    str(exc),
                )
            except Exception:
                logger.exception(
                    "Job %s: failed to mark archive error",
                    job_id,
                )

        except Exception as exc:
            logger.exception(
                "Job %s processing failed",
                job_id,
            )

            try:
                self.job_manager.mark_failed(
                    job_id,
                    str(exc),
                )
            except Exception:
                logger.exception(
                    "Job %s: failed to mark FAILED",
                    job_id,
                )

        finally:
            # ---------------------------------------------------------
            # IMPORTANT QUEUE FIX
            #
            # telegram_client.py currently puts the job into
            # JobManager's queue AND PipelineWorker has its own queue.
            #
            # Therefore, when this worker finishes a job, remove ONLY
            # this specific job from JobManager's pending queue.
            #
            # Do NOT call dequeue() here because that could remove
            # another job waiting behind this one.
            # ---------------------------------------------------------
            try:
                self.job_manager.remove_from_queue(job_id)

            except Exception:
                logger.exception(
                    "Job %s: failed to remove from queue",
                    job_id,
                )

            # ---------------------------------------------------------
            # Cleanup downloaded/extracted files
            # ---------------------------------------------------------
            try:
                self.job_manager.cleanup_job_files(job_id)

            except Exception:
                logger.exception(
                    "Job %s: cleanup failed",
                    job_id,
                )

    async def _download(self, job: Job) -> Path:
        """
        Download Telegram document to the job archive directory.
        """

        job_dir = Path(job.job_dir)

        archive_dir = job_dir / "archive"
        archive_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        filename = Path(job.filename).name

        archive_path = archive_dir / filename

        await self.telegram_client.download_document(
            job.message,
            archive_path,
        )

        return archive_path


async def main() -> None:
    setup_logging()

    logger.info(
        "Starting Telegram Media Processor (PART 1)"
    )

    # -------------------------------------------------------------
    # Health server
    # -------------------------------------------------------------
    health_server = HealthServer(
        host="0.0.0.0",
        port=8000,
    )

    await health_server.start()

    # -------------------------------------------------------------
    # Job manager
    # -------------------------------------------------------------
    job_manager = JobManager()

    # -------------------------------------------------------------
    # Telegram userbot
    # -------------------------------------------------------------
    telegram_client = TelegramUserbot(
        job_manager=job_manager,
    )

    logger.info("Telegram client starting")

    await telegram_client.start()

    # -------------------------------------------------------------
    # Login / QR authentication
    # -------------------------------------------------------------
    if not await telegram_client.is_authorized():

        logger.info(
            "Telegram session is not authorized; "
            "waiting for QR login"
        )

        if LoginBot is None:
            raise RuntimeError(
                "LoginBot is unavailable but Telegram "
                "authentication is required"
            )

        logger.info(
            "Telegram userbot is not authorized; "
            "starting QR login bot"
        )

        login_bot = LoginBot(
            telegram_client=telegram_client,
        )

        await login_bot.start()

        try:
            await login_bot.wait_until_authenticated()

        finally:
            await login_bot.stop()

        logger.info(
            "QR authentication completed"
        )

    # -------------------------------------------------------------
    # Telegram handlers
    # -------------------------------------------------------------
    await telegram_client.register_handlers()

    # -------------------------------------------------------------
    # Pipeline
    # -------------------------------------------------------------
    pipeline = PipelineWorker(
        job_manager=job_manager,
        telegram_client=telegram_client,
    )

    worker_count = max(
        1,
        settings.worker_count,
    )

    logger.info(
        "Starting %s pipeline worker(s)",
        worker_count,
    )

    await pipeline.start(
        worker_count=worker_count,
    )

    # Give Telegram client access to pipeline.
    telegram_client.pipeline = pipeline

    logger.info(
        "Telegram Media Processor is ready"
    )

    # -------------------------------------------------------------
    # Keep application alive
    # -------------------------------------------------------------
    try:
        await asyncio.Event().wait()

    except asyncio.CancelledError:
        logger.info(
            "Main application cancelled"
        )

    finally:
        logger.info(
            "Stopping Telegram Media Processor"
        )

        try:
            await pipeline.stop()
        except Exception:
            logger.exception(
                "Failed to stop pipeline"
            )

        try:
            await telegram_client.stop()
        except Exception:
            logger.exception(
                "Failed to stop Telegram client"
            )

        try:
            await health_server.stop()
        except Exception:
            logger.exception(
                "Failed to stop health server"
            )

        logger.info(
            "Telegram Media Processor stopped"
        )


if __name__ == "__main__":
    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        logger.info(
            "Application stopped by user"
        )
