"""Application entry point for the Telegram media processor."""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from .archive_processor import safe_extract, validate_archive
from .config import ConfigError, Settings
from .job_manager import JobManager, JobStatus
from .logging_config import setup_logging
from .login_bot import LoginBot
from .media_scanner import scan_directory
from .telegram_client import TelegramUserbot

logger = logging.getLogger("app.main")


class PipelineWorker:
    """Background worker for Part 1 archive processing."""

    def __init__(
        self,
        settings: Settings,
        job_manager: JobManager,
    ) -> None:
        self.settings = settings
        self.job_manager = job_manager

        self._queue: asyncio.Queue = asyncio.Queue()
        self._workers: list[asyncio.Task] = []
        self._stopping = False

    async def submit(self, job) -> None:
        """Submit a job to the processing queue."""
        await self._queue.put(job)

        logger.info(
            "Job submitted to pipeline: %s",
            job.job_id,
        )

    async def start(self) -> None:
        """Start pipeline workers."""
        self._stopping = False

        worker_count = max(
            1,
            int(self.settings.worker_count),
        )

        logger.info(
            "Starting %s pipeline worker(s)",
            worker_count,
        )

        for index in range(worker_count):
            task = asyncio.create_task(
                self._worker_loop(index + 1),
                name=f"pipeline-worker-{index + 1}",
            )
            self._workers.append(task)

    async def stop(self) -> None:
        """Stop pipeline workers."""
        self._stopping = True

        if not self._workers:
            return

        logger.info("Stopping pipeline workers")

        for task in self._workers:
            task.cancel()

        await asyncio.gather(
            *self._workers,
            return_exceptions=True,
        )

        self._workers.clear()

        logger.info("Pipeline workers stopped")

    async def _worker_loop(self, worker_id: int) -> None:
        """Continuously process queued jobs."""
        logger.info(
            "Pipeline worker %s started",
            worker_id,
        )

        while not self._stopping:
            try:
                job = await self._queue.get()

                try:
                    await self._process(job)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception(
                        "Unhandled error processing job %s",
                        getattr(job, "job_id", "unknown"),
                    )
                finally:
                    self._queue.task_done()

            except asyncio.CancelledError:
                break

            except Exception:
                logger.exception(
                    "Pipeline worker %s loop error",
                    worker_id,
                )

        logger.info(
            "Pipeline worker %s stopped",
            worker_id,
        )

    async def _process(self, job) -> None:
        """Run download -> validate -> extract -> scan."""
        job_id = job.job_id

        try:
            # A queued job can be cancelled before its worker starts.
            if job.cancel_requested:
                logger.info(
                    "Job %s was cancelled before processing started",
                    job_id,
                )
                self.job_manager.cancel(job_id)
                return

            self.job_manager.set_status(
                job_id,
                JobStatus.DOWNLOADING,
            )

            logger.info(
                "Job %s: downloading archive",
                job_id,
            )

            archive_path = await self._download(job)

            if job.cancel_requested:
                self.job_manager.cancel(job_id)
                return

            logger.info(
                "Job %s: download completed: %s",
                job_id,
                archive_path,
            )

            archive_info = await asyncio.to_thread(
                validate_archive,
                archive_path,
                self.settings,
            )

            job.archive_size_bytes = archive_info.archive_size_bytes
            job.write_metadata()

            if job.cancel_requested:
                self.job_manager.cancel(job_id)
                return

            self.job_manager.set_status(
                job_id,
                JobStatus.EXTRACTING,
            )

            logger.info(
                "Job %s: extracting archive",
                job_id,
            )

            extraction_result = await asyncio.to_thread(
                safe_extract,
                archive_path,
                job.extracted_dir,
                self.settings,
                should_cancel=lambda: job.cancel_requested,
            )

            if job.cancel_requested:
                self.job_manager.cancel(job_id)
                return

            logger.info(
                "Job %s: extracted %s files (%s bytes)",
                job_id,
                extraction_result.extracted_files,
                extraction_result.total_bytes,
            )

            self.job_manager.set_status(
                job_id,
                JobStatus.SCANNING,
            )

            logger.info(
                "Job %s: scanning media",
                job_id,
            )

            result = await asyncio.to_thread(
                scan_directory,
                job.extracted_dir,
            )

            if job.cancel_requested:
                self.job_manager.cancel(job_id)
                return

            self.job_manager.update_counts(
                job_id,
                result.counts(),
            )

            self.job_manager.set_status(
                job_id,
                JobStatus.COMPLETED,
            )

            logger.info(
                "Job %s: processing completed "
                "(images=%s videos=%s ignored=%s)",
                job_id,
                result.image_count,
                result.video_count,
                result.ignored_count,
            )

        except asyncio.CancelledError:
            logger.info(
                "Job %s: worker task cancelled",
                job_id,
            )
            raise

        except Exception as exc:
            logger.exception(
                "Job %s: processing failed",
                job_id,
            )

            try:
                current = self.job_manager.get(job_id)

                if current.cancel_requested and not current.is_terminal:
                    self.job_manager.cancel(job_id)

                elif not current.is_terminal:
                    self.job_manager.fail(
                        job_id,
                        f"{type(exc).__name__}: {exc}",
                    )

            except Exception:
                logger.exception(
                    "Failed to update final state for job %s",
                    job_id,
                )

        finally:
            try:
                self.job_manager.cleanup_job_files(job_id)
            except Exception:
                logger.exception(
                    "Job %s: cleanup failed",
                    job_id,
                )

    async def _download(self, job):
        """Download the Telegram document for the job."""
        message = getattr(
            job,
            "_telegram_message",
            None,
        )

        if message is None:
            raise RuntimeError(
                "Telegram source message is unavailable for job"
            )

        archive_dir = job.archive_dir
        archive_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        archive_path = job.archive_path

        downloaded = await message.download_media(
            file=str(archive_path),
        )

        if not downloaded:
            raise RuntimeError(
                "Telegram returned no downloaded file"
            )

        return archive_path


class Application:
    """Coordinate userbot, login bot and processing pipeline."""

    def __init__(
        self,
        settings: Settings,
    ) -> None:
        self.settings = settings

        self.job_manager: Optional[JobManager] = None
        self.pipeline: Optional[PipelineWorker] = None
        self.userbot: Optional[TelegramUserbot] = None
        self.login_bot: Optional[LoginBot] = None

        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        """Start the complete application."""
        logger.info(
            "Starting Telegram Media Processor (PART 1)"
        )

        self.settings.ensure_directories()

        self.job_manager = JobManager(
            self.settings.job_dir,
            keep_files=self.settings.keep_job_files,
        )

        self.pipeline = PipelineWorker(
            self.settings,
            self.job_manager,
        )

        self.userbot = TelegramUserbot(
            self.settings,
            self.job_manager,
            self.pipeline,
        )

        self.login_bot = LoginBot(
            self.settings,
            self.userbot,
        )

        await self.userbot.start()

        if await self.userbot.client.is_user_authorized():
            logger.info(
                "Existing Telegram userbot session is authorized"
            )

        else:
            if not self.settings.bot_token:
                raise ConfigError(
                    "BOT_TOKEN is required for first-time QR login"
                )

            if self.settings.bot_owner_id is None:
                raise ConfigError(
                    "BOT_OWNER_ID is required for first-time QR login"
                )

            logger.info(
                "Telegram userbot is not authorized; "
                "starting QR login bot"
            )

            await self.login_bot.start()

            if not self.login_bot._running:
                raise RuntimeError(
                    "Login bot could not be started"
                )

            await self.login_bot.wait_until_authorized()

            logger.info(
                "QR authentication completed"
            )

        await self.pipeline.start()

        logger.info(
            "Telegram Media Processor is ready"
        )

    async def run(self) -> None:
        """Run until shutdown."""
        await self.start()

        try:
            await self._stop_event.wait()
        finally:
            await self.stop()

    async def stop(self) -> None:
        """Gracefully stop all components."""
        logger.info(
            "Stopping Telegram Media Processor"
        )

        if self.pipeline is not None:
            await self.pipeline.stop()

        if self.login_bot is not None:
            await self.login_bot.stop()

        if self.userbot is not None:
            await self.userbot.stop()

        logger.info(
            "Telegram Media Processor stopped"
        )

    def request_stop(self) -> None:
        """Request application shutdown."""
        self._stop_event.set()


async def async_main() -> None:
    """Async entry point."""
    settings = Settings.from_env()

    setup_logging(settings)

    application = Application(settings)

    loop = asyncio.get_running_loop()

    import signal

    for signal_name in ("SIGINT", "SIGTERM"):
        try:
            signal_value = getattr(
                signal,
                signal_name,
            )

            loop.add_signal_handler(
                signal_value,
                application.request_stop,
            )

        except (
            AttributeError,
            NotImplementedError,
        ):
            pass

    await application.run()


def main() -> None:
    """Synchronous entry point."""
    try:
        asyncio.run(async_main())

    except KeyboardInterrupt:
        logging.getLogger("app.main").info(
            "Application interrupted by user"
        )

    except ConfigError:
        logging.getLogger("app.main").exception(
            "Configuration error"
        )
        raise

    except Exception:
        logging.getLogger("app.main").exception(
            "Application terminated unexpectedly"
        )
        raise


if __name__ == "__main__":
    main()
