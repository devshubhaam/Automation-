"""Application entry point for the Telegram media processor."""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from .config import ConfigError, Settings
from .job_manager import JobManager
from .login_bot import LoginBot
from .telegram_client import TelegramUserbot

logger = logging.getLogger("app.main")


class PipelineWorker:
    """Background worker that processes submitted media jobs."""

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
        """Start configured worker tasks."""

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
        """Stop all worker tasks."""

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

    async def _worker_loop(
        self,
        worker_id: int,
    ) -> None:
        """Process jobs continuously."""

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
        """Run the Part 1 processing stages for one job."""

        job_id = job.job_id

        try:
            await self.job_manager.mark_running(job_id)

            logger.info(
                "Job %s: processing started",
                job_id,
            )

            # ---------------------------------------------------------- #
            # DOWNLOADING
            # ---------------------------------------------------------- #

            await self.job_manager.update_stage(
                job_id,
                "DOWNLOADING",
            )

            archive_path = await self._download(job)

            logger.info(
                "Job %s: download completed: %s",
                job_id,
                archive_path,
            )

            # ---------------------------------------------------------- #
            # EXTRACTING
            # ---------------------------------------------------------- #

            await self.job_manager.update_stage(
                job_id,
                "EXTRACTING",
            )

            extracted_dir = await self._extract(
                job,
                archive_path,
            )

            logger.info(
                "Job %s: extraction completed",
                job_id,
            )

            # ---------------------------------------------------------- #
            # SCANNING
            # ---------------------------------------------------------- #

            await self.job_manager.update_stage(
                job_id,
                "SCANNING",
            )

            result = await self._scan(
                job,
                extracted_dir,
            )

            logger.info(
                "Job %s: media scan completed",
                job_id,
            )

            # ---------------------------------------------------------- #
            # COMPLETED
            # ---------------------------------------------------------- #

            await self.job_manager.mark_completed(
                job_id,
                result,
            )

            logger.info(
                "Job %s: processing completed",
                job_id,
            )

        except asyncio.CancelledError:
            logger.info(
                "Job %s: processing cancelled",
                job_id,
            )

            try:
                await self.job_manager.mark_cancelled(
                    job_id,
                )
            except Exception:
                logger.exception(
                    "Failed to mark job %s as cancelled",
                    job_id,
                )

            raise

        except Exception as exc:
            logger.exception(
                "Job %s: processing failed",
                job_id,
            )

            try:
                await self.job_manager.mark_failed(
                    job_id,
                    type(exc).__name__,
                )
            except Exception:
                logger.exception(
                    "Failed to mark job %s as failed",
                    job_id,
                )

        finally:
            try:
                await self._cleanup(job)
            except Exception:
                logger.exception(
                    "Job %s: cleanup failed",
                    job_id,
                )

    # ------------------------------------------------------------------ #
    # Pipeline stages
    # ------------------------------------------------------------------ #

    async def _download(self, job):
        """Download the Telegram ZIP archive."""

        message = await job.get_message()

        archive_dir = job.archive_dir
        archive_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        archive_path = archive_dir / "archive.zip"

        downloaded = await message.download_media(
            file=str(archive_path),
        )

        if not downloaded:
            raise RuntimeError(
                "Telegram returned no downloaded file"
            )

        return archive_path

    async def _extract(
        self,
        job,
        archive_path,
    ):
        """Safely extract the ZIP archive."""

        from .archive_processor import ArchiveProcessor

        processor = ArchiveProcessor(
            settings=self.settings,
        )

        return await asyncio.to_thread(
            processor.extract,
            archive_path,
            job.extracted_dir,
        )

    async def _scan(
        self,
        job,
        extracted_dir,
    ):
        """Recursively scan extracted files for media."""

        from .media_scanner import MediaScanner

        scanner = MediaScanner()

        result = await asyncio.to_thread(
            scanner.scan,
            extracted_dir,
        )

        return result

    async def _cleanup(self, job) -> None:
        """Cleanup job files according to KEEP_JOB_FILES."""

        if self.settings.keep_job_files:
            logger.info(
                "Job %s: keeping job files",
                job.job_id,
            )
            return

        # Keep metadata, but remove archive/extracted data.
        for directory in (
            job.archive_dir,
            job.extracted_dir,
        ):
            if directory.exists():
                await asyncio.to_thread(
                    self._remove_directory,
                    directory,
                )

    @staticmethod
    def _remove_directory(directory) -> None:
        import shutil

        shutil.rmtree(
            directory,
            ignore_errors=True,
        )


class Application:
    """Coordinates the Telegram client, login bot and pipeline."""

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
            self.settings,
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

        # -------------------------------------------------------------- #
        # Connect userbot session
        # -------------------------------------------------------------- #

        await self.userbot.start()

        # -------------------------------------------------------------- #
        # Existing authorized session
        # -------------------------------------------------------------- #

        if await self.userbot.client.is_user_authorized():
            logger.info(
                "Existing Telegram userbot session is authorized"
            )

        # -------------------------------------------------------------- #
        # First-time QR login
        # -------------------------------------------------------------- #

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
                "Telegram userbot is not authorized"
            )

            logger.info(
                "Starting login bot; send /login to authorize "
                "the userbot via QR"
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

        # -------------------------------------------------------------- #
        # Start processing pipeline
        # -------------------------------------------------------------- #

        await self.pipeline.start()

        logger.info(
            "Telegram Media Processor is ready"
        )

    async def run(self) -> None:
        """Run application until shutdown."""

        await self.start()

        try:
            await self._stop_event.wait()

        finally:
            await self.stop()

    async def stop(self) -> None:
        """Gracefully stop all application components."""

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
    """Async application entry point."""

    settings = Settings.from_env()

    application = Application(settings)

    loop = asyncio.get_running_loop()

    for signal_name in ("SIGINT", "SIGTERM"):
        try:
            import signal

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
            # Signal handlers are not available on every platform.
            pass

    await application.run()


def main() -> None:
    """Synchronous entry point."""

    try:
        asyncio.run(async_main())

    except KeyboardInterrupt:
        logger.info(
            "Application interrupted by user"
        )

    except ConfigError:
        logger.exception(
            "Configuration error"
        )
        raise

    except Exception:
        logger.exception(
            "Application terminated unexpectedly"
        )
        raise


if __name__ == "__main__":
    main()
