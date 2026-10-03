#!/usr/bin/env python3
"""Startup smoke test (no Telegram network access).

Proves, offline, that:

1. ``Settings.from_env`` loads and validates a ``.env`` file.
2. ``ensure_directories`` creates every runtime directory.
3. ``setup_logging`` writes a rotating log file with the expected format.
4. The API hash never appears in the log file.
5. ``JobManager`` creates the documented job directory layout on disk.

Usage:
    python scripts/smoke_startup.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from app.config import ConfigError, Settings  # noqa: E402
from app.job_manager import JobManager, JobStatus  # noqa: E402
from app.logging_config import get_logger, setup_logging  # noqa: E402

RESULTS: list[tuple[str, bool, str]] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    RESULTS.append((name, bool(condition), detail))
    print(f"[{'PASS' if condition else 'FAIL'}] {name}" + (f" - {detail}" if detail else ""))


DUMMY_HASH = "deadbeefdeadbeefdeadbeefdeadbeef"


def main() -> int:
    work = PROJECT_ROOT / "data" / "_smoke"
    if work.exists():
        import shutil

        shutil.rmtree(work)
    work.mkdir(parents=True, exist_ok=True)

    env_file = work / ".env"
    env_file.write_text(
        "\n".join(
            [
                "API_ID=12345678",
                f"API_HASH={DUMMY_HASH}",
                "SESSION_NAME=smoke_session",
                f"SESSION_DIR={work / 'sessions'}",
                f"DOWNLOAD_DIR={work / 'downloads'}",
                f"JOB_DIR={work / 'jobs'}",
                f"LOG_DIR={work / 'logs'}",
                "MAX_ARCHIVE_SIZE_MB=500",
                "MAX_EXTRACTED_SIZE_MB=2000",
                "MAX_FILES_PER_ARCHIVE=10000",
                "LOG_LEVEL=INFO",
                "LOG_MAX_BYTES=1048576",
                "LOG_BACKUP_COUNT=3",
                "KEEP_JOB_FILES=false",
                "WORKER_COUNT=1",
            ]
        ),
        encoding="utf-8",
    )

    # 1) configuration -------------------------------------------------- #
    settings = Settings.from_env(env_file)
    check("Config loads and converts types", settings.api_id == 12345678)
    check("Numeric limits are ints", isinstance(settings.max_extracted_size_mb, int))
    check("Session path derived from config", settings.session_path.name == "smoke_session")

    # 2) directories ---------------------------------------------------- #
    settings.ensure_directories()
    for directory in (
        settings.session_dir,
        settings.download_dir,
        settings.job_dir,
        settings.log_dir,
    ):
        check(f"Directory exists: {directory.name}", directory.is_dir())

    # 3) logging -------------------------------------------------------- #
    logger = setup_logging(settings)
    logger.info("Smoke test: application starting")
    logger.info("Smoke test: Telegram authentication successful")
    logger.info("Smoke test: ZIP received: media.zip")
    logger.info("Smoke test: Created job JOB-SMOKE-0001")
    logger.info("Smoke test: ZIP downloaded")
    logger.info("Smoke test: ZIP extraction started")
    logger.info("Smoke test: ZIP extraction completed")
    logger.info("Smoke test: Media scan started")
    logger.info("Smoke test: Found 3 images and 2 videos")
    logger.info("Smoke test: Job completed")
    for handler in logging_handlers():
        handler.flush()

    log_text = settings.log_file.read_text(encoding="utf-8")
    check("Log file written", settings.log_file.is_file() and settings.log_file.stat().st_size > 0)
    check(
        "Log lines match the documented format",
        bool(
            re.search(
                r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} INFO app(?:\.\w+)? "
                r"Smoke test: Job completed$",
                log_text,
                re.MULTILINE,
            )
        ),
    )

    # 4) secret hygiene -------------------------------------------------- #
    leaked = [name for name in ("api_hash", "API_HASH", DUMMY_HASH, "password") if name in log_text]
    check("No secrets in the log file", not leaked, f"leaked={leaked}" if leaked else "clean")

    # 5) job layout ------------------------------------------------------ #
    manager = JobManager(settings.job_dir)
    job = manager.create_job(archive_name="media.zip", source_message_id=7, chat_id=1, sender_id=1)
    check("Job directory created", Path(job.job_directory).is_dir())
    check("archive/ subdirectory created", job.archive_dir.is_dir())
    check("extracted/ subdirectory created", job.extracted_dir.is_dir())
    check("metadata.json created", job.metadata_path.is_file())
    check("Initial status RECEIVED", job.status is JobStatus.RECEIVED)

    metadata = job.metadata_path.read_text(encoding="utf-8")
    check(
        "metadata.json holds no credentials",
        not any(token in metadata for token in ("api_hash", DUMMY_HASH, "password", "session")),
    )

    # 6) config failure path -------------------------------------------- #
    # The previous Settings.from_env() call loaded API_ID/API_HASH into the
    # process environment; remove them so the missing-value path is exercised.
    import os

    saved = {key: os.environ.pop(key, None) for key in ("API_ID", "API_HASH")}
    try:
        Settings.from_env(work / "does-not-exist.env")
        check("Missing API_ID raises ConfigError", False, "no exception")
    except ConfigError as exc:
        check("Missing API_ID raises ConfigError", True, str(exc)[:50])
    finally:
        for key, value in saved.items():
            if value is not None:
                os.environ[key] = value

    passed = sum(1 for _, ok, _ in RESULTS if ok)
    failed = len(RESULTS) - passed
    print(f"\n=== STARTUP SMOKE: {passed}/{len(RESULTS)} passed, {failed} failed ===")
    print(f"Log file: {settings.log_file}")
    return 0 if failed == 0 else 1


def logging_handlers():
    import logging

    return [h for h in logging.getLogger().handlers if hasattr(h, "flush")]


if __name__ == "__main__":
    raise SystemExit(main())
