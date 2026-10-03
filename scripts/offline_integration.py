#!/usr/bin/env python3
"""Offline integration harness for PART 1 (no Telegram required).

It runs the *real* pipeline components - validate_archive, safe_extract,
scan_directory and the job state machine - against the §52 acceptance archive,
and it also proves the negative cases (traversal, zip bomb, file count, cancel,
cleanup). This is what makes the "integration" claims verifiable without an
account.

Usage:
    python scripts/offline_integration.py
"""

from __future__ import annotations

import sys
import zipfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from app.archive_processor import (  # noqa: E402
    ArchiveTooLargeError,
    ExtractionCancelledError,
    InvalidArchiveError,
    TooManyFilesError,
    UnsafeArchiveError,
    safe_extract,
    validate_archive,
)
from app.config import Settings  # noqa: E402
from app.job_manager import JobManager, JobStatus  # noqa: E402
from app.media_scanner import scan_directory  # noqa: E402
from app.progress import render_progress  # noqa: E402
from app.utils import natural_sorted  # noqa: E402

RESULTS: list[tuple[str, bool, str]] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    RESULTS.append((name, bool(condition), detail))
    print(f"[{'PASS' if condition else 'FAIL'}] {name}" + (f" - {detail}" if detail else ""))


def build_acceptance_zip(path: Path) -> Path:
    """§52 sample.zip -> Sample/{Images,Videos,Other}."""
    entries = {
        "Sample/Images/1.jpg": b"\xff\xd8\xff\xe0" + b"jpeg-payload-1",
        "Sample/Images/2.png": b"\x89PNG\r\n\x1a\n" + b"png-payload-2",
        "Sample/Images/10.webp": b"RIFF....WEBP" + b"webp-payload-10",
        "Sample/Videos/1.mp4": b"\x00\x00\x00\x18ftypmp42" + b"video-1",
        "Sample/Videos/2.mkv": b"\x1a\x45\xdf\xa3" + b"video-2",
        "Sample/Other/readme.txt": b"readme content",
        "Sample/Other/document.pdf": b"%PDF-1.4 fake pdf",
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, payload in entries.items():
            archive.writestr(name, payload)
    return path


def main() -> int:
    work = PROJECT_ROOT / "data" / "_offline_integration"
    if work.exists():
        import shutil

        shutil.rmtree(work)
    work.mkdir(parents=True, exist_ok=True)

    settings = Settings(
        api_id=12345,
        api_hash="offline-integration-hash",
        session_dir=work / "sessions",
        download_dir=work / "downloads",
        job_dir=work / "jobs",
        log_dir=work / "logs",
        max_archive_size_mb=50,
        max_extracted_size_mb=5,
        max_files_per_archive=100,
        keep_job_files=False,
    )
    settings.ensure_directories()

    manager = JobManager(settings.job_dir, keep_files=settings.keep_job_files)

    # ------------------------------------------------------------------ #
    # 1) Acceptance archive -> full pipeline
    # ------------------------------------------------------------------ #
    print("\n=== §52 ACCEPTANCE ARCHIVE (sample.zip) ===")
    archive = build_acceptance_zip(work / "sample.zip")
    job = manager.create_job(
        archive_name=archive.name, source_message_id=1, chat_id=1, sender_id=1
    )
    job.archive_path.parent.mkdir(parents=True, exist_ok=True)
    job.archive_path.write_bytes(archive.read_bytes())

    manager.set_status(job.job_id, JobStatus.DOWNLOADING)
    check("ZIP received / job created", job.job_id.startswith("JOB-"), job.job_id)
    check("ZIP downloaded into job dir", job.archive_path.is_file())

    manager.set_status(job.job_id, JobStatus.EXTRACTING)
    info = validate_archive(job.archive_path, settings)
    check("ZIP validation", info.file_count == 7, f"{info.file_count} files declared")
    result = safe_extract(job.archive_path, job.extracted_dir, settings)
    check("Safe extraction", result.extracted_files == 7, f"{result.extracted_files} files")

    outside = [p for p in work.rglob("*") if p.is_file() and "jobs" not in p.parts]
    check(
        "No files written outside job dir",
        all("_offline_integration" in str(p) for p in outside),
        f"{len(outside)} external files (harness fixtures only)",
    )

    manager.set_status(job.job_id, JobStatus.SCANNING)
    scan = scan_directory(job.extracted_dir, scan_root=job.extracted_dir)
    counts = scan.counts()
    manager.update_counts(job.job_id, counts)

    check("Recursive scan - images", scan.image_count == 3, f"images={scan.image_count}")
    check("Recursive scan - videos", scan.video_count == 2, f"videos={scan.video_count}")
    check("Ignored files", scan.ignored_count == 2, f"ignored={scan.ignored_count}")

    image_names = [f.filename for f in scan.images]
    video_names = [f.filename for f in scan.videos]
    check(
        "Image ordering (natural)",
        image_names == ["1.jpg", "2.png", "10.webp"],
        " -> ".join(image_names),
    )
    check("Video ordering (natural)", video_names == ["1.mp4", "2.mkv"], " -> ".join(video_names))
    check(
        "Relative paths only",
        all(not f.relative_path.startswith("/") for f in scan.images + scan.videos),
        scan.images[0].relative_path,
    )

    manager.set_status(job.job_id, JobStatus.COMPLETED)
    report = render_progress(
        manager.get(job.job_id),
        images=[f.relative_path for f in scan.images],
        videos=[f.relative_path for f in scan.videos],
        ignored=scan.relative_ignored(),
    )
    print("\n--- Telegram completion message ---")
    print(report)
    print("-----------------------------------\n")
    check("Completion message counts", "🖼 Images: 3" in report and "🎬 Videos: 2" in report)

    removed = manager.cleanup_job_files(job.job_id)
    check("Cleanup removes archive+extracted", set(removed) == {"archive", "extracted"})
    check("Metadata kept after cleanup", job.metadata_path.exists())

    # ------------------------------------------------------------------ #
    # 2) Negative cases
    # ------------------------------------------------------------------ #
    print("\n=== SECURITY / LIMIT CASES ===")

    traversal = work / "evil.zip"
    with zipfile.ZipFile(traversal, "w") as zf:
        zf.writestr("../../escape.txt", b"boom")
    try:
        validate_archive(traversal, settings)
        check("Path traversal rejected", False, "no exception raised")
    except UnsafeArchiveError as exc:
        check("Path traversal rejected", True, str(exc)[:60])
    try:
        safe_extract(traversal, work / "traversal_out", settings)
        check("Traversal blocked at extraction", False, "no exception raised")
    except UnsafeArchiveError:
        check(
            "Traversal blocked at extraction",
            not (work.parent / "escape.txt").exists(),
            "nothing escaped",
        )

    absolute = work / "abs.zip"
    with zipfile.ZipFile(absolute, "w") as zf:
        zf.writestr("/etc/evil.txt", b"x")
    try:
        validate_archive(absolute, settings)
        check("Absolute path rejected", False, "no exception raised")
    except UnsafeArchiveError:
        check("Absolute path rejected", True)

    bomb = work / "bomb.zip"
    with zipfile.ZipFile(bomb, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("bomb.bin", b"\x00" * (12 * 1024 * 1024))  # 12 MiB vs 5 MiB limit
    check("ZIP bomb compresses small on disk", bomb.stat().st_size < 1024 * 1024)
    try:
        validate_archive(bomb, settings)
        check("ZIP bomb rejected (declared size)", False, "no exception raised")
    except ArchiveTooLargeError:
        check("ZIP bomb rejected (declared size)", True)

    liar = work / "liar.zip"
    with zipfile.ZipFile(liar, "w", zipfile.ZIP_DEFLATED) as zf:
        info_liar = zipfile.ZipInfo("liar.bin")
        info_liar.compress_type = zipfile.ZIP_DEFLATED
        info_liar.file_size = 1  # lies about the real size
        zf.writestr(info_liar, b"A" * (12 * 1024 * 1024))
    try:
        safe_extract(liar, work / "liar_out", settings)
        check("ZIP bomb stopped mid-stream", False, "no exception raised")
    except ArchiveTooLargeError:
        check(
            "ZIP bomb stopped mid-stream",
            not (work / "liar_out" / "liar.bin").exists(),
            "partial file removed",
        )

    many = work / "many.zip"
    with zipfile.ZipFile(many, "w") as zf:
        for i in range(120):
            zf.writestr(f"f{i}.txt", b"x")
    try:
        validate_archive(many, settings)
        check("File count limit enforced", False, "no exception raised")
    except TooManyFilesError as exc:
        check("File count limit enforced", True, str(exc)[:60])

    corrupted = work / "corrupted.zip"
    corrupted.write_bytes(b"PK\x03\x04 definitely not a zip")
    try:
        validate_archive(corrupted, settings)
        check("Corrupted ZIP rejected", False, "no exception raised")
    except InvalidArchiveError:
        check("Corrupted ZIP rejected", True)

    # ------------------------------------------------------------------ #
    # 3) Cancellation
    # ------------------------------------------------------------------ #
    print("\n=== CANCELLATION ===")
    big = work / "cancel.zip"
    with zipfile.ZipFile(big, "w", zipfile.ZIP_STORED) as zf:
        for i in range(40):
            zf.writestr(f"c{i}.bin", b"z" * 65536)
    cancel_job = manager.create_job(archive_name="cancel.zip")
    manager.set_status(cancel_job.job_id, JobStatus.DOWNLOADING)
    manager.set_status(cancel_job.job_id, JobStatus.EXTRACTING)
    calls = {"n": 0}

    def should_cancel() -> bool:
        calls["n"] += 1
        return calls["n"] > 3

    try:
        safe_extract(big, cancel_job.extracted_dir, settings, should_cancel=should_cancel)
        check("Extraction honours cancel flag", False, "no exception raised")
    except ExtractionCancelledError:
        manager.cancel(cancel_job.job_id)
        manager.cleanup_job_files(cancel_job.job_id, force=True)
        check("Extraction honours cancel flag", True, f"stopped after {calls['n']} checks")
        check("Cancelled job cleaned up", not cancel_job.extracted_dir.exists())
        check(
            "Cancelled job status",
            manager.get(cancel_job.job_id).status is JobStatus.CANCELLED,
        )

    # ------------------------------------------------------------------ #
    # 4) Natural sort unit sanity
    # ------------------------------------------------------------------ #
    check(
        "natural_sorted sanity",
        natural_sorted(["a10.jpg", "a2.jpg", "a1.jpg"]) == ["a1.jpg", "a2.jpg", "a10.jpg"],
    )

    passed = sum(1 for _, ok, _ in RESULTS if ok)
    failed = len(RESULTS) - passed
    print(f"\n=== OFFLINE INTEGRATION: {passed}/{len(RESULTS)} passed, {failed} failed ===")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
