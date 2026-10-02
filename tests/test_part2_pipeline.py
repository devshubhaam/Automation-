"""Part 2 pipeline tests: image uploads, size limit, videos, cancellation, queue.

Everything is offline: uploaders are mocked and no Telegram/network is used.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import shutil
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from app.config import Settings
from app.job_manager import JobManager, JobStatus
from app.main import PipelineWorker
from app.media_scanner import MediaFile
from app.telegram_client import TelegramUserbot
from app.uploaders import PART2_IMAGE_MAX_BYTES, UploadError

from .conftest import build_zip

MAX = PART2_IMAGE_MAX_BYTES


class FakeUploader:
    """Records calls; returns a URL or raises UploadError."""

    def __init__(self, provider: str, *, fail: str | None = None, on_call=None) -> None:
        self.provider = provider
        self.fail = fail
        self.on_call = on_call
        self.calls: list[Path] = []

    async def upload(self, path: Path) -> dict[str, Any]:
        self.calls.append(Path(path))
        if self.on_call:
            self.on_call(Path(path))
        if self.fail:
            raise UploadError(self.fail)
        return {"provider": self.provider, "url": f"https://{self.provider}.test/{Path(path).name}"}


class FakeMessage:
    """Stands in for a Telethon Message; 'downloads' a prepared ZIP."""

    def __init__(self, source: Path) -> None:
        self.source = source
        self.downloads = 0

    async def download_media(self, file: str) -> str:
        self.downloads += 1
        shutil.copyfile(self.source, file)
        return file


def make_worker(settings: Settings, tmp_path: Path, imgbb=None, telegraph=None):
    manager = JobManager(settings.job_dir)
    imgbb = imgbb or FakeUploader("imgbb")
    telegraph = telegraph or FakeUploader("telegraph")
    worker = PipelineWorker(settings, manager, imgbb_uploader=imgbb, telegraph_uploader=telegraph)
    return worker, manager, imgbb, telegraph


def make_job(manager: JobManager, zip_path: Path | None = None):
    job = manager.create_job(user_id=1, chat_id=1, archive_name="a.zip", message_id=1)
    if zip_path is not None:
        job._telegram_message = FakeMessage(zip_path)
    manager.mark_queued(job.job_id)
    return job


def media(path: Path, media_type: str = "image") -> MediaFile:
    return MediaFile(path=path, relative_path=path.name, filename=path.name,
                     media_type=media_type, index=1, size_bytes=path.stat().st_size)


def write(path: Path, size: int) -> Path:
    path.write_bytes(b"x" * size)
    return path


def pairs(job) -> set[tuple[str, str]]:
    return {(r.filename, r.provider) for r in job.upload_results}


# --- A-F: provider behaviour -------------------------------------------------

@pytest.mark.asyncio
async def test_small_image_uploads_to_both_providers(settings, tmp_path):
    worker, manager, imgbb, telegraph = make_worker(settings, tmp_path)
    job = make_job(manager)
    img = write(tmp_path / "a.jpg", 100)
    manager.set_status(job.job_id, JobStatus.DOWNLOADING)
    for s in (JobStatus.EXTRACTING, JobStatus.SCANNING):
        manager.set_status(job.job_id, s)

    await worker._upload_images(job, [media(img)])

    assert imgbb.calls == [img] and telegraph.calls == [img]
    assert pairs(job) == {("a.jpg", "imgbb"), ("a.jpg", "telegraph")}
    assert job.status == JobStatus.UPLOADING
    assert job.metadata["part2_skipped_large_images"] == []
    by_provider = {r.provider: r.url for r in job.upload_results}
    assert by_provider == {"imgbb": "https://imgbb.test/a.jpg", "telegraph": "https://telegraph.test/a.jpg"}


@pytest.mark.asyncio
async def test_exactly_2mib_is_eligible_and_over_is_skipped(settings, tmp_path):
    worker, manager, imgbb, telegraph = make_worker(settings, tmp_path)
    job = make_job(manager)
    for s in (JobStatus.DOWNLOADING, JobStatus.EXTRACTING, JobStatus.SCANNING):
        manager.set_status(job.job_id, s)
    ok = write(tmp_path / "ok.jpg", MAX)
    big = write(tmp_path / "big.jpg", MAX + 1)

    await worker._upload_images(job, [media(ok), media(big)])

    assert imgbb.calls == [ok] and telegraph.calls == [ok]
    skipped = job.metadata["part2_skipped_large_images"]
    assert [e["filename"] for e in skipped] == ["big.jpg"]
    assert skipped[0]["size_bytes"] == MAX + 1
    assert job.upload_failures == []  # intentional skip is NOT a failure


@pytest.mark.asyncio
async def test_large_image_calls_no_provider_and_skips_uploading_state(settings, tmp_path):
    worker, manager, imgbb, telegraph = make_worker(settings, tmp_path)
    job = make_job(manager)
    for s in (JobStatus.DOWNLOADING, JobStatus.EXTRACTING, JobStatus.SCANNING):
        manager.set_status(job.job_id, s)
    big = write(tmp_path / "big.png", MAX + 1)

    await worker._upload_images(job, [media(big)])

    assert imgbb.calls == [] and telegraph.calls == []
    assert job.upload_results == [] and job.upload_failures == []
    assert job.status == JobStatus.SCANNING  # no eligible images -> no UPLOADING
    assert len(job.metadata["part2_skipped_large_images"]) == 1


@pytest.mark.asyncio
async def test_imgbb_ok_telegraph_fails(settings, tmp_path):
    worker, manager, imgbb, telegraph = make_worker(
        settings, tmp_path, telegraph=FakeUploader("telegraph", fail="telegraph down"))
    job = make_job(manager)
    for s in (JobStatus.DOWNLOADING, JobStatus.EXTRACTING, JobStatus.SCANNING):
        manager.set_status(job.job_id, s)

    await worker._upload_images(job, [media(write(tmp_path / "a.jpg", 10))])

    assert pairs(job) == {("a.jpg", "imgbb")}
    assert [(f.provider, f.error) for f in job.upload_failures] == [("telegraph", "telegraph down")]


@pytest.mark.asyncio
async def test_imgbb_fails_telegraph_ok(settings, tmp_path):
    worker, manager, imgbb, telegraph = make_worker(
        settings, tmp_path, imgbb=FakeUploader("imgbb", fail="imgbb down"))
    job = make_job(manager)
    for s in (JobStatus.DOWNLOADING, JobStatus.EXTRACTING, JobStatus.SCANNING):
        manager.set_status(job.job_id, s)

    await worker._upload_images(job, [media(write(tmp_path / "a.jpg", 10))])

    assert pairs(job) == {("a.jpg", "telegraph")}
    assert [(f.provider, f.error) for f in job.upload_failures] == [("imgbb", "imgbb down")]
    assert len(telegraph.calls) == 1  # Telegraph still attempted after ImgBB failure


@pytest.mark.asyncio
async def test_both_providers_fail(settings, tmp_path):
    worker, manager, _, _ = make_worker(
        settings, tmp_path,
        imgbb=FakeUploader("imgbb", fail="e1"), telegraph=FakeUploader("telegraph", fail="e2"))
    job = make_job(manager)
    for s in (JobStatus.DOWNLOADING, JobStatus.EXTRACTING, JobStatus.SCANNING):
        manager.set_status(job.job_id, s)

    await worker._upload_images(job, [media(write(tmp_path / "a.jpg", 10))])

    assert job.upload_results == []
    assert {(f.provider, f.error) for f in job.upload_failures} == {("imgbb", "e1"), ("telegraph", "e2")}


# --- C: videos ----------------------------------------------------------------

@pytest.mark.asyncio
async def test_video_calls_neither_provider(settings, tmp_path):
    worker, manager, imgbb, telegraph = make_worker(settings, tmp_path)
    zip_path = build_zip(tmp_path / "v.zip", {"clip.mp4": b"\x00\x00\x00\x18ftypmp42", "b.mkv": b"x"})
    job = make_job(manager, zip_path)

    await worker._process(job)

    assert imgbb.calls == [] and telegraph.calls == []
    assert job.status == JobStatus.COMPLETED
    assert job.metadata["video_processing"] == "pending_part3"
    assert sorted(v["filename"] for v in job.metadata["video_files"]) == ["b.mkv", "clip.mp4"]
    assert job.upload_results == [] and job.upload_failures == []


@pytest.mark.asyncio
async def test_mixed_zip_end_to_end(settings, tmp_path):
    settings = dataclasses.replace(settings, keep_job_files=True)
    worker, manager, imgbb, telegraph = make_worker(settings, tmp_path)
    zip_path = build_zip(tmp_path / "m.zip", {"a.jpg": b"img", "v.mp4": b"vid"})
    job = make_job(manager, zip_path)

    await worker._process(job)

    assert job.status == JobStatus.COMPLETED
    assert [p.name for p in imgbb.calls] == ["a.jpg"] and [p.name for p in telegraph.calls] == ["a.jpg"]
    assert all(p.suffix != ".mp4" for p in imgbb.calls + telegraph.calls)
    data = job.to_dict()
    assert {(r["filename"], r["provider"]) for r in data["upload_results"]} == {("a.jpg", "imgbb"), ("a.jpg", "telegraph")}
    assert data["metadata"]["video_processing"] == "pending_part3"
    json.dumps(data)  # serialisable


@pytest.mark.asyncio
async def test_upload_failures_give_completed_with_errors(settings, tmp_path):
    worker, manager, _, _ = make_worker(settings, tmp_path, imgbb=FakeUploader("imgbb", fail="x"))
    job = make_job(manager, build_zip(tmp_path / "m.zip", {"a.jpg": b"img"}))

    await worker._process(job)

    assert job.status == JobStatus.COMPLETED_WITH_ERRORS


# --- G, H, I: cancellation ----------------------------------------------------

def test_cancel_only_sets_flag_for_active_job(settings, tmp_path):
    manager = JobManager(settings.job_dir)
    job = make_job(manager)
    manager.set_status(job.job_id, JobStatus.DOWNLOADING)

    manager.cancel(job.job_id)

    assert job.cancel_requested is True
    assert job.status == JobStatus.DOWNLOADING  # NOT terminal until the worker stops


@pytest.mark.asyncio
async def test_cancel_before_processing(settings, tmp_path):
    worker, manager, imgbb, telegraph = make_worker(settings, tmp_path)
    msg_zip = build_zip(tmp_path / "a.zip", {"a.jpg": b"img"})
    job = make_job(manager, msg_zip)
    manager.cancel(job.job_id)

    await worker._process(job)

    assert job.status == JobStatus.CANCELLED
    assert job._telegram_message.downloads == 0
    assert imgbb.calls == [] and telegraph.calls == []


@pytest.mark.asyncio
async def test_cancel_between_provider_uploads(settings, tmp_path):
    manager_holder: dict[str, Any] = {}
    imgbb = FakeUploader("imgbb", on_call=lambda p: manager_holder["m"].cancel(manager_holder["id"]))
    worker, manager, imgbb, telegraph = make_worker(settings, tmp_path, imgbb=imgbb)
    zip_path = build_zip(tmp_path / "a.zip", {"a.jpg": b"1", "b.jpg": b"2"})
    job = make_job(manager, zip_path)
    manager_holder.update(m=manager, id=job.job_id)

    await worker._process(job)

    assert job.status == JobStatus.CANCELLED
    assert len(imgbb.calls) == 1
    assert telegraph.calls == []  # no further provider processing
    assert pairs(job) == {("a.jpg", "imgbb")}  # earlier result is preserved


@pytest.mark.asyncio
async def test_cancelled_is_never_overwritten_by_completed(settings, tmp_path):
    worker, manager, _, _ = make_worker(settings, tmp_path)
    job = make_job(manager)
    manager.mark_cancelled(job.job_id)

    manager.complete(job.job_id)
    manager.complete_with_errors(job.job_id)
    await worker._process(job)  # terminal jobs are skipped

    assert job.status == JobStatus.CANCELLED


@pytest.mark.asyncio
async def test_queued_cancelled_job_is_not_processed_and_not_stuck(settings, tmp_path):
    worker, manager, imgbb, telegraph = make_worker(settings, tmp_path)
    zip_path = build_zip(tmp_path / "a.zip", {"a.jpg": b"img"})
    job = make_job(manager, zip_path)
    await worker.submit(job)
    manager.cancel(job.job_id)

    await worker.start()
    try:
        for _ in range(100):
            if job.is_terminal():
                break
            await asyncio.sleep(0.02)
    finally:
        await worker.stop()

    assert job.status == JobStatus.CANCELLED
    assert job._telegram_message.downloads == 0
    assert imgbb.calls == [] and telegraph.calls == []


# --- J, K: queue --------------------------------------------------------------

@pytest.mark.asyncio
async def test_each_submitted_job_is_queued_once(settings, tmp_path):
    worker, manager, _, _ = make_worker(settings, tmp_path)
    j1, j2 = make_job(manager), make_job(manager)
    # JobManager has no processing queue of its own any more.
    for name in ("enqueue", "get_next_job", "queue_size", "remove_from_queue", "_queue"):
        assert not hasattr(manager, name)

    await worker.submit(j1)
    await worker.submit(j2)

    assert worker.queue_size() == 2
    assert j1.status == j2.status == JobStatus.QUEUED


def test_telegram_client_does_not_enqueue_in_job_manager():
    source = (Path(__file__).resolve().parent.parent / "app" / "telegram_client.py").read_text(encoding="utf-8")
    assert "job_manager.enqueue" not in source
    assert "job_manager.queue_size" not in source


@pytest.mark.asyncio
async def test_status_reports_pipeline_queue_size(settings, tmp_path):
    worker, manager, _, _ = make_worker(settings, tmp_path)
    jobs = [make_job(manager) for _ in range(3)]
    for j in jobs:
        await worker.submit(j)

    bot = object.__new__(TelegramUserbot)
    bot.job_manager, bot.pipeline = manager, worker
    replies: list[str] = []

    async def reply(text, **kwargs):
        replies.append(text)

    await bot.handle_status(SimpleNamespace(reply=reply))

    assert "Queued jobs: 3" in replies[0]
