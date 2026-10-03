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

    def __init__(self, provider: str = "imgbb", *, fail: str | None = None, on_call=None) -> None:
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


class FakePublisher:
    """Stands in for TelegraphPublisher. It can only receive image URLs.

    The signature is exactly ``publish(title, image_urls)``: passing video links
    would raise a TypeError.
    """

    def __init__(self, *, fail: str | None = None) -> None:
        self.fail = fail
        self.calls: list[tuple[str, list[str]]] = []

    async def publish(self, title: str, image_urls: list[str]) -> list[dict[str, Any]]:
        self.calls.append((title, list(image_urls)))
        if self.fail:
            raise UploadError(self.fail)
        return [{"provider": "telegraph_article", "title": title,
                 "url": f"https://telegra.ph/{title}", "path": title,
                 "image_count": len(image_urls)}]


class FakeVideoBot:
    """Stands in for VideoBotUploader; records the files it was given."""

    def __init__(self, *, fail: str | None = None, on_call=None) -> None:
        self.fail = fail
        self.on_call = on_call
        self.calls: list[Path] = []

    async def upload(self, path: Path, *, client=None, should_cancel=None) -> dict[str, Any]:
        self.calls.append(Path(path))
        if self.on_call:
            self.on_call(Path(path))
        if self.fail:
            raise UploadError(self.fail)
        return {"provider": "video_bot", "url": f"https://www.domain.com/app/{Path(path).stem}"}


class FakeMessage:
    """Stands in for a Telethon Message; 'downloads' a prepared ZIP."""

    def __init__(self, source: Path) -> None:
        self.source = source
        self.downloads = 0

    async def download_media(self, file: str) -> str:
        self.downloads += 1
        shutil.copyfile(self.source, file)
        return file


def make_worker(settings: Settings, tmp_path: Path, imgbb=None, telegraph=None, video=None):
    manager = JobManager(settings.job_dir)
    imgbb = imgbb or FakeUploader("imgbb")
    telegraph = telegraph or FakePublisher()
    worker = PipelineWorker(settings, manager, imgbb_uploader=imgbb, telegraph_publisher=telegraph,
                            video_uploader=video or FakeVideoBot())
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

def prep(manager, job):
    for s in (JobStatus.DOWNLOADING, JobStatus.EXTRACTING, JobStatus.SCANNING):
        manager.set_status(job.job_id, s)


@pytest.mark.asyncio
async def test_small_image_goes_to_imgbb_then_telegraph_article(settings, tmp_path):
    worker, manager, imgbb, telegraph = make_worker(settings, tmp_path)
    job = make_job(manager)
    img = write(tmp_path / "a.jpg", 100)
    prep(manager, job)

    await worker._upload_media(job, [media(img)], [])

    assert imgbb.calls == [img]
    # Telegraph receives ONLY the ImgBB URL, never a file path.
    assert telegraph.calls == [("a", ["https://imgbb.test/a.jpg"])]
    assert pairs(job) == {("a.jpg", "imgbb"), ("a", "telegraph_article")}
    assert job.status == JobStatus.UPLOADING
    assert job.metadata["part2_skipped_large_images"] == []
    article = next(r for r in job.upload_results if r.provider == "telegraph_article")
    assert article.media_type == "article" and article.url == "https://telegra.ph/a"


@pytest.mark.asyncio
async def test_article_embeds_all_imgbb_links_in_order(settings, tmp_path):
    worker, manager, imgbb, telegraph = make_worker(settings, tmp_path)
    job = make_job(manager)
    prep(manager, job)
    files = [write(tmp_path / n, 10) for n in ("1.jpg", "2.png", "3.jpg")]

    await worker._upload_media(job, [media(f) for f in files], [])

    assert len(telegraph.calls) == 1  # one article per job
    assert telegraph.calls[0][1] == [f"https://imgbb.test/{n}" for n in ("1.jpg", "2.png", "3.jpg")]


@pytest.mark.asyncio
async def test_exactly_2mib_is_eligible_and_over_is_skipped(settings, tmp_path):
    worker, manager, imgbb, telegraph = make_worker(settings, tmp_path)
    job = make_job(manager)
    prep(manager, job)
    ok = write(tmp_path / "ok.jpg", MAX)
    big = write(tmp_path / "big.jpg", MAX + 1)

    await worker._upload_media(job, [media(ok), media(big)], [])

    assert imgbb.calls == [ok]
    assert telegraph.calls == [("a", ["https://imgbb.test/ok.jpg"])]  # big.jpg never reaches the article
    skipped = job.metadata["part2_skipped_large_images"]
    assert [e["filename"] for e in skipped] == ["big.jpg"]
    assert skipped[0]["size_bytes"] == MAX + 1
    assert job.upload_failures == []  # intentional skip is NOT a failure


@pytest.mark.asyncio
async def test_large_image_calls_nothing_and_skips_uploading_state(settings, tmp_path):
    worker, manager, imgbb, telegraph = make_worker(settings, tmp_path)
    job = make_job(manager)
    prep(manager, job)
    big = write(tmp_path / "big.png", MAX + 1)

    await worker._upload_media(job, [media(big)], [])

    assert imgbb.calls == [] and telegraph.calls == []
    assert job.upload_results == [] and job.upload_failures == []
    assert job.status == JobStatus.SCANNING  # no eligible images -> no UPLOADING
    assert len(job.metadata["part2_skipped_large_images"]) == 1


@pytest.mark.asyncio
async def test_one_imgbb_failure_still_publishes_the_rest(settings, tmp_path):
    class Flaky(FakeUploader):
        async def upload(self, path):
            if path.name == "bad.jpg":
                raise UploadError("imgbb rejected")
            return await super().upload(path)

    worker, manager, imgbb, telegraph = make_worker(settings, tmp_path, imgbb=Flaky())
    job = make_job(manager)
    prep(manager, job)

    await worker._upload_media(
        job, [media(write(tmp_path / "good.jpg", 10)), media(write(tmp_path / "bad.jpg", 10))], [])

    assert telegraph.calls == [("a", ["https://imgbb.test/good.jpg"])]
    assert [(f.provider, f.filename, f.error) for f in job.upload_failures] == [
        ("imgbb", "bad.jpg", "imgbb rejected")]


@pytest.mark.asyncio
async def test_all_imgbb_fail_means_no_telegraph_article(settings, tmp_path):
    worker, manager, _, telegraph = make_worker(
        settings, tmp_path, imgbb=FakeUploader("imgbb", fail="imgbb down"))
    job = make_job(manager)
    prep(manager, job)

    await worker._upload_media(job, [media(write(tmp_path / "a.jpg", 10))], [])

    assert telegraph.calls == []
    assert job.upload_results == []
    assert [(f.provider, f.error) for f in job.upload_failures] == [("imgbb", "imgbb down")]


@pytest.mark.asyncio
async def test_telegraph_failure_keeps_imgbb_links(settings, tmp_path):
    worker, manager, imgbb, telegraph = make_worker(
        settings, tmp_path, telegraph=FakePublisher(fail="telegraph down"))
    job = make_job(manager)
    prep(manager, job)

    await worker._upload_media(job, [media(write(tmp_path / "a.jpg", 10))], [])

    assert pairs(job) == {("a.jpg", "imgbb")}
    assert [(f.provider, f.error) for f in job.upload_failures] == [("telegraph_article", "telegraph down")]


@pytest.mark.asyncio
async def test_no_image_file_is_ever_given_to_telegraph(settings, tmp_path):
    worker, manager, imgbb, telegraph = make_worker(settings, tmp_path)
    assert not hasattr(worker, "telegraph_uploader")
    assert not hasattr(telegraph, "upload")


# --- C: videos (Part 3: video bot) --------------------------------------------

@pytest.mark.asyncio
async def test_videos_go_to_video_bot_only_and_never_to_telegraph(settings, tmp_path):
    video = FakeVideoBot()
    worker, manager, imgbb, telegraph = make_worker(settings, tmp_path, video=video)
    zip_path = build_zip(tmp_path / "v.zip", {"clip.mp4": b"\x00\x00\x00\x18ftypmp42", "b.mkv": b"x"})
    job = make_job(manager, zip_path)

    await worker._process(job)

    assert imgbb.calls == []  # a video never reaches ImgBB
    assert sorted(p.name for p in video.calls) == ["b.mkv", "clip.mp4"]
    assert telegraph.calls == []  # videos only -> no Telegraph article
    assert job.status == JobStatus.COMPLETED
    assert job.metadata["video_processing"] == "video_bot"
    assert pairs(job) == {("clip.mp4", "video_bot"), ("b.mkv", "video_bot")}


@pytest.mark.asyncio
async def test_mixed_zip_end_to_end(settings, tmp_path):
    settings = dataclasses.replace(settings, keep_job_files=True)
    video = FakeVideoBot()
    worker, manager, imgbb, telegraph = make_worker(settings, tmp_path, video=video)
    zip_path = build_zip(tmp_path / "m.zip", {"a.jpg": b"img", "v.mp4": b"vid"})
    job = make_job(manager, zip_path)

    await worker._process(job)

    assert job.status == JobStatus.COMPLETED
    assert [p.name for p in imgbb.calls] == ["a.jpg"]          # image -> ImgBB only
    assert [p.name for p in video.calls] == ["v.mp4"]          # video -> video bot only
    assert telegraph.calls == [("a", ["https://imgbb.test/a.jpg"])]
    data = job.to_dict()
    assert {(r["filename"], r["provider"]) for r in data["upload_results"]} == {
        ("a.jpg", "imgbb"), ("v.mp4", "video_bot"), ("a", "telegraph_article")}
    assert job.metadata["video_results"] == [
        {"filename": "v.mp4", "relative_path": "v.mp4", "url": "https://www.domain.com/app/v"}]
    json.dumps(data)  # serialisable


@pytest.mark.asyncio
async def test_video_failure_is_recorded_and_next_video_still_runs(settings, tmp_path):
    class Flaky(FakeVideoBot):
        async def upload(self, path, **kw):
            if path.name == "bad.mp4":
                self.calls.append(path)
                raise UploadError("bot timeout")
            return await super().upload(path, **kw)

    video = Flaky()
    worker, manager, _, telegraph = make_worker(settings, tmp_path, video=video)
    job = make_job(manager, build_zip(tmp_path / "v.zip", {"bad.mp4": b"1", "good.mp4": b"2"}))

    await worker._process(job)

    assert job.status == JobStatus.COMPLETED_WITH_ERRORS
    assert [(f.provider, f.filename, f.error) for f in job.upload_failures] == [("video_bot", "bad.mp4", "bot timeout")]
    assert telegraph.calls == []  # no images -> no article, and videos never go there
    assert [r["filename"] for r in job.metadata["video_results"]] == ["good.mp4"]
    assert [f["filename"] for f in job.metadata["video_failures"]] == ["bad.mp4"]


@pytest.mark.asyncio
async def test_all_videos_fail_means_no_article(settings, tmp_path):
    worker, manager, _, telegraph = make_worker(settings, tmp_path, video=FakeVideoBot(fail="down"))
    job = make_job(manager, build_zip(tmp_path / "v.zip", {"a.mp4": b"1"}))

    await worker._process(job)

    assert telegraph.calls == []
    assert job.status == JobStatus.COMPLETED_WITH_ERRORS


@pytest.mark.asyncio
async def test_unconfigured_video_bot_is_a_clear_failure(settings, tmp_path):
    from app.uploaders import VideoBotUploader
    manager = JobManager(settings.job_dir)
    worker = PipelineWorker(settings, manager, imgbb_uploader=FakeUploader(),
                            telegraph_publisher=FakePublisher(),
                            video_uploader=VideoBotUploader(None))
    job = make_job(manager, build_zip(tmp_path / "v.zip", {"a.mp4": b"1"}))

    await worker._process(job)

    assert [f.error for f in job.upload_failures] == ["VIDEO_BOT_USERNAME is not configured"]


@pytest.mark.asyncio
async def test_cancel_during_video_uploads(settings, tmp_path):
    holder: dict[str, Any] = {}
    video = FakeVideoBot(on_call=lambda p: holder["m"].cancel(holder["id"]))
    worker, manager, _, telegraph = make_worker(settings, tmp_path, video=video)
    job = make_job(manager, build_zip(tmp_path / "v.zip", {"a.mp4": b"1", "b.mp4": b"2"}))
    holder.update(m=manager, id=job.job_id)

    await worker._process(job)

    assert job.status == JobStatus.CANCELLED
    assert len(video.calls) == 1 and telegraph.calls == []


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
async def test_cancel_during_imgbb_uploads(settings, tmp_path):
    manager_holder: dict[str, Any] = {}
    imgbb = FakeUploader("imgbb", on_call=lambda p: manager_holder["m"].cancel(manager_holder["id"]))
    worker, manager, imgbb, telegraph = make_worker(settings, tmp_path, imgbb=imgbb)
    zip_path = build_zip(tmp_path / "a.zip", {"a.jpg": b"1", "b.jpg": b"2"})
    job = make_job(manager, zip_path)
    manager_holder.update(m=manager, id=job.job_id)

    await worker._process(job)

    assert job.status == JobStatus.CANCELLED
    assert len(imgbb.calls) == 1  # second image never uploaded
    assert telegraph.calls == []  # no article after cancellation
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
