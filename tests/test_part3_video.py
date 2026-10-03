"""Final media flow tests: images -> ImgBB -> Telegraph, videos -> video bot only.

Everything is offline: ImgBB, Telegraph and the Telegram video bot are fakes.
"""

from __future__ import annotations

import asyncio
import builtins
import dataclasses
import json
import os
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from app.config import ConfigError, Settings
from app.job_manager import JobManager, JobStatus
from app.main import JobCancelled, PipelineWorker
from app.media_scanner import MediaFile
from app.progress import ProgressRenderer
from app.uploaders import (
    PART2_IMAGE_MAX_BYTES,
    ImgBBUploader,
    UploadCancelled,
    UploadError,
    VideoBotUploader,
    VideoTooLargeError,
)
from app.uploaders import video_bot as video_bot_module

from .conftest import build_zip

LIMIT = 1536 * 1024 * 1024  # 1.5 GB


# --------------------------------------------------------------------------- #
# Fakes
# --------------------------------------------------------------------------- #


class FakeImgBB:
    def __init__(self) -> None:
        self.calls: list[Path] = []

    async def upload(self, path: Path) -> dict[str, Any]:
        self.calls.append(Path(path))
        return {"provider": "imgbb", "url": f"https://imgbb.test/{Path(path).name}"}


class FakePublisher:
    """``publish(title, image_urls)`` only: a third argument is a TypeError."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, list[str]]] = []

    async def publish(self, title: str, image_urls: list[str]) -> list[dict[str, Any]]:
        self.calls.append((title, list(image_urls)))
        return [{"provider": "telegraph_article", "title": title,
                 "url": f"https://telegra.ph/{title}", "path": title,
                 "image_count": len(image_urls)}]


class FakeVideoBot:
    """Maps filename -> URL; behaviour per file can be customised."""

    def __init__(self, *, fail: dict[str, str] | None = None, on_call=None) -> None:
        self.fail = fail or {}
        self.on_call = on_call
        self.calls: list[Path] = []

    async def upload(self, path: Path, *, client=None, should_cancel=None) -> dict[str, Any]:
        self.calls.append(Path(path))
        if self.on_call:
            self.on_call(Path(path))
        if Path(path).name in self.fail:
            raise UploadError(self.fail[Path(path).name])
        return {"provider": "video_bot", "url": f"https://www.domain.com/app/{Path(path).stem.upper()}"}


class FakeMessage:
    def __init__(self, source: Path | None = None, client: Any = None) -> None:
        self.source, self.client, self.downloads = source, client, 0

    async def download_media(self, file: str) -> str:
        import shutil
        self.downloads += 1
        shutil.copyfile(self.source, file)
        return file


def assert_subset(actual: list[dict[str, Any]], expected: list[dict[str, Any]]) -> None:
    """Every expected key/value must be present (entries may carry extra fields,
    e.g. ``file``/``provider``/``bot``/``status``/``reason`` added in Part 3)."""
    assert len(actual) == len(expected)
    for got, want in zip(actual, expected):
        assert {k: got.get(k) for k in want} == want


def make_worker(settings: Settings, imgbb=None, telegraph=None, video=None):
    manager = JobManager(settings.job_dir)
    imgbb = imgbb or FakeImgBB()
    telegraph = telegraph or FakePublisher()
    video = video or FakeVideoBot()
    worker = PipelineWorker(settings, manager, imgbb_uploader=imgbb,
                            telegraph_publisher=telegraph, video_uploader=video)
    return worker, manager, imgbb, telegraph, video


def make_job(manager: JobManager, zip_path: Path | None = None, client: Any = None):
    job = manager.create_job(user_id=1, chat_id=1, archive_name="album.zip", message_id=1)
    if zip_path is not None or client is not None:
        job._telegram_message = FakeMessage(zip_path, client)
    manager.mark_queued(job.job_id)
    return job


def prep(manager: JobManager, job) -> None:
    for status in (JobStatus.DOWNLOADING, JobStatus.EXTRACTING, JobStatus.SCANNING):
        manager.set_status(job.job_id, status)


def media(path: Path, media_type: str) -> MediaFile:
    return MediaFile(path=path, relative_path=path.name, filename=path.name,
                     media_type=media_type, index=1, size_bytes=path.stat().st_size)


def write(path: Path, size: int) -> Path:
    path.write_bytes(b"x" * size)
    return path


def sparse(path: Path, size: int) -> Path:
    """A file of ``size`` bytes without using disk space."""
    with open(path, "wb") as handle:
        handle.truncate(size)
    return path


# --------------------------------------------------------------------------- #
# 1-3  Image / video provider routing
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_image_up_to_2mib_calls_imgbb(settings, tmp_path):
    worker, manager, imgbb, telegraph, _ = make_worker(settings)
    job = make_job(manager)
    prep(manager, job)
    img = write(tmp_path / "ok.jpg", PART2_IMAGE_MAX_BYTES)  # exactly 2 MiB

    await worker._upload_media(job, [media(img, "image")], [])

    assert imgbb.calls == [img]
    assert telegraph.calls == [("album", ["https://imgbb.test/ok.jpg"])]


@pytest.mark.asyncio
async def test_image_over_2mib_does_not_call_imgbb_and_is_not_a_failure(settings, tmp_path):
    worker, manager, imgbb, telegraph, _ = make_worker(settings)
    job = make_job(manager)
    prep(manager, job)
    img = write(tmp_path / "big.jpg", PART2_IMAGE_MAX_BYTES + 1)

    await worker._upload_media(job, [media(img, "image")], [])

    assert imgbb.calls == [] and telegraph.calls == []
    assert [e["filename"] for e in job.metadata["part2_skipped_large_images"]] == ["big.jpg"]
    assert job.upload_failures == []


@pytest.mark.asyncio
async def test_video_never_reaches_imgbb(settings, tmp_path):
    worker, manager, imgbb, _, video = make_worker(settings)
    job = make_job(manager)
    prep(manager, job)
    vid = write(tmp_path / "clip.mp4", 10)

    await worker._upload_media(job, [], [media(vid, "video")])

    assert imgbb.calls == [] and video.calls == [vid]
    # ... and the real ImgBB uploader refuses a video before any network access.
    with pytest.raises(UploadError, match="images only"):
        await ImgBBUploader(api_key="k").upload(vid)


# --------------------------------------------------------------------------- #
# 4-7  Telegraph receives image URLs only
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_video_never_reaches_telegraph(settings, tmp_path):
    worker, manager, _, telegraph, video = make_worker(settings)
    job = make_job(manager)
    prep(manager, job)
    vid = write(tmp_path / "clip.mp4", 10)

    await worker._upload_media(job, [], [media(vid, "video")])

    assert telegraph.calls == []
    assert [r.provider for r in job.upload_results] == ["video_bot"]


@pytest.mark.asyncio
async def test_images_give_telegraph_only_image_urls(settings, tmp_path):
    worker, manager, imgbb, telegraph, _ = make_worker(settings)
    job = make_job(manager)
    prep(manager, job)
    files = [write(tmp_path / n, 10) for n in ("1.jpg", "2.png")]

    await worker._upload_media(job, [media(f, "image") for f in files], [])

    assert telegraph.calls == [("album", ["https://imgbb.test/1.jpg", "https://imgbb.test/2.png"])]


@pytest.mark.asyncio
async def test_videos_only_create_no_telegraph_article(settings, tmp_path):
    worker, manager, _, telegraph, _ = make_worker(settings)
    job = make_job(manager)
    prep(manager, job)
    vids = [write(tmp_path / n, 10) for n in ("a.mp4", "b.mkv")]

    await worker._upload_media(job, [], [media(v, "video") for v in vids])

    assert telegraph.calls == []
    assert not any(r.provider == "telegraph_article" for r in job.upload_results)
    assert "telegraph_articles" not in job.metadata


@pytest.mark.asyncio
async def test_images_plus_videos_telegraph_gets_only_image_urls(settings, tmp_path):
    worker, manager, imgbb, telegraph, video = make_worker(settings)
    job = make_job(manager)
    prep(manager, job)
    img = write(tmp_path / "a.jpg", 10)
    vid = write(tmp_path / "v.mp4", 10)

    await worker._upload_media(job, [media(img, "image")], [media(vid, "video")])

    assert telegraph.calls == [("album", ["https://imgbb.test/a.jpg"])]
    sent = json.dumps(telegraph.calls)
    assert "domain.com" not in sent and "v.mp4" not in sent
    assert job.metadata["telegraph_articles"] == ["https://telegra.ph/album"]
    assert_subset(job.metadata["video_results"], [
        {"filename": "v.mp4", "relative_path": "v.mp4", "url": "https://www.domain.com/app/V"}])


def test_no_video_to_telegraph_code_is_left():
    root = Path(__file__).resolve().parent.parent / "app"
    for name in ("main.py", "uploaders/telegraph.py"):
        source = (root / name).read_text(encoding="utf-8")
        assert "video_links" not in source and "VideoLink" not in source
    assert not (root.parent / "apply_part2_patch.py").exists()


# --------------------------------------------------------------------------- #
# 8-9  Per-video mapping and independence
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_multiple_videos_keep_filename_to_url_mapping(settings, tmp_path):
    worker, manager, _, _, video = make_worker(settings)
    job = make_job(manager)
    prep(manager, job)
    names = ["video1.mp4", "video2.mp4", "video3.mp4"]
    vids = [write(tmp_path / n, 10) for n in names]

    await worker._upload_media(job, [], [media(v, "video") for v in vids])

    assert {r["filename"]: r["url"] for r in job.metadata["video_results"]} == {
        "video1.mp4": "https://www.domain.com/app/VIDEO1",
        "video2.mp4": "https://www.domain.com/app/VIDEO2",
        "video3.mp4": "https://www.domain.com/app/VIDEO3",
    }
    assert {(r.filename, r.url) for r in job.upload_results if r.media_type == "video"} == {
        ("video1.mp4", "https://www.domain.com/app/VIDEO1"),
        ("video2.mp4", "https://www.domain.com/app/VIDEO2"),
        ("video3.mp4", "https://www.domain.com/app/VIDEO3"),
    }
    assert [s["status"] for s in job.metadata["video_status"]] == ["done"] * 3
    result = next(r for r in job.upload_results if r.filename == "video1.mp4")
    assert result.media_type == "video" and result.provider == "video_bot"


@pytest.mark.asyncio
async def test_same_filename_in_two_folders_is_not_mixed_up(settings, tmp_path):
    class ByPath(FakeVideoBot):
        async def upload(self, path, **kw):
            self.calls.append(path)
            return {"provider": "video_bot", "url": f"https://www.domain.com/app/{path.parent.name}"}

    worker, manager, *_ = make_worker(settings, video=ByPath())
    job = make_job(manager)
    prep(manager, job)
    files = []
    for folder in ("a", "b"):
        (tmp_path / folder).mkdir()
        files.append(MediaFile(path=write(tmp_path / folder / "v.mp4", 5), relative_path=f"{folder}/v.mp4",
                               filename="v.mp4", media_type="video", index=1, size_bytes=5))

    await worker._upload_media(job, [], files)

    assert {(r["relative_path"], r["url"]) for r in job.metadata["video_results"]} == {
        ("a/v.mp4", "https://www.domain.com/app/a"), ("b/v.mp4", "https://www.domain.com/app/b")}


@pytest.mark.asyncio
async def test_one_failing_video_does_not_affect_the_others(settings, tmp_path):
    video = FakeVideoBot(fail={"v2.mp4": "bot exploded"})
    worker, manager, _, _, _ = make_worker(settings, video=video)
    job = make_job(manager)
    prep(manager, job)
    vids = [write(tmp_path / n, 10) for n in ("v1.mp4", "v2.mp4", "v3.mp4")]

    await worker._upload_media(job, [], [media(v, "video") for v in vids])

    assert [p.name for p in video.calls] == ["v1.mp4", "v2.mp4", "v3.mp4"]
    assert [r["filename"] for r in job.metadata["video_results"]] == ["v1.mp4", "v3.mp4"]
    assert_subset(job.metadata["video_failures"], [
        {"filename": "v2.mp4", "relative_path": "v2.mp4", "error": "bot exploded"}])
    assert [(s["filename"], s["status"]) for s in job.metadata["video_status"]] == [
        ("v1.mp4", "done"), ("v2.mp4", "failed"), ("v3.mp4", "done")]


# --------------------------------------------------------------------------- #
# 10  1.5 GB per-video limit
# --------------------------------------------------------------------------- #


def test_video_limit_default_is_1_5_gb(settings, monkeypatch):
    assert settings.video_max_size_gb == 1.5
    assert settings.video_max_size_bytes == LIMIT == 1536 * 1024 * 1024
    monkeypatch.setenv("VIDEO_MAX_SIZE_GB", "0.5")
    monkeypatch.setenv("API_ID", "1")
    monkeypatch.setenv("API_HASH", "h" * 32)
    assert Settings.from_env().video_max_size_bytes == 512 * 1024 * 1024
    monkeypatch.setenv("VIDEO_MAX_SIZE_GB", "abc")
    with pytest.raises(ConfigError):
        Settings.from_env()


@pytest.mark.asyncio
async def test_video_over_limit_is_rejected_without_calling_the_bot(settings, tmp_path):
    worker, manager, _, telegraph, video = make_worker(settings)
    job = make_job(manager)
    prep(manager, job)
    huge = sparse(tmp_path / "huge.mp4", LIMIT + 1)
    ok = write(tmp_path / "ok.mp4", 10)

    await worker._upload_media(job, [], [media(huge, "video"), media(ok, "video")])

    assert [p.name for p in video.calls] == ["ok.mp4"]  # the bot never saw huge.mp4
    failure = job.metadata["video_failures"][0]
    assert failure["filename"] == "huge.mp4" and "size limit" in failure["error"]
    assert [r["filename"] for r in job.metadata["video_results"]] == ["ok.mp4"]


@pytest.mark.asyncio
async def test_video_exactly_at_limit_is_allowed(settings, tmp_path):
    worker, manager, _, _, video = make_worker(settings)
    job = make_job(manager)
    prep(manager, job)
    edge = sparse(tmp_path / "edge.mp4", LIMIT)

    await worker._upload_media(job, [], [media(edge, "video")])

    assert [p.name for p in video.calls] == ["edge.mp4"]


@pytest.mark.asyncio
async def test_uploader_enforces_the_limit_itself(tmp_path):
    client = FakeBotClient(lambda *a: asyncio.sleep(0))
    huge = sparse(tmp_path / "huge.mp4", 101)
    with pytest.raises(VideoTooLargeError, match="size limit"):
        await VideoBotUploader("Bot", max_size_bytes=100).upload(huge, client=client)
    assert client.send_calls == 0


@pytest.mark.asyncio
async def test_over_limit_video_gives_completed_with_errors_and_zip_size_is_not_capped(settings, tmp_path):
    """The 1.5 GB limit is per video: it is not applied to the archive size."""
    settings = dataclasses.replace(settings, video_max_size_gb=0.00001)  # ~10 KB
    worker, manager, _, _, video = make_worker(settings)
    zip_path = build_zip(tmp_path / "a.zip", {"big.mp4": b"v" * 20000, "ok.mp4": b"v"})
    job = make_job(manager, zip_path)

    await worker._process(job)

    assert job.status == JobStatus.COMPLETED_WITH_ERRORS
    assert [p.name for p in video.calls] == ["ok.mp4"]


# --------------------------------------------------------------------------- #
# Telegram-level fake used by the uploader tests
# --------------------------------------------------------------------------- #


class FakeBotClient:
    """Minimal Telethon client. ``behaviour(name, sent_id, handlers)`` plays the bot."""

    def __init__(self, behaviour, *, fail_first_sends: int = 0, send_delay: float = 0.0) -> None:
        self.behaviour = behaviour
        self.handlers: list[Any] = []
        self.sent: list[tuple[str, int, Any, dict]] = []
        self.send_calls = 0
        self.fail_first_sends = fail_first_sends
        self.send_delay = send_delay
        self.send_cancelled = False
        self.removed = 0
        self._next_id = 100

    def add_event_handler(self, cb, builder): self.handlers.append(cb)
    def remove_event_handler(self, cb, builder):
        self.removed += 1
        if cb in self.handlers:
            self.handlers.remove(cb)

    async def send_file(self, entity, path, **kw):
        self.send_calls += 1
        try:
            if self.send_delay:
                await asyncio.sleep(self.send_delay)
        except asyncio.CancelledError:
            self.send_cancelled = True
            raise
        if self.send_calls <= self.fail_first_sends:
            raise ConnectionError("network down")
        self._next_id += 10
        sid = self._next_id
        self.sent.append((Path(path).name, sid, path, kw))
        asyncio.get_running_loop().create_task(self.behaviour(Path(path).name, sid, self.handlers))
        return SimpleNamespace(id=sid)


def event(text: str, msg_id: int, reply_to: int | None = None):
    return SimpleNamespace(message=SimpleNamespace(
        id=msg_id, raw_text=text, entities=None, buttons=None, reply_to_msg_id=reply_to))


@pytest.fixture()
def no_telethon(monkeypatch):
    monkeypatch.setattr(VideoBotUploader, "_event_builders", lambda self: [object()])


def real_uploader(settings: Settings | None = None, **kw) -> VideoBotUploader:
    kw.setdefault("poll_seconds", 0.01)
    kw.setdefault("retry_delay_seconds", 0)
    return VideoBotUploader("@FileUploaderBot", **kw)


# --------------------------------------------------------------------------- #
# 8 (again, at Telegram level)  replies are matched to the right video
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_late_reply_to_a_timed_out_video_is_not_given_to_the_next_video(settings, tmp_path, no_telethon):
    state: dict[str, int] = {}

    async def bot(name, sid, handlers):
        if name == "v1.mp4":
            state["v1"] = sid  # v1 is slow: no reply before the timeout
            return
        # v2 is being processed; the bot's late answer for v1 arrives first.
        await handlers[0](event("https://www.domain.com/app/LATE-V1", 9000, reply_to=state["v1"]))
        await handlers[0](event("https://www.domain.com/app/V2", sid + 1, reply_to=sid))

    client = FakeBotClient(bot)
    worker, manager, *_ = make_worker(settings, video=real_uploader(timeout_seconds=0.15))
    job = make_job(manager, client=client)
    prep(manager, job)
    vids = [write(tmp_path / n, 10) for n in ("v1.mp4", "v2.mp4")]

    await worker._upload_media(job, [], [media(v, "video") for v in vids])

    assert [r["filename"] for r in job.metadata["video_results"]] == ["v2.mp4"]
    assert job.metadata["video_results"][0]["url"] == "https://www.domain.com/app/V2"
    assert "did not reply with a link" in job.metadata["video_failures"][0]["error"]
    assert job.metadata["video_failures"][0]["filename"] == "v1.mp4"


@pytest.mark.asyncio
async def test_replies_that_arrive_out_of_order_still_map_correctly(tmp_path, no_telethon):
    async def bot(name, sid, handlers):
        await handlers[0](event(f"https://www.domain.com/app/{Path(name).stem.upper()}", sid + 1, reply_to=sid))

    uploader, client = real_uploader(), FakeBotClient(bot)
    urls = {}
    for name in ("c.mp4", "a.mp4", "b.mp4"):
        urls[name] = (await uploader.upload(write(tmp_path / name, 5), client=client))["url"]

    assert urls == {n: f"https://www.domain.com/app/{n[0].upper()}" for n in ("c.mp4", "a.mp4", "b.mp4")}


# --------------------------------------------------------------------------- #
# 11-12  Cancellation
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_cancel_before_video_processing(settings, tmp_path):
    worker, manager, _, telegraph, video = make_worker(settings)
    job = make_job(manager)
    prep(manager, job)
    manager.cancel(job.job_id)
    vid = write(tmp_path / "v.mp4", 10)

    with pytest.raises(JobCancelled):
        await worker._upload_videos(job, [media(vid, "video")])

    assert video.calls == [] and telegraph.calls == []
    assert job.status == JobStatus.SCANNING  # request-first: the worker, not /cancel, ends the job


@pytest.mark.asyncio
async def test_cancel_between_videos_keeps_earlier_results(settings, tmp_path):
    holder: dict[str, Any] = {}
    video = FakeVideoBot(on_call=lambda p: holder["m"].cancel(holder["id"]) if p.name == "v1.mp4" else None)
    worker, manager, _, telegraph, _ = make_worker(settings, video=video)
    zip_path = build_zip(tmp_path / "v.zip", {"v1.mp4": b"1", "v2.mp4": b"2", "v3.mp4": b"3"})
    job = make_job(manager, zip_path)
    holder.update(m=manager, id=job.job_id)

    await worker._process(job)

    assert job.status == JobStatus.CANCELLED
    assert [p.name for p in video.calls] == ["v1.mp4"]  # v2 / v3 never sent
    assert [(r.filename, r.provider) for r in job.upload_results] == [("v1.mp4", "video_bot")]
    assert [s["status"] for s in job.metadata["video_status"]] == ["done", "pending", "pending"]
    assert telegraph.calls == []


@pytest.mark.asyncio
async def test_cancel_during_slow_send_stops_the_upload(tmp_path, no_telethon):
    client = FakeBotClient(lambda *a: asyncio.sleep(0), send_delay=30)  # a huge video still uploading
    flag = {"cancel": False}

    async def flip():
        await asyncio.sleep(0.05)
        flag["cancel"] = True

    asyncio.get_running_loop().create_task(flip())
    started = asyncio.get_running_loop().time()
    with pytest.raises(UploadCancelled):
        await real_uploader().upload(write(tmp_path / "v.mp4", 5), client=client,
                                     should_cancel=lambda: flag["cancel"])

    assert asyncio.get_running_loop().time() - started < 2
    assert client.send_cancelled is True and client.removed == 1


@pytest.mark.asyncio
async def test_cancel_while_waiting_for_the_bot(tmp_path, no_telethon):
    client = FakeBotClient(lambda *a: asyncio.sleep(0))  # bot never answers
    flag = {"cancel": False}

    async def flip():
        await asyncio.sleep(0.05)
        flag["cancel"] = True

    asyncio.get_running_loop().create_task(flip())
    with pytest.raises(UploadCancelled):
        await real_uploader(timeout_seconds=30).upload(write(tmp_path / "v.mp4", 5), client=client,
                                                       should_cancel=lambda: flag["cancel"])


def test_job_manager_cancel_is_request_first(settings):
    manager = JobManager(settings.job_dir)
    job = make_job(manager)
    manager.set_status(job.job_id, JobStatus.DOWNLOADING)
    manager.set_status(job.job_id, JobStatus.EXTRACTING)
    manager.set_status(job.job_id, JobStatus.SCANNING)
    manager.set_status(job.job_id, JobStatus.UPLOADING)

    manager.request_cancel(job.job_id)

    assert job.cancel_requested is True
    assert job.status == JobStatus.UPLOADING and not job.is_terminal()
    manager.mark_cancelled(job.job_id)
    assert job.status == JobStatus.CANCELLED


# --------------------------------------------------------------------------- #
# 13-14  Timeout and retry (per video)
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_timeout_is_per_video_and_other_videos_continue(settings, tmp_path, no_telethon):
    async def bot(name, sid, handlers):
        if name == "slow.mp4":
            await handlers[0](event("Processing...", sid + 1, reply_to=sid))  # no link, ever
            return
        await handlers[0](event("https://www.domain.com/app/FAST", sid + 1, reply_to=sid))

    client = FakeBotClient(bot)
    worker, manager, *_ = make_worker(settings, video=real_uploader(timeout_seconds=0.1))
    job = make_job(manager, client=client)
    prep(manager, job)
    vids = [write(tmp_path / n, 10) for n in ("slow.mp4", "fast.mp4")]

    await worker._upload_media(job, [], [media(v, "video") for v in vids])

    assert [r["filename"] for r in job.metadata["video_results"]] == ["fast.mp4"]
    assert job.metadata["video_failures"][0]["filename"] == "slow.mp4"
    assert "within" in job.metadata["video_failures"][0]["error"]
    assert client.send_calls == 2  # the timed-out video was NOT re-sent


@pytest.mark.asyncio
async def test_failed_delivery_is_retried_for_that_video_only(tmp_path, no_telethon):
    async def bot(name, sid, handlers):
        await handlers[0](event("https://www.domain.com/app/OK", sid + 1, reply_to=sid))

    client = FakeBotClient(bot, fail_first_sends=1)
    result = await real_uploader(send_attempts=2).upload(write(tmp_path / "v.mp4", 5), client=client)

    assert result["url"] == "https://www.domain.com/app/OK"
    assert client.send_calls == 2 and len(client.sent) == 1  # one failed delivery, one real send


@pytest.mark.asyncio
async def test_delivery_retries_are_bounded(tmp_path, no_telethon):
    client = FakeBotClient(lambda *a: asyncio.sleep(0), fail_first_sends=99)
    with pytest.raises(UploadError, match="after 3 attempt"):
        await real_uploader(send_attempts=3).upload(write(tmp_path / "v.mp4", 5), client=client)
    assert client.send_calls == 3 and client.removed == 1


@pytest.mark.asyncio
async def test_timeout_never_resends_a_video_that_may_still_be_processing(tmp_path, no_telethon):
    client = FakeBotClient(lambda *a: asyncio.sleep(0))
    with pytest.raises(UploadError, match="did not reply"):
        await real_uploader(timeout_seconds=0.05, send_attempts=5).upload(
            write(tmp_path / "v.mp4", 5), client=client)
    assert client.send_calls == 1


# --------------------------------------------------------------------------- #
# 15  The video is never loaded into RAM
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_video_is_passed_as_a_path_and_never_read_into_memory(tmp_path, no_telethon, monkeypatch):
    async def bot(name, sid, handlers):
        await handlers[0](event("https://www.domain.com/app/OK", sid + 1, reply_to=sid))

    client = FakeBotClient(bot)
    video = write(tmp_path / "big.mp4", 1024)

    real_open, real_path_open, real_read_bytes = builtins.open, Path.open, Path.read_bytes

    def refuse(what: str):
        raise AssertionError(f"{what} on the video: it must be streamed by Telethon, not read here")

    def guarded_open(target, *a, **k):
        if str(target) == str(video):
            refuse("open()")
        return real_open(target, *a, **k)

    def guarded_path_open(self, *a, **k):
        if self == video:
            refuse("Path.open()")
        return real_path_open(self, *a, **k)

    def guarded_read_bytes(self):
        if self == video:
            refuse("Path.read_bytes()")
        return real_read_bytes(self)

    monkeypatch.setattr(builtins, "open", guarded_open)
    monkeypatch.setattr(Path, "open", guarded_path_open)
    monkeypatch.setattr(Path, "read_bytes", guarded_read_bytes)

    await real_uploader().upload(video, client=client)

    _, _, sent_arg, kwargs = client.sent[0]
    assert isinstance(sent_arg, str) and sent_arg == str(video)  # a path, not bytes / a file object
    assert not isinstance(sent_arg, (bytes, bytearray))


def test_video_code_has_no_full_file_reads():
    root = Path(__file__).resolve().parent.parent / "app"
    for name in ("uploaders/video_bot.py", "main.py"):
        source = (root / name).read_text(encoding="utf-8")
        for needle in ("read_bytes", ".read()", "readall"):
            assert needle not in source, f"{needle} in {name}"


# --------------------------------------------------------------------------- #
# 16-18  Queue ownership and final status
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_only_pipeline_worker_owns_the_queue(settings):
    worker, manager, *_ = make_worker(settings)
    jobs = [make_job(manager) for _ in range(2)]
    for job in jobs:
        await worker.submit(job)

    assert isinstance(worker._queue, asyncio.Queue) and worker.queue_size() == 2
    for name in ("enqueue", "get_next_job", "queue_size", "_queue"):
        assert not hasattr(manager, name)


@pytest.mark.asyncio
async def test_status_completed_vs_completed_with_errors(settings, tmp_path):
    ok_worker, ok_manager, *_ = make_worker(settings)
    ok_job = make_job(ok_manager, build_zip(tmp_path / "ok.zip", {"a.jpg": b"i", "v.mp4": b"v"}))
    await ok_worker._process(ok_job)
    assert ok_job.status == JobStatus.COMPLETED

    bad_worker, bad_manager, *_ = make_worker(settings, video=FakeVideoBot(fail={"v.mp4": "timeout"}))
    bad_job = make_job(bad_manager, build_zip(tmp_path / "bad.zip", {"a.jpg": b"i", "v.mp4": b"v"}))
    await bad_worker._process(bad_job)
    assert bad_job.status == JobStatus.COMPLETED_WITH_ERRORS
    assert bad_job.metadata["video_failures"][0]["error"] == "timeout"
    assert bad_job.metadata["telegraph_articles"] == ["https://telegra.ph/album"]  # image side still done


@pytest.mark.asyncio
async def test_uploading_status_covers_video_stage(settings, tmp_path):
    seen: list[str] = []
    holder: dict[str, Any] = {}
    video = FakeVideoBot(on_call=lambda p: seen.append(holder["job"].status))
    worker, manager, *_ = make_worker(settings, video=video)
    job = make_job(manager, build_zip(tmp_path / "v.zip", {"v.mp4": b"v"}))
    holder["job"] = job

    await worker._process(job)

    assert seen == [JobStatus.UPLOADING]
    assert job.status == JobStatus.COMPLETED


# --------------------------------------------------------------------------- #
# Metadata, progress, secrets
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_metadata_exposes_everything_and_is_json_serialisable(settings, tmp_path):
    video = FakeVideoBot(fail={"bad.mp4": "timeout"})
    worker, manager, *_ = make_worker(settings, video=video)
    entries = {"s.jpg": b"i", "big.jpg": b"i" * (PART2_IMAGE_MAX_BYTES + 1), "ok.mp4": b"v", "bad.mp4": b"v"}
    zip_path = build_zip(tmp_path / "m.zip", entries)
    settings = dataclasses.replace(settings, max_archive_size_mb=10, max_extracted_size_mb=10)
    worker.settings = settings
    job = make_job(manager, zip_path)

    await worker._process(job)

    md = job.metadata
    assert {m["filename"] for m in md["detected_images"]} == {"s.jpg", "big.jpg"}
    assert {m["filename"] for m in md["detected_videos"]} == {"ok.mp4", "bad.mp4"}
    assert [e["filename"] for e in md["part2_skipped_large_images"]] == ["big.jpg"]
    assert md["imgbb_results"][0]["filename"] == "s.jpg"
    assert md["telegraph_articles"] == ["https://telegra.ph/album"]
    assert md["video_processing"] == "video_bot"
    assert md["video_results"][0]["filename"] == "ok.mp4"
    assert_subset(md["video_failures"][:1], [{"filename": "bad.mp4", "relative_path": "bad.mp4", "error": "timeout"}])
    assert {s["filename"]: s["status"] for s in md["video_status"]} == {"ok.mp4": "done", "bad.mp4": "failed"}
    json.dumps(job.to_dict())


def test_progress_shows_per_video_status_without_secrets(settings):
    manager = JobManager(settings.job_dir)
    job = make_job(manager)
    manager.set_media_counts(job.job_id, image_count=0, video_count=2)
    manager.set_status(job.job_id, JobStatus.DOWNLOADING)
    manager.set_status(job.job_id, JobStatus.EXTRACTING)
    manager.set_status(job.job_id, JobStatus.SCANNING)
    manager.set_status(job.job_id, JobStatus.UPLOADING)
    manager.set_metadata(job.job_id, "video_status", [
        {"filename": "a<b>.mp4", "status": "done", "url": "https://x/app/1", "error": None},
        {"filename": "b.mp4", "status": "failed", "url": None, "error": "timeout"},
        {"filename": "c.mp4", "status": "processing", "url": None, "error": None},
    ])

    text = ProgressRenderer().render(job)

    assert "a&lt;b&gt;.mp4 → ✅ URL received" in text
    assert "b.mp4 → ❌ timeout" in text
    assert "c.mp4 → ⏳ Processing (video bot)" in text
    assert "Telegraph" not in text


def test_settings_repr_hides_secrets(settings):
    secret = dataclasses.replace(settings, imgbb_api_key="IMGBBSECRET", telegraph_access_token="TGSECRET",
                                 bot_token="BOTSECRET", mongodb_uri="mongodb://u:PASS@h")
    text = repr(secret)
    for value in ("IMGBBSECRET", "TGSECRET", "BOTSECRET", "PASS@", "0123456789abcdef0123456789abcdef"):
        assert value not in text
    assert "video_max_size_gb=1.5" in text


def test_default_url_pattern_still_accepts_both_link_types():
    import re
    pattern = re.compile(video_bot_module.DEFAULT_URL_PATTERN)
    for url in ("https://www.domain.com/app/6abfae122a52418b24707585",
                "https://domain.com/s/dauv7n9bjlnn77sqlrogow6-ryxopea"):
        assert pattern.search(f"link: {url}").group(0) == url
