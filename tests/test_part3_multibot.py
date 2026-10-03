"""Part 3 multi-bot tests: VIDEO_BOTS, provider URLs, reply matching, fallback.

Everything is offline: ImgBB, Telegraph and Telegram are fakes, so no real
Telegram credentials or network are needed.
"""

from __future__ import annotations

import asyncio
import dataclasses
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from app.config import Settings, parse_video_bots
from app.job_manager import JobManager, JobStatus
from app.progress import ProgressRenderer
from app.uploaders import (
    MultiVideoBotUploader,
    UploadCancelled,
    UploadError,
    VideoBotUploader,
    VideoTooLargeError,
    VideoUploadFailed,
    provider_for_url,
)
from app.uploaders import video_bot as vb

from .test_part3_video import (  # shared helpers (fakes, builders)
    FakeImgBB,
    FakePublisher,
    event,
    make_job,
    make_worker,
    media,
    prep,
    sparse,
    write,
)

DISK = "DiskWalaFileUploaderBot"
FLEZEN = "FlezenUploadBot"
DISK_URL = "https://www.diskwala.com/app/6abfae122a52418b24707585"
FLEZEN_URL = "https://flezen.com/s/dauv7n9bjlnn77sqlrogow6-ryxopea"
LIMIT = 1536 * 1024 * 1024  # 1.5 GB
VALID_HASH = "0123456789abcdef0123456789abcdef"


# --------------------------------------------------------------------------- #
# Telegram-level fake that knows WHICH bot a file was sent to
# --------------------------------------------------------------------------- #


class MultiClient:
    """Minimal Telethon client. ``behaviour(bot, name, sid, handlers)`` plays a bot.

    ``handlers`` only contains the handlers registered for that bot, like the
    ``from_users`` filter of the real event builders.
    """

    def __init__(self, behaviour, *, fail_send_for: tuple[str, ...] = (), send_delay: float = 0.0) -> None:
        self.behaviour = behaviour
        self.fail_send_for = tuple(b.lower() for b in fail_send_for)
        self.send_delay = send_delay
        self.handlers: dict[str, list[Any]] = {}
        self.sent: list[tuple[str, str, int]] = []  # (bot, filename, sent id)
        self.send_calls: dict[str, int] = {}
        self.in_flight = 0
        self.max_in_flight = 0
        self.removed = 0
        self._next_id = 1000

    # --- Telethon API used by the uploader ---------------------------------
    def add_event_handler(self, cb, builder):
        self.handlers.setdefault(builder.bot, []).append(cb)

    def remove_event_handler(self, cb, builder):
        self.removed += 1
        if cb in self.handlers.get(builder.bot, []):
            self.handlers[builder.bot].remove(cb)

    async def send_file(self, entity, path, **kw):
        bot = str(entity)
        self.send_calls[bot] = self.send_calls.get(bot, 0) + 1
        if self.send_delay:
            await asyncio.sleep(self.send_delay)
        if bot.lower() in self.fail_send_for:
            raise ConnectionError("network down")
        self._next_id += 10
        sid = self._next_id
        name = Path(path).name
        self.sent.append((bot, name, sid))
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)

        async def play() -> None:
            try:
                await self.behaviour(bot, name, sid, list(self.handlers.get(bot, [])))
            finally:
                self.in_flight -= 1

        asyncio.get_running_loop().create_task(play())
        return SimpleNamespace(id=sid)

    # --- helpers -----------------------------------------------------------
    def active_handlers(self) -> int:
        return sum(len(v) for v in self.handlers.values())

    def bots_sent_to(self, name: str) -> list[str]:
        return [bot for bot, filename, _ in self.sent if filename == name]


async def emit(handlers, *events_) -> None:
    for handler in handlers:
        for ev in events_:
            await handler(ev)


@pytest.fixture()
def no_events(monkeypatch):
    """Replace the Telethon event builders by a tag carrying the bot name."""
    monkeypatch.setattr(
        VideoBotUploader, "_event_builders", lambda self: [SimpleNamespace(bot=self.bot_username)]
    )


def multi(bots=(DISK, FLEZEN), **kw) -> MultiVideoBotUploader:
    kw.setdefault("poll_seconds", 0.01)
    kw.setdefault("retry_delay_seconds", 0)
    kw.setdefault("send_attempts", 1)
    kw.setdefault("timeout_seconds", 0.2)
    kw.setdefault("mode", "fallback")  # Part 3 tests cover the fallback policy
    return MultiVideoBotUploader(list(bots), **kw)


def good_bot(urls: dict[str, str]):
    """A bot that replies (to the video) with ``urls[bot]`` + the file stem."""

    async def behaviour(bot, name, sid, handlers):
        url = f"{urls[bot]}-{Path(name).stem}"
        await emit(handlers, event(url, sid + 1, reply_to=sid))

    return behaviour


URLS = {DISK: "https://www.diskwala.com/app/DW", FLEZEN: "https://flezen.com/s/FZ"}


# --------------------------------------------------------------------------- #
# 1. VIDEO_BOTS parsing / configuration
# --------------------------------------------------------------------------- #


def test_video_bots_parsing_example():
    assert parse_video_bots("@DiskWalaFileUploaderBot,@FlezenUploadBot") == (
        "DiskWalaFileUploaderBot",
        "FlezenUploadBot",
    )
    assert list(parse_video_bots("@DiskWalaFileUploaderBot, @FlezenUploadBot")) == [
        "DiskWalaFileUploaderBot",
        "FlezenUploadBot",
    ]


def test_video_bots_parsing_edge_cases():
    assert parse_video_bots(None) == ()
    assert parse_video_bots("") == ()
    assert parse_video_bots(" , ,, ") == ()
    assert parse_video_bots("BotA,@BotB") == ("BotA", "BotB")  # with and without @
    assert parse_video_bots("  @BotA  ,\t@BotB ,") == ("BotA", "BotB")
    assert parse_video_bots("https://t.me/BotA,t.me/BotB") == ("BotA", "BotB")
    assert parse_video_bots("@BotA,@bota,@BotB") == ("BotA", "BotB")  # no duplicates, order kept


def _env_file(tmp_path: Path, *lines: str) -> Path:
    env = tmp_path / ".env"
    env.write_text("\n".join(["API_ID=1", f"API_HASH={VALID_HASH}", *lines]), encoding="utf-8")
    return env


def _clear_video_env(monkeypatch) -> None:
    for key in ("VIDEO_BOTS", "VIDEO_BOT_USERNAME", "VIDEO_BOT_FALLBACK_ON_TIMEOUT", "VIDEO_BOT_REQUIRE_REPLY"):
        monkeypatch.delenv(key, raising=False)


def test_settings_reads_video_bots_from_env(tmp_path, monkeypatch):
    _clear_video_env(monkeypatch)
    env = _env_file(tmp_path, "VIDEO_BOTS=@DiskWalaFileUploaderBot, @FlezenUploadBot")
    settings = Settings.from_env(env)
    assert settings.video_bots == (DISK, FLEZEN)
    assert settings.video_bot_fallback_on_timeout is False  # safe default
    assert settings.video_bot_require_reply is True


def test_video_bots_wins_over_legacy_username(tmp_path, monkeypatch):
    _clear_video_env(monkeypatch)
    env = _env_file(tmp_path, "VIDEO_BOTS=@NewBot", "VIDEO_BOT_USERNAME=@OldBot")
    assert Settings.from_env(env).video_bots == ("NewBot",)


def test_legacy_video_bot_username_still_works(tmp_path, monkeypatch):
    _clear_video_env(monkeypatch)
    settings = Settings.from_env(_env_file(tmp_path, "VIDEO_BOT_USERNAME=@FileUploaderBot"))
    assert settings.video_bots == ("FileUploaderBot",)
    assert settings.video_bot_username == "@FileUploaderBot"


def test_settings_normalises_video_bots_given_directly(settings):
    custom = dataclasses.replace(settings, video_bots=("@A", "B", " @C "))
    assert custom.video_bots == ("A", "B", "C")


# --------------------------------------------------------------------------- #
# 2-3. Provider URL extraction and validation
# --------------------------------------------------------------------------- #


def _pattern():
    import re

    return re.compile(vb.build_provider_pattern())


def msg(text: str = "", entities=None, buttons=None, **kw):
    return SimpleNamespace(raw_text=text, entities=entities, buttons=buttons, **kw)


def test_extracts_diskwala_and_flezen_urls():
    pattern = _pattern()
    assert vb.extract_url(msg("Your link: https://www.diskwala.com/app/abc123"), pattern) == (
        "https://www.diskwala.com/app/abc123"
    )
    assert vb.extract_url(msg("https://flezen.com/s/abc123"), pattern) == "https://flezen.com/s/abc123"
    assert vb.extract_url(msg("Done! https://flezen.com/s/dauv7n9bjlnn77sqlrogow6-ryxopea."), pattern) == (
        "https://flezen.com/s/dauv7n9bjlnn77sqlrogow6-ryxopea"
    )


def test_extracts_url_from_entities_and_buttons():
    pattern = _pattern()
    hidden = msg("Click here", entities=[SimpleNamespace(url="https://www.diskwala.com/app/ent1")])
    assert vb.extract_url(hidden, pattern) == "https://www.diskwala.com/app/ent1"

    button = msg("Open", buttons=[[SimpleNamespace(url="https://flezen.com/s/btn1")]])
    assert vb.extract_url(button, pattern) == "https://flezen.com/s/btn1"


def test_urls_are_normalised():
    assert vb.normalise_url("HTTPS://WWW.DiskWala.com/app/AbC123/") == "https://www.diskwala.com/app/AbC123"
    assert vb.normalise_url("https://flezen.com/s/abc#frag") == "https://flezen.com/s/abc"
    assert vb.normalise_url("https://flezen.com/s/abc),") == "https://flezen.com/s/abc"


def test_unrelated_urls_are_rejected():
    pattern = _pattern()
    for text in (
        "https://google.com",
        "https://telegram.org",
        "https://t.me/SomeChannel",
        "https://www.diskwala.com/",  # right domain, not a file link
        "https://evil-diskwala.com/app/abc",  # look-alike domain
        "https://diskwala.com.evil.com/app/abc",
        "https://flezen.com/other/abc",  # wrong path for this provider
    ):
        assert vb.extract_url(msg(text), pattern) is None, text
    assert vb.extract_url(msg("see https://google.com", entities=[SimpleNamespace(url="https://telegram.org")]), pattern) is None


def test_provider_detection_from_url():
    assert provider_for_url(DISK_URL) == "diskwala"
    assert provider_for_url(FLEZEN_URL) == "flezen"
    assert provider_for_url("https://google.com") is None
    assert provider_for_url("https://x.test/app/1", r"https://x\.test/app/\w+") == "custom"


@pytest.mark.asyncio
async def test_unrelated_url_in_reply_is_ignored_then_provider_url_is_used(no_events, tmp_path):
    async def bot(b, name, sid, handlers):
        await emit(handlers, event("Join https://t.me/channel or https://google.com", sid + 1, reply_to=sid))
        await emit(handlers, event(f"link {DISK_URL}", sid + 2, reply_to=sid))

    client = MultiClient(bot)
    result = await multi([DISK]).upload(write(tmp_path / "a.mp4", 8), client=client)

    assert result["url"] == DISK_URL
    assert result["provider_name"] == "diskwala"


# --------------------------------------------------------------------------- #
# 4. Correct video -> URL mapping (never cross-mapped)
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_each_video_gets_its_own_url_even_with_out_of_order_replies(no_events, tmp_path):
    async def bot(b, name, sid, handlers):
        # Unrelated chatter + a late reply to a DIFFERENT message arrive first.
        await emit(handlers, event("https://www.diskwala.com/app/UNRELATED", sid + 1, reply_to=sid - 5))
        await emit(handlers, event("https://www.diskwala.com/app/NOREPLY", sid + 2, reply_to=None))
        await emit(handlers, event(f"https://www.diskwala.com/app/{Path(name).stem.upper()}", sid + 3, reply_to=sid))

    client = MultiClient(bot)
    uploader = multi([DISK])
    urls = {}
    for name in ("video1.mp4", "video2.mp4", "video3.mp4"):
        urls[name] = (await uploader.upload(write(tmp_path / name, 5), client=client))["url"]

    assert urls == {n: f"https://www.diskwala.com/app/{n[:-4].upper()}" for n in urls}
    assert client.active_handlers() == 0


@pytest.mark.asyncio
async def test_videos_are_sent_one_at_a_time_even_when_requested_together(no_events, tmp_path):
    async def bot(b, name, sid, handlers):
        await asyncio.sleep(0.05)
        await emit(handlers, event(f"https://www.diskwala.com/app/{Path(name).stem.upper()}", sid + 1, reply_to=sid))

    client = MultiClient(bot)
    uploader = multi([DISK], timeout_seconds=5)
    paths = [write(tmp_path / f"v{i}.mp4", 5) for i in range(3)]

    results = await asyncio.gather(*(uploader.upload(p, client=client) for p in paths))

    assert client.max_in_flight == 1  # never two videos in flight
    assert [r["url"].rsplit("/", 1)[1] for r in results] == ["V0", "V1", "V2"]


@pytest.mark.asyncio
async def test_multi_video_job_maps_every_video_to_its_provider_and_bot(settings, tmp_path, no_events):
    async def bot(b, name, sid, handlers):
        await good_bot(URLS)(b, name, sid, handlers)

    # video1 -> first bot fails to deliver; video2 -> first bot works.
    client = MultiClient(bot, fail_send_for=())
    worker, manager, *_ = make_worker(settings, video=multi())
    job = make_job(manager, client=client)
    prep(manager, job)
    vids = [write(tmp_path / n, 10) for n in ("video1.mp4", "video2.mp4", "video3.mp4")]

    await worker._upload_media(job, [], [media(v, "video") for v in vids])

    results = job.metadata["video_results"]
    assert [r["file"] for r in results] == ["video1.mp4", "video2.mp4", "video3.mp4"]
    for r in results:
        assert r["url"] == f"https://www.diskwala.com/app/DW-{r['file'][:-4]}"
        assert (r["provider"], r["bot"], r["status"]) == ("diskwala", DISK, "completed")
    assert job.metadata["video_failures"] == []


# --------------------------------------------------------------------------- #
# 5-6. Edited responses and reply-to matching
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_url_that_appears_in_an_edited_message(no_events, tmp_path):
    async def bot(b, name, sid, handlers):
        await emit(handlers, event("⏳ Processing...", sid + 1, reply_to=sid))
        await asyncio.sleep(0.03)
        # The bot EDITS its message; the edit event does not carry reply_to.
        await emit(handlers, event(f"✅ {FLEZEN_URL}", sid + 1, reply_to=None))

    client = MultiClient(bot)
    result = await multi([FLEZEN], timeout_seconds=2).upload(write(tmp_path / "e.mp4", 5), client=client)

    assert result["url"] == FLEZEN_URL
    assert result["provider_name"] == "flezen"
    assert client.active_handlers() == 0


@pytest.mark.asyncio
async def test_reply_chain_to_the_bots_own_status_message_is_accepted(no_events, tmp_path):
    async def bot(b, name, sid, handlers):
        await emit(handlers, event("Uploading...", sid + 1, reply_to=sid))
        await emit(handlers, event(DISK_URL, sid + 2, reply_to=sid + 1))  # replies to the status message

    client = MultiClient(bot)
    assert (await multi([DISK]).upload(write(tmp_path / "c.mp4", 5), client=client))["url"] == DISK_URL


@pytest.mark.asyncio
async def test_non_reply_messages_are_ignored_in_strict_mode(no_events, tmp_path):
    async def bot(b, name, sid, handlers):
        await emit(handlers, event(DISK_URL, sid + 1, reply_to=None))  # newest message, but not a reply

    client = MultiClient(bot)
    with pytest.raises(UploadError, match="did not reply"):
        await multi([DISK], timeout_seconds=0.1).upload(write(tmp_path / "s.mp4", 5), client=client)

    # ... and accepted when VIDEO_BOT_REQUIRE_REPLY is switched off.
    client = MultiClient(bot)
    lenient = multi([DISK], require_reply=False)
    assert (await lenient.upload(write(tmp_path / "s2.mp4", 5), client=client))["url"] == DISK_URL


@pytest.mark.asyncio
async def test_reply_to_another_video_is_never_used(no_events, tmp_path):
    state: dict[str, int] = {}

    async def bot(b, name, sid, handlers):
        if name == "v1.mp4":
            state["v1"] = sid  # slow: answers only after the timeout
            return
        # v2 is being processed; the bot's LATE answer for v1 arrives first.
        await emit(handlers, event("https://www.diskwala.com/app/LATE-V1", 9999, reply_to=state["v1"]))
        await emit(handlers, event("https://www.diskwala.com/app/V2", sid + 1, reply_to=sid))

    client = MultiClient(bot)
    uploader = multi([DISK], timeout_seconds=0.15)
    with pytest.raises(UploadError):
        await uploader.upload(write(tmp_path / "v1.mp4", 5), client=client)
    result = await uploader.upload(write(tmp_path / "v2.mp4", 5), client=client)

    assert result["url"] == "https://www.diskwala.com/app/V2"


# --------------------------------------------------------------------------- #
# Multi-bot selection and fallback
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_first_bot_is_used_when_it_works(no_events, tmp_path):
    client = MultiClient(good_bot(URLS))
    result = await multi().upload(write(tmp_path / "a.mp4", 5), client=client)

    assert result["bot"] == DISK and result["provider_name"] == "diskwala"
    assert client.bots_sent_to("a.mp4") == [DISK]  # the second bot was never contacted


@pytest.mark.asyncio
async def test_delivery_failure_falls_back_to_next_bot(no_events, tmp_path):
    client = MultiClient(good_bot(URLS), fail_send_for=(DISK,))
    result = await multi().upload(write(tmp_path / "a.mp4", 5), client=client)

    assert result["bot"] == FLEZEN and result["provider_name"] == "flezen"
    assert client.bots_sent_to("a.mp4") == [FLEZEN]  # the first bot never received it
    assert [a["kind"] for a in result["attempts"]] == ["delivery", "ok"]


@pytest.mark.asyncio
async def test_bot_that_rejects_the_video_falls_back_to_next_bot(no_events, tmp_path):
    async def bot(b, name, sid, handlers):
        if b == DISK:
            await emit(handlers, event("❌ Error: unsupported file", sid + 1, reply_to=sid))
        else:
            await good_bot(URLS)(b, name, sid, handlers)

    client = MultiClient(bot)
    result = await multi().upload(write(tmp_path / "a.mp4", 5), client=client)

    assert result["bot"] == FLEZEN
    assert client.bots_sent_to("a.mp4") == [DISK, FLEZEN]
    assert client.active_handlers() == 0


@pytest.mark.asyncio
async def test_processing_message_is_not_a_rejection(no_events, tmp_path):
    async def bot(b, name, sid, handlers):
        await emit(handlers, event("⏳ Processing your video, please wait", sid + 1, reply_to=sid))
        await asyncio.sleep(0.03)
        await emit(handlers, event(DISK_URL, sid + 1, reply_to=None))

    client = MultiClient(bot)
    result = await multi(timeout_seconds=2).upload(write(tmp_path / "a.mp4", 5), client=client)
    assert result["bot"] == DISK and client.bots_sent_to("a.mp4") == [DISK]


@pytest.mark.asyncio
async def test_slow_bot_is_not_followed_by_a_duplicate_upload(no_events, tmp_path):
    async def bot(b, name, sid, handlers):
        await emit(handlers, event("Processing...", sid + 1, reply_to=sid))  # never gives a link

    client = MultiClient(bot)
    with pytest.raises(UploadError, match="did not reply"):
        await multi(timeout_seconds=0.1).upload(write(tmp_path / "slow.mp4", 5), client=client)

    assert client.bots_sent_to("slow.mp4") == [DISK]  # NOT re-sent to the second bot
    assert client.active_handlers() == 0


@pytest.mark.asyncio
async def test_timeout_fallback_is_opt_in(no_events, tmp_path):
    async def bot(b, name, sid, handlers):
        if b == DISK:
            return  # silent: timeout
        await good_bot(URLS)(b, name, sid, handlers)

    client = MultiClient(bot)
    result = await multi(timeout_seconds=0.1, fallback_on_timeout=True).upload(
        write(tmp_path / "a.mp4", 5), client=client
    )
    assert result["bot"] == FLEZEN
    assert client.bots_sent_to("a.mp4") == [DISK, FLEZEN]


@pytest.mark.asyncio
async def test_all_bots_failing_reports_every_attempt(no_events, tmp_path):
    client = MultiClient(good_bot(URLS), fail_send_for=(DISK, FLEZEN))
    with pytest.raises(VideoUploadFailed) as info:
        await multi().upload(write(tmp_path / "a.mp4", 5), client=client)

    assert [a["bot"] for a in info.value.attempts] == [DISK, FLEZEN]
    assert info.value.bot == FLEZEN
    assert DISK in str(info.value) and FLEZEN in str(info.value)
    assert client.sent == []


@pytest.mark.asyncio
async def test_no_bot_configured_gives_a_clear_error(tmp_path):
    with pytest.raises(UploadError, match="VIDEO_BOTS is not configured"):
        await multi([]).upload(write(tmp_path / "a.mp4", 5), client=object())


@pytest.mark.asyncio
async def test_bot_names_are_normalised_and_deduplicated():
    assert multi(["@A", "B", "a", " @C "]).bots == ["A", "B", "C"]


# --------------------------------------------------------------------------- #
# 7. Timeout handling / 13. temporary handlers are cleaned up
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_handlers_are_removed_after_success_timeout_failure_and_retry(no_events, tmp_path):
    # success
    client = MultiClient(good_bot(URLS))
    await multi([DISK]).upload(write(tmp_path / "ok.mp4", 5), client=client)
    assert client.active_handlers() == 0 and client.removed == 1

    # timeout
    client = MultiClient(lambda *a: asyncio.sleep(0))
    with pytest.raises(UploadError):
        await multi([DISK], timeout_seconds=0.05).upload(write(tmp_path / "t.mp4", 5), client=client)
    assert client.active_handlers() == 0

    # delivery failure with retries: handlers must not pile up per retry
    client = MultiClient(lambda *a: asyncio.sleep(0), fail_send_for=(DISK,))
    with pytest.raises(UploadError):
        await multi([DISK], send_attempts=3).upload(write(tmp_path / "f.mp4", 5), client=client)
    assert client.active_handlers() == 0
    assert client.send_calls[DISK] == 3


@pytest.mark.asyncio
async def test_handlers_are_removed_on_cancellation_and_task_shutdown(no_events, tmp_path):
    client = MultiClient(lambda *a: asyncio.sleep(0))
    uploader = multi([DISK], timeout_seconds=30)

    # cooperative cancel (job cancel)
    flag = {"cancel": False}
    task = asyncio.ensure_future(
        uploader.upload(write(tmp_path / "c.mp4", 5), client=client, should_cancel=lambda: flag["cancel"])
    )
    await asyncio.sleep(0.1)
    assert client.active_handlers() > 0  # listening while it waits
    flag["cancel"] = True
    with pytest.raises(UploadCancelled):
        await task
    assert client.active_handlers() == 0

    # hard task cancellation (Koyeb shutdown)
    client = MultiClient(lambda *a: asyncio.sleep(0))
    task = asyncio.ensure_future(uploader.upload(write(tmp_path / "d.mp4", 5), client=client))
    await asyncio.sleep(0.1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert client.active_handlers() == 0


@pytest.mark.asyncio
async def test_no_background_tasks_survive_an_upload(no_events, tmp_path):
    client = MultiClient(good_bot(URLS))
    before = {t for t in asyncio.all_tasks()}
    await multi([DISK]).upload(write(tmp_path / "a.mp4", 5), client=client)
    await asyncio.sleep(0.05)
    assert {t for t in asyncio.all_tasks() if not t.done()} <= before


# --------------------------------------------------------------------------- #
# 8-12. Pipeline behaviour (worker level)
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_one_failing_video_does_not_stop_the_next(settings, tmp_path, no_events):
    async def bot(b, name, sid, handlers):
        if name == "bad.mp4":
            await emit(handlers, event("Processing...", sid + 1, reply_to=sid))  # no link -> timeout
            return
        await good_bot(URLS)(b, name, sid, handlers)

    client = MultiClient(bot)
    worker, manager, *_ = make_worker(settings, video=multi(timeout_seconds=0.1))
    job = make_job(manager, client=client)
    prep(manager, job)
    vids = [write(tmp_path / n, 10) for n in ("bad.mp4", "good1.mp4", "good2.mp4")]

    await worker._upload_media(job, [], [media(v, "video") for v in vids])

    assert [r["file"] for r in job.metadata["video_results"]] == ["good1.mp4", "good2.mp4"]
    failure = job.metadata["video_failures"][0]
    assert failure["file"] == "bad.mp4" and failure["bot"] == DISK
    assert "did not reply" in failure["reason"] and failure["status"] == "failed"
    assert client.bots_sent_to("bad.mp4") == [DISK]  # no duplicate upload


@pytest.mark.asyncio
async def test_video_over_1_5_gb_is_rejected_and_never_sent_to_any_bot(settings, tmp_path, no_events):
    client = MultiClient(good_bot(URLS))
    worker, manager, *_ = make_worker(settings, video=multi())
    job = make_job(manager, client=client)
    prep(manager, job)
    big = sparse(tmp_path / "big.mp4", LIMIT + 1)
    ok = write(tmp_path / "ok.mp4", 10)

    await worker._upload_media(job, [], [media(big, "video"), media(ok, "video")])

    assert client.bots_sent_to("big.mp4") == []  # not sent to ANY bot
    assert [r["file"] for r in job.metadata["video_results"]] == ["ok.mp4"]  # the rest continued
    reason = job.metadata["video_failures"][0]["reason"]
    assert "size limit" in reason and job.metadata["video_failures"][0]["file"] == "big.mp4"


@pytest.mark.asyncio
async def test_uploader_itself_refuses_over_limit_videos(tmp_path):
    big = sparse(tmp_path / "big.mp4", LIMIT + 1)
    with pytest.raises(VideoTooLargeError):
        await multi().upload(big, client=object())


@pytest.mark.asyncio
async def test_video_exactly_at_the_limit_is_allowed(settings, tmp_path, no_events):
    client = MultiClient(good_bot(URLS))
    worker, manager, *_ = make_worker(settings, video=multi())
    job = make_job(manager, client=client)
    prep(manager, job)
    exact = sparse(tmp_path / "exact.mp4", LIMIT)

    await worker._upload_media(job, [], [media(exact, "video")])

    assert [r["file"] for r in job.metadata["video_results"]] == ["exact.mp4"]


@pytest.mark.asyncio
async def test_videos_never_reach_imgbb_or_telegraph(settings, tmp_path, no_events):
    client = MultiClient(good_bot(URLS))
    imgbb, telegraph = FakeImgBB(), FakePublisher()
    worker, manager, *_ = make_worker(settings, imgbb=imgbb, telegraph=telegraph, video=multi())
    job = make_job(manager, client=client)
    prep(manager, job)
    vids = [write(tmp_path / n, 10) for n in ("a.mp4", "b.mp4")]
    img = write(tmp_path / "pic.jpg", 10)

    await worker._upload_media(job, [media(img, "image")], [media(v, "video") for v in vids])

    assert [p.name for p in imgbb.calls] == ["pic.jpg"]  # only the image
    assert len(telegraph.calls) == 1
    title, urls = telegraph.calls[0]
    assert urls == ["https://imgbb.test/pic.jpg"]  # only the image URL; no video / provider link
    assert not any("diskwala" in u or "flezen" in u for u in urls)
    assert len(job.metadata["video_results"]) == 2

    # videos only: neither ImgBB nor Telegraph is touched at all
    imgbb2, telegraph2 = FakeImgBB(), FakePublisher()
    worker2, manager2, *_ = make_worker(settings, imgbb=imgbb2, telegraph=telegraph2, video=multi())
    job2 = make_job(manager2, client=MultiClient(good_bot(URLS)))
    prep(manager2, job2)
    await worker2._upload_media(job2, [], [media(vids[0], "video")])
    assert imgbb2.calls == [] and telegraph2.calls == []


class ExplodingPublisher:
    def __init__(self, exc: Exception) -> None:
        self.exc = exc
        self.calls = 0

    async def publish(self, title, image_urls):
        self.calls += 1
        raise self.exc


@pytest.mark.asyncio
async def test_telegraph_failure_does_not_stop_video_processing(settings, tmp_path, no_events):
    for exc in (UploadError("telegraph is down"), RuntimeError("boom")):
        client = MultiClient(good_bot(URLS))
        publisher = ExplodingPublisher(exc)
        worker, manager, *_ = make_worker(settings, telegraph=publisher, video=multi())
        job = make_job(manager, client=client)
        prep(manager, job)
        img = write(tmp_path / f"p{type(exc).__name__}.jpg", 10)
        vid = write(tmp_path / f"v{type(exc).__name__}.mp4", 10)

        await worker._upload_media(job, [media(img, "image")], [media(vid, "video")])

        assert publisher.calls == 1
        assert [r["status"] for r in job.metadata["video_results"]] == ["completed"]  # videos still done
        assert any(f.provider == "telegraph_article" for f in job.upload_failures)


@pytest.mark.asyncio
async def test_image_stage_crash_does_not_stop_video_processing(settings, tmp_path, no_events):
    class CrashingImgBB(FakeImgBB):
        async def upload(self, path):
            raise RuntimeError("imgbb crashed")

    client = MultiClient(good_bot(URLS))
    worker, manager, *_ = make_worker(settings, imgbb=CrashingImgBB(), video=multi())
    job = make_job(manager, client=client)
    prep(manager, job)
    img = write(tmp_path / "x.jpg", 10)
    vid = write(tmp_path / "x.mp4", 10)

    await worker._upload_media(job, [media(img, "image")], [media(vid, "video")])

    assert [r["file"] for r in job.metadata["video_results"]] == ["x.mp4"]


# --------------------------------------------------------------------------- #
# Progress / results
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_progress_shows_the_active_bot_and_the_provider(settings, tmp_path, no_events):
    seen: list[str] = []

    async def bot(b, name, sid, handlers):
        await good_bot(URLS)(b, name, sid, handlers)

    client = MultiClient(bot, fail_send_for=(DISK,))
    worker, manager, *_ = make_worker(settings, video=multi())
    job = make_job(manager, client=client)
    prep(manager, job)

    async def spy(j):
        seen.append(ProgressRenderer().render(j))

    worker._notify = spy  # capture every progress edit
    vid = write(tmp_path / "one.mp4", 10)
    await worker._upload_media(job, [], [media(vid, "video")])

    joined = "\n".join(seen)
    assert f"@{DISK}" in joined and f"@{FLEZEN}" in joined  # fell back, and it was shown
    assert "1/1 one.mp4 → ✅ URL received · Flezen" in seen[-1]

    manager.complete(job.job_id)
    final = ProgressRenderer().render(job)
    assert "https://flezen.com/s/FZ-one" in final
    assert "Flezen: https://flezen.com/s/FZ-one" in final
    assert "imgbb" not in final.lower()  # a video link is never an ImgBB link


def test_failed_video_is_listed_with_its_reason(settings):
    manager = JobManager(settings.job_dir)
    job = make_job(manager)
    manager.set_media_counts(job.job_id, image_count=0, video_count=1)
    for status in (JobStatus.DOWNLOADING, JobStatus.EXTRACTING, JobStatus.SCANNING, JobStatus.UPLOADING):
        manager.set_status(job.job_id, status)
    manager.set_metadata(job.job_id, "video_status", [
        {"filename": "v3.mp4", "status": "failed", "url": None, "error": "timeout", "bot": DISK, "provider": "diskwala"},
        {"filename": "v4.mp4", "status": "processing", "url": None, "error": None, "bot": FLEZEN, "provider": None},
    ])

    text = ProgressRenderer().render(job)

    assert "1/2 v3.mp4 → ❌ timeout" in text
    assert f"2/2 v4.mp4 → ⏳ Processing (video bot) · @{FLEZEN}" in text


def test_no_single_bot_username_is_hard_coded_in_source():
    root = Path(__file__).resolve().parent.parent / "app"
    for path in root.rglob("*.py"):
        source = path.read_text(encoding="utf-8")
        for name in ("DiskWalaFileUploaderBot", "FlezenUploadBot"):
            assert name not in source, f"{name} is hard-coded in {path.name}"
