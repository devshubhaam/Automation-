"""Uploader tests (offline; httpx is mocked)."""

from pathlib import Path

import pytest

from app.uploaders import (ImgBBUploader, PART2_IMAGE_MAX_BYTES, TelegraphPublisher, UploadCancelled,
                           UploadError, VideoBotUploader)
from app.uploaders.video_bot import extract_url, normalise_username
from app.uploaders import video_bot as video_bot_module
from app.uploaders import imgbb as imgbb_module
from app.uploaders import telegraph as telegraph_module

SECRET = "SUPERSECRETKEY123"


def test_single_shared_limit():
    assert PART2_IMAGE_MAX_BYTES == 2 * 1024 * 1024
    assert ImgBBUploader(SECRET).max_bytes == PART2_IMAGE_MAX_BYTES


@pytest.mark.asyncio
async def test_imgbb_rejects_video_without_network(tmp_path: Path):
    path = tmp_path / "clip.mp4"
    path.write_bytes(b"video")
    with pytest.raises(UploadError, match="images only"):
        await ImgBBUploader(SECRET).upload(path)


@pytest.mark.asyncio
async def test_imgbb_rejects_large_image(tmp_path: Path):
    path = tmp_path / "large.png"
    path.write_bytes(b"x" * (PART2_IMAGE_MAX_BYTES + 1))
    with pytest.raises(UploadError, match="exceeds"):
        await ImgBBUploader(SECRET).upload(path)


@pytest.mark.asyncio
async def test_imgbb_requires_api_key(tmp_path: Path):
    path = tmp_path / "a.jpg"
    path.write_bytes(b"img")
    with pytest.raises(UploadError, match="IMGBB_API_KEY"):
        await ImgBBUploader(None).upload(path)


class _FakeResponse:
    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self):
        return self._payload


def _patch_client(monkeypatch, module, response):
    calls = []

    class FakeClient:
        def __init__(self, *a, **k): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False
        async def post(self, url, **kwargs):
            calls.append((url, kwargs))
            return response.pop(0) if isinstance(response, list) else response

    monkeypatch.setattr(module.httpx, "AsyncClient", FakeClient)
    return calls


@pytest.mark.asyncio
async def test_imgbb_error_message_never_contains_api_key(tmp_path, monkeypatch):
    path = tmp_path / "a.jpg"
    path.write_bytes(b"img")
    _patch_client(monkeypatch, imgbb_module,
                  _FakeResponse(200, {"success": False, "error": {"message": f"Invalid key {SECRET}"}}))
    with pytest.raises(UploadError) as info:
        await ImgBBUploader(SECRET, max_retries=0).upload(path)
    assert SECRET not in str(info.value)


@pytest.mark.asyncio
async def test_imgbb_http_error_never_contains_api_key(tmp_path, monkeypatch):
    path = tmp_path / "a.jpg"
    path.write_bytes(b"img")
    _patch_client(monkeypatch, imgbb_module, _FakeResponse(400, {}))
    with pytest.raises(UploadError) as info:
        await ImgBBUploader(SECRET, max_retries=0).upload(path)
    assert SECRET not in str(info.value) and SECRET not in repr(info.value)


@pytest.mark.asyncio
async def test_imgbb_success_normalised(tmp_path, monkeypatch):
    path = tmp_path / "a.jpg"
    path.write_bytes(b"img")
    _patch_client(monkeypatch, imgbb_module,
                  _FakeResponse(200, {"success": True, "data": {"url": "https://i.ibb.co/x/a.jpg", "id": "x"}}))
    result = await ImgBBUploader(SECRET).upload(path)
    assert result["provider"] == "imgbb" and result["url"] == "https://i.ibb.co/x/a.jpg"


# --- Telegraph: article publisher (never uploads files) ----------------------

def test_telegraph_module_has_no_file_upload():
    source = Path(telegraph_module.__file__).read_text(encoding="utf-8")
    assert "files=" not in source and "/upload" not in source
    assert not hasattr(telegraph_module, "TelegraphUploader")


@pytest.mark.asyncio
async def test_telegraph_publish_embeds_imgbb_urls(monkeypatch):
    calls = _patch_client(monkeypatch, telegraph_module, _FakeResponse(
        200, {"ok": True, "result": {"path": "a-10-03", "url": "https://telegra.ph/a-10-03"}}))
    urls = ["https://i.ibb.co/1/a.jpg", "https://i.ibb.co/2/b.jpg"]

    results = await TelegraphPublisher("TOKEN").publish("Album", urls)

    assert len(results) == 1 and results[0]["url"] == "https://telegra.ph/a-10-03"
    assert results[0]["provider"] == "telegraph_article" and results[0]["image_count"] == 2
    url, kwargs = calls[0]
    assert url.endswith("/createPage") and "files" not in kwargs
    assert kwargs["data"]["title"] == "Album" and kwargs["data"]["access_token"] == "TOKEN"
    import json
    assert json.loads(kwargs["data"]["content"]) == [
        {"tag": "img", "attrs": {"src": urls[0]}}, {"tag": "img", "attrs": {"src": urls[1]}}]


@pytest.mark.asyncio
async def test_telegraph_creates_account_when_no_token(monkeypatch):
    calls = _patch_client(monkeypatch, telegraph_module, [
        _FakeResponse(200, {"ok": True, "result": {"access_token": "NEWTOKEN"}}),
        _FakeResponse(200, {"ok": True, "result": {"path": "p", "url": "https://telegra.ph/p"}}),
        _FakeResponse(200, {"ok": True, "result": {"path": "q", "url": "https://telegra.ph/q"}}),
    ])
    publisher = TelegraphPublisher(None)

    await publisher.publish("T", ["https://i.ibb.co/a.jpg"])
    await publisher.publish("T", ["https://i.ibb.co/a.jpg"])

    methods = [c[0].rsplit("/", 1)[1] for c in calls]
    assert methods == ["createAccount", "createPage", "createPage"]  # account created once
    assert calls[1][1]["data"]["access_token"] == "NEWTOKEN"


@pytest.mark.asyncio
async def test_telegraph_splits_large_albums(monkeypatch):
    pages = [_FakeResponse(200, {"ok": True, "result": {"path": f"p{i}", "url": f"https://telegra.ph/p{i}"}})
             for i in range(3)]
    calls = _patch_client(monkeypatch, telegraph_module, pages)
    urls = [f"https://i.ibb.co/{i}.jpg" for i in range(5)]

    results = await TelegraphPublisher("T", max_images_per_article=2).publish("Album", urls)

    assert [r["image_count"] for r in results] == [2, 2, 1]
    assert [c[1]["data"]["title"] for c in calls] == ["Album (Part 1)", "Album (Part 2)", "Album (Part 3)"]


@pytest.mark.asyncio
async def test_telegraph_rejects_empty_and_relative_urls():
    with pytest.raises(UploadError, match="No image URLs"):
        await TelegraphPublisher("T").publish("A", [])
    with pytest.raises(UploadError, match="absolute"):
        await TelegraphPublisher("T").publish("A", ["/file/x.jpg"])


@pytest.mark.asyncio
async def test_telegraph_api_error_is_permanent_and_hides_token(monkeypatch):
    _patch_client(monkeypatch, telegraph_module,
                  _FakeResponse(200, {"ok": False, "error": "ACCESS_DENIED secret123"}))
    with pytest.raises(UploadError) as info:
        await TelegraphPublisher("secret123", max_retries=0).publish("A", ["https://i.ibb.co/a.jpg"])
    assert "ACCESS_DENIED" in str(info.value) and "secret123" not in str(info.value)


@pytest.mark.asyncio
async def test_telegraph_article_contains_only_image_nodes(monkeypatch):
    calls = _patch_client(monkeypatch, telegraph_module, _FakeResponse(
        200, {"ok": True, "result": {"path": "a", "url": "https://telegra.ph/a"}}))

    results = await TelegraphPublisher("T").publish("Album", ["https://i.ibb.co/1.jpg"])

    assert results[0]["image_count"] == 1 and "video_count" not in results[0]
    import json
    nodes = json.loads(calls[0][1]["data"]["content"])
    assert nodes == [{"tag": "img", "attrs": {"src": "https://i.ibb.co/1.jpg"}}]


@pytest.mark.asyncio
async def test_telegraph_publish_takes_no_video_links(monkeypatch):
    _patch_client(monkeypatch, telegraph_module, _FakeResponse(
        200, {"ok": True, "result": {"path": "a", "url": "https://telegra.ph/a"}}))
    with pytest.raises(TypeError):
        await TelegraphPublisher("T").publish("A", ["https://i.ibb.co/1.jpg"], [("v.mp4", "https://x.test/app/1")])
    assert not hasattr(telegraph_module, "VideoLink")


# --- Video bot ---------------------------------------------------------------

import asyncio
import re
from types import SimpleNamespace

PATTERN = re.compile(video_bot_module.DEFAULT_URL_PATTERN)
BOT_LINK = "https://www.domain.com/app/6abfae122a52418b24707585"


def test_normalise_username():
    assert normalise_username("@FileUploaderBot") == "FileUploaderBot"
    assert normalise_username("https://t.me/FileUploaderBot") == "FileUploaderBot"
    assert normalise_username("  FileUploaderBot ") == "FileUploaderBot"
    assert normalise_username("") is None and normalise_username(None) is None


def test_extract_url_from_reply_text():
    msg = SimpleNamespace(raw_text=f"Your link: {BOT_LINK}.", entities=None, buttons=None)
    assert extract_url(msg, PATTERN) == BOT_LINK
    assert extract_url(SimpleNamespace(raw_text="Uploading...", entities=None, buttons=None), PATTERN) is None


def test_extract_url_from_hidden_link_and_button():
    entity = SimpleNamespace(url=BOT_LINK)
    assert extract_url(SimpleNamespace(raw_text="Open", entities=[entity], buttons=None), PATTERN) == BOT_LINK
    button = SimpleNamespace(url=BOT_LINK)
    assert extract_url(SimpleNamespace(raw_text="Done", entities=None, buttons=[[button]]), PATTERN) == BOT_LINK


class FakeTgClient:
    """Minimal Telethon client: send_file() then the bot 'replies' via script."""

    def __init__(self, script):
        self.script = script  # async fn(handlers, sent_id)
        self.handlers = []
        self.sent = []
        self.removed = 0

    def add_event_handler(self, cb, builder): self.handlers.append(cb)
    def remove_event_handler(self, cb, builder): self.removed += 1

    async def send_file(self, entity, path, **kw):
        self.sent.append((entity, path, kw))
        sent = SimpleNamespace(id=100)
        asyncio.get_running_loop().create_task(self.script(self.handlers, 100))
        return sent


def _evt(text, msg_id):
    return SimpleNamespace(message=SimpleNamespace(id=msg_id, raw_text=text, entities=None, buttons=None))


def _video(tmp_path, name="clip.mp4"):
    path = tmp_path / name
    path.write_bytes(b"video")
    return path


@pytest.mark.asyncio
async def test_video_bot_returns_link_from_reply(tmp_path, monkeypatch):
    monkeypatch.setattr(VideoBotUploader, "_event_builders", lambda self: [object()])

    async def script(handlers, sent_id):
        await asyncio.sleep(0.01)
        await handlers[0](_evt("Processing...", sent_id + 1))   # progress message, no link
        await handlers[0](_evt(f"Link: {BOT_LINK}", sent_id + 2))

    client = FakeTgClient(script)
    result = await VideoBotUploader("@FileUploaderBot", poll_seconds=0.01).upload(_video(tmp_path), client=client)

    assert result == {"provider": "video_bot", "url": BOT_LINK}
    assert client.sent[0][0] == "FileUploaderBot" and client.sent[0][1].endswith("clip.mp4")
    assert client.removed == 1  # handler is always cleaned up


@pytest.mark.asyncio
async def test_video_bot_accepts_link_added_by_edit(tmp_path, monkeypatch):
    monkeypatch.setattr(VideoBotUploader, "_event_builders", lambda self: [object()])

    async def script(handlers, sent_id):
        await handlers[0](_evt("Uploading 50%", sent_id + 1))
        await asyncio.sleep(0.02)
        await handlers[0](_evt(f"Done {BOT_LINK}", sent_id + 1))  # same message, edited

    result = await VideoBotUploader("Bot", poll_seconds=0.01).upload(_video(tmp_path), client=FakeTgClient(script))
    assert result["url"] == BOT_LINK


@pytest.mark.asyncio
async def test_video_bot_ignores_links_from_older_messages(tmp_path, monkeypatch):
    monkeypatch.setattr(VideoBotUploader, "_event_builders", lambda self: [object()])

    async def script(handlers, sent_id):
        await handlers[0](_evt("https://www.domain.com/app/OLD", sent_id - 5))  # earlier video's reply
        await handlers[0](_evt(BOT_LINK, sent_id + 1))

    result = await VideoBotUploader("Bot", poll_seconds=0.01).upload(_video(tmp_path), client=FakeTgClient(script))
    assert result["url"] == BOT_LINK


@pytest.mark.asyncio
async def test_video_bot_timeout(tmp_path, monkeypatch):
    monkeypatch.setattr(VideoBotUploader, "_event_builders", lambda self: [object()])

    async def script(handlers, sent_id):
        await handlers[0](_evt("no link here", sent_id + 1))

    client = FakeTgClient(script)
    with pytest.raises(UploadError, match="did not reply with a link"):
        await VideoBotUploader("Bot", timeout_seconds=0.05, poll_seconds=0.01).upload(_video(tmp_path), client=client)
    assert client.removed == 1


@pytest.mark.asyncio
async def test_video_bot_cancel(tmp_path, monkeypatch):
    monkeypatch.setattr(VideoBotUploader, "_event_builders", lambda self: [object()])

    async def script(handlers, sent_id):
        pass

    with pytest.raises(UploadCancelled):
        await VideoBotUploader("Bot", poll_seconds=0.01).upload(
            _video(tmp_path), client=FakeTgClient(script), should_cancel=lambda: True)


@pytest.mark.asyncio
async def test_video_bot_custom_url_pattern(tmp_path, monkeypatch):
    monkeypatch.setattr(VideoBotUploader, "_event_builders", lambda self: [object()])

    async def script(handlers, sent_id):
        await handlers[0](_evt("see https://other.test/x and " + BOT_LINK, sent_id + 1))

    uploader = VideoBotUploader("Bot", poll_seconds=0.01, url_pattern=r"https://www\.domain\.com/app/\w+")
    result = await uploader.upload(_video(tmp_path), client=FakeTgClient(script))
    assert result["url"] == BOT_LINK


@pytest.mark.asyncio
async def test_video_bot_rejects_images_missing_config_and_no_client(tmp_path):
    with pytest.raises(UploadError, match="VIDEO_BOT_USERNAME"):
        await VideoBotUploader(None).upload(_video(tmp_path), client=object())
    img = tmp_path / "a.jpg"
    img.write_bytes(b"x")
    with pytest.raises(UploadError, match="videos only"):
        await VideoBotUploader("Bot").upload(img, client=object())
    with pytest.raises(UploadError, match="client is unavailable"):
        await VideoBotUploader("Bot").upload(_video(tmp_path))
