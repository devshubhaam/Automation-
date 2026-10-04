"""DiskWala reply-to URL capture: sent message id + reply_to_msg_id, history fallback."""

from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest

from app.uploaders.video_bot import DEFAULT_LINK_TIMEOUT_SECONDS, VideoBotUploader
from app.uploaders import UploadError

LINK = "https://www.diskwala.com/app/DW-abc"


def msg(mid, reply_to=None, text="", out=False):
    return SimpleNamespace(id=mid, reply_to_msg_id=reply_to, message=text, raw_text=text,
                           text=text, out=out, entities=None, reply_markup=None, buttons=None)


class Client:
    """No live events at all: the bot's answers are only visible in the chat history."""

    def __init__(self, history, sent_id=100):
        self.history, self.sent_id, self.polls = history, sent_id, 0

    def add_event_handler(self, *a, **k): pass
    def remove_event_handler(self, *a, **k): pass

    async def send_file(self, *a, **k):
        return SimpleNamespace(id=self.sent_id)

    async def get_messages(self, entity, min_id=0, limit=50):
        self.polls += 1
        return [m for m in self.history() if m.id > min_id][::-1]   # Telethon: newest first


def uploader(**kw):
    kw.setdefault("timeout_seconds", 1.0)
    kw.setdefault("poll_seconds", 0.05)
    kw.setdefault("history_poll_seconds", 0.1)
    kw.setdefault("require_reply", True)
    return VideoBotUploader("DiskWalaFileUploaderBot", **kw)


@pytest.fixture
def video(tmp_path):
    p = tmp_path / "v.mp4"
    p.write_bytes(b"x" * 10)
    return p


def test_default_wait_is_15_minutes():
    assert DEFAULT_LINK_TIMEOUT_SECONDS == 900.0 and VideoBotUploader("Bot").timeout_seconds == 900.0


@pytest.mark.asyncio
async def test_link_is_recovered_from_history_when_no_live_event_arrives(video, caplog):
    client = Client(lambda: [msg(101, reply_to=100, text=LINK)])
    with caplog.at_level(logging.INFO):
        result = await uploader().upload(video, client=client)
    assert result["url"] == LINK
    assert "Sent v.mp4 to @DiskWalaFileUploaderBot as message id=100" in caplog.text
    assert "URL received for v.mp4" in caplog.text and "reply_to=100 matches sent id=100" in caplog.text


@pytest.mark.asyncio
async def test_edited_placeholder_reply_is_found_in_history(video):
    state = {"n": 0}

    def history():                       # placeholder first, later EDITED to carry the link
        state["n"] += 1
        return [msg(101, reply_to=100, text=LINK if state["n"] > 2 else "Waiting...")]

    assert (await uploader().upload(video, client=Client(history)))["url"] == LINK


@pytest.mark.asyncio
async def test_reply_to_another_video_or_non_reply_is_never_accepted(video, caplog):
    other = "https://www.diskwala.com/app/DW-other"
    client = Client(lambda: [msg(101, reply_to=55, text=other), msg(102, text=other),
                             msg(103, reply_to=999, text=other)])
    with caplog.at_level(logging.INFO), pytest.raises(UploadError, match="did not reply"):
        await uploader(timeout_seconds=0.5).upload(video, client=client)
    assert "reply to a different message" in caplog.text and "not a reply to the sent video" in caplog.text
    assert "Timeout: no link" in caplog.text


@pytest.mark.asyncio
async def test_reply_chain_in_history_is_followed(video):
    client = Client(lambda: [msg(101, reply_to=100, text="Processing"), msg(102, reply_to=101, text=LINK)])
    assert (await uploader().upload(video, client=client))["url"] == LINK


@pytest.mark.asyncio
async def test_our_own_messages_in_history_are_ignored(video):
    client = Client(lambda: [msg(101, reply_to=100, text=LINK, out=True)])
    with pytest.raises(UploadError):
        await uploader(timeout_seconds=0.4).upload(video, client=client)


@pytest.mark.asyncio
async def test_history_check_can_be_disabled_and_missing_get_messages_is_tolerated(video):
    client = Client(lambda: [msg(101, reply_to=100, text=LINK)])
    with pytest.raises(UploadError):
        await uploader(timeout_seconds=0.4, history_poll_seconds=0).upload(video, client=client)
    assert client.polls == 0
    plain = Client(lambda: [])
    plain.get_messages = None
    with pytest.raises(UploadError):
        await uploader(timeout_seconds=0.4).upload(video, client=plain)


@pytest.mark.asyncio
async def test_list_returned_by_send_file_still_yields_the_sent_id(video):
    client = Client(lambda: [msg(101, reply_to=100, text=LINK)])

    async def send_file(*a, **k):
        return [SimpleNamespace(id=100)]

    client.send_file = send_file
    assert (await uploader().upload(video, client=client))["url"] == LINK
