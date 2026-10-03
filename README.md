# Telegram Media Processor

A **Telegram userbot** (Telethon, running on your own account). Send it a ZIP
archive; it downloads and safely extracts it, scans it for images and videos and
processes them like this:

```
ZIP ─► download ─► safe extract ─► scan
                                    │
      images ≤ 2 MiB ─► ImgBB ─► image URLs ─► ONE Telegraph article (images only)
      images > 2 MiB ─► skipped (metadata["part2_skipped_large_images"], not an error)
                                    │
      each video (< 1.5 GB) ─► video bot ─► generated URL ─► stored against that
                                            exact video filename
```

* Videos are **never** uploaded to ImgBB and **never** sent to Telegraph.
* Telegraph receives **only ImgBB image URLs** (`TelegraphPublisher.publish(title, image_urls)`).
  If there are no uploaded images (e.g. a ZIP with only videos) no article is created.
* The progress message is one Telegram message, edited in place, ending in
  `COMPLETED`, `COMPLETED_WITH_ERRORS`, `FAILED` or `CANCELLED`.

---

## 1. Components

| Module | Responsibility |
| --- | --- |
| `app/config.py` | Reads `.env` into one frozen `Settings` object. Secrets are redacted in `repr`. |
| `app/telegram_client.py` | Userbot client, owner restriction, `/ping /start /status /cancel`. Session stored in MongoDB when `MONGODB_URI` is set. |
| `app/login_bot.py` | Small Telegram bot (`BOT_TOKEN`) that performs the QR `/login` of the userbot (owner only). |
| `app/session_store.py` | MongoDB storage of the Telethon `StringSession`. |
| `app/archive_processor.py` | ZIP validation and hardened, streamed extraction. |
| `app/media_scanner.py` | Recursive image/video detection, natural ordering. |
| `app/job_manager.py` | `Job` model, status machine, results/failures/metadata bookkeeping. **No queue.** |
| `app/main.py` | `PipelineWorker` (owns the only `asyncio.Queue`), upload pipeline, health server, entry point. |
| `app/uploaders/imgbb.py` | ImgBB upload (images only, ≤ 2 MiB). |
| `app/uploaders/telegraph.py` | Telegraph `createPage` with `<img>` nodes (image URLs only). |
| `app/uploaders/video_bot.py` | Sends a video to the video bot and returns the link it replies with. |
| `app/progress.py` | Renders the progress message. |

### Job status

```
RECEIVED ► QUEUED ► DOWNLOADING ► EXTRACTING ► SCANNING ► UPLOADING ► COMPLETED
                                                                     └► COMPLETED_WITH_ERRORS
        any active state ► FAILED        any active state ► CANCELLED
```

`UPLOADING` covers ImgBB, the Telegraph article **and** the video-bot stage.
`COMPLETED_WITH_ERRORS` means at least one upload failed (including a video that
timed out or was over the size limit). Terminal states never change again.

---

## 2. Videos in detail

For each video, one after another and independently:

1. check cancellation, 2. check the file exists, 3. check the size (`VIDEO_MAX_SIZE_GB`,
default **1.5 GB = 1536 MiB**; larger videos are **not** sent and are recorded as a
failure), 4. send it to `VIDEO_BOT_USERNAME`, 5. wait for the bot's link, 6. store the
link against that video, 7. continue with the next one.

**Mapping video → URL.** Videos are sent strictly one at a time. A bot message is only
accepted for the current video if it is newer than the message we sent and, when it is
a Telegram *reply*, it replies to exactly that message. A late answer for an earlier
(timed-out) video can therefore never be attached to the next one. The link is the
first match of `VIDEO_URL_PATTERN` in the reply text, in a hidden link or in a button;
by default any `https://<domain>/app/<id>` or `/s/<id>` link. Edited bot messages
(“Uploading…” → link) are handled too.

**Memory.** The file path is given to Telethon, which streams it from disk in small
parts. The video is never read into RAM, so 100 MB – 1.4 GB videos are fine.

**Timeout and retry (per video).**
`VIDEO_BOT_TIMEOUT_SECONDS` (default 1800) is the wait for the link after the bot has
the file. A timeout is **not** retried, because the bot may still be processing it
(re-sending would duplicate work). Only a failed *delivery* of the file is retried, up
to `VIDEO_BOT_SEND_ATTEMPTS` (default 2).

**One failure never affects the others.** The failed video is recorded and the next one
runs; successful results are kept.

### Result metadata (`job.metadata`, included in `job.to_dict()`)

| Key | Content |
| --- | --- |
| `detected_images`, `detected_videos` | Everything found by the scanner |
| `part2_skipped_large_images` | Images > 2 MiB that were skipped |
| `imgbb_results` | `[{filename, relative_path, url}]` for successful ImgBB uploads |
| `telegraph_articles` | Article URL(s), only if images were uploaded |
| `video_processing` | `"video_bot"` or `"not_required"` |
| `video_status` | Per video: `{filename, relative_path, status, url, error}` (`pending`, `processing`, `done`, `failed`, `cancelled`) |
| `video_results` | `[{filename, relative_path, url}]` — the filename → URL mapping |
| `video_failures` | `[{filename, relative_path, error}]` |

`job.upload_results` / `job.upload_failures` hold the same information as structured
records (`provider` is `imgbb`, `telegraph_article` or `video_bot`).

---

## 3. Cancellation

`/cancel <job_id>` only sets `cancel_requested`. The worker checks it between stages, between
images, between videos, while a video is being sent to the bot and while waiting for the
bot's link (about every second), then stops safely and marks the job `CANCELLED`. The
worker task itself is never killed. A queued job that is cancelled is skipped.

## 4. Queue

`PipelineWorker` owns the only processing queue (`asyncio.Queue`). `JobManager` is
bookkeeping only. `/status` shows the real `PipelineWorker` queue size.

## 5. Security

Owner-only processing; path-traversal, symlink/special-file and ZIP-bomb protection
(`MAX_ARCHIVE_SIZE_MB`, `MAX_EXTRACTED_SIZE_MB`, `MAX_FILES_PER_ARCHIVE`); the Telegram
session is stored in MongoDB (or a local file); `API_HASH`, `BOT_TOKEN`, `MONGODB_URI`,
`IMGBB_API_KEY`, the Telegraph token and session strings are never logged or shown in
progress messages. `.env` and `*.session` are git-ignored.

The ZIP limits apply to the archive. The 1.5 GB limit applies to **each video**.

---

## 6. Configuration

Copy `.env.example` to `.env`. Required: `API_ID`, `API_HASH`.

| Variable | Default | Meaning |
| --- | --- | --- |
| `API_ID`, `API_HASH` | — | Telegram API credentials (required) |
| `BOT_TOKEN`, `BOT_OWNER_ID` | — | Login bot for the QR `/login` |
| `MONGODB_URI`, `MONGODB_DATABASE`, `MONGODB_COLLECTION` | — / `telegram_media_processor` / `sessions` | Persistent session storage |
| `IMGBB_API_KEY` | — | ImgBB key; without it every image upload fails with a clear error |
| `TELEGRAPH_ACCESS_TOKEN` | *(anonymous account per start)* | Telegraph account token |
| `TELEGRAPH_AUTHOR_NAME` | — | Author shown on the article |
| `VIDEO_BOT_USERNAME` | — | e.g. `@FileUploaderBot`; without it every video fails with a clear error |
| `VIDEO_BOT_TIMEOUT_SECONDS` | `1800` | Per-video wait for the link (10–7200) |
| `VIDEO_BOT_SEND_ATTEMPTS` | `2` | Delivery attempts per video (1–5) |
| `VIDEO_MAX_SIZE_GB` | `1.5` | Per-video size limit |
| `VIDEO_URL_PATTERN` | any `/app/<id>` or `/s/<id>` link | Regex for the link in the bot's reply |
| `MAX_ARCHIVE_SIZE_MB` / `MAX_EXTRACTED_SIZE_MB` / `MAX_FILES_PER_ARCHIVE` | `500` / `2000` / `10000` | ZIP safety limits |
| `DOWNLOAD_DIR`, `JOB_DIR`, `LOG_DIR`, `SESSION_DIR` | `./data/...` | Storage paths |
| `KEEP_JOB_FILES` | `false` | Keep `archive/` and `extracted/` after a job |
| `WORKER_COUNT` | `1` | Pipeline workers (1–4) |
| `LOG_LEVEL`, `LOG_MAX_BYTES`, `LOG_BACKUP_COUNT` | `INFO`, `5242880`, `5` | Logging |

Disk: the ZIP and its extracted files exist on disk at the same time, so a 500 MB ZIP
needs roughly 1 GB or more of free space on the instance.

## 7. Koyeb deployment

1. Deploy the repository (Dockerfile build). The container runs `python -m app.main`
   and serves a health endpoint on port 8000.
2. Set the environment variables from section 6 as Koyeb secrets/env — at least
   `API_ID`, `API_HASH`, `BOT_TOKEN`, `BOT_OWNER_ID`, `MONGODB_URI`, `IMGBB_API_KEY`,
   `VIDEO_BOT_USERNAME` (and `TELEGRAPH_ACCESS_TOKEN`).
3. Open your login bot in Telegram and send `/login`, then scan the QR code. The session
   is saved in MongoDB, so redeploys do not require a new login.
4. Send a ZIP to your Saved Messages. Start with a small ZIP (one image, one short video)
   before a large one.

## 8. Commands

| Command | Reply |
| --- | --- |
| `/ping` | Pong |
| `/status` | Job counts, real pipeline queue size and the latest jobs |
| `/cancel <job_id>` | Requests cancellation of that job |

## 9. Tests

```bash
pip install -r requirements.txt
python -m compileall app
python -m pytest -q
```

All tests are offline: ImgBB, Telegraph, Telegram and the video bot are replaced by
fakes. `tests/test_part3_video.py` covers the video flow (routing, mapping, size limit,
timeout/retry, cancellation, no full-file reads, metadata, status).
`scripts/smoke_startup.py` is a startup/config smoke test.
