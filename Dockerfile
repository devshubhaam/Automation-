# Telegram Media Processor - PART 1 (Telegram userbot)
#
# Build:  docker build -t telegram-media-processor:part1 .
# Run:    docker run -d --name tmp-bot \
#             --env-file .env \
#             -v "$(pwd)/data:/data" \
#             telegram-media-processor:part1
#
# The Telegram session, job directories and logs all live under /data, which is
# a volume, so a container restart never forces a new Telegram login.

FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# System dependencies. The app is pure Python; ffmpeg (which ships ffprobe) is
# only needed for MERGE_VIDEOS (lossless stream-copy merge of multiple videos).
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tzdata ffmpeg \
    && rm -rf /var/lib/apt/lists/* \
    && ffmpeg -version \
    && ffprobe -version

# Dependencies first for better layer caching
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Application code
COPY app ./app
COPY tests ./tests

# Persistent state lives here and is mounted as a volume
RUN mkdir -p /data/sessions /data/downloads /data/jobs /data/logs
VOLUME ["/data"]

# Paths are redirected into the volume; secrets still come from the environment.
ENV SESSION_DIR=/data/sessions \
    DOWNLOAD_DIR=/data/downloads \
    JOB_DIR=/data/jobs \
    LOG_DIR=/data/logs \
    SESSION_NAME=media_processor

# Run as a non-root user
RUN useradd --create-home --uid 1000 appuser \
    && chown -R appuser:appuser /app /data
USER appuser

CMD ["python", "-m", "app.main"]
