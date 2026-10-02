"""Upload providers used by the media processor.

* ImgBB hosts the images.
* The video bot (a Telegram bot) hosts the videos and replies with a link.
* Telegraph only publishes an article that embeds the ImgBB image links and
  lists the video links; no file is ever uploaded to Telegraph.
"""
from .common import PART2_IMAGE_MAX_BYTES, UploadCancelled, UploadError
from .imgbb import ImgBBUploader
from .telegraph import TelegraphPublisher
from .video_bot import VideoBotUploader
__all__ = [
    "ImgBBUploader",
    "TelegraphPublisher",
    "VideoBotUploader",
    "UploadError",
    "UploadCancelled",
    "PART2_IMAGE_MAX_BYTES",
]
