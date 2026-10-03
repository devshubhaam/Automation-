"""Upload providers used by the media processor.

* ImgBB hosts the images.
* The video bot (a Telegram bot) processes the videos and replies with a link.
* Telegraph only publishes an article that embeds the ImgBB IMAGE links; no
  file and no video link is ever sent to Telegraph.
"""
from .common import PART2_IMAGE_MAX_BYTES, UploadCancelled, UploadError
from .imgbb import ImgBBUploader
from .telegraph import TelegraphPublisher
from .video_bot import VideoBotUploader, VideoTooLargeError, video_too_large_message
__all__ = [
    "ImgBBUploader",
    "TelegraphPublisher",
    "VideoBotUploader",
    "VideoTooLargeError",
    "video_too_large_message",
    "UploadError",
    "UploadCancelled",
    "PART2_IMAGE_MAX_BYTES",
]
