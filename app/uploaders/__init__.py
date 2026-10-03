"""Upload providers used by the media processor.

* ImgBB hosts the images.
* The video bots (Telegram bots listed in ``VIDEO_BOTS``) process the videos and
  reply with a link.
* Telegraph only publishes an article that embeds the ImgBB IMAGE links; no
  file and no video link is ever sent to Telegraph.
"""
from .common import PART2_IMAGE_MAX_BYTES, UploadCancelled, UploadError
from .imgbb import ImgBBUploader
from .telegraph import TelegraphPublisher
from .video_bot import (
    MultiVideoBotUploader,
    VideoBotRejectedError,
    VideoBotUploader,
    VideoDeliveryError,
    VideoTimeoutError,
    VideoTooLargeError,
    VideoUploadFailed,
    provider_for_url,
    provider_label,
    video_too_large_message,
)
__all__ = [
    "ImgBBUploader",
    "TelegraphPublisher",
    "VideoBotUploader",
    "MultiVideoBotUploader",
    "VideoTooLargeError",
    "VideoDeliveryError",
    "VideoTimeoutError",
    "VideoBotRejectedError",
    "VideoUploadFailed",
    "provider_for_url",
    "provider_label",
    "video_too_large_message",
    "UploadError",
    "UploadCancelled",
    "PART2_IMAGE_MAX_BYTES",
]
