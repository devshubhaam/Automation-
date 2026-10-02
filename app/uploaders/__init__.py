"""Upload providers used by the media processor.

ImgBB hosts the images. Telegraph only publishes articles that embed the ImgBB
links; no image file is ever uploaded to Telegraph.
"""
from .common import PART2_IMAGE_MAX_BYTES, UploadError
from .imgbb import ImgBBUploader
from .telegraph import TelegraphPublisher
__all__ = ["ImgBBUploader", "TelegraphPublisher", "UploadError", "PART2_IMAGE_MAX_BYTES"]
