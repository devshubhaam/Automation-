"""Upload providers used by the media processor."""
from .common import PART2_IMAGE_MAX_BYTES, UploadError
from .imgbb import ImgBBUploader
from .telegraph import TelegraphUploader
__all__ = ["ImgBBUploader", "TelegraphUploader", "UploadError", "PART2_IMAGE_MAX_BYTES"]
