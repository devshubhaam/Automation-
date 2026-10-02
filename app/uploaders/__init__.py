"""Upload providers used by the media processor."""
from .imgbb import ImgBBUploader
from .telegraph import TelegraphUploader
__all__ = ["ImgBBUploader", "TelegraphUploader"]
