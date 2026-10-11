"""Image format ingestor using Pillow for raster and drone imagery verification."""

import logging
from pathlib import Path

from PIL import Image, UnidentifiedImageError

from beacon.ingestion.base import BaseIngestor
from beacon.ingestion.result import IngestionResult
from beacon.models import (
    FORBIDDEN_SECRET_KEYS,
    ExtractionMethod,
    ExtractionStatus,
    SourceType,
)

logger = logging.getLogger("beacon.ingestion.image")


class ImageIngestor(BaseIngestor):
    """Validates raster images, extracts image container metadata, and records OCR deferrals."""

    def __init__(self) -> None:
        super().__init__(SourceType.IMAGE, ExtractionMethod.OCR)

    def ingest(self, path: Path) -> IngestionResult:
        content_hash, file_size = self.compute_sha256(path)
        logger.info("Ingesting image source '%s' (hash=%s, size=%d bytes)", path.name, content_hash[:8], file_size)

        # 1. Validate that the file is genuinely a valid readable image
        try:
            with Image.open(path) as img:
                img.verify()
        except UnidentifiedImageError as err:
            err_msg = f"File '{path.name}' is not a recognized or supported image format: {err}"
            logger.error("Image identification failure: %s", err_msg)
            source_rec = self.create_source_record(
                path=path,
                content_hash=content_hash,
                status=ExtractionStatus.FAILED,
                metadata={"file_size": file_size, "image_error": "UnidentifiedImageError"},
            )
            return IngestionResult(
                source_record=source_rec,
                status=ExtractionStatus.FAILED,
                errors=[err_msg],
            )
        except Exception as err:
            err_msg = f"Corrupt or unreadable image file '{path.name}': {err}"
            logger.error("Image validation failure: %s", err_msg)
            source_rec = self.create_source_record(
                path=path,
                content_hash=content_hash,
                status=ExtractionStatus.FAILED,
                metadata={"file_size": file_size, "image_error": str(err)},
            )
            return IngestionResult(
                source_record=source_rec,
                status=ExtractionStatus.FAILED,
                errors=[err_msg],
            )

        # 2. Extract genuine container metadata (dimensions, format, mode)
        # Note: Must re-open image because verify() invalidates the image instance
        try:
            with Image.open(path) as img:
                width, height = img.size
                img_format = img.format or "UNKNOWN"
                img_mode = img.mode

                # Clean and filter image.info to ensure no secrets or huge binary objects
                safe_info: dict[str, str] = {}
                for k, v in (img.info or {}).items():
                    k_str = str(k).lower()
                    if any(secret in k_str for secret in FORBIDDEN_SECRET_KEYS):
                        continue
                    if isinstance(v, (str, int, float, bool)):
                        safe_info[str(k)] = str(v)
        except Exception as err:
            err_msg = f"Failed to extract image metadata from '{path.name}': {err}"
            logger.error("Image metadata error: %s", err_msg)
            source_rec = self.create_source_record(
                path=path,
                content_hash=content_hash,
                status=ExtractionStatus.FAILED,
                metadata={"file_size": file_size},
            )
            return IngestionResult(
                source_record=source_rec,
                status=ExtractionStatus.FAILED,
                errors=[err_msg],
            )

        meta = {
            "file_size": file_size,
            "width": width,
            "height": height,
            "image_format": img_format,
            "color_mode": img_mode,
            "has_visual_scene": True,
        }
        if safe_info:
            meta["embedded_info"] = safe_info

        source_rec = self.create_source_record(
            path=path,
            content_hash=content_hash,
            status=ExtractionStatus.SUCCESS,
            metadata=meta,
        )

        # Strictly per Phase 2 boundary:
        # Visual disaster scenes contain no machine-readable text without OCR/Vision.
        # We do NOT infer disaster facts programmatically. Return SUCCESS with 0 observations.
        warning_msg = (
            "Image processed successfully. Visual raster scene contains no machine-readable text; "
            "semantic disaster extraction deferred to OCR/computer-vision phase (Phase 2 boundary)."
        )

        return IngestionResult(
            source_record=source_rec,
            observations=[],
            status=ExtractionStatus.SUCCESS,
            warnings=[warning_msg],
            processed_count=0,
            accepted_count=0,
            rejected_count=0,
        )
