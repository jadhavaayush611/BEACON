"""Central format dispatcher and public ingestion API for BEACON."""

from datetime import datetime, timezone
import logging
from pathlib import Path
from typing import Union

from beacon.ingestion.base import BaseIngestor
from beacon.ingestion.csv_ingestor import CsvIngestor
from beacon.ingestion.exceptions import (
    SourceFileNotFoundError,
    UnsupportedFormatError,
)
from beacon.ingestion.image_ingestor import ImageIngestor
from beacon.ingestion.json_ingestor import JsonIngestor
from beacon.ingestion.pdf_ingestor import PdfIngestor
from beacon.ingestion.result import IngestionResult
from beacon.ingestion.txt_ingestor import TxtIngestor
from beacon.models import (
    ExtractionMethod,
    ExtractionStatus,
    SourceRecord,
    SourceType,
    generate_source_id,
)

logger = logging.getLogger("beacon.ingestion.dispatcher")

# Mapping of file extensions (lowercase) to their specialized ingestor classes
EXTENSION_INGESTOR_MAP: dict[str, type[BaseIngestor]] = {
    ".json": JsonIngestor,
    ".csv": CsvIngestor,
    ".txt": TxtIngestor,
    ".pdf": PdfIngestor,
    ".png": ImageIngestor,
    ".jpg": ImageIngestor,
    ".jpeg": ImageIngestor,
    ".tiff": ImageIngestor,
    ".bmp": ImageIngestor,
    ".webp": ImageIngestor,
}


def get_ingestor_for_path(path: Path) -> BaseIngestor:
    """Resolve the appropriate ingestor for a given file path based on extension.

    Raises:
        UnsupportedFormatError: If the file extension is not recognized.
    """
    ext = path.suffix.lower()
    ingestor_cls = EXTENSION_INGESTOR_MAP.get(ext)
    if not ingestor_cls:
        raise UnsupportedFormatError(
            f"Unsupported file format '{ext}' for file '{path.name}'. "
            f"Supported extensions: {sorted(list(EXTENSION_INGESTOR_MAP.keys()))}"
        )
    return ingestor_cls()


def ingest_file(
    path: Union[str, Path],
    raise_on_unsupported: bool = False,
    raise_on_missing: bool = False,
) -> IngestionResult:
    """Public entry point for BEACON multi-format file ingestion.

    Inspects the input file, identifies format via extension case-insensitively,
    computes deterministic provenance hashes, dispatches to the specialized ingestor,
    and returns a standardized IngestionResult.

    Args:
        path: Path to the target source document or dataset.
        raise_on_unsupported: If True, raises UnsupportedFormatError instead of returning a FAILED result.
        raise_on_missing: If True, raises SourceFileNotFoundError instead of returning a FAILED result.

    Returns:
        IngestionResult encapsulating the SourceRecord, observations, status, and diagnostics.
    """
    target_path = Path(path)
    logger.info("Initiating ingestion dispatch for: %s", target_path)

    # 1. Verify filesystem existence and file type
    if not target_path.exists() or not target_path.is_file():
        err_msg = f"File not found or is not a regular file: {target_path}"
        logger.error(err_msg)
        if raise_on_missing:
            raise SourceFileNotFoundError(err_msg)

        # Build fallback failed SourceRecord
        norm_file = str(target_path).replace("\\", "/")
        fallback_source_id = generate_source_id(SourceType.TXT, norm_file)
        source_rec = SourceRecord(
            source_id=fallback_source_id,
            source_file=norm_file,
            source_type=SourceType.TXT,
            extraction_method=ExtractionMethod.TEXT_READ,
            ingested_at=datetime.now(timezone.utc),
            extraction_status=ExtractionStatus.FAILED,
            metadata={"error": "FileNotFound"},
        )
        return IngestionResult(
            source_record=source_rec,
            status=ExtractionStatus.FAILED,
            errors=[err_msg],
        )

    # 2. Resolve ingestor for the file format
    try:
        ingestor = get_ingestor_for_path(target_path)
    except UnsupportedFormatError as err:
        logger.error(str(err))
        if raise_on_unsupported:
            raise

        norm_file = str(target_path).replace("\\", "/")
        fallback_source_id = generate_source_id(SourceType.TXT, norm_file)
        source_rec = SourceRecord(
            source_id=fallback_source_id,
            source_file=norm_file,
            source_type=SourceType.TXT,
            extraction_method=ExtractionMethod.TEXT_READ,
            ingested_at=datetime.now(timezone.utc),
            extraction_status=ExtractionStatus.FAILED,
            metadata={"error": "UnsupportedFormat", "extension": target_path.suffix.lower()},
        )
        return IngestionResult(
            source_record=source_rec,
            status=ExtractionStatus.FAILED,
            errors=[str(err)],
        )

    # 3. Execute format-specific ingestion
    return ingestor.ingest(target_path)
