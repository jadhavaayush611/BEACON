"""BEACON ingestion subsystem public interface."""

from beacon.ingestion.base import BaseIngestor
from beacon.ingestion.csv_ingestor import CsvIngestor
from beacon.ingestion.dispatcher import (
    EXTENSION_INGESTOR_MAP,
    get_ingestor_for_path,
    ingest_file,
)
from beacon.ingestion.exceptions import (
    CorruptFileError,
    IngestionError,
    RecordParsingError,
    SourceFileNotFoundError,
    UnsupportedFormatError,
)
from beacon.ingestion.image_ingestor import ImageIngestor
from beacon.ingestion.json_ingestor import JsonIngestor
from beacon.ingestion.pdf_ingestor import PdfIngestor
from beacon.ingestion.result import IngestionResult
from beacon.ingestion.txt_ingestor import TxtIngestor

__all__ = [
    "ingest_file",
    "get_ingestor_for_path",
    "EXTENSION_INGESTOR_MAP",
    "IngestionResult",
    "BaseIngestor",
    "JsonIngestor",
    "CsvIngestor",
    "TxtIngestor",
    "PdfIngestor",
    "ImageIngestor",
    "IngestionError",
    "SourceFileNotFoundError",
    "UnsupportedFormatError",
    "CorruptFileError",
    "RecordParsingError",
]
