"""CSV format ingestor for tabular sensor telemetry and field reports."""

import csv
import logging
from pathlib import Path
from typing import Optional

from pydantic import ValidationError

from beacon.ingestion.base import BaseIngestor
from beacon.ingestion.result import IngestionResult
from beacon.models import (
    DisasterObservation,
    ExtractionMethod,
    ExtractionStatus,
    SourceType,
    generate_observation_id,
)

logger = logging.getLogger("beacon.ingestion.csv")


class CsvIngestor(BaseIngestor):
    """Parses CSV files containing tabular disaster incident records."""

    def __init__(self) -> None:
        super().__init__(SourceType.CSV, ExtractionMethod.CSV_PARSE)

    def ingest(self, path: Path) -> IngestionResult:
        content_hash, file_size = self.compute_sha256(path)
        logger.info("Ingesting CSV source '%s' (hash=%s, size=%d bytes)", path.name, content_hash[:8], file_size)

        try:
            with open(path, "r", encoding="utf-8-sig", newline="") as f:
                content = f.read()
            if not content.strip():
                source_rec = self.create_source_record(
                    path=path,
                    content_hash=content_hash,
                    status=ExtractionStatus.SUCCESS,
                    metadata={"file_size": file_size, "headers": [], "total_rows": 0},
                )
                return IngestionResult(
                    source_record=source_rec,
                    observations=[],
                    status=ExtractionStatus.SUCCESS,
                    errors=[],
                    warnings=[f"CSV file '{path.name}' is empty or contains no rows."],
                    processed_count=0,
                    accepted_count=0,
                    rejected_count=0,
                )
            reader = csv.DictReader(content.splitlines())
            headers = reader.fieldnames or []
            rows = list(reader)
        except UnicodeDecodeError as err:
            err_msg = f"Invalid character encoding in '{path.name}': {err}. Expected UTF-8."
            logger.error("CSV encoding error: %s", err_msg)
            source_rec = self.create_source_record(
                path=path,
                content_hash=content_hash,
                status=ExtractionStatus.FAILED,
                metadata={"file_size": file_size, "parse_error": "UnicodeDecodeError"},
            )
            return IngestionResult(
                source_record=source_rec,
                status=ExtractionStatus.FAILED,
                errors=[err_msg],
                processed_count=0,
                accepted_count=0,
                rejected_count=0,
            )
        except Exception as err:
            err_msg = f"Failed to read CSV file '{path.name}': {err}"
            logger.error("CSV read error: %s", err_msg)
            source_rec = self.create_source_record(
                path=path,
                content_hash=content_hash,
                status=ExtractionStatus.FAILED,
                metadata={"file_size": file_size, "read_error": str(err)},
            )
            return IngestionResult(
                source_record=source_rec,
                status=ExtractionStatus.FAILED,
                errors=[err_msg],
            )

        processed_count = len(rows)
        observations: list[DisasterObservation] = []
        errors: list[str] = []
        warnings: list[str] = []
        accepted_count = 0
        rejected_count = 0

        source_rec = self.create_source_record(
            path=path,
            content_hash=content_hash,
            metadata={"file_size": file_size, "headers": headers, "total_rows": processed_count},
        )

        for row_idx, row in enumerate(rows, start=2):  # Row 1 is header
            record_id = (row.get("record_id") or "").strip()
            source_ref = f"row_{row_idx}:{record_id}" if record_id else f"row_{row_idx}"

            # 1. event_type
            event_type = (row.get("event_type") or "").strip()
            if not event_type:
                rejected_count += 1
                errors.append(f"Row {row_idx} [{source_ref}] is missing required 'event_type'")
                continue

            # 2. location
            location = (row.get("location") or "").strip() or None

            # 3. event_time
            time_str = (row.get("event_time") or "").strip()
            parsed_time = None
            if time_str:
                try:
                    parsed_time = self.parse_datetime_to_utc(time_str)
                except Exception as e:
                    rejected_count += 1
                    errors.append(f"Row {row_idx} [{source_ref}] has invalid event_time '{time_str}': {e}")
                    continue

            # 4. coordinates
            lat_str = (row.get("latitude") or "").strip()
            parsed_lat: Optional[float] = None
            if lat_str:
                try:
                    parsed_lat = float(lat_str)
                except ValueError:
                    rejected_count += 1
                    errors.append(f"Row {row_idx} [{source_ref}] has non-numeric latitude '{lat_str}'")
                    continue

            lon_str = (row.get("longitude") or "").strip()
            parsed_lon: Optional[float] = None
            if lon_str:
                try:
                    parsed_lon = float(lon_str)
                except ValueError:
                    rejected_count += 1
                    errors.append(f"Row {row_idx} [{source_ref}] has non-numeric longitude '{lon_str}'")
                    continue

            # 5. severity & description
            severity = (row.get("severity") or "").strip() or None
            description = (row.get("description") or "").strip() or None

            # 6. confidence
            conf_str = (row.get("confidence") or row.get("extraction_confidence") or "").strip()
            parsed_conf: Optional[float] = None
            if conf_str:
                try:
                    parsed_conf = float(conf_str)
                except ValueError:
                    rejected_count += 1
                    errors.append(f"Row {row_idx} [{source_ref}] has non-numeric confidence '{conf_str}'")
                    continue

            # 7. page number
            page_str = (row.get("page_number") or "").strip()
            parsed_page: Optional[int] = None
            if page_str:
                try:
                    parsed_page = int(page_str)
                except ValueError:
                    rejected_count += 1
                    errors.append(f"Row {row_idx} [{source_ref}] has non-integer page_number '{page_str}'")
                    continue

            # 8. raw claim & observation id
            raw_claim = ", ".join(f"{k}={v}" for k, v in row.items() if v.strip())
            obs_id = generate_observation_id(
                source_id=source_rec.source_id,
                source_reference=source_ref,
                raw_claim=raw_claim,
                index=row_idx,
            )

            try:
                obs = DisasterObservation(
                    observation_id=obs_id,
                    source_id=source_rec.source_id,
                    event_type=event_type,
                    location=location,
                    event_time=parsed_time,
                    description=description,
                    severity=severity,
                    latitude=parsed_lat,
                    longitude=parsed_lon,
                    extraction_confidence=parsed_conf,
                    page_number=parsed_page,
                    source_reference=source_ref,
                    raw_claim=raw_claim,
                )
                observations.append(obs)
                accepted_count += 1
                if obs.validation_warnings:
                    warnings.extend([f"Row {row_idx}: {w}" for w in obs.validation_warnings])
            except ValidationError as val_err:
                rejected_count += 1
                errors.append(f"Validation failure in row {row_idx} [{source_ref}]: {val_err}")
            except Exception as err:
                rejected_count += 1
                errors.append(f"Unexpected error in row {row_idx} [{source_ref}]: {err}")

        # Determine extraction status
        if processed_count == 0:
            status = ExtractionStatus.SUCCESS
        elif accepted_count > 0 and rejected_count == 0:
            status = ExtractionStatus.SUCCESS
        elif accepted_count > 0 and rejected_count > 0:
            status = ExtractionStatus.PARTIAL
            warnings.append(
                f"Partial ingestion: {accepted_count} of {processed_count} rows accepted, "
                f"{rejected_count} rejected due to errors."
            )
        else:
            status = ExtractionStatus.FAILED
            errors.append(f"All {processed_count} CSV rows failed validation.")

        source_rec.extraction_status = status
        source_rec.metadata["accepted_count"] = accepted_count
        source_rec.metadata["rejected_count"] = rejected_count

        return IngestionResult(
            source_record=source_rec,
            observations=observations,
            status=status,
            errors=errors,
            warnings=warnings,
            processed_count=processed_count,
            accepted_count=accepted_count,
            rejected_count=rejected_count,
        )
