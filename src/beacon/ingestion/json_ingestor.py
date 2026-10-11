"""JSON format ingestor for structured telemetry and event streams."""

import json
import logging
from pathlib import Path
from typing import Any

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

logger = logging.getLogger("beacon.ingestion.json")


class JsonIngestor(BaseIngestor):
    """Parses JSON files containing structured disaster observations."""

    def __init__(self) -> None:
        super().__init__(SourceType.JSON, ExtractionMethod.JSON_PARSE)

    def ingest(self, path: Path) -> IngestionResult:
        content_hash, file_size = self.compute_sha256(path)
        logger.info("Ingesting JSON source '%s' (hash=%s, size=%d bytes)", path.name, content_hash[:8], file_size)

        try:
            with open(path, "r", encoding="utf-8") as f:
                raw_text = f.read()
                data = json.loads(raw_text)
        except json.JSONDecodeError as err:
            err_msg = f"Malformed JSON syntax at line {err.lineno}, column {err.colno}: {err.msg}"
            logger.error("JSON decode error in '%s': %s", path.name, err_msg)
            source_rec = self.create_source_record(
                path=path,
                content_hash=content_hash,
                status=ExtractionStatus.FAILED,
                metadata={"file_size": file_size, "parse_error": "JSONDecodeError"},
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
            err_msg = f"Failed to read JSON file '{path.name}': {err}"
            logger.error("File read error: %s", err_msg)
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

        # Normalize data into list of records
        records_to_process: list[tuple[int, Any]] = []
        if isinstance(data, list):
            records_to_process = list(enumerate(data))
        elif isinstance(data, dict):
            # Single object or container with items key
            if "items" in data and isinstance(data["items"], list):
                records_to_process = list(enumerate(data["items"]))
            elif "events" in data and isinstance(data["events"], list):
                records_to_process = list(enumerate(data["events"]))
            else:
                records_to_process = [(0, data)]
        else:
            err_msg = f"Top-level JSON payload must be an array or object, got {type(data).__name__}"
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

        observations: list[DisasterObservation] = []
        errors: list[str] = []
        warnings: list[str] = []
        processed_count = len(records_to_process)
        accepted_count = 0
        rejected_count = 0

        # We will create source record once status is known, but generate preliminary source_id
        source_rec = self.create_source_record(
            path=path,
            content_hash=content_hash,
            metadata={"file_size": file_size, "total_records": processed_count},
        )

        for idx, item in records_to_process:
            if not isinstance(item, dict):
                rejected_count += 1
                errors.append(f"Record index [{idx}] is not a JSON object: {type(item).__name__}")
                continue

            ref = item.get("source_reference") or f"items[{idx}]"
            record_id = item.get("record_id")

            # Parse event_time safely
            raw_event_time = item.get("event_time")
            parsed_time = None
            if raw_event_time is not None:
                try:
                    parsed_time = self.parse_datetime_to_utc(raw_event_time)
                except Exception as e:
                    rejected_count += 1
                    errors.append(f"Record [{ref}] has invalid event_time '{raw_event_time}': {e}")
                    continue

            # Build observation payload
            raw_claim_str = json.dumps(item, separators=(",", ":"))
            obs_id = generate_observation_id(
                source_id=source_rec.source_id,
                source_reference=str(record_id or ref),
                raw_claim=raw_claim_str,
                index=idx,
            )

            # Map confidence key
            confidence_val = item.get("confidence")
            if confidence_val is None:
                confidence_val = item.get("extraction_confidence")

            try:
                obs = DisasterObservation(
                    observation_id=obs_id,
                    source_id=source_rec.source_id,
                    event_type=item.get("event_type", ""),
                    location=item.get("location"),
                    event_time=parsed_time,
                    description=item.get("description"),
                    severity=item.get("severity"),
                    latitude=item.get("latitude"),
                    longitude=item.get("longitude"),
                    extraction_confidence=confidence_val,
                    page_number=item.get("page_number"),
                    source_reference=str(ref),
                    raw_claim=raw_claim_str,
                )
                observations.append(obs)
                accepted_count += 1
                if obs.validation_warnings:
                    warnings.extend([f"Record [{ref}]: {w}" for w in obs.validation_warnings])
            except ValidationError as val_err:
                rejected_count += 1
                errors.append(f"Validation failure in record [{ref}]: {val_err}")
            except Exception as err:
                rejected_count += 1
                errors.append(f"Unexpected parsing error in record [{ref}]: {err}")

        # Determine extraction status
        if processed_count == 0:
            status = ExtractionStatus.SUCCESS
        elif accepted_count > 0 and rejected_count == 0:
            status = ExtractionStatus.SUCCESS
        elif accepted_count > 0 and rejected_count > 0:
            status = ExtractionStatus.PARTIAL
            warnings.append(
                f"Partial ingestion: {accepted_count} of {processed_count} records accepted, "
                f"{rejected_count} rejected due to errors."
            )
        else:
            status = ExtractionStatus.FAILED
            errors.append(f"All {processed_count} records failed validation.")

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
