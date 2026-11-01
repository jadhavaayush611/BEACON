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
    ExtractionOutcome,
    ExtractionStatus,
    ObservationType,
    SourceType,
    generate_observation_id,
)

logger = logging.getLogger("beacon.ingestion.json")


class JsonIngestor(BaseIngestor):
    """Parses JSON files containing structured disaster observations or dataset manifests."""

    def __init__(self) -> None:
        super().__init__(SourceType.JSON, ExtractionMethod.JSON_PARSE)

    @staticmethod
    def _is_manifest_or_metadata(data: dict[str, Any]) -> bool:
        """Identify if a JSON dictionary represents a dataset manifest or metadata document."""
        # If the JSON contains an events/items list or top-level event_type, it's an observation container
        if "events" in data and isinstance(data["events"], list):
            return False
        if "items" in data and isinstance(data["items"], list):
            if data["items"] and isinstance(data["items"][0], dict) and ("event_type" in data["items"][0] or "EVENT" in data["items"][0]):
                return False
        if "event_type" in data or "EVENT" in data:
            return False

        manifest_keys = {
            "manifest_version",
            "dataset_name",
            "dataset_description",
            "dataset_title",
            "dataset_version",
            "samples",
            "file_inventory",
            "intended_demo",
            "expected_behaviors",
        }
        if any(k in data for k in manifest_keys):
            return True
        if "files" in data and isinstance(data["files"], list):
            return True
        if "disclaimer" in data and ("intended_demo" in data or "expected_behaviors" in data):
            return True
        return False

    def _extract_manifest_metadata(
        self,
        path: Path,
        data: dict[str, Any],
        content_hash: str,
        file_size: int,
    ) -> IngestionResult:
        """Extract structured dataset manifest and provenance metadata without fabricating observations."""
        dataset_name = data.get("dataset_name") or data.get("dataset_title") or data.get("title") or "BEACON Dataset Manifest"
        version = data.get("dataset_version") or data.get("manifest_version") or data.get("version") or "1.0.0"
        description = data.get("dataset_description") or data.get("description") or ""
        disclaimer = data.get("disclaimer") or ""
        intended_demo = data.get("intended_demo") or ""
        expected_behaviors = data.get("expected_behaviors") or []

        # Extract file inventory
        raw_files = data.get("files") or data.get("samples") or data.get("file_inventory") or []
        file_inventory: list[dict[str, Any]] = []
        if isinstance(raw_files, list):
            for entry in raw_files:
                if isinstance(entry, dict):
                    fn = entry.get("file") or entry.get("file_name") or entry.get("name") or "unknown"
                    fmt = entry.get("format") or entry.get("source_type") or Path(fn).suffix.lstrip(".")
                    purpose = entry.get("purpose") or entry.get("description") or ""
                    item_meta = {
                        "file_name": str(fn),
                        "format": str(fmt).upper(),
                        "purpose": str(purpose),
                    }
                    if "sample_records" in entry:
                        item_meta["sample_records_count"] = len(entry["sample_records"])
                    file_inventory.append(item_meta)

        doc_meta = {
            "document_type": "dataset_manifest",
            "dataset_name": dataset_name,
            "version": str(version),
            "description": description,
            "disclaimer": disclaimer,
            "intended_demo": intended_demo,
            "expected_behaviors": expected_behaviors,
            "referenced_files": [f["file_name"] for f in file_inventory if "file_name" in f],
            "file_inventory": file_inventory,
            "inventory_count": len(file_inventory),
        }

        source_rec = self.create_source_record(
            path=path,
            content_hash=content_hash,
            status=ExtractionStatus.SUCCESS,
            metadata={"file_size": file_size, **doc_meta},
        )

        explanation = (
            f"Dataset manifest analysed: '{dataset_name}' (v{version}) referencing "
            f"{len(file_inventory)} file(s). Formed 1 DATASET_METADATA observation record."
        )

        obs_id = generate_observation_id(
            source_id=source_rec.source_id,
            source_reference="manifest:root",
            raw_claim=json.dumps(doc_meta, sort_keys=True),
            index=1,
            observation_type=ObservationType.DATASET_METADATA.value,
        )

        manifest_obs = DisasterObservation(
            observation_id=obs_id,
            source_id=source_rec.source_id,
            observation_type=ObservationType.DATASET_METADATA,
            event_type=None,
            location=None,
            event_time=None,
            description=f"Dataset Manifest: {dataset_name} v{version} ({len(file_inventory)} files referenced)",
            severity=None,
            source_reference="manifest:root",
            raw_claim=json.dumps(doc_meta, indent=2),
            context_payload=doc_meta,
        )

        return IngestionResult(
            source_record=source_rec,
            observations=[manifest_obs],
            status=ExtractionStatus.SUCCESS,
            outcome=ExtractionOutcome.METADATA_EXTRACTED,
            document_metadata=doc_meta,
            explanation=explanation,
            errors=[],
            warnings=[],
            processed_count=1,
            accepted_count=1,
            rejected_count=0,
        )

    def ingest(self, path: Path) -> IngestionResult:
        content_hash, file_size = self.compute_sha256(path)
        logger.info("Ingesting JSON source '%s' (hash=%s, size=%d bytes)", path.name, content_hash[:8], file_size)

        try:
            with open(path, "r", encoding="utf-8") as f:
                raw_text = f.read()
            if not raw_text.strip():
                err_msg = f"JSON file '{path.name}' is empty or whitespace-only."
                logger.error("JSON empty error in '%s': %s", path.name, err_msg)
                source_rec = self.create_source_record(
                    path=path,
                    content_hash=content_hash,
                    status=ExtractionStatus.FAILED,
                    metadata={"file_size": file_size, "parse_error": "EmptyFileError"},
                )
                return IngestionResult(
                    source_record=source_rec,
                    status=ExtractionStatus.FAILED,
                    outcome=ExtractionOutcome.EXTRACTION_FAILED,
                    explanation=err_msg,
                    errors=[err_msg],
                    processed_count=0,
                    accepted_count=0,
                    rejected_count=0,
                )
            data = json.loads(raw_text)
        except UnicodeDecodeError as err:
            err_msg = f"Invalid character encoding in '{path.name}': {err}. Expected UTF-8."
            logger.error("JSON encoding error in '%s': %s", path.name, err_msg)
            source_rec = self.create_source_record(
                path=path,
                content_hash=content_hash,
                status=ExtractionStatus.FAILED,
                metadata={"file_size": file_size, "parse_error": "UnicodeDecodeError"},
            )
            return IngestionResult(
                source_record=source_rec,
                status=ExtractionStatus.FAILED,
                outcome=ExtractionOutcome.EXTRACTION_FAILED,
                explanation=err_msg,
                errors=[err_msg],
                processed_count=0,
                accepted_count=0,
                rejected_count=0,
            )
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
                outcome=ExtractionOutcome.EXTRACTION_FAILED,
                explanation=err_msg,
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
                outcome=ExtractionOutcome.EXTRACTION_FAILED,
                explanation=err_msg,
                errors=[err_msg],
            )

        # Check if payload is a dataset manifest or metadata-oriented document
        if isinstance(data, dict) and self._is_manifest_or_metadata(data):
            return self._extract_manifest_metadata(path, data, content_hash, file_size)

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
                outcome=ExtractionOutcome.EXTRACTION_FAILED,
                explanation=err_msg,
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

        # Determine extraction status and semantic outcome
        if processed_count == 0:
            status = ExtractionStatus.SUCCESS
            outcome = ExtractionOutcome.DOCUMENT_ANALYZED_NO_OBSERVATIONS
            explanation = "Empty JSON array processed. Zero observations declared."
        elif accepted_count > 0 and rejected_count == 0:
            status = ExtractionStatus.SUCCESS
            outcome = ExtractionOutcome.OBSERVATIONS_EXTRACTED
            explanation = f"Successfully extracted {accepted_count} canonical disaster observation(s)."
        elif accepted_count > 0 and rejected_count > 0:
            status = ExtractionStatus.PARTIAL
            outcome = ExtractionOutcome.OBSERVATIONS_EXTRACTED
            explanation = f"Partial extraction: {accepted_count} observation(s) accepted, {rejected_count} rejected."
            warnings.append(
                f"Partial ingestion: {accepted_count} of {processed_count} records accepted, "
                f"{rejected_count} rejected due to errors."
            )
        else:
            status = ExtractionStatus.FAILED
            outcome = ExtractionOutcome.EXTRACTION_FAILED
            explanation = f"All {processed_count} records failed validation."
            errors.append(f"All {processed_count} records failed validation.")

        source_rec.extraction_status = status
        source_rec.metadata["accepted_count"] = accepted_count
        source_rec.metadata["rejected_count"] = rejected_count

        return IngestionResult(
            source_record=source_rec,
            observations=observations,
            status=status,
            outcome=outcome,
            explanation=explanation,
            errors=errors,
            warnings=warnings,
            processed_count=processed_count,
            accepted_count=accepted_count,
            rejected_count=rejected_count,
        )
