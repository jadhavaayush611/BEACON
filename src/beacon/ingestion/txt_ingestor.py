"""TXT format ingestor for structured disaster situation report feeds."""

import logging
from pathlib import Path
import re
from typing import Optional

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

logger = logging.getLogger("beacon.ingestion.txt")

# Canonical aliases mapping situational text events to standard categories
TXT_EVENT_MAP: dict[str, str] = {
    "flash flood": "flood",
    "riverine flood": "flood",
    "urban flood": "flood",
    "brushfire": "wildfire",
    "forest fire": "wildfire",
    "quake": "earthquake",
    "tremor": "earthquake",
    "industrial accident": "industrial_accident",
    "chemical drill": "industrial_accident",
}

KNOWN_TXT_FIELDS: set[str] = {
    "TIMESTAMP",
    "LOCATION",
    "EVENT",
    "SEVERITY",
    "NARRATIVE",
    "SOURCE_ID",
    "SOURCE_NAME",
    "LATITUDE",
    "LONGITUDE",
}

TXT_FIELD_ALIASES: dict[str, str] = {
    "EVENT_TYPE": "EVENT",
    "RECORD_ID": "SOURCE_ID",
    "INCIDENT_ID": "SOURCE_ID",
    "DESCRIPTION": "NARRATIVE",
    "FIELD_SUMMARY": "NARRATIVE",
    "TIME": "TIMESTAMP",
    "EVENT_TIME": "TIMESTAMP",
    "LAT": "LATITUDE",
    "LON": "LONGITUDE",
    "SOURCE_NAME": "SOURCE_NAME",
}


class TxtIngestor(BaseIngestor):
    """Parses situation report text feeds or supporting human-readable documents."""

    def __init__(self) -> None:
        super().__init__(SourceType.TXT, ExtractionMethod.TEXT_READ)

    def _extract_document_context(
        self, path: Path, content: str, content_hash: str, file_size: int
    ) -> IngestionResult:
        """Analyze human-readable documentation without fabricating incident observations."""
        lines = [line.strip() for line in content.splitlines()]
        non_empty_lines = [ln for ln in lines if ln]

        if not non_empty_lines:
            doc_meta = {"document_type": "empty_text", "file_size": file_size}
            source_rec = self.create_source_record(
                path=path,
                content_hash=content_hash,
                status=ExtractionStatus.SUCCESS,
                metadata=doc_meta,
            )
            return IngestionResult(
                source_record=source_rec,
                observations=[],
                status=ExtractionStatus.SUCCESS,
                outcome=ExtractionOutcome.DOCUMENT_ANALYZED_NO_OBSERVATIONS,
                document_metadata=doc_meta,
                explanation=f"Text file '{path.name}' is empty or whitespace-only.",
                warnings=[f"TXT file '{path.name}' is empty or whitespace-only."],
            )

        doc_title = non_empty_lines[0]

        # Extract section headings
        sections: list[str] = []
        for i, line in enumerate(non_empty_lines):
            if i + 1 < len(non_empty_lines) and re.match(r"^[=\-~]{3,}$", non_empty_lines[i + 1]):
                sections.append(line)
            elif line.endswith(":") and len(line) < 60 and not line.startswith("-") and not line.startswith("*"):
                sections.append(line[:-1])

        # Extract file references (e.g., events.json, events.csv, sample_report.pdf)
        file_pattern = re.compile(
            r"\b([A-Za-z0-9_\-]+\.(?:json|csv|txt|pdf|png|jpe?g|bmp|webp))\b",
            re.IGNORECASE,
        )
        referenced_files = sorted(list(set(file_pattern.findall(content))))

        disclaimer = None
        for line in non_empty_lines:
            if any(kw in line.upper() for kw in ["DISCLAIMER:", "IMPORTANT:", "NOTICE:", "SYNTHETIC"]):
                disclaimer = line
                break

        doc_meta = {
            "document_type": "supporting_documentation",
            "document_title": doc_title,
            "sections": sections,
            "referenced_files": referenced_files,
            "disclaimer": disclaimer,
            "line_count": len(lines),
            "char_count": len(content),
            "file_size": file_size,
        }

        source_rec = self.create_source_record(
            path=path,
            content_hash=content_hash,
            status=ExtractionStatus.SUCCESS,
            metadata=doc_meta,
        )

        explanation = (
            f"Supporting documentation analysed: '{doc_title}' ({len(sections)} section(s), "
            f"{len(referenced_files)} referenced file(s)). Formed 1 DOCUMENT_CONTEXT observation record."
        )

        obs_id = generate_observation_id(
            source_id=source_rec.source_id,
            source_reference="document:context",
            raw_claim=content[:2000],
            index=1,
            observation_type=ObservationType.DOCUMENT_CONTEXT.value,
        )

        doc_obs = DisasterObservation(
            observation_id=obs_id,
            source_id=source_rec.source_id,
            observation_type=ObservationType.DOCUMENT_CONTEXT,
            event_type=None,
            location=None,
            event_time=None,
            description=f"Documentation Context: {doc_title} ({len(sections)} sections, {len(referenced_files)} files referenced)",
            severity=None,
            source_reference="document:context",
            raw_claim=content[:2000],
            context_payload=doc_meta,
        )

        return IngestionResult(
            source_record=source_rec,
            observations=[doc_obs],
            status=ExtractionStatus.SUCCESS,
            outcome=ExtractionOutcome.DOCUMENT_ANALYZED_NO_OBSERVATIONS,
            document_metadata=doc_meta,
            explanation=explanation,
            errors=[],
            warnings=[f"No structured [REPORT ...] incident blocks found in '{path.name}'. Formed 1 DOCUMENT_CONTEXT observation."],
            processed_count=1,
            accepted_count=1,
            rejected_count=0,
        )

    def ingest(self, path: Path) -> IngestionResult:
        content_hash, file_size = self.compute_sha256(path)
        logger.info("Ingesting TXT source '%s' (hash=%s, size=%d bytes)", path.name, content_hash[:8], file_size)

        try:
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()
        except UnicodeDecodeError as err:
            err_msg = f"Invalid character encoding in '{path.name}': {err}. Expected UTF-8."
            logger.error("TXT encoding error: %s", err_msg)
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
        except Exception as err:
            err_msg = f"Failed to read TXT file '{path.name}': {err}"
            logger.error("TXT read error: %s", err_msg)
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

        blocks = self._extract_report_blocks(content)
        if not blocks:
            # Human-readable documentation or empty text
            return self._extract_document_context(path, content, content_hash, file_size)

        processed_count = len(blocks)
        observations: list[DisasterObservation] = []
        errors: list[str] = []
        warnings: list[str] = []
        accepted_count = 0
        rejected_count = 0

        source_rec = self.create_source_record(
            path=path,
            content_hash=content_hash,
            metadata={"file_size": file_size, "total_blocks": processed_count},
        )

        for idx, (report_tag, block_dict, raw_block) in enumerate(blocks, start=1):
            source_id_field = block_dict.get("SOURCE_ID") or report_tag
            source_ref = f"{report_tag}:{source_id_field}"

            # Check for unknown fields in block
            for k in block_dict.keys():
                if k not in KNOWN_TXT_FIELDS:
                    warnings.append(f"Block [{report_tag}] contains unrecognized field '{k}'")

            # 1. Event Type
            raw_event = block_dict.get("EVENT", "").strip()
            if not raw_event:
                rejected_count += 1
                errors.append(f"Block [{report_tag}] is missing required 'EVENT' field")
                continue

            event_type = TXT_EVENT_MAP.get(raw_event.lower(), raw_event)

            # 2. Timestamp
            raw_time = block_dict.get("TIMESTAMP", "").strip()
            parsed_time = None
            if raw_time:
                try:
                    parsed_time = self.parse_datetime_to_utc(raw_time)
                except Exception as e:
                    rejected_count += 1
                    errors.append(f"Block [{report_tag}] has invalid TIMESTAMP '{raw_time}': {e}")
                    continue

            # 3. Location
            location = block_dict.get("LOCATION", "").strip() or None

            # 4. Severity
            raw_sev = block_dict.get("SEVERITY", "").strip()
            severity: Optional[str] = None
            if raw_sev:
                if raw_sev.lower() == "unknown":
                    severity = None
                else:
                    severity = raw_sev.lower()

            # 5. Narrative / Description
            narrative = block_dict.get("NARRATIVE", "").strip() or None

            # 6. Optional Coordinates
            parsed_lat: Optional[float] = None
            parsed_lon: Optional[float] = None
            if "LATITUDE" in block_dict:
                try:
                    parsed_lat = float(block_dict["LATITUDE"])
                except ValueError:
                    warnings.append(f"Block [{report_tag}]: invalid LATITUDE '{block_dict['LATITUDE']}'")
            if "LONGITUDE" in block_dict:
                try:
                    parsed_lon = float(block_dict["LONGITUDE"])
                except ValueError:
                    warnings.append(f"Block [{report_tag}]: invalid LONGITUDE '{block_dict['LONGITUDE']}'")

            # Generate stable observation ID
            obs_id = generate_observation_id(
                source_id=source_rec.source_id,
                source_reference=source_ref,
                raw_claim=raw_block.strip(),
                index=idx,
            )

            try:
                obs = DisasterObservation(
                    observation_id=obs_id,
                    source_id=source_rec.source_id,
                    event_type=event_type,
                    location=location,
                    event_time=parsed_time,
                    description=narrative,
                    severity=severity,
                    latitude=parsed_lat,
                    longitude=parsed_lon,
                    source_reference=source_ref,
                    raw_claim=raw_block.strip(),
                )
                observations.append(obs)
                accepted_count += 1
                if obs.validation_warnings:
                    warnings.extend([f"Block [{report_tag}]: {w}" for w in obs.validation_warnings])
            except ValidationError as val_err:
                rejected_count += 1
                errors.append(f"Validation failure in block [{report_tag}]: {val_err}")
            except Exception as err:
                rejected_count += 1
                errors.append(f"Unexpected error in block [{report_tag}]: {err}")

        # Determine extraction status
        if processed_count == 0:
            status = ExtractionStatus.SUCCESS
            outcome = ExtractionOutcome.DOCUMENT_ANALYZED_NO_OBSERVATIONS
            explanation = "No incident report blocks identified in text file."
        elif accepted_count > 0 and rejected_count == 0:
            status = ExtractionStatus.SUCCESS
            outcome = ExtractionOutcome.OBSERVATIONS_EXTRACTED
            explanation = f"Successfully extracted {accepted_count} canonical disaster observation(s) from {processed_count} report block(s)."
        elif accepted_count > 0 and rejected_count > 0:
            status = ExtractionStatus.PARTIAL
            outcome = ExtractionOutcome.OBSERVATIONS_EXTRACTED
            explanation = f"Partial extraction: {accepted_count} of {processed_count} report block(s) accepted, {rejected_count} rejected."
            warnings.append(
                f"Partial ingestion: {accepted_count} of {processed_count} blocks accepted, "
                f"{rejected_count} rejected due to errors."
            )
        else:
            status = ExtractionStatus.FAILED
            outcome = ExtractionOutcome.EXTRACTION_FAILED
            explanation = f"All {processed_count} text blocks failed validation."
            errors.append(f"All {processed_count} text blocks failed validation.")

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

    def _extract_report_blocks(self, content: str) -> list[tuple[str, dict[str, str], str]]:
        """Split text content into [REPORT ...] blocks and parse field key-values."""
        block_pattern = re.compile(r"(?m)^\[REPORT(?:\s+([^\]]+))?\]\s*$", re.IGNORECASE)
        matches = list(block_pattern.finditer(content))
        if not matches:
            return []

        parsed_blocks: list[tuple[str, dict[str, str], str]] = []
        for i, match in enumerate(matches):
            raw_tag = match.group(1)
            tag = raw_tag.strip() if raw_tag else f"REPORT_{i + 1}"
            start = match.end()
            end = matches[i + 1].start() if i + 1 < len(matches) else len(content)
            raw_block = content[match.start():end]
            block_body = content[start:end]

            field_dict: dict[str, str] = {}
            current_field: Optional[str] = None
            field_regex = re.compile(r"^([A-Za-z_]+):\s*(.*)$")

            for line in block_body.splitlines():
                line_str = line.strip()
                if not line_str:
                    continue
                match_field = field_regex.match(line_str)
                if match_field:
                    raw_field = match_field.group(1).upper()
                    current_field = TXT_FIELD_ALIASES.get(raw_field, raw_field)
                    field_dict[current_field] = match_field.group(2).strip()
                elif current_field:
                    # Multi-line continuation (e.g. narrative paragraph)
                    field_dict[current_field] += " " + line_str

            parsed_blocks.append((tag, field_dict, raw_block))

        return parsed_blocks
