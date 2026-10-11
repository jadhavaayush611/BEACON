"""PDF format ingestor using pypdf for structured situational reports."""

import logging
from pathlib import Path
import re
from typing import Optional

from pydantic import ValidationError
import pypdf

from beacon.ingestion.base import BaseIngestor
from beacon.ingestion.result import IngestionResult
from beacon.models import (
    DisasterObservation,
    ExtractionMethod,
    ExtractionStatus,
    SourceType,
    generate_observation_id,
)

logger = logging.getLogger("beacon.ingestion.pdf")

PDF_EVENT_MAP: dict[str, str] = {
    "flood": "flood",
    "riverine inundation": "flood",
    "flood / riverine inundation": "flood",
    "flash flood": "flood",
    "wildfire": "wildfire",
    "earthquake": "earthquake",
    "cyclone": "cyclone",
    "hurricane": "hurricane",
    "storm": "storm",
}


class PdfIngestor(BaseIngestor):
    """Extracts text from multi-page PDF documents and parses structured incident reports."""

    def __init__(self) -> None:
        super().__init__(SourceType.PDF, ExtractionMethod.PDF_TEXT)

    def ingest(self, path: Path) -> IngestionResult:
        content_hash, file_size = self.compute_sha256(path)
        logger.info("Ingesting PDF source '%s' (hash=%s, size=%d bytes)", path.name, content_hash[:8], file_size)

        try:
            reader = pypdf.PdfReader(str(path))
            total_pages = len(reader.pages)
        except Exception as err:
            err_msg = f"Failed to open or parse PDF '{path.name}': {err}"
            logger.error("PDF read error: %s", err_msg)
            source_rec = self.create_source_record(
                path=path,
                content_hash=content_hash,
                status=ExtractionStatus.FAILED,
                metadata={"file_size": file_size, "pdf_error": str(err)},
            )
            return IngestionResult(
                source_record=source_rec,
                status=ExtractionStatus.FAILED,
                errors=[err_msg],
            )

        observations: list[DisasterObservation] = []
        errors: list[str] = []
        warnings: list[str] = []
        processed_count = 0
        accepted_count = 0
        rejected_count = 0
        pages_with_text = 0

        source_rec = self.create_source_record(
            path=path,
            content_hash=content_hash,
            metadata={"file_size": file_size, "total_pages": total_pages},
        )

        for page_idx, page in enumerate(reader.pages):
            page_num = page_idx + 1
            try:
                page_text = page.extract_text() or ""
            except Exception as page_err:
                errors.append(f"Page {page_num}: Text extraction failed: {page_err}")
                continue

            cleaned_text = page_text.strip()
            if not cleaned_text:
                warnings.append(
                    f"Page {page_num}: Contains no machine-readable text (page may be blank or scanned). "
                    "OCR is required to extract visual text (Phase 2 boundary)."
                )
                continue

            pages_with_text += 1
            # Attempt to parse structured report on this page
            parsed_data = self._parse_page_report(cleaned_text, page_num)
            if not parsed_data:
                warnings.append(f"Page {page_num}: Text present but does not match recognized situation report headers.")
                continue

            processed_count += 1
            incident_id = parsed_data.get("incident_id") or f"page_{page_num}"
            source_ref = f"page_{page_num}:{incident_id}"

            # 1. Event Type
            raw_event = parsed_data.get("event_type", "").strip()
            if not raw_event:
                rejected_count += 1
                errors.append(f"Page {page_num} [{source_ref}]: Missing required event category")
                continue

            event_type = PDF_EVENT_MAP.get(raw_event.lower(), raw_event)

            # 2. Timestamp
            raw_time = parsed_data.get("timestamp")
            parsed_time = None
            if raw_time:
                try:
                    parsed_time = self.parse_datetime_to_utc(raw_time)
                except Exception as e:
                    rejected_count += 1
                    errors.append(f"Page {page_num} [{source_ref}]: Invalid timestamp '{raw_time}': {e}")
                    continue

            # 3. Coordinates
            lat = parsed_data.get("latitude")
            lon = parsed_data.get("longitude")

            # 4. Severity & Narrative
            severity = parsed_data.get("severity")
            narrative = parsed_data.get("narrative")

            obs_id = generate_observation_id(
                source_id=source_rec.source_id,
                source_reference=source_ref,
                raw_claim=cleaned_text,
                index=page_num,
            )

            try:
                obs = DisasterObservation(
                    observation_id=obs_id,
                    source_id=source_rec.source_id,
                    event_type=event_type,
                    location=parsed_data.get("location"),
                    event_time=parsed_time,
                    description=narrative,
                    severity=severity,
                    latitude=lat,
                    longitude=lon,
                    extraction_confidence=0.90,
                    page_number=page_num,
                    source_reference=source_ref,
                    raw_claim=cleaned_text,
                )
                observations.append(obs)
                accepted_count += 1
                if obs.validation_warnings:
                    warnings.extend([f"Page {page_num}: {w}" for w in obs.validation_warnings])
            except ValidationError as val_err:
                rejected_count += 1
                errors.append(f"Page {page_num} [{source_ref}]: Validation failure: {val_err}")
            except Exception as err:
                rejected_count += 1
                errors.append(f"Page {page_num} [{source_ref}]: Parsing error: {err}")

        # Check document-level conditions
        if total_pages > 0 and pages_with_text == 0:
            warnings.append(
                "PDF contains zero extractable text across all pages. "
                "Scanned document OCR is not performed in Phase 2."
            )
            status = ExtractionStatus.SUCCESS
        elif processed_count == 0:
            status = ExtractionStatus.SUCCESS
        elif accepted_count > 0 and rejected_count == 0:
            status = ExtractionStatus.SUCCESS
        elif accepted_count > 0 and rejected_count > 0:
            status = ExtractionStatus.PARTIAL
            warnings.append(
                f"Partial ingestion: {accepted_count} of {processed_count} pages accepted, "
                f"{rejected_count} rejected due to errors."
            )
        else:
            status = ExtractionStatus.FAILED
            errors.append(f"All {processed_count} page incident reports failed validation.")

        source_rec.extraction_status = status
        source_rec.metadata["pages_with_text"] = pages_with_text
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

    def _parse_page_report(self, text: str, page_num: int) -> Optional[dict]:
        """Extract structured key-value incident fields from a PDF page text."""
        # Key patterns based on sample_report.pdf
        incident_id_m = re.search(r"Incident ID:\s*([^\n\r]+)", text, re.IGNORECASE)
        event_cat_m = re.search(r"Event (?:Category|Type):\s*([^\n\r]+)", text, re.IGNORECASE)
        time_m = re.search(r"Timestamp:\s*([^\n\r]+)", text, re.IGNORECASE)
        loc_m = re.search(r"Location:\s*([^\n\r]+)", text, re.IGNORECASE)
        coords_m = re.search(r"(?:Estimated )?Coordinates:\s*Lat\s*([-\d.]+),\s*Lon\s*([-\d.]+)", text, re.IGNORECASE)
        sev_m = re.search(r"Severity:\s*([^\n\r]+)", text, re.IGNORECASE)
        narrative_m = re.search(r"(?:Incident Narrative|Field Summary):\s*(.+?)(?=(?:Cross-Reference|$))", text, re.DOTALL | re.IGNORECASE)

        if not (incident_id_m or event_cat_m or loc_m):
            return None

        lat: Optional[float] = None
        lon: Optional[float] = None
        if coords_m:
            try:
                lat = float(coords_m.group(1))
                lon = float(coords_m.group(2))
            except ValueError:
                pass

        return {
            "incident_id": incident_id_m.group(1).strip() if incident_id_m else None,
            "event_type": event_cat_m.group(1).strip() if event_cat_m else "",
            "timestamp": time_m.group(1).strip() if time_m else None,
            "location": loc_m.group(1).strip() if loc_m else None,
            "latitude": lat,
            "longitude": lon,
            "severity": sev_m.group(1).strip().lower() if sev_m else None,
            "narrative": " ".join(narrative_m.group(1).split()) if narrative_m else None,
        }
