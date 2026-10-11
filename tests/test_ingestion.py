"""Comprehensive unit and integration test suite for the BEACON ingestion subsystem."""

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import struct
import zlib
import pytest

from beacon.ingestion import (
    CsvIngestor,
    ImageIngestor,
    IngestionResult,
    JsonIngestor,
    PdfIngestor,
    SourceFileNotFoundError,
    TxtIngestor,
    UnsupportedFormatError,
    get_ingestor_for_path,
    ingest_file,
)
from beacon.models import ExtractionStatus, SourceType


SAMPLE_DIR = Path("data/sample")


# ==============================================================================
# 1. Dispatcher & Routing Tests
# ==============================================================================

class TestDispatcher:
    """Tests for format resolution and dispatching."""

    @pytest.mark.parametrize(
        "ext, expected_cls",
        [
            (".json", JsonIngestor),
            (".csv", CsvIngestor),
            (".txt", TxtIngestor),
            (".pdf", PdfIngestor),
            (".png", ImageIngestor),
            (".jpg", ImageIngestor),
            (".jpeg", ImageIngestor),
            (".tiff", ImageIngestor),
            (".bmp", ImageIngestor),
            (".webp", ImageIngestor),
        ],
    )
    def test_dispatcher_supported_extensions_lowercase(self, ext: str, expected_cls: type) -> None:
        dummy_path = Path(f"simulated/input{ext}")
        ingestor = get_ingestor_for_path(dummy_path)
        assert isinstance(ingestor, expected_cls)

    @pytest.mark.parametrize(
        "ext, expected_cls",
        [
            (".JSON", JsonIngestor),
            (".CSV", CsvIngestor),
            (".TXT", TxtIngestor),
            (".PDF", PdfIngestor),
            (".PNG", ImageIngestor),
        ],
    )
    def test_dispatcher_supported_extensions_uppercase(self, ext: str, expected_cls: type) -> None:
        dummy_path = Path(f"simulated/INPUT{ext}")
        ingestor = get_ingestor_for_path(dummy_path)
        assert isinstance(ingestor, expected_cls)

    def test_dispatcher_unsupported_extension(self, tmp_path: Path) -> None:
        unsupported_file = tmp_path / "document.docx"
        unsupported_file.write_text("dummy docx content")

        # By default returns a FAILED result
        res = ingest_file(unsupported_file, raise_on_unsupported=False)
        assert res.status == ExtractionStatus.FAILED
        assert any("Unsupported file format" in err for err in res.errors)

        # When raise_on_unsupported=True, raises typed exception
        with pytest.raises(UnsupportedFormatError):
            ingest_file(unsupported_file, raise_on_unsupported=True)

    def test_dispatcher_missing_file(self) -> None:
        missing_path = Path("data/sample/non_existent_file.json")

        res = ingest_file(missing_path, raise_on_missing=False)
        assert res.status == ExtractionStatus.FAILED
        assert any("File not found" in err for err in res.errors)

        with pytest.raises(SourceFileNotFoundError):
            ingest_file(missing_path, raise_on_missing=True)


# ==============================================================================
# 2. JSON Ingestion Tests
# ==============================================================================

class TestJsonIngestion:
    """Tests for JSON telemetry ingestion."""

    def test_sample_events_json_ingestion(self) -> None:
        res = ingest_file(SAMPLE_DIR / "events.json")
        assert res.status == ExtractionStatus.SUCCESS
        assert res.processed_count == 3
        assert res.accepted_count == 3
        assert res.rejected_count == 0
        assert len(res.observations) == 3

        # Verify record 1 (full coordinates and time)
        obs1 = res.observations[0]
        assert obs1.event_type == "flood"
        assert obs1.location == "Riverside Metro, Sector 4"
        assert obs1.latitude == 28.6139
        assert obs1.longitude == 77.2090
        assert obs1.severity == "severe"
        assert obs1.extraction_confidence == 0.95
        assert obs1.event_time == datetime(2026, 10, 12, 8, 0, tzinfo=timezone.utc)

        # Verify record 2 (missing coordinates and severity preserved as None)
        obs2 = res.observations[1]
        assert obs2.event_type == "wildfire"
        assert obs2.latitude is None
        assert obs2.longitude is None
        assert obs2.severity is None

    def test_malformed_json_syntax(self, tmp_path: Path) -> None:
        bad_json = tmp_path / "corrupt.json"
        bad_json.write_text("[{broken_json: true, missing_quotes}]", encoding="utf-8")

        res = ingest_file(bad_json)
        assert res.status == ExtractionStatus.FAILED
        assert len(res.observations) == 0
        assert any("Malformed JSON syntax" in err for err in res.errors)

    def test_json_partial_success(self, tmp_path: Path) -> None:
        mixed_json = tmp_path / "mixed.json"
        payload = [
            {
                "record_id": "REC-VALID",
                "event_type": "flood",
                "location": "Safe Zone",
                "latitude": 28.0,
                "longitude": 77.0,
            },
            {
                "record_id": "REC-INVALID-COORDS",
                "event_type": "flood",
                "latitude": 999.0,  # Invalid latitude > 90
                "longitude": 77.0,
            },
        ]
        mixed_json.write_text(json.dumps(payload), encoding="utf-8")

        res = ingest_file(mixed_json)
        assert res.status == ExtractionStatus.PARTIAL
        assert res.processed_count == 2
        assert res.accepted_count == 1
        assert res.rejected_count == 1
        assert len(res.observations) == 1
        assert res.observations[0].location == "Safe Zone"
        assert any("Latitude must be between" in err for err in res.errors)

    def test_empty_json_array(self, tmp_path: Path) -> None:
        empty_json = tmp_path / "empty.json"
        empty_json.write_text("[]", encoding="utf-8")

        res = ingest_file(empty_json)
        assert res.status == ExtractionStatus.SUCCESS
        assert res.processed_count == 0
        assert len(res.observations) == 0


# ==============================================================================
# 3. CSV Ingestion Tests
# ==============================================================================

class TestCsvIngestion:
    """Tests for CSV sensor log ingestion."""

    def test_sample_events_csv_ingestion(self) -> None:
        res = ingest_file(SAMPLE_DIR / "events.csv")
        assert res.status == ExtractionStatus.SUCCESS
        assert res.processed_count == 3
        assert res.accepted_count == 3
        assert res.rejected_count == 0
        assert len(res.observations) == 3

        # Row 1: missing coordinates
        obs1 = res.observations[0]
        assert obs1.event_type == "flood"
        assert obs1.latitude is None
        assert obs1.longitude is None
        assert obs1.severity == "moderate"
        assert obs1.event_time == datetime(2026, 10, 12, 8, 30, tzinfo=timezone.utc)

        # Row 2: missing event_time
        obs2 = res.observations[1]
        assert obs2.event_type == "earthquake"
        assert obs2.event_time is None
        assert obs2.latitude == 34.0522
        assert obs2.longitude == -118.2437

    def test_csv_partial_success(self, tmp_path: Path) -> None:
        mixed_csv = tmp_path / "mixed.csv"
        csv_content = (
            "record_id,event_type,location,latitude,longitude,confidence\n"
            "ROW-1,flood,District A,25.0,80.0,0.9\n"
            "ROW-2,flood,District B,25.0,80.0,2.5\n"  # Invalid confidence > 1.0
            "ROW-3,,District C,25.0,80.0,0.8\n"       # Missing required event_type
        )
        mixed_csv.write_text(csv_content, encoding="utf-8")

        res = ingest_file(mixed_csv)
        assert res.status == ExtractionStatus.PARTIAL
        assert res.processed_count == 3
        assert res.accepted_count == 1
        assert res.rejected_count == 2
        assert len(res.observations) == 1
        assert res.observations[0].location == "District A"

    def test_empty_csv_with_headers(self, tmp_path: Path) -> None:
        empty_csv = tmp_path / "empty.csv"
        empty_csv.write_text("record_id,event_type,location\n", encoding="utf-8")

        res = ingest_file(empty_csv)
        assert res.status == ExtractionStatus.SUCCESS
        assert res.processed_count == 0
        assert len(res.observations) == 0


# ==============================================================================
# 4. TXT Ingestion Tests
# ==============================================================================

class TestTxtIngestion:
    """Tests for structured text situation report ingestion."""

    def test_sample_events_txt_ingestion(self) -> None:
        res = ingest_file(SAMPLE_DIR / "events.txt")
        assert res.status == ExtractionStatus.SUCCESS
        assert res.processed_count == 3
        assert res.accepted_count == 3
        assert res.rejected_count == 0
        assert len(res.observations) == 3

        # Report 1
        obs1 = res.observations[0]
        assert obs1.event_type == "flood"  # Normalized from "Flash Flood"
        assert "Riverside Metro" in obs1.location
        assert obs1.severity == "severe"
        assert obs1.event_time == datetime(2026, 10, 12, 8, 15, tzinfo=timezone.utc)

        # Report 2
        obs2 = res.observations[1]
        assert obs2.event_type == "industrial_accident"
        assert obs2.severity is None  # "Unknown" converted to None

        # Report 3: unstated timestamp
        obs3 = res.observations[2]
        assert obs3.event_type == "wildfire"
        assert obs3.event_time is None

    def test_txt_malformed_record(self, tmp_path: Path) -> None:
        txt_file = tmp_path / "mixed.txt"
        content = (
            "[REPORT 001]\n"
            "EVENT: Flood\n"
            "LOCATION: North Basin\n"
            "\n"
            "[REPORT 002]\n"
            "LOCATION: Missing Event\n"  # Missing EVENT field
        )
        txt_file.write_text(content, encoding="utf-8")

        res = ingest_file(txt_file)
        assert res.status == ExtractionStatus.PARTIAL
        assert res.processed_count == 2
        assert res.accepted_count == 1
        assert res.rejected_count == 1
        assert len(res.observations) == 1
        assert res.observations[0].location == "North Basin"

    def test_txt_without_report_blocks(self, tmp_path: Path) -> None:
        unstructured_file = tmp_path / "random_notes.txt"
        unstructured_file.write_text("Just some arbitrary meeting notes from team sync.", encoding="utf-8")

        res = ingest_file(unstructured_file)
        assert res.status == ExtractionStatus.SUCCESS
        assert res.processed_count == 0
        assert len(res.observations) == 0
        assert any("No structured [REPORT ...] incident blocks" in w for w in res.warnings)


# ==============================================================================
# 5. PDF Ingestion Tests
# ==============================================================================

class TestPdfIngestion:
    """Tests for PDF text extraction and multi-page handling."""

    def test_sample_report_pdf_ingestion(self) -> None:
        res = ingest_file(SAMPLE_DIR / "sample_report.pdf")
        assert res.status == ExtractionStatus.SUCCESS
        assert res.processed_count == 1
        assert res.accepted_count == 1
        assert len(res.observations) == 1

        obs = res.observations[0]
        assert obs.event_type == "flood"
        assert obs.location == "Riverside Metro, Sector 4 Environs"
        assert obs.latitude == 28.6142
        assert obs.longitude == 77.2095
        assert obs.severity == "severe"
        assert obs.page_number == 1
        assert obs.event_time == datetime(2026, 10, 12, 9, 0, tzinfo=timezone.utc)

    def test_multipage_pdf_ingestion(self, tmp_path: Path) -> None:
        """Create a synthetic 2-page PDF and verify extraction across pages."""
        multipage_pdf = tmp_path / "two_page.pdf"

        page1_content = (
            b"BT\n/F1 12 Tf\n50 720 Td\n"
            b"(Incident ID: SIM-MULTI-1) Tj\n0 -20 Td\n"
            b"(Event Category: Wildfire) Tj\n0 -20 Td\n"
            b"(Location: Ridge Peak) Tj\n0 -20 Td\n"
            b"(Severity: Moderate) Tj\nET\n"
        )
        page2_content = (
            b"BT\n/F1 12 Tf\n50 720 Td\n"
            b"(Incident ID: SIM-MULTI-2) Tj\n0 -20 Td\n"
            b"(Event Category: Flood) Tj\n0 -20 Td\n"
            b"(Location: Lower Basin) Tj\n0 -20 Td\n"
            b"(Severity: Severe) Tj\nET\n"
        )

        objects = [
            b"1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
            b"2 0 obj\n<< /Type /Pages /Kids [3 0 R 4 0 R] /Count 2 >>\nendobj\n",
            # Page 1
            b"3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 5 0 R /Resources << /Font << /F1 7 0 R >> >> >>\nendobj\n",
            # Page 2
            b"4 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 6 0 R /Resources << /Font << /F1 7 0 R >> >> >>\nendobj\n",
            # Stream 1
            b"5 0 obj\n<< /Length " + str(len(page1_content)).encode("ascii") + b" >>\nstream\n" + page1_content + b"endstream\nendobj\n",
            # Stream 2
            b"6 0 obj\n<< /Length " + str(len(page2_content)).encode("ascii") + b" >>\nstream\n" + page2_content + b"endstream\nendobj\n",
            # Font
            b"7 0 obj\n<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>\nendobj\n",
        ]

        header = b"%PDF-1.4\n"
        body = b""
        offsets = [0]
        curr_offset = len(header)
        for obj in objects:
            offsets.append(curr_offset)
            body += obj
            curr_offset += len(obj)

        xref_offset = len(header) + len(body)
        xref = b"xref\n0 " + str(len(objects) + 1).encode("ascii") + b"\n0000000000 65535 f \n"
        for off in offsets[1:]:
            xref += f"{off:010d} 00000 n \n".encode("ascii")
        trailer = (
            b"trailer\n<< /Size " + str(len(objects) + 1).encode("ascii") + b" /Root 1 0 R >>\n"
            b"startxref\n" + str(xref_offset).encode("ascii") + b"\n%%EOF\n"
        )

        multipage_pdf.write_bytes(header + body + xref + trailer)

        res = ingest_file(multipage_pdf)
        assert res.status == ExtractionStatus.SUCCESS
        assert len(res.observations) == 2
        assert res.observations[0].page_number == 1
        assert res.observations[0].event_type == "wildfire"
        assert res.observations[1].page_number == 2
        assert res.observations[1].event_type == "flood"

    def test_scanned_or_blank_pdf_no_false_ocr(self, tmp_path: Path) -> None:
        """A PDF with blank content stream should not extract false observations."""
        blank_pdf = tmp_path / "blank.pdf"
        empty_stream = b""
        objects = [
            b"1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
            b"2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n",
            b"3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R >>\nendobj\n",
            b"4 0 obj\n<< /Length 0 >>\nstream\nendstream\nendobj\n",
        ]
        header = b"%PDF-1.4\n"
        body = b"".join(objects)
        trailer = b"\nxref\n0 5\n0000000000 65535 f \ntrailer\n<< /Size 5 /Root 1 0 R >>\nstartxref\n100\n%%EOF\n"
        blank_pdf.write_bytes(header + body + trailer)

        res = ingest_file(blank_pdf)
        assert res.status == ExtractionStatus.SUCCESS
        assert len(res.observations) == 0
        assert any("no machine-readable text" in w or "zero extractable text" in w for w in res.warnings)

    def test_corrupt_pdf_failure(self, tmp_path: Path) -> None:
        corrupt_pdf = tmp_path / "damaged.pdf"
        corrupt_pdf.write_bytes(b"%PDF-1.4\nThis is completely corrupt binary data not conforming to PDF.")

        res = ingest_file(corrupt_pdf)
        assert res.status == ExtractionStatus.FAILED
        assert any("Failed to open or parse PDF" in err for err in res.errors)


# ==============================================================================
# 6. Image Ingestion Tests
# ==============================================================================

class TestImageIngestion:
    """Tests for image container validation and OCR deferral."""

    def test_sample_disaster_png_ingestion(self) -> None:
        res = ingest_file(SAMPLE_DIR / "sample_disaster.png")
        assert res.status == ExtractionStatus.SUCCESS
        assert len(res.observations) == 0  # No OCR inference in Phase 2
        assert res.source_record.source_type == SourceType.IMAGE
        assert res.source_record.metadata["width"] == 240
        assert res.source_record.metadata["height"] == 160
        assert res.source_record.metadata["image_format"] == "PNG"
        assert any("semantic disaster extraction deferred to OCR" in w for w in res.warnings)

    def test_corrupt_image_rejection(self, tmp_path: Path) -> None:
        corrupt_img = tmp_path / "corrupted.png"
        corrupt_img.write_bytes(b"\x89PNG\r\n\x1a\nNot a real PNG chunk sequence.")

        res = ingest_file(corrupt_img)
        assert res.status == ExtractionStatus.FAILED
        assert any("not a recognized or supported image format" in err or "Corrupt or unreadable" in err for err in res.errors)


# ==============================================================================
# 7. Provenance, Hashing, and Security Tests
# ==============================================================================

class TestProvenanceAndIntegrity:
    """Tests for SHA-256 calculation, deterministic IDs, and security hygiene."""

    def test_sha256_exact_hash_match(self) -> None:
        target = SAMPLE_DIR / "events.json"
        expected_hash = hashlib.sha256(target.read_bytes()).hexdigest()

        res = ingest_file(target)
        assert res.source_record.content_hash == expected_hash

    def test_stable_identifier_rerun_consistency(self) -> None:
        target = SAMPLE_DIR / "events.csv"
        run1 = ingest_file(target)
        run2 = ingest_file(target)

        assert run1.source_record.source_id == run2.source_record.source_id
        assert len(run1.observations) == len(run2.observations)
        for o1, o2 in zip(run1.observations, run2.observations):
            assert o1.observation_id == o2.observation_id
            assert o1.source_id == run1.source_record.source_id

    def test_observation_foreign_key_to_source_record(self) -> None:
        for sample_name in ["events.json", "events.csv", "events.txt", "sample_report.pdf"]:
            res = ingest_file(SAMPLE_DIR / sample_name)
            for obs in res.observations:
                assert obs.source_id == res.source_record.source_id

    def test_utc_aware_ingestion_timestamp(self) -> None:
        res = ingest_file(SAMPLE_DIR / "events.json")
        ingested_at = res.source_record.ingested_at
        assert ingested_at.tzinfo is not None
        assert ingested_at.tzinfo == timezone.utc

    def test_no_credential_leak_in_metadata_or_logs(self) -> None:
        res = ingest_file(SAMPLE_DIR / "events.json")
        for k in res.source_record.metadata.keys():
            k_lower = k.lower()
            assert "password" not in k_lower
            assert "token" not in k_lower
            assert "secret" not in k_lower
            assert "api_key" not in k_lower
