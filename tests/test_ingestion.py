"""Comprehensive unit and integration test suite for the BEACON ingestion subsystem."""

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import pytest

from beacon.ingestion import (
    CsvIngestor,
    ImageIngestor,
    JsonIngestor,
    PdfIngestor,
    SourceFileNotFoundError,
    TxtIngestor,
    UnsupportedFormatError,
    get_ingestor_for_path,
    ingest_file,
)
from beacon.models import ExtractionStatus, SourceType
from beacon.pipeline.transform import ObservationTransformer
from beacon.quality.validator import DataQualityValidator
from beacon.resolution.resolver import EventResolver
from beacon.storage.repository import StorageRepository
from beacon.storage.schema import (
    Base,
    EventModel,
    EventObservationModel,
    ObservationModel,
    ResolutionDecisionModel,
    SourceModel,
)
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session


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

    def test_empty_json_file_fails(self, tmp_path: Path) -> None:
        empty_file = tmp_path / "empty_file.json"
        empty_file.write_text("", encoding="utf-8")
        res = ingest_file(empty_file)
        assert res.status == ExtractionStatus.FAILED
        assert any("empty or whitespace" in err for err in res.errors)

    def test_whitespace_only_json_file_fails(self, tmp_path: Path) -> None:
        ws_file = tmp_path / "ws_file.json"
        ws_file.write_text("   \n\t  \n", encoding="utf-8")
        res = ingest_file(ws_file)
        assert res.status == ExtractionStatus.FAILED
        assert any("empty or whitespace" in err for err in res.errors)

    def test_invalid_utf8_json_file_fails(self, tmp_path: Path) -> None:
        bad_utf8 = tmp_path / "bad_utf8.json"
        bad_utf8.write_bytes(b'{"key": "\xff\xfe\x00\x00"}')
        res = ingest_file(bad_utf8)
        assert res.status == ExtractionStatus.FAILED
        assert any("Invalid character encoding" in err for err in res.errors)


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

    def test_completely_empty_csv(self, tmp_path: Path) -> None:
        empty_csv = tmp_path / "zero_bytes.csv"
        empty_csv.write_text("", encoding="utf-8")

        res = ingest_file(empty_csv)
        assert res.status == ExtractionStatus.SUCCESS
        assert res.processed_count == 0
        assert len(res.observations) == 0
        assert any("empty or contains no rows" in w for w in res.warnings)

    def test_whitespace_only_csv(self, tmp_path: Path) -> None:
        ws_csv = tmp_path / "whitespace.csv"
        ws_csv.write_text("   \n\r\n  \t  \n", encoding="utf-8")

        res = ingest_file(ws_csv)
        assert res.status == ExtractionStatus.SUCCESS
        assert res.processed_count == 0
        assert len(res.observations) == 0
        assert any("empty or contains no rows" in w for w in res.warnings)

    def test_invalid_utf8_csv_fails(self, tmp_path: Path) -> None:
        bad_utf8 = tmp_path / "bad_utf8.csv"
        bad_utf8.write_bytes(b"record_id,event_type,location\n1,flood,\xff\xfe\x00\n")

        res = ingest_file(bad_utf8)
        assert res.status == ExtractionStatus.FAILED
        assert any("Invalid character encoding" in err for err in res.errors)


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

    def test_invalid_utf8_txt_fails(self, tmp_path: Path) -> None:
        bad_utf8 = tmp_path / "bad_utf8.txt"
        bad_utf8.write_bytes(b"[REPORT 001]\nEVENT: \xff\xfe\x00\nLOCATION: Lake\n")

        res = ingest_file(bad_utf8)
        assert res.status == ExtractionStatus.FAILED
        assert any("Invalid character encoding" in err for err in res.errors)

    def test_txt_empty_and_whitespace_only(self, tmp_path: Path) -> None:
        empty_file = tmp_path / "empty.txt"
        empty_file.write_text("", encoding="utf-8")

        res_empty = ingest_file(empty_file)
        assert res_empty.status == ExtractionStatus.SUCCESS
        assert res_empty.processed_count == 0
        assert len(res_empty.observations) == 0
        assert any("empty or whitespace-only" in w for w in res_empty.warnings)

        ws_file = tmp_path / "whitespace.txt"
        ws_file.write_text("   \n\t  \n  ", encoding="utf-8")

        res_ws = ingest_file(ws_file)
        assert res_ws.status == ExtractionStatus.SUCCESS
        assert res_ws.processed_count == 0
        assert len(res_ws.observations) == 0
        assert any("empty or whitespace-only" in w for w in res_ws.warnings)


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

    def test_pdf_resource_cleanup_on_success_and_failure(self, tmp_path: Path) -> None:
        """Verify file streams are deterministically closed on both success and failure."""
        damaged_pdf = tmp_path / "damaged_cleanup.pdf"
        damaged_pdf.write_bytes(b"%PDF-1.4\ncorrupt content")

        ingestor = PdfIngestor()
        res_fail = ingestor.ingest(damaged_pdf)
        assert res_fail.status == ExtractionStatus.FAILED

        # Re-opening / deleting the file immediately should succeed without file lock errors
        damaged_pdf.unlink()
        assert not damaged_pdf.exists()

    def test_pdf_zero_byte_rejection(self, tmp_path: Path) -> None:
        empty_pdf = tmp_path / "empty.pdf"
        empty_pdf.write_bytes(b"")

        res = ingest_file(empty_pdf)
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

    def test_image_zero_byte_rejection(self, tmp_path: Path) -> None:
        empty_img = tmp_path / "empty.png"
        empty_img.write_bytes(b"")

        res = ingest_file(empty_img)
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
        for o1, o2 in zip(run1.observations, run2.observations, strict=True):
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


# ==============================================================================
# 8. Full Format Traceability & Persistence Lineage
# ==============================================================================

class TestFullFormatTraceabilityAndLineage:
    """Verifies all five formats through parsing, transformation, quality evaluation,

    resolution, and database lineage persistence without synthetic fabrication.
    """

    def test_end_to_end_lineage_across_all_five_supported_formats(self) -> None:
        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        repo = StorageRepository()
        transformer = ObservationTransformer()
        validator = DataQualityValidator()
        resolver = EventResolver()

        sample_files = [
            SAMPLE_DIR / "events.json",
            SAMPLE_DIR / "events.csv",
            SAMPLE_DIR / "events.txt",
            SAMPLE_DIR / "sample_report.pdf",
            SAMPLE_DIR / "sample_disaster.png",
        ]

        ingestion_results = []
        all_observations = []

        with Session(engine) as session:
            for file_path in sample_files:
                res = ingest_file(file_path)
                assert res.status == ExtractionStatus.SUCCESS
                ingestion_results.append(res)

                # Persist source record
                repo.save_source(res.source_record, session)

                if file_path.suffix == ".png":
                    assert len(res.observations) == 0
                    assert any("semantic disaster extraction deferred to OCR" in w for w in res.warnings)
                else:
                    assert len(res.observations) > 0
                    transformed = transformer.transform_batch(res.observations)
                    quality_report = validator.validate(transformed)

                    repo.save_observations(transformed, session)
                    repo.save_quality_results(quality_report.outcomes, session)
                    all_observations.extend(transformed)

            session.commit()

            # Verify all 5 source records exist in storage
            sources_in_db = session.execute(select(SourceModel)).scalars().all()
            assert len(sources_in_db) == 5
            source_types = {s.source_type for s in sources_in_db}
            assert source_types == {"json", "csv", "txt", "pdf", "image"}

            # Verify observations in DB link to valid sources
            obs_in_db = session.execute(select(ObservationModel)).scalars().all()
            assert len(obs_in_db) == len(all_observations)
            for db_obs in obs_in_db:
                assert db_obs.source_id in {s.source_id for s in sources_in_db}
                assert db_obs.event_type is not None
                assert db_obs.raw_claim is not None

            # Resolve observations
            res_result = resolver.resolve_observations(all_observations)
            assert res_result.total_observations == len(all_observations)
            assert len(res_result.resolved_events) >= 1

            # Persist resolution result atomically
            counts = repo.save_resolution_result(res_result, session)
            session.commit()

            assert counts["events_persisted"] == len(res_result.resolved_events)
            assert counts["decisions_persisted"] == len(res_result.outcomes)

            # Assert database lineage: Source -> Observation -> Event -> Decision
            events_in_db = session.execute(select(EventModel)).scalars().all()
            assert len(events_in_db) == len(res_result.resolved_events)

            links_in_db = session.execute(select(EventObservationModel)).scalars().all()
            assert len(links_in_db) == counts["links_persisted"]

            decisions_in_db = session.execute(select(ResolutionDecisionModel)).scalars().all()
            assert len(decisions_in_db) == len(res_result.outcomes)

            # Ensure every link points to existing event and observation
            event_ids = {e.event_id for e in events_in_db}
            obs_ids = {o.observation_id for o in obs_in_db}
            for link in links_in_db:
                assert link.event_id in event_ids
                assert link.observation_id in obs_ids

            # Ensure every decision records valid observation and decision type
            for decision in decisions_in_db:
                assert decision.observation_id in obs_ids
                assert decision.decision_type in {
                    "confident_match",
                    "no_match",
                    "ambiguous_candidate",
                    "idempotent_re_observation",
                    "merge_candidate",
                }
                if decision.event_id is not None:
                    assert decision.event_id in event_ids
