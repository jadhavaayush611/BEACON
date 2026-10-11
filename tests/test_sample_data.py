"""Tests verifying integrity, synthetic nature, and co-references of sample datasets."""

import csv
import json
from pathlib import Path
import pytest


SAMPLE_DIR = Path("data/sample")


class TestSampleDataIntegrity:
    """Tests ensuring sample data files exist, are well-formed, and contain co-referent records."""

    @pytest.mark.parametrize(
        "filename",
        [
            "events.json",
            "events.csv",
            "events.txt",
            "sample_report.pdf",
            "sample_disaster.png",
            "manifest.json",
        ],
    )
    def test_sample_files_exist_and_non_empty(self, filename: str) -> None:
        target = SAMPLE_DIR / filename
        assert target.exists(), f"Sample file {filename} does not exist in {SAMPLE_DIR}"
        assert target.is_file(), f"{filename} is not a regular file"
        assert target.stat().st_size > 0, f"{filename} is unexpectedly empty (0 bytes)"

    def test_json_structure_and_disclaimer(self) -> None:
        json_file = SAMPLE_DIR / "events.json"
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        assert isinstance(data, list)
        assert len(data) >= 3

        # Verify synthetic disclaimer and required keys
        for item in data:
            assert "SYNTHETIC" in item.get("disclaimer", "")
            assert "event_type" in item
            assert "location" in item

        # Verify missing optional fields on at least one record
        has_missing_coords = any(item.get("latitude") is None for item in data)
        assert has_missing_coords, "Expected at least one JSON record with missing coordinates"

    def test_csv_structure_and_headers(self) -> None:
        csv_file = SAMPLE_DIR / "events.csv"
        with open(csv_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames or []
            rows = list(reader)

        expected_headers = {"record_id", "event_type", "location", "event_time", "severity", "disclaimer"}
        assert expected_headers.issubset(set(headers))
        assert len(rows) >= 3

        # Verify synthetic disclaimer in CSV rows
        for row in rows:
            assert "SYNTHETIC" in row["disclaimer"]

        # Verify missing fields in CSV (empty strings)
        has_empty_time = any(not row.get("event_time") for row in rows)
        has_empty_coords = any(not row.get("latitude") for row in rows)
        assert has_empty_time, "Expected at least one CSV row with missing event_time"
        assert has_empty_coords, "Expected at least one CSV row with missing coordinates"

    def test_txt_structure_and_disclaimer(self) -> None:
        txt_file = SAMPLE_DIR / "events.txt"
        content = txt_file.read_text(encoding="utf-8")

        assert "SYNTHETIC" in content
        assert "DISCLAIMER" in content
        assert "[REPORT" in content
        assert "Riverside Metro" in content

    def test_pdf_binary_validity(self) -> None:
        pdf_file = SAMPLE_DIR / "sample_report.pdf"
        data = pdf_file.read_bytes()

        assert data.startswith(b"%PDF-1.4"), "PDF must start with valid PDF-1.4 header"
        assert b"%%EOF" in data, "PDF must contain standard %%EOF trailer marker"
        assert b"SYNTHETIC" in data, "PDF must embed synthetic exercise disclaimer"

    def test_png_binary_validity(self) -> None:
        png_file = SAMPLE_DIR / "sample_disaster.png"
        data = png_file.read_bytes()

        # PNG 8-byte signature
        png_sig = b"\x89PNG\r\n\x1a\n"
        assert data.startswith(png_sig), "PNG must start with standard 8-byte signature"
        assert b"IHDR" in data, "PNG must contain IHDR chunk"
        assert b"IEND" in data, "PNG must contain IEND chunk"

    def test_manifest_covers_all_five_input_formats(self) -> None:
        manifest_file = SAMPLE_DIR / "manifest.json"
        with open(manifest_file, "r", encoding="utf-8") as f:
            manifest = json.load(f)

        assert "samples" in manifest
        formats = {sample["format"] for sample in manifest["samples"]}
        expected_formats = {"json", "csv", "txt", "pdf", "png"}
        assert expected_formats.issubset(formats)

    def test_cross_source_coreference_plausibility(self) -> None:
        """Verify that at least two records across different formats plausibly refer to the same event."""
        # 1. JSON record
        with open(SAMPLE_DIR / "events.json", "r", encoding="utf-8") as f:
            json_data = json.load(f)
        json_sector4 = next(
            (item for item in json_data if "Sector 4" in item.get("location", "")),
            None,
        )
        assert json_sector4 is not None
        assert json_sector4["event_type"] == "flood"

        # 2. CSV record
        with open(SAMPLE_DIR / "events.csv", "r", encoding="utf-8") as f:
            csv_rows = list(csv.DictReader(f))
        csv_sector4 = next(
            (row for row in csv_rows if "Sector 4" in row.get("description", "")),
            None,
        )
        assert csv_sector4 is not None
        assert csv_sector4["event_type"] == "flood"

        # 3. TXT record
        txt_content = (SAMPLE_DIR / "events.txt").read_text(encoding="utf-8")
        assert "Sector 4" in txt_content
        assert "Flash Flood" in txt_content or "urban flood" in txt_content.lower()

        # 4. PDF report
        pdf_raw = (SAMPLE_DIR / "sample_report.pdf").read_bytes()
        assert b"Sector 4" in pdf_raw
        assert b"Flood" in pdf_raw
