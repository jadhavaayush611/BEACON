"""Deterministic generator for synthetic sample PDF and PNG files.

Generates standard-compliant PDF and PNG files for BEACON Phase 1 sample data
using pure Python standard library (no binary dependencies required).

Usage:
    python scripts/generate_sample_binaries.py
"""

from pathlib import Path
import struct
import zlib


def generate_synthetic_png(output_path: Path, width: int = 240, height: int = 160) -> None:
    """Generate a valid PNG image representing a simulated drone/satellite flood map."""
    signature = b"\x89PNG\r\n\x1a\n"

    # IHDR chunk
    ihdr_data = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)
    ihdr_crc = struct.pack(">I", zlib.crc32(b"IHDR" + ihdr_data))
    ihdr = struct.pack(">I", len(ihdr_data)) + b"IHDR" + ihdr_data + ihdr_crc

    # Raw scanlines with RGB pixels:
    # Background: dark slate river basin
    # Inundation zone: highlighted flood polygon (cyan/blue)
    # Warning marker: high-hazard centroid (orange/red)
    raw_scanlines = bytearray()
    for y in range(height):
        raw_scanlines.append(0)  # Filter type 0: None
        for x in range(width):
            if 50 <= y <= 110 and 60 <= x <= 180:
                if 70 <= y <= 90 and 100 <= x <= 140:
                    # Orange/red critical marker
                    raw_scanlines.extend([230, 75, 45])
                else:
                    # Cyan/blue simulated flood plume
                    raw_scanlines.extend([40, 140, 220])
            elif y < 25 or y > height - 25:
                # Dark perimeter boundary
                raw_scanlines.extend([45, 50, 58])
            else:
                # Land terrain (earth tone)
                raw_scanlines.extend([110, 125, 95])

    compressed = zlib.compress(bytes(raw_scanlines), level=9)
    idat_crc = struct.pack(">I", zlib.crc32(b"IDAT" + compressed))
    idat = struct.pack(">I", len(compressed)) + b"IDAT" + compressed + idat_crc

    # IEND chunk
    iend_crc = struct.pack(">I", zlib.crc32(b"IEND"))
    iend = struct.pack(">I", 0) + b"IEND" + iend_crc

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(signature + ihdr + idat + iend)


def generate_synthetic_pdf(output_path: Path) -> None:
    """Generate a standard PDF-1.4 situation report with extractable text."""
    content_stream = (
        b"BT\n"
        b"/F1 18 Tf\n"
        b"50 720 Td\n"
        b"(BEACON SYNTHETIC SITUATION REPORT) Tj\n"
        b"/F1 10 Tf\n"
        b"0 -26 Td\n"
        b"(DISCLAIMER: SYNTHETIC EXERCISE DATA - NOT A REAL INCIDENT) Tj\n"
        b"0 -24 Td\n"
        b"(Incident ID: PDF-SIM-301) Tj\n"
        b"0 -18 Td\n"
        b"(Event Category: Flood / Riverine Inundation) Tj\n"
        b"0 -18 Td\n"
        b"(Timestamp: 2026-10-12T09:00:00Z) Tj\n"
        b"0 -18 Td\n"
        b"(Location: Riverside Metro, Sector 4 Environs) Tj\n"
        b"0 -18 Td\n"
        b"(Estimated Coordinates: Lat 28.6142, Lon 77.2095) Tj\n"
        b"0 -18 Td\n"
        b"(Severity: Severe) Tj\n"
        b"0 -28 Td\n"
        b"(Incident Narrative:) Tj\n"
        b"0 -18 Td\n"
        b"(Aerial reconnaissance confirms severe floodwater inundation along Sector 4.) Tj\n"
        b"0 -18 Td\n"
        b"(River stage sensors indicate water levels have crested at 1.45 meters.) Tj\n"
        b"0 -18 Td\n"
        b"(Field teams report embankment breach expanding along eastern retaining wall.) Tj\n"
        b"0 -24 Td\n"
        b"(Cross-Reference: Co-referent to telemetry in events.json (JSON-SIM-001)) Tj\n"
        b"0 -18 Td\n"
        b"(and field log events.csv (CSV-SIM-101).) Tj\n"
        b"ET\n"
    )

    objects = [
        # Obj 1: Catalog
        b"1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        # Obj 2: Pages
        b"2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n",
        # Obj 3: Page
        b"3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>\nendobj\n",
        # Obj 4: Content stream
        b"4 0 obj\n<< /Length "
        + str(len(content_stream)).encode("ascii")
        + b" >>\nstream\n"
        + content_stream
        + b"endstream\nendobj\n",
        # Obj 5: Font descriptor
        b"5 0 obj\n<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>\nendobj\n",
    ]

    header = b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n"
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
        b"trailer\n<< /Size "
        + str(len(objects) + 1).encode("ascii")
        + b" /Root 1 0 R >>\n"
        + b"startxref\n"
        + str(xref_offset).encode("ascii")
        + b"\n%%EOF\n"
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(header + body + xref + trailer)


def main() -> None:
    sample_dir = Path("data/sample")
    pdf_target = sample_dir / "sample_report.pdf"
    png_target = sample_dir / "sample_disaster.png"

    print(f"Generating synthetic PDF at {pdf_target}...")
    generate_synthetic_pdf(pdf_target)
    print(f"PDF generated ({pdf_target.stat().st_size} bytes).")

    print(f"Generating synthetic PNG at {png_target}...")
    generate_synthetic_png(png_target)
    print(f"PNG generated ({png_target.stat().st_size} bytes).")


if __name__ == "__main__":
    main()
