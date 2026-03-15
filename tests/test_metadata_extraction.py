import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.extract_letter_metadata import run


class _FakePage:
    def extract_text(self):
        return "Shareholder Letter 2022"


class _FakeReader:
    def __init__(self, _path: str):
        self.pages = [_FakePage()]


class MetadataExtractionTests(unittest.TestCase):
    def test_run_writes_expected_metadata_fields(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output_root = Path(tmpdir) / "output"
            letter_dir = output_root / "acme_inc" / "shareholder_letter"
            letter_dir.mkdir(parents=True)

            pdf_path = letter_dir / "2022.pdf"
            pdf_path.write_bytes(b"%PDF-1.4\n")

            original_metadata = {
                "company_id": "acme_inc",
                "year": "2022",
                "url": "https://example.com/letter-2022.pdf",
                "fetched_at_utc": "2024-01-01T00:00:00+00:00",
            }
            pdf_path.with_suffix(".metadata.json").write_text(json.dumps(original_metadata), encoding="utf-8")

            with patch("scripts.extract_letter_metadata.resolve_pdf_reader", return_value=_FakeReader):
                processed = run(output_root=output_root, document_type="shareholder_letter")

            self.assertEqual(processed, 1)
            metadata = json.loads(pdf_path.with_suffix(".metadata.json").read_text(encoding="utf-8"))

            for field in [
                "company_id",
                "year",
                "page_count",
                "file_size",
                "url",
                "download_timestamp",
                "detected_year",
                "first_page_text_preview",
            ]:
                self.assertIn(field, metadata)

            self.assertEqual(metadata["company_id"], "acme_inc")
            self.assertEqual(metadata["year"], "2022")
            self.assertEqual(metadata["url"], "https://example.com/letter-2022.pdf")
            self.assertEqual(metadata["download_timestamp"], "2024-01-01T00:00:00+00:00")
            self.assertEqual(metadata["page_count"], 1)
            self.assertGreater(metadata["file_size"], 0)
            self.assertEqual(metadata["detected_year"], "2022")


if __name__ == "__main__":
    unittest.main()
