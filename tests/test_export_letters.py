import unittest
import json
import tempfile
from unittest.mock import patch

from export_letters import ManifestValidationError, normalized_pdf_path, process_rows, validate_manifest
from pathlib import Path


class ExportLettersTests(unittest.TestCase):
    def setUp(self):
        self.base_row = {
            "company_id": "acme_inc",
            "company_name": "Acme Inc",
            "document_type": "chairman_letter",
            "year": "2023",
            "source_type": "PDF",
            "url": "https://example.com/2023.pdf",
        }

    def test_validate_manifest_accepts_valid_rows(self):
        validate_manifest([self.base_row])

    def test_validate_manifest_rejects_duplicate_composite_key(self):
        rows = [self.base_row, dict(self.base_row)]
        with self.assertRaises(ManifestValidationError):
            validate_manifest(rows)

    def test_validate_manifest_rejects_non_numeric_year(self):
        row = dict(self.base_row, year="20X3")
        with self.assertRaises(ManifestValidationError):
            validate_manifest([row])

    def test_normalized_output_path_uses_company_doc_and_year(self):
        path = normalized_pdf_path(Path("output"), self.base_row)
        self.assertEqual(path.as_posix(), "output/acme_inc/chairman_letter/2023.pdf")

    def test_existing_files_are_skipped(self):
        with tempfile.TemporaryDirectory() as tmp:
            output_root = Path(tmp) / "output"
            reports_dir = Path(tmp) / "reports"
            normalized_path = normalized_pdf_path(output_root, self.base_row)
            normalized_path.parent.mkdir(parents=True, exist_ok=True)
            normalized_path.write_bytes(b"existing-pdf")

            with patch("export_letters.fetch_binary") as mock_fetch_binary:
                _, json_report = process_rows(
                    rows=[self.base_row],
                    output_root=output_root,
                    reports_dir=reports_dir,
                    render_overrides={},
                    retries=0,
                    timeout_seconds=1,
                    force_redownload=False,
                )

            mock_fetch_binary.assert_not_called()
            self.assertEqual(normalized_path.read_bytes(), b"existing-pdf")
            rows = json.loads(json_report.read_text(encoding="utf-8"))
            self.assertEqual(rows[0]["status"], "SKIPPED_EXISTING")

    def test_force_redownload_overwrites_existing_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            output_root = Path(tmp) / "output"
            reports_dir = Path(tmp) / "reports"
            normalized_path = normalized_pdf_path(output_root, self.base_row)
            normalized_path.parent.mkdir(parents=True, exist_ok=True)
            normalized_path.write_bytes(b"old-content")

            def fake_fetch_binary(_url, dest, _timeout_seconds, _retries):
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_bytes(b"new-content")

            with patch("export_letters.fetch_binary", side_effect=fake_fetch_binary) as mock_fetch_binary:
                _, json_report = process_rows(
                    rows=[self.base_row],
                    output_root=output_root,
                    reports_dir=reports_dir,
                    render_overrides={},
                    retries=0,
                    timeout_seconds=1,
                    force_redownload=True,
                )

            mock_fetch_binary.assert_called_once()
            self.assertEqual(normalized_path.read_bytes(), b"new-content")
            rows = json.loads(json_report.read_text(encoding="utf-8"))
            self.assertEqual(rows[0]["status"], "success")


if __name__ == "__main__":
    unittest.main()
