import unittest

from export_letters import ManifestValidationError, normalized_pdf_path, validate_manifest
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


if __name__ == "__main__":
    unittest.main()
