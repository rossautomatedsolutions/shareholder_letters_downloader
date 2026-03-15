import tempfile
import unittest
from pathlib import Path

try:
    import pandas as pd
except ModuleNotFoundError:  # pragma: no cover
    pd = None

from scripts.validate_and_clean_manifest import validate_and_clean_manifest


@unittest.skipIf(pd is None, "pandas is not installed")
class ManifestValidatorTests(unittest.TestCase):
    def test_validate_and_clean_manifest_removes_invalid_and_duplicate_rows(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            input_path = root / "letters_manifest.csv"
            clean_output = root / "letters_manifest.cleaned.csv"
            rejected_output = root / "rejected_manifest_rows.csv"

            pd.DataFrame(
                [
                    {
                        "company_id": "acme",
                        "company_name": "Acme",
                        "document_type": "shareholder_letter",
                        "year": "2024",
                        "source_type": "PDF",
                        "url": "https://example.com/acme-2024.pdf",
                    },
                    {
                        "company_id": "acme",
                        "company_name": "Acme",
                        "document_type": "shareholder_letter",
                        "year": "2024",
                        "source_type": "PDF",
                        "url": "https://example.com/acme-2024-duplicate.pdf",
                    },
                    {
                        "company_id": "beta",
                        "company_name": "Beta",
                        "document_type": "shareholder_letter",
                        "year": "abcd",
                        "source_type": "PDF",
                        "url": "https://example.com/beta.pdf",
                    },
                    {
                        "company_id": "gamma",
                        "company_name": "Gamma",
                        "document_type": "shareholder_letter",
                        "year": "2023",
                        "source_type": "DOC",
                        "url": "https://example.com/gamma.pdf",
                    },
                    {
                        "company_id": "delta",
                        "company_name": "Delta",
                        "document_type": "chairman_letter",
                        "year": "2023",
                        "source_type": "HTML",
                        "url": "https://example.com/delta.html",
                    },
                    {
                        "company_id": "epsilon",
                        "company_name": "Epsilon",
                        "document_type": "shareholder_letter",
                        "year": "2022",
                        "source_type": "PDF",
                        "url": "ftp://example.com/epsilon.pdf",
                    },
                    {
                        "company_id": "zeta",
                        "company_name": "Zeta",
                        "document_type": "shareholder_letter",
                        "year": "2021",
                        "source_type": "PDF",
                        "url": "https://example.com/earnings-presentation-2021.pdf",
                    },
                ]
            ).to_csv(input_path, index=False)

            summary = validate_and_clean_manifest(input_path, clean_output, rejected_output)

            cleaned = pd.read_csv(clean_output, dtype=str, keep_default_na=False)
            rejected = pd.read_csv(rejected_output, dtype=str, keep_default_na=False)

            self.assertEqual(summary["rows_scanned"], 7)
            self.assertEqual(summary["rows_accepted"], 1)
            self.assertEqual(summary["rows_rejected"], 6)
            self.assertEqual(summary["duplicate_rows_removed"], 1)

            self.assertEqual(len(cleaned), 1)
            self.assertEqual(cleaned.iloc[0]["company_id"], "acme")
            self.assertEqual(cleaned.iloc[0]["year"], "2024")

            reasons = set(rejected["rejection_reason"].tolist())
            self.assertIn("invalid_year", reasons)
            self.assertIn("invalid_source_type", reasons)
            self.assertIn("invalid_document_type", reasons)
            self.assertIn("invalid_url_scheme", reasons)
            self.assertIn("invalid_url_pattern", reasons)
            self.assertIn("duplicate_company_year", reasons)

    def test_missing_required_columns_raises_error(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            input_path = root / "letters_manifest.csv"

            pd.DataFrame(
                [
                    {
                        "company_id": "acme",
                        "company_name": "Acme",
                        "year": "2024",
                        "source_type": "PDF",
                        "url": "https://example.com/acme-2024.pdf",
                    }
                ]
            ).to_csv(input_path, index=False)

            with self.assertRaises(ValueError):
                validate_and_clean_manifest(input_path, root / "clean.csv", root / "rejected.csv")


if __name__ == "__main__":
    unittest.main()
