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
                        "confidence_score": "1.0",
                    },
                    {
                        "company_id": "acme",
                        "company_name": "Acme",
                        "document_type": "shareholder_letter",
                        "year": "2024",
                        "source_type": "PDF",
                        "url": "https://example.com/acme-2024-duplicate.pdf",
                        "confidence_score": "1.0",
                    },
                    {
                        "company_id": "beta",
                        "company_name": "Beta",
                        "document_type": "shareholder_letter",
                        "year": "abcd",
                        "source_type": "PDF",
                        "url": "https://example.com/beta.pdf",
                        "confidence_score": "1.0",
                    },
                    {
                        "company_id": "gamma",
                        "company_name": "Gamma",
                        "document_type": "shareholder_letter",
                        "year": "2023",
                        "source_type": "DOC",
                        "url": "https://example.com/gamma.pdf",
                        "confidence_score": "1.0",
                    },
                    {
                        "company_id": "delta",
                        "company_name": "Delta",
                        "document_type": "chairman_letter",
                        "year": "2023",
                        "source_type": "HTML",
                        "url": "https://example.com/delta.html",
                        "confidence_score": "1.0",
                    },
                    {
                        "company_id": "epsilon",
                        "company_name": "Epsilon",
                        "document_type": "shareholder_letter",
                        "year": "2022",
                        "source_type": "PDF",
                        "url": "ftp://example.com/epsilon.pdf",
                        "confidence_score": "1.0",
                    },
                    {
                        "company_id": "zeta",
                        "company_name": "Zeta",
                        "document_type": "shareholder_letter",
                        "year": "2021",
                        "source_type": "PDF",
                        "url": "https://example.com/earnings-presentation-2021.pdf",
                        "confidence_score": "0.3",
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


    def test_validate_and_clean_manifest_normalizes_accepted_values(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            input_path = root / "letters_manifest.csv"
            clean_output = root / "letters_manifest.cleaned.csv"
            rejected_output = root / "rejected_manifest_rows.csv"

            pd.DataFrame(
                [
                    {
                        "company_id": "acme ",
                        "company_name": " Acme",
                        "document_type": "shareholder_letter ",
                        "year": "2024 ",
                        "source_type": "PDF ",
                        "url": " https://example.com/acme-2024.pdf ",
                        "confidence_score": " 0.8 ",
                    }
                ]
            ).to_csv(input_path, index=False)

            summary = validate_and_clean_manifest(input_path, clean_output, rejected_output)

            cleaned = pd.read_csv(clean_output, dtype=str, keep_default_na=False)

            self.assertEqual(summary["rows_accepted"], 1)
            self.assertEqual(cleaned.iloc[0]["company_id"], "acme")
            self.assertEqual(cleaned.iloc[0]["company_name"], "Acme")
            self.assertEqual(cleaned.iloc[0]["document_type"], "shareholder_letter")
            self.assertEqual(cleaned.iloc[0]["year"], "2024")
            self.assertEqual(cleaned.iloc[0]["source_type"], "PDF")
            self.assertEqual(cleaned.iloc[0]["url"], "https://example.com/acme-2024.pdf")
            self.assertEqual(cleaned.iloc[0]["confidence_score"], "0.8")

    def test_validate_and_clean_manifest_rejects_malformed_http_urls(self):
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
                        "url": "https:///acme-2024.pdf",
                        "confidence_score": "0.9",
                    },
                    {
                        "company_id": "beta",
                        "company_name": "Beta",
                        "document_type": "shareholder_letter",
                        "year": "2024",
                        "source_type": "PDF",
                        "url": "http:example.com/beta-2024.pdf",
                        "confidence_score": "0.9",
                    },
                ]
            ).to_csv(input_path, index=False)

            summary = validate_and_clean_manifest(input_path, clean_output, rejected_output)

            rejected = pd.read_csv(rejected_output, dtype=str, keep_default_na=False)

            self.assertEqual(summary["rows_accepted"], 0)
            self.assertEqual(summary["rows_rejected"], 2)
            self.assertTrue((rejected["rejection_reason"] == "invalid_url_scheme").all())


    def test_validate_and_clean_manifest_rejects_low_confidence_rows(self):
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
                        "url": "https://example.com/investor-doc.pdf",
                        "confidence_score": "0.3",
                    }
                ]
            ).to_csv(input_path, index=False)

            summary = validate_and_clean_manifest(input_path, clean_output, rejected_output)
            rejected = pd.read_csv(rejected_output, dtype=str, keep_default_na=False)

            self.assertEqual(summary["rows_accepted"], 0)
            self.assertEqual(summary["rows_rejected"], 1)
            self.assertEqual(rejected.iloc[0]["rejection_reason"], "low_confidence_score")

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
                        "confidence_score": "1.0",
                    }
                ]
            ).to_csv(input_path, index=False)

            with self.assertRaises(ValueError):
                validate_and_clean_manifest(input_path, root / "clean.csv", root / "rejected.csv")


if __name__ == "__main__":
    unittest.main()
