import tempfile
import unittest
from pathlib import Path

try:
    import pandas as pd
except ModuleNotFoundError:  # pragma: no cover
    pd = None

from scripts.merge_manifests import merge_manifests


@unittest.skipIf(pd is None, "pandas is not installed")
class MergeManifestTests(unittest.TestCase):
    def test_merge_deduplicates_by_company_and_year_using_first_valid_url(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            auto_path = root / "letters_manifest.auto.csv"
            sec_path = root / "letters_manifest.sec.csv"
            manual_path = root / "letters_manifest.manual.csv"
            output_path = root / "letters_manifest.csv"

            pd.DataFrame(
                [
                    {
                        "company_id": "acme",
                        "company_name": "Acme",
                        "document_type": "shareholder_letter",
                        "year": "2024",
                        "source_type": "PDF",
                        "url": "not-a-valid-url",
                    },
                    {
                        "company_id": "acme",
                        "company_name": "Acme",
                        "document_type": "shareholder_letter",
                        "year": "2023",
                        "source_type": "PDF",
                        "url": "https://example.com/acme-2023.pdf",
                    },
                ]
            ).to_csv(auto_path, index=False)

            pd.DataFrame(
                [
                    {
                        "company_id": "acme",
                        "company_name": "Acme",
                        "document_type": "shareholder_letter",
                        "year": "2024",
                        "source_type": "PDF",
                        "url": "https://sec.example.com/acme-2024.pdf",
                    }
                ]
            ).to_csv(sec_path, index=False)

            pd.DataFrame(
                [
                    {
                        "company_id": "beta",
                        "company_name": "Beta",
                        "document_type": "shareholder_letter",
                        "year": "2024",
                        "source_type": "PDF",
                        "url": "https://manual.example.com/beta-2024.pdf",
                    }
                ]
            ).to_csv(manual_path, index=False)

            merged = merge_manifests([auto_path, sec_path, manual_path], output_path)

            self.assertEqual(len(merged), 3)
            acme_2024 = merged[(merged["company_id"] == "acme") & (merged["year"] == "2024")].iloc[0]
            self.assertEqual(acme_2024["url"], "https://sec.example.com/acme-2024.pdf")

    def test_merge_sorts_by_company_id_then_year_descending(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            auto_path = root / "letters_manifest.auto.csv"
            sec_path = root / "letters_manifest.sec.csv"
            manual_path = root / "letters_manifest.manual.csv"
            output_path = root / "letters_manifest.csv"

            pd.DataFrame(
                [
                    {
                        "company_id": "zeta",
                        "company_name": "Zeta",
                        "document_type": "shareholder_letter",
                        "year": "2022",
                        "source_type": "PDF",
                        "url": "https://example.com/zeta-2022.pdf",
                    },
                    {
                        "company_id": "acme",
                        "company_name": "Acme",
                        "document_type": "shareholder_letter",
                        "year": "2023",
                        "source_type": "PDF",
                        "url": "https://example.com/acme-2023.pdf",
                    },
                ]
            ).to_csv(auto_path, index=False)
            pd.DataFrame([], columns=["company_id", "company_name", "document_type", "year", "source_type", "url"]).to_csv(sec_path, index=False)
            pd.DataFrame(
                [
                    {
                        "company_id": "acme",
                        "company_name": "Acme",
                        "document_type": "shareholder_letter",
                        "year": "2024",
                        "source_type": "PDF",
                        "url": "https://example.com/acme-2024.pdf",
                    }
                ]
            ).to_csv(manual_path, index=False)

            merged = merge_manifests([auto_path, sec_path, manual_path], output_path)

            ordered_pairs = list(merged[["company_id", "year"]].itertuples(index=False, name=None))
            self.assertEqual(ordered_pairs, [("acme", "2024"), ("acme", "2023"), ("zeta", "2022")])
            self.assertTrue(output_path.exists())


if __name__ == "__main__":
    unittest.main()
