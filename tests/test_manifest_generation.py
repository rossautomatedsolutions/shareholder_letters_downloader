import unittest

try:
    import pandas as pd
except ModuleNotFoundError:  # pragma: no cover
    pd = None

from scripts.generate_manifest_from_ir_pages import (
    MANIFEST_COLUMNS,
    deduplicate_urls,
    is_valid_shareholder_letter,
    validate_manifest_schema,
)


class ShareholderLetterFilteringTests(unittest.TestCase):
    def test_accepts_expected_letter_patterns(self):
        self.assertTrue(
            is_valid_shareholder_letter(
                "https://example.com/docs/2024-letter-to-shareholders.pdf",
                "Download annual filing",
            )
        )
        self.assertTrue(
            is_valid_shareholder_letter(
                "https://example.com/docs/2024-update.pdf",
                "CEO Letter",
            )
        )

    def test_rejects_non_letter_urls(self):
        rejected_urls = [
            "https://example.com/docs/2024-proxy-statement.pdf",
            "https://example.com/docs/financial-data-2024.pdf",
            "https://example.com/docs/investor-presentation.pdf",
            "https://example.com/docs/board-committee-report.pdf",
            "https://example.com/docs/shareholder-information.pdf",
        ]

        for url in rejected_urls:
            with self.subTest(url=url):
                self.assertFalse(is_valid_shareholder_letter(url, "Shareholder Letter"))



@unittest.skipIf(pd is None, "pandas is not installed")
class ManifestGenerationTests(unittest.TestCase):
    def test_output_schema_matches_manifest_requirements(self):
        frame = pd.DataFrame(
            [
                {
                    "company_id": "acme",
                    "company_name": "Acme",
                    "document_type": "shareholder_letter",
                    "year": "2024",
                    "source_type": "PDF",
                    "url": "https://example.com/acme-2024-letter.pdf",
                }
            ],
            columns=MANIFEST_COLUMNS,
        )

        validate_manifest_schema(frame)

    def test_duplicate_urls_are_removed(self):
        rows = [
            {
                "company_id": "acme",
                "company_name": "Acme",
                "document_type": "shareholder_letter",
                "year": "2024",
                "source_type": "PDF",
                "url": "https://example.com/acme-letter.pdf",
            },
            {
                "company_id": "acme",
                "company_name": "Acme",
                "document_type": "shareholder_letter",
                "year": "2023",
                "source_type": "PDF",
                "url": "https://example.com/acme-letter.pdf",
            },
        ]

        deduped = deduplicate_urls(rows)
        self.assertEqual(len(deduped), 1)
        self.assertEqual(deduped[0]["url"], "https://example.com/acme-letter.pdf")


if __name__ == "__main__":
    unittest.main()
