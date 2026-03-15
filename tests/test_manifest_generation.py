import unittest

try:
    import pandas as pd
except ModuleNotFoundError:  # pragma: no cover
    pd = None


from scripts.generate_manifest_from_ir_pages import (
    MANIFEST_COLUMNS,
    CompanyDefinition,
    deduplicate_urls,
    generate_manifest,
    validate_manifest_schema,
)


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

    @unittest.skipIf(pd is None, "pandas is not installed")
    @unittest.mock.patch("scripts.generate_manifest_from_ir_pages.fetch_candidates")
    @unittest.mock.patch("scripts.generate_manifest_from_ir_pages.get_archive_scraper")
    def test_generate_manifest_prefers_archive_scraper(self, mock_get_archive_scraper, mock_fetch_candidates):
        archive_rows = [
            {
                "company_id": "amazon",
                "company_name": "Amazon",
                "document_type": "shareholder_letter",
                "year": "2024",
                "source_type": "PDF",
                "url": "https://example.com/amazon-2024-letter.pdf",
            }
        ]

        mock_get_archive_scraper.return_value = lambda: archive_rows

        frame = generate_manifest([CompanyDefinition("amazon", "Amazon", "https://example.com/ir")])

        mock_fetch_candidates.assert_not_called()
        self.assertEqual(len(frame), 1)
        self.assertEqual(frame.iloc[0]["url"], archive_rows[0]["url"])



if __name__ == "__main__":
    unittest.main()
