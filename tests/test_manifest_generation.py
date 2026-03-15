import unittest
from unittest import mock

try:
    import pandas as pd
except ModuleNotFoundError:  # pragma: no cover
    pd = None

from scripts.generate_manifest_from_ir_pages import (
    MANIFEST_COLUMNS,
    CompanyDefinition,
    deduplicate_urls,
    fetch_candidates,
    requests,
    validate_manifest_schema,
)


class ManifestGenerationTests(unittest.TestCase):
    @unittest.skipIf(pd is None, "pandas is not installed")
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

    @unittest.skipIf(requests is None, "requests is not installed")
    @mock.patch("scripts.generate_manifest_from_ir_pages.requests.get")
    def test_fetch_candidates_uses_browser_headers(self, mock_get):
        mock_get.return_value = mock.Mock(
            status_code=200,
            text='''
                <html>
                    <body>
                        <a href="https://example.com/2024-shareholder-letter.pdf">2024 Shareholder Letter</a>
                    </body>
                </html>
            ''',
        )
        company = CompanyDefinition("acme", "Acme", "https://example.com/ir")

        rows = fetch_candidates(company, timeout_seconds=30)

        self.assertEqual(len(rows), 1)
        mock_get.assert_called_with(
            "https://example.com/ir",
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=30,
        )

    @unittest.skipIf(requests is None, "requests is not installed")
    @mock.patch("scripts.generate_manifest_from_ir_pages.time.sleep")
    @mock.patch("scripts.generate_manifest_from_ir_pages.requests.get")
    def test_fetch_candidates_retries_after_http_403(self, mock_get, mock_sleep):
        first_response = mock.Mock(status_code=403)
        first_response.raise_for_status.return_value = None
        second_response = mock.Mock(
            status_code=200,
            text='''
                <html>
                    <body>
                        <a href="https://example.com/2023-shareholder-letter.pdf">2023 Shareholder Letter</a>
                    </body>
                </html>
            ''',
        )
        second_response.raise_for_status.return_value = None
        mock_get.side_effect = [first_response, second_response]

        company = CompanyDefinition("acme", "Acme", "https://example.com/ir")
        rows = fetch_candidates(company, timeout_seconds=30)

        self.assertEqual(len(rows), 1)
        self.assertEqual(mock_get.call_count, 2)
        mock_sleep.assert_called_once_with(1)


if __name__ == "__main__":
    unittest.main()
