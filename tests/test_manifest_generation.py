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
    generate_manifest,
    is_candidate_link,
    is_valid_shareholder_letter,
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


    def test_is_valid_shareholder_letter_accepts_url_keywords(self):
        self.assertTrue(
            is_valid_shareholder_letter(
                "https://example.com/2024-letter-to-shareholders.pdf",
                "Annual report",
            )
        )
        self.assertTrue(
            is_valid_shareholder_letter(
                "https://example.com/files/2023-chairman-letter.pdf",
                "Download",
            )
        )

    def test_is_candidate_link_accepts_berkshire_letters_ltr_pdf_with_year_only_text(self):
        self.assertTrue(
            is_candidate_link(
                "https://www.berkshirehathaway.com/letters/2024ltr.pdf",
                "2024",
            )
        )

    def test_is_candidate_link_accepts_berkshire_letters_ltr_pdf_with_query(self):
        self.assertTrue(
            is_candidate_link(
                "https://www.berkshirehathaway.com/letters/2024ltr.pdf?source=archive",
                "2024",
            )
        )

    def test_is_candidate_link_rejects_non_berkshire_ltr_pdf_pattern_without_keywords(self):
        self.assertFalse(
            is_candidate_link(
                "https://example.com/letters/2024ltr.pdf",
                "2024",
            )
        )

    def test_is_valid_shareholder_letter_accepts_text_keywords(self):
        self.assertTrue(
            is_valid_shareholder_letter(
                "https://example.com/files/2024-report.pdf",
                "2024 Shareholder Letter",
            )
        )
        self.assertTrue(
            is_valid_shareholder_letter(
                "https://example.com/files/ceo-update.pdf",
                "CEO Letter to investors",
            )
        )

    def test_is_candidate_link_rejects_non_pdf_even_with_letter_text(self):
        self.assertFalse(
            is_candidate_link(
                "https://example.com/letters/2024-shareholder-letter",
                "2024 Shareholder Letter",
            )
        )

    def test_is_valid_shareholder_letter_rejects_non_letter_documents(self):
        self.assertFalse(
            is_valid_shareholder_letter(
                "https://example.com/2024-proxy-statement.pdf",
                "Proxy Statement",
            )
        )
        self.assertFalse(
            is_valid_shareholder_letter(
                "https://example.com/investor-presentation-q4.pdf",
                "Q4 presentation",
            )
        )
        self.assertFalse(
            is_valid_shareholder_letter(
                "https://example.com/q4-transcript.pdf",
                "Q4 earnings transcript",
            )
        )
        self.assertFalse(
            is_valid_shareholder_letter(
                "https://example.com/financial-data/annual.pdf",
                "Shareholder Letter",
            )
        )
        self.assertFalse(
            is_valid_shareholder_letter(
                "https://example.com/shareholder-information/overview.pdf",
                "Letter",
            )
        )

    def test_is_valid_shareholder_letter_exclude_keywords_override_link_text(self):
        self.assertFalse(
            is_valid_shareholder_letter(
                "https://example.com/proxy/shareholder-letter-2024.pdf",
                "2024 Shareholder Letter",
            )
        )

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

    @unittest.skipIf(pd is None, "pandas is not installed")
    @mock.patch("scripts.generate_manifest_from_ir_pages.time.sleep")
    @mock.patch("scripts.generate_manifest_from_ir_pages.fetch_candidates")
    @mock.patch("scripts.generate_manifest_from_ir_pages.get_archive_scraper")
    def test_generate_manifest_prefers_archive_scraper(self, mock_get_archive_scraper, mock_fetch_candidates, mock_sleep):
        company = CompanyDefinition("berkshire_hathaway", "Berkshire Hathaway", "https://example.com")
        archive_rows = [
            {
                "company_id": "berkshire_hathaway",
                "company_name": "Berkshire Hathaway",
                "document_type": "shareholder_letter",
                "year": "2024",
                "source_type": "PDF",
                "url": "https://www.berkshirehathaway.com/letters/2024ltr.pdf",
            }
        ]
        mock_get_archive_scraper.return_value = mock.Mock(return_value=archive_rows)

        frame = generate_manifest([company])

        self.assertEqual(len(frame), 1)
        self.assertEqual(frame.iloc[0]["url"], "https://www.berkshirehathaway.com/letters/2024ltr.pdf")
        mock_fetch_candidates.assert_not_called()
        mock_sleep.assert_not_called()


    @unittest.skipIf(pd is None, "pandas is not installed")
    @mock.patch("scripts.generate_manifest_from_ir_pages.time.sleep")
    @mock.patch("scripts.generate_manifest_from_ir_pages.fetch_candidates")
    @mock.patch("scripts.generate_manifest_from_ir_pages.get_archive_scraper")
    def test_generate_manifest_falls_back_when_archive_scraper_returns_no_rows(
        self, mock_get_archive_scraper, mock_fetch_candidates, mock_sleep
    ):
        company = CompanyDefinition("amazon", "Amazon", "https://example.com")
        mock_get_archive_scraper.return_value = mock.Mock(return_value=[])
        generic_rows = [
            {
                "company_id": "amazon",
                "company_name": "Amazon",
                "document_type": "shareholder_letter",
                "year": "2024",
                "source_type": "PDF",
                "url": "https://example.com/2024-shareholder-letter.pdf",
            }
        ]
        mock_fetch_candidates.return_value = generic_rows

        frame = generate_manifest([company])

        self.assertEqual(len(frame), 1)
        self.assertEqual(frame.iloc[0]["url"], "https://example.com/2024-shareholder-letter.pdf")
        mock_fetch_candidates.assert_called_once_with(company)
        mock_sleep.assert_not_called()

    @unittest.skipIf(pd is None, "pandas is not installed")
    @mock.patch("scripts.generate_manifest_from_ir_pages.time.sleep")
    @mock.patch("scripts.generate_manifest_from_ir_pages.fetch_candidates")
    @mock.patch("scripts.generate_manifest_from_ir_pages.get_archive_scraper")
    def test_generate_manifest_uses_generic_path_when_archive_scraper_missing(
        self, mock_get_archive_scraper, mock_fetch_candidates, mock_sleep
    ):
        company = CompanyDefinition("acme", "Acme", "https://example.com")
        generic_rows = [
            {
                "company_id": "acme",
                "company_name": "Acme",
                "document_type": "shareholder_letter",
                "year": "2023",
                "source_type": "PDF",
                "url": "https://example.com/2023-shareholder-letter.pdf",
            }
        ]
        mock_get_archive_scraper.return_value = None
        mock_fetch_candidates.return_value = generic_rows

        frame = generate_manifest([company])

        self.assertEqual(len(frame), 1)
        self.assertEqual(frame.iloc[0]["company_id"], "acme")
        mock_fetch_candidates.assert_called_once_with(company)
        mock_sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main()
