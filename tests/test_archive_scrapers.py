import unittest
from unittest import mock

from scripts import archive_scrapers


class ArchiveScrapersTests(unittest.TestCase):
    @unittest.skipIf(archive_scrapers.requests is None, "requests is not installed")
    @mock.patch("scripts.archive_scrapers.requests.get")
    def test_scrape_berkshire_letters_extracts_pdf_links_and_year(self, mock_get):
        mock_get.return_value = mock.Mock(
            text='''
                <html>
                    <body>
                        <a href="/letters/2024ltr.pdf">2024 Letter</a>
                        <a href="https://www.berkshirehathaway.com/letters/2023ltr.pdf?download=1">Letter to Shareholders</a>
                        <a href="/files/2022-report.pdf">Annual report PDF</a>
                        <a href="/letters/readme.txt">not pdf</a>
                    </body>
                </html>
            '''
        )
        mock_get.return_value.raise_for_status.return_value = None

        rows = archive_scrapers.scrape_berkshire_letters()

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["year"], "2024")
        self.assertEqual(rows[0]["company_id"], "berkshire_hathaway")
        self.assertEqual(rows[0]["source_type"], "PDF")
        self.assertEqual(rows[1]["year"], "2023")

    def test_get_archive_scraper_returns_known_scraper(self):
        scraper = archive_scrapers.get_archive_scraper("amazon")
        self.assertIs(scraper, archive_scrapers.scrape_amazon_letters)

    def test_get_archive_scraper_returns_none_for_unknown_company(self):
        self.assertIsNone(archive_scrapers.get_archive_scraper("acme"))


    @unittest.skipIf(archive_scrapers.requests is None, "requests is not installed")
    @mock.patch("scripts.archive_scrapers.time.sleep")
    @mock.patch("scripts.archive_scrapers.requests.get")
    def test_extract_pdf_rows_retries_and_returns_empty_after_exhausted_retries(self, mock_get, mock_sleep):
        failure_response = mock.Mock(status_code=403)
        failure_response.raise_for_status.return_value = None
        mock_get.side_effect = [failure_response, failure_response, failure_response, failure_response]

        rows = archive_scrapers._extract_pdf_rows(
            company_id="acme",
            company_name="Acme",
            archive_url="https://example.com/archive",
        )

        self.assertEqual(rows, [])
        self.assertEqual(mock_get.call_count, 4)
        self.assertEqual(mock_sleep.call_count, 3)

    def test_is_shareholder_letter_pdf_rejects_generic_pdf_without_letter_signals(self):
        self.assertFalse(
            archive_scrapers.is_shareholder_letter_pdf(
                "https://example.com/files/2024-annual-report.pdf",
                "Annual report",
            )
        )

    def test_is_shareholder_letter_pdf_accepts_text_keyword_for_non_berkshire_link(self):
        self.assertTrue(
            archive_scrapers.is_shareholder_letter_pdf(
                "https://example.com/files/document.pdf",
                "2024 Shareholder Letter",
            )
        )

if __name__ == "__main__":
    unittest.main()
