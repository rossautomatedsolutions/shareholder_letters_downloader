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


    def test_archive_candidate_filter_rejects_excluded_keyword_even_with_letter_text(self):
        self.assertFalse(
            archive_scrapers._is_archive_letter_candidate(
                "https://example.com/files/2024-letter-proxy.pdf",
                "2024 Shareholder Letter",
            )
        )

    def test_archive_candidate_filter_accepts_keyword_pdf(self):
        self.assertTrue(
            archive_scrapers._is_archive_letter_candidate(
                "https://example.com/files/2024-shareholder-letter.pdf",
                "Download",
            )
        )

    def test_get_archive_scraper_returns_known_scraper(self):
        scraper = archive_scrapers.get_archive_scraper("amazon")
        self.assertIs(scraper, archive_scrapers.scrape_amazon_letters)

    def test_get_archive_scraper_returns_none_for_unknown_company(self):
        self.assertIsNone(archive_scrapers.get_archive_scraper("acme"))


if __name__ == "__main__":
    unittest.main()
