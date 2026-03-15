import unittest
from unittest import mock

from scripts import archive_scrapers


class ArchiveScraperTests(unittest.TestCase):
    @unittest.skipIf(archive_scrapers.requests is None, "requests is not installed")
    @mock.patch("scripts.archive_scrapers.requests.get")
    def test_scrape_berkshire_extracts_pdf_links_and_year(self, mock_get):
        mock_get.return_value = mock.Mock(
            status_code=200,
            text='''
                <html><body>
                    <a href="/letters/2024ltr.pdf">2024 Letter</a>
                    <a href="https://example.com/files/no-year-letter.pdf">Shareholder Letter</a>
                    <a href="/letters/not-a-pdf.html">Not PDF</a>
                </body></html>
            ''',
        )
        mock_get.return_value.raise_for_status.return_value = None

        rows = archive_scrapers.scrape_berkshire_letters()

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["url"], "https://www.berkshirehathaway.com/letters/2024ltr.pdf")
        self.assertEqual(rows[0]["year"], "2024")
        self.assertEqual(rows[1]["year"], "")
        self.assertEqual(rows[0]["source_type"], "PDF")
        self.assertEqual(rows[0]["document_type"], "shareholder_letter")

    def test_get_archive_scraper_router(self):
        self.assertIs(archive_scrapers.get_archive_scraper("amazon"), archive_scrapers.scrape_amazon_letters)
        self.assertIsNone(archive_scrapers.get_archive_scraper("unknown"))


if __name__ == "__main__":
    unittest.main()
