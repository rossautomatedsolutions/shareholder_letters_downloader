import unittest
from unittest.mock import patch

from scripts import archive_scrapers
from scripts.archive_scrapers import get_archive_scraper, scrape_amazon_letters


@unittest.skipIf(archive_scrapers.requests is None or archive_scrapers.BeautifulSoup is None, "requests/bs4 not installed")
class ArchiveScrapersTests(unittest.TestCase):
    @patch("scripts.archive_scrapers.requests.get")
    def test_scrape_amazon_letters_extracts_pdf_rows(self, mock_get):
        mock_get.return_value.raise_for_status.return_value = None
        mock_get.return_value.text = """
            <html>
              <body>
                <a href=\"/files/2024-shareholder-letter.pdf\">2024 Shareholder Letter</a>
                <a href=\"https://cdn.example.com/docs/report-2023.pdf\">Annual report 2023</a>
                <a href=\"/files/not-a-pdf.html\">Not a PDF</a>
              </body>
            </html>
        """

        rows = scrape_amazon_letters("amazon", "Amazon")

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["year"], "2024")
        self.assertEqual(rows[1]["year"], "2023")
        self.assertTrue(rows[0]["url"].endswith("2024-shareholder-letter.pdf"))

    def test_get_archive_scraper_returns_none_for_non_specialized_company(self):
        self.assertIsNone(get_archive_scraper("apple"))


if __name__ == "__main__":
    unittest.main()
