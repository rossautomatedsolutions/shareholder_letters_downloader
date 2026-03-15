import unittest
from unittest import mock

from scripts.archive_scrapers import BeautifulSoup, _scrape_archive_page, get_archive_scraper


class ArchiveScraperTests(unittest.TestCase):
    def test_get_archive_scraper_returns_blackrock_scraper(self):
        self.assertIsNotNone(get_archive_scraper("blackrock"))
        self.assertIsNone(get_archive_scraper("acme"))

    @unittest.skipIf(BeautifulSoup is None, "beautifulsoup4 is not installed")
    def test_scrape_archive_page_filters_non_letter_pdfs(self):
        html = '''
            <html><body>
              <a href="/files/2024-shareholder-letter.pdf">2024 Shareholder Letter</a>
              <a href="/files/2024-proxy-statement.pdf">2024 Proxy Statement</a>
            </body></html>
        '''
        response = mock.Mock(text=html)
        request_with_retries = mock.Mock(return_value=response)

        rows = _scrape_archive_page(
            "blackrock",
            "BlackRock",
            "https://ir.blackrock.com/annual-reports-and-proxy/default.aspx",
            request_with_retries,
        )

        self.assertEqual(len(rows), 1)
        self.assertIn("shareholder-letter", rows[0]["url"])

    @unittest.skipIf(BeautifulSoup is None, "beautifulsoup4 is not installed")
    def test_scrape_archive_page_returns_empty_when_request_fails(self):
        request_with_retries = mock.Mock(return_value=None)

        rows = _scrape_archive_page(
            "blackrock",
            "BlackRock",
            "https://ir.blackrock.com/annual-reports-and-proxy/default.aspx",
            request_with_retries,
        )

        self.assertEqual(rows, [])


if __name__ == "__main__":
    unittest.main()
