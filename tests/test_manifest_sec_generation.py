import unittest
from unittest import mock

from scripts.generate_manifest_from_sec import (
    Company,
    Filing,
    confidence_score_for_url,
    detect_source_type,
    discover_letter_documents_for_filing,
    generate_rows,
    has_target_phrase,
    select_latest_filings,
)


class ManifestSecGenerationTests(unittest.TestCase):
    def test_has_target_phrase_matches_supported_letter_patterns(self):
        self.assertTrue(has_target_phrase("2024 Letter to Shareholders.pdf"))
        self.assertTrue(has_target_phrase("Chairman Letter - Annual Report"))
        self.assertTrue(has_target_phrase("CEO_Letter_2023.html"))
        self.assertTrue(has_target_phrase("letterfromchairman2022.pdf"))
        self.assertFalse(has_target_phrase("proxy statement"))

    def test_detect_source_type_limits_manifest_sources_to_html_or_pdf(self):
        self.assertEqual(detect_source_type("ceo-letter.pdf"), "PDF")
        self.assertEqual(detect_source_type("chairman-letter.htm"), "HTML")
        self.assertEqual(detect_source_type("letter-to-shareholders.html"), "HTML")
        self.assertIsNone(detect_source_type("letter-to-shareholders.docx"))

    def test_confidence_score_for_url_rules(self):
        self.assertEqual(confidence_score_for_url("https://example.com/ceo-letter-2024.pdf"), 1.0)
        self.assertEqual(confidence_score_for_url("https://example.com/annual-report-2024.pdf"), 0.7)
        self.assertEqual(confidence_score_for_url("https://example.com/10-k-2024.pdf"), 0.3)

    def test_select_latest_filings_returns_most_recent_first(self):
        filings = [
            Filing("a", "2022-02-01", "2021-12-31", "10-K"),
            Filing("b", "2024-02-01", "2023-12-31", "10-K"),
            Filing("c", "2023-02-01", "2022-12-31", "10-K"),
        ]
        selected = select_latest_filings(filings, max_filings_per_company=2)
        self.assertEqual([f.accession_number for f in selected], ["b", "c"])

    def test_discover_letter_documents_for_filing_filters_and_extracts_urls(self):
        mock_client = mock.Mock()
        mock_client.get_json.return_value = {
            "directory": {
                "item": [
                    {"name": "ceo-letter-2024.pdf", "type": "PDF"},
                    {"name": "letter-to-shareholders-2024.html", "type": "EX-99"},
                    {"name": "chairman-letter.txt", "type": "TXT"},
                    {"name": "annual-report.pdf", "type": "PDF"},
                ]
            }
        }

        docs = discover_letter_documents_for_filing(
            mock_client,
            cik="0000320193",
            accession_number="0000320193-24-000123",
        )

        self.assertEqual(
            docs,
            [
                {
                    "source_type": "PDF",
                    "url": "https://www.sec.gov/Archives/edgar/data/320193/000032019324000123/ceo-letter-2024.pdf",
                },
                {
                    "source_type": "HTML",
                    "url": "https://www.sec.gov/Archives/edgar/data/320193/000032019324000123/letter-to-shareholders-2024.html",
                },
            ],
        )

    @mock.patch("scripts.generate_manifest_from_sec.discover_letter_documents_for_filing")
    def test_generate_rows_emits_manifest_schema_rows(self, mock_discover):
        mock_client = mock.Mock()
        mock_client.get_json.return_value = {
            "filings": {
                "recent": {
                    "accessionNumber": ["0000320193-24-000123"],
                    "filingDate": ["2024-11-01"],
                    "reportDate": ["2024-09-30"],
                    "form": ["10-K"],
                }
            }
        }
        mock_discover.return_value = [
            {
                "source_type": "PDF",
                "url": "https://www.sec.gov/Archives/edgar/data/320193/000032019324000123/ceo-letter-2024.pdf",
            }
        ]

        rows = generate_rows(
            mock_client,
            companies=[Company(ticker="AAPL", cik="0000320193", name="Apple Inc.")],
            years=10,
            max_filings_per_company=1,
        )

        self.assertEqual(
            rows,
            [
                {
                    "company_id": "aapl",
                    "company_name": "Apple Inc.",
                    "document_type": "shareholder_letter",
                    "year": "2024",
                    "source_type": "PDF",
                    "url": "https://www.sec.gov/Archives/edgar/data/320193/000032019324000123/ceo-letter-2024.pdf",
                    "confidence_score": 1.0,
                }
            ],
        )


if __name__ == "__main__":
    unittest.main()
