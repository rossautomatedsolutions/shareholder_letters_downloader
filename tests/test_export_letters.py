import unittest
import json
import tempfile
from unittest.mock import patch
from types import SimpleNamespace

from export_letters import (
    ManifestValidationError,
    filter_rows,
    load_manifest,
    normalized_pdf_path,
    normalize_source_type,
    process_rows,
    validate_manifest,
)
from pathlib import Path


class ExportLettersTests(unittest.TestCase):
    def setUp(self):
        self.base_row = {
            "company_id": "acme_inc",
            "company_name": "Acme Inc",
            "document_type": "chairman_letter",
            "year": "2023",
            "source_type": "standalone_letter_pdf",
            "url": "https://example.com/2023.pdf",
        }
        self.base_row_2022 = dict(self.base_row, year="2022", url="https://example.com/2022.pdf")
        self.other_company_row = dict(
            self.base_row,
            company_id="other_inc",
            company_name="Other Inc",
            year="2021",
            url="https://example.com/2021.pdf",
        )

    def test_validate_manifest_accepts_valid_rows(self):
        validate_manifest([self.base_row])

    def test_normalize_source_type_accepts_legacy_and_canonical_values(self):
        self.assertEqual(normalize_source_type("PDF"), "standalone_letter_pdf")
        self.assertEqual(normalize_source_type("HTML"), "html_letter")
        self.assertEqual(normalize_source_type("STANDALONE_LETTER_PDF"), "standalone_letter_pdf")
        self.assertEqual(normalize_source_type("annual_report_pdf"), "annual_report_pdf")

    def test_validate_manifest_rejects_duplicate_composite_key(self):
        rows = [self.base_row, dict(self.base_row)]
        with self.assertRaises(ManifestValidationError):
            validate_manifest(rows)

    def test_validate_manifest_rejects_non_numeric_year(self):
        row = dict(self.base_row, year="20X3")
        with self.assertRaises(ManifestValidationError):
            validate_manifest([row])

    def test_load_manifest_normalizes_source_type_to_canonical_lowercase(self):
        with tempfile.TemporaryDirectory() as tmp:
            manifest_path = Path(tmp) / "letters_manifest.csv"
            manifest_path.write_text(
                "company_id,company_name,document_type,year,source_type,url\n"
                "acme,Acme,shareholder_letter,2024,PDF,https://example.com/acme.pdf\n"
                "beta,Beta,shareholder_letter,2024,HTML,https://example.com/beta.html\n",
                encoding="utf-8",
            )

            rows = load_manifest(manifest_path)

            self.assertEqual(rows[0]["source_type"], "standalone_letter_pdf")
            self.assertEqual(rows[1]["source_type"], "html_letter")

    def test_normalized_output_path_uses_company_doc_and_year(self):
        path = normalized_pdf_path(Path("output"), self.base_row)
        self.assertEqual(path.as_posix(), "output/acme_inc/chairman_letter/2023.pdf")

    def test_existing_files_are_skipped(self):
        with tempfile.TemporaryDirectory() as tmp:
            output_root = Path(tmp) / "output"
            reports_dir = Path(tmp) / "reports"
            normalized_path = normalized_pdf_path(output_root, self.base_row)
            normalized_path.parent.mkdir(parents=True, exist_ok=True)
            normalized_path.write_bytes(b"existing-pdf")

            with patch("export_letters.fetch_binary") as mock_fetch_binary:
                _, json_report = process_rows(
                    rows=[self.base_row],
                    output_root=output_root,
                    reports_dir=reports_dir,
                    render_overrides={},
                    retries=0,
                    timeout_seconds=1,
                    force_redownload=False,
                )

            mock_fetch_binary.assert_not_called()
            self.assertEqual(normalized_path.read_bytes(), b"existing-pdf")
            rows = json.loads(json_report.read_text(encoding="utf-8"))
            self.assertEqual(rows[0]["status"], "SKIPPED_EXISTING")

    def test_force_redownload_overwrites_existing_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            output_root = Path(tmp) / "output"
            reports_dir = Path(tmp) / "reports"
            normalized_path = normalized_pdf_path(output_root, self.base_row)
            normalized_path.parent.mkdir(parents=True, exist_ok=True)
            normalized_path.write_bytes(b"old-content")

            raw_pdf_path = output_root / "raw" / self.base_row["company_id"] / f"{self.base_row['year']}.pdf"

            def fake_fetch_binary(_url, company_id, year, actual_output_root, _timeout_seconds, _retries):
                self.assertEqual(company_id, self.base_row["company_id"])
                self.assertEqual(year, self.base_row["year"])
                self.assertEqual(actual_output_root, output_root)
                raw_pdf_path.parent.mkdir(parents=True, exist_ok=True)
                raw_pdf_path.write_bytes(b"new-content")
                return True, None, raw_pdf_path

            with patch("export_letters.fetch_binary", side_effect=fake_fetch_binary) as mock_fetch_binary:
                _, json_report = process_rows(
                    rows=[self.base_row],
                    output_root=output_root,
                    reports_dir=reports_dir,
                    render_overrides={},
                    retries=0,
                    timeout_seconds=1,
                    force_redownload=True,
                )

            mock_fetch_binary.assert_called_once_with(
                self.base_row["url"],
                self.base_row["company_id"],
                self.base_row["year"],
                output_root,
                1,
                0,
            )
            self.assertEqual(normalized_path.read_bytes(), b"new-content")
            rows = json.loads(json_report.read_text(encoding="utf-8"))
            self.assertEqual(rows[0]["status"], "success")

    def test_process_rows_fails_cleanly_for_manual_review_source_type(self):
        row = dict(self.base_row, source_type="manual_review_needed", url="https://example.com/review")
        with tempfile.TemporaryDirectory() as tmp:
            output_root = Path(tmp) / "output"
            reports_dir = Path(tmp) / "reports"

            _, json_report = process_rows(
                rows=[row],
                output_root=output_root,
                reports_dir=reports_dir,
                render_overrides={},
                retries=0,
                timeout_seconds=1,
                force_redownload=False,
            )

            rows = json.loads(json_report.read_text(encoding="utf-8"))
            self.assertEqual(rows[0]["status"], "failed")
            self.assertIn("requires manual source review", rows[0]["error_message"])

    def test_process_rows_stops_started_playwright_instance_for_html(self):
        row = dict(self.base_row, source_type="html_letter", url="https://example.com/review")

        class DummyPage:
            def __init__(self):
                self.closed = False

            def close(self):
                self.closed = True

        class DummyBrowser:
            def __init__(self, page):
                self.page = page
                self.closed = False

            def new_page(self):
                return self.page

            def close(self):
                self.closed = True

        class DummyPlaywright:
            def __init__(self, browser):
                self.chromium = SimpleNamespace(launch=lambda: browser)
                self.stopped = False

            def stop(self):
                self.stopped = True

        class DummyContextManager:
            def __init__(self, playwright):
                self.playwright = playwright
                self.start_called = False

            def start(self):
                self.start_called = True
                return self.playwright

        with tempfile.TemporaryDirectory() as tmp:
            output_root = Path(tmp) / "output"
            reports_dir = Path(tmp) / "reports"
            page = DummyPage()
            browser = DummyBrowser(page)
            started_playwright = DummyPlaywright(browser)
            context_manager = DummyContextManager(started_playwright)

            with (
                patch("playwright.sync_api.sync_playwright", return_value=context_manager),
                patch("export_letters.fetch_text") as mock_fetch_text,
                patch("export_letters.render_html_to_pdf") as mock_render_html_to_pdf,
                patch("export_letters.compute_sha256", return_value="abc123"),
            ):
                _, json_report = process_rows(
                    rows=[row],
                    output_root=output_root,
                    reports_dir=reports_dir,
                    render_overrides={},
                    retries=0,
                    timeout_seconds=1,
                    force_redownload=True,
                )

            mock_fetch_text.assert_called_once()
            mock_render_html_to_pdf.assert_called_once()
            self.assertTrue(context_manager.start_called)
            self.assertTrue(page.closed)
            self.assertTrue(browser.closed)
            self.assertTrue(started_playwright.stopped)
            rows = json.loads(json_report.read_text(encoding="utf-8"))
            self.assertEqual(rows[0]["status"], "success")

    def test_filter_rows_by_company(self):
        rows = [self.base_row, self.other_company_row]
        filtered = filter_rows(rows, company="acme_inc")
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0]["company_id"], "acme_inc")

    def test_filter_rows_by_exact_year(self):
        rows = [self.base_row_2022, self.base_row]
        filtered = filter_rows(rows, year=2023)
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0]["year"], "2023")

    def test_filter_rows_by_year_range(self):
        rows = [self.other_company_row, self.base_row_2022, self.base_row]
        filtered = filter_rows(rows, year_start=2022, year_end=2023)
        self.assertEqual([row["year"] for row in filtered], ["2022", "2023"])

    def test_filter_rows_rejects_invalid_year_filter_combinations(self):
        with self.assertRaises(ManifestValidationError):
            filter_rows([self.base_row], year=2023, year_start=2022)

    def test_filter_rows_rejects_inverted_year_range(self):
        with self.assertRaises(ManifestValidationError):
            filter_rows([self.base_row], year_start=2024, year_end=2023)

    def test_filter_rows_rejects_empty_results(self):
        with self.assertRaises(ManifestValidationError):
            filter_rows([self.base_row], company="missing")


if __name__ == "__main__":
    unittest.main()
