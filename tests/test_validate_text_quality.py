import csv
import tempfile
import unittest
from pathlib import Path

from scripts.validate_text_quality import run


class ValidateTextQualityTests(unittest.TestCase):
    def test_run_passes_full_length_text(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            input_root = tmp_path / "output_text"
            company_dir = input_root / "acme"
            company_dir.mkdir(parents=True)
            text = ("Shareholders " * 1600).strip()
            (company_dir / "2024.txt").write_text(text, encoding="utf-8")
            (company_dir / "2024.json").write_text(
                '{"company_id":"acme","year":"2024","document_type":"shareholder_letter"}',
                encoding="utf-8",
            )
            output_path = tmp_path / "reports" / "text_quality_report.csv"

            rows, failed_count = run(
                input_root=input_root,
                output_path=output_path,
                min_char_count=5000,
                min_word_count=1000,
            )

            self.assertEqual(failed_count, 0)
            self.assertEqual(rows[0]["status"], "pass")
            with output_path.open(encoding="utf-8", newline="") as handle:
                written_rows = list(csv.DictReader(handle))
            self.assertEqual(written_rows[0]["company_id"], "acme")
            self.assertEqual(written_rows[0]["status"], "pass")

    def test_run_fails_short_error_page_text(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            input_root = tmp_path / "output_text"
            company_dir = input_root / "badco"
            company_dir.mkdir(parents=True)
            (company_dir / "2024.txt").write_text(
                "Access denied. Page not found.",
                encoding="utf-8",
            )
            output_path = tmp_path / "reports" / "text_quality_report.csv"

            rows, failed_count = run(
                input_root=input_root,
                output_path=output_path,
                min_char_count=5000,
                min_word_count=1000,
            )

            self.assertEqual(failed_count, 1)
            self.assertEqual(rows[0]["status"], "fail")
            self.assertIn("char_count_below_threshold", rows[0]["failed_checks"])
            self.assertIn("word_count_below_threshold", rows[0]["failed_checks"])
            self.assertIn("bad_phrase_detected", rows[0]["failed_checks"])

    def test_run_does_not_flag_normal_prose_with_forbidden_word(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            input_root = tmp_path / "output_text"
            company_dir = input_root / "berkshire_hathaway"
            company_dir.mkdir(parents=True)
            text = (("The letter discussed forbidden words at board meetings. " * 250) + "end").strip()
            (company_dir / "2024.txt").write_text(text, encoding="utf-8")
            output_path = tmp_path / "reports" / "text_quality_report.csv"

            rows, failed_count = run(
                input_root=input_root,
                output_path=output_path,
                min_char_count=5000,
                min_word_count=1000,
            )

            self.assertEqual(failed_count, 0)
            self.assertEqual(rows[0]["status"], "pass")


if __name__ == "__main__":
    unittest.main()
