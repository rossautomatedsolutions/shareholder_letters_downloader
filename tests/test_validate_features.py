import io
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from scripts import validate_features


class ValidateFeaturesTests(unittest.TestCase):
    def test_main_returns_zero_for_valid_feature_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            sentiment_path = tmp_path / "sentiment_features.csv"
            sentiment_path.write_text(
                "company_id,year,sentiment_score\nacme,2021,0.1\nacme,2022,0.2\n",
                encoding="utf-8",
            )
            keyword_path = tmp_path / "keyword_features.csv"
            keyword_path.write_text(
                "company_id,year,keyword,frequency\nacme,2021,innovation,3\nacme,2022,growth,2\n",
                encoding="utf-8",
            )

            stdout = io.StringIO()
            stderr = io.StringIO()
            with patch.object(validate_features, "SENTIMENT_FEATURES_PATH", sentiment_path), patch.object(
                validate_features,
                "KEYWORD_FEATURES_PATH",
                keyword_path,
            ), redirect_stdout(stdout), redirect_stderr(stderr):
                with self.assertRaises(SystemExit) as exit_context:
                    validate_features.main()

            self.assertEqual(exit_context.exception.code, 0)
            self.assertIn("Sentiment rows: 2", stdout.getvalue())
            self.assertEqual(stderr.getvalue(), "")

    def test_main_returns_warning_for_missing_years_and_duplicates(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            sentiment_path = tmp_path / "sentiment_features.csv"
            sentiment_path.write_text(
                (
                    "company_id,year,sentiment_score\n"
                    "acme,2021,0.1\n"
                    "acme,2023,0.2\n"
                    "acme,2023,0.2\n"
                ),
                encoding="utf-8",
            )
            keyword_path = tmp_path / "keyword_features.csv"
            keyword_path.write_text("company_id,year,keyword,frequency\n", encoding="utf-8")

            stdout = io.StringIO()
            stderr = io.StringIO()
            with patch.object(validate_features, "SENTIMENT_FEATURES_PATH", sentiment_path), patch.object(
                validate_features,
                "KEYWORD_FEATURES_PATH",
                keyword_path,
            ), redirect_stdout(stdout), redirect_stderr(stderr):
                with self.assertRaises(SystemExit) as exit_context:
                    validate_features.main()

            self.assertEqual(exit_context.exception.code, 1)
            error_output = stderr.getvalue()
            self.assertIn("missing years: [2022]", error_output)
            self.assertIn("duplicate row(s)", error_output)
            self.assertIn("has zero rows", error_output)


if __name__ == "__main__":
    unittest.main()
