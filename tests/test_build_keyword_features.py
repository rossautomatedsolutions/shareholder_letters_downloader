import csv
import tempfile
import unittest
from pathlib import Path

from scripts.build_keyword_features import extract_top_keywords, run


class BuildKeywordFeaturesTests(unittest.TestCase):
    def test_extract_top_keywords_removes_stopwords(self):
        keywords = extract_top_keywords(
            "The business business grows with durable durable durable results for shareholders.",
            top_n=3,
        )

        self.assertEqual(keywords, [("durable", 3), ("business", 2), ("grows", 1)])

    def test_run_writes_keyword_feature_rows(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            input_root = Path(tmpdir) / "output_text"
            output_path = Path(tmpdir) / "features" / "keyword_features.csv"

            acme_dir = input_root / "acme_inc"
            acme_dir.mkdir(parents=True)
            (acme_dir / "2022.txt").write_text(
                "Innovation innovation and growth deliver growth for customers and innovation.",
                encoding="utf-8",
            )

            beta_dir = input_root / "beta_corp"
            beta_dir.mkdir(parents=True)
            (beta_dir / "2021.txt").write_text(
                "Capital allocation discipline and capital returns matter.",
                encoding="utf-8",
            )

            row_count = run(input_root=input_root, output_path=output_path, top_n=2)

            self.assertEqual(row_count, 4)
            with output_path.open(encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))

            self.assertEqual(
                rows,
                [
                    {"company_id": "acme_inc", "year": "2022", "keyword": "innovation", "frequency": "3"},
                    {"company_id": "acme_inc", "year": "2022", "keyword": "growth", "frequency": "2"},
                    {"company_id": "beta_corp", "year": "2021", "keyword": "capital", "frequency": "2"},
                    {"company_id": "beta_corp", "year": "2021", "keyword": "allocation", "frequency": "1"},
                ],
            )


if __name__ == "__main__":
    unittest.main()
