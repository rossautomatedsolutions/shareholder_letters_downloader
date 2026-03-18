import csv
import tempfile
import unittest
from pathlib import Path

from scripts.build_sentiment_features import compute_sentiment_metrics, run


class BuildSentimentFeaturesTests(unittest.TestCase):
    def test_compute_sentiment_metrics_counts_positive_and_negative_words(self):
        metrics = compute_sentiment_metrics(
            "Strong execution improved value but risk and uncertainty remain.",
            positive_words={"strong", "improved", "value"},
            negative_words={"risk", "uncertainty"},
        )

        self.assertEqual(metrics["positive_word_count"], 3)
        self.assertEqual(metrics["negative_word_count"], 2)
        self.assertAlmostEqual(metrics["sentiment_score"], 1 / 9)

    def test_run_writes_sentiment_feature_rows(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            input_root = Path(tmpdir) / "output_text"
            output_path = Path(tmpdir) / "features" / "sentiment_features.csv"

            acme_dir = input_root / "acme_inc"
            acme_dir.mkdir(parents=True)
            (acme_dir / "2022.txt").write_text(
                "Strong innovation improved value despite risk.",
                encoding="utf-8",
            )

            beta_dir = input_root / "beta_corp"
            beta_dir.mkdir(parents=True)
            (beta_dir / "2021.txt").write_text(
                "Weak demand and losses created uncertainty.",
                encoding="utf-8",
            )

            positive_lexicon = Path(tmpdir) / "positive.txt"
            positive_lexicon.write_text("strong\ninnovation\nimproved\nvalue\n", encoding="utf-8")
            negative_lexicon = Path(tmpdir) / "negative.txt"
            negative_lexicon.write_text("risk\nweak\nlosses\nuncertainty\n", encoding="utf-8")

            row_count = run(
                input_root=input_root,
                output_path=output_path,
                positive_lexicon_path=positive_lexicon,
                negative_lexicon_path=negative_lexicon,
            )

            self.assertEqual(row_count, 2)
            with output_path.open(encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))

            self.assertEqual(
                rows,
                [
                    {"company_id": "acme_inc", "year": "2022", "sentiment_score": "0.500000"},
                    {"company_id": "beta_corp", "year": "2021", "sentiment_score": "-0.500000"},
                ],
            )


if __name__ == "__main__":
    unittest.main()
