import tempfile
import unittest
from pathlib import Path

try:
    import pandas as pd
except ModuleNotFoundError:  # pragma: no cover - handled by skip
    pd = None

from src.features.sentiment_stability import build_sentiment_stability, run


@unittest.skipIf(pd is None, "pandas is not installed")
class BuildSentimentStabilityTests(unittest.TestCase):
    def test_build_sentiment_stability_uses_expanding_median_per_company(self):
        frame = pd.DataFrame(
            [
                {"company_id": "beta", "year": "2021", "sentiment_score": 0.20},
                {"company_id": "acme", "year": "2022", "sentiment_score": 0.10},
                {"company_id": "acme", "year": "2020", "sentiment_score": -0.20},
                {"company_id": "acme", "year": "2021", "sentiment_score": 0.40},
                {"company_id": "beta", "year": "2022", "sentiment_score": 0.50},
            ]
        )

        result = build_sentiment_stability(frame)

        self.assertEqual(
            result.to_dict("records"),
            [
                {
                    "company_id": "acme",
                    "year": "2020",
                    "sentiment_score": -0.2,
                    "sentiment_deviation": 0.0,
                    "sentiment_stability_score": -0.0,
                },
                {
                    "company_id": "acme",
                    "year": "2021",
                    "sentiment_score": 0.4,
                    "sentiment_deviation": 0.3,
                    "sentiment_stability_score": -0.3,
                },
                {
                    "company_id": "acme",
                    "year": "2022",
                    "sentiment_score": 0.1,
                    "sentiment_deviation": 0.0,
                    "sentiment_stability_score": -0.0,
                },
                {
                    "company_id": "beta",
                    "year": "2021",
                    "sentiment_score": 0.2,
                    "sentiment_deviation": 0.0,
                    "sentiment_stability_score": -0.0,
                },
                {
                    "company_id": "beta",
                    "year": "2022",
                    "sentiment_score": 0.5,
                    "sentiment_deviation": 0.15,
                    "sentiment_stability_score": -0.15,
                },
            ],
        )

    def test_run_reads_and_writes_sentiment_stability_csv(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            input_path = Path(tmpdir) / "sentiment_features.csv"
            output_path = Path(tmpdir) / "sentiment_stability.csv"
            pd.DataFrame(
                [
                    {"company_id": "acme", "year": "2020", "sentiment_score": 0.10},
                    {"company_id": "acme", "year": "2021", "sentiment_score": -0.10},
                ]
            ).to_csv(input_path, index=False)

            result = run(input_path=input_path, output_path=output_path)

            self.assertEqual(len(result), 2)
            written = pd.read_csv(output_path, dtype={"company_id": str, "year": str})
            self.assertEqual(
                written.to_dict("records"),
                [
                    {
                        "company_id": "acme",
                        "year": "2020",
                        "sentiment_score": 0.1,
                        "sentiment_deviation": 0.0,
                        "sentiment_stability_score": -0.0,
                    },
                    {
                        "company_id": "acme",
                        "year": "2021",
                        "sentiment_score": -0.1,
                        "sentiment_deviation": 0.1,
                        "sentiment_stability_score": -0.1,
                    },
                ],
            )


if __name__ == "__main__":
    unittest.main()
