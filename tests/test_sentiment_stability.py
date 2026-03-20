from __future__ import annotations

from pathlib import Path

import pytest

pd = pytest.importorskip("pandas")

from scripts.build_sentiment_stability import validate_output
from src.features.sentiment_stability import build_sentiment_stability, run

def test_build_sentiment_stability_uses_expanding_median_without_lookahead() -> None:
    frame = pd.DataFrame(
        [
            {"company_id": "beta", "year": "2022", "sentiment_score": 0.50},
            {"company_id": "acme", "year": "2022", "sentiment_score": 0.10},
            {"company_id": "acme", "year": "2020", "sentiment_score": -0.20},
            {"company_id": "acme", "year": "2021", "sentiment_score": 0.40},
            {"company_id": "beta", "year": "2021", "sentiment_score": 0.20},
        ]
    )

    result = build_sentiment_stability(frame)

    assert result.to_dict("records") == [
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
    ]

def test_build_sentiment_stability_calculates_deviation_from_expanding_median() -> None:
    frame = pd.DataFrame(
        [
            {"company_id": "acme", "year": "2020", "sentiment_score": 0.00},
            {"company_id": "acme", "year": "2021", "sentiment_score": 0.20},
            {"company_id": "acme", "year": "2022", "sentiment_score": -0.30},
            {"company_id": "acme", "year": "2023", "sentiment_score": 0.40},
        ]
    )

    result = build_sentiment_stability(frame)

    assert result["sentiment_deviation"].tolist() == [0.0, 0.1, 0.3, 0.35]
    assert result["sentiment_stability_score"].tolist() == [-0.0, -0.1, -0.3, -0.35]

def test_build_sentiment_stability_handles_missing_data() -> None:
    frame = pd.DataFrame(
        [
            {"company_id": "acme", "year": "2020", "sentiment_score": 0.10},
            {"company_id": "acme", "year": "2021", "sentiment_score": None},
            {"company_id": "acme", "year": "2022", "sentiment_score": -0.30},
        ]
    )

    result = build_sentiment_stability(frame)

    assert result["sentiment_score"].isna().tolist() == [False, True, False]
    assert result["sentiment_deviation"].isna().tolist() == [False, True, False]
    assert result["sentiment_stability_score"].isna().tolist() == [False, True, False]
    assert result.loc[2, "sentiment_deviation"] == 0.2

def test_build_sentiment_stability_aggregates_duplicate_company_year_rows() -> None:
    frame = pd.DataFrame(
        [
            {"company_id": "acme", "year": "2020", "sentiment_score": 0.10},
            {"company_id": "acme", "year": "2020", "sentiment_score": 0.30},
            {"company_id": "acme", "year": "2021", "sentiment_score": 0.20},
        ]
    )

    result = build_sentiment_stability(frame)

    assert len(result) == 2
    assert result.to_dict("records") == [
        {
            "company_id": "acme",
            "year": "2020",
            "sentiment_score": 0.2,
            "sentiment_deviation": 0.0,
            "sentiment_stability_score": -0.0,
        },
        {
            "company_id": "acme",
            "year": "2021",
            "sentiment_score": 0.2,
            "sentiment_deviation": 0.0,
            "sentiment_stability_score": -0.0,
        },
    ]

def test_run_overwrites_output_and_validation_passes(tmp_path: Path) -> None:
    input_path = tmp_path / "sentiment_features.csv"
    output_path = tmp_path / "sentiment_stability.csv"

    pd.DataFrame(
        [
            {"company_id": "acme", "year": "2021", "sentiment_score": 0.10},
            {"company_id": "acme", "year": "2020", "sentiment_score": 0.00},
        ]
    ).to_csv(input_path, index=False)

    output_path.write_text("stale,data\n1,2\n", encoding="utf-8")

    result = run(input_path=input_path, output_path=output_path)
    validated = validate_output(output_path)

    assert len(result) == 2
    assert len(validated) == 2
    assert validated.to_dict("records") == [
        {
            "company_id": "acme",
            "year": "2020",
            "sentiment_score": 0.0,
            "sentiment_deviation": 0.0,
            "sentiment_stability_score": -0.0,
        },
        {
            "company_id": "acme",
            "year": "2021",
            "sentiment_score": 0.1,
            "sentiment_deviation": 0.05,
            "sentiment_stability_score": -0.05,
        },
    ]
