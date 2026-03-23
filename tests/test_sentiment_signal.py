from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")

from src.signals.sentiment_signal import build_sentiment_signal, summarize_signal_returns


def test_build_sentiment_signal_assigns_expected_buckets() -> None:
    frame = pd.DataFrame(
        [
            {"company_id": "acme", "year": "2020", "sentiment_score": 0.10, "sentiment_deviation": 0.50},
            {"company_id": "acme", "year": "2021", "sentiment_score": 0.20, "sentiment_deviation": 0.10},
            {"company_id": "acme", "year": "2022", "sentiment_score": 0.30, "sentiment_deviation": 0.20},
            {"company_id": "acme", "year": "2023", "sentiment_score": 0.40, "sentiment_deviation": 0.80},
            {"company_id": "acme", "year": "2024", "sentiment_score": 0.50, "sentiment_deviation": 0.40},
        ]
    )

    result = build_sentiment_signal(frame)

    assert result[["year", "deviation_rank", "signal"]].to_dict("records") == [
        {"year": "2020", "deviation_rank": 0.5, "signal": "neutral"},
        {"year": "2021", "deviation_rank": 0.0, "signal": "bullish"},
        {"year": "2022", "deviation_rank": 0.5, "signal": "neutral"},
        {"year": "2023", "deviation_rank": 1.0, "signal": "bearish"},
        {"year": "2024", "deviation_rank": 0.5, "signal": "neutral"},
    ]



def test_build_sentiment_signal_has_no_lookahead_bias() -> None:
    base_frame = pd.DataFrame(
        [
            {"company_id": "acme", "year": "2020", "sentiment_score": 0.10, "sentiment_deviation": 0.20},
            {"company_id": "acme", "year": "2021", "sentiment_score": 0.20, "sentiment_deviation": 0.40},
            {"company_id": "acme", "year": "2022", "sentiment_score": 0.30, "sentiment_deviation": 0.10},
        ]
    )
    future_frame = pd.concat(
        [
            base_frame,
            pd.DataFrame(
                [
                    {
                        "company_id": "acme",
                        "year": "2023",
                        "sentiment_score": 0.90,
                        "sentiment_deviation": 0.95,
                    }
                ]
            ),
        ],
        ignore_index=True,
    )

    base_result = build_sentiment_signal(base_frame)
    future_result = build_sentiment_signal(future_frame).iloc[: len(base_frame)].reset_index(drop=True)

    pd.testing.assert_frame_equal(base_result.reset_index(drop=True), future_result)



def test_build_sentiment_signal_returns_expected_schema_and_summary_stats() -> None:
    frame = pd.DataFrame(
        [
            {
                "company_id": "acme",
                "year": "2020",
                "sentiment_score": 0.10,
                "sentiment_deviation": 0.40,
                "next_year_return": 0.12,
            },
            {
                "company_id": "acme",
                "year": "2021",
                "sentiment_score": 0.20,
                "sentiment_deviation": 0.10,
                "next_year_return": 0.18,
            },
            {
                "company_id": "beta",
                "year": "2020",
                "sentiment_score": -0.10,
                "sentiment_deviation": 0.70,
                "next_year_return": -0.08,
            },
        ]
    )

    result = build_sentiment_signal(frame)

    assert result.columns.tolist() == [
        "company_id",
        "year",
        "sentiment_score",
        "sentiment_deviation",
        "sentiment_rank",
        "deviation_rank",
        "signal",
    ]

    summary = result.attrs["summary_stats"]
    assert summary.to_dict("records") == [
        {"signal": "bullish", "average_next_year_return": 0.18, "observation_count": 1},
        {"signal": "neutral", "average_next_year_return": 0.02, "observation_count": 2},
    ]

    direct_summary = summarize_signal_returns(
        pd.DataFrame(
            [
                {"signal": "bullish", "next_year_return": 0.18},
                {"signal": "neutral", "next_year_return": 0.12},
                {"signal": "neutral", "next_year_return": -0.08},
            ]
        )
    )
    assert direct_summary.to_dict("records") == summary.to_dict("records")
