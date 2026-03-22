from __future__ import annotations

import math

import pytest

pd = pytest.importorskip("pandas")

from src.sentiment_pipeline.stability_analysis import (
    assign_stability_regime,
    build_analysis_df,
    compute_correlation,
    compute_regime_summary,
    compute_volatility_profile,
    get_execution_rules,
)


def _build_input_frames() -> tuple[pd.DataFrame, pd.DataFrame]:
    stability_df = pd.DataFrame(
        [
            {"company_id": "acme", "year": 2020, "sentiment_deviation": 0.10},
            {"company_id": "beta", "year": 2020, "sentiment_deviation": 0.20},
            {"company_id": "acme", "year": 2021, "sentiment_deviation": 0.30},
            {"company_id": "beta", "year": 2021, "sentiment_deviation": 0.40},
            {"company_id": "acme", "year": 2022, "sentiment_deviation": 0.50},
            {"company_id": "beta", "year": 2022, "sentiment_deviation": 0.60},
        ]
    )
    market_df = pd.DataFrame(
        [
            {"year": 2020, "next_year_return": 0.15},
            {"year": 2021, "next_year_return": -0.05},
            {"year": 2022, "next_year_return": 0.30},
        ]
    )
    return stability_df, market_df


def test_build_analysis_df_aggregates_to_one_row_per_year() -> None:
    stability_df, market_df = _build_input_frames()

    result = build_analysis_df(stability_df=stability_df, market_df=market_df)

    assert len(result) == 3
    assert result["year"].is_unique
    assert result.to_dict("records") == [
        {"year": 2020, "sentiment_deviation": 0.15, "next_year_return": 0.15},
        {"year": 2021, "sentiment_deviation": 0.35, "next_year_return": -0.05},
        {"year": 2022, "sentiment_deviation": 0.55, "next_year_return": 0.30},
    ]


def test_assign_stability_regime_produces_three_buckets() -> None:
    stability_df, market_df = _build_input_frames()
    analysis_df = build_analysis_df(stability_df=stability_df, market_df=market_df)

    result = assign_stability_regime(analysis_df)

    assert sorted(result["stability_regime"].astype(str).unique().tolist()) == [
        "neutral",
        "stable",
        "unstable",
    ]


def test_compute_regime_summary_is_not_empty() -> None:
    stability_df, market_df = _build_input_frames()
    analysis_df = assign_stability_regime(build_analysis_df(stability_df=stability_df, market_df=market_df))

    summary = compute_regime_summary(analysis_df)

    assert not summary.empty
    assert summary.loc["stable", "mean_return"] == pytest.approx(0.15)
    assert summary.loc["neutral", "negative_return_pct"] == pytest.approx(100.0)
    assert summary.loc["unstable", "above_20pct_return_pct"] == pytest.approx(100.0)


def test_compute_correlation_returns_float() -> None:
    stability_df, market_df = _build_input_frames()
    analysis_df = build_analysis_df(stability_df=stability_df, market_df=market_df)

    correlation = compute_correlation(analysis_df)

    assert isinstance(correlation, float)
    assert not math.isnan(correlation)


def test_execution_rules_structure_is_valid() -> None:
    rules = get_execution_rules()

    assert list(rules.columns) == [
        "stability_regime",
        "position_size_multiplier",
        "max_risk_per_trade",
        "preferred_strategy",
        "avoid_strategy",
    ]
    assert rules["stability_regime"].tolist() == ["stable", "neutral", "unstable"]


def test_compute_volatility_profile_is_not_empty() -> None:
    stability_df, market_df = _build_input_frames()
    analysis_df = assign_stability_regime(build_analysis_df(stability_df=stability_df, market_df=market_df))

    profile = compute_volatility_profile(analysis_df)

    assert not profile.empty
    assert profile["stability_regime"].tolist() == ["stable", "neutral", "unstable"]
