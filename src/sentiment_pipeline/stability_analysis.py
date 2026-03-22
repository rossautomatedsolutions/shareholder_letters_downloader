from __future__ import annotations

from typing import Final

import pandas as pd

_ANALYSIS_REQUIRED_COLUMNS: Final[tuple[str, ...]] = (
    "year",
    "sentiment_deviation",
)
_MARKET_REQUIRED_COLUMNS: Final[tuple[str, ...]] = (
    "year",
    "next_year_return",
)
_REGIME_ORDER: Final[list[str]] = ["stable", "neutral", "unstable"]


def _validate_columns(df: pd.DataFrame, required_columns: tuple[str, ...], name: str) -> None:
    """Raise a clear error when required columns are missing from a dataframe."""
    missing_columns = [column for column in required_columns if column not in df.columns]
    if missing_columns:
        missing = ", ".join(missing_columns)
        raise ValueError(f"Missing required column(s) in {name}: {missing}")


def build_analysis_df(stability_df: pd.DataFrame, market_df: pd.DataFrame) -> pd.DataFrame:
    """Merge yearly stability and market data into a clean one-row-per-year analysis frame."""
    _validate_columns(stability_df, _ANALYSIS_REQUIRED_COLUMNS, "stability_df")
    _validate_columns(market_df, _MARKET_REQUIRED_COLUMNS, "market_df")

    stability = stability_df.loc[:, list(_ANALYSIS_REQUIRED_COLUMNS)].copy()
    market = market_df.loc[:, list(_MARKET_REQUIRED_COLUMNS)].copy()

    stability["year"] = pd.to_numeric(stability["year"], errors="coerce").astype("Int64")
    market["year"] = pd.to_numeric(market["year"], errors="coerce").astype("Int64")
    stability["sentiment_deviation"] = pd.to_numeric(stability["sentiment_deviation"], errors="coerce")
    market["next_year_return"] = pd.to_numeric(market["next_year_return"], errors="coerce")

    analysis_df = (
        stability.merge(market, on="year", how="left", validate="many_to_one")
        .dropna(subset=["year", "sentiment_deviation", "next_year_return"])
        .groupby("year", as_index=False)
        .agg(
            sentiment_deviation=("sentiment_deviation", "mean"),
            next_year_return=("next_year_return", "first"),
        )
        .sort_values("year", kind="mergesort")
        .reset_index(drop=True)
    )

    return analysis_df


def assign_stability_regime(df: pd.DataFrame) -> pd.DataFrame:
    """Assign quantile-based stability regimes from sentiment deviation values."""
    _validate_columns(df, ("sentiment_deviation",), "df")

    result = df.copy()
    result["stability_regime"] = pd.qcut(
        result["sentiment_deviation"],
        q=3,
        labels=_REGIME_ORDER,
        duplicates="drop",
    )
    return result


def compute_regime_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Compute grouped return statistics for each stability regime."""
    _validate_columns(df, ("stability_regime", "next_year_return"), "df")

    summary = (
        df.assign(
            is_negative=df["next_year_return"] < 0,
            is_above_20=df["next_year_return"] > 0.20,
        )
        .groupby("stability_regime", observed=False)
        .agg(
            observations=("next_year_return", "size"),
            mean_return=("next_year_return", "mean"),
            median_return=("next_year_return", "median"),
            std_return=("next_year_return", "std"),
            negative_return_pct=("is_negative", "mean"),
            above_20pct_return_pct=("is_above_20", "mean"),
        )
        .reindex(_REGIME_ORDER)
    )

    percentage_columns = ["negative_return_pct", "above_20pct_return_pct"]
    summary[percentage_columns] = summary[percentage_columns].mul(100)
    return summary


def compute_volatility_profile(df: pd.DataFrame) -> pd.DataFrame:
    """Compute absolute-return and volatility metrics by stability regime."""
    _validate_columns(df, ("stability_regime", "next_year_return"), "df")

    profile = (
        df.assign(absolute_return=df["next_year_return"].abs())
        .groupby("stability_regime", observed=False)
        .agg(
            observations=("next_year_return", "size"),
            average_absolute_return=("absolute_return", "mean"),
            return_std_dev=("next_year_return", "std"),
        )
        .reindex(_REGIME_ORDER)
        .reset_index()
    )
    return profile


def get_execution_rules() -> pd.DataFrame:
    """Return the static execution rule table for each stability regime."""
    return pd.DataFrame(
        {
            "stability_regime": _REGIME_ORDER,
            "position_size_multiplier": [1.25, 1.0, 0.5],
            "max_risk_per_trade": ["higher", "standard", "low"],
            "preferred_strategy": [
                "credit spreads / directional trades",
                "balanced spreads",
                "defined-risk / hedged trades",
            ],
            "avoid_strategy": [
                "over-hedging",
                "over-leverage",
                "naked exposure",
            ],
        }
    )


def compute_correlation(df: pd.DataFrame) -> float:
    """Return the correlation between sentiment deviation and next-year returns."""
    _validate_columns(df, ("sentiment_deviation", "next_year_return"), "df")
    return float(df["sentiment_deviation"].corr(df["next_year_return"]))
