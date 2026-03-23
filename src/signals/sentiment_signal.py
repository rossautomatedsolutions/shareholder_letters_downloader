from __future__ import annotations

from typing import Final

import pandas as pd

REQUIRED_COLUMNS: Final[tuple[str, ...]] = (
    "company_id",
    "year",
    "sentiment_score",
    "sentiment_deviation",
)
OUTPUT_COLUMNS: Final[list[str]] = [
    "company_id",
    "year",
    "sentiment_score",
    "sentiment_deviation",
    "sentiment_rank",
    "deviation_rank",
    "signal",
]
SUMMARY_COLUMNS: Final[list[str]] = [
    "signal",
    "average_next_year_return",
    "observation_count",
]


def _validate_columns(df: pd.DataFrame, required_columns: tuple[str, ...] = REQUIRED_COLUMNS) -> None:
    missing_columns = [column for column in required_columns if column not in df.columns]
    if missing_columns:
        missing = ", ".join(missing_columns)
        raise ValueError(f"Missing required column(s): {missing}")



def _prepare_frame(df: pd.DataFrame) -> pd.DataFrame:
    _validate_columns(df)

    optional_columns = [column for column in ("next_year_return",) if column in df.columns]
    frame = df.loc[:, [*REQUIRED_COLUMNS, *optional_columns]].copy()
    frame["company_id"] = frame["company_id"].astype("string").str.strip()
    frame["year"] = frame["year"].astype("string").str.strip()
    frame["year_sort"] = pd.to_numeric(frame["year"], errors="coerce")
    frame["sentiment_score"] = pd.to_numeric(frame["sentiment_score"], errors="coerce")
    frame["sentiment_deviation"] = pd.to_numeric(frame["sentiment_deviation"], errors="coerce")

    aggregations: dict[str, str] = {
        "year_sort": "min",
        "sentiment_score": "median",
        "sentiment_deviation": "median",
    }
    if "next_year_return" in frame.columns:
        frame["next_year_return"] = pd.to_numeric(frame["next_year_return"], errors="coerce")
        aggregations["next_year_return"] = "median"

    prepared = (
        frame.groupby(["company_id", "year"], dropna=False, as_index=False)
        .agg(aggregations)
        .sort_values(
            by=["company_id", "year_sort", "year"],
            ascending=[True, True, True],
            na_position="last",
            kind="mergesort",
        )
        .reset_index(drop=True)
    )
    return prepared



def _expanding_percentile_rank(series: pd.Series) -> pd.Series:
    expanding = series.expanding()
    counts = expanding.count()
    ranks = expanding.rank(method="average")
    percentiles = (ranks - 1) / (counts - 1)
    percentiles = percentiles.where(counts > 1, 0.5)
    return percentiles.round(4)



def summarize_signal_returns(df: pd.DataFrame) -> pd.DataFrame:
    _validate_columns(df, required_columns=("signal", "next_year_return"))

    summary = (
        df.dropna(subset=["next_year_return"])
        .groupby("signal", dropna=False, as_index=False)
        .agg(
            average_next_year_return=("next_year_return", "mean"),
            observation_count=("next_year_return", "count"),
        )
        .sort_values("signal", kind="mergesort")
        .reset_index(drop=True)
    )
    return summary.loc[:, SUMMARY_COLUMNS]



def build_sentiment_signal(df: pd.DataFrame) -> pd.DataFrame:
    """Build an expanding, within-company sentiment signal without lookahead bias."""
    frame = _prepare_frame(df)

    grouped_scores = frame.groupby("company_id", sort=False)["sentiment_score"]
    grouped_deviation = frame.groupby("company_id", sort=False)["sentiment_deviation"]

    frame["sentiment_rank"] = grouped_scores.transform(_expanding_percentile_rank)
    frame["deviation_rank"] = grouped_deviation.transform(_expanding_percentile_rank)

    frame["signal"] = "neutral"
    frame.loc[frame["deviation_rank"] <= 0.30, "signal"] = "bullish"
    frame.loc[frame["deviation_rank"] >= 0.70, "signal"] = "bearish"
    frame.loc[frame["deviation_rank"].isna(), "signal"] = pd.NA

    output = frame.loc[:, OUTPUT_COLUMNS]
    if "next_year_return" in frame.columns:
        output.attrs["summary_stats"] = summarize_signal_returns(
            frame.loc[:, ["signal", "next_year_return"]]
        )

    return output
