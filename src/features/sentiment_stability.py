from __future__ import annotations

import logging
from pathlib import Path
from typing import Final

import pandas as pd

LOGGER = logging.getLogger(__name__)

REQUIRED_COLUMNS: Final[tuple[str, ...]] = ("company_id", "year", "sentiment_score")
OUTPUT_COLUMNS: Final[list[str]] = [
    "company_id",
    "year",
    "sentiment_score",
    "sentiment_deviation",
    "sentiment_stability_score",
]
DEFAULT_INPUT_PATH: Final[Path] = Path("features/sentiment_features.csv")
DEFAULT_OUTPUT_PATH: Final[Path] = Path("features/sentiment_stability.csv")


def _prepare_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Validate and normalize the sentiment input frame."""
    missing_columns = [column for column in REQUIRED_COLUMNS if column not in df.columns]
    if missing_columns:
        missing = ", ".join(missing_columns)
        raise ValueError(f"Missing required column(s): {missing}")

    frame = df.loc[:, list(REQUIRED_COLUMNS)].copy()
    frame = frame.drop_duplicates(ignore_index=True)
    frame["company_id"] = frame["company_id"].astype("string")
    frame["year"] = frame["year"].astype("string")
    frame["sentiment_score"] = pd.to_numeric(frame["sentiment_score"], errors="coerce")
    frame["year_sort"] = pd.to_numeric(frame["year"], errors="coerce")
    frame = frame.sort_values(
        by=["company_id", "year_sort", "year"],
        ascending=[True, True, True],
        na_position="last",
        kind="mergesort",
    ).reset_index(drop=True)
    return frame


def build_sentiment_stability(df: pd.DataFrame) -> pd.DataFrame:
    """Build sentiment stability features using an expanding historical median per company."""
    frame = _prepare_frame(df)

    frame["expanding_median"] = frame.groupby("company_id", sort=False)["sentiment_score"].transform(
        lambda series: series.expanding().median()
    )
    frame["sentiment_deviation"] = (frame["sentiment_score"] - frame["expanding_median"]).abs()
    frame["sentiment_stability_score"] = -frame["sentiment_deviation"]

    return frame.loc[:, OUTPUT_COLUMNS]


def log_frame_stats(frame: pd.DataFrame, label: str) -> None:
    """Log basic row, year, and null-count statistics for a feature frame."""
    year_values = pd.to_numeric(frame["year"], errors="coerce")
    if year_values.notna().any():
        year_range = f"{int(year_values.min())}-{int(year_values.max())}"
    else:
        year_range = "unavailable"

    null_counts = frame.isna().sum().to_dict()
    LOGGER.info("%s rows: %s", label, len(frame))
    LOGGER.info("%s year range: %s", label, year_range)
    LOGGER.info("%s null counts: %s", label, null_counts)


def run(input_path: Path = DEFAULT_INPUT_PATH, output_path: Path = DEFAULT_OUTPUT_PATH) -> pd.DataFrame:
    """Read sentiment features, build sentiment stability factors, and overwrite the output file."""
    frame = pd.read_csv(input_path, dtype={"company_id": "string", "year": "string"})
    log_frame_stats(frame, label="Input")

    stability = build_sentiment_stability(frame)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    stability.to_csv(output_path, index=False)

    log_frame_stats(stability, label="Output")
    LOGGER.info("Rows written: %s", len(stability))
    return stability
