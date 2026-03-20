from __future__ import annotations

from pathlib import Path

try:
    import pandas as pd
except ModuleNotFoundError:  # pragma: no cover - handled at runtime
    pd = None

REQUIRED_COLUMNS = ("company_id", "year", "sentiment_score")
DEFAULT_INPUT_PATH = Path("features/sentiment_features.csv")
DEFAULT_OUTPUT_PATH = Path("features/sentiment_stability.csv")
OUTPUT_COLUMNS = [
    "company_id",
    "year",
    "sentiment_score",
    "sentiment_deviation",
    "sentiment_stability_score",
]


def _require_pandas():
    if pd is None:
        raise ModuleNotFoundError(
            "pandas is required to build sentiment stability features. Install dependencies with `pip install pandas`."
        )


def build_sentiment_stability(df):
    """Build per-company sentiment stability features without look-ahead across future years."""
    _require_pandas()

    missing_columns = [column for column in REQUIRED_COLUMNS if column not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required column(s): {', '.join(missing_columns)}")

    frame = df.loc[:, list(REQUIRED_COLUMNS)].copy()
    frame["company_id"] = frame["company_id"].astype(str)
    frame["year"] = frame["year"].astype(str)
    frame["sentiment_score"] = pd.to_numeric(frame["sentiment_score"], errors="raise")
    frame["year_sort"] = pd.to_numeric(frame["year"], errors="coerce")
    frame = frame.sort_values(
        by=["company_id", "year_sort", "year"],
        ascending=[True, True, True],
        na_position="last",
        kind="mergesort",
    ).reset_index(drop=True)

    frame["expanding_median"] = frame.groupby("company_id", sort=False)["sentiment_score"].transform(
        lambda series: series.expanding().median()
    )
    frame["sentiment_deviation"] = (frame["sentiment_score"] - frame["expanding_median"]).abs()
    frame["sentiment_stability_score"] = -frame["sentiment_deviation"]

    return frame.loc[:, OUTPUT_COLUMNS]


def run(input_path: Path = DEFAULT_INPUT_PATH, output_path: Path = DEFAULT_OUTPUT_PATH):
    _require_pandas()

    frame = pd.read_csv(input_path, dtype={"company_id": str, "year": str})
    stability = build_sentiment_stability(frame)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    stability.to_csv(output_path, index=False, float_format="%.6f")
    return stability
