from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Final

import pandas as pd

PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.features.sentiment_stability import DEFAULT_INPUT_PATH, DEFAULT_OUTPUT_PATH, run

LOGGER = logging.getLogger(__name__)
EXPECTED_COLUMNS: Final[list[str]] = [
    "company_id",
    "year",
    "sentiment_score",
    "sentiment_deviation",
    "sentiment_stability_score",
]

def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for the sentiment stability builder."""
    parser = argparse.ArgumentParser(
        description=(
            "Build sentiment stability features from features/sentiment_features.csv "
            "and write features/sentiment_stability.csv."
        )
    )
    parser.add_argument("--input-path", type=Path, default=DEFAULT_INPUT_PATH)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_OUTPUT_PATH)
    return parser.parse_args()

def configure_logging() -> None:
    """Configure basic CLI logging."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")

def validate_output(output_path: Path) -> pd.DataFrame:
    """Load and validate the generated output file, raising on obvious data issues."""
    frame = pd.read_csv(output_path, dtype={"company_id": "string", "year": "string"})

    if frame.empty:
        raise ValueError("Generated sentiment stability output is empty.")

    missing_columns = [column for column in EXPECTED_COLUMNS if column not in frame.columns]
    if missing_columns:
        raise ValueError(f"Generated sentiment stability output is missing columns: {missing_columns}.")

    frame = frame.loc[:, EXPECTED_COLUMNS]
    year_values = pd.to_numeric(frame["year"], errors="coerce")
    year_range = "unavailable"
    if year_values.notna().any():
        year_range = f"{int(year_values.min())} -> {int(year_values.max())}"

    print(f"row_count: {len(frame)}")
    print(f"year_range: {year_range}")
    print("null_counts:")
    print(frame.isna().sum())
    print("sample_rows:")
    print(frame.head())

    if frame.duplicated(subset=["company_id", "year"]).any():
        raise ValueError("Generated sentiment stability output contains duplicate company-year rows.")

    if (frame["sentiment_deviation"] < 0).fillna(False).any():
        raise ValueError("Found negative sentiment_deviation values, which are invalid.")

    company_aggregation = (
        frame.groupby("company_id", dropna=False)["sentiment_deviation"]
        .mean()
        .sort_index()
    )
    print("mean_deviation_by_company:")
    print(company_aggregation.head())

    if company_aggregation.dropna().empty:
        raise ValueError(
            "Generated sentiment stability output has no non-null deviation values to validate."
        )

    return frame

def main() -> None:
    """Build and validate sentiment stability features."""
    configure_logging()
    args = parse_args()
    run(input_path=args.input_path, output_path=args.output_path)
    validated = validate_output(args.output_path)
    LOGGER.info("Validated %s sentiment stability row(s) at %s.", len(validated), args.output_path)

if __name__ == "__main__":
    main()
