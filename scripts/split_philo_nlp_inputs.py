from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

REQUIRED_COLUMNS = [
    "company_id",
    "company_name",
    "year",
    "document_type",
    "text",
    "sentiment_score",
    "sentiment_deviation",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Split a philo_nlp export into Berkshire-only and non-Berkshire CSVs, "
            "then print row counts and company coverage."
        )
    )
    parser.add_argument("--input-path", type=Path, default=Path("features/philo_nlp_input.csv"))
    parser.add_argument("--buffett-output", type=Path, default=Path("features/buffett_input.csv"))
    parser.add_argument(
        "--shareholder-output",
        type=Path,
        default=Path("features/shareholder_input.csv"),
    )
    return parser.parse_args()


def require_columns(frame: pd.DataFrame) -> None:
    missing = [column for column in REQUIRED_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(f"Input export is missing required columns: {missing}.")


def print_summary(label: str, frame: pd.DataFrame, output_path: Path) -> None:
    company_ids = sorted(frame["company_id"].astype(str).unique().tolist()) if not frame.empty else []
    print(f"{label}_rows: {len(frame)}")
    print(f"{label}_company_ids: {','.join(company_ids)}")
    print(f"{label}_output: {output_path}")


def run(input_path: Path, buffett_output: Path, shareholder_output: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    frame = pd.read_csv(input_path)
    require_columns(frame)
    frame = frame.loc[:, REQUIRED_COLUMNS].copy()

    buffett_frame = frame.loc[frame["company_id"] == "berkshire_hathaway"].copy()
    shareholder_frame = frame.loc[frame["company_id"] != "berkshire_hathaway"].copy()

    buffett_output.parent.mkdir(parents=True, exist_ok=True)
    shareholder_output.parent.mkdir(parents=True, exist_ok=True)
    buffett_frame.to_csv(buffett_output, index=False)
    shareholder_frame.to_csv(shareholder_output, index=False)

    print_summary("buffett", buffett_frame, buffett_output)
    print_summary("shareholder", shareholder_frame, shareholder_output)
    print_summary("combined", frame, input_path)
    return buffett_frame, shareholder_frame


def main() -> None:
    args = parse_args()
    run(
        input_path=args.input_path,
        buffett_output=args.buffett_output,
        shareholder_output=args.shareholder_output,
    )


if __name__ == "__main__":
    main()
