from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Final

import pandas as pd
import yfinance as yf

LOGGER = logging.getLogger(__name__)
DEFAULT_REFERENCE_PATH: Final[Path] = Path("features/sentiment_stability.csv")
DEFAULT_OUTPUT_PATH: Final[Path] = Path("features/market_returns.csv")
DEFAULT_TICKER: Final[str] = "SPY"
EXPECTED_COLUMNS: Final[list[str]] = ["year", "ticker", "close", "next_year_return"]

def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for the market return builder."""
    parser = argparse.ArgumentParser(
        description=(
            "Download annual market close prices with yfinance and compute next-year returns "
            "using Close prices."
        )
    )
    parser.add_argument("--reference-path", type=Path, default=DEFAULT_REFERENCE_PATH)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--ticker", default=DEFAULT_TICKER)
    return parser.parse_args()

def configure_logging() -> None:
    """Configure basic CLI logging."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")

def _log_frame_stats(frame: pd.DataFrame, label: str) -> None:
    """Log row counts, year range, and null counts for a frame."""
    year_range = "unavailable"
    if "year" in frame.columns:
        year_values = pd.to_numeric(frame["year"], errors="coerce")
        if year_values.notna().any():
            year_range = f"{int(year_values.min())}-{int(year_values.max())}"

    LOGGER.info("%s rows: %s", label, len(frame))
    LOGGER.info("%s year range: %s", label, year_range)
    LOGGER.info("%s null counts: %s", label, frame.isna().sum().to_dict())

def load_reference_years(reference_path: Path) -> pd.DataFrame:
    """Load the reference feature file used to anchor the market return year range."""
    frame = pd.read_csv(reference_path, dtype={"year": "string"})
    if frame.empty:
        raise ValueError(f"Reference feature file is empty: {reference_path}")
    if "year" not in frame.columns:
        raise ValueError(f"Reference feature file is missing required column 'year': {reference_path}")

    _log_frame_stats(frame, label="Reference input")
    reference_years = pd.to_numeric(frame["year"], errors="coerce").dropna().astype(int)
    if reference_years.empty:
        raise ValueError(f"Reference feature file does not contain any numeric years: {reference_path}")

    return pd.DataFrame({"year": sorted(reference_years.unique())})

def download_market_returns(ticker: str, years: pd.Series) -> pd.DataFrame:
    """Download annual close prices and compute forward returns for the requested year range."""
    start_year = int(years.min())
    end_year = int(years.max())
    start_date = f"{start_year}-01-01"
    end_date = f"{end_year + 2}-01-01"

    price_history = yf.download(
        ticker,
        start=start_date,
        end=end_date,
        auto_adjust=False,
        progress=False,
    )
    if price_history.empty:
        raise ValueError(f"No market data returned for ticker {ticker!r}.")

    if isinstance(price_history.columns, pd.MultiIndex):
        price_history.columns = price_history.columns.get_level_values(0)

    if "Close" not in price_history.columns:
        raise ValueError(f"Downloaded data for ticker {ticker!r} does not include a Close column.")

    annual_close = (
        price_history.loc[:, ["Close"]]
        .rename(columns={"Close": "close"})
        .assign(year=lambda frame: frame.index.year)
        .groupby("year", as_index=False, sort=True)["close"]
        .last()
    )
    annual_close["next_year_return"] = annual_close["close"].shift(-1) / annual_close["close"] - 1.0
    annual_close["ticker"] = ticker.upper()

    market_returns = (
        annual_close.merge(pd.DataFrame({"year": years.astype(int)}), on="year", how="inner")
        .sort_values("year", kind="mergesort")
        .drop_duplicates(subset=["year"], keep="first")
        .loc[:, EXPECTED_COLUMNS]
        .reset_index(drop=True)
    )
    return market_returns

def write_market_returns(output_path: Path, market_returns: pd.DataFrame) -> None:
    """Write the market return output, overwriting any existing file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    market_returns.to_csv(output_path, index=False)

def validate_output(output_path: Path) -> pd.DataFrame:
    """Load and validate the generated market return output."""
    frame = pd.read_csv(output_path)

    if frame.empty:
        raise ValueError("Generated market return output is empty.")

    missing_columns = [column for column in EXPECTED_COLUMNS if column not in frame.columns]
    if missing_columns:
        raise ValueError(f"Generated market return output is missing columns: {missing_columns}.")

    frame = frame.loc[:, EXPECTED_COLUMNS]

    print(f"row_count: {len(frame)}")
    print(f"year_range: {int(frame['year'].min())} -> {int(frame['year'].max())}")
    print("null_counts:")
    print(frame.isna().sum())
    print("sample_rows:")
    print(frame.head())

    if frame.duplicated(subset=["year"]).any():
        raise ValueError("Generated market return output contains duplicate years.")

    if frame["next_year_return"].dropna().empty:
        raise ValueError("Generated market return output has no non-null forward returns to validate.")

    print("return_summary:")
    print(frame["next_year_return"].agg(["count", "mean", "median", "min", "max"]))

    return frame

def main() -> None:
    """Build and validate forward market returns."""
    configure_logging()
    args = parse_args()
    reference_years = load_reference_years(args.reference_path)
    market_returns = download_market_returns(ticker=args.ticker, years=reference_years["year"])
    _log_frame_stats(market_returns, label="Output")
    LOGGER.info("Rows written: %s", len(market_returns))
    write_market_returns(args.output_path, market_returns)
    validated = validate_output(args.output_path)
    LOGGER.info("Validated %s market return row(s) at %s.", len(validated), args.output_path)

if __name__ == "__main__":
    main()
