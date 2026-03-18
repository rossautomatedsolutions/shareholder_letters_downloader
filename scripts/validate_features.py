import csv
import sys
from pathlib import Path
from typing import Iterable

SENTIMENT_FEATURES_PATH = Path("features/sentiment_features.csv")
KEYWORD_FEATURES_PATH = Path("features/keyword_features.csv")


def load_rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def count_nulls(rows: Iterable[dict[str, str]], fieldnames: list[str]) -> dict[str, int]:
    null_counts = {fieldname: 0 for fieldname in fieldnames}
    for row in rows:
        for fieldname in fieldnames:
            value = row.get(fieldname)
            if value is None or not value.strip():
                null_counts[fieldname] += 1
    return null_counts


def count_duplicate_rows(rows: list[dict[str, str]], fieldnames: list[str]) -> int:
    seen: set[tuple[str, ...]] = set()
    duplicate_rows = 0
    for row in rows:
        row_key = tuple(row.get(fieldname, "") for fieldname in fieldnames)
        if row_key in seen:
            duplicate_rows += 1
            continue
        seen.add(row_key)
    return duplicate_rows


def summarize_years(rows: list[dict[str, str]]) -> tuple[str, list[int]]:
    years = sorted({int(row["year"]) for row in rows if row.get("year", "").strip()})
    if not years:
        return "N/A", []

    missing_years = [year for year in range(years[0], years[-1] + 1) if year not in set(years)]
    year_range = f"{years[0]} - {years[-1]}"
    return year_range, missing_years


def validate_dataset(label: str, path: Path) -> bool:
    if not path.exists():
        print(f"WARNING: {label} file not found at {path}.", file=sys.stderr)
        return True

    rows = load_rows(path)
    fieldnames = list(rows[0].keys()) if rows else []
    null_counts = count_nulls(rows, fieldnames)
    duplicate_rows = count_duplicate_rows(rows, fieldnames)
    year_range, missing_years = summarize_years(rows)

    print(f"{label} rows: {len(rows)}")
    print(f"{label} year range: {year_range}")
    print(f"{label} nulls: {null_counts}")
    print(f"{label} duplicate rows: {duplicate_rows}")

    has_warning = False
    if len(rows) == 0:
        print(f"WARNING: {label} has zero rows.", file=sys.stderr)
        has_warning = True
    if missing_years:
        print(f"WARNING: {label} is missing years: {missing_years}", file=sys.stderr)
        has_warning = True
    if duplicate_rows:
        print(f"WARNING: {label} contains {duplicate_rows} duplicate row(s).", file=sys.stderr)
        has_warning = True

    return has_warning


def main() -> None:
    has_warning = False
    has_warning |= validate_dataset("Sentiment", SENTIMENT_FEATURES_PATH)
    has_warning |= validate_dataset("Keyword", KEYWORD_FEATURES_PATH)
    raise SystemExit(1 if has_warning else 0)


if __name__ == "__main__":
    main()
