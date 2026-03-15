import argparse
import json
from pathlib import Path
from typing import Dict, List, Tuple

DEFAULT_OUTPUT_ROOT = Path("output")
DEFAULT_REPORT_PATH = Path("reports/dataset_summary.json")


def _extract_letter_records(output_root: Path) -> List[Tuple[str, int]]:
    """Return (company_id, year) tuples inferred from normalized PDF paths."""
    records: List[Tuple[str, int]] = []

    if not output_root.exists():
        return records

    for pdf_path in output_root.glob("*/*/*.pdf"):
        if not pdf_path.is_file():
            continue

        try:
            company_id = pdf_path.relative_to(output_root).parts[0]
        except ValueError:
            continue

        if company_id == "raw":
            continue

        stem = pdf_path.stem
        if not stem.isdigit():
            continue

        records.append((company_id, int(stem)))

    return records


def _compute_missing_years(years: List[int]) -> List[int]:
    if not years:
        return []

    start = min(years)
    end = max(years)
    observed = set(years)
    return [year for year in range(start, end + 1) if year not in observed]


def generate_summary(output_root: Path) -> Dict[str, object]:
    records = _extract_letter_records(output_root)

    letters_per_company: Dict[str, int] = {}
    all_years: List[int] = []

    for company_id, year in records:
        letters_per_company[company_id] = letters_per_company.get(company_id, 0) + 1
        all_years.append(year)

    if all_years:
        year_range: List[int] = [min(all_years), max(all_years)]
    else:
        year_range = []

    summary = {
        "number_of_companies": len(letters_per_company),
        "total_companies": len(letters_per_company),
        "letters_per_company": dict(sorted(letters_per_company.items())),
        "total_letters": len(records),
        "year_range": year_range,
        "missing_years": _compute_missing_years(all_years),
    }

    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate dataset summary statistics from downloaded letters.")
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_OUTPUT_ROOT,
        help=f"Directory containing normalized letter outputs (default: {DEFAULT_OUTPUT_ROOT}).",
    )
    parser.add_argument(
        "--report-path",
        type=Path,
        default=DEFAULT_REPORT_PATH,
        help=f"Path to write summary report JSON (default: {DEFAULT_REPORT_PATH}).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = generate_summary(args.output_root)

    args.report_path.parent.mkdir(parents=True, exist_ok=True)
    args.report_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print(f"Wrote dataset summary to {args.report_path}")


if __name__ == "__main__":
    main()
