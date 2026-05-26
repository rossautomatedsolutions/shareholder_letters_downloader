import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Iterable

BAD_PHRASES = (
    "access denied",
    "not found",
    "page not found",
    "403 forbidden",
    "temporarily unavailable",
    "redirecting",
    "enable javascript",
    "request unsuccessful",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run a lightweight quality gate over extracted text files stored at "
            "output_text/<company_id>/<year>.txt and emit a CSV report."
        )
    )
    parser.add_argument("--input-root", type=Path, default=Path("output_text"))
    parser.add_argument("--output-path", type=Path, default=Path("reports/text_quality_report.csv"))
    parser.add_argument("--min-char-count", type=int, default=5000)
    parser.add_argument("--min-word-count", type=int, default=1000)
    parser.add_argument(
        "--companies",
        nargs="*",
        default=(),
        help="Optional list of company_id values to include.",
    )
    return parser.parse_args()


def iter_text_paths(input_root: Path, companies: Iterable[str]) -> list[Path]:
    company_filter = {company for company in companies if company}
    text_paths = sorted(input_root.glob("*/*.txt"))
    if not company_filter:
        return text_paths
    return [path for path in text_paths if path.parent.name in company_filter]


def evaluate_text(
    text_path: Path,
    min_char_count: int,
    min_word_count: int,
) -> dict[str, str]:
    text = text_path.read_text(encoding="utf-8")
    lowered = text.lower()
    words = text.split()
    failed_checks: list[str] = []

    if len(text) < min_char_count:
        failed_checks.append("char_count_below_threshold")
    if len(words) < min_word_count:
        failed_checks.append("word_count_below_threshold")

    matched_phrases = [phrase for phrase in BAD_PHRASES if phrase in lowered]
    if matched_phrases:
        failed_checks.append("bad_phrase_detected")

    metadata_path = text_path.with_suffix(".json")
    document_type = ""
    year = text_path.stem
    if metadata_path.exists():
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        document_type = str(metadata.get("document_type", ""))
        year = str(metadata.get("year", year))

    return {
        "company_id": text_path.parent.name,
        "year": year,
        "document_type": document_type,
        "char_count": str(len(text)),
        "word_count": str(len(words)),
        "matched_bad_phrases": "|".join(matched_phrases),
        "status": "pass" if not failed_checks else "fail",
        "failed_checks": "|".join(failed_checks),
        "path": str(text_path),
    }


def write_report(rows: list[dict[str, str]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "company_id",
        "year",
        "document_type",
        "char_count",
        "word_count",
        "matched_bad_phrases",
        "status",
        "failed_checks",
        "path",
    ]
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def run(
    input_root: Path,
    output_path: Path,
    min_char_count: int,
    min_word_count: int,
    companies: Iterable[str] = (),
) -> tuple[list[dict[str, str]], int]:
    rows = [
        evaluate_text(
            text_path=path,
            min_char_count=min_char_count,
            min_word_count=min_word_count,
        )
        for path in iter_text_paths(input_root=input_root, companies=companies)
    ]
    write_report(rows=rows, output_path=output_path)
    failed_rows = [row for row in rows if row["status"] != "pass"]
    return rows, len(failed_rows)


def main() -> None:
    args = parse_args()
    rows, failed_count = run(
        input_root=args.input_root,
        output_path=args.output_path,
        min_char_count=args.min_char_count,
        min_word_count=args.min_word_count,
        companies=args.companies,
    )
    print(
        f"Reviewed {len(rows)} extracted text file(s); "
        f"{len(rows) - failed_count} passed and {failed_count} failed."
    )
    print(f"Report written to {args.output_path}")
    raise SystemExit(1 if failed_count else 0)


if __name__ == "__main__":
    main()
