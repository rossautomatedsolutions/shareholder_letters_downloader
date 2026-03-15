from pathlib import Path
from typing import Dict
from urllib.parse import urlparse

try:
    import pandas as pd
except ModuleNotFoundError:  # pragma: no cover - handled at runtime
    pd = None

INPUT_MANIFEST = Path("manifests/letters_manifest.csv")
CLEAN_OUTPUT = Path("manifests/letters_manifest.cleaned.csv")
REJECTED_OUTPUT = Path("reports/rejected_manifest_rows.csv")

REQUIRED_COLUMNS = [
    "company_id",
    "company_name",
    "document_type",
    "year",
    "source_type",
    "url",
]

ALLOWED_SOURCE_TYPES = {"PDF", "HTML"}
EXPECTED_DOCUMENT_TYPE = "shareholder_letter"
INVALID_URL_SUBSTRINGS = (
    "proxy",
    "corporate-data",
    "financial-statements",
    "earnings-presentation",
)
DEDUPLICATION_KEYS = ["company_id", "year"]


def _normalize_text(value: str) -> str:
    return str(value).strip()


def _row_rejection_reason(row) -> str:
    year = _normalize_text(row["year"])
    if not year.isdigit():
        return "invalid_year"

    source_type = _normalize_text(row["source_type"])
    if source_type not in ALLOWED_SOURCE_TYPES:
        return "invalid_source_type"

    document_type = _normalize_text(row["document_type"])
    if document_type != EXPECTED_DOCUMENT_TYPE:
        return "invalid_document_type"

    url = _normalize_text(row["url"])
    parsed_url = urlparse(url)
    if parsed_url.scheme.lower() not in {"http", "https"} or not parsed_url.netloc:
        return "invalid_url_scheme"

    url = url.lower()

    if any(pattern in url for pattern in INVALID_URL_SUBSTRINGS):
        return "invalid_url_pattern"

    return ""


def _validate_required_columns(frame) -> None:
    missing = [column for column in REQUIRED_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")


def validate_and_clean_manifest(
    input_path: Path = INPUT_MANIFEST,
    clean_output_path: Path = CLEAN_OUTPUT,
    rejected_output_path: Path = REJECTED_OUTPUT,
) -> Dict[str, int]:
    if pd is None:
        raise ModuleNotFoundError(
            "pandas is required to validate and clean manifests. Install dependencies with `pip install pandas`."
        )

    frame = pd.read_csv(input_path, dtype=str, keep_default_na=False)
    _validate_required_columns(frame)

    normalized_frame = frame.copy()
    for column in normalized_frame.columns:
        normalized_frame[column] = normalized_frame[column].map(_normalize_text)

    annotated = normalized_frame.copy()
    annotated["rejection_reason"] = annotated.apply(_row_rejection_reason, axis=1)

    valid_rows = annotated[annotated["rejection_reason"] == ""].drop(columns=["rejection_reason"])
    rejected_rows = annotated[annotated["rejection_reason"] != ""]

    deduped_valid_rows = valid_rows.drop_duplicates(subset=DEDUPLICATION_KEYS, keep="first")
    duplicate_rows_removed = len(valid_rows) - len(deduped_valid_rows)

    duplicate_rows = valid_rows[valid_rows.duplicated(subset=DEDUPLICATION_KEYS, keep="first")].copy()
    if not duplicate_rows.empty:
        duplicate_rows["rejection_reason"] = "duplicate_company_year"

    all_rejected = pd.concat([rejected_rows, duplicate_rows], ignore_index=True)

    clean_output_path.parent.mkdir(parents=True, exist_ok=True)
    rejected_output_path.parent.mkdir(parents=True, exist_ok=True)

    deduped_valid_rows.to_csv(clean_output_path, index=False)
    all_rejected.to_csv(rejected_output_path, index=False)

    summary = {
        "rows_scanned": len(frame),
        "rows_accepted": len(deduped_valid_rows),
        "rows_rejected": len(all_rejected),
        "duplicate_rows_removed": duplicate_rows_removed,
    }

    print(f"Rows scanned: {summary['rows_scanned']}")
    print(f"Rows accepted: {summary['rows_accepted']}")
    print(f"Rows rejected: {summary['rows_rejected']}")
    print(f"Duplicate rows removed: {summary['duplicate_rows_removed']}")

    return summary


def main() -> None:
    validate_and_clean_manifest()


if __name__ == "__main__":
    main()
