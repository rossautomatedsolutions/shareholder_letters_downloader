from pathlib import Path
from urllib.parse import urlparse
from datetime import datetime
from typing import Dict, List

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
    "confidence_score",
]

ALLOWED_SOURCE_TYPES = {"PDF"}
INVALID_URL_KEYWORDS = ("10k", "10-k", "proxy", "earnings", "presentation", "transcript")
DEDUPLICATION_KEYS = ["company_id", "document_type", "year"]


def _is_valid_http_url(url: str) -> bool:
    parsed = urlparse(str(url).strip())
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _validate_required_columns(frame) -> None:
    missing = [column for column in REQUIRED_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")


def _normalize_row(row: dict) -> dict:
    normalized = {key: str(value).strip() for key, value in dict(row).items()}
    normalized["company_id"] = normalized.get("company_id", "").lower().strip()
    normalized["company_name"] = normalized.get("company_name", "").strip()
    normalized["document_type"] = normalized.get("document_type", "").lower().strip()
    normalized["source_type"] = normalized.get("source_type", "").upper().strip()
    normalized["url"] = normalized.get("url", "").strip()
    normalized["confidence_score"] = normalized.get("confidence_score", "").strip()
    normalized["year"] = normalized.get("year", "").strip()

    if normalized["document_type"] in {"shareholder letter", "shareholder-letter"}:
        normalized["document_type"] = "shareholder_letter"

    return normalized


def _has_valid_url_pattern(url: str) -> bool:
    lowered_url = str(url).strip().lower()
    return not any(keyword in lowered_url for keyword in INVALID_URL_KEYWORDS)


def _row_rejection_reasons(row: dict, current_year_plus_one: int) -> List[str]:
    year = str(row["year"]).strip()

    try:
        confidence_score = float(row["confidence_score"])
    except Exception:
        confidence_score = None

    if not year.isdigit():
        return ["invalid_year"]
    elif row["document_type"] != "shareholder_letter":
        return ["invalid_document_type"]
    elif row["source_type"] != "PDF":
        return ["invalid_source_type"]
    elif not _is_valid_http_url(row["url"]):
        return ["invalid_url_scheme"]
    elif "presentation" in str(row["url"]).lower():
        return ["invalid_url_pattern"]
    elif confidence_score is None:
        return ["invalid_confidence_score"]
    elif confidence_score < 0.5:
        return ["low_confidence_score"]

    year_int = int(year)
    if not (1900 <= year_int <= current_year_plus_one):
        return ["invalid_year_range"]
    elif not _has_valid_url_pattern(row["url"]):
        return ["invalid_url_pattern"]

    return []


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

    rows_scanned = len(frame)
    current_year_plus_one = datetime.now().year + 1

    normalized_rows = [_normalize_row(raw_row) for raw_row in frame.to_dict(orient="records")]
    valid_rows: List[dict] = []
    rejected_rows: List[dict] = []

    for row in normalized_rows:
        rejection_reasons = _row_rejection_reasons(row, current_year_plus_one)

        if rejection_reasons:
            for rejection_reason in rejection_reasons:
                rejected_rows.append({**row, "rejection_reason": rejection_reason})
            continue

        valid_rows.append(row)

    valid_df = pd.DataFrame(valid_rows)
    rejected_df = pd.DataFrame(rejected_rows)

    if valid_df.empty:
        duplicate_rows = pd.DataFrame(columns=REQUIRED_COLUMNS + ["rejection_reason"])
    else:
        duplicate_rows = valid_df[
            valid_df.duplicated(subset=DEDUPLICATION_KEYS, keep="first")
        ].copy()

    before_dedup = len(valid_df)

    valid_df = valid_df.drop_duplicates(
        subset=["company_id", "document_type", "year"]
    )

    after_dedup = len(valid_df)

    duplicates_removed = before_dedup - after_dedup

    rows_accepted = after_dedup

    if not duplicate_rows.empty:
        duplicate_rows["rejection_reason"] = "duplicate_company_year"
    duplicate_rows_removed = duplicates_removed

    all_rejected = pd.concat([rejected_df, duplicate_rows], ignore_index=True)

    clean_output_path.parent.mkdir(parents=True, exist_ok=True)
    rejected_output_path.parent.mkdir(parents=True, exist_ok=True)

    valid_df.to_csv(clean_output_path, index=False)
    all_rejected.to_csv(rejected_output_path, index=False)

    summary = {
        "rows_scanned": rows_scanned,
        "rows_accepted": rows_accepted,
        "rows_rejected": len(all_rejected),
        "duplicate_rows_removed": duplicate_rows_removed,
    }

    print(f"Rows scanned: {summary['rows_scanned']}")
    print(f"Rows accepted: {summary['rows_accepted']}")
    print(f"Rows rejected: {summary['rows_rejected']}")
    print(f"Duplicates removed: {summary['duplicate_rows_removed']}")

    return summary


def main() -> None:
    validate_and_clean_manifest()


if __name__ == "__main__":
    main()
