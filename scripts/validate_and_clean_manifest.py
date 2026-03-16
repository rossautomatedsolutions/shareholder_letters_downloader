from pathlib import Path
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
]

ALLOWED_SOURCE_TYPES = {"PDF", "HTML"}
DEDUPLICATION_KEYS = ["company_id", "year"]


def _validate_required_columns(frame) -> None:
    missing = [column for column in REQUIRED_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")


def _normalize_row(row: dict) -> dict:
    normalized = dict(row)
    normalized["document_type"] = str(normalized["document_type"]).lower().strip()
    normalized["source_type"] = str(normalized["source_type"]).upper().strip()
    normalized["url"] = str(normalized["url"]).strip()
    normalized["company_id"] = str(normalized["company_id"]).strip()
    normalized["company_name"] = str(normalized["company_name"]).strip()

    if "letter" in normalized["document_type"]:
        normalized["document_type"] = "shareholder_letter"

    return normalized


def _row_rejection_reason(row: dict, current_year_plus_one: int) -> str:
    try:
        row["year"] = int(row["year"])
    except Exception:
        return "invalid_year"

    if row["source_type"] not in ALLOWED_SOURCE_TYPES:
        return "invalid_source_type"

    if not str(row["url"]).startswith("http"):
        return "invalid_url"

    if not (1900 <= row["year"] <= current_year_plus_one):
        return "invalid_year_range"

    return ""


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

    accepted_rows: List[dict] = []
    rejected_rows: List[dict] = []

    for raw_row in frame.to_dict(orient="records"):
        row = _normalize_row(raw_row)
        rejection_reason = _row_rejection_reason(row, current_year_plus_one)

        if rejection_reason:
            row["rejection_reason"] = rejection_reason
            rejected_rows.append(row)
            continue

        accepted_rows.append(row)

    valid_rows = pd.DataFrame(accepted_rows)
    rejected_df = pd.DataFrame(rejected_rows)

    if valid_rows.empty:
        deduped_valid_rows = valid_rows
        duplicate_rows = pd.DataFrame(columns=REQUIRED_COLUMNS + ["rejection_reason"])
    else:
        deduped_valid_rows = valid_rows.drop_duplicates(subset=DEDUPLICATION_KEYS, keep="first")
        duplicate_rows = valid_rows[
            valid_rows.duplicated(subset=DEDUPLICATION_KEYS, keep="first")
        ].copy()
        if not duplicate_rows.empty:
            duplicate_rows["rejection_reason"] = "duplicate_company_year"

    duplicate_rows_removed = len(valid_rows) - len(deduped_valid_rows)

    all_rejected = pd.concat([rejected_df, duplicate_rows], ignore_index=True)

    clean_output_path.parent.mkdir(parents=True, exist_ok=True)
    rejected_output_path.parent.mkdir(parents=True, exist_ok=True)

    deduped_valid_rows.to_csv(clean_output_path, index=False)
    all_rejected.to_csv(rejected_output_path, index=False)

    summary = {
        "rows_scanned": rows_scanned,
        "rows_accepted": len(deduped_valid_rows),
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
