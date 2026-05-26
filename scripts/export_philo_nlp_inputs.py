from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Final

import pandas as pd

DEFAULT_TEXT_ROOT: Final[Path] = Path("output_text")
DEFAULT_SENTIMENT_FEATURES: Final[Path] = Path("features/sentiment_features.csv")
DEFAULT_SENTIMENT_STABILITY: Final[Path] = Path("features/sentiment_stability.csv")
DEFAULT_MANIFEST_PATH: Final[Path] = Path("manifests/letters_manifest.cleaned.csv")
DEFAULT_OUTPUT_PATH: Final[Path] = Path("features/philo_nlp_input.csv")

OUTPUT_COLUMNS: Final[list[str]] = [
    "company_id",
    "company_name",
    "year",
    "document_type",
    "text",
    "sentiment_score",
    "sentiment_deviation",
]
FEATURE_COLUMNS: Final[list[str]] = ["company_id", "year", "sentiment_score"]
STABILITY_COLUMNS: Final[list[str]] = ["company_id", "year", "sentiment_deviation"]
MANIFEST_COLUMNS: Final[list[str]] = ["company_id", "company_name", "year", "document_type"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Export normalized shareholder-letter inputs for downstream philo_nlp ingestion "
            "by combining extracted text with sentiment feature tables."
        )
    )
    parser.add_argument("--text-root", type=Path, default=DEFAULT_TEXT_ROOT)
    parser.add_argument("--sentiment-features", type=Path, default=DEFAULT_SENTIMENT_FEATURES)
    parser.add_argument("--sentiment-stability", type=Path, default=DEFAULT_SENTIMENT_STABILITY)
    parser.add_argument(
        "--manifest-path",
        type=Path,
        default=DEFAULT_MANIFEST_PATH,
        help="Optional manifest used to resolve canonical company_name/document_type metadata.",
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    return parser.parse_args()


def normalize_year(value: object) -> str:
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text


def humanize_company_id(company_id: str) -> str:
    return company_id.replace("_", " ").title()


def require_columns(frame: pd.DataFrame, required_columns: list[str], source_name: str) -> None:
    missing_columns = [column for column in required_columns if column not in frame.columns]
    if missing_columns:
        raise ValueError(f"{source_name} is missing required columns: {missing_columns}.")


def raise_on_duplicate_company_year(frame: pd.DataFrame, source_name: str) -> None:
    duplicate_mask = frame.duplicated(subset=["company_id", "year"], keep=False)
    if not duplicate_mask.any():
        return

    duplicate_rows = (
        frame.loc[duplicate_mask, ["company_id", "year"]]
        .astype("string")
        .drop_duplicates()
        .sort_values(["company_id", "year"], kind="stable")
    )
    duplicate_labels = ", ".join(
        f"{row.company_id}/{row.year}" for row in duplicate_rows.itertuples(index=False)
    )
    raise ValueError(
        f"{source_name} contains duplicate company/year rows: {duplicate_labels}."
    )


def load_feature_frame(path: Path, required_columns: list[str], source_name: str) -> pd.DataFrame:
    frame = pd.read_csv(path)
    require_columns(frame, required_columns, source_name)
    frame = frame.loc[:, required_columns].copy()
    frame["company_id"] = frame["company_id"].astype("string")
    frame["year"] = frame["year"].map(normalize_year).astype("string")
    raise_on_duplicate_company_year(frame, source_name)
    return frame


def load_manifest_metadata(manifest_path: Path) -> pd.DataFrame:
    if not manifest_path.exists():
        return pd.DataFrame(columns=MANIFEST_COLUMNS)

    frame = pd.read_csv(manifest_path)
    require_columns(frame, MANIFEST_COLUMNS, f"manifest metadata file ({manifest_path})")
    frame = frame.loc[:, MANIFEST_COLUMNS].copy()
    frame["company_id"] = frame["company_id"].astype("string")
    frame["company_name"] = frame["company_name"].astype("string")
    frame["year"] = frame["year"].map(normalize_year).astype("string")
    frame["document_type"] = frame["document_type"].astype("string")
    raise_on_duplicate_company_year(frame, f"manifest metadata file ({manifest_path})")
    return frame


def load_text_records(text_root: Path) -> pd.DataFrame:
    records: list[dict[str, object]] = []
    for text_path in sorted(text_root.rglob("*.txt")):
        relative_parts = text_path.relative_to(text_root).parts
        if len(relative_parts) < 2:
            raise ValueError(
                f"Text file must live under <text-root>/<company_id>/.../<year>.txt: {text_path}."
            )

        company_id = relative_parts[0]
        year = normalize_year(text_path.stem)
        metadata_path = text_path.with_suffix(".json")
        metadata: dict[str, object] = {}
        if metadata_path.exists():
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))

        document_type = metadata.get("document_type")
        if document_type is None and len(relative_parts) >= 3:
            document_type = relative_parts[-2]

        records.append(
            {
                "company_id": company_id,
                "year": year,
                "document_type": document_type,
                "text": text_path.read_text(encoding="utf-8"),
            }
        )

    frame = pd.DataFrame.from_records(records, columns=["company_id", "year", "document_type", "text"])
    if frame.empty:
        return frame

    frame["company_id"] = frame["company_id"].astype("string")
    frame["year"] = frame["year"].astype("string")
    frame["document_type"] = frame["document_type"].astype("string")
    frame["text"] = frame["text"].astype("string")
    raise_on_duplicate_company_year(frame, f"text files under {text_root}")
    return frame


def build_export_frame(
    text_root: Path,
    sentiment_features_path: Path,
    sentiment_stability_path: Path,
    manifest_path: Path | None = DEFAULT_MANIFEST_PATH,
) -> pd.DataFrame:
    text_frame = load_text_records(text_root)
    if text_frame.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    feature_frame = load_feature_frame(
        sentiment_features_path,
        required_columns=FEATURE_COLUMNS,
        source_name=f"sentiment features file ({sentiment_features_path})",
    )
    stability_frame = load_feature_frame(
        sentiment_stability_path,
        required_columns=STABILITY_COLUMNS,
        source_name=f"sentiment stability file ({sentiment_stability_path})",
    )
    manifest_frame = (
        load_manifest_metadata(manifest_path)
        if manifest_path is not None
        else pd.DataFrame(columns=MANIFEST_COLUMNS)
    )

    merged = text_frame.merge(
        manifest_frame,
        on=["company_id", "year"],
        how="left",
        suffixes=("", "_manifest"),
    )
    merged["company_name"] = merged["company_name"].fillna(
        merged["company_id"].map(lambda value: humanize_company_id(str(value)))
    )
    merged["document_type"] = merged["document_type"].fillna(merged["document_type_manifest"])
    merged["document_type"] = merged["document_type"].fillna("unknown")
    merged = merged.drop(columns=["document_type_manifest"], errors="ignore")

    merged = merged.merge(feature_frame, on=["company_id", "year"], how="left")
    merged = merged.merge(stability_frame, on=["company_id", "year"], how="left")
    merged = merged.loc[:, OUTPUT_COLUMNS]
    merged = merged.sort_values(["company_id", "year", "document_type"], kind="stable").reset_index(drop=True)
    validate_export_schema(merged)
    return merged


def validate_export_schema(frame: pd.DataFrame) -> None:
    actual_columns = frame.columns.tolist()
    if actual_columns != OUTPUT_COLUMNS:
        raise ValueError(
            f"Export schema mismatch. Expected columns {OUTPUT_COLUMNS}, found {actual_columns}."
        )


def run(
    text_root: Path,
    sentiment_features_path: Path,
    sentiment_stability_path: Path,
    output_path: Path,
    manifest_path: Path | None = DEFAULT_MANIFEST_PATH,
) -> pd.DataFrame:
    frame = build_export_frame(
        text_root=text_root,
        sentiment_features_path=sentiment_features_path,
        sentiment_stability_path=sentiment_stability_path,
        manifest_path=manifest_path,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output_path, index=False)
    return frame


def main() -> None:
    args = parse_args()
    frame = run(
        text_root=args.text_root,
        sentiment_features_path=args.sentiment_features,
        sentiment_stability_path=args.sentiment_stability,
        output_path=args.output,
        manifest_path=args.manifest_path,
    )
    print(f"Wrote {len(frame)} row(s) to {args.output}.")


if __name__ == "__main__":
    main()
