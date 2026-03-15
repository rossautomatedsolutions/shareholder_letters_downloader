from pathlib import Path
from typing import Iterable, Sequence
from urllib.parse import urlparse

try:
    import pandas as pd
except ModuleNotFoundError:  # pragma: no cover - handled at runtime
    pd = None

INPUT_MANIFESTS = (
    Path("manifests/letters_manifest.auto.csv"),
    Path("manifests/letters_manifest.sec.csv"),
    Path("manifests/letters_manifest.manual.csv"),
)
OUTPUT_MANIFEST = Path("manifests/letters_manifest.csv")
DEDUPLICATION_KEYS = ["company_id", "year"]


def is_valid_url(url: str) -> bool:
    parsed = urlparse(str(url).strip())
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def read_manifests(paths: Sequence[Path]):
    if pd is None:
        raise ModuleNotFoundError(
            "pandas is required to merge manifests. Install dependencies with `pip install pandas`."
        )

    frames = []
    for path in paths:
        frame = pd.read_csv(path, dtype=str, keep_default_na=False)
        frames.append(frame)
    return pd.concat(frames, ignore_index=True)


def deduplicate_manifest(frame):
    annotated = frame.copy()
    annotated["_is_valid_url"] = annotated["url"].map(is_valid_url)

    selected_rows = []
    for _, group in annotated.groupby(DEDUPLICATION_KEYS, sort=False, dropna=False):
        valid_group = group[group["_is_valid_url"]]
        chosen_row = valid_group.iloc[0] if not valid_group.empty else group.iloc[0]
        selected_rows.append(chosen_row.drop(labels=["_is_valid_url"]))

    return pd.DataFrame(selected_rows).reset_index(drop=True)


def sort_manifest(frame):
    sortable = frame.copy()
    sortable["_year_num"] = pd.to_numeric(sortable["year"], errors="coerce")
    sortable = sortable.sort_values(
        by=["company_id", "_year_num"],
        ascending=[True, False],
        na_position="last",
        kind="mergesort",
    )
    return sortable.drop(columns=["_year_num"]).reset_index(drop=True)


def merge_manifests(input_paths: Iterable[Path] = INPUT_MANIFESTS, output_path: Path = OUTPUT_MANIFEST):
    frame = read_manifests([Path(path) for path in input_paths])
    deduped = deduplicate_manifest(frame)
    merged = sort_manifest(deduped)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(output_path, index=False)
    return merged


def main() -> None:
    merged = merge_manifests()
    print(f"Wrote {len(merged)} rows to {OUTPUT_MANIFEST}")


if __name__ == "__main__":
    main()
