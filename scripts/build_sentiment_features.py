import argparse
import csv
import re
from pathlib import Path
from typing import Iterable, Iterator, Sequence

TOKEN_PATTERN = re.compile(r"\b[a-zA-Z']+\b")
DEFAULT_POSITIVE_WORDS = {
    "achieve",
    "achieved",
    "advancement",
    "advantage",
    "advantaged",
    "benefit",
    "benefited",
    "benefiting",
    "benefits",
    "confidence",
    "confident",
    "constructive",
    "create",
    "created",
    "delivered",
    "delivering",
    "durable",
    "efficient",
    "encouraging",
    "enhanced",
    "excellent",
    "flexibility",
    "improve",
    "improved",
    "improvement",
    "improvements",
    "innovate",
    "innovation",
    "innovative",
    "leader",
    "leading",
    "momentum",
    "opportunity",
    "outperform",
    "outperformed",
    "progress",
    "profitable",
    "profitability",
    "resilient",
    "resilience",
    "robust",
    "solid",
    "strength",
    "strengthen",
    "strong",
    "success",
    "successful",
    "superior",
    "value",
    "valuable",
    "win",
    "winning",
}
DEFAULT_NEGATIVE_WORDS = {
    "adverse",
    "bad",
    "challenge",
    "challenges",
    "decline",
    "declined",
    "decrease",
    "decreased",
    "deficit",
    "deterioration",
    "difficult",
    "difficulty",
    "downturn",
    "exposure",
    "fail",
    "failed",
    "failure",
    "headwind",
    "headwinds",
    "impairment",
    "loss",
    "losses",
    "negative",
    "pressure",
    "pressures",
    "recession",
    "restructuring",
    "risk",
    "risks",
    "shortfall",
    "slowdown",
    "uncertain",
    "uncertainties",
    "uncertainty",
    "underperform",
    "underperformed",
    "volatility",
    "weak",
    "weakness",
    "worse",
    "worst",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build sentiment features from extracted text files stored at "
            "<input-root>/<company_id>/<year>.txt and write a CSV output."
        )
    )
    parser.add_argument("--input-root", type=Path, default=Path("output_text"))
    parser.add_argument("--output-path", type=Path, default=Path("features/sentiment_features.csv"))
    parser.add_argument("--company", help="Optional company_id to process.")
    parser.add_argument(
        "--positive-lexicon",
        type=Path,
        help="Optional newline-delimited positive lexicon file. Defaults to the built-in list.",
    )
    parser.add_argument(
        "--negative-lexicon",
        type=Path,
        help="Optional newline-delimited negative lexicon file. Defaults to the built-in list.",
    )
    return parser.parse_args()


def iter_text_files(input_root: Path, company_id: str | None = None) -> Iterator[Path]:
    if not input_root.exists():
        return

    if company_id:
        company_dir = input_root / company_id
        if not company_dir.exists():
            return
        yield from sorted(company_dir.glob("*.txt"))
        return

    for company_dir in sorted(path for path in input_root.iterdir() if path.is_dir()):
        yield from sorted(company_dir.glob("*.txt"))


def tokenize(text: str) -> list[str]:
    return [token.lower() for token in TOKEN_PATTERN.findall(text)]


def load_lexicon(path: Path | None, default_words: Sequence[str] | set[str]) -> set[str]:
    if path is None:
        return set(default_words)

    words = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        normalized = line.strip().lower()
        if not normalized or normalized.startswith(";") or normalized.startswith("#"):
            continue
        words.add(normalized)
    return words


def compute_sentiment_metrics(
    text: str,
    positive_words: Iterable[str],
    negative_words: Iterable[str],
) -> dict[str, int | float]:
    tokens = tokenize(text)
    total_words = len(tokens)
    positive_word_set = set(positive_words)
    negative_word_set = set(negative_words)

    positive_word_count = sum(1 for token in tokens if token in positive_word_set)
    negative_word_count = sum(1 for token in tokens if token in negative_word_set)
    sentiment_score = 0.0
    if total_words:
        sentiment_score = (positive_word_count - negative_word_count) / total_words

    return {
        "positive_word_count": positive_word_count,
        "negative_word_count": negative_word_count,
        "sentiment_score": sentiment_score,
    }


def build_row(text_path: Path, positive_words: set[str], negative_words: set[str]) -> dict[str, str | float]:
    company_id = text_path.parent.name
    year = text_path.stem
    text = text_path.read_text(encoding="utf-8")
    metrics = compute_sentiment_metrics(text, positive_words=positive_words, negative_words=negative_words)
    return {
        "company_id": company_id,
        "year": year,
        "sentiment_score": f"{metrics['sentiment_score']:.6f}",
    }


def write_rows(output_path: Path, rows: list[dict[str, str | float]]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["company_id", "year", "sentiment_score"])
        writer.writeheader()
        writer.writerows(rows)


def run(
    input_root: Path,
    output_path: Path,
    company_id: str | None = None,
    positive_lexicon_path: Path | None = None,
    negative_lexicon_path: Path | None = None,
) -> int:
    positive_words = load_lexicon(positive_lexicon_path, DEFAULT_POSITIVE_WORDS)
    negative_words = load_lexicon(negative_lexicon_path, DEFAULT_NEGATIVE_WORDS)

    rows = [
        build_row(text_path=text_path, positive_words=positive_words, negative_words=negative_words)
        for text_path in iter_text_files(input_root=input_root, company_id=company_id)
    ]
    write_rows(output_path=output_path, rows=rows)
    return len(rows)


def main() -> None:
    args = parse_args()
    row_count = run(
        input_root=args.input_root,
        output_path=args.output_path,
        company_id=args.company,
        positive_lexicon_path=args.positive_lexicon,
        negative_lexicon_path=args.negative_lexicon,
    )
    print(f"Wrote {row_count} sentiment feature row(s) to {args.output_path}.")


if __name__ == "__main__":
    main()
