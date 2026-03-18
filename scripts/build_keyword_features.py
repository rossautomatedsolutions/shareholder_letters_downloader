import argparse
import csv
import re
from collections import Counter
from pathlib import Path
from typing import Iterable, Iterator, List, Sequence, Tuple

TOKEN_PATTERN = re.compile(r"\b[a-zA-Z']+\b")
DEFAULT_STOPWORDS = {
    "a",
    "about",
    "above",
    "after",
    "again",
    "against",
    "all",
    "am",
    "an",
    "and",
    "any",
    "are",
    "as",
    "at",
    "be",
    "because",
    "been",
    "before",
    "being",
    "below",
    "between",
    "both",
    "but",
    "by",
    "can",
    "could",
    "did",
    "do",
    "does",
    "doing",
    "down",
    "during",
    "each",
    "few",
    "for",
    "from",
    "further",
    "had",
    "has",
    "have",
    "having",
    "he",
    "her",
    "here",
    "hers",
    "herself",
    "him",
    "himself",
    "his",
    "how",
    "i",
    "if",
    "in",
    "into",
    "is",
    "it",
    "its",
    "itself",
    "just",
    "me",
    "more",
    "most",
    "my",
    "myself",
    "no",
    "nor",
    "not",
    "now",
    "of",
    "off",
    "on",
    "once",
    "only",
    "or",
    "other",
    "our",
    "ours",
    "ourselves",
    "out",
    "over",
    "own",
    "s",
    "same",
    "she",
    "should",
    "so",
    "some",
    "such",
    "t",
    "than",
    "that",
    "the",
    "their",
    "theirs",
    "them",
    "themselves",
    "then",
    "there",
    "these",
    "they",
    "this",
    "those",
    "through",
    "to",
    "too",
    "under",
    "until",
    "up",
    "very",
    "was",
    "we",
    "were",
    "what",
    "when",
    "where",
    "which",
    "while",
    "who",
    "whom",
    "why",
    "will",
    "with",
    "you",
    "your",
    "yours",
    "yourself",
    "yourselves",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build top-keyword features from extracted text files stored at "
            "<input-root>/<company_id>/<year>.txt and write a CSV output."
        )
    )
    parser.add_argument("--input-root", type=Path, default=Path("output_text"))
    parser.add_argument("--output-path", type=Path, default=Path("features/keyword_features.csv"))
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument("--company", help="Optional company_id to process.")
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


def tokenize(text: str) -> List[str]:
    return [token.lower() for token in TOKEN_PATTERN.findall(text)]


def filter_stopwords(tokens: Iterable[str], stopwords: Sequence[str] | set[str] = DEFAULT_STOPWORDS) -> List[str]:
    stopword_set = set(stopwords)
    return [token for token in tokens if token not in stopword_set]


def extract_top_keywords(text: str, top_n: int = 20) -> List[Tuple[str, int]]:
    filtered_tokens = filter_stopwords(tokenize(text))
    counts = Counter(filtered_tokens)
    ranked_keywords = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return ranked_keywords[:top_n]


def build_rows(text_path: Path, top_n: int) -> List[dict[str, str | int]]:
    company_id = text_path.parent.name
    year = text_path.stem
    text = text_path.read_text(encoding="utf-8")
    return [
        {
            "company_id": company_id,
            "year": year,
            "keyword": keyword,
            "frequency": frequency,
        }
        for keyword, frequency in extract_top_keywords(text=text, top_n=top_n)
    ]


def write_rows(output_path: Path, rows: List[dict[str, str | int]]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["company_id", "year", "keyword", "frequency"])
        writer.writeheader()
        writer.writerows(rows)


def run(input_root: Path, output_path: Path, top_n: int = 20, company_id: str | None = None) -> int:
    rows: List[dict[str, str | int]] = []
    for text_path in iter_text_files(input_root=input_root, company_id=company_id):
        rows.extend(build_rows(text_path=text_path, top_n=top_n))

    write_rows(output_path=output_path, rows=rows)
    return len(rows)


def main() -> None:
    args = parse_args()
    row_count = run(
        input_root=args.input_root,
        output_path=args.output_path,
        top_n=args.top_n,
        company_id=args.company,
    )
    print(f"Wrote {row_count} keyword feature row(s) to {args.output_path}.")


if __name__ == "__main__":
    main()
