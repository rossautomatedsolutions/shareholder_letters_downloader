from pathlib import Path


def count_files(root: Path, suffix: str) -> int:
    if not root.exists():
        return 0
    return sum(1 for path in root.rglob(f'*{suffix}') if path.is_file())


def count_csv_rows(path: Path) -> int:
    if not path.exists():
        return 0

    with path.open('r', encoding='utf-8', newline='') as handle:
        line_count = sum(1 for _ in handle)

    return max(line_count - 1, 0)


def main() -> None:
    pdf_count = count_files(Path('output'), '.pdf')
    text_count = count_files(Path('output_text'), '.txt')
    keyword_rows = count_csv_rows(Path('features/keyword_features.csv'))
    sentiment_rows = count_csv_rows(Path('features/sentiment_features.csv'))

    print(f'PDFs: {pdf_count}')
    print(f'Texts: {text_count}')
    print(f'Keyword rows: {keyword_rows}')
    print(f'Sentiment rows: {sentiment_rows}')


if __name__ == '__main__':
    main()
