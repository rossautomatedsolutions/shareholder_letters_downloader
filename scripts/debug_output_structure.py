from pathlib import Path
from typing import Iterable


MAX_TREE_DEPTH = 2
MAX_PDF_PREVIEW = 10


def iter_pdf_paths(root: Path) -> Iterable[Path]:
    for path in root.rglob('*'):
        if path.is_file() and path.suffix.lower() == '.pdf':
            yield path


def print_directory_tree(root: Path, max_depth: int = MAX_TREE_DEPTH) -> None:
    print('\n=== DIRECTORY TREE (max 2 levels) ===')
    print(f'{root.name}/')

    for path in sorted(root.rglob('*')):
        try:
            relative_parts = path.relative_to(root).parts
        except ValueError:
            continue

        depth = len(relative_parts)
        if depth > max_depth:
            continue

        indent = '  ' * depth
        suffix = '/' if path.is_dir() else ''
        print(f'{indent}{path.name}{suffix}')


def main() -> None:
    output_dir = Path('output')
    if not output_dir.exists():
        print('output/ directory not found')
        return

    all_files = sorted(path for path in output_dir.rglob('*') if path.is_file())
    pdf_paths = sorted(iter_pdf_paths(output_dir))

    total_files = len(all_files)
    total_pdfs = len(pdf_paths)

    print('=== OUTPUT DIRECTORY DEBUG ===')
    print(f'Total files: {total_files}')
    print(f'Total PDFs: {total_pdfs}')

    if total_pdfs == 0:
        print('No PDF files found. Downloader likely failed.')
    else:
        print('\nFirst 10 PDF paths found:')
        for pdf_path in pdf_paths[:MAX_PDF_PREVIEW]:
            print(pdf_path)

    print_directory_tree(output_dir)


if __name__ == '__main__':
    main()
