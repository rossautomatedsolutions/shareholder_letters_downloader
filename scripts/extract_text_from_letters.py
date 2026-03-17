import argparse
import json
import re
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

SECTION_HEADER_PATTERN = re.compile(r"^[A-Z][A-Z\s,&\-]{2,}$")
TITLE_CASE_HEADER_PATTERN = re.compile(r"^(?:[A-Z][a-z]+\s){1,6}[A-Z][a-z]+$")


def resolve_pdf_backend() -> Tuple[str, object]:
    """Return the available PDF text extraction backend.

    Prefers PyMuPDF (`fitz`) and falls back to `pdfplumber`.
    """
    try:
        import fitz  # type: ignore

        return "pymupdf", fitz
    except ModuleNotFoundError:
        try:
            import pdfplumber  # type: ignore

            return "pdfplumber", pdfplumber
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "Install PyMuPDF (fitz) or pdfplumber to extract PDF text."
            ) from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Extract plain text from downloaded shareholder letters and write "
            "output_text/<company>/<year>.txt plus metadata sidecars."
        )
    )
    parser.add_argument("--output-root", type=Path, default=Path("output"))
    parser.add_argument("--text-output-root", type=Path, default=Path("output_text"))
    parser.add_argument("--company", help="Optional company_id to process.")
    parser.add_argument("--document-type", default="shareholder_letter")
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip extraction when <year>.txt already exists.",
    )
    return parser.parse_args()


def iter_letter_pdfs(
    output_root: Path, document_type: str, company_id: Optional[str] = None
) -> Iterable[Path]:
    if not output_root.exists():
        return []

    if company_id:
        letter_dir = output_root / company_id / document_type
        return sorted(letter_dir.glob("*.pdf"))

    pdf_paths: List[Path] = []
    for company_dir in sorted(path for path in output_root.iterdir() if path.is_dir()):
        letter_dir = company_dir / document_type
        if letter_dir.exists():
            pdf_paths.extend(sorted(letter_dir.glob("*.pdf")))
    return pdf_paths


def extract_text_with_pymupdf(fitz_module: object, pdf_path: Path) -> str:
    chunks: List[str] = []
    document = fitz_module.open(str(pdf_path))
    try:
        for page in document:
            chunks.append(page.get_text("text") or "")
    finally:
        document.close()
    return "\n".join(chunks)


def extract_text_with_pdfplumber(pdfplumber_module: object, pdf_path: Path) -> str:
    chunks: List[str] = []
    with pdfplumber_module.open(str(pdf_path)) as pdf:
        for page in pdf.pages:
            chunks.append(page.extract_text() or "")
    return "\n".join(chunks)


def detect_sections(text: str) -> List[str]:
    sections: List[str] = []
    seen = set()
    for raw_line in text.splitlines():
        line = " ".join(raw_line.split()).strip()
        if len(line) < 4 or len(line) > 80:
            continue
        if SECTION_HEADER_PATTERN.match(line) or TITLE_CASE_HEADER_PATTERN.match(line):
            normalized = line.title()
            if normalized not in seen:
                sections.append(normalized)
                seen.add(normalized)
        if len(sections) >= 25:
            break
    return sections


def build_metadata(text: str, backend_name: str) -> Dict[str, object]:
    words = re.findall(r"\b\w+\b", text)
    return {
        "word_count": len(words),
        "detected_sections": detect_sections(text),
        "extraction_backend": backend_name,
    }


def write_outputs(
    text_output_root: Path,
    company_id: str,
    year: str,
    text: str,
    metadata: Dict[str, object],
) -> Tuple[Path, Path]:
    company_dir = text_output_root / company_id
    company_dir.mkdir(parents=True, exist_ok=True)

    text_path = company_dir / f"{year}.txt"
    metadata_path = company_dir / f"{year}.metadata.json"

    text_path.write_text(text, encoding="utf-8")
    metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    return text_path, metadata_path


def run(
    output_root: Path,
    text_output_root: Path,
    document_type: str,
    company_id: Optional[str] = None,
    skip_existing: bool = False,
) -> int:
    backend_name, backend_module = resolve_pdf_backend()
    processed = 0

    for pdf_path in iter_letter_pdfs(
        output_root=output_root,
        document_type=document_type,
        company_id=company_id,
    ):
        company = pdf_path.parent.parent.name
        year = pdf_path.stem
        target_text_path = text_output_root / company / f"{year}.txt"
        if skip_existing and target_text_path.exists():
            continue

        if backend_name == "pymupdf":
            text = extract_text_with_pymupdf(backend_module, pdf_path)
        else:
            text = extract_text_with_pdfplumber(backend_module, pdf_path)

        metadata = build_metadata(text=text, backend_name=backend_name)
        write_outputs(
            text_output_root=text_output_root,
            company_id=company,
            year=year,
            text=text,
            metadata=metadata,
        )
        processed += 1

    return processed


def main() -> None:
    args = parse_args()
    processed = run(
        output_root=args.output_root,
        text_output_root=args.text_output_root,
        document_type=args.document_type,
        company_id=args.company,
        skip_existing=args.skip_existing,
    )
    print(f"Extracted text for {processed} PDF(s).")


if __name__ == "__main__":
    main()
