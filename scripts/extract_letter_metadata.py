import argparse
import json
import re
from pathlib import Path
from typing import Dict, Iterator, Optional

PREVIEW_LENGTH = 280


def resolve_pdf_reader():
    try:
        from PyPDF2 import PdfReader as _PdfReader  # type: ignore

        return _PdfReader
    except ModuleNotFoundError:
        try:
            from pdfminer.high_level import extract_text as _extract_text  # type: ignore
            from pdfminer.pdfpage import PDFPage as _PDFPage  # type: ignore

            class _PdfMinerReader:
                def __init__(self, path: str):
                    self.path = path
                    with open(path, "rb") as handle:
                        self.pages = [object() for _ in _PDFPage.get_pages(handle)]

                def first_page_text(self) -> str:
                    return _extract_text(self.path, page_numbers=[0]) or ""

            return _PdfMinerReader
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "Install PyPDF2 or pdfminer.six to extract letter metadata."
            ) from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Extract PDF metadata for downloaded shareholder letters and write "
            "<year>.metadata.json files beside each PDF."
        )
    )
    parser.add_argument("--output-root", type=Path, default=Path("output"))
    parser.add_argument("--company", help="Optional company_id to process.")
    parser.add_argument("--document-type", default="shareholder_letter")
    return parser.parse_args()


def iter_letter_pdfs(output_root: Path, document_type: str, company_id: Optional[str] = None) -> Iterator[Path]:
    if company_id:
        base = output_root / company_id / document_type
        yield from sorted(base.glob("*.pdf"))
        return

    for company_dir in sorted(path for path in output_root.iterdir() if path.is_dir()):
        letter_dir = company_dir / document_type
        if letter_dir.exists():
            yield from sorted(letter_dir.glob("*.pdf"))


def load_existing_metadata(metadata_path: Path) -> Dict[str, object]:
    if not metadata_path.exists():
        return {}
    try:
        return json.loads(metadata_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def extract_detected_year(pdf_path: Path, existing_metadata: Dict[str, object], first_page_text: str) -> str:
    year_from_name = re.search(r"(19|20)\d{2}", pdf_path.stem)
    if year_from_name:
        return year_from_name.group(0)

    year_from_existing = str(existing_metadata.get("year", "")).strip()
    if re.fullmatch(r"(19|20)\d{2}", year_from_existing):
        return year_from_existing

    year_from_text = re.search(r"\b(19|20)\d{2}\b", first_page_text)
    if year_from_text:
        return year_from_text.group(0)

    return pdf_path.stem


def extract_pdf_metadata(pdf_path: Path) -> Dict[str, object]:
    reader_cls = resolve_pdf_reader()
    reader = reader_cls(str(pdf_path))
    first_page_text = ""
    if hasattr(reader, "first_page_text") and reader.pages:
        first_page_text = reader.first_page_text() or ""
    elif reader.pages:
        first_page_text = reader.pages[0].extract_text() or ""

    first_page_text = " ".join(first_page_text.split())
    existing_metadata = load_existing_metadata(pdf_path.with_suffix(".metadata.json"))
    detected_year = extract_detected_year(pdf_path, existing_metadata, first_page_text)

    return {
        "page_count": len(reader.pages),
        "file_size": pdf_path.stat().st_size,
        "detected_year": detected_year,
        "first_page_text_preview": first_page_text[:PREVIEW_LENGTH],
    }


def build_metadata_record(company_id: str, pdf_path: Path, extracted: Dict[str, object]) -> Dict[str, object]:
    existing_metadata = load_existing_metadata(pdf_path.with_suffix(".metadata.json"))
    year = str(existing_metadata.get("year") or extracted["detected_year"])

    return {
        "company_id": company_id,
        "year": year,
        "page_count": extracted["page_count"],
        "file_size": extracted["file_size"],
        "url": existing_metadata.get("url", ""),
        "download_timestamp": existing_metadata.get("download_timestamp")
        or existing_metadata.get("fetched_at_utc", ""),
        "detected_year": extracted["detected_year"],
        "first_page_text_preview": extracted["first_page_text_preview"],
    }


def write_metadata(pdf_path: Path, metadata: Dict[str, object]) -> Path:
    metadata_path = pdf_path.with_suffix(".metadata.json")
    metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    return metadata_path


def run(output_root: Path, document_type: str, company_id: Optional[str] = None) -> int:
    if not output_root.exists():
        return 0

    processed = 0
    for pdf_path in iter_letter_pdfs(output_root=output_root, document_type=document_type, company_id=company_id):
        company = pdf_path.parent.parent.name
        extracted = extract_pdf_metadata(pdf_path)
        metadata = build_metadata_record(company_id=company, pdf_path=pdf_path, extracted=extracted)
        write_metadata(pdf_path, metadata)
        processed += 1

    return processed


def main() -> None:
    args = parse_args()
    processed = run(output_root=args.output_root, document_type=args.document_type, company_id=args.company)
    print(f"Processed {processed} PDF(s).")


if __name__ == "__main__":
    main()
