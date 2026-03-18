import argparse
import json
import logging
import re
from pathlib import Path
from typing import Iterable, Optional

LOGGER = logging.getLogger(__name__)
WORD_PATTERN = re.compile(r"\b\w+\b")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Extract text from PDFs stored at output/<company>/<document_type>/<year>.pdf "
            "and write output_text/<company>/<year>.txt plus output_text/<company>/<year>.json."
        )
    )
    parser.add_argument("--input-root", type=Path, default=Path("output"))
    parser.add_argument("--output-root", type=Path, default=Path("output_text"))
    parser.add_argument("--document-type", default="shareholder_letter")
    parser.add_argument("--company", help="Optional company_id to process.")
    parser.add_argument(
        "--failure-log",
        type=Path,
        help="Optional path for the failure log file. Defaults to <output-root>/extract_text_failures.log.",
    )
    return parser.parse_args()


def configure_logging(output_root: Path, failure_log: Optional[Path] = None) -> Path:
    output_root.mkdir(parents=True, exist_ok=True)
    log_path = failure_log or output_root / "extract_text_failures.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    root_logger.addHandler(stream_handler)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(logging.WARNING)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    return log_path


def iter_pdfs(input_root: Path, document_type: str, company_id: Optional[str] = None) -> Iterable[Path]:
    pdf_files = list(Path("output").rglob("*.pdf"))
    pdf_files = sorted(pdf_files)
    print(f"Discovered {len(pdf_files)} PDF files")

    if not pdf_files:
        print("No PDFs found under output/. Check downloader output.")
        return []

    if company_id is None:
        return pdf_files

    return [pdf_path for pdf_path in pdf_files if company_id in pdf_path.parts]


def resolve_pdf_metadata(pdf_path: Path, input_root: Path) -> tuple[str, Optional[str], str]:
    year = pdf_path.stem
    path_parts = pdf_path.parts
    if len(path_parts) < 3:
        raise ValueError(f"Unexpected PDF path format: {pdf_path}")

    company_id = path_parts[-3]
    document_type = path_parts[-2]
    return company_id, document_type, year


def extract_pdf_text(pdf_path: Path) -> str:
    try:
        import fitz
    except ModuleNotFoundError as exc:
        raise RuntimeError("PyMuPDF (fitz) is required to extract text from PDFs.") from exc

    text_chunks = []
    with fitz.open(pdf_path) as document:
        for page in document:
            text_chunks.append(page.get_text("text") or "")
    return "\n".join(text_chunks).strip()


def build_metadata(company_id: str, year: str, text: str, document_type: Optional[str] = None) -> dict:
    metadata = {
        "company_id": company_id,
        "year": year,
        "word_count": len(WORD_PATTERN.findall(text)),
        "char_count": len(text),
    }
    if document_type is not None:
        metadata["document_type"] = document_type
    return metadata


def already_processed(output_root: Path, company_id: str, year: str) -> bool:
    company_output_dir = output_root / company_id
    return (company_output_dir / f"{year}.txt").exists() and (company_output_dir / f"{year}.json").exists()


def write_outputs(output_root: Path, company_id: str, year: str, text: str, metadata: dict) -> None:
    company_output_dir = output_root / company_id
    company_output_dir.mkdir(parents=True, exist_ok=True)
    (company_output_dir / f"{year}.txt").write_text(text, encoding="utf-8")
    (company_output_dir / f"{year}.json").write_text(
        json.dumps(metadata, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def run(
    input_root: Path,
    output_root: Path,
    document_type: str,
    company_id: Optional[str] = None,
) -> int:
    processed_count = 0
    skipped_count = 0
    failed_count = 0

    for pdf_path in iter_pdfs(input_root=input_root, document_type=document_type, company_id=company_id):
        resolved_company_id, resolved_document_type, year = resolve_pdf_metadata(
            pdf_path=pdf_path,
            input_root=input_root,
        )

        if already_processed(output_root=output_root, company_id=resolved_company_id, year=year):
            skipped_count += 1
            LOGGER.info("Skipping already processed file: %s", pdf_path)
            continue

        try:
            text = extract_pdf_text(pdf_path)
            metadata = build_metadata(
                company_id=resolved_company_id,
                year=year,
                text=text,
                document_type=resolved_document_type or document_type,
            )
            write_outputs(
                output_root=output_root,
                company_id=resolved_company_id,
                year=year,
                text=text,
                metadata=metadata,
            )
            processed_count += 1
            LOGGER.info("Processed %s", pdf_path)
        except Exception as exc:  # noqa: BLE001
            failed_count += 1
            LOGGER.exception("Failed to process %s: %s", pdf_path, exc)

    LOGGER.info(
        "Finished text extraction. processed=%s skipped=%s failed=%s",
        processed_count,
        skipped_count,
        failed_count,
    )
    return processed_count


def main() -> None:
    args = parse_args()
    log_path = configure_logging(output_root=args.output_root, failure_log=args.failure_log)
    LOGGER.info("Failure log path: %s", log_path)
    run(
        input_root=args.input_root,
        output_root=args.output_root,
        document_type=args.document_type,
        company_id=args.company,
    )


if __name__ == "__main__":
    main()
