import argparse
import csv
import hashlib
import json
import shutil
import socket
import ssl
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

ALLOWED_SOURCE_TYPES = {"HTML", "PDF"}
REQUIRED_COLUMNS = {
    "company_id",
    "company_name",
    "document_type",
    "year",
    "source_type",
    "url",
}


@dataclass
class RenderConfig:
    wait_until: str = "networkidle"
    viewport_width: int = 1280
    viewport_height: int = 720
    user_agent: Optional[str] = None


class ManifestValidationError(Exception):
    pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download/render shareholder letters from a multi-company manifest.")
    parser.add_argument("--manifest", type=Path, default=Path("manifests/letters_manifest.csv"))
    parser.add_argument("--company", help="Optional company_id filter.")
    parser.add_argument("--output-root", type=Path, default=Path("output"))
    parser.add_argument("--reports-dir", type=Path, default=Path("reports"))
    parser.add_argument("--config", type=Path, default=Path("config/rendering_overrides.json"))
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--timeout-seconds", type=int, default=60)
    parser.add_argument("--preflight-urls", action="store_true")
    return parser.parse_args()


def load_render_overrides(path: Path) -> Dict[str, RenderConfig]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {
        company_id: RenderConfig(
            wait_until=data.get("wait_until", "networkidle"),
            viewport_width=int(data.get("viewport_width", 1280)),
            viewport_height=int(data.get("viewport_height", 720)),
            user_agent=data.get("user_agent"),
        )
        for company_id, data in payload.items()
    }


def load_manifest(path: Path) -> List[Dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    validate_manifest(rows)
    return rows


def validate_manifest(rows: Sequence[Dict[str, str]]) -> None:
    if not rows:
        raise ManifestValidationError("Manifest is empty.")

    missing_columns = REQUIRED_COLUMNS.difference(rows[0].keys())
    if missing_columns:
        raise ManifestValidationError(f"Missing required manifest columns: {sorted(missing_columns)}")

    key_guard = set()
    problems = []
    for index, row in enumerate(rows, start=2):
        key = (row["company_id"], row["document_type"], row["year"])

        if row["source_type"] not in ALLOWED_SOURCE_TYPES:
            problems.append(f"Row {index}: invalid source_type '{row['source_type']}'")
        if not row["year"].isdigit():
            problems.append(f"Row {index}: year must be numeric (got '{row['year']}')")

        parsed = urlparse(row["url"])
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            problems.append(f"Row {index}: invalid URL '{row['url']}'")

        if key in key_guard:
            problems.append(f"Row {index}: duplicate key {key}")
        key_guard.add(key)

    if problems:
        raise ManifestValidationError("\n".join(problems))


def preflight_urls(rows: Sequence[Dict[str, str]], timeout_seconds: int) -> None:
    issues = []
    for row in rows:
        url = row["url"]
        try:
            code = head_status(url, timeout_seconds)
            if code >= 400:
                issues.append(f"{url} -> HTTP {code}")
        except Exception as exc:
            issues.append(f"{url} -> {categorize_error(exc)}")
    if issues:
        raise ManifestValidationError("URL preflight failed:\n" + "\n".join(issues))


def head_status(url: str, timeout_seconds: int) -> int:
    req = Request(url, method="HEAD")
    try:
        with urlopen(req, timeout=timeout_seconds) as response:
            return response.status
    except HTTPError as exc:
        return exc.code


def with_retry(action, retries: int, backoff_seconds: float = 1.0):
    attempt = 0
    while True:
        try:
            return action()
        except Exception:
            attempt += 1
            if attempt > retries:
                raise
            time.sleep(backoff_seconds * (2 ** (attempt - 1)))


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def compute_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def normalized_pdf_path(output_root: Path, row: Dict[str, str]) -> Path:
    return output_root / row["company_id"] / row["document_type"] / f"{row['year']}.pdf"


def raw_path(output_root: Path, row: Dict[str, str]) -> Path:
    ext = ".pdf" if row["source_type"] == "PDF" else ".html"
    return output_root / "raw" / row["company_id"] / row["document_type"] / f"{row['year']}{ext}"


def fetch_binary(url: str, dest: Path, timeout_seconds: int, retries: int) -> None:
    ensure_parent(dest)

    def _run():
        with urlopen(url, timeout=timeout_seconds) as response, dest.open("wb") as output:
            shutil.copyfileobj(response, output)

    with_retry(_run, retries=retries)


def fetch_text(url: str, dest: Path, timeout_seconds: int, retries: int) -> None:
    ensure_parent(dest)

    def _run():
        with urlopen(url, timeout=timeout_seconds) as response:
            dest.write_text(response.read().decode("utf-8", errors="replace"), encoding="utf-8")

    with_retry(_run, retries=retries)


def render_html_to_pdf(
    page,
    html_path: Path,
    output_path: Path,
    timeout_seconds: int,
    retries: int,
    config: RenderConfig,
) -> None:
    ensure_parent(output_path)

    def _run():
        page.set_viewport_size({"width": config.viewport_width, "height": config.viewport_height})
        page.set_extra_http_headers({"User-Agent": config.user_agent} if config.user_agent else {})
        html = html_path.read_text(encoding="utf-8")
        page.set_content(html, wait_until=config.wait_until, timeout=timeout_seconds * 1000)
        page.pdf(path=str(output_path), format="Letter", print_background=True)

    with_retry(_run, retries=retries)


def categorize_error(exc: BaseException) -> str:
    if isinstance(exc, HTTPError):
        if exc.code == 404:
            return "not_found"
        if exc.code in {401, 403}:
            return "access_denied"
        if exc.code >= 500:
            return "server_error"
        return "http_error"
    if isinstance(exc, URLError):
        if isinstance(exc.reason, socket.timeout):
            return "timeout"
        if isinstance(exc.reason, ssl.SSLError):
            return "ssl_error"
        return "connection_error"
    if isinstance(exc, TimeoutError):
        return "timeout"
    return "rendering_or_unknown"


def process_rows(rows, output_root, reports_dir, render_overrides, retries, timeout_seconds) -> Tuple[Path, Path]:
    report_rows = []
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    output_root.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    from playwright.sync_api import sync_playwright

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        page = browser.new_page()
        for row in rows:
            normalized_path = normalized_pdf_path(output_root, row)
            source_raw_path = raw_path(output_root, row)
            metadata_path = normalized_path.with_suffix(".metadata.json")

            status = "success"
            error_category = ""
            error_message = ""
            try:
                if row["source_type"] == "PDF":
                    fetch_binary(row["url"], source_raw_path, timeout_seconds, retries)
                    ensure_parent(normalized_path)
                    shutil.copy2(source_raw_path, normalized_path)
                else:
                    fetch_text(row["url"], source_raw_path, timeout_seconds, retries)
                    render_cfg = render_overrides.get(row["company_id"], RenderConfig())
                    render_html_to_pdf(page, source_raw_path, normalized_path, timeout_seconds, retries, render_cfg)

                metadata = {
                    "company_id": row["company_id"],
                    "company_name": row["company_name"],
                    "document_type": row["document_type"],
                    "year": row["year"],
                    "source_type": row["source_type"],
                    "url": row["url"],
                    "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
                    "normalized_pdf": str(normalized_path),
                    "raw_artifact": str(source_raw_path),
                    "sha256": compute_sha256(normalized_path),
                }
                ensure_parent(metadata_path)
                metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
            except Exception as exc:
                status = "failed"
                error_category = categorize_error(exc)
                error_message = str(exc)

            report_rows.append(
                {
                    "company_id": row["company_id"],
                    "company_name": row["company_name"],
                    "document_type": row["document_type"],
                    "year": row["year"],
                    "source_type": row["source_type"],
                    "url": row["url"],
                    "status": status,
                    "error_category": error_category,
                    "error_message": error_message,
                    "normalized_path": str(normalized_path),
                    "raw_path": str(source_raw_path),
                    "run_id": run_id,
                }
            )
        browser.close()

    csv_report = reports_dir / f"run_report_{run_id}.csv"
    json_report = reports_dir / f"run_report_{run_id}.json"
    with csv_report.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(report_rows[0].keys()))
        writer.writeheader()
        writer.writerows(report_rows)
    json_report.write_text(json.dumps(report_rows, indent=2), encoding="utf-8")
    return csv_report, json_report


def main() -> None:
    args = parse_args()
    rows = load_manifest(args.manifest)

    if args.company:
        rows = [row for row in rows if row["company_id"] == args.company]
        if not rows:
            raise ManifestValidationError(f"No rows found for company_id '{args.company}'.")

    if args.preflight_urls:
        preflight_urls(rows, args.timeout_seconds)

    overrides = load_render_overrides(args.config)
    csv_report, json_report = process_rows(
        rows=rows,
        output_root=args.output_root,
        reports_dir=args.reports_dir,
        render_overrides=overrides,
        retries=args.retries,
        timeout_seconds=args.timeout_seconds,
    )
    print(f"Run completed. Reports:\n- {csv_report}\n- {json_report}")


if __name__ == "__main__":
    main()
