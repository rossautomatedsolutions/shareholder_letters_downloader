import argparse
import csv
import json
import subprocess
import sys
from pathlib import Path
from typing import Iterable, List, Sequence, Tuple


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run export_letters.py for multiple companies from the unified manifest. "
            "Useful for smoke tests and segmented runs."
        )
    )
    parser.add_argument("--manifest", type=Path, default=Path("manifests/letters_manifest.csv"))
    parser.add_argument(
        "--companies",
        nargs="+",
        help="Optional explicit company_id values. Defaults to all company_ids in the manifest.",
    )
    parser.add_argument("--output-root", type=Path, default=Path("output"))
    parser.add_argument("--reports-dir", type=Path, default=Path("reports"))
    parser.add_argument("--config", type=Path, default=Path("config/rendering_overrides.json"))
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--timeout-seconds", type=int, default=60)
    parser.add_argument("--preflight-urls", action="store_true")
    parser.add_argument(
        "--stop-on-error",
        action="store_true",
        help="Stop the batch immediately if a company run fails.",
    )
    return parser.parse_args()


def load_company_ids(manifest: Path) -> List[str]:
    with manifest.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if "company_id" not in (reader.fieldnames or []):
            raise ValueError("Manifest is missing required 'company_id' column.")
        company_ids = sorted({(row.get("company_id") or "").strip() for row in reader if (row.get("company_id") or "").strip()})
    if not company_ids:
        raise ValueError("No company_id values found in manifest.")
    return company_ids


def report_files(reports_dir: Path) -> Sequence[Path]:
    return sorted(reports_dir.glob("run_report_*.json"))


def report_has_failures(report_path: Path) -> bool:
    rows = json.loads(report_path.read_text(encoding="utf-8"))
    return any((row.get("status") or "").strip().lower() == "failed" for row in rows)


def run_company(company_id: str, args: argparse.Namespace) -> Tuple[int, bool]:
    cmd = [
        sys.executable,
        "export_letters.py",
        "--manifest",
        str(args.manifest),
        "--company",
        company_id,
        "--output-root",
        str(args.output_root),
        "--reports-dir",
        str(args.reports_dir),
        "--config",
        str(args.config),
        "--retries",
        str(args.retries),
        "--timeout-seconds",
        str(args.timeout_seconds),
    ]
    if args.preflight_urls:
        cmd.append("--preflight-urls")

    before_reports = set(report_files(args.reports_dir))

    print(f"\n=== Running company: {company_id} ===")
    print("$", " ".join(cmd))
    completed = subprocess.run(cmd)

    after_reports = set(report_files(args.reports_dir))
    new_reports = sorted(after_reports - before_reports)
    had_row_failures = False
    if completed.returncode == 0 and new_reports:
        had_row_failures = report_has_failures(new_reports[-1])
        if had_row_failures:
            print(f"Company '{company_id}' completed with failed rows in report: {new_reports[-1]}")

    return completed.returncode, had_row_failures


def iterate_companies(args: argparse.Namespace) -> Iterable[str]:
    return args.companies if args.companies else load_company_ids(args.manifest)


def main() -> None:
    args = parse_args()
    failed = []

    for company_id in iterate_companies(args):
        rc, had_row_failures = run_company(company_id, args)
        if rc != 0 or had_row_failures:
            failed.append(company_id)
            if args.stop_on_error:
                break

    if failed:
        print("\nBatch finished with failures:", ", ".join(failed))
        sys.exit(1)

    print("\nBatch finished successfully.")


if __name__ == "__main__":
    main()
