import argparse
import csv
import datetime as dt
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional

try:
    import requests
except ModuleNotFoundError:  # pragma: no cover - handled at runtime
    requests = None

OUTPUT_PATH = Path("manifests/letters_manifest.sec.csv")
MANIFEST_COLUMNS = [
    "company_id",
    "company_name",
    "document_type",
    "year",
    "source_type",
    "url",
]
SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SUBMISSIONS_URL_TEMPLATE = "https://data.sec.gov/submissions/CIK{cik}.json"
FILING_INDEX_URL_TEMPLATE = "https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/index.json"
ARCHIVES_BASE_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/{filename}"
TARGET_PHRASES = (
    "letter to shareholders",
    "ceo letter",
    "chairman letter",
)
USER_AGENT = "shareholder-letters-downloader/1.0 (contact: ops@example.com)"


@dataclass(frozen=True)
class Company:
    ticker: str
    cik: str
    name: str


@dataclass(frozen=True)
class Filing:
    accession_number: str
    filing_date: str
    report_date: str
    form: str


class SecClient:
    def __init__(self, timeout_seconds: int = 20) -> None:
        if requests is None:
            raise ModuleNotFoundError(
                "requests is required to query SEC EDGAR. Install it with `pip install requests`."
            )
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": USER_AGENT,
                "Accept-Encoding": "gzip, deflate",
            }
        )

    def get_json(self, url: str) -> Dict:
        response = self.session.get(url, timeout=self.timeout_seconds)
        response.raise_for_status()
        return response.json()



def normalize_company_id(ticker: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", ticker.lower()).strip("_")



def normalize_for_matching(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()



def load_companies_by_ticker(client: SecClient, requested_tickers: Iterable[str]) -> List[Company]:
    payload = client.get_json(SEC_TICKERS_URL)
    by_ticker = {
        str(value["ticker"]).upper(): Company(
            ticker=str(value["ticker"]).upper(),
            cik=f"{int(value['cik_str']):010d}",
            name=str(value["title"]),
        )
        for value in payload.values()
    }

    companies: List[Company] = []
    for ticker in requested_tickers:
        key = ticker.upper()
        if key not in by_ticker:
            print(f"Ticker not found in SEC mapping and will be skipped: {ticker}")
            continue
        companies.append(by_ticker[key])
    return companies



def parse_recent_filings(submissions_json: Dict) -> List[Filing]:
    recent = submissions_json.get("filings", {}).get("recent", {})
    accession_numbers = recent.get("accessionNumber", [])
    filing_dates = recent.get("filingDate", [])
    report_dates = recent.get("reportDate", [])
    forms = recent.get("form", [])

    filings: List[Filing] = []
    for accession, filing_date, report_date, form in zip(accession_numbers, filing_dates, report_dates, forms):
        filings.append(
            Filing(
                accession_number=accession,
                filing_date=filing_date,
                report_date=report_date,
                form=form,
            )
        )
    return filings



def filter_10k_filings(filings: Iterable[Filing], years: int) -> List[Filing]:
    current_year = dt.date.today().year
    min_year = current_year - years + 1
    filtered: List[Filing] = []
    for filing in filings:
        if filing.form != "10-K":
            continue
        year_text = filing.report_date or filing.filing_date
        if not year_text:
            continue
        filing_year = int(year_text[:4])
        if filing_year < min_year:
            continue
        filtered.append(filing)
    return filtered



def accession_without_dashes(accession_number: str) -> str:
    return accession_number.replace("-", "")



def detect_source_type(filename: str) -> Optional[str]:
    lowered = filename.lower()
    if lowered.endswith(".pdf"):
        return "PDF"
    if lowered.endswith(".htm") or lowered.endswith(".html"):
        return "HTML"
    return None



def has_target_phrase(*values: str) -> bool:
    haystack = " ".join(normalize_for_matching(value) for value in values if value)
    return any(phrase in haystack for phrase in TARGET_PHRASES)



def discover_letter_documents_for_filing(client: SecClient, cik: str, accession_number: str) -> List[Dict[str, str]]:
    accession_compact = accession_without_dashes(accession_number)
    index_url = FILING_INDEX_URL_TEMPLATE.format(cik=str(int(cik)), accession=accession_compact)

    try:
        index_json = client.get_json(index_url)
    except Exception as exc:  # pragma: no cover - network/runtime issue
        print(f"Failed to read filing index {index_url}: {exc}")
        return []

    items = index_json.get("directory", {}).get("item", [])
    matches: List[Dict[str, str]] = []
    for item in items:
        filename = str(item.get("name", ""))
        source_type = detect_source_type(filename)
        if source_type is None:
            continue
        item_text = str(item.get("type", ""))
        href_text = str(item.get("href", ""))
        if not has_target_phrase(filename, item_text, href_text):
            continue

        url = ARCHIVES_BASE_URL.format(cik=str(int(cik)), accession=accession_compact, filename=filename)
        matches.append({"source_type": source_type, "url": url})
    return matches



def generate_rows(client: SecClient, companies: Iterable[Company], years: int) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for company in companies:
        submissions_url = SUBMISSIONS_URL_TEMPLATE.format(cik=company.cik)
        print(f"Fetching submissions for {company.ticker}: {submissions_url}")
        try:
            submissions_json = client.get_json(submissions_url)
        except Exception as exc:  # pragma: no cover - network/runtime issue
            print(f"Failed to read submissions for {company.ticker}: {exc}")
            continue

        recent_filings = parse_recent_filings(submissions_json)
        ten_k_filings = filter_10k_filings(recent_filings, years=years)
        for filing in ten_k_filings:
            filing_year = (filing.report_date or filing.filing_date)[:4]
            documents = discover_letter_documents_for_filing(
                client,
                cik=company.cik,
                accession_number=filing.accession_number,
            )
            for document in documents:
                rows.append(
                    {
                        "company_id": normalize_company_id(company.ticker),
                        "company_name": company.name,
                        "document_type": "shareholder_letter",
                        "year": filing_year,
                        "source_type": document["source_type"],
                        "url": document["url"],
                    }
                )
    return rows



def deduplicate_rows(rows: Iterable[Dict[str, str]]) -> List[Dict[str, str]]:
    seen = set()
    deduped: List[Dict[str, str]] = []
    for row in rows:
        key = (row["company_id"], row["year"], row["url"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped



def write_manifest(rows: Iterable[Dict[str, str]], output_path: Path = OUTPUT_PATH) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rows_list = list(rows)
    rows_list.sort(key=lambda row: (row["company_id"], row["year"], row["url"]), reverse=False)

    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=MANIFEST_COLUMNS)
        writer.writeheader()
        writer.writerows(rows_list)

    return len(rows_list)



def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate shareholder-letter manifest rows from SEC EDGAR filings."
    )
    parser.add_argument(
        "--tickers",
        nargs="+",
        required=True,
        help="One or more stock tickers (e.g., AAPL MSFT AMZN JPM).",
    )
    parser.add_argument(
        "--years",
        type=int,
        default=10,
        help="Look back this many report years for 10-K filings (default: 10).",
    )
    return parser.parse_args()



def main() -> None:
    args = parse_args()
    client = SecClient()
    companies = load_companies_by_ticker(client, requested_tickers=args.tickers)
    rows = generate_rows(client, companies=companies, years=args.years)
    deduped_rows = deduplicate_rows(rows)
    row_count = write_manifest(deduped_rows, output_path=OUTPUT_PATH)
    print(f"Wrote {row_count} rows to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
