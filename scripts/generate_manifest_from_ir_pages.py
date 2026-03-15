import re
from dataclasses import dataclass
from typing import Iterable, List, Dict
from urllib.parse import urljoin

from scripts.archive_scrapers import get_archive_scraper

try:
    import pandas as pd
except ModuleNotFoundError:  # pragma: no cover - handled at runtime
    pd = None
try:
    import requests
except ModuleNotFoundError:  # pragma: no cover - handled at runtime
    requests = None

try:
    from bs4 import BeautifulSoup
except ModuleNotFoundError:  # pragma: no cover - handled at runtime
    BeautifulSoup = None

OUTPUT_PATH = "manifests/letters_manifest.auto.csv"
MANIFEST_COLUMNS = [
    "company_id",
    "company_name",
    "document_type",
    "year",
    "source_type",
    "url",
]
KEYWORDS = ("letter", "shareholder", "annual-letter", "ceo-letter")
YEAR_PATTERN = re.compile(r"\b(19|20)\d{2}\b")


@dataclass(frozen=True)
class CompanyDefinition:
    company_id: str
    company_name: str
    investor_relations_page: str


COMPANIES: List[CompanyDefinition] = [
    CompanyDefinition("berkshire_hathaway", "Berkshire Hathaway", "https://www.berkshirehathaway.com/reports.html"),
    CompanyDefinition("amazon", "Amazon", "https://www.aboutamazon.com/investor-relations/annual-reports-proxies-and-shareholder-letters"),
    CompanyDefinition("apple", "Apple", "https://investor.apple.com/investor-relations/default.aspx"),
    CompanyDefinition("microsoft", "Microsoft", "https://www.microsoft.com/en-us/investor"),
    CompanyDefinition("alphabet", "Alphabet", "https://abc.xyz/investor/"),
    CompanyDefinition("nvidia", "NVIDIA", "https://investor.nvidia.com/financial-info/annual-reports/default.aspx"),
    CompanyDefinition("meta", "Meta", "https://investor.atmeta.com/financials/annual-reports/default.aspx"),
    CompanyDefinition("tesla", "Tesla", "https://ir.tesla.com/#tab-quarterly-disclosure"),
    CompanyDefinition("jpmorgan_chase", "JPMorgan Chase", "https://www.jpmorganchase.com/ir/annual-report"),
    CompanyDefinition("blackrock", "BlackRock", "https://ir.blackrock.com/annual-reports-and-proxy/default.aspx"),
    CompanyDefinition("costco", "Costco", "https://investor.costco.com/financial-information/annual-reports"),
    CompanyDefinition("walmart", "Walmart", "https://stock.walmart.com/financials/annual-reports/default.aspx"),
    CompanyDefinition("coca_cola", "Coca-Cola", "https://investors.coca-colacompany.com/financial-information/annual-filings"),
    CompanyDefinition("disney", "Disney", "https://thewaltdisneycompany.com/investor-relations/"),
    CompanyDefinition("nike", "Nike", "https://investors.nike.com/investors/news-events-and-reports/default.aspx"),
    CompanyDefinition("procter_gamble", "Procter & Gamble", "https://www.pginvestor.com/financial-reporting/annual-reports"),
]


def detect_year(url: str, link_text: str) -> str:
    for text in (url, link_text):
        match = YEAR_PATTERN.search(text)
        if match:
            return match.group(0)
    return ""


def is_candidate_link(url: str, link_text: str) -> bool:
    lowered_url = url.lower()
    lowered_text = link_text.lower()
    has_pdf = ".pdf" in lowered_url
    if not has_pdf:
        return False
    return any(keyword in lowered_url or keyword in lowered_text for keyword in KEYWORDS)


def fetch_candidates(company: CompanyDefinition, timeout_seconds: int = 20) -> List[Dict[str, str]]:
    if requests is None or BeautifulSoup is None:
        raise ModuleNotFoundError(
            "requests and beautifulsoup4 are required to scan investor relations pages."
        )
    print(f"Scanning company: {company.company_id}")
    try:
        response = requests.get(company.investor_relations_page, timeout=timeout_seconds)
        response.raise_for_status()
    except requests.RequestException as exc:
        print(f"Failed to fetch {company.investor_relations_page}: {exc}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    rows: List[Dict[str, str]] = []
    for link in soup.find_all("a", href=True):
        href = link.get("href", "").strip()
        if not href:
            continue
        absolute_url = urljoin(company.investor_relations_page, href)
        link_text = " ".join(link.get_text(" ", strip=True).split())
        if not is_candidate_link(absolute_url, link_text):
            continue
        print(f"Found candidate: {absolute_url}")
        rows.append(
            {
                "company_id": company.company_id,
                "company_name": company.company_name,
                "document_type": "shareholder_letter",
                "year": detect_year(absolute_url, link_text),
                "source_type": "PDF",
                "url": absolute_url,
            }
        )
    return rows


def deduplicate_urls(rows: Iterable[Dict[str, str]]) -> List[Dict[str, str]]:
    seen_urls = set()
    deduped: List[Dict[str, str]] = []
    for row in rows:
        url = row["url"]
        if url in seen_urls:
            continue
        seen_urls.add(url)
        deduped.append(row)
    return deduped


def validate_manifest_schema(frame) -> None:
    actual_columns = list(frame.columns)
    if actual_columns != MANIFEST_COLUMNS:
        raise ValueError(
            f"Generated manifest schema mismatch. Expected {MANIFEST_COLUMNS}, got {actual_columns}"
        )


def sort_manifest(frame):
    sortable = frame.copy()
    sortable["_year_num"] = pd.to_numeric(sortable["year"], errors="coerce")
    sortable = sortable.sort_values(
        by=["company_id", "_year_num", "url"],
        ascending=[True, False, True],
        na_position="last",
        kind="mergesort",
    )
    return sortable.drop(columns=["_year_num"])


def generate_manifest(companies: Iterable[CompanyDefinition]):
    if pd is None:
        raise ModuleNotFoundError(
            "pandas is required to generate the manifest. Install dependencies with `pip install pandas`."
        )
    rows: List[Dict[str, str]] = []
    for company in companies:
        archive_scraper = get_archive_scraper(company.company_id)
        if archive_scraper is not None:
            print(f"Using structured archive scraper: {company.company_id}")
            rows.extend(archive_scraper(company.company_id, company.company_name, 20))
            continue
        rows.extend(fetch_candidates(company))

    deduped_rows = deduplicate_urls(rows)
    frame = pd.DataFrame(deduped_rows, columns=MANIFEST_COLUMNS)
    validate_manifest_schema(frame)
    return sort_manifest(frame)


def main() -> None:
    frame = generate_manifest(COMPANIES)
    frame.to_csv(OUTPUT_PATH, index=False)
    print(f"Wrote {len(frame)} rows to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
