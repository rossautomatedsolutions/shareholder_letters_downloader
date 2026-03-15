import importlib
import importlib.util
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Dict, Tuple
from urllib.parse import urljoin, urlparse


def load_archive_scraper_getter():
    module_names = ("scripts.archive_scrapers", "archive_scrapers")
    if __package__ in (None, ""):
        scripts_dir = Path(__file__).resolve().parent
        if str(scripts_dir) not in sys.path:
            sys.path.insert(0, str(scripts_dir))

    for module_name in module_names:
        try:
            module_spec = importlib.util.find_spec(module_name)
        except ModuleNotFoundError as exc:
            missing_name = getattr(exc, "name", None)
            top_level_package = module_name.split(".")[0]
            if missing_name in (module_name, top_level_package):
                continue
            raise

        if module_spec is None:
            continue

        try:
            module = importlib.import_module(module_name)
        except ModuleNotFoundError as exc:
            missing_name = getattr(exc, "name", None)
            top_level_package = module_name.split(".")[0]
            if missing_name in (module_name, top_level_package):
                continue
            raise

        return getattr(module, "get_archive_scraper", None)
    return None


get_archive_scraper = load_archive_scraper_getter()

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
ACCEPT_URL_KEYWORDS = (
    "letter-to-shareholders",
    "shareholder-letter",
    "ceo-letter",
    "chairman-letter",
    "annual-letter",
)
ACCEPT_TEXT_KEYWORDS = ("letter", "ceo letter", "shareholder letter")
EXCLUDE_URL_KEYWORDS = (
    "corporate-data",
    "shareholder-information",
    "financial-data",
    "proxy",
    "presentation",
    "transcript",
    "earnings",
    "line-of-business",
    "board",
    "committee",
    "supplement",
)
KNOWN_SHAREHOLDER_LETTER_PATH_PATTERNS = (
    re.compile(r"/letters/[^/]*ltr\.pdf$"),
)
BERKSHIRE_HOSTS = ("berkshirehathaway.com", "www.berkshirehathaway.com")
YEAR_PATTERN = re.compile(r"(19|20)\d{2}")
REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
    "Accept": "text/html,application/pdf,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}
REQUEST_TIMEOUT_SECONDS = 30
RETRY_DELAYS_SECONDS = (1, 3, 5)
RETRYABLE_STATUS_CODES = {403, 404, 429}


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


def is_candidate_link(url: str, text: str) -> bool:
    lowered_url = url.lower()
    lowered_text = text.lower()
    parsed_url = urlparse(url)
    parsed_path = parsed_url.path.lower()
    parsed_host = parsed_url.netloc.lower()

    if ".pdf" not in parsed_path:
        return False

    if any(keyword in lowered_url for keyword in EXCLUDE_URL_KEYWORDS) or any(
        keyword in lowered_text for keyword in EXCLUDE_URL_KEYWORDS
    ):
        return False

    if parsed_host in BERKSHIRE_HOSTS and any(
        pattern.search(parsed_path) for pattern in KNOWN_SHAREHOLDER_LETTER_PATH_PATTERNS
    ):
        return True

    return any(keyword in lowered_url for keyword in ACCEPT_URL_KEYWORDS) or any(
        keyword in lowered_text for keyword in ACCEPT_TEXT_KEYWORDS
    )


def is_valid_shareholder_letter(url: str, text: str) -> bool:
    return is_candidate_link(url, text)


def request_with_retries(url: str, timeout_seconds: int):
    if requests is None:
        raise ModuleNotFoundError("requests is required to scan investor relations pages.")

    for attempt in range(len(RETRY_DELAYS_SECONDS) + 1):
        try:
            response = requests.get(url, headers=REQUEST_HEADERS, timeout=REQUEST_TIMEOUT_SECONDS)
            if response.status_code in RETRYABLE_STATUS_CODES:
                print(
                    f"Request to {url} returned HTTP {response.status_code} "
                    f"(attempt {attempt + 1}/{len(RETRY_DELAYS_SECONDS) + 1})."
                )
                if attempt < len(RETRY_DELAYS_SECONDS):
                    time.sleep(RETRY_DELAYS_SECONDS[attempt])
                    continue
                return None

            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            print(
                f"Request to {url} failed with connection/request error "
                f"(attempt {attempt + 1}/{len(RETRY_DELAYS_SECONDS) + 1}): {exc}"
            )
            if attempt < len(RETRY_DELAYS_SECONDS):
                time.sleep(RETRY_DELAYS_SECONDS[attempt])
                continue
            return None

    return None


def fetch_candidates(company: CompanyDefinition, timeout_seconds: int = 20) -> List[Dict[str, str]]:
    if requests is None or BeautifulSoup is None:
        raise ModuleNotFoundError(
            "requests and beautifulsoup4 are required to scan investor relations pages."
        )
    print(f"Scanning company: {company.company_id}")
    response = request_with_retries(company.investor_relations_page, timeout_seconds=timeout_seconds)
    if response is None:
        print(f"Failed to fetch {company.investor_relations_page} after retries.")
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    rows: List[Dict[str, str]] = []
    for link in soup.find_all("a", href=True):
        href = link.get("href", "").strip()
        if not href:
            continue
        if ".pdf" not in href.lower():
            continue
        absolute_url = urljoin(company.investor_relations_page, href)
        link_text = " ".join(link.get_text(" ", strip=True).split())
        if not is_candidate_link(absolute_url, link_text):
            print(f"Rejected document: {absolute_url}")
            continue
        print(f"Accepted letter: {absolute_url}")
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


def scrape_berkshire_letters(timeout_seconds: int = 20) -> List[Dict[str, str]]:
    if requests is None or BeautifulSoup is None:
        raise ModuleNotFoundError(
            "requests and beautifulsoup4 are required to scrape Berkshire letters."
        )

    source_url = "https://www.berkshirehathaway.com/letters/letters.html"
    response = request_with_retries(source_url, timeout_seconds=timeout_seconds)
    if response is None:
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    rows: List[Dict[str, str]] = []
    seen_urls = set()

    for link in soup.find_all("a", href=True):
        href = link.get("href", "").strip()
        if not href:
            continue
        absolute_url = urljoin(source_url, href)
        if ".pdf" not in urlparse(absolute_url).path.lower():
            continue
        if absolute_url in seen_urls:
            continue
        seen_urls.add(absolute_url)
        year_match = re.search(r"(19|20)\d{2}", absolute_url)
        year = year_match.group(0) if year_match else ""
        rows.append(
            {
                "company_id": "berkshire_hathaway",
                "company_name": "Berkshire Hathaway",
                "document_type": "shareholder_letter",
                "year": year,
                "source_type": "PDF",
                "url": absolute_url,
            }
        )

    return sorted(rows, key=lambda row: (row["year"], row["url"]), reverse=True)


def normalize_and_filter_rows(rows: Iterable[Dict[str, str]]) -> Tuple[List[Dict[str, str]], int]:
    normalized_rows: List[Dict[str, str]] = []
    skipped_missing_year = 0

    for row in rows:
        company_id = str(row.get("company_id", "")).strip()
        company_name = str(row.get("company_name", "")).strip()
        url = str(row.get("url", "")).strip()
        link_text = str(row.get("link_text", "")).strip()
        extracted_year = detect_year(url, link_text)
        year = extracted_year or detect_year("", str(row.get("year", "")).strip())

        if not year:
            skipped_missing_year += 1
            continue

        normalized_rows.append(
            {
                "company_id": company_id,
                "company_name": company_name,
                "document_type": "shareholder_letter",
                "year": year,
                "source_type": "PDF",
                "url": url,
            }
        )

    return normalized_rows, skipped_missing_year


def deduplicate_company_year(rows: Iterable[Dict[str, str]]) -> List[Dict[str, str]]:
    seen_company_year = set()
    deduped: List[Dict[str, str]] = []
    for row in rows:
        key = (row["company_id"], row["year"])
        if key in seen_company_year:
            continue
        seen_company_year.add(key)
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
    companies_list = list(companies)
    rows: List[Dict[str, str]] = []
    for index, company in enumerate(companies_list):
        company_rows: List[Dict[str, str]] = []
        if company.company_id == "berkshire_hathaway":
            company_rows = scrape_berkshire_letters()
            if not company_rows:
                print("Dedicated Berkshire scraper returned no candidates; falling back.")

        if not company_rows:
            archive_scraper = get_archive_scraper(company.company_id) if get_archive_scraper else None
            if archive_scraper is not None:
                company_rows = archive_scraper()
                if not company_rows:
                    print(
                        f"Archive scraper returned no candidates for {company.company_id}; "
                        "falling back to IR-page scan."
                    )

        if company_rows:
            rows.extend(company_rows)
        else:
            rows.extend(fetch_candidates(company))
        if index < len(companies_list) - 1:
            time.sleep(2)

    normalized_rows, skipped_missing_year = normalize_and_filter_rows(rows)
    deduped_rows = deduplicate_company_year(normalized_rows)
    frame = pd.DataFrame(deduped_rows, columns=MANIFEST_COLUMNS)
    validate_manifest_schema(frame)
    print(f"Rows discovered: {len(rows)}")
    print(f"Rows skipped (missing year): {skipped_missing_year}")
    print(f"Rows written: {len(deduped_rows)}")
    return sort_manifest(frame)


def main() -> None:
    frame = generate_manifest(COMPANIES)
    frame.to_csv(OUTPUT_PATH, index=False)
    print(f"Wrote {len(frame)} rows to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
