import re
import time
from typing import Callable, Dict, Iterable, List, Optional
from urllib.parse import urljoin, urlparse

try:
    import requests
except ModuleNotFoundError:  # pragma: no cover - handled at runtime
    requests = None

try:
    from bs4 import BeautifulSoup
except ModuleNotFoundError:  # pragma: no cover - handled at runtime
    BeautifulSoup = None

YEAR_REGEX = re.compile(r"(19|20)\d{2}")
PDF_REGEX = re.compile(r"\.pdf($|[?#])", re.IGNORECASE)
KNOWN_SHAREHOLDER_LETTER_PATH_PATTERNS = (
    re.compile(r"/letters/[^/]*ltr\.pdf$"),
)
BERKSHIRE_HOSTS = ("berkshirehathaway.com", "www.berkshirehathaway.com")
ACCEPT_URL_KEYWORDS = (
    "letter-to-shareholders",
    "shareholder-letter",
    "ceo-letter",
    "chairman-letter",
    "annual-letter",
)
ACCEPT_TEXT_KEYWORDS = (
    "shareholder letter",
    "letter to shareholders",
    "ceo letter",
    "chairman letter",
    "annual letter",
)
EXCLUDE_KEYWORDS = (
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
REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}
RETRY_DELAYS_SECONDS = (1, 3, 5)
RETRYABLE_STATUS_CODES = {403, 404, 429}


def _request_with_retries(url: str, timeout_seconds: int):
    if requests is None:
        raise ModuleNotFoundError("requests is required to scrape archive pages.")

    for attempt in range(len(RETRY_DELAYS_SECONDS) + 1):
        try:
            response = requests.get(url, headers=REQUEST_HEADERS, timeout=timeout_seconds)
            if response.status_code in RETRYABLE_STATUS_CODES:
                if attempt < len(RETRY_DELAYS_SECONDS):
                    time.sleep(RETRY_DELAYS_SECONDS[attempt])
                    continue
                return None

            response.raise_for_status()
            return response
        except requests.RequestException:
            if attempt < len(RETRY_DELAYS_SECONDS):
                time.sleep(RETRY_DELAYS_SECONDS[attempt])
                continue
            return None

    return None


def _is_archive_letter_candidate(url: str, text: str) -> bool:
    lowered_url = url.lower()
    lowered_text = text.lower()

    if not PDF_REGEX.search(url):
        return False

    if any(keyword in lowered_url for keyword in EXCLUDE_KEYWORDS) or any(
        keyword in lowered_text for keyword in EXCLUDE_KEYWORDS
    ):
        return False

    parsed_url = urlparse(url)
    parsed_host = parsed_url.netloc.lower()
    parsed_path = parsed_url.path.lower()

    if parsed_host in BERKSHIRE_HOSTS and any(
        pattern.search(parsed_path) for pattern in KNOWN_SHAREHOLDER_LETTER_PATH_PATTERNS
    ):
        return True

    return any(keyword in lowered_url for keyword in ACCEPT_URL_KEYWORDS) or any(
        keyword in lowered_text for keyword in ACCEPT_TEXT_KEYWORDS
    )


def _iter_link_targets(link) -> Iterable[str]:
    for attribute in ("href", "data-href", "data-url", "data-link"):
        value = link.get(attribute, "")
        if isinstance(value, str):
            cleaned = value.strip()
            if cleaned:
                yield cleaned


def _extract_pdf_rows(company_id: str, company_name: str, archive_url: str) -> List[Dict[str, str]]:
    if requests is None or BeautifulSoup is None:
        raise ModuleNotFoundError(
            "requests and beautifulsoup4 are required to scrape archive pages."
        )

    response = _request_with_retries(archive_url, timeout_seconds=20)
    if response is None:
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    rows: List[Dict[str, str]] = []
    seen_urls = set()

    for link in soup.find_all("a"):
        link_text = " ".join(link.get_text(" ", strip=True).split())
        for target in _iter_link_targets(link):
            absolute_url = urljoin(archive_url, target)
            if not _is_archive_letter_candidate(absolute_url, link_text):
                continue

            if absolute_url in seen_urls:
                continue
            seen_urls.add(absolute_url)

            year_match = YEAR_REGEX.search(f"{absolute_url} {link_text}")
            year = year_match.group(0) if year_match else ""

            rows.append(
                {
                    "company_id": company_id,
                    "company_name": company_name,
                    "document_type": "shareholder_letter",
                    "year": year,
                    "source_type": "PDF",
                    "url": absolute_url,
                }
            )

    return rows


def scrape_berkshire_letters() -> List[Dict[str, str]]:
    return _extract_pdf_rows(
        company_id="berkshire_hathaway",
        company_name="Berkshire Hathaway",
        archive_url="https://www.berkshirehathaway.com/letters/letters.html",
    )


def scrape_amazon_letters() -> List[Dict[str, str]]:
    return _extract_pdf_rows(
        company_id="amazon",
        company_name="Amazon",
        archive_url="https://ir.aboutamazon.com/annual-reports-proxies-and-shareholder-letters/default.aspx",
    )


def scrape_jpmorgan_letters() -> List[Dict[str, str]]:
    return _extract_pdf_rows(
        company_id="jpmorgan_chase",
        company_name="JPMorgan Chase",
        archive_url="https://www.jpmorganchase.com/ir/annual-report",
    )


def scrape_blackrock_letters() -> List[Dict[str, str]]:
    return _extract_pdf_rows(
        company_id="blackrock",
        company_name="BlackRock",
        archive_url="https://ir.blackrock.com/annual-reports-and-proxy",
    )


ARCHIVE_SCRAPERS: Dict[str, Callable[[], List[Dict[str, str]]]] = {
    "berkshire_hathaway": scrape_berkshire_letters,
    "amazon": scrape_amazon_letters,
    "jpmorgan_chase": scrape_jpmorgan_letters,
    "blackrock": scrape_blackrock_letters,
}


def get_archive_scraper(company_id: str) -> Optional[Callable[[], List[Dict[str, str]]]]:
    return ARCHIVE_SCRAPERS.get(company_id)
