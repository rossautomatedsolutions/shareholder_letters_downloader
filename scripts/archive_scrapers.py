import re
from typing import Callable, Dict, List, Optional
from urllib.parse import urljoin

try:
    import requests
except ModuleNotFoundError:  # pragma: no cover - handled at runtime
    requests = None

try:
    from bs4 import BeautifulSoup
except ModuleNotFoundError:  # pragma: no cover - handled at runtime
    BeautifulSoup = None

YEAR_PATTERN = re.compile(r"(19|20)\d{2}")
REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}


def _extract_year(url: str, link_text: str) -> str:
    for candidate in (url, link_text):
        match = YEAR_PATTERN.search(candidate)
        if match:
            return match.group(0)
    return ""


def _scrape_archive_page(company_id: str, company_name: str, archive_url: str) -> List[Dict[str, str]]:
    if requests is None or BeautifulSoup is None:
        raise ModuleNotFoundError("requests and beautifulsoup4 are required to scrape archive pages.")

    response = requests.get(archive_url, headers=REQUEST_HEADERS, timeout=20)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    rows: List[Dict[str, str]] = []
    for link in soup.find_all("a", href=True):
        href = link.get("href", "").strip()
        if not href:
            continue
        absolute_url = urljoin(archive_url, href)
        if ".pdf" not in absolute_url.lower():
            continue
        link_text = " ".join(link.get_text(" ", strip=True).split())
        rows.append(
            {
                "company_id": company_id,
                "company_name": company_name,
                "document_type": "shareholder_letter",
                "year": _extract_year(absolute_url, link_text),
                "source_type": "PDF",
                "url": absolute_url,
            }
        )
    return rows


def scrape_berkshire_letters() -> List[Dict[str, str]]:
    return _scrape_archive_page(
        company_id="berkshire_hathaway",
        company_name="Berkshire Hathaway",
        archive_url="https://www.berkshirehathaway.com/letters/letters.html",
    )


def scrape_amazon_letters() -> List[Dict[str, str]]:
    return _scrape_archive_page(
        company_id="amazon",
        company_name="Amazon",
        archive_url="https://ir.aboutamazon.com/annual-reports-proxies-and-shareholder-letters/default.aspx",
    )


def scrape_jpmorgan_letters() -> List[Dict[str, str]]:
    return _scrape_archive_page(
        company_id="jpmorgan_chase",
        company_name="JPMorgan Chase",
        archive_url="https://www.jpmorganchase.com/ir/annual-report",
    )


def scrape_blackrock_letters() -> List[Dict[str, str]]:
    return _scrape_archive_page(
        company_id="blackrock",
        company_name="BlackRock",
        archive_url="https://ir.blackrock.com/annual-reports-and-proxy",
    )


def get_archive_scraper(company_id: str) -> Optional[Callable[[], List[Dict[str, str]]]]:
    scrapers = {
        "berkshire_hathaway": scrape_berkshire_letters,
        "amazon": scrape_amazon_letters,
        "jpmorgan_chase": scrape_jpmorgan_letters,
        "blackrock": scrape_blackrock_letters,
    }
    return scrapers.get(company_id)
