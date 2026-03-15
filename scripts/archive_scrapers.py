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

ARCHIVE_SOURCES: Dict[str, str] = {
    "berkshire_hathaway": "https://www.berkshirehathaway.com/letters/letters.html",
    "amazon": "https://ir.aboutamazon.com/annual-reports-proxies-and-shareholder-letters/default.aspx",
    "jpmorgan_chase": "https://www.jpmorganchase.com/ir/annual-report",
    "blackrock": "https://ir.blackrock.com/annual-reports-and-proxy",
}


def _detect_year(url: str, link_text: str) -> str:
    for value in (url, link_text):
        match = YEAR_PATTERN.search(value)
        if match:
            return match.group(0)
    return ""


def _extract_pdf_rows(
    *,
    company_id: str,
    company_name: str,
    archive_url: str,
    timeout_seconds: int,
) -> List[Dict[str, str]]:
    if requests is None or BeautifulSoup is None:
        raise ModuleNotFoundError(
            "requests and beautifulsoup4 are required to scrape structured archive pages."
        )

    try:
        response = requests.get(archive_url, timeout=timeout_seconds)
        response.raise_for_status()
    except requests.RequestException as exc:
        print(f"Failed to fetch archive {archive_url}: {exc}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    rows: List[Dict[str, str]] = []
    seen_urls = set()

    for link in soup.find_all("a", href=True):
        href = link.get("href", "").strip()
        if not href:
            continue

        absolute_url = urljoin(archive_url, href)
        if ".pdf" not in absolute_url.lower():
            continue
        if absolute_url in seen_urls:
            continue

        seen_urls.add(absolute_url)
        link_text = " ".join(link.get_text(" ", strip=True).split())
        rows.append(
            {
                "company_id": company_id,
                "company_name": company_name,
                "document_type": "shareholder_letter",
                "year": _detect_year(absolute_url, link_text),
                "source_type": "PDF",
                "url": absolute_url,
            }
        )

    return rows


def scrape_berkshire_letters(company_id: str, company_name: str, timeout_seconds: int = 20) -> List[Dict[str, str]]:
    return _extract_pdf_rows(
        company_id=company_id,
        company_name=company_name,
        archive_url=ARCHIVE_SOURCES["berkshire_hathaway"],
        timeout_seconds=timeout_seconds,
    )


def scrape_amazon_letters(company_id: str, company_name: str, timeout_seconds: int = 20) -> List[Dict[str, str]]:
    return _extract_pdf_rows(
        company_id=company_id,
        company_name=company_name,
        archive_url=ARCHIVE_SOURCES["amazon"],
        timeout_seconds=timeout_seconds,
    )


def scrape_jpmorgan_letters(company_id: str, company_name: str, timeout_seconds: int = 20) -> List[Dict[str, str]]:
    return _extract_pdf_rows(
        company_id=company_id,
        company_name=company_name,
        archive_url=ARCHIVE_SOURCES["jpmorgan_chase"],
        timeout_seconds=timeout_seconds,
    )


def scrape_blackrock_letters(company_id: str, company_name: str, timeout_seconds: int = 20) -> List[Dict[str, str]]:
    return _extract_pdf_rows(
        company_id=company_id,
        company_name=company_name,
        archive_url=ARCHIVE_SOURCES["blackrock"],
        timeout_seconds=timeout_seconds,
    )


def get_archive_scraper(company_id: str) -> Optional[Callable[[str, str, int], List[Dict[str, str]]]]:
    scrapers: Dict[str, Callable[[str, str, int], List[Dict[str, str]]]] = {
        "berkshire_hathaway": scrape_berkshire_letters,
        "amazon": scrape_amazon_letters,
        "jpmorgan_chase": scrape_jpmorgan_letters,
        "blackrock": scrape_blackrock_letters,
    }
    return scrapers.get(company_id)
