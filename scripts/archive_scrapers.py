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

MANIFEST_DOCUMENT_TYPE = "shareholder_letter"
MANIFEST_SOURCE_TYPE = "PDF"


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
                "document_type": MANIFEST_DOCUMENT_TYPE,
                "year": _detect_year(absolute_url, link_text),
                "source_type": MANIFEST_SOURCE_TYPE,
                "url": absolute_url,
            }
        )

    return rows


def scrape_berkshire_letters(timeout_seconds: int = 20) -> List[Dict[str, str]]:
    return _extract_pdf_rows(
        company_id="berkshire_hathaway",
        company_name="Berkshire Hathaway",
        archive_url="https://www.berkshirehathaway.com/letters/letters.html",
        timeout_seconds=timeout_seconds,
    )


def scrape_amazon_letters(timeout_seconds: int = 20) -> List[Dict[str, str]]:
    return _extract_pdf_rows(
        company_id="amazon",
        company_name="Amazon",
        archive_url="https://ir.aboutamazon.com/annual-reports-proxies-and-shareholder-letters/default.aspx",
        timeout_seconds=timeout_seconds,
    )


def scrape_jpmorgan_letters(timeout_seconds: int = 20) -> List[Dict[str, str]]:
    return _extract_pdf_rows(
        company_id="jpmorgan_chase",
        company_name="JPMorgan Chase",
        archive_url="https://www.jpmorganchase.com/ir/annual-report",
        timeout_seconds=timeout_seconds,
    )


def scrape_blackrock_letters(timeout_seconds: int = 20) -> List[Dict[str, str]]:
    return _extract_pdf_rows(
        company_id="blackrock",
        company_name="BlackRock",
        archive_url="https://ir.blackrock.com/annual-reports-and-proxy",
        timeout_seconds=timeout_seconds,
    )


def get_archive_scraper(company_id: str) -> Optional[Callable[[], List[Dict[str, str]]]]:
    scrapers: Dict[str, Callable[[], List[Dict[str, str]]]] = {
        "berkshire_hathaway": scrape_berkshire_letters,
        "amazon": scrape_amazon_letters,
        "jpmorgan_chase": scrape_jpmorgan_letters,
        "blackrock": scrape_blackrock_letters,
    }
    return scrapers.get(company_id)
