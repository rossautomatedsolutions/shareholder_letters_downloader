import re
from typing import Callable, Dict, List, Optional
from urllib.parse import urljoin, urlparse

try:
    from bs4 import BeautifulSoup
except ModuleNotFoundError:  # pragma: no cover - handled at runtime
    BeautifulSoup = None

YEAR_PATTERN = re.compile(r"\b(19|20)\d{2}\b")
ACCEPT_URL_KEYWORDS = (
    "letter-to-shareholders",
    "shareholder-letter",
    "ceo-letter",
    "chairman-letter",
    "annual-letter",
)
ACCEPT_TEXT_KEYWORDS = ("letter", "ceo letter", "shareholder letter")
KNOWN_SHAREHOLDER_LETTER_PATH_PATTERNS = (
    re.compile(r"/letters/[^/]*ltr\.pdf$"),
)


def _extract_year(url: str, link_text: str) -> str:
    for candidate in (url, link_text):
        match = YEAR_PATTERN.search(candidate)
        if match:
            return match.group(0)
    return ""


def _is_shareholder_letter_pdf(url: str, link_text: str) -> bool:
    parsed_path = urlparse(url).path.lower()
    lowered_url = url.lower()
    lowered_text = link_text.lower()

    if ".pdf" not in parsed_path:
        return False

    if any(pattern.search(parsed_path) for pattern in KNOWN_SHAREHOLDER_LETTER_PATH_PATTERNS):
        return True

    return any(keyword in lowered_url for keyword in ACCEPT_URL_KEYWORDS) or any(
        keyword in lowered_text for keyword in ACCEPT_TEXT_KEYWORDS
    )


def _scrape_archive_page(
    company_id: str,
    company_name: str,
    archive_url: str,
    request_with_retries: Callable[..., Optional[object]],
    timeout_seconds: int = 20,
) -> List[Dict[str, str]]:
    if BeautifulSoup is None:
        raise ModuleNotFoundError("beautifulsoup4 is required to scrape archive pages.")

    response = request_with_retries(archive_url, timeout_seconds=timeout_seconds)
    if response is None:
        print(f"Failed to fetch archive page for {company_id}: {archive_url}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    rows: List[Dict[str, str]] = []
    for link in soup.find_all("a", href=True):
        href = link.get("href", "").strip()
        if not href:
            continue
        absolute_url = urljoin(archive_url, href)
        link_text = " ".join(link.get_text(" ", strip=True).split())

        if not _is_shareholder_letter_pdf(absolute_url, link_text):
            continue

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


def get_archive_scraper(company_id: str):
    archive_urls = {
        "blackrock": "https://ir.blackrock.com/annual-reports-and-proxy/default.aspx",
    }
    archive_url = archive_urls.get(company_id)
    if archive_url is None:
        return None

    return lambda company_name, request_with_retries, timeout_seconds=20: _scrape_archive_page(
        company_id,
        company_name,
        archive_url,
        request_with_retries,
        timeout_seconds,
    )
