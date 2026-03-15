import csv
import shutil
import requests
from pathlib import Path
from playwright.sync_api import sync_playwright

OUTPUT_DIR = Path("berkshire_letters_pdf")
OUTPUT_DIR.mkdir(exist_ok=True)

BASE_DIR = Path(__file__).parent
MANIFEST = BASE_DIR / "berkshire_letters_manifest.csv"

# -------------------------
# Helper: download PDF
# -------------------------
def download_pdf(url: str, dest: Path):
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            shutil.copyfileobj(r.raw, f)

# -------------------------
# Load manifest
# -------------------------
with open(MANIFEST, newline="", encoding="utf-8") as f:
    rows = list(csv.DictReader(f))

# -------------------------
# Process HTML years first
# -------------------------
with sync_playwright() as p:
    browser = p.chromium.launch()
    page = browser.new_page()

    for row in rows:
        year = row["year"]
        source = row["source_type"]
        url = row["url"]

        output_path = OUTPUT_DIR / f"Berkshire_Chairman_Letter_{year}.pdf"

        try:
            if source == "HTML":
                print(f"Rendering {year} (HTML)...")
                page.goto(url, wait_until="networkidle", timeout=60_000)
                page.pdf(
                    path=str(output_path),
                    format="Letter",
                    print_background=True,
                )

            elif source == "PDF":
                print(f"Downloading {year} (PDF)...")
                download_pdf(url, output_path)

            else:
                raise ValueError(f"Unknown source_type: {source}")

        except Exception as e:
            print(f"❌ Failed {year}: {e}")

    browser.close()
