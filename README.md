# Multi-Company Shareholder Letter Downloader

This repository provides a deterministic ingestion pipeline for building a normalized corpus of shareholder letters across multiple companies.

## Manifest format

The pipeline uses a **unified manifest** at `manifests/letters_manifest.csv` with required columns:

- `company_id`
- `company_name`
- `document_type`
- `year`
- `source_type` (`HTML` or `PDF`)
- `url`

Optional columns are supported and preserved in the manifest:

- `language`
- `fiscal_year_end`
- `notes`

Uniqueness is enforced on `(company_id, document_type, year)`.

## Project layout

- `manifests/` - source manifests
- `config/` - optional per-company rendering overrides
- `output/` - normalized PDFs and raw fetched artifacts
- `reports/` - machine-readable run summaries
- `tests/` - schema and path-generation checks

## Usage

Run all companies:

```bash
python export_letters.py
```

Run a single company:

```bash
python export_letters.py --company berkshire_hathaway
```

Use custom paths:

```bash
python export_letters.py --manifest manifests/letters_manifest.csv --output-root output --reports-dir reports
```

Optional URL preflight validation:

```bash
python export_letters.py --preflight-urls
```

Extract/refresh letter metadata from downloaded PDFs:

```bash
python scripts/extract_letter_metadata.py
```


## Output behavior

For each row, the pipeline writes:

- Normalized PDF at `output/<company_id>/<document_type>/<year>.pdf`
- Raw artifact at `output/raw/<company_id>/<document_type>/<year>.<ext>`
- Metadata sidecar at `output/<company_id>/<document_type>/<year>.metadata.json`

Each run also writes:

- `reports/run_report_<timestamp>.csv`
- `reports/run_report_<timestamp>.json`

## Validation and reliability

Before ingestion, the pipeline validates:

- required columns
- allowed `source_type`
- numeric `year`
- unique `(company_id, document_type, year)` keys
- URL format (and optionally preflight checks)

Runtime reliability features include:

- retry + exponential backoff for network/render operations
- categorized errors in reports (timeouts, SSL, DNS/connection, HTTP classes)
- per-company rendering overrides via `config/rendering_overrides.json`



## Running multiple companies in batches

Use the helper script to run one company at a time (useful for retries and smoke tests):

```bash
python scripts/run_multiple_companies.py --preflight-urls
```

Run only a subset:

```bash
python scripts/run_multiple_companies.py --companies berkshire_hathaway apple microsoft
```

Stop as soon as one company fails:

```bash
python scripts/run_multiple_companies.py --stop-on-error
```

## Preparing your own company link data

1. Copy the template:

```bash
cp manifests/letters_manifest.template.csv manifests/letters_manifest.csv
```

2. Populate rows with real `url` values and keep `(company_id, document_type, year)` unique.
3. Validate links before download/render:

```bash
python export_letters.py --preflight-urls
```

## Automatically Generating a Manifest

You can generate a draft manifest of shareholder-letter PDF candidates from company investor-relations pages:

```bash
python scripts/generate_manifest_from_ir_pages.py
```

The script writes results to `manifests/letters_manifest.auto.csv` using the existing manifest schema.
Review the generated rows, verify that each URL is a valid shareholder letter, and then copy valid rows into `manifests/letters_manifest.csv`.

### Optional SEC EDGAR-based generator

You can also generate a manifest from SEC EDGAR submissions by scanning recent `10-K` filings and matching PDF filenames that contain one of:

- `letter`
- `shareholder`
- `chairman`

Example:

```bash
python scripts/generate_manifest_from_sec.py --tickers AAPL MSFT AMZN JPM --years 10
```

This writes `manifests/letters_manifest.sec.csv` with rows in the standard manifest schema (`company_id`, `company_name`, `document_type`, `year`, `source_type`, `url`).

## Testing

Run:

```bash
python -m unittest discover -s tests
```
