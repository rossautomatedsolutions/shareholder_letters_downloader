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

## Testing

Run:

```bash
python -m unittest discover -s tests
```
