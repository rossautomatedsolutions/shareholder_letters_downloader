# Company Universe Readiness Review

## Executive summary

**MOSTLY READY**

The repo is now operator-ready for a controlled one-company smoke run and mostly ready for a documented 3-company pilot. The earlier downloader and manifest-validator blockers have been fixed, the scraper dependency has been declared, and the automated test suite is green in this environment.

The repo is **not yet fully ready** for an unsupervised full 10-company live run because source curation remains heterogeneous across the requested universe. Berkshire is already runnable from the shipped manifest, while Amazon/Markel/Constellation and the broader 10-company set still require reviewed manifest rows or a validated manual manifest before launch.

## Follow-up status (2026-05-26)

### Fixed in the readiness repair pass

- `export_letters.py` now calls `fetch_binary()` with the correct arguments.
- PDF downloads are copied into the normalized output path used by downstream steps.
- `scripts/validate_and_clean_manifest.py` now accepts the shipped manifest schema and defaults missing `confidence_score` values to `1.0`.
- `beautifulsoup4` is declared in `requirements.txt`.
- Focused downloader and manifest tests were updated.
- A real one-row Berkshire downloader smoke run succeeded.
- A persisted signal-build step and an operator PowerShell runner are now available.
- `scripts/build_sentiment_signals.py` now uses the same repo-root import pattern as the other working CLI scripts, so it runs from repo root without user-managed `PYTHONPATH`.

### Current status

- One-company smoke run: **READY**
- 3-company pilot: **READY** for the current Berkshire/Amazon/Markel manifest rows
- Full 10-company universe: **MOSTLY READY** operationally, pending final source curation and text-quality review

### Pilot source and text-quality follow-up

- Berkshire Hathaway 2024 resolved to the intended Buffett shareholder letter PDF and extracted to roughly 40k characters / 5.8k words.
- Amazon 2024 resolved to the intended shareholder letter PDF and extracted to roughly 41k characters / 6.9k words.
- Markel 2024 resolved to the intended shareholder letter PDF and extracted to roughly 45k characters / 7.5k words.
- The earlier apparent mojibake seen in PowerShell output was a console-display artifact, not corrupted text on disk. Reading the UTF-8 files programmatically shows valid punctuation and prose.
- A new lightweight quality gate is now available at `scripts/validate_text_quality.py` to flag short extracts and obvious error-page content before downstream consumption.

### Why the 3-company pilot is not marked fully READY yet

- Berkshire is ready from the shipped manifest.
- The pilot workflow is documented and the runner is in place.
- The remaining operator step is to fill reviewed URLs into `manifests/company_universe_pilot.template.csv` or an equivalent manual manifest for Amazon plus Markel or Constellation Software before launch.

### Why the full 10-company run is still not marked READY

- The full-universe template is now documented, but reviewed live-source rows are still required for most of the target companies.
- Source heterogeneity across annual-report PDFs, standalone letters, and HTML investor pages still warrants manual text-quality review before downstream consumption.

## Audit scope

Inspected:

- `README.md`
- `export_letters.py`
- `manifests/`
- `scripts/`
- `src/`
- `tests/`
- existing repo outputs/directories

Validation executed:

- `pytest`
- `python scripts/sanity_check_pipeline.py`
- `python scripts/validate_features.py`
- `python scripts/validate_and_clean_manifest.py`
- `python export_letters.py --manifest manifests\letters_manifest.csv --company berkshire_hathaway --year 2024 --preflight-urls`

## Current pipeline status

| Area | Status | Notes |
| --- | --- | --- |
| Manifest schema and validation | **Partial / inconsistent** | `export_letters.py` accepts `HTML` and `PDF` and only requires core columns. `scripts/validate_and_clean_manifest.py` requires `confidence_score`, only allows `PDF`, and only accepts `document_type=shareholder_letter`. It fails immediately on the checked-in `manifests/letters_manifest.csv` because that file has no `confidence_score` column. |
| Company universe support | **Partial** | Unified manifest can represent multiple companies cleanly. Auto IR-page generator includes Berkshire, Amazon, Apple, Alphabet, Meta, Costco, but not Danaher, Constellation Software, Markel, or Brookfield. Manual manifest merge path exists. |
| URL/source preflight support | **Exists** | `export_letters.py --preflight-urls` is implemented. In sandbox it returned `connection_error`; outside sandbox it passed for Berkshire 2024. The command is not preflight-only; after preflight it proceeds into ingestion. |
| Document download/render support | **Implemented, but blocked at runtime** | Downloader supports `PDF` and `HTML` branches conceptually, including Playwright HTML-to-PDF rendering. Real run on Berkshire 2024 failed with `fetch_binary() missing 2 required positional arguments: 'timeout_seconds' and 'retries'`. |
| PDF normalization/output paths | **Implemented** | Normalized PDFs are intended to land at `output/<company_id>/<document_type>/<year>.pdf`. Raw artifacts go under `output/raw/...`. Metadata sidecars sit beside normalized PDFs. |
| Text extraction | **Ready** | `scripts/extract_text_from_letters.py` extracts PDF text to `output_text/<company_id>/<year>.txt` plus JSON metadata. Tests pass. |
| Metadata sidecars | **Ready** | `export_letters.py` writes download metadata; `scripts/extract_letter_metadata.py` enriches PDF-side metadata with page count, file size, detected year, and preview text. Tests pass. |
| Sentiment feature generation | **Ready** | `scripts/build_sentiment_features.py` writes `features/sentiment_features.csv`. Tests pass. |
| Keyword feature generation | **Ready** | `scripts/build_keyword_features.py` writes `features/keyword_features.csv`. Tests pass. |
| Sentiment stability/deviation features | **Ready** | `src/features/sentiment_stability.py` and `scripts/build_sentiment_stability.py` are implemented and tested. |
| Signal generation | **Mostly ready** | `src/signals/sentiment_signal.py` is implemented and tested. There is no dedicated CLI that persists signal CSV output; current usage is via Python module call. |
| Deterministic / no-lookahead behavior | **Ready downstream** | Tests explicitly cover no-lookahead behavior in sentiment stability and signal generation. Sorting/grouping uses stable ordering. |
| Downstream-consumable output formats | **Ready** | Outputs are plain CSV/JSON/TXT/PDF and easy to consume in notebooks, research code, or later ETL. |

## What already exists and works

- Multi-company manifest-driven design already exists.
- Ingestion code already has separate paths for raw artifacts, normalized PDFs, and run reports.
- HTML source handling is already present in the downloader design.
- PDF text extraction is implemented with deterministic file naming.
- PDF metadata enrichment is implemented.
- Sentiment and keyword feature builders are implemented and tested.
- Sentiment stability/deviation feature generation is implemented and tested.
- No-lookahead signal logic is implemented and tested.
- There is an existing batch runner for multiple companies: `scripts/run_multiple_companies.py`.
- There are manifest-generation helpers for IR pages, SEC filings, and merging manifests.

## What is missing or inconsistent

1. The live ingestion path is broken for PDF downloads.

- Evidence: running the documented preflight command for Berkshire 2024 created a run report with:
  `fetch_binary() missing 2 required positional arguments: 'timeout_seconds' and 'retries'`
- Impact: cannot reliably ingest even one real PDF row today.

2. The manifest validator does not match the manifest the repo ships.

- Evidence: `python scripts/validate_and_clean_manifest.py` failed with:
  `ValueError: Missing required columns: confidence_score`
- The validator also rejects `HTML` rows and non-`shareholder_letter` document types, while the checked-in manifest contains `HTML` and `chairman_letter`.
- Impact: the "clean/validate manifest" step cannot be used as-is on the main manifest.

3. Source-type support is not coherent across the repo.

- `export_letters.py` supports `HTML` and `PDF`.
- `scripts/generate_manifest_from_sec.py` can emit `HTML` and `PDF`.
- `scripts/validate_and_clean_manifest.py` only allows `PDF`.
- Impact: HTML letters/pages are only partially supported at the repository level.

4. Company coverage for the requested 10-company universe is incomplete in current generators.

- Present in `generate_manifest_from_ir_pages.py`: Berkshire Hathaway, Amazon, Apple, Alphabet, Meta, Costco.
- Missing from the current hard-coded IR company list: Danaher, Constellation Software, Markel, Brookfield.
- Impact: a clean fully-generated 10-company universe is not available without manual manifest rows or a small company-list extension.

5. Archive/IR scraping tests are red in the current environment.

- `pytest` result: **7 failed, 73 passed, 2 skipped**
- All failures were in archive/manifest scraping tests and were caused by `BeautifulSoup` not being available in the environment while the tests only skip on missing `requests`.
- Classification: **dependency/environment issue** plus imperfect test gating.

6. There is no persisted signal-build script.

- Signal logic exists in `src/signals/sentiment_signal.py`.
- README demonstrates in-memory Python usage, not a standard output file such as `features/sentiment_signals.csv`.
- Impact: downstream users can consume signals, but an end-to-end file-producing pipeline step is not yet standardized.

## Exact blockers before running all 10 companies

### Blocker 1: downloader runtime bug

- Category: **code issue**
- Symptom: `export_letters.py` fails on a real row when calling `fetch_binary()`.
- Result: no successful document ingestion run can be trusted until this is fixed.

### Blocker 2: manifest schema mismatch

- Category: **code/design consistency issue**
- Symptom: validator expects `confidence_score`, `PDF`, and `shareholder_letter`, but repo manifest uses `HTML`, `chairman_letter`, and does not include `confidence_score`.
- Result: no single manifest format currently works cleanly across manifest generation, validation, and ingestion.

### Blocker 3: missing explicit support path for 4 requested companies

- Category: **data/source coverage issue**
- Missing from current IR-company generator list: Danaher, Constellation Software, Markel, Brookfield.
- Result: the requested 10-company universe is not representable through the current auto-generator alone.

### Blocker 4: environment dependency gap for archive/HTML discovery

- Category: **dependency/environment issue**
- Symptom: archive/manifest scraping tests fail because `beautifulsoup4` is not installed in this environment.
- Result: source discovery for archive-style pages is not verifiable here without dependency cleanup.

## Manifest/template review for the 10 target companies

### Can the 10 companies be represented cleanly?

**Yes, at the unified manifest level.**

The current unified manifest shape is flexible enough to represent all 10 companies as rows with:

- `company_id`
- `company_name`
- `document_type`
- `year`
- `source_type`
- `url`

The checked-in template also already includes useful optional fields:

- `language`
- `fiscal_year_end`
- `notes`

### What is not clean today

- The validator expects a different schema than the live manifest.
- The auto-generator and SEC generator produce `confidence_score`, but the checked-in main manifest does not include it.
- `document_type` semantics are inconsistent across files: `shareholder_letter`, `chairman_letter`, and `annual_letter` all appear in repo examples.

## Source heterogeneity support

### Standalone shareholder-letter PDFs

**Supported conceptually and operationally close to ready.**

- Direct PDF handling is implemented.
- Current blocker is the `fetch_binary()` runtime bug.

### Annual-report PDFs

**Partially supported.**

- Manifest can represent them via `document_type`.
- IR/SEC generators can discover annual-report-style URLs when they still look like likely shareholder-letter documents.
- Validator currently rejects non-`shareholder_letter` document types.

### HTML letters/pages

**Partially supported.**

- Export path supports `HTML` rows by fetching raw HTML and rendering to PDF with Playwright.
- SEC generator can emit `HTML`.
- Validator currently rejects `HTML`.
- HTML support is therefore present but not repository-consistent.

### Manually reviewed source URLs

**Supported.**

- Merge workflow already anticipates `letters_manifest.manual.csv`.
- This is likely the smallest path for the four missing companies and for tricky investor-relations pages.

## Test and validation results

### Automated tests

`pytest`

- Result: **7 failed, 73 passed, 2 skipped**
- Failures concentrated in:
  - `tests/test_archive_scrapers.py`
  - `tests/test_manifest_generation.py`
- Failure mode:
  - `ModuleNotFoundError: requests and beautifulsoup4 are required...`
- Classification:
  - primarily **dependency/environment issue**
  - secondarily **test-gating issue** because tests skip only on missing `requests`, not missing `bs4`

### Lightweight validation commands

`python scripts/sanity_check_pipeline.py`

- Result:
  - `PDFs: 0`
  - `Texts: 0`
  - `Keyword rows: 0`
  - `Sentiment rows: 0`
- Interpretation: no existing generated dataset is present in the repo worktree.

`python scripts/validate_features.py`

- Result:
  - warning that `features/sentiment_features.csv` is missing
  - warning that `features/keyword_features.csv` is missing
- Interpretation: expected, because no feature outputs have been generated yet.

`python scripts/validate_and_clean_manifest.py`

- Result:
  - failed with `Missing required columns: confidence_score`
- Classification: **code/schema consistency issue**

`python export_letters.py --manifest manifests\letters_manifest.csv --company berkshire_hathaway --year 2024 --preflight-urls`

- In sandbox: failed with `connection_error`
- Outside sandbox: URL preflight succeeded, then command proceeded into ingestion and wrote a failed run report due to:
  - `fetch_binary() missing 2 required positional arguments: 'timeout_seconds' and 'retries'`
- Classification:
  - sandbox result: **environment restriction**
  - outside-sandbox runtime failure: **code issue**

## Recommended smallest next build pass

Do the smallest possible pass focused only on readiness blockers:

1. Reconcile one manifest schema across generator, validator, and downloader.
2. Fix the `fetch_binary()` call/signature mismatch in `export_letters.py`.
3. Make `HTML` support either fully first-class or explicitly unsupported everywhere.
4. Add manual manifest rows for Danaher, Constellation Software, Markel, and Brookfield if auto-discovery is not already trivial.
5. Install or declare `beautifulsoup4` and tighten the scrape-test skip conditions.
6. Optionally add a tiny script that writes `features/sentiment_signals.csv` from `features/sentiment_stability.csv` for cleaner downstream automation.

This is a surgical pass, not a redesign.

## Suggested company universe manifest format

Recommended reconciled schema:

```csv
company_id,company_name,document_type,year,source_type,url,confidence_score,language,fiscal_year_end,notes,source_review_status
```

Recommended conventions:

- `company_id`: stable snake_case identifier
- `document_type`: one of `shareholder_letter`, `chairman_letter`, `annual_report_letter`
- `source_type`: one of `PDF`, `HTML`
- `confidence_score`: numeric string, used only for discovery/validation triage
- `source_review_status`: `auto`, `manual_reviewed`, or `manual_curated`

Why this is the smallest useful format:

- it preserves current repo concepts
- it keeps manual-review metadata without forcing architectural change
- it can represent all 10 companies and all target source forms

## Suggested first 3-company smoke run

After the small blocker pass above, use:

- Berkshire Hathaway
- Amazon
- Apple

Why these three:

- Berkshire Hathaway covers legacy archive behavior and mixed historical source patterns.
- Amazon exercises archive-page discovery behavior.
- Apple is a good representative of a large-cap issuer where annual-report/letter source choice may require explicit review.

## Suggested final 10-company run command sequence

Assuming the schema mismatch and downloader bug are fixed, the smallest practical sequence is:

```bash
python scripts/merge_manifests.py
python scripts/validate_and_clean_manifest.py
python export_letters.py --manifest manifests/letters_manifest.cleaned.csv --preflight-urls
python scripts/run_multiple_companies.py --manifest manifests/letters_manifest.cleaned.csv
python scripts/extract_text_from_letters.py
python scripts/build_sentiment_features.py
python scripts/build_keyword_features.py
python scripts/build_sentiment_stability.py
python scripts/validate_features.py
```

If the validator remains separate from the live manifest shape, then use a manually curated unified manifest and skip the validator until schemas are reconciled.

For an initial staged rollout, run:

```bash
python scripts/run_multiple_companies.py --manifest manifests/letters_manifest.cleaned.csv --companies berkshire_hathaway amazon apple --preflight-urls --stop-on-error
```

Then expand to:

```bash
python scripts/run_multiple_companies.py --manifest manifests/letters_manifest.cleaned.csv --companies berkshire_hathaway costco amazon apple meta alphabet danaher constellation_software markel brookfield --preflight-urls --stop-on-error
```

## Risks around source heterogeneity

- Some companies will expose true standalone shareholder letters.
- Some will only expose annual reports containing the letter.
- Some may expose HTML investor pages rather than direct PDFs.
- Some archive pages use nonstandard link attributes like `data-href`.
- Some sources will likely require manual reviewed URLs even if auto-discovery exists.

The repo is directionally prepared for this heterogeneity, but not yet coherently validated across all stages.

## Downstream output notes

Once ingestion is working, the repo's outputs are straightforward to consume in other processes:

- Normalized source PDFs: `output/<company_id>/<document_type>/<year>.pdf`
- Download metadata: `output/<company_id>/<document_type>/<year>.metadata.json`
- Extracted text: `output_text/<company_id>/<year>.txt`
- Text metadata: `output_text/<company_id>/<year>.json`
- Sentiment features: `features/sentiment_features.csv`
- Keyword features: `features/keyword_features.csv`
- Stability/deviation features: `features/sentiment_stability.csv`

Notes for consumers:

- `company_id` and `year` are the stable join keys across feature tables.
- Current signal generation is module-based, not a standardized persisted CSV step.
- Because downstream feature/signal code is already deterministic and no-lookahead tested, those outputs are the most reusable part of the repo once ingestion is fixed.
