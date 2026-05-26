# Company Universe Runbook

## Current status

- One-company smoke run: **READY**
- 3-company pilot: **READY** for the current Berkshire/Amazon/Markel source set, subject to the text-quality gate below
- Full 10-company universe: **MOSTLY READY** operationally, but still requires reviewed source rows and row-by-row text-quality review

The repo is ready to validate manifests, download a real Berkshire row, extract text, build features, and persist signal output from repo-root Python commands without requiring `PYTHONPATH`. The remaining gating item before a 3-company or 10-company live run is reviewed manifest coverage for non-Berkshire companies.

## Key paths

- Normalized PDFs: `output/<company_id>/<document_type>/<year>.pdf`
- Download metadata: `output/<company_id>/<document_type>/<year>.metadata.json`
- Extracted text: `output_text/<company_id>/<year>.txt`
- Extracted text metadata: `output_text/<company_id>/<year>.json`
- Sentiment features: `features/sentiment_features.csv`
- Keyword features: `features/keyword_features.csv`
- Stability features: `features/sentiment_stability.csv`
- Signal output: `features/sentiment_signals.csv`
- Logs from the PowerShell runner: `logs/company_universe_pipeline_<timestamp>.log`

## Manifest assets

- Shipped Berkshire manifest: `manifests/letters_manifest.csv`
- Pilot template: `manifests/company_universe_pilot.template.csv`
- Full-universe template: `manifests/company_universe_full.template.csv`

### Pilot template contents

- Berkshire Hathaway
- Amazon
- Markel

### Full-universe template contents

- Berkshire Hathaway
- Costco
- Amazon
- Apple
- Meta
- Alphabet
- Danaher
- Constellation Software
- Markel
- Brookfield

## Manifest validation

Validate the default shipped manifest:

```powershell
python scripts\validate_and_clean_manifest.py
```

Validate a custom pilot or full-universe manifest:

```powershell
python scripts\validate_and_clean_manifest.py --input-path manifests\my_manifest.csv --clean-output-path manifests\my_manifest.cleaned.csv --rejected-output-path reports\my_manifest.rejected.csv
```

Expected result:

- cleaned manifest written to the requested `*.cleaned.csv`
- rejected rows written to the requested rejected-row CSV
- console summary of scanned, accepted, rejected, and duplicate rows

## One-company smoke run

Use Berkshire first because the shipped manifest already contains a verified real row.

```powershell
python scripts\validate_and_clean_manifest.py
python export_letters.py --manifest manifests\letters_manifest.csv --company berkshire_hathaway --year 2024
python scripts\extract_text_from_letters.py --input-root output --output-root output_text --company berkshire_hathaway --document-type chairman_letter
python scripts\build_sentiment_features.py --input-root output_text --output-path features\sentiment_features.csv --company berkshire_hathaway
python scripts\build_keyword_features.py --input-root output_text --output-path features\keyword_features.csv --company berkshire_hathaway
python scripts\build_sentiment_stability.py --input-path features\sentiment_features.csv --output-path features\sentiment_stability.csv
python scripts\build_sentiment_signals.py --input-path features\sentiment_stability.csv --output-path features\sentiment_signals.csv
```

## 3-company pilot

### Step 1: prepare reviewed manifest rows

Copy the pilot template and replace placeholder URLs before validation:

```powershell
Copy-Item manifests\company_universe_pilot.template.csv manifests\company_universe_pilot.csv
```

Required edits before running:

- Replace `REPLACE_WITH_REVIEWED_URL` for Amazon.
- Replace `REPLACE_WITH_REVIEWED_URL` for Markel.
- If Markel is not the preferred third company, replace the Markel row with Constellation Software and a reviewed URL.

### Step 2: validate the pilot manifest

```powershell
python scripts\validate_and_clean_manifest.py --input-path manifests\company_universe_pilot.csv --clean-output-path manifests\company_universe_pilot.cleaned.csv --rejected-output-path reports\company_universe_pilot.rejected.csv
```

### Step 3: run the pilot

Recommended low-error path:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_company_universe_pipeline.ps1 -ManifestPath manifests\company_universe_pilot.csv -Companies berkshire_hathaway,amazon,markel
```

If using Constellation Software instead of Markel:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_company_universe_pipeline.ps1 -ManifestPath manifests\company_universe_pilot.csv -Companies berkshire_hathaway,amazon,constellation_software
```

### Step 4: run the text-quality gate

Run the lightweight extraction check before trusting the generated feature files downstream:

```powershell
python scripts\validate_text_quality.py --input-root output_text --output-path reports\text_quality_report.csv --companies berkshire_hathaway amazon markel
```

Expected result for a healthy pilot:

- exit code `0`
- one `pass` row per company-year in `reports\text_quality_report.csv`
- char and word counts comfortably above the thresholds
- no obvious error-page phrases such as `access denied`, `not found`, or `redirecting`

## Full 10-company universe

### Step 1: prepare reviewed full manifest

Copy the full template and replace every placeholder URL before validation:

```powershell
Copy-Item manifests\company_universe_full.template.csv manifests\company_universe_full.csv
```

### Step 2: validate the full manifest

```powershell
python scripts\validate_and_clean_manifest.py --input-path manifests\company_universe_full.csv --clean-output-path manifests\company_universe_full.cleaned.csv --rejected-output-path reports\company_universe_full.rejected.csv
```

### Step 3: launch only after manual source review is complete

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_company_universe_pipeline.ps1 -ManifestPath manifests\company_universe_full.csv -Companies berkshire_hathaway,costco,amazon,apple,meta,alphabet,danaher,constellation_software,markel,brookfield
```

This run is intentionally not recommended until the manual review checklist below is complete for every company-year row.

## Expected outputs

For a successful company-year ingestion:

- normalized PDF at `output/<company_id>/<document_type>/<year>.pdf`
- metadata sidecar at `output/<company_id>/<document_type>/<year>.metadata.json`
- extracted text at `output_text/<company_id>/<year>.txt`
- extracted text metadata at `output_text/<company_id>/<year>.json`

For a successful pipeline run:

- `features/sentiment_features.csv`
- `features/keyword_features.csv`
- `features/sentiment_stability.csv`
- `features/sentiment_signals.csv`

## Manual text-quality review checklist

Review at least one row per company before downstream use, and review every row that was manually curated or came from a heterogeneous source type.

- Confirm the normalized PDF is the shareholder letter or annual-report letter section intended for analysis, not a proxy, transcript, deck, or earnings supplement.
- Confirm the document year matches the manifest year.
- Confirm the extracted text is not blank or severely truncated.
- Confirm the extracted text is legible English prose rather than OCR noise or navigation chrome.
- Confirm the first page/first paragraphs reflect the expected company and reporting period.
- Confirm tables, appendices, and boilerplate did not overwhelm the narrative letter portion.
- Confirm HTML-rendered documents preserve the actual letter content and not only shell page markup.
- Confirm annual-report PDFs still contain the intended shareholder letter narrative if they are being used instead of a standalone letter.
- Confirm the automated `reports/text_quality_report.csv` row is `pass` before using a company-year downstream.

## Operator notes

- `python scripts\validate_features.py` is expected to warn before the pipeline is run because feature files do not exist yet.
- `python scripts\sanity_check_pipeline.py` is expected to report zero outputs before the first successful run.
- `python scripts\validate_text_quality.py` is a lightweight gate, not a replacement for manual review.
- The PowerShell runner stops on failure, creates a timestamped log, and prints the output locations at the end.
- Do not launch the full 10-company run until reviewed URLs are filled in and manual text-quality review has passed for the pilot companies.
