# Shareholder Letters Downloader

A deterministic research pipeline for building a structured dataset of shareholder letters across multiple companies. The project ingests source documents from a normalized manifest, downloads and organizes the underlying files, extracts machine-readable text, computes reusable NLP-style features, and converts those features into simple interpretable signals for downstream analysis.

The repository exists to make shareholder-letter research reproducible. Instead of treating annual letters as ad hoc documents, it models them as a consistent time series: each company-year observation can be validated, extracted, transformed into features, and compared over time without introducing hidden manual steps or lookahead bias.

## Overview

This project turns raw shareholder-letter links into a research-ready dataset. Starting from a manifest of company, year, and source metadata, the pipeline validates each record, downloads or renders the source document, normalizes the output layout, and extracts text into a consistent folder structure. From there, the repository builds sentiment and keyword features, computes stability and deviation metrics over time, and derives simple sentiment-based signals.

The emphasis is on deterministic execution. Given the same manifest and source files, the pipeline produces the same normalized outputs and feature tables, making it suitable for portfolio projects, systematic research workflows, and backtesting-oriented experimentation.

## Architecture

The project is organized as a linear research pipeline:

```text
Data -> Extraction -> Features -> Signals
```

### 1. Data

A manifest defines the universe of documents to ingest. Each row includes the company identifier, document type, year, source type, and URL. Validation enforces required columns, type constraints, and uniqueness of `(company_id, document_type, year)` before ingestion begins.

### 2. Extraction

The ingestion step downloads or renders the original shareholder letter into a normalized PDF layout. A text-extraction step then reads those PDFs and writes plain-text outputs plus lightweight metadata such as character count and word count.

### 3. Features

Feature builders transform extracted text into structured tabular outputs:

- sentiment scores based on positive and negative lexicons
- keyword frequency tables
- sentiment deviation and stability features computed over time within each company

### 4. Signals

The signal layer converts feature tables into simple directional labels. In the current implementation, expanding within-company percentile ranks are used to classify observations into `bullish`, `neutral`, or `bearish` buckets based on sentiment deviation, preserving a no-lookahead workflow.

## Features

- Multi-company ingestion from a unified manifest
- Deterministic manifest validation and normalization
- PDF-to-text extraction with metadata sidecars
- Sentiment and keyword feature generation
- Stability and deviation modeling over time
- Signal generation from sentiment-derived features

## Project Structure

### `manifests/`

Stores the primary input manifest and supporting templates. This is the entry point for defining which shareholder letters belong in the dataset.

### `scripts/`

Contains command-line utilities for each pipeline stage, including manifest cleaning, text extraction, keyword generation, sentiment feature generation, and stability calculations.

### `src/`

Holds the reusable Python modules for feature engineering and signal construction. This is where the core logic for stability modeling and signal generation lives.

### `features/`

Output location for generated feature tables such as sentiment scores, keyword counts, and sentiment stability metrics.

### `tests/`

Automated tests covering ingestion, manifest validation, extraction, feature engineering, and signal behavior. The suite is designed to protect deterministic behavior and no-lookahead assumptions.

## Usage

### 1. Ingestion

Download or render shareholder letters from the manifest:

```bash
python export_letters.py
```

Run for a single company:

```bash
python export_letters.py --company berkshire_hathaway
```

Optionally validate URLs before running ingestion:

```bash
python export_letters.py --preflight-urls
```

### 2. Text extraction

Convert normalized PDFs into plain text and metadata files:

```bash
python scripts/extract_text_from_letters.py
```

### 3. Feature generation

Build sentiment features:

```bash
python scripts/build_sentiment_features.py
```

Build keyword features:

```bash
python scripts/build_keyword_features.py
```

Build sentiment stability and deviation features:

```bash
python scripts/build_sentiment_stability.py
```

### 4. Signal generation

Signals are generated from the sentiment feature set plus sentiment deviation. The repository exposes this through the reusable signal module in `src/signals/sentiment_signal.py`:

```bash
python - <<'PY'
import pandas as pd
from src.signals.sentiment_signal import build_sentiment_signal

frame = pd.read_csv("features/sentiment_stability.csv")
signals = build_sentiment_signal(frame)
print(signals.head())
PY
```

## Example Output

### `features/sentiment_features.csv`

```csv
company_id,year,sentiment_score
acme_inc,2022,0.500000
beta_corp,2021,-0.500000
```

### Signal output

```csv
company_id,year,sentiment_score,sentiment_deviation,sentiment_rank,deviation_rank,signal
acme,2020,0.10,0.40,0.5,0.5,neutral
acme,2021,0.20,0.10,1.0,0.0,bullish
beta,2020,-0.10,0.70,0.5,0.5,neutral
```

These outputs are intentionally simple: feature tables remain easy to audit, and signal outputs stay interpretable enough for downstream backtests or portfolio research notebooks.

## Testing

Run the full automated suite with `pytest`:

```bash
pytest
```

Run a specific test module:

```bash
pytest tests/test_sentiment_signal.py
```

The test suite validates manifest rules, extraction behavior, feature calculations, stability logic, and signal construction. In particular, several tests verify that the rolling and expanding computations do not rely on future observations.

## Design Principles

### Deterministic pipeline

The pipeline favors explicit schemas, fixed output paths, and repeatable transformations so results can be reproduced reliably.

### No lookahead bias

Time-series feature engineering and signal generation are built using expanding historical information only, which keeps the outputs aligned with realistic research and backtesting constraints.

### Modular architecture

Each pipeline stage is separated into focused scripts and reusable modules, making it easy to extend or swap individual components.

### Test-driven development

The repository includes a broad automated test suite to protect expected behavior as the project evolves.

## Future Work

- Expand manifest coverage across more companies, sectors, and geographies
- Add richer NLP techniques, including LLM-assisted document analysis and topic extraction
- Integrate generated signals with downstream backtesting or trading-system workflows
