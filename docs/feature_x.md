# Feature X: Row Filtering by Company and Year

This feature adds CLI filters to limit manifest rows before download/render.

## New CLI options

- `--company <company_id>`: keep only rows matching the company.
- `--year <year>`: keep only rows for a specific year.
- `--year-start <year>`: keep only rows with `year >= year-start`.
- `--year-end <year>`: keep only rows with `year <= year-end`.

## Validation rules

- `--year` cannot be combined with `--year-start` or `--year-end`.
- `--year-start` cannot be greater than `--year-end`.
- If filters produce zero rows, the command fails with `ManifestValidationError`.

## Examples

Run a single company and year:

```bash
python export_letters.py --company berkshire_hathaway --year 2023
```

Run a year range for all companies:

```bash
python export_letters.py --year-start 2020 --year-end 2023
```
