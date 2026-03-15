# Dataset Summary Report

Generate a summary of downloaded letters from the `output/` directory:

```bash
python scripts/generate_dataset_summary.py
```

This writes `reports/dataset_summary.json` with:

- `number_of_companies` / `total_companies`
- `total_letters`
- `year_range`
- `letters_per_company`
- `missing_years`

You can override paths:

```bash
python scripts/generate_dataset_summary.py --output-root output --report-path reports/dataset_summary.json
```
