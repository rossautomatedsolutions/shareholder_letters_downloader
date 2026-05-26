import csv
import subprocess
import sys
from pathlib import Path

import pandas as pd

from scripts.build_sentiment_signals import run


def test_run_writes_signal_rows(tmp_path: Path) -> None:
    input_path = tmp_path / "sentiment_stability.csv"
    output_path = tmp_path / "sentiment_signals.csv"

    pd.DataFrame(
        [
            {"company_id": "acme", "year": "2020", "sentiment_score": 0.10, "sentiment_deviation": 0.40},
            {"company_id": "acme", "year": "2021", "sentiment_score": 0.20, "sentiment_deviation": 0.10},
            {"company_id": "acme", "year": "2022", "sentiment_score": 0.30, "sentiment_deviation": 0.70},
        ]
    ).to_csv(input_path, index=False)

    result = run(input_path=input_path, output_path=output_path)

    assert len(result) == 3
    with output_path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert rows[0]["signal"] == "neutral"
    assert rows[1]["signal"] == "bullish"
    assert rows[2]["signal"] == "bearish"


def test_cli_runs_from_repo_root_without_pythonpath(tmp_path: Path) -> None:
    input_path = tmp_path / "sentiment_stability.csv"
    output_path = tmp_path / "sentiment_signals.csv"
    repo_root = Path(__file__).resolve().parents[1]

    pd.DataFrame(
        [
            {"company_id": "acme", "year": "2020", "sentiment_score": 0.10, "sentiment_deviation": 0.40},
            {"company_id": "acme", "year": "2021", "sentiment_score": 0.20, "sentiment_deviation": 0.10},
        ]
    ).to_csv(input_path, index=False)

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/build_sentiment_signals.py",
            "--input-path",
            str(input_path),
            "--output-path",
            str(output_path),
        ],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert output_path.exists()
