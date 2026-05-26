from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.signals.sentiment_signal import build_sentiment_signal

DEFAULT_INPUT_PATH = Path("features/sentiment_stability.csv")
DEFAULT_OUTPUT_PATH = Path("features/sentiment_signals.csv")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build persisted sentiment signals from features/sentiment_stability.csv "
            "and write features/sentiment_signals.csv."
        )
    )
    parser.add_argument("--input-path", type=Path, default=DEFAULT_INPUT_PATH)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_OUTPUT_PATH)
    return parser.parse_args()


def run(input_path: Path = DEFAULT_INPUT_PATH, output_path: Path = DEFAULT_OUTPUT_PATH) -> pd.DataFrame:
    frame = pd.read_csv(input_path, dtype={"company_id": "string", "year": "string"})
    signals = build_sentiment_signal(frame)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    signals.to_csv(output_path, index=False)
    return signals


def main() -> None:
    args = parse_args()
    signals = run(input_path=args.input_path, output_path=args.output_path)
    print(f"Wrote {len(signals)} signal row(s) to {args.output_path}.")


if __name__ == "__main__":
    main()
