import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.features.sentiment_stability import DEFAULT_INPUT_PATH, DEFAULT_OUTPUT_PATH, run


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build sentiment stability features from features/sentiment_features.csv "
            "and write features/sentiment_stability.csv."
        )
    )
    parser.add_argument("--input-path", type=Path, default=DEFAULT_INPUT_PATH)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_OUTPUT_PATH)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    frame = run(input_path=args.input_path, output_path=args.output_path)
    print(f"Wrote {len(frame)} sentiment stability row(s) to {args.output_path}.")


if __name__ == "__main__":
    main()
