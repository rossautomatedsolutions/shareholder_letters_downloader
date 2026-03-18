from pathlib import Path


def get_project_root():
    path = Path(__file__).resolve()
    return path.parents[2]


BASE_DIR = get_project_root()
FEATURES_DIR = BASE_DIR / "features"
OUTPUT_DIR = BASE_DIR / "output"
