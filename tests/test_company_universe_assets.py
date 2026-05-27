from pathlib import Path

try:
    import pandas as pd
except ModuleNotFoundError:  # pragma: no cover
    pd = None


def test_pilot_template_lists_expected_companies() -> None:
    assert pd is not None
    repo_root = Path(__file__).resolve().parents[1]
    pilot_path = repo_root / "manifests" / "company_universe_pilot.template.csv"
    frame = pd.read_csv(pilot_path, dtype=str, keep_default_na=False)

    assert frame["company_id"].tolist() == [
        "berkshire_hathaway",
        "amazon",
        "markel",
    ]


def test_full_template_lists_expected_companies() -> None:
    assert pd is not None
    repo_root = Path(__file__).resolve().parents[1]
    full_path = repo_root / "manifests" / "company_universe_full.template.csv"
    frame = pd.read_csv(full_path, dtype=str, keep_default_na=False)

    assert frame["company_id"].tolist() == [
        "berkshire_hathaway",
        "costco",
        "amazon",
        "apple",
        "meta",
        "alphabet",
        "danaher",
        "constellation_software",
        "markel",
        "brookfield",
    ]


def test_runner_uses_strict_mode_and_creates_logs() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    runner_path = repo_root / "scripts" / "run_company_universe_pipeline.ps1"
    contents = runner_path.read_text(encoding="utf-8")

    assert contents.lstrip().startswith("[CmdletBinding()]")
    assert "Set-StrictMode -Version Latest" in contents
    assert '$logsDir = Join-Path $repoRoot "logs"' in contents
    assert "$NormalizedCompanies = @()" in contents
    assert "($CompanyEntry -split ',')" in contents
    assert '$runMultipleArgs += "--companies"' in contents
    assert "$runMultipleArgs += $NormalizedCompanies" in contents
    assert "Import-Csv -LiteralPath $cleanManifestPath" in contents
    assert "--document-type $DocumentType" in contents
    assert "Start-Transcript" in contents
    assert "Resolve-RepoPath" in contents
    assert "Set-Location -LiteralPath $repoRoot" in contents
    assert "Run this script from the repo root" not in contents
    assert "throw " in contents
