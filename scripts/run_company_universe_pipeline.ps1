[CmdletBinding()]
param(
    [string]$ManifestPath = "manifests\letters_manifest.csv",
    [string[]]$Companies = @(),
    [string]$OutputRoot = "output",
    [string]$TextOutputRoot = "output_text",
    [string]$FeaturesDir = "features"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$NormalizedCompanies = @()
foreach ($CompanyEntry in $Companies) {
    foreach ($SplitValue in ($CompanyEntry -split ',')) {
        $Trimmed = $SplitValue.Trim()
        if ($Trimmed) {
            $NormalizedCompanies += $Trimmed
        }
    }
}

$repoRoot = Split-Path -Parent $PSScriptRoot
$currentLocation = (Get-Location).Path
if (([System.IO.Path]::GetFullPath($currentLocation)) -ne ([System.IO.Path]::GetFullPath($repoRoot))) {
    throw "Run this script from the repo root: $repoRoot"
}

$resolvedManifest = [System.IO.Path]::GetFullPath((Join-Path $repoRoot $ManifestPath))
if (-not (Test-Path -LiteralPath $resolvedManifest)) {
    throw "Manifest not found: $resolvedManifest"
}

$logsDir = Join-Path $repoRoot "logs"
New-Item -ItemType Directory -Path $logsDir -Force | Out-Null
$timestamp = Get-Date -Format "yyyyMMddTHHmmss"
$logPath = Join-Path $logsDir "company_universe_pipeline_$timestamp.log"
Start-Transcript -Path $logPath -Force | Out-Null

try {
    $cleanManifestPath = Join-Path $repoRoot "manifests\letters_manifest.cleaned.csv"
    $rejectedPath = Join-Path $repoRoot "reports\rejected_manifest_rows.csv"

    Write-Host "Validating manifest: $resolvedManifest"
    & python "scripts\validate_and_clean_manifest.py" `
        --input-path $resolvedManifest `
        --clean-output-path $cleanManifestPath `
        --rejected-output-path $rejectedPath
    if ($LASTEXITCODE -ne 0) {
        throw "Manifest validation failed with exit code $LASTEXITCODE"
    }

    $runMultipleArgs = @(
        "scripts\run_multiple_companies.py",
        "--manifest", $cleanManifestPath,
        "--output-root", $OutputRoot,
        "--reports-dir", "reports",
        "--stop-on-error"
    )
    if ($NormalizedCompanies.Count -gt 0) {
        $runMultipleArgs += "--companies"
        $runMultipleArgs += $NormalizedCompanies
    }

    Write-Host "Downloading and normalizing letters"
    & python @runMultipleArgs
    if ($LASTEXITCODE -ne 0) {
        throw "Downloader batch failed with exit code $LASTEXITCODE"
    }

    $documentTypes = Import-Csv -LiteralPath $cleanManifestPath |
        Select-Object -ExpandProperty document_type -Unique

    foreach ($DocumentType in $documentTypes) {
        Write-Host "Extracting text for document_type=$DocumentType"
        & python "scripts\extract_text_from_letters.py" `
            --input-root $OutputRoot `
            --output-root $TextOutputRoot `
            --document-type $DocumentType
        if ($LASTEXITCODE -ne 0) {
            throw "Text extraction failed for document_type=$DocumentType with exit code $LASTEXITCODE"
        }
    }

    Write-Host "Building sentiment features"
    & python "scripts\build_sentiment_features.py" --input-root $TextOutputRoot --output-path (Join-Path $FeaturesDir "sentiment_features.csv")
    if ($LASTEXITCODE -ne 0) {
        throw "Sentiment feature build failed with exit code $LASTEXITCODE"
    }

    Write-Host "Building keyword features"
    & python "scripts\build_keyword_features.py" --input-root $TextOutputRoot --output-path (Join-Path $FeaturesDir "keyword_features.csv")
    if ($LASTEXITCODE -ne 0) {
        throw "Keyword feature build failed with exit code $LASTEXITCODE"
    }

    Write-Host "Building stability features"
    & python "scripts\build_sentiment_stability.py" `
        --input-path (Join-Path $FeaturesDir "sentiment_features.csv") `
        --output-path (Join-Path $FeaturesDir "sentiment_stability.csv")
    if ($LASTEXITCODE -ne 0) {
        throw "Sentiment stability build failed with exit code $LASTEXITCODE"
    }

    Write-Host "Building signal output"
    & python "scripts\build_sentiment_signals.py" `
        --input-path (Join-Path $FeaturesDir "sentiment_stability.csv") `
        --output-path (Join-Path $FeaturesDir "sentiment_signals.csv")
    if ($LASTEXITCODE -ne 0) {
        throw "Signal build failed with exit code $LASTEXITCODE"
    }

    Write-Host ""
    Write-Host "Pipeline completed successfully."
    Write-Host "Validated manifest: $cleanManifestPath"
    Write-Host "Normalized PDFs: $(Join-Path $repoRoot $OutputRoot)"
    Write-Host "Extracted text: $(Join-Path $repoRoot $TextOutputRoot)"
    Write-Host "Sentiment features: $(Join-Path $repoRoot (Join-Path $FeaturesDir 'sentiment_features.csv'))"
    Write-Host "Keyword features: $(Join-Path $repoRoot (Join-Path $FeaturesDir 'keyword_features.csv'))"
    Write-Host "Stability features: $(Join-Path $repoRoot (Join-Path $FeaturesDir 'sentiment_stability.csv'))"
    Write-Host "Signal output: $(Join-Path $repoRoot (Join-Path $FeaturesDir 'sentiment_signals.csv'))"
    Write-Host "Log file: $logPath"
}
finally {
    Stop-Transcript | Out-Null
}
