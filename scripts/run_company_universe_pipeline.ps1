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

function Resolve-RepoPath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$PathValue
    )

    if ([System.IO.Path]::IsPathRooted($PathValue)) {
        return [System.IO.Path]::GetFullPath($PathValue)
    }

    return [System.IO.Path]::GetFullPath((Join-Path $repoRoot $PathValue))
}

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
$repoRoot = [System.IO.Path]::GetFullPath($repoRoot)
$manifestOutputStem = [System.IO.Path]::GetFileNameWithoutExtension($ManifestPath)
Set-Location -LiteralPath $repoRoot

$resolvedManifest = Resolve-RepoPath -PathValue $ManifestPath
if (-not (Test-Path -LiteralPath $resolvedManifest)) {
    throw "Manifest not found: $resolvedManifest"
}

$resolvedOutputRoot = Resolve-RepoPath -PathValue $OutputRoot
$resolvedTextOutputRoot = Resolve-RepoPath -PathValue $TextOutputRoot
$resolvedFeaturesDir = Resolve-RepoPath -PathValue $FeaturesDir

$logsDir = Join-Path $repoRoot "logs"
New-Item -ItemType Directory -Path $logsDir -Force | Out-Null
$timestamp = Get-Date -Format "yyyyMMddTHHmmss"
$logPath = Join-Path $logsDir "company_universe_pipeline_$timestamp.log"
Start-Transcript -Path $logPath -Force | Out-Null

try {
    $cleanManifestPath = Join-Path $repoRoot ("manifests\{0}.cleaned.csv" -f $manifestOutputStem)
    $rejectedPath = Join-Path $repoRoot ("reports\{0}.rejected_rows.csv" -f $manifestOutputStem)

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
        "--output-root", $resolvedOutputRoot,
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
            --input-root $resolvedOutputRoot `
            --output-root $resolvedTextOutputRoot `
            --document-type $DocumentType
        if ($LASTEXITCODE -ne 0) {
            throw "Text extraction failed for document_type=$DocumentType with exit code $LASTEXITCODE"
        }
    }

    Write-Host "Building sentiment features"
    & python "scripts\build_sentiment_features.py" --input-root $resolvedTextOutputRoot --output-path (Join-Path $resolvedFeaturesDir "sentiment_features.csv")
    if ($LASTEXITCODE -ne 0) {
        throw "Sentiment feature build failed with exit code $LASTEXITCODE"
    }

    Write-Host "Building keyword features"
    & python "scripts\build_keyword_features.py" --input-root $resolvedTextOutputRoot --output-path (Join-Path $resolvedFeaturesDir "keyword_features.csv")
    if ($LASTEXITCODE -ne 0) {
        throw "Keyword feature build failed with exit code $LASTEXITCODE"
    }

    Write-Host "Building stability features"
    & python "scripts\build_sentiment_stability.py" `
        --input-path (Join-Path $resolvedFeaturesDir "sentiment_features.csv") `
        --output-path (Join-Path $resolvedFeaturesDir "sentiment_stability.csv")
    if ($LASTEXITCODE -ne 0) {
        throw "Sentiment stability build failed with exit code $LASTEXITCODE"
    }

    Write-Host "Building signal output"
    & python "scripts\build_sentiment_signals.py" `
        --input-path (Join-Path $resolvedFeaturesDir "sentiment_stability.csv") `
        --output-path (Join-Path $resolvedFeaturesDir "sentiment_signals.csv")
    if ($LASTEXITCODE -ne 0) {
        throw "Signal build failed with exit code $LASTEXITCODE"
    }

    Write-Host ""
    Write-Host "Pipeline completed successfully."
    Write-Host "Validated manifest: $cleanManifestPath"
    Write-Host "Rejected rows: $rejectedPath"
    Write-Host "Normalized PDFs: $resolvedOutputRoot"
    Write-Host "Extracted text: $resolvedTextOutputRoot"
    Write-Host "Sentiment features: $(Join-Path $resolvedFeaturesDir 'sentiment_features.csv')"
    Write-Host "Keyword features: $(Join-Path $resolvedFeaturesDir 'keyword_features.csv')"
    Write-Host "Stability features: $(Join-Path $resolvedFeaturesDir 'sentiment_stability.csv')"
    Write-Host "Signal output: $(Join-Path $resolvedFeaturesDir 'sentiment_signals.csv')"
    Write-Host "Log file: $logPath"
}
finally {
    Stop-Transcript | Out-Null
}
