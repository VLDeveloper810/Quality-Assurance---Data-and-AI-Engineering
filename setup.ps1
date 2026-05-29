<#
One-click setup for Windows PowerShell.
Run from the repository root with:
    .\setup.ps1
#>

$ErrorActionPreference = 'Stop'

Write-Host '=== Setting up project environment ==='

if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    Write-Error 'Python is not available on PATH. Install Python 3.9+ and rerun.'
    exit 1
}

if (-not (Test-Path '.venv')) {
    python -m venv .venv
    Write-Host 'Created virtual environment at .venv'
} else {
    Write-Host '.venv already exists, skipping creation.'
}

Write-Host 'Installing Python dependencies...'
.\.venv\Scripts\Activate.ps1
pip install --upgrade pip
pip install -r requirements.txt

if (-not (Test-Path '.env')) {
    Copy-Item -Path 'example.env' -Destination '.env'
    Write-Host 'Created .env from example.env. Please update it with your AWS settings.'
} else {
    Write-Host '.env already exists, leaving it unchanged.'
}

foreach ($folder in @('iceberg_warehouse', 'mlruns')) {
    if (-not (Test-Path $folder)) {
        New-Item -ItemType Directory -Path $folder | Out-Null
        Write-Host "Created folder: $folder"
    } else {
        Write-Host "Folder exists: $folder"
    }
}

Write-Host 'Setup complete. To activate the virtual environment:'
Write-Host '    .\.venv\Scripts\Activate.ps1'
Write-Host 'Then run:'
Write-Host '    python trigger_pipeline.py'
