#Requires -Version 5.1
<#
.SYNOPSIS
    Convenience launcher for the PPAC Dataverse Inventory.
    Run this script from the PPACInventoryManagement folder.
.DESCRIPTION
    Runs prerequisites check, then launches the full inventory collection,
    then generates the HTML report and opens it in your browser.

    All operations are READ-ONLY.
#>
[CmdletBinding()]
param(
    [string]$TenantId           = '',
    [string]$EnvironmentFilter  = '',
    [switch]$IncludeEntityCounts = $true,
    [switch]$IncludeFO           = $true,
    [switch]$Force,
    [switch]$SkipPrereqCheck,
    [switch]$UseDeviceCode,
    [switch]$NoTranscript
)

$ErrorActionPreference = 'Stop'
$Root      = $PSScriptRoot
$DataPath  = Join-Path $Root 'data'
$ScriptDir = Join-Path $Root 'scripts'
$LogsDir   = Join-Path $DataPath 'logs'

# ── Transcript: capture everything that reaches the host ─────────────────────
# Start-Transcript records Write-Host, Write-Warning, uncaught exceptions, and
# stdout from child scripts (prereqs, collector, report generator). It runs in
# addition to the structured inventory.log that Write-InventoryLog writes to.
# Opt out with -NoTranscript (e.g. when piping output yourself).
$transcriptActive = $false
if (-not $NoTranscript) {
    if (-not (Test-Path $LogsDir)) { $null = New-Item -ItemType Directory -Path $LogsDir -Force }
    $transcriptPath = Join-Path $LogsDir ("session-$(Get-Date -Format 'yyyyMMdd-HHmmss').log")
    try {
        Start-Transcript -Path $transcriptPath -Force | Out-Null
        $transcriptActive = $true
    } catch {
        Write-Host "WARN: could not start transcript: $_" -ForegroundColor Yellow
    }
}

try {
    Write-Host ''
    Write-Host '============================================================' -ForegroundColor Cyan
    Write-Host '  PPAC Dataverse Inventory - Quick Start' -ForegroundColor Cyan
    Write-Host '  READ-ONLY: No changes will be made to any environment.' -ForegroundColor Green
    Write-Host '============================================================' -ForegroundColor Cyan
    if ($transcriptActive) {
        Write-Host "  Transcript: $transcriptPath" -ForegroundColor DarkGray
    }
    Write-Host ''

    # Step 1: Prerequisites
    if (-not $SkipPrereqCheck) {
        Write-Host 'Step 1/3 - Checking prerequisites...' -ForegroundColor Yellow
        & "$ScriptDir\00_Prerequisites.ps1"
    }

    # Step 2: Collect data
    Write-Host ''
    Write-Host 'Step 2/3 - Collecting inventory data...' -ForegroundColor Yellow
    Write-Host '  This may take 30-120 minutes for large tenants.' -ForegroundColor DarkGray

    $collectParams = @{
        OutputPath          = $DataPath
        IncludeEntityCounts = $IncludeEntityCounts
        IncludeFO           = $IncludeFO
        Force               = $Force
        UseDeviceCode       = $UseDeviceCode
    }
    if ($TenantId)          { $collectParams['TenantId']         = $TenantId }
    if ($EnvironmentFilter) { $collectParams['EnvironmentFilter']= $EnvironmentFilter }

    & "$ScriptDir\Invoke-DataverseInventory.ps1" @collectParams

    # Step 3: Generate report
    Write-Host ''
    Write-Host 'Step 3/3 - Generating HTML report...' -ForegroundColor Yellow

    & "$ScriptDir\Generate-Report.ps1" -DataPath $DataPath -OpenReport

    Write-Host ''
    Write-Host 'Done! Check the reports\ folder for your HTML report.' -ForegroundColor Green
}
finally {
    if ($transcriptActive) {
        try { Stop-Transcript | Out-Null } catch {}
    }
}
