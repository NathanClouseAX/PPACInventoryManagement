#Requires -Version 5.1
<#
.SYNOPSIS
    Convenience test runner. Checks for Pester v5, installs it if missing,
    then runs all .Tests.ps1 files in this directory.

.PARAMETER Filter
    Optional. Pester -TagFilter or -TestName pattern. Example: 'Format-MB'

.PARAMETER CI
    If specified, exits with a non-zero code on test failure (suitable for CI pipelines).

.PARAMETER SkipInstall
    Skip the automatic Pester v5 installation check.

.EXAMPLE
    # Run all tests
    .\tests\RunTests.ps1

.EXAMPLE
    # Run only the module tests
    .\tests\RunTests.ps1 -Filter 'Get-SafeDirectoryName'

.EXAMPLE
    # CI mode - exits non-zero on failure
    .\tests\RunTests.ps1 -CI
#>
[CmdletBinding()]
param(
    [string]$Filter      = '',
    [switch]$CI,
    [switch]$SkipInstall
)

$ErrorActionPreference = 'Stop'
$testDir = $PSScriptRoot

# ── Ensure Pester v5 is available ─────────────────────────────────────────────
if (-not $SkipInstall) {
    $pester = Get-Module -Name Pester -ListAvailable | Sort-Object Version -Descending | Select-Object -First 1
    if (-not $pester -or $pester.Version.Major -lt 5) {
        Write-Host 'Pester v5 not found. Installing...' -ForegroundColor Yellow
        try {
            # -SkipPublisherCheck needed on some machines where the publisher cert is not trusted
            Install-Module -Name Pester -MinimumVersion 5.0 -Force -SkipPublisherCheck -Scope CurrentUser
            Write-Host 'Pester v5 installed.' -ForegroundColor Green
        } catch {
            Write-Error "Failed to install Pester v5: $_`nInstall manually: Install-Module Pester -Force -SkipPublisherCheck"
            exit 1
        }
    } else {
        Write-Host "Pester $($pester.Version) found." -ForegroundColor DarkGray
    }
}

Import-Module Pester -MinimumVersion 5.0 -Force

# ── Build Pester configuration ────────────────────────────────────────────────
$config = New-PesterConfiguration
$config.Run.Path      = $testDir
$config.Run.PassThru  = $true
$config.Output.Verbosity = 'Detailed'

if ($Filter) {
    $config.Filter.FullNameFilter = "*$Filter*"
}

# ── Run tests ─────────────────────────────────────────────────────────────────
Write-Host ''
Write-Host '============================================================' -ForegroundColor Cyan
Write-Host '  PPAC Inventory - Test Suite' -ForegroundColor Cyan
Write-Host "  Path   : $testDir" -ForegroundColor DarkGray
Write-Host "  Filter : $(if ($Filter) { $Filter } else { '(all)' })" -ForegroundColor DarkGray
Write-Host '============================================================' -ForegroundColor Cyan
Write-Host ''

$result = Invoke-Pester -Configuration $config

# ── Summary ───────────────────────────────────────────────────────────────────
Write-Host ''
Write-Host '============================================================' -ForegroundColor Cyan
Write-Host "  Passed : $($result.PassedCount)" -ForegroundColor Green
Write-Host "  Failed : $($result.FailedCount)" -ForegroundColor $(if ($result.FailedCount -gt 0) { 'Red' } else { 'Green' })
Write-Host "  Skipped: $($result.SkippedCount)" -ForegroundColor DarkGray
Write-Host '============================================================' -ForegroundColor Cyan

if ($CI -and $result.FailedCount -gt 0) {
    exit $result.FailedCount
}
