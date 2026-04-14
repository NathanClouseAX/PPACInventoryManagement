#Requires -Version 5.1
<#
.SYNOPSIS
    Checks and installs prerequisites for PPAC Dataverse Inventory.
.DESCRIPTION
    Verifies PowerShell version, installs required Az modules, and confirms
    you have a working Internet connection before running the main inventory.
.EXAMPLE
    .\00_Prerequisites.ps1
#>
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$PSDefaultParameterValues['*:ErrorAction'] = 'Stop'

Write-Host ''
Write-Host '=========================================================' -ForegroundColor Cyan
Write-Host '   PPAC Dataverse Inventory - Prerequisites Check' -ForegroundColor Cyan
Write-Host '=========================================================' -ForegroundColor Cyan
Write-Host ''

# --- PowerShell version ---
Write-Host 'PowerShell version...' -NoNewline
if ($PSVersionTable.PSVersion.Major -lt 5 -or
    ($PSVersionTable.PSVersion.Major -eq 5 -and $PSVersionTable.PSVersion.Minor -lt 1)) {
    Write-Host ' FAIL' -ForegroundColor Red
    throw "PowerShell 5.1 or later required. Found: $($PSVersionTable.PSVersion)"
}
Write-Host " OK  ($($PSVersionTable.PSVersion))" -ForegroundColor Green

# --- Execution policy ---
Write-Host 'Execution policy...' -NoNewline
$policy = Get-ExecutionPolicy -Scope CurrentUser
if ($policy -in 'Restricted', 'AllSigned') {
    Write-Host ' WARN' -ForegroundColor Yellow
    Write-Host "  Execution policy is '$policy'. You may need to run:" -ForegroundColor Yellow
    Write-Host "  Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser" -ForegroundColor Yellow
} else {
    Write-Host " OK  ($policy)" -ForegroundColor Green
}

# --- Required modules ---
$requiredModules = @(
    @{ Name = 'Az.Accounts';  MinVersion = '2.12.0'; Desc = 'Azure AD authentication + token acquisition' },
    @{ Name = 'Az.Resources'; MinVersion = '6.0.0';  Desc = 'Azure resource enumeration'                  }
)

foreach ($mod in $requiredModules) {
    Write-Host "$($mod.Name) >= $($mod.MinVersion)..." -NoNewline
    $installed = Get-Module -ListAvailable -Name $mod.Name |
                 Where-Object { $_.Version -ge [version]$mod.MinVersion } |
                 Sort-Object Version -Descending |
                 Select-Object -First 1
    if ($installed) {
        Write-Host " OK  (v$($installed.Version))" -ForegroundColor Green
    } else {
        Write-Host ' INSTALLING...' -ForegroundColor Yellow
        try {
            Install-Module -Name $mod.Name -MinimumVersion $mod.MinVersion `
                -Scope CurrentUser -Force -AllowClobber -Repository PSGallery
            $v = (Get-Module -ListAvailable $mod.Name | Sort-Object Version -Desc | Select-Object -First 1).Version
            Write-Host "  -> Installed v$v" -ForegroundColor Green
        } catch {
            Write-Host "  FAILED: $_" -ForegroundColor Red
            Write-Host "  Run manually: Install-Module -Name $($mod.Name) -Scope CurrentUser -Force" -ForegroundColor Yellow
        }
    }
}

# --- Optional modules ---
$optionalModules = @(
    @{ Name = 'Microsoft.PowerApps.Administration.PowerShell'; Desc = 'Power Apps Admin cmdlets (used for app/flow metadata)' }
)

Write-Host ''
Write-Host 'Optional modules:' -ForegroundColor DarkGray
foreach ($mod in $optionalModules) {
    Write-Host "  $($mod.Name)..." -NoNewline
    $installed = Get-Module -ListAvailable -Name $mod.Name | Select-Object -First 1
    if ($installed) {
        Write-Host " OK  (v$($installed.Version))" -ForegroundColor Green
    } else {
        Write-Host ' not installed' -ForegroundColor DarkGray
        Write-Host "    Install with: Install-Module '$($mod.Name)' -Scope CurrentUser -Force" -ForegroundColor DarkGray
        Write-Host "    Purpose: $($mod.Desc)" -ForegroundColor DarkGray
    }
}

# --- Network connectivity ---
Write-Host ''
Write-Host 'Connectivity to key endpoints:' -ForegroundColor DarkGray
$endpoints = @(
    'https://login.microsoftonline.com',
    'https://api.bap.microsoft.com',
    'https://management.azure.com'
)
foreach ($ep in $endpoints) {
    Write-Host "  $ep..." -NoNewline
    try {
        $null = Invoke-WebRequest -Uri $ep -UseBasicParsing -TimeoutSec 10 -ErrorAction SilentlyContinue
        Write-Host ' reachable' -ForegroundColor Green
    } catch {
        # Any response (even 4xx) proves it's reachable
        if ($_.Exception.Response) {
            Write-Host ' reachable' -ForegroundColor Green
        } else {
            Write-Host " UNREACHABLE - $_" -ForegroundColor Red
        }
    }
}

Write-Host ''
Write-Host '=========================================================' -ForegroundColor Cyan
Write-Host '  Prerequisites check complete.' -ForegroundColor Cyan
Write-Host "  Next step: .\Invoke-DataverseInventory.ps1 -OutputPath '..\data'" -ForegroundColor White
Write-Host '=========================================================' -ForegroundColor Cyan
Write-Host ''
