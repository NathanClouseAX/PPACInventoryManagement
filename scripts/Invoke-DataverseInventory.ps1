#Requires -Version 5.1
<#
.SYNOPSIS
    Collects a comprehensive inventory of all Power Platform / Dataverse environments
    in a Microsoft 365 tenant and stores the raw data locally for analysis.
.DESCRIPTION
    This is the main orchestrator script. It:
      1. Authenticates to the tenant using the Az module (interactive or device code)
      2. Enumerates all Power Platform environments via the BAP API
      3. For each environment that has a Dataverse org, collects:
           - Storage capacity (BAP API)
           - System users, bulk delete jobs, async ops, solutions, workflows,
             plugins, duplicate detection rules, app modules, audit samples,
             retention policies, entity statistics (CE side)
      4. For environments with Finance & Operations solutions detected,
         additionally collects FO batch jobs, DIXF history, users, and legal entities
      5. Saves everything as structured JSON under -OutputPath

    All operations are READ-ONLY. No changes are made to any environment.

.PARAMETER OutputPath
    Root directory where collected data will be stored.
    Default: .\data  (relative to this script's location)

.PARAMETER TenantId
    Azure AD tenant ID. If omitted, uses the current Az context tenant.

.PARAMETER SubscriptionId
    Azure subscription ID. Optional – only needed if Az context has multiple subscriptions.

.PARAMETER EnvironmentFilter
    Regex pattern to filter environment display names. Collect only matching environments.
    Default: '' (collect all)

.PARAMETER SkipEnvironmentIds
    Array of environment GUIDs to skip. Useful for known test environments.

.PARAMETER IncludeEntityCounts
    If specified, fetches record counts for up to -EntityCountTop entities per
    Dataverse org. This adds significant time for large tenants.

.PARAMETER EntityCountTop
    How many entities to count per environment when -IncludeEntityCounts is used.
    Default: 150

.PARAMETER IncludeFO
    If specified, collects Finance & Operations data for environments where FO solutions
    are detected. Requires the authenticated user to have the System Administrator role
    in each FO environment.

.PARAMETER MaxEnvironments
    Safety limit - stops after processing this many environments. 0 = no limit.
    Default: 0

.PARAMETER Force
    Overwrite existing data files. By default, environments with a complete
    ce-summary.json are skipped (resume support).

.PARAMETER UseDeviceCode
    Force device-code authentication flow instead of interactive browser login.

.EXAMPLE
    # Full collection, interactive login
    .\Invoke-DataverseInventory.ps1 -OutputPath C:\PPACData

.EXAMPLE
    # Include entity record counts and FO data
    .\Invoke-DataverseInventory.ps1 -OutputPath C:\PPACData -IncludeEntityCounts -IncludeFO

.EXAMPLE
    # Only production environments, resume if partially run
    .\Invoke-DataverseInventory.ps1 -OutputPath C:\PPACData -EnvironmentFilter 'Production'

.EXAMPLE
    # Device code flow (for headless / remote sessions)
    .\Invoke-DataverseInventory.ps1 -OutputPath C:\PPACData -UseDeviceCode
#>

[CmdletBinding()]
param(
    [string]  $OutputPath          = '',
    [string]  $TenantId            = '',
    [string]  $SubscriptionId      = '',
    [string]  $EnvironmentFilter   = '',
    [string[]]$SkipEnvironmentIds  = @(),
    [switch]  $IncludeEntityCounts,
    [int]     $EntityCountTop       = 150,
    [switch]  $IncludeFO,
    [int]     $MaxEnvironments      = 0,
    [switch]  $Force,
    [switch]  $UseDeviceCode
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Off   # off to allow null properties on deserialized JSON

# ── Resolve paths ─────────────────────────────────────────────────────────────
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
if (-not $OutputPath) {
    $OutputPath = Join-Path (Split-Path -Parent $ScriptDir) 'data'
}

# ── Import shared module ──────────────────────────────────────────────────────
$modulePath = Join-Path $ScriptDir 'modules\PPACInventory.psm1'
if (-not (Test-Path $modulePath)) {
    throw "Cannot find PPACInventory.psm1 at: $modulePath"
}
Import-Module $modulePath -Force
Set-InventoryOutputPath -Path $OutputPath

# Dot-source collector scripts
. (Join-Path $ScriptDir 'collectors\Collect-EnvironmentList.ps1')
. (Join-Path $ScriptDir 'collectors\Collect-CEData.ps1')
. (Join-Path $ScriptDir 'collectors\Collect-FOData.ps1')

# ── Initialize output directories ─────────────────────────────────────────────
$envDataRoot = Join-Path $OutputPath 'environments'
$logPath     = Join-Path $OutputPath 'inventory.log'
$null        = New-Item -ItemType Directory -Path $OutputPath     -Force
$null        = New-Item -ItemType Directory -Path $envDataRoot    -Force
$null        = New-Item -ItemType Directory -Path (Join-Path (Split-Path -Parent $ScriptDir) 'reports') -Force

Set-InventoryLogFile -Path $logPath

# ── Banner ────────────────────────────────────────────────────────────────────
Write-InventoryLog ''
Write-InventoryLog '============================================================'
Write-InventoryLog '  PPAC Dataverse Environment Inventory'
Write-InventoryLog "  Started : $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
Write-InventoryLog "  Output  : $OutputPath"
Write-InventoryLog '  Mode    : READ-ONLY (no changes will be made)'
Write-InventoryLog '============================================================'
Write-InventoryLog ''

# ── Authentication ────────────────────────────────────────────────────────────
Write-InventoryLog 'Authenticating to Azure AD...'

$connectParams = @{}
if ($TenantId)       { $connectParams['TenantId']       = $TenantId }
if ($SubscriptionId) { $connectParams['SubscriptionId'] = $SubscriptionId }
if ($UseDeviceCode)  { $connectParams['UseDeviceAuthentication'] = $true }

try {
    $ctx = Get-AzContext
    if (-not $ctx -or -not $ctx.Account) {
        Write-InventoryLog "No active Az context - initiating login..." -Indent 1
        $null = Connect-AzAccount @connectParams
    } else {
        Write-InventoryLog "Using existing context: $($ctx.Account.Id) / $($ctx.Tenant.Id)" -Level OK -Indent 1
        # Switch tenant if specified
        if ($TenantId -and $ctx.Tenant.Id -ne $TenantId) {
            Write-InventoryLog "Switching to tenant $TenantId..." -Indent 1
            $null = Connect-AzAccount @connectParams
        }
    }
} catch {
    Write-InventoryLog "Connect-AzAccount failed: $_" -Level ERROR
    Write-InventoryLog "Try running: Connect-AzAccount -TenantId <yourTenantId>" -Level ERROR
    throw
}

$finalCtx = Get-AzContext
Write-InventoryLog "Authenticated as : $($finalCtx.Account.Id)" -Level OK
Write-InventoryLog "Tenant           : $($finalCtx.Tenant.Id)"  -Level OK

# Warm up the BAP token now so we fail fast on permission issues
try {
    $null = Get-AzureToken -ResourceUrl 'https://service.powerapps.com/'
    Write-InventoryLog "BAP API token acquired." -Level OK
} catch {
    Write-InventoryLog "Failed to acquire BAP API token: $_" -Level ERROR
    Write-InventoryLog "Ensure the account has the 'Power Platform Administrator' or 'Global Administrator' role." -Level ERROR
    throw
}

# ── Get environment list ──────────────────────────────────────────────────────
Write-InventoryLog ''
$allEnvironments = Get-AllEnvironments -OutputPath $OutputPath

# ── Apply filters ─────────────────────────────────────────────────────────────
$environments = $allEnvironments

if ($EnvironmentFilter) {
    $environments = @($environments | Where-Object { $_.DisplayName -match $EnvironmentFilter })
    Write-InventoryLog "After filter '$EnvironmentFilter': $($environments.Count) environments." -Indent 1
}

if ($SkipEnvironmentIds.Count -gt 0) {
    $environments = @($environments | Where-Object { $_.EnvironmentId -notin $SkipEnvironmentIds })
    Write-InventoryLog "After skip list: $($environments.Count) environments." -Indent 1
}

if ($MaxEnvironments -gt 0 -and $environments.Count -gt $MaxEnvironments) {
    $environments = @($environments | Select-Object -First $MaxEnvironments)
    Write-InventoryLog "Capped at $MaxEnvironments environments." -Level WARN -Indent 1
}

Write-InventoryLog "Will process $($environments.Count) environments." -Level OK

# ── Summary trackers ─────────────────────────────────────────────────────────
$processSummary = [System.Collections.Generic.List[hashtable]]::new()
$totalFlags     = [System.Collections.Generic.List[string]]::new()
$processedCount = 0
$skippedCount   = 0
$errorCount     = 0

# ── Main collection loop ──────────────────────────────────────────────────────
$envIdx = 0
foreach ($env in $environments) {
    $envIdx++
    $displayName  = $env.DisplayName
    $envId        = $env.EnvironmentId
    $safeName     = Get-SafeDirectoryName -Name "$($env.EnvironmentSku)_$displayName"
    $envOutputDir = Join-Path $envDataRoot $safeName

    Write-InventoryLog ''
    Write-InventoryLog "[$envIdx / $($environments.Count)] Processing: $displayName ($($env.EnvironmentSku))"
    Write-InventoryLog "  ID: $envId" -Indent 1
    Write-InventoryLog "  State: $($env.State) | Runtime: $($env.RuntimeState)" -Indent 1

    # Resume support: skip if already collected and not Force
    $ceSummaryFile = Join-Path $envOutputDir 'ce-summary.json'
    if (-not $Force -and (Test-Path $ceSummaryFile)) {
        Write-InventoryLog "  Already collected (use -Force to overwrite). Skipping." -Level SKIP -Indent 1
        $skippedCount++
        # Still load the summary for the report
        try {
            $existingCE = Get-Content $ceSummaryFile -Raw | ConvertFrom-Json
            $envEntry = @{
                EnvironmentId    = $envId
                DisplayName      = $displayName
                EnvironmentSku   = $env.EnvironmentSku
                IsDefault        = $env.IsDefault
                State            = $env.State
                Location         = $env.Location
                StorageDB_MB     = $env.StorageDB_MB
                StorageFile_MB   = $env.StorageFile_MB
                StorageLog_MB    = $env.StorageLog_MB
                StorageTotal_MB  = $env.StorageTotal_MB
                HasDataverse     = $env.HasDataverse
                HasFO            = if ($existingCE.HasFO) { $existingCE.HasFO } else { $false }
                AllFlags         = if ($existingCE.AllFlags) { $existingCE.AllFlags } else { @() }
                OutputDir        = $envOutputDir
                Skipped          = $true
            }
            $processSummary.Add($envEntry)
        } catch {}
        continue
    }

    # Skip environments in non-ready states
    if ($env.State -notin 'Ready', 'Enabled', $null) {
        Write-InventoryLog "  Environment state is '$($env.State)' - skipping." -Level SKIP -Indent 1
        $skippedCount++
        continue
    }

    # Create output directory
    $null = New-Item -ItemType Directory -Path $envOutputDir -Force

    # Save environment metadata
    Save-EnvironmentData -EnvironmentDir $envOutputDir -FileName 'metadata.json' -Data $env

    $envSummaryEntry = @{
        EnvironmentId   = $envId
        DisplayName     = $displayName
        EnvironmentSku  = $env.EnvironmentSku
        IsDefault       = $env.IsDefault
        State           = $env.State
        Location        = $env.Location
        CreatedTime     = $env.CreatedTime
        StorageDB_MB    = $env.StorageDB_MB
        StorageFile_MB  = $env.StorageFile_MB
        StorageLog_MB   = $env.StorageLog_MB
        StorageTotal_MB = $env.StorageTotal_MB
        HasDataverse    = $env.HasDataverse
        HasFO           = $false
        AllFlags        = @()
        OutputDir       = $envOutputDir
        Skipped         = $false
        Error           = $null
    }

    try {
        # ── CE Data Collection ────────────────────────────────────────────────
        if ($env.HasDataverse -and $env.OrgApiUrl) {
            $ceResult = Collect-CEEnvironmentData `
                -EnvEntry           $env `
                -EnvOutputDir       $envOutputDir `
                -IncludeEntityCounts:$IncludeEntityCounts `
                -EntityCountTop     $EntityCountTop

            if ($ceResult) {
                $envSummaryEntry.HasFO    = $ceResult.HasFO
                $envSummaryEntry.AllFlags = @($ceResult.AllFlags)
            }
        } else {
            Write-InventoryLog "  No Dataverse org - CE collection skipped." -Level SKIP -Indent 1
        }

        # ── FO Data Collection ────────────────────────────────────────────────
        if ($IncludeFO -and $envSummaryEntry.HasFO) {
            Write-InventoryLog "  FO solutions detected - starting FO collection..." -Indent 1
            try {
                $foResult = Collect-FOEnvironmentData `
                    -EnvEntry     $env `
                    -EnvOutputDir $envOutputDir

                if ($foResult -and $foResult.AllFlags) {
                    $envSummaryEntry.AllFlags = @(
                        @($envSummaryEntry.AllFlags) + @($foResult.AllFlags) | Sort-Object -Unique
                    )
                }
            } catch {
                Write-InventoryLog "  FO collection failed: $_" -Level WARN -Indent 1
            }
        } elseif ($IncludeFO -and -not $envSummaryEntry.HasFO) {
            Write-InventoryLog "  No FO solutions detected - FO collection skipped." -Level SKIP -Indent 1
        }

        $processedCount++
        Write-InventoryLog "  Done. Flags: $($envSummaryEntry.AllFlags.Count) | Storage: $($env.StorageTotal_MB) MB" -Level OK -Indent 1

    } catch {
        Write-InventoryLog "  ERROR processing $displayName : $_" -Level ERROR -Indent 1
        $envSummaryEntry.Error = $_.ToString()
        $errorCount++
    }

    $processSummary.Add($envSummaryEntry)
    $totalFlags.AddRange([string[]]$envSummaryEntry.AllFlags)

    # Mild pacing to avoid rate limiting on large tenants
    if ($envIdx % 10 -eq 0) { Start-Sleep -Milliseconds 500 }
}

# ── Save master summary ───────────────────────────────────────────────────────
Write-InventoryLog ''
Write-InventoryLog 'Saving master summary...'

$masterSummary = @{
    RunAt              = (Get-Date -Format 'o')
    TenantId           = $finalCtx.Tenant.Id
    AuthenticatedAs    = $finalCtx.Account.Id
    TotalEnvironments  = $allEnvironments.Count
    Processed          = $processedCount
    Skipped            = $skippedCount
    Errors             = $errorCount
    IncludedFO         = [bool]$IncludeFO
    IncludedEntityCounts = [bool]$IncludeEntityCounts
    Environments       = @($processSummary)
    AllFlagsDistinct   = @($totalFlags | Group-Object | Select-Object Name, Count | Sort-Object Count -Descending)
}

Save-RootData -FileName 'master-summary.json' -Data $masterSummary

# ── Console summary ───────────────────────────────────────────────────────────
Write-InventoryLog ''
Write-InventoryLog '============================================================'
Write-InventoryLog '  Collection Complete'
Write-InventoryLog "  Processed : $processedCount environments"
Write-InventoryLog "  Skipped   : $skippedCount environments (already collected)"
Write-InventoryLog "  Errors    : $errorCount environments"
Write-InventoryLog ''

# Top flags across tenant
$topFlags = @($masterSummary.AllFlagsDistinct | Select-Object -First 15)
if ($topFlags.Count -gt 0) {
    Write-InventoryLog '  Top issues across tenant:'
    foreach ($f in $topFlags) {
        Write-InventoryLog "    $($f.Count.ToString().PadLeft(4))x  $($f.Name)" -Level WARN
    }
}

Write-InventoryLog ''
Write-InventoryLog "  Data saved to : $OutputPath"
Write-InventoryLog "  Log file      : $logPath"
Write-InventoryLog ''
Write-InventoryLog '  Next step: run .\Generate-Report.ps1 to produce the HTML analysis report.'
Write-InventoryLog '============================================================'
