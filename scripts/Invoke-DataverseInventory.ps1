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

    # Resume support: if ce-summary.json already exists and -Force was not specified,
    # skip re-collection to allow resuming an interrupted run. We still load the
    # existing summary so it appears correctly in the final report and delta comparison.
    $ceSummaryFile = Join-Path $envOutputDir 'ce-summary.json'
    if (-not $Force -and (Test-Path $ceSummaryFile)) {
        Write-InventoryLog "  Already collected (use -Force to overwrite). Skipping." -Level SKIP -Indent 1
        $skippedCount++
        # Re-hydrate just enough fields for the report (storage comes from live env list, not cached summary)
        try {
            $existingCE = Get-Content $ceSummaryFile -Raw | ConvertFrom-Json
            $envEntry = @{
                EnvironmentId    = $envId
                DisplayName      = $displayName
                EnvironmentSku   = $env.EnvironmentSku
                IsDefault        = $env.IsDefault
                State            = $env.State
                Location         = $env.Location
                StorageDB_MB     = $env.StorageDB_MB     # Always from live BAP data (most current)
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

    # Build the per-environment summary entry that flows into master-summary.json
    # and is read by Generate-Report.ps1. Fields come from both the BAP environment
    # list (storage, metadata) and the CE/FO collectors (HasFO, AllFlags).
    # GovernanceWeight, governance flags, and Admins are added below after collection.
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
        HasFO           = $false   # Overwritten by CE collector if FO solutions detected
        AllFlags        = @()      # Merged from: CE flags + FO flags + BAP governance flags
        OutputDir       = $envOutputDir
        Skipped         = $false
        Error           = $null
    }

    # ── Environment Governance Checks (BAP metadata) ─────────────────────
    # These apply to all environments regardless of Dataverse presence
    $envFlags = [System.Collections.Generic.List[string]]::new()

    # Item 15: Managed Environments status
    $protLevel = $null
    try { $protLevel = $env.RawProperties.governanceConfiguration.protectionLevel } catch {}
    if ($env.EnvironmentSku -in 'Production','Sandbox' -and $protLevel -ne 'Protected') {
        $envFlags.Add("PRODUCTION_NOT_MANAGED_ENVIRONMENT ($($env.EnvironmentSku) environment is not a Managed Environment - governance policies and extended features unavailable)")
    }

    # Item 16: Environment Group membership
    $envGroupId = $null
    try { $envGroupId = $env.RawProperties.environmentGroupId } catch {}
    if ($env.EnvironmentSku -in 'Production','Sandbox' -and -not $envGroupId) {
        $envFlags.Add("NOT_IN_ENVIRONMENT_GROUP ($($env.EnvironmentSku) environment not assigned to any Environment Group - cannot inherit policies or be managed in bulk)")
    }

    # Item 14: Environment Admin Assignments
    try {
        $roleResp = Invoke-BAPRequest `
            -Path "/providers/Microsoft.BusinessAppPlatform/scopes/admin/environments/$envId/roleAssignments" `
            -ApiVersion '2022-03-01-preview' `
            -TimeoutSec 30
        $roleAssignments = if ($roleResp.value) { $roleResp.value } else { @() }
        Save-EnvironmentData -EnvironmentDir $envOutputDir -FileName 'role-assignments.json' -Data $roleAssignments

        $envAdmins = @($roleAssignments | Where-Object {
            $_.properties.roleDefinition.displayName -match 'Admin' -or
            $_.properties.roleDefinition.id -match 'admin'
        })
        $userAdmins  = @($envAdmins | Where-Object { $_.properties.principal.type -eq 'User' })
        $groupAdmins = @($envAdmins | Where-Object { $_.properties.principal.type -eq 'Group' })

        if ($envAdmins.Count -eq 0 -and $env.EnvironmentSku -in 'Production','Sandbox') {
            $envFlags.Add("NO_DEDICATED_ENVIRONMENT_ADMIN (no users or groups explicitly assigned as Environment Admin - relying solely on tenant-wide admin roles)")
        }
        if ($userAdmins.Count -gt 0 -and $groupAdmins.Count -eq 0 -and $env.EnvironmentSku -eq 'Production') {
            $envFlags.Add("ENVIRONMENT_ADMIN_IS_USER_NOT_GROUP ($($userAdmins.Count) individual user admin assignments on Production - group-based assignment preferred for continuity)")
        }

        # Auto-populate owners from admin assignments
        if ($envAdmins.Count -gt 0) {
            $adminNames = @($envAdmins | ForEach-Object {
                $p = $_.properties.principal
                if ($p.displayName) { $p.displayName } elseif ($p.email) { $p.email } else { $p.id }
            } | Select-Object -First 5)
            $envSummaryEntry['Admins'] = $adminNames
        }

        Write-InventoryLog "  Admins: $($envAdmins.Count) ($($userAdmins.Count) users, $($groupAdmins.Count) groups)" -Level OK -Indent 1
    } catch {
        Write-InventoryLog "  Role assignments query failed: $_" -Level WARN -Indent 1
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

        # ── Merge environment-level governance flags ──────────────────────
        if ($envFlags.Count -gt 0) {
            $envSummaryEntry.AllFlags = @(
                @($envSummaryEntry.AllFlags) + @($envFlags) | Sort-Object -Unique
            )
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

    # Mild pacing: every 10 environments, pause briefly to reduce the risk of
    # hitting BAP or Dataverse rate limits on large tenants (>50 environments).
    # The 500ms pause is intentionally short - the token cache and retry backoff
    # handle actual 429s, so this is just a courtesy delay.
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

# ── Save run history snapshot for delta reporting ─────────────────────────────
$runHistoryDir = Join-Path $OutputPath 'run-history'
$null = New-Item -ItemType Directory -Path $runHistoryDir -Force
$runTs = Get-Date -Format 'yyyy-MM-dd_HHmmss'
$runSnapshot = @{
    RunAt     = $masterSummary.RunAt
    TenantId  = $masterSummary.TenantId
    Environments = @($processSummary | ForEach-Object {
        @{
            EnvironmentId   = $_.EnvironmentId
            DisplayName     = $_.DisplayName
            EnvironmentSku  = $_.EnvironmentSku
            StorageDB_MB    = $_.StorageDB_MB
            StorageFile_MB  = $_.StorageFile_MB
            StorageLog_MB   = $_.StorageLog_MB
            StorageTotal_MB = $_.StorageTotal_MB
            AllFlags        = @($_.AllFlags)
            Admins          = if ($_.Admins) { $_.Admins } else { @() }
        }
    })
}
$snapshotFile = Join-Path $runHistoryDir "$runTs.json"
$runSnapshot | ConvertTo-Json -Depth 10 | Set-Content -Path $snapshotFile -Encoding UTF8 -Force
Write-InventoryLog "Run history snapshot saved: $snapshotFile" -Level OK

# ── Auto-populate owners.json from admin assignments ──────────────────────────
$configDir   = Join-Path (Split-Path -Parent $ScriptDir) 'config'
$ownersFile  = Join-Path $configDir 'owners.json'
# Build owners hashtable from existing file + new admin data
$ownersHT = @{}
if (Test-Path $ownersFile) {
    try {
        $owRaw = Get-Content $ownersFile -Raw | ConvertFrom-Json
        foreach ($prop in $owRaw.PSObject.Properties) {
            if ($prop.Name -notin '_comment','_example') {
                $ownersHT[$prop.Name] = $prop.Value
            }
        }
    } catch {}
}

$ownersUpdated = $false
foreach ($entry in $processSummary) {
    $eid = $entry.EnvironmentId
    if ($eid -and $entry.Admins -and $entry.Admins.Count -gt 0) {
        # Only auto-populate if no existing manual entry (or entry was auto-populated before)
        $existing = if ($ownersHT.ContainsKey($eid)) { $ownersHT[$eid] } else { $null }
        $isManual = $existing -and $existing.PSObject.Properties['AutoPopulated'] -and $existing.AutoPopulated -eq $false
        if (-not $isManual) {
            $ownersHT[$eid] = @{
                Owner         = $entry.Admins[0]
                AllAdmins     = @($entry.Admins)
                AutoPopulated = $true
                DisplayName   = $entry.DisplayName
            }
            $ownersUpdated = $true
        }
    }
}
if ($ownersUpdated -and (Test-Path $configDir)) {
    $ownersHT | ConvertTo-Json -Depth 5 | Set-Content -Path $ownersFile -Encoding UTF8 -Force
    Write-InventoryLog "owners.json updated with admin assignments." -Level OK
}

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
