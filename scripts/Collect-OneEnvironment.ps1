#Requires -Version 5.1
<#
.SYNOPSIS
    Per-environment collection worker. Invoked once per environment — either
    inline (sequential mode) or inside a RunspacePool thread (parallel mode)
    from Invoke-DataverseInventory.ps1.

.DESCRIPTION
    This script encapsulates the full per-environment collection flow that
    used to live inline in the main loop of Invoke-DataverseInventory.ps1.
    Extracting it lets the orchestrator run multiple environments in parallel
    without duplicating the logic across code paths.

    Each invocation:
      - imports PPACInventory.psm1 into this runspace
      - redirects Write-InventoryLog to environments/<name>/worker.log
      - runs resume check, governance flags, F&O detection, CE/Maker/Gov/RBAC/
        Metadata/Activity/FO collectors
      - returns a single hashtable describing the outcome

    The caller aggregates results (counters, AllFlags, master-summary) AFTER
    all workers have completed.

.OUTPUTS
    Hashtable with fields:
      Status       - 'Processed' | 'Skipped' | 'Error' | 'SkippedState'
      Entry        - the $envSummaryEntry hashtable (or $null for SkippedState)
      WorkerLog    - absolute path to the per-env worker log (for merge)
      EnvironmentId, DisplayName - identifiers for parent-side logging
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory)][object]  $EnvEntry,            # BAP env PSObject
    [Parameter(Mandatory)][string]  $OutputPath,
    [Parameter(Mandatory)][string]  $ScriptDir,            # parent's script dir (for resolving collectors)
    [int]     $EnvIndex            = 0,
    [int]     $TotalEnvironments   = 0,
    [bool]    $IncludeEntityCounts = $true,
    [int]     $EntityCountTop      = 0,
    [bool]    $IncludeFO            = $true,
    [bool]    $IncludeMakerInventory = $true,
    [bool]    $IncludeGovernance     = $true,
    [bool]    $IncludeRBAC           = $true,
    [bool]    $IncludeMetadataDepth  = $true,
    [bool]    $IncludeActivity       = $false,
    [bool]    $Force                 = $false
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Off

# Workers must not paint progress bars on the shared host — it corrupts output
# when multiple runspaces write concurrently. Entity-count collectors call
# Write-Progress heavily; silencing it here keeps the console readable.
$ProgressPreference = 'SilentlyContinue'

# ── Import shared module + collectors into this runspace ─────────────────────
Import-Module (Join-Path $ScriptDir 'modules\PPACInventory.psm1') -Force
Set-InventoryOutputPath -Path $OutputPath

# Dot-source the collectors we may need. Sourcing unused ones is cheap and keeps
# the conditional below readable.
. (Join-Path $ScriptDir 'collectors\Collect-CEData.ps1')
. (Join-Path $ScriptDir 'collectors\Collect-FOData.ps1')
. (Join-Path $ScriptDir 'collectors\Collect-MakerInventory.ps1')
. (Join-Path $ScriptDir 'collectors\Collect-TenantGovernance.ps1')
. (Join-Path $ScriptDir 'collectors\Collect-RBAC.ps1')
. (Join-Path $ScriptDir 'collectors\Collect-MetadataDepth.ps1')
. (Join-Path $ScriptDir 'collectors\Collect-Activity.ps1')

# ── Resolve env-specific paths and point the logger at the worker log ────────
$displayName  = $EnvEntry.DisplayName
$envId        = $EnvEntry.EnvironmentId
$safeName     = Get-SafeDirectoryName -Name "$($EnvEntry.EnvironmentSku)_$displayName"
$envDataRoot  = Join-Path $OutputPath 'environments'
$envOutputDir = Join-Path $envDataRoot $safeName
$null         = New-Item -ItemType Directory -Path $envOutputDir -Force
$workerLog    = Join-Path $envOutputDir 'worker.log'
# Fresh worker log per invocation — stale content from previous runs would
# confuse the merge-into-inventory.log step at the end of orchestration.
if (Test-Path $workerLog) { Remove-Item $workerLog -Force -ErrorAction SilentlyContinue }
Set-InventoryLogFile -Path $workerLog

Write-InventoryLog ''
$countLabel = if ($TotalEnvironments -gt 0) { "[$EnvIndex / $TotalEnvironments]" } else { '[worker]' }
Write-InventoryLog "$countLabel Processing: $displayName ($($EnvEntry.EnvironmentSku))"
Write-InventoryLog "  ID: $envId" -Indent 1
Write-InventoryLog "  State: $($EnvEntry.State) | Runtime: $($EnvEntry.RuntimeState)" -Indent 1

# ── Resume support ────────────────────────────────────────────────────────────
# Skip re-collection only if ce-summary.json exists AND all requested outputs
# from the current run are already present. Missing outputs (e.g., entity counts
# when -IncludeEntityCounts is on) trigger a re-collection.
$ceSummaryFile    = Join-Path $envOutputDir 'ce-summary.json'
$entityCountsFile = Join-Path $envOutputDir 'entity-counts.json'
$foDetailsFile    = Join-Path $envOutputDir 'fo-integration-details.json'
$foCountsFile     = Join-Path $envOutputDir 'fo-entity-counts.json'
$shouldSkip = $false
$existingCE = $null
if (-not $Force -and (Test-Path $ceSummaryFile)) {
    $shouldSkip = $true
    try { $existingCE = Get-Content $ceSummaryFile -Raw | ConvertFrom-Json } catch { $shouldSkip = $false }

    if ($shouldSkip -and $IncludeEntityCounts -and -not (Test-Path $entityCountsFile)) {
        Write-InventoryLog "  Entity counts missing - re-collecting." -Level INFO -Indent 1
        $shouldSkip = $false
    }
    if ($shouldSkip -and $IncludeFO -and $existingCE) {
        $hasFOProp = $existingCE.PSObject.Properties.Name -contains 'HasFO'
        if (-not $hasFOProp) {
            Write-InventoryLog "  F&O detection not run in prior pass - re-collecting." -Level INFO -Indent 1
            $shouldSkip = $false
        } elseif ($existingCE.HasFO -eq $true -and -not (Test-Path $foDetailsFile)) {
            Write-InventoryLog "  F&O details file missing - re-collecting." -Level INFO -Indent 1
            $shouldSkip = $false
        } elseif ($existingCE.HasFO -eq $true -and $IncludeEntityCounts -and -not (Test-Path $foCountsFile)) {
            Write-InventoryLog "  F&O entity counts missing - re-collecting." -Level INFO -Indent 1
            $shouldSkip = $false
        }
    }
}

if ($shouldSkip) {
    Write-InventoryLog "  Already collected (use -Force to overwrite). Skipping." -Level SKIP -Indent 1
    try {
        $skippedEntry = @{
            EnvironmentId    = $envId
            DisplayName      = $displayName
            EnvironmentSku   = $EnvEntry.EnvironmentSku
            IsDefault        = $EnvEntry.IsDefault
            State            = $EnvEntry.State
            Location         = $EnvEntry.Location
            StorageDB_MB     = $EnvEntry.StorageDB_MB
            StorageFile_MB   = $EnvEntry.StorageFile_MB
            StorageLog_MB    = $EnvEntry.StorageLog_MB
            StorageTotal_MB  = $EnvEntry.StorageTotal_MB
            HasDataverse     = $EnvEntry.HasDataverse
            HasFO            = if ($existingCE -and $existingCE.HasFO)    { $existingCE.HasFO }    else { $false }
            FOBaseUrl        = if ($existingCE -and $existingCE.FOBaseUrl) { $existingCE.FOBaseUrl } else { $null }
            AllFlags         = if ($existingCE -and $existingCE.AllFlags)  { $existingCE.AllFlags }  else { @() }
            OutputDir        = $envOutputDir
            Skipped          = $true
        }
        return @{
            Status        = 'Skipped'
            Entry         = $skippedEntry
            WorkerLog     = $workerLog
            EnvironmentId = $envId
            DisplayName   = $displayName
        }
    } catch {
        return @{
            Status        = 'Skipped'
            Entry         = $null
            WorkerLog     = $workerLog
            EnvironmentId = $envId
            DisplayName   = $displayName
        }
    }
}

# ── Guard: non-ready environments get a lightweight skip ────────────────────
if ($EnvEntry.State -notin 'Ready', 'Enabled', $null) {
    Write-InventoryLog "  Environment state is '$($EnvEntry.State)' - skipping." -Level SKIP -Indent 1
    return @{
        Status        = 'SkippedState'
        Entry         = $null
        WorkerLog     = $workerLog
        EnvironmentId = $envId
        DisplayName   = $displayName
    }
}

# ── Main collection flow ─────────────────────────────────────────────────────
Save-EnvironmentData -EnvironmentDir $envOutputDir -FileName 'metadata.json' -Data $EnvEntry

$envSummaryEntry = @{
    EnvironmentId   = $envId
    DisplayName     = $displayName
    EnvironmentSku  = $EnvEntry.EnvironmentSku
    IsDefault       = $EnvEntry.IsDefault
    State           = $EnvEntry.State
    Location        = $EnvEntry.Location
    CreatedTime     = $EnvEntry.CreatedTime
    StorageDB_MB    = $EnvEntry.StorageDB_MB
    StorageFile_MB  = $EnvEntry.StorageFile_MB
    StorageLog_MB   = $EnvEntry.StorageLog_MB
    StorageTotal_MB = $EnvEntry.StorageTotal_MB
    HasDataverse    = $EnvEntry.HasDataverse
    HasFO           = $false
    FOBaseUrl       = $null
    AllFlags        = @()
    OutputDir       = $envOutputDir
    Skipped         = $false
    Error           = $null
}

$envFlags = [System.Collections.Generic.List[string]]::new()
$status   = 'Processed'

# Managed Environments status
$protLevel = $null
try { $protLevel = $EnvEntry.RawProperties.governanceConfiguration.protectionLevel } catch {}
if ($EnvEntry.EnvironmentSku -in 'Production','Sandbox' -and $protLevel -ne 'Protected') {
    $envFlags.Add("PRODUCTION_NOT_MANAGED_ENVIRONMENT ($($EnvEntry.EnvironmentSku) environment is not a Managed Environment - governance policies and extended features unavailable)")
}

# Environment Group membership
$envGroupId = $null
try { $envGroupId = $EnvEntry.RawProperties.environmentGroupId } catch {}
if ($EnvEntry.EnvironmentSku -in 'Production','Sandbox' -and -not $envGroupId) {
    $envFlags.Add("NOT_IN_ENVIRONMENT_GROUP ($($EnvEntry.EnvironmentSku) environment not assigned to any Environment Group - cannot inherit policies or be managed in bulk)")
}

# Environment Admin Assignments
try {
    $roleResp = Invoke-BAPRequest `
        -Path "/providers/Microsoft.BusinessAppPlatform/scopes/admin/environments/$envId/roleAssignments" `
        -ApiVersion '2024-05-01' `
        -TimeoutSec 30
    $roleAssignments = if ($roleResp.value) { $roleResp.value } else { @() }
    Save-EnvironmentData -EnvironmentDir $envOutputDir -FileName 'role-assignments.json' -Data $roleAssignments

    $envAdmins = @($roleAssignments | Where-Object {
        $_.properties.roleDefinition.displayName -match 'Admin' -or
        $_.properties.roleDefinition.id -match 'admin'
    })
    $userAdmins  = @($envAdmins | Where-Object { $_.properties.principal.type -eq 'User' })
    $groupAdmins = @($envAdmins | Where-Object { $_.properties.principal.type -eq 'Group' })

    if ($envAdmins.Count -eq 0 -and $EnvEntry.EnvironmentSku -in 'Production','Sandbox') {
        $envFlags.Add("NO_DEDICATED_ENVIRONMENT_ADMIN (no users or groups explicitly assigned as Environment Admin - relying solely on tenant-wide admin roles)")
    }
    if ($userAdmins.Count -gt 0 -and $groupAdmins.Count -eq 0 -and $EnvEntry.EnvironmentSku -eq 'Production') {
        $envFlags.Add("ENVIRONMENT_ADMIN_IS_USER_NOT_GROUP ($($userAdmins.Count) individual user admin assignments on Production - group-based assignment preferred for continuity)")
    }

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
    # F&O integration detection
    if ($EnvEntry.HasDataverse -and $EnvEntry.OrgApiUrl) {
        Write-InventoryLog '  Checking F&O integration status...' -Indent 1
        $foDetails = Get-FOIntegrationDetails `
            -InstanceApiUrl $EnvEntry.OrgApiUrl `
            -InstanceUrl    $EnvEntry.OrgUrl
        $envSummaryEntry.HasFO     = $foDetails.HasFO
        $envSummaryEntry.FOBaseUrl = $foDetails.FOUrl
        if ($foDetails.HasFO) {
            Write-InventoryLog "  F&O integration active: $($foDetails.FOUrl)" -Level OK -Indent 1
            Save-EnvironmentData -EnvironmentDir $envOutputDir -FileName 'fo-integration-details.json' -Data $foDetails
        } else {
            Write-InventoryLog '  No F&O integration.' -Level SKIP -Indent 1
        }
    }

    # CE Data Collection
    if ($EnvEntry.HasDataverse -and $EnvEntry.OrgApiUrl) {
        $ceResult = Collect-CEEnvironmentData `
            -EnvEntry           $EnvEntry `
            -EnvOutputDir       $envOutputDir `
            -HasFO              $envSummaryEntry.HasFO `
            -IncludeEntityCounts:$IncludeEntityCounts `
            -EntityCountTop     $EntityCountTop

        if ($ceResult) {
            $envSummaryEntry.AllFlags = @($ceResult.AllFlags)
        }
    } else {
        Write-InventoryLog "  No Dataverse org - CE collection skipped." -Level SKIP -Indent 1
    }

    if ($IncludeMakerInventory) {
        try {
            $makerEnvEntry = @{
                DisplayName   = $displayName
                EnvironmentId = $envId
                OrgUrl        = $EnvEntry.OrgUrl
                OrgApiUrl     = $EnvEntry.OrgApiUrl
                HasDataverse  = $EnvEntry.HasDataverse
            }
            $makerResult = Collect-MakerEnvironmentInventory `
                -EnvEntry     $makerEnvEntry `
                -EnvOutputDir $envOutputDir
            if ($makerResult -and $makerResult.AllFlags) {
                $envSummaryEntry.AllFlags = @(
                    @($envSummaryEntry.AllFlags) + @($makerResult.AllFlags) | Sort-Object -Unique
                )
            }
        } catch {
            Write-InventoryLog "  Maker inventory failed: $_" -Level WARN -Indent 1
        }
    }

    if ($IncludeGovernance) {
        try {
            $govResult = Collect-EnvironmentGovernance `
                -EnvEntry     $EnvEntry `
                -EnvOutputDir $envOutputDir
            if ($govResult -and $govResult.AllFlags) {
                $envSummaryEntry.AllFlags = @(
                    @($envSummaryEntry.AllFlags) + @($govResult.AllFlags) | Sort-Object -Unique
                )
            }
        } catch {
            Write-InventoryLog "  Governance collection failed: $_" -Level WARN -Indent 1
        }
    }

    if ($IncludeRBAC -and $EnvEntry.HasDataverse -and $EnvEntry.OrgApiUrl) {
        try {
            $rbacResult = Collect-RBACInventory `
                -EnvEntry     $EnvEntry `
                -EnvOutputDir $envOutputDir
            if ($rbacResult -and $rbacResult.AllFlags) {
                $envSummaryEntry.AllFlags = @(
                    @($envSummaryEntry.AllFlags) + @($rbacResult.AllFlags) | Sort-Object -Unique
                )
            }
        } catch {
            Write-InventoryLog "  RBAC collection failed: $_" -Level WARN -Indent 1
        }
    }

    if ($IncludeMetadataDepth) {
        try {
            $mdResult = Collect-MetadataDepthInventory `
                -EnvEntry     $EnvEntry `
                -EnvOutputDir $envOutputDir
            if ($mdResult -and $mdResult.AllFlags) {
                $envSummaryEntry.AllFlags = @(
                    @($envSummaryEntry.AllFlags) + @($mdResult.AllFlags) | Sort-Object -Unique
                )
            }
        } catch {
            Write-InventoryLog "  Metadata depth collection failed: $_" -Level WARN -Indent 1
        }
    }

    if ($IncludeActivity) {
        try {
            $actResult = Collect-ActivityTelemetry `
                -EnvEntry     $EnvEntry `
                -EnvOutputDir $envOutputDir
            if ($actResult -and $actResult.AllFlags) {
                $envSummaryEntry.AllFlags = @(
                    @($envSummaryEntry.AllFlags) + @($actResult.AllFlags) | Sort-Object -Unique
                )
            }
        } catch {
            Write-InventoryLog "  Activity telemetry failed: $_" -Level WARN -Indent 1
        }
    }

    if ($IncludeFO -and $envSummaryEntry.HasFO) {
        Write-InventoryLog "  F&O integration detected - starting FO collection..." -Indent 1
        try {
            $foResult = Collect-FOEnvironmentData `
                -EnvEntry             $EnvEntry `
                -EnvOutputDir         $envOutputDir `
                -FOBaseUrl            $envSummaryEntry.FOBaseUrl `
                -IncludeEntityCounts:$IncludeEntityCounts

            if ($foResult -and $foResult.AllFlags) {
                $envSummaryEntry.AllFlags = @(
                    @($envSummaryEntry.AllFlags) + @($foResult.AllFlags) | Sort-Object -Unique
                )
            }
        } catch {
            Write-InventoryLog "  FO collection failed: $_" -Level WARN -Indent 1
        }
    } elseif ($IncludeFO -and -not $envSummaryEntry.HasFO) {
        Write-InventoryLog "  No F&O integration - FO collection skipped." -Level SKIP -Indent 1
    }

    if ($envFlags.Count -gt 0) {
        $envSummaryEntry.AllFlags = @(
            @($envSummaryEntry.AllFlags) + @($envFlags) | Sort-Object -Unique
        )
    }

    Write-InventoryLog "  Done. Flags: $($envSummaryEntry.AllFlags.Count) | Storage: $($EnvEntry.StorageTotal_MB) MB" -Level OK -Indent 1

} catch {
    Write-InventoryLog "  ERROR processing $displayName : $_" -Level ERROR -Indent 1
    $envSummaryEntry.Error = $_.ToString()
    $status = 'Error'
}

return @{
    Status        = $status
    Entry         = $envSummaryEntry
    WorkerLog     = $workerLog
    EnvironmentId = $envId
    DisplayName   = $displayName
}
