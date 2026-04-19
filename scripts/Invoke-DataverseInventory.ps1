#Requires -Version 5.1
<#
.SYNOPSIS
    Collects a comprehensive inventory of all Power Platform / Dataverse environments
    in a Microsoft 365 tenant and stores the raw data locally for analysis.
.DESCRIPTION
    This is the main orchestrator script. It:
      1. Authenticates to the tenant using the Az module (interactive or device code)
      2. Enumerates all Power Platform environments via the BAP API
      3. For each environment that has a Dataverse org, calls the Dataverse
         action RetrieveFinanceAndOperationsIntegrationDetails to determine
         whether the environment is linked to a Finance & Operations app, and
         records the F&O URL / environment ID / tenant ID when it is.
      4. Then collects CE-side data:
           - Storage capacity (BAP API)
           - System users, bulk delete jobs, async ops, solutions, workflows,
             plugins, duplicate detection rules, app modules, audit samples,
             retention policies, entity statistics
      5. For environments with F&O integration detected, additionally collects
         FO batch jobs, DIXF history, users, and legal entities (unless
         -IncludeFO:$false is passed).
      6. Saves everything as structured JSON under -OutputPath

    All operations are READ-ONLY. No changes are made to any environment.

    By default, the script collects everything available: entity record counts
    (-IncludeEntityCounts) and F&O data (-IncludeFO) are both on. Pass
    -IncludeEntityCounts:$false or -IncludeFO:$false to skip those stages.

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
    Fetch record counts per entity for each Dataverse org, and (when combined
    with -IncludeFO) for every entity set exposed by the F&O OData service.
    Default: $true. Adds significant time on large tenants; pass
    -IncludeEntityCounts:$false to skip.

.PARAMETER EntityCountTop
    Safety cap on how many CE entities to count per environment when
    -IncludeEntityCounts is used. 0 = unlimited (count every queryable entity,
    excluding mserp_* virtual tables which are counted via the native F&O
    endpoint instead). Priority order: custom entities first, then known
    high-volume OOB tables, then everything else.
    Default: 0 (unlimited)

.PARAMETER IncludeFO
    Collect Finance & Operations data (batch jobs, DIXF history, users, legal
    entities) for environments where F&O integration is detected via the Dataverse
    RetrieveFinanceAndOperationsIntegrationDetails action. Detection itself always
    runs; this flag only gates the deeper F&O AOS queries. Requires the
    authenticated user to have the System Administrator role in each F&O
    environment.
    Default: $true. Pass -IncludeFO:$false to skip.

.PARAMETER MaxEnvironments
    Safety limit - stops after processing this many environments. 0 = no limit.
    Default: 0

.PARAMETER MaxDegreeOfParallelism
    Number of environments to collect concurrently. Each environment runs in its
    own runspace; the orchestrator joins results after all workers complete.
    Per-environment worker logs are written to environments/<name>/worker.log
    and merged into inventory.log at the end. Set to 1 to force the legacy
    sequential path (useful for debugging). Very high values (>10) risk hitting
    Dataverse/F&O tenant-level throttling; the built-in exponential backoff in
    Invoke-RestWithRetry will survive 429s but slow the overall run.
    Default: 8

.PARAMETER Force
    Overwrite existing data files. By default, environments with a complete
    ce-summary.json are skipped (resume support).

.PARAMETER UseDeviceCode
    Force device-code authentication flow instead of interactive browser login.

.EXAMPLE
    # Full collection, interactive login (entity counts and F&O data are on by default)
    .\Invoke-DataverseInventory.ps1 -OutputPath C:\PPACData

.EXAMPLE
    # Fast run: skip the slow per-entity record counts
    .\Invoke-DataverseInventory.ps1 -OutputPath C:\PPACData -IncludeEntityCounts:$false

.EXAMPLE
    # Skip F&O collection (no System Administrator role in F&O envs)
    .\Invoke-DataverseInventory.ps1 -OutputPath C:\PPACData -IncludeFO:$false

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
    [switch]  $IncludeEntityCounts  = $true,
    [int]     $EntityCountTop       = 0,
    [switch]  $IncludeFO             = $true,
    [switch]  $IncludeMakerInventory = $true,
    [switch]  $IncludeGovernance     = $true,
    [switch]  $IncludeRBAC           = $true,
    [switch]  $IncludeMetadataDepth  = $true,
    [switch]  $IncludeActivity       = $false,
    [int]     $MaxEnvironments      = 0,
    [int]     $MaxDegreeOfParallelism = 8,
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
. (Join-Path $ScriptDir 'collectors\Collect-MakerInventory.ps1')
. (Join-Path $ScriptDir 'collectors\Collect-TenantGovernance.ps1')
. (Join-Path $ScriptDir 'collectors\Collect-RBAC.ps1')
. (Join-Path $ScriptDir 'collectors\Collect-MetadataDepth.ps1')
. (Join-Path $ScriptDir 'collectors\Collect-Activity.ps1')

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

# ── Tenant-level governance collection (runs once) ────────────────────────────
# DLP policies, tenant settings, tenant isolation, and env requests are all
# scoped at the tenant level (not per-env), so fetch them once here and feed
# the flags into every production/sandbox environment later.
$tenantGovernance = $null
if ($IncludeGovernance) {
    try {
        $tenantGovernance = Collect-TenantGovernance `
            -OutputPath $OutputPath `
            -TenantId   $finalCtx.Tenant.Id
    } catch {
        Write-InventoryLog "Tenant governance collection failed: $_" -Level WARN
    }
}

# ── Summary trackers ─────────────────────────────────────────────────────────
$processSummary = [System.Collections.Generic.List[hashtable]]::new()
$totalFlags     = [System.Collections.Generic.List[string]]::new()
$processedCount = 0
$skippedCount   = 0
$errorCount     = 0

# ── Main collection loop ──────────────────────────────────────────────────────
# The per-environment body was extracted into Collect-OneEnvironment.ps1 so the
# same code path serves both sequential and parallel execution. The worker
# returns a hashtable {Status, Entry, WorkerLog, EnvironmentId, DisplayName};
# we aggregate those into $processSummary / $totalFlags after each completes.
$workerScript = Join-Path $ScriptDir 'Collect-OneEnvironment.ps1'
if (-not (Test-Path $workerScript)) { throw "Cannot find worker script: $workerScript" }

# Helper: fold a single worker result into aggregate counters and summary list.
function Add-WorkerResult {
    param($Result)
    if (-not $Result) {
        $script:errorCount++
        return
    }
    switch ($Result.Status) {
        'Processed'    { $script:processedCount++ }
        'Skipped'      { $script:skippedCount++ }
        'SkippedState' { $script:skippedCount++ }
        'Error'        { $script:errorCount++ }
        default        { $script:errorCount++ }
    }
    if ($Result.Entry) {
        $script:processSummary.Add($Result.Entry)
        if ($Result.Entry.AllFlags) {
            $script:totalFlags.AddRange([string[]]$Result.Entry.AllFlags)
        }
    }
}

$workerLogPaths = [System.Collections.Generic.List[string]]::new()

if ($MaxDegreeOfParallelism -le 1) {
    # ── Sequential path ─────────────────────────────────────────────────────
    Write-InventoryLog "Sequential mode (MaxDegreeOfParallelism = $MaxDegreeOfParallelism)." -Indent 1
    $envIdx = 0
    foreach ($env in $environments) {
        $envIdx++
        Write-InventoryLog ''
        Write-InventoryLog "[$envIdx / $($environments.Count)] Dispatching: $($env.DisplayName) ($($env.EnvironmentSku))"
        try {
            $res = & $workerScript `
                -EnvEntry              $env `
                -OutputPath            $OutputPath `
                -ScriptDir             $ScriptDir `
                -EnvIndex              $envIdx `
                -TotalEnvironments     $environments.Count `
                -IncludeEntityCounts   $IncludeEntityCounts.IsPresent `
                -EntityCountTop        $EntityCountTop `
                -IncludeFO             $IncludeFO.IsPresent `
                -IncludeMakerInventory $IncludeMakerInventory.IsPresent `
                -IncludeGovernance     $IncludeGovernance.IsPresent `
                -IncludeRBAC           $IncludeRBAC.IsPresent `
                -IncludeMetadataDepth  $IncludeMetadataDepth.IsPresent `
                -IncludeActivity       $IncludeActivity.IsPresent `
                -Force                 $Force.IsPresent
            Add-WorkerResult -Result $res
            if ($res -and $res.WorkerLog) { $workerLogPaths.Add([string]$res.WorkerLog) }
        } catch {
            Write-InventoryLog "  Worker failed for $($env.DisplayName): $_" -Level ERROR -Indent 1
            $errorCount++
        }
        if ($envIdx % 10 -eq 0) { Start-Sleep -Milliseconds 500 }
    }

} else {
    # ── Parallel path (RunspacePool) ────────────────────────────────────────
    # RunspacePool runs $MaxDegreeOfParallelism environments concurrently inside
    # this same process. Each runspace imports PPACInventory.psm1 independently,
    # writes to its own worker.log, and returns a result hashtable. We poll for
    # completion so progress logging is responsive rather than waiting for the
    # slowest environment to finish before reporting any.
    $effectiveDop = [Math]::Min($MaxDegreeOfParallelism, [Math]::Max(1, $environments.Count))
    Write-InventoryLog "Parallel mode: $effectiveDop concurrent worker(s) across $($environments.Count) environments." -Indent 1

    $iss  = [System.Management.Automation.Runspaces.InitialSessionState]::CreateDefault()
    $pool = [runspacefactory]::CreateRunspacePool(1, $effectiveDop, $iss, $Host)
    $pool.Open()

    $pending = [System.Collections.Generic.List[object]]::new()
    $envIdx  = 0
    foreach ($env in $environments) {
        $envIdx++
        # Guard every step that can throw under memory pressure or when the
        # runspace pool is in a bad state. If Create/AddCommand/BeginInvoke fails
        # for one env, dispose what we've built for that one and keep dispatching
        # the others - a single bad env should not abort the whole parallel run.
        $ps = $null
        try {
            $ps = [powershell]::Create()
            $ps.RunspacePool = $pool
            [void]$ps.AddCommand($workerScript)
            [void]$ps.AddParameters(@{
                EnvEntry              = $env
                OutputPath            = $OutputPath
                ScriptDir             = $ScriptDir
                EnvIndex              = $envIdx
                TotalEnvironments     = $environments.Count
                IncludeEntityCounts   = $IncludeEntityCounts.IsPresent
                EntityCountTop        = $EntityCountTop
                IncludeFO             = $IncludeFO.IsPresent
                IncludeMakerInventory = $IncludeMakerInventory.IsPresent
                IncludeGovernance     = $IncludeGovernance.IsPresent
                IncludeRBAC           = $IncludeRBAC.IsPresent
                IncludeMetadataDepth  = $IncludeMetadataDepth.IsPresent
                IncludeActivity       = $IncludeActivity.IsPresent
                Force                 = $Force.IsPresent
            })
            $async = $ps.BeginInvoke()
            $pending.Add([PSCustomObject]@{
                PowerShell  = $ps
                AsyncResult = $async
                EnvIndex    = $envIdx
                EnvName     = $env.DisplayName
                EnvSku      = $env.EnvironmentSku
            })
        } catch {
            Write-InventoryLog "  [$($env.DisplayName)] failed to dispatch worker: $_" -Level ERROR -Indent 1
            $errorCount++
            if ($ps) { try { $ps.Dispose() } catch {} }
        }
    }

    Write-InventoryLog "Queued $($pending.Count) environment workers. Waiting for completion..." -Indent 1
    try {
        while ($pending.Count -gt 0) {
            $completed = @($pending | Where-Object { $_.AsyncResult.IsCompleted })
            foreach ($job in $completed) {
                $result = $null
                try {
                    $output = $job.PowerShell.EndInvoke($job.AsyncResult)
                    # Worker's return is the last hashtable in the output stream.
                    foreach ($item in $output) {
                        if ($item -is [hashtable] -and $item.ContainsKey('Status')) { $result = $item }
                        elseif ($item -is [psobject] -and $item.PSObject.Properties['Status']) { $result = $item }
                    }
                    if ($job.PowerShell.HadErrors) {
                        foreach ($er in $job.PowerShell.Streams.Error) {
                            Write-InventoryLog "    [$($job.EnvName)] worker error: $er" -Level WARN -Indent 1
                        }
                    }
                } catch {
                    Write-InventoryLog "  [$($job.EnvName)] worker invocation failed: $_" -Level ERROR -Indent 1
                } finally {
                    $job.PowerShell.Dispose()
                }
                if ($result) {
                    $status = if ($result -is [hashtable]) { $result.Status } else { $result.Status }
                    Write-InventoryLog "  [$($job.EnvIndex) / $($environments.Count)] $($job.EnvName) ($($job.EnvSku)) -> $status" -Level OK -Indent 1
                    Add-WorkerResult -Result $result
                    if ($result -is [hashtable] -and $result.WorkerLog) {
                        $workerLogPaths.Add([string]$result.WorkerLog)
                    } elseif ($result -and $result.WorkerLog) {
                        $workerLogPaths.Add([string]$result.WorkerLog)
                    }
                } else {
                    Write-InventoryLog "  [$($job.EnvName)] worker produced no result." -Level ERROR -Indent 1
                    $errorCount++
                }
                [void]$pending.Remove($job)
            }
            if ($pending.Count -gt 0) { Start-Sleep -Milliseconds 500 }
        }
    } finally {
        $pool.Close()
        $pool.Dispose()
    }
}

# ── Merge per-env worker logs into the canonical inventory.log ──────────────
# Each worker wrote to environments/<name>/worker.log. Concatenate them into
# inventory.log so the main log still tells the whole story, with section
# headers to mark env boundaries. Intra-env ordering is preserved; cross-env
# ordering reflects worker completion order (not dispatch order) in parallel
# mode - that's expected and documented.
if ($workerLogPaths.Count -gt 0) {
    Write-InventoryLog ''
    Write-InventoryLog "Merging $($workerLogPaths.Count) worker log(s) into inventory.log..."
    foreach ($wlp in $workerLogPaths) {
        if (-not (Test-Path $wlp)) { continue }
        try {
            $header = "`n===== worker log: $wlp ====="
            Add-Content -Path $logPath -Value $header -Encoding UTF8
            Add-Content -Path $logPath -Value (Get-Content -Path $wlp -Raw) -Encoding UTF8
        } catch {
            Write-InventoryLog "  Could not merge $wlp : $_" -Level WARN -Indent 1
        }
    }
}

# ── Save master summary ───────────────────────────────────────────────────────
Write-InventoryLog ''
Write-InventoryLog 'Saving master summary...'

$masterSummary = @{
    RunAt                  = (Get-Date -Format 'o')
    TenantId               = $finalCtx.Tenant.Id
    AuthenticatedAs        = $finalCtx.Account.Id
    TotalEnvironments      = $allEnvironments.Count
    Processed              = $processedCount
    Skipped                = $skippedCount
    Errors                 = $errorCount
    IncludedFO             = [bool]$IncludeFO
    IncludedEntityCounts   = [bool]$IncludeEntityCounts
    IncludedMakerInventory = [bool]$IncludeMakerInventory
    IncludedGovernance     = [bool]$IncludeGovernance
    IncludedRBAC           = [bool]$IncludeRBAC
    IncludedMetadataDepth  = [bool]$IncludeMetadataDepth
    IncludedActivity       = [bool]$IncludeActivity
    TenantGovernance       = $tenantGovernance
    Environments           = @($processSummary)
    AllFlagsDistinct       = @($totalFlags | Group-Object | Select-Object Name, Count | Sort-Object Count -Descending)
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
