#Requires -Version 5.1
<#
.SYNOPSIS
    Reads the collected inventory data and generates an interactive HTML analysis report.
.DESCRIPTION
    Analyzes all environment JSON files under -DataPath and produces a single
    self-contained HTML report with:
      - Executive summary (total storage, environment counts, issue summary)
      - Environments with flagged issues (sortable/filterable table)
      - Storage analysis (top consumers by DB, file, and log)
      - Unused / low-activity environments
      - Cleanup job gaps (environments missing bulk delete / async cleanup)
      - FO-specific issues (batch errors, missing cleanup jobs)
      - Per-environment drill-down detail

.PARAMETER DataPath
    Path to the root data directory produced by Invoke-DataverseInventory.ps1.
    Default: ..\data  (relative to this script)

.PARAMETER ReportPath
    Where to write the HTML report.
    Default: ..\reports\PPACInventoryReport_<timestamp>.html

.PARAMETER OpenReport
    If specified, opens the report in the default browser after generation.

.EXAMPLE
    .\Generate-Report.ps1 -DataPath C:\PPACData -OpenReport
#>
[CmdletBinding()]
param(
    [string]$DataPath   = '',
    [string]$ReportPath = '',
    [switch]$OpenReport
)

$ErrorActionPreference = 'Stop'
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

if (-not $DataPath) {
    $DataPath = Join-Path (Split-Path -Parent $ScriptDir) 'data'
}
if (-not $ReportPath) {
    $ts = Get-Date -Format 'yyyyMMdd_HHmmss'
    $ReportPath = Join-Path (Split-Path -Parent $ScriptDir) "reports\PPACInventoryReport_$ts.html"
}

if (-not (Test-Path $DataPath)) {
    throw "Data directory not found: $DataPath. Run Invoke-DataverseInventory.ps1 first."
}

Write-Host "Reading data from: $DataPath" -ForegroundColor Cyan

# ── Load master summary ───────────────────────────────────────────────────────
$masterFile = Join-Path $DataPath 'master-summary.json'
if (-not (Test-Path $masterFile)) {
    throw "master-summary.json not found. Run the inventory collection first."
}
$master = Get-Content $masterFile -Raw | ConvertFrom-Json

# ── Load tenant-level governance (once) ───────────────────────────────────────
$tenantGov = $null
$tenantGovFile = Join-Path $DataPath 'tenant\governance-summary.json'
if (Test-Path $tenantGovFile) {
    try { $tenantGov = Get-Content $tenantGovFile -Raw | ConvertFrom-Json } catch {}
}

# ── Load per-environment summaries ────────────────────────────────────────────
$envDetails = [System.Collections.Generic.List[PSObject]]::new()

foreach ($envEntry in $master.Environments) {
    $outDir = $envEntry.OutputDir
    if (-not $outDir -or -not (Test-Path $outDir)) { continue }

    $ceSummaryFile = Join-Path $outDir 'ce-summary.json'
    $foSummaryFile = Join-Path $outDir 'fo-summary.json'
    $metaFile      = Join-Path $outDir 'metadata.json'

    # New collector summaries (Phase 1-5)
    $makerFile     = Join-Path $outDir 'maker-summary.json'
    $govFile       = Join-Path $outDir 'governance-summary.json'
    $rbacFile      = Join-Path $outDir 'rbac-summary.json'
    $mdFile        = Join-Path $outDir 'metadata-depth-summary.json'
    $actFile       = Join-Path $outDir 'activity-summary.json'

    $ceSummary = if (Test-Path $ceSummaryFile) { Get-Content $ceSummaryFile -Raw | ConvertFrom-Json } else { $null }
    $foSummary = if (Test-Path $foSummaryFile) { Get-Content $foSummaryFile -Raw | ConvertFrom-Json } else { $null }
    $meta      = if (Test-Path $metaFile)      { Get-Content $metaFile      -Raw | ConvertFrom-Json } else { $null }
    $maker     = if (Test-Path $makerFile)     { Get-Content $makerFile     -Raw | ConvertFrom-Json } else { $null }
    $gov       = if (Test-Path $govFile)       { Get-Content $govFile       -Raw | ConvertFrom-Json } else { $null }
    $rbac      = if (Test-Path $rbacFile)      { Get-Content $rbacFile      -Raw | ConvertFrom-Json } else { $null }
    $md        = if (Test-Path $mdFile)        { Get-Content $mdFile        -Raw | ConvertFrom-Json } else { $null }
    $activity  = if (Test-Path $actFile)       { Get-Content $actFile       -Raw | ConvertFrom-Json } else { $null }

    # Extract OrgUrl/Host for deep-link token expansion in fix snippets
    $orgUrl = $null; $orgHost = $null
    if ($meta -and $meta.OrgUrl) {
        $orgUrl  = [string]$meta.OrgUrl
        try { $orgHost = ([Uri]$orgUrl).Host } catch { $orgHost = $null }
    }

    $detail = [PSCustomObject]@{
        EnvironmentId   = $envEntry.EnvironmentId
        DisplayName     = $envEntry.DisplayName
        Sku             = $envEntry.EnvironmentSku
        IsDefault       = $envEntry.IsDefault
        State           = $envEntry.State
        Location        = $envEntry.Location
        CreatedTime     = $envEntry.CreatedTime
        StorageDB_MB    = [double]$envEntry.StorageDB_MB
        StorageFile_MB  = [double]$envEntry.StorageFile_MB
        StorageLog_MB   = [double]$envEntry.StorageLog_MB
        StorageTotal_MB = [double]$envEntry.StorageTotal_MB
        HasDataverse    = $envEntry.HasDataverse
        HasFO           = $envEntry.HasFO
        AllFlags        = @($envEntry.AllFlags)
        OrgUrl          = $orgUrl
        OrgHost         = $orgHost
        CE              = $ceSummary
        FO              = $foSummary
        Maker           = $maker
        Governance      = $gov
        RBAC            = $rbac
        MetadataDepth   = $md
        Activity        = $activity
        HasError        = ($null -ne $envEntry.Error)
        ErrorMsg        = $envEntry.Error
    }
    $envDetails.Add($detail)
}

Write-Host "Loaded $($envDetails.Count) environment records." -ForegroundColor Green

# ── Analysis helpers ──────────────────────────────────────────────────────────

function Get-FlagBadgeHtml {
    param([string[]]$Flags)
    if (-not $Flags -or $Flags.Count -eq 0) { return '<span class="badge bg-success">Clean</span>' }
    $html = ''
    foreach ($f in $Flags) {
        # Map flag name prefix patterns to Bootstrap badge colors:
        #   bg-danger   = red  (active breakage or severe misconfiguration)
        #   bg-warning  = amber (health risk, needs attention soon)
        #   bg-secondary = gray (informational / audit-only)
        #   bg-info     = blue  (default catch-all: informational but noteworthy)
        # NOTE: Order matters — PowerShell switch -Wildcard uses first match.
        # More specific patterns must appear before broad ones (e.g., MAILBOX_* before NO_*).
        $cls = switch -Wildcard ($f) {
            'BROKEN_*'               { 'bg-danger'            }  # Active integration failures
            '*OWNED_BY_DISABLED*'    { 'bg-danger'            }  # Flows/apps that will break
            'MAILBOX_*'              { 'bg-danger'            }  # Email sync broken
            'PRODUCTION_NOT_MANAGED*' { 'bg-danger'           }  # Missing governance feature on prod
            'NOT_IN_ENVIRONMENT*'    { 'bg-warning text-dark' }  # No group policy inheritance
            'ENVIRONMENT_ADMIN_*'    { 'bg-warning text-dark' }  # Admin assignment risk
            'STALE_*'                { 'bg-warning text-dark' }  # Abandoned data accumulating
            'TEAMS_*'                { 'bg-warning text-dark' }  # Teams storage approaching limit
            'NO_*'                   { 'bg-warning text-dark' }  # Missing expected configuration
            'HIGH_*'                 { 'bg-danger'            }  # Count above critical threshold
            'LARGE_*'                { 'bg-danger'            }  # Size above critical threshold
            'OLD_*'                  { 'bg-warning text-dark' }  # Data accumulating without cleanup
            'MANY_*'                 { 'bg-warning text-dark' }  # Count above warning threshold
            'FO_*ERROR*'             { 'bg-danger'            }  # FO job errors
            'FO_*FAILED*'            { 'bg-danger'            }  # FO job failures
            'FO_MISSING_*'           { 'bg-warning text-dark' }  # Missing FO cleanup jobs
            '*MISSING_VALUES*'       { 'bg-warning text-dark' }  # Env vars without values
            'AUDIT_DISABLED*'        { 'bg-secondary'         }  # Audit off (info, not a break)
            '*FAILED*'               { 'bg-danger'            }  # Generic failure catch-all
            default                  { 'bg-info text-dark'    }  # Informational
        }
        $fEsc = [System.Web.HttpUtility]::HtmlEncode($f)
        $html += "<span class='badge $cls me-1' title='$fEsc' style='font-size:0.7em'>$($fEsc -replace '_',' ')</span> "
    }
    return $html
}

function Format-MB {
    param([double]$MB)
    if ($MB -ge 1024) { return "$([Math]::Round($MB/1024,1)) GB" }
    return "$([Math]::Round($MB,0)) MB"
}

function Get-SectionValue {
    param($CE, [string]$Section, [string]$Field)
    if (-not $CE) { return 'N/A' }
    $s = $CE.Sections.$Section
    if (-not $s) { return 'N/A' }
    $v = $s.$Field
    if ($null -eq $v) { return 'N/A' }
    return $v
}

# ── Issue categorization ───────────────────────────────────────────────────────
$issueCategories = [ordered]@{
    'Storage (High DB)'        = @($envDetails | Where-Object { $_.StorageDB_MB   -gt 5120  })
    'Storage (High File)'      = @($envDetails | Where-Object { $_.StorageFile_MB -gt 10240 })
    'Storage (High Log)'       = @($envDetails | Where-Object { $_.StorageLog_MB  -gt 2048  })
    'No Bulk Delete Scheduled' = @($envDetails | Where-Object { $_.AllFlags -contains 'NO_SCHEDULED_BULK_DELETE' })
    'Old Completed Async Jobs' = @($envDetails | Where-Object { $_.AllFlags -contains 'OLD_COMPLETED_JOBS_NOT_CLEANED' })
    'High Pending/Suspended Jobs' = @($envDetails | Where-Object { $_.AllFlags -contains 'HIGH_SUSPENDED_JOBS' })
    'High Failed Jobs (30d)'   = @($envDetails | Where-Object { $_.AllFlags -contains 'HIGH_FAILED_JOBS_30D' })
    'No Active Users'          = @($envDetails | Where-Object { $_.AllFlags -contains 'NO_ACTIVE_USERS' })
    'No Recent Activity (90d)' = @($envDetails | Where-Object { $_.AllFlags -contains 'NO_AUDIT_ACTIVITY_90D' })
    'No Duplicate Detection'   = @($envDetails | Where-Object { $_.AllFlags -contains 'NO_DUPLICATE_DETECTION_RULES' })
    'No Retention Policies'    = @($envDetails | Where-Object { $_.AllFlags -contains 'NO_RETENTION_POLICIES' })
    'Many Unmanaged Solutions' = @($envDetails | Where-Object { $_.AllFlags | Where-Object { $_ -match 'HIGH_UNMANAGED' } })
    'FO Batch Errors'          = @($envDetails | Where-Object { $_.AllFlags | Where-Object { $_ -match 'FO_BATCH_JOBS_IN_ERROR' } })
    'FO Missing Cleanup Jobs'  = @($envDetails | Where-Object { $_.AllFlags | Where-Object { $_ -match 'FO_MISSING_CLEANUP' } })
    'FO Cleanup Jobs Disabled' = @($envDetails | Where-Object { $_.AllFlags | Where-Object { $_ -match 'FO_CLEANUP_JOBS_NOT_ENABLED|FO_CLEANUP_JOBS_IN_ERROR' } })
    'DualWrite Map Errors'     = @($envDetails | Where-Object { $_.AllFlags | Where-Object { $_ -match 'DUALWRITE_MAPS_IN_ERROR' } })
    'Broken Connection Refs'   = @($envDetails | Where-Object { $_.AllFlags | Where-Object { $_ -match 'BROKEN_CONNECTION_REFERENCES' } })
    'Flows: Disabled Owners'   = @($envDetails | Where-Object { $_.AllFlags | Where-Object { $_ -match 'ACTIVE_FLOWS_OWNED_BY_DISABLED' } })
    'Env Vars Missing Values'  = @($envDetails | Where-Object { $_.AllFlags | Where-Object { $_ -match 'ENV_VARS_MISSING_VALUES' } })
    'No Managed Solutions'     = @($envDetails | Where-Object { $_.AllFlags | Where-Object { $_ -match 'NO_MANAGED_SOLUTIONS' } })
    'Trials Expiring (>20d)'   = @($envDetails | Where-Object {
        if ($_.Sku -ne 'Trial' -or -not $_.CreatedTime) { return $false }
        $dt = [datetime]::MinValue
        [datetime]::TryParse($_.CreatedTime, [ref]$dt) -and (New-TimeSpan -Start $dt -End (Get-Date)).TotalDays -gt 20
    })
    'Mailbox Sync Errors'      = @($envDetails | Where-Object { $_.AllFlags | Where-Object { $_ -match 'MAILBOX_SYNC_ERRORS' } })
    'Unresolved Duplicates'    = @($envDetails | Where-Object { $_.AllFlags | Where-Object { $_ -match 'HIGH_UNRESOLVED_DUPLICATES|MANY_UNRESOLVED_DUPLICATES' } })
    'High Queue Backlog'       = @($envDetails | Where-Object { $_.AllFlags | Where-Object { $_ -match 'HIGH_QUEUE_ITEM_BACKLOG' } })
    'SLA Violations'           = @($envDetails | Where-Object { $_.AllFlags | Where-Object { $_ -match 'HIGH_SLA_VIOLATIONS' } })
    'Stale BPF Instances'      = @($envDetails | Where-Object { $_.AllFlags | Where-Object { $_ -match 'STALE_BPF_INSTANCES' } })
    'Teams Table Storage'      = @($envDetails | Where-Object { $_.AllFlags | Where-Object { $_ -match 'TEAMS_TABLE_STORAGE_HIGH' } })
    'Not Managed Environment'  = @($envDetails | Where-Object { $_.AllFlags | Where-Object { $_ -match 'PRODUCTION_NOT_MANAGED_ENVIRONMENT' } })
    'No Env Group'             = @($envDetails | Where-Object { $_.AllFlags | Where-Object { $_ -match 'NOT_IN_ENVIRONMENT_GROUP' } })
    'No Dedicated Admin'       = @($envDetails | Where-Object { $_.AllFlags | Where-Object { $_ -match 'NO_DEDICATED_ENVIRONMENT_ADMIN' } })
}

# ── Summary stats ─────────────────────────────────────────────────────────────
$totalDbMB    = ($envDetails | Measure-Object -Property StorageDB_MB    -Sum).Sum
$totalFileMB  = ($envDetails | Measure-Object -Property StorageFile_MB  -Sum).Sum
$totalLogMB   = ($envDetails | Measure-Object -Property StorageLog_MB   -Sum).Sum
$totalAllMB   = ($envDetails | Measure-Object -Property StorageTotal_MB -Sum).Sum
$envsWithFO   = @($envDetails | Where-Object { $_.HasFO }).Count
$envsWithDV   = @($envDetails | Where-Object { $_.HasDataverse }).Count
$envsWithFlags= @($envDetails | Where-Object { $_.AllFlags.Count -gt 0 }).Count

# ── Load configuration files ─────────────────────────────────────────────────
$configDir = Join-Path (Split-Path -Parent $ScriptDir) 'config'

# Flag severity mapping
$flagSeverity = @{ Critical = @(); High = @(); Medium = @(); Low = @(); Weights = @{ Critical = 15; High = 8; Medium = 4; Low = 1 } }
$severityFile = Join-Path $configDir 'flag-severity.json'
if (Test-Path $severityFile) {
    try {
        $fsCfg = Get-Content $severityFile -Raw | ConvertFrom-Json
        $flagSeverity.Critical = @($fsCfg.Critical)
        $flagSeverity.High     = @($fsCfg.High)
        $flagSeverity.Medium   = @($fsCfg.Medium)
        $flagSeverity.Low      = @($fsCfg.Low)
        if ($fsCfg.Weights) {
            $flagSeverity.Weights.Critical = [int]$fsCfg.Weights.Critical
            $flagSeverity.Weights.High     = [int]$fsCfg.Weights.High
            $flagSeverity.Weights.Medium   = [int]$fsCfg.Weights.Medium
            $flagSeverity.Weights.Low      = [int]$fsCfg.Weights.Low
        }
        Write-Host "  Loaded flag severity config ($($flagSeverity.Critical.Count) critical, $($flagSeverity.High.Count) high, $($flagSeverity.Medium.Count) medium, $($flagSeverity.Low.Count) low)" -ForegroundColor DarkGray
    } catch {
        Write-Host "  Warning: Could not parse flag-severity.json - using defaults" -ForegroundColor Yellow
    }
}

# SKU profiles
$skuProfiles = @{}
$skuProfileFile = Join-Path $configDir 'sku-profiles.json'
if (Test-Path $skuProfileFile) {
    try {
        $spRaw = Get-Content $skuProfileFile -Raw | ConvertFrom-Json
        foreach ($prop in $spRaw.PSObject.Properties) {
            if ($prop.Name -ne '_comment') { $skuProfiles[$prop.Name] = $prop.Value }
        }
        Write-Host "  Loaded SKU profiles: $($skuProfiles.Keys -join ', ')" -ForegroundColor DarkGray
    } catch {
        Write-Host "  Warning: Could not parse sku-profiles.json" -ForegroundColor Yellow
    }
}

# Owners
$owners = @{}
$ownersFile = Join-Path $configDir 'owners.json'
if (Test-Path $ownersFile) {
    try {
        $owRaw = Get-Content $ownersFile -Raw | ConvertFrom-Json
        foreach ($prop in $owRaw.PSObject.Properties) {
            if ($prop.Name -notin '_comment','_example') { $owners[$prop.Name] = $prop.Value }
        }
        Write-Host "  Loaded owner data for $($owners.Count) environments" -ForegroundColor DarkGray
    } catch {}
}

# Entity count ignore list
$entityCountIgnore = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
$entityIgnoreFile = Join-Path $configDir 'entity-count-ignore.json'
if (Test-Path $entityIgnoreFile) {
    try {
        $eiRaw = Get-Content $entityIgnoreFile -Raw | ConvertFrom-Json
        foreach ($entry in $eiRaw.Entities) {
            if ($entry.Name) { $null = $entityCountIgnore.Add($entry.Name) }
        }
        Write-Host "  Loaded entity count ignore list: $($entityCountIgnore.Count) entries" -ForegroundColor DarkGray
    } catch {
        Write-Host "  Warning: Could not parse entity-count-ignore.json" -ForegroundColor Yellow
    }
}

# Flag fix snippets — per-flag Why / Fix / DeepLink / Snippet for inline rendering in the Action Plan
$flagFixes = @{}
$flagFixesFile = Join-Path $configDir 'flag-fixes.json'
if (Test-Path $flagFixesFile) {
    try {
        $ffRaw = Get-Content $flagFixesFile -Raw | ConvertFrom-Json
        if ($ffRaw.Fixes) {
            foreach ($prop in $ffRaw.Fixes.PSObject.Properties) {
                if ($prop.Name -notmatch '^_') { $flagFixes[$prop.Name] = $prop.Value }
            }
        }
        Write-Host "  Loaded $($flagFixes.Count) flag-fix snippets" -ForegroundColor DarkGray
    } catch {
        Write-Host "  Warning: Could not parse flag-fixes.json: $_" -ForegroundColor Yellow
    }
}

function Get-FlagFix {
    param([string]$Flag)
    if (-not $Flag) { return $null }
    $flagName = ($Flag -split '\s*\(')[0].Trim()
    if ($flagFixes.ContainsKey($flagName)) { return $flagFixes[$flagName] }
    return $null
}

# Replace {ENV_ID}, {ENV_NAME}, {ORG_URL}, {ORG_HOST}, {TENANT_ID} with per-env values.
# Keeps literal tokens when data is unavailable so the snippet is still self-describing.
function Expand-FixTokens {
    param([string]$Text, [PSObject]$Env, [string]$TenantId)
    if (-not $Text) { return '' }
    $envId   = if ($Env -and $Env.EnvironmentId) { [string]$Env.EnvironmentId } else { '{ENV_ID}' }
    $envName = if ($Env -and $Env.DisplayName)   { [string]$Env.DisplayName   } else { '{ENV_NAME}' }
    $orgUrl  = if ($Env -and $Env.OrgUrl)  { [string]$Env.OrgUrl  } else { '{ORG_URL}' }
    $orgHost = if ($Env -and $Env.OrgHost) { [string]$Env.OrgHost } else { '{ORG_HOST}' }
    $tid     = if ($TenantId) { [string]$TenantId } else { '{TENANT_ID}' }
    return $Text.Replace('{ENV_ID}', $envId).
                  Replace('{ENV_NAME}', $envName).
                  Replace('{ORG_URL}', $orgUrl).
                  Replace('{ORG_HOST}', $orgHost).
                  Replace('{TENANT_ID}', $tid)
}

# ── Governance score computation ─────────────────────────────────────────────
function Get-FlagSeverity {
    param([string]$Flag)
    # Extract flag name prefix (before the parenthetical detail)
    $flagName = ($Flag -split '\s*\(')[0].Trim()
    if ($flagName -in $flagSeverity.Critical) { return 'Critical' }
    if ($flagName -in $flagSeverity.High)     { return 'High' }
    if ($flagName -in $flagSeverity.Medium)   { return 'Medium' }
    if ($flagName -in $flagSeverity.Low)      { return 'Low' }
    return 'Info'
}

function Get-GovernanceScore {
    # Score starts at 100. Each flag deducts points by severity (Critical=15, High=8,
    # Medium=4, Low=1) using weights from config/flag-severity.json. Flags listed in the
    # SKU profile's Suppress list (e.g. NO_RETENTION_POLICIES on Developer environments)
    # are expected for that env type and count as only 1 point instead of full deduction.
    # Score is clamped to [0, 100].
    param([string[]]$Flags, [string]$Sku)
    $score = 100
    $profile = if ($skuProfiles.ContainsKey($Sku)) { $skuProfiles[$Sku] } else { $null }
    $suppressList = if ($profile -and $profile.Suppress) { @($profile.Suppress) } else { @() }

    foreach ($f in $Flags) {
        $flagName = ($f -split '\s*\(')[0].Trim()
        # SKU profile suppression: suppressed flags count as Info (1 point) instead of their normal severity
        if ($flagName -in $suppressList) {
            $score -= 1
            continue
        }
        $sev = Get-FlagSeverity $f
        $deduction = switch ($sev) {
            'Critical' { $flagSeverity.Weights.Critical }
            'High'     { $flagSeverity.Weights.High }
            'Medium'   { $flagSeverity.Weights.Medium }
            'Low'      { $flagSeverity.Weights.Low }
            default    { 0 }
        }
        $score -= $deduction
    }
    return [Math]::Max(0, [Math]::Min(100, $score))
}

# Compute per-environment scores
foreach ($e in $envDetails) {
    $score = Get-GovernanceScore -Flags $e.AllFlags -Sku $e.Sku
    Add-Member -InputObject $e -NotePropertyName 'GovernanceScore' -NotePropertyValue $score -Force

    $ownerInfo = if ($owners.ContainsKey($e.EnvironmentId)) { $owners[$e.EnvironmentId] } else { $null }
    $ownerName = if ($ownerInfo -and $ownerInfo.Owner) { $ownerInfo.Owner } else { '' }
    Add-Member -InputObject $e -NotePropertyName 'Owner' -NotePropertyValue $ownerName -Force
}

# Tenant-wide weighted governance score
# Production environments count 3×, Sandbox 1.5×, Developer 0.5×, Trial 0.25×
# (GovernanceWeight from sku-profiles.json) so production health dominates the score.
# Formula: sum(score × weight) ÷ sum(weights), rounded to the nearest integer.
$weightedSum   = 0.0
$weightTotal   = 0.0
foreach ($e in $envDetails) {
    $sku = $e.Sku
    $w   = if ($skuProfiles.ContainsKey($sku) -and $skuProfiles[$sku].GovernanceWeight) {
               [double]$skuProfiles[$sku].GovernanceWeight
           } else { 1.0 }
    $weightedSum += $e.GovernanceScore * $w
    $weightTotal += $w
}
$tenantScore = if ($weightTotal -gt 0) { [Math]::Round($weightedSum / $weightTotal, 0) } else { 0 }

$criticalEnvs = @($envDetails | Where-Object { $_.GovernanceScore -lt 50 }).Count
$healthyEnvs  = @($envDetails | Where-Object { $_.GovernanceScore -ge 80 }).Count

Write-Host "  Tenant governance score: $tenantScore / 100 ($criticalEnvs critical, $healthyEnvs healthy)" -ForegroundColor $(if ($tenantScore -ge 70) {'Green'} elseif ($tenantScore -ge 40) {'Yellow'} else {'Red'})

# ── Delta reporting (compare with previous run) ─────────────────────────────
# Invoke-DataverseInventory.ps1 saves a timestamped snapshot to data/run-history/
# at the end of every run. This block loads the second-to-last snapshot (Select -Skip 1
# skips the current run's snapshot) and diffs flags and storage totals against it.
#
# Flags are compared by name prefix only — the parenthetical detail suffix is stripped —
# so "HIGH_FAILED_JOBS_30D (12 jobs)" and "HIGH_FAILED_JOBS_30D (8 jobs)" are treated
# as the same issue rather than generating a resolved+new pair on every count change.
# Storage differences ≥ 10 MB are surfaced (growth and reduction both).
$deltaHtml = ''
$runHistoryDir = Join-Path $DataPath 'run-history'
if (Test-Path $runHistoryDir) {
    $previousRuns = @(Get-ChildItem -Path $runHistoryDir -Filter '*.json' | Sort-Object Name -Descending | Select-Object -Skip 1 -First 1)
    if ($previousRuns.Count -gt 0) {
        try {
            $prevRun = Get-Content $previousRuns[0].FullName -Raw | ConvertFrom-Json
            $prevEnvMap = @{}
            foreach ($pe in $prevRun.Environments) { $prevEnvMap[$pe.EnvironmentId] = $pe }

            $newFlags     = [System.Collections.Generic.List[string]]::new()
            $resolvedFlags = [System.Collections.Generic.List[string]]::new()
            $storageGrowth = [System.Collections.Generic.List[PSObject]]::new()

            foreach ($e in $envDetails) {
                $prev = if ($prevEnvMap.ContainsKey($e.EnvironmentId)) { $prevEnvMap[$e.EnvironmentId] } else { $null }
                if (-not $prev) { continue }

                $prevFlags = @($prev.AllFlags)
                $currFlags = @($e.AllFlags)

                # Extract flag name prefixes for comparison
                $prevNames = @($prevFlags | ForEach-Object { ($_ -split '\s*\(')[0].Trim() })
                $currNames = @($currFlags | ForEach-Object { ($_ -split '\s*\(')[0].Trim() })

                foreach ($cn in $currNames) {
                    if ($cn -and $cn -notin $prevNames) { $newFlags.Add("$($e.DisplayName): $cn") }
                }
                foreach ($pn in $prevNames) {
                    if ($pn -and $pn -notin $currNames) { $resolvedFlags.Add("$($e.DisplayName): $pn") }
                }

                # Storage growth
                $prevStorage = if ($prev.StorageTotal_MB) { [double]$prev.StorageTotal_MB } else { 0 }
                $currStorage = [double]$e.StorageTotal_MB
                $growth = $currStorage - $prevStorage
                if ([Math]::Abs($growth) -gt 10) {
                    $storageGrowth.Add([PSCustomObject]@{
                        DisplayName = $e.DisplayName
                        Sku         = $e.Sku
                        Previous    = $prevStorage
                        Current     = $currStorage
                        Growth      = [Math]::Round($growth, 1)
                    })
                }
            }

            $prevDate = try { [datetime]$prevRun.RunAt | Get-Date -Format 'yyyy-MM-dd HH:mm' } catch { $previousRuns[0].BaseName }

            $deltaRows = ''
            if ($newFlags.Count -gt 0) {
                foreach ($nf in ($newFlags | Select-Object -First 30)) {
                    $nfEsc = [System.Web.HttpUtility]::HtmlEncode($nf)
                    $deltaRows += "<tr><td><span class='badge bg-danger'>NEW</span></td><td>$nfEsc</td></tr>"
                }
            }
            if ($resolvedFlags.Count -gt 0) {
                foreach ($rf in ($resolvedFlags | Select-Object -First 30)) {
                    $rfEsc = [System.Web.HttpUtility]::HtmlEncode($rf)
                    $deltaRows += "<tr><td><span class='badge bg-success'>RESOLVED</span></td><td>$rfEsc</td></tr>"
                }
            }

            # Show top 15 growth and top 15 reductions separately so reductions
            # aren't crowded out when many environments grew.
            $storageGrowthRows = ''
            foreach ($sg in ($storageGrowth | Where-Object { $_.Growth -gt 0 } | Sort-Object Growth -Descending | Select-Object -First 15)) {
                $nameEsc = [System.Web.HttpUtility]::HtmlEncode($sg.DisplayName)
                $growthFmt = "+$(Format-MB $sg.Growth)"
                $growthClass = if ($sg.Growth -gt 500) {'text-danger fw-bold'} else {'text-warning'}
                $storageGrowthRows += "<tr><td>$nameEsc</td><td>$($sg.Sku)</td><td>$(Format-MB $sg.Previous)</td><td>$(Format-MB $sg.Current)</td><td class='$growthClass'>$growthFmt</td></tr>"
            }

            $storageReductionRows = ''
            foreach ($sr in ($storageGrowth | Where-Object { $_.Growth -lt 0 } | Sort-Object Growth | Select-Object -First 15)) {
                $nameEsc = [System.Web.HttpUtility]::HtmlEncode($sr.DisplayName)
                $reductionFmt = Format-MB $sr.Growth
                $storageReductionRows += "<tr><td>$nameEsc</td><td>$($sr.Sku)</td><td>$(Format-MB $sr.Previous)</td><td>$(Format-MB $sr.Current)</td><td class='text-success fw-bold'>$reductionFmt</td></tr>"
            }

            $deltaHtml = @"
<section id="delta" class="mb-5">
  <div class="section-header"><h5 class="mb-0">Changes Since Last Run</h5></div>
  <p class="text-muted small">Comparing with previous run: $prevDate ($($newFlags.Count) new issues, $($resolvedFlags.Count) resolved)</p>
  $(if ($deltaRows) {
    @"
  <h6>Flag Changes</h6>
  <table class="table table-sm table-bordered" style="max-width:800px">
    <thead class="table-secondary"><tr><th style="width:80px">Status</th><th>Environment: Flag</th></tr></thead>
    <tbody>$deltaRows</tbody>
  </table>
"@
  } else { "<p class='text-muted'>No flag changes detected.</p>" })
  $(if ($storageGrowthRows) {
    @"
  <h6 class="mt-3">Storage Growth (Top 15)</h6>
  <table class="table table-sm table-bordered" style="max-width:800px">
    <thead class="table-secondary"><tr><th>Environment</th><th>SKU</th><th>Previous</th><th>Current</th><th>Growth</th></tr></thead>
    <tbody>$storageGrowthRows</tbody>
  </table>
"@
  } else { '' })
  $(if ($storageReductionRows) {
    @"
  <h6 class="mt-3">Storage Reductions (Top 15)</h6>
  <table class="table table-sm table-bordered" style="max-width:800px">
    <thead class="table-secondary"><tr><th>Environment</th><th>SKU</th><th>Previous</th><th>Current</th><th>Reduction</th></tr></thead>
    <tbody>$storageReductionRows</tbody>
  </table>
"@
  } else { '' })
</section>
"@
            Write-Host "  Delta report: $($newFlags.Count) new flags, $($resolvedFlags.Count) resolved, $($storageGrowth.Count) storage changes" -ForegroundColor Cyan
        } catch {
            Write-Host "  Warning: Could not compute delta report: $_" -ForegroundColor Yellow
        }
    } else {
        Write-Host "  No previous run found for delta comparison" -ForegroundColor DarkGray
    }
}

Add-Type -AssemblyName System.Web

# ── Priority Action Plan (ranked top-N across tenant) ────────────────────────
# Each flag is scored: severityWeight (from flag-severity.json) × skuWeight (from sku-profiles.json)
# × blastRadius (log10 of the parenthetical count, if any; otherwise 1). Highest scores float up,
# so the result is the top issues to fix tenant-wide — severity, SKU importance, and scale combined.
$actionPlan = [System.Collections.Generic.List[PSObject]]::new()
foreach ($e in $envDetails) {
    $skuWeight = if ($skuProfiles.ContainsKey($e.Sku) -and $skuProfiles[$e.Sku].GovernanceWeight) {
                     [double]$skuProfiles[$e.Sku].GovernanceWeight
                 } else { 1.0 }
    $suppressList = if ($skuProfiles.ContainsKey($e.Sku) -and $skuProfiles[$e.Sku].Suppress) {
                        @($skuProfiles[$e.Sku].Suppress)
                    } else { @() }

    foreach ($f in $e.AllFlags) {
        $flagName = ($f -split '\s*\(')[0].Trim()
        # SKU-suppressed flags: expected for this env type, don't surface in action plan
        if ($flagName -in $suppressList) { continue }

        $sev = Get-FlagSeverity $f
        $sevW = switch ($sev) {
            'Critical' { 15.0 }
            'High'     { 8.0  }
            'Medium'   { 4.0  }
            'Low'      { 1.0  }
            default    { 0.0  }
        }
        if ($sevW -eq 0) { continue }  # skip info-only flags

        # Blast radius from parenthetical count ("HIGH_FAILED_JOBS_30D (12 jobs)" → use 12)
        $blast = 1.0
        if ($f -match '\(\s*([0-9]+(?:[,0-9]+)?)') {
            $n = [double]($matches[1] -replace ',','')
            if ($n -gt 0) { $blast = [Math]::Max(1.0, [Math]::Log10($n + 10)) }
        }

        $priority = $sevW * $skuWeight * $blast

        $actionPlan.Add([PSCustomObject]@{
            Env      = $e
            Flag     = $f
            FlagName = $flagName
            Severity = $sev
            SevW     = $sevW
            SkuW     = $skuWeight
            Blast    = $blast
            Priority = $priority
        })
    }
}
$topActions = @($actionPlan | Sort-Object -Property Priority -Descending | Select-Object -First 15)
Write-Host "  Priority action plan: $($topActions.Count) top items (out of $($actionPlan.Count) scored flags)" -ForegroundColor Cyan

# ── Trends & Forecast (all run-history snapshots) ───────────────────────────
# Loads every snapshot in data/run-history, recomputes per-snapshot tenant score + flag breakdown
# + storage totals, and builds a per-env storage history for linear-regression forecasting.
$trendData   = [System.Collections.Generic.List[PSObject]]::new()
$envHistory  = @{}
$trendLabels = [System.Collections.Generic.List[string]]::new()

if (Test-Path $runHistoryDir) {
    $allSnapshots = @(Get-ChildItem -Path $runHistoryDir -Filter '*.json' | Sort-Object Name)
    foreach ($sf in $allSnapshots) {
        try {
            $snap = Get-Content $sf.FullName -Raw | ConvertFrom-Json
            $runAt = $null
            try { $runAt = [datetime]$snap.RunAt } catch { $runAt = $null }
            if (-not $runAt) {
                # Fall back to filename: YYYY-MM-DD_HHMMSS
                if ($sf.BaseName -match '^(\d{4}-\d{2}-\d{2})[_ ](\d{2})(\d{2})(\d{2})$') {
                    $runAt = [datetime]"$($matches[1])T$($matches[2]):$($matches[3]):$($matches[4])"
                } else { continue }
            }

            $totDb = 0.0; $totFile = 0.0; $totLog = 0.0
            $critN = 0; $highN = 0; $medN = 0; $lowN = 0
            $wSum  = 0.0; $wTot = 0.0

            foreach ($se in $snap.Environments) {
                $totDb   += [double]$se.StorageDB_MB
                $totFile += [double]$se.StorageFile_MB
                $totLog  += [double]$se.StorageLog_MB

                $sFlags = @($se.AllFlags)
                foreach ($sf2 in $sFlags) {
                    switch (Get-FlagSeverity $sf2) {
                        'Critical' { $critN++ }
                        'High'     { $highN++ }
                        'Medium'   { $medN++ }
                        'Low'      { $lowN++ }
                    }
                }

                $sSku = [string]$se.EnvironmentSku
                $sw = if ($skuProfiles.ContainsKey($sSku) -and $skuProfiles[$sSku].GovernanceWeight) {
                          [double]$skuProfiles[$sSku].GovernanceWeight
                      } else { 1.0 }
                $sScore = Get-GovernanceScore -Flags $sFlags -Sku $sSku
                $wSum += $sScore * $sw
                $wTot += $sw

                # Per-env storage history for forecasting
                if (-not $envHistory.ContainsKey($se.EnvironmentId)) {
                    $envHistory[$se.EnvironmentId] = [System.Collections.Generic.List[PSObject]]::new()
                }
                $envHistory[$se.EnvironmentId].Add([PSCustomObject]@{
                    RunAt   = $runAt
                    TotalMB = [double]$se.StorageTotal_MB
                    DbMB    = [double]$se.StorageDB_MB
                    FileMB  = [double]$se.StorageFile_MB
                    LogMB   = [double]$se.StorageLog_MB
                })
            }

            $tScore = if ($wTot -gt 0) { [Math]::Round($wSum / $wTot, 1) } else { 0.0 }
            $trendLabels.Add($runAt.ToString('yyyy-MM-dd HH:mm'))
            $trendData.Add([PSCustomObject]@{
                RunAt     = $runAt
                TotalDbGB   = [Math]::Round($totDb   / 1024.0, 2)
                TotalFileGB = [Math]::Round($totFile / 1024.0, 2)
                TotalLogGB  = [Math]::Round($totLog  / 1024.0, 2)
                TotalGB     = [Math]::Round(($totDb + $totFile + $totLog) / 1024.0, 2)
                Critical  = $critN
                High      = $highN
                Medium    = $medN
                Low       = $lowN
                Score     = $tScore
            })
        } catch {
            Write-Host "  Warning: could not parse snapshot $($sf.Name): $_" -ForegroundColor Yellow
        }
    }
}
Write-Host "  Trends: loaded $($trendData.Count) run-history snapshots" -ForegroundColor Cyan

# Ordinary least squares forecast — returns null if fewer than 2 points or zero variance in X.
function Invoke-LinearForecast {
    param([object[]]$Points, [string]$YField)
    if (-not $Points -or $Points.Count -lt 2) { return $null }
    $xs = @($Points | ForEach-Object { [double]($_.RunAt - [datetime]'1970-01-01').TotalDays })
    $ys = @($Points | ForEach-Object { [double]$_.$YField })
    $n = $xs.Count
    $sx = 0.0; $sy = 0.0; $sxy = 0.0; $sxx = 0.0
    for ($i = 0; $i -lt $n; $i++) {
        $sx += $xs[$i]; $sy += $ys[$i]
        $sxy += $xs[$i] * $ys[$i]; $sxx += $xs[$i] * $xs[$i]
    }
    $denom = ($n * $sxx) - ($sx * $sx)
    if ($denom -eq 0) { return $null }
    $slope     = (($n * $sxy) - ($sx * $sy)) / $denom
    $intercept = ($sy - ($slope * $sx)) / $n

    # R² — coefficient of determination
    $meanY = $sy / $n
    $ssTot = 0.0; $ssRes = 0.0
    for ($i = 0; $i -lt $n; $i++) {
        $pred   = ($slope * $xs[$i]) + $intercept
        $ssRes += [Math]::Pow($ys[$i] - $pred, 2)
        $ssTot += [Math]::Pow($ys[$i] - $meanY, 2)
    }
    $r2 = if ($ssTot -gt 0) { 1 - ($ssRes / $ssTot) } else { 1.0 }
    return @{ Slope = $slope; Intercept = $intercept; R2 = $r2; N = $n }
}

# ── Build HTML rows for environments table ────────────────────────────────────

function Build-EnvTableRows {
    param([object[]]$Envs)
    $sb = [System.Text.StringBuilder]::new()

    foreach ($e in ($Envs | Sort-Object StorageTotal_MB -Descending)) {
        $flags    = Get-FlagBadgeHtml -Flags $e.AllFlags
        $skuClass = switch ($e.Sku) {
            'Production' { 'table-light'   }
            'Sandbox'    { 'table-info'    }
            'Trial'      { 'table-warning' }
            'Developer'  { 'table-success' }
            default      { ''              }
        }
        $foTag    = if ($e.HasFO) { '<span class="badge bg-purple" style="background:#6f42c1">FO</span>' } else { '' }
        $defTag   = if ($e.IsDefault) { '<span class="badge bg-secondary">Default</span>' } else { '' }
        $errTag   = if ($e.HasError)  { '<span class="badge bg-danger">Error</span>'       } else { '' }

        $users     = Get-SectionValue $e.CE 'Users'      'ActiveCount'
        $bulkDel   = Get-SectionValue $e.CE 'BulkDeleteJobs' 'ScheduledCount'
        $asyncFail = Get-SectionValue $e.CE 'AsyncOperations' 'Counts'
        $failNum   = if ($asyncFail -ne 'N/A' -and $asyncFail.Failed_Last30d -ne $null) {
                        $asyncFail.Failed_Last30d
                     } else { 'N/A' }

        $auditInfo = Get-SectionValue $e.CE 'AuditLog' 'LastEntry'
        $lastAudit = if ($auditInfo -and $auditInfo -ne 'N/A') {
                        try { [datetime]$auditInfo | Get-Date -Format 'yyyy-MM-dd' } catch { $auditInfo }
                     } else { 'N/A' }

        $totalFmt = Format-MB $e.StorageTotal_MB
        $dbFmt    = Format-MB $e.StorageDB_MB
        $fileFmt  = Format-MB $e.StorageFile_MB
        $logFmt   = Format-MB $e.StorageLog_MB

        $nameEsc = [System.Web.HttpUtility]::HtmlEncode($e.DisplayName)
        $stateVal    = if ($e.State)    { $e.State }    else { 'N/A' }
        $locationVal = if ($e.Location) { $e.Location } else { 'N/A' }

        # Governance score badge
        $gs = if ($null -ne $e.GovernanceScore) { $e.GovernanceScore } else { 'N/A' }
        $gsClass = if ($gs -is [int] -or $gs -is [double]) {
            if     ($gs -ge 80) { 'bg-success' }
            elseif ($gs -ge 50) { 'bg-warning text-dark' }
            else                { 'bg-danger' }
        } else { 'bg-secondary' }

        # Owner
        $ownerEsc = if ($e.Owner) { [System.Web.HttpUtility]::HtmlEncode($e.Owner) } else { '<span class="text-muted">-</span>' }

        $null = $sb.Append(@"
<tr class='$skuClass'>
  <td><strong>$nameEsc</strong><br><small class='text-muted'>$($e.EnvironmentId)</small><br>$defTag $foTag $errTag</td>
  <td>$($e.Sku)</td>
  <td>$stateVal</td>
  <td>$locationVal</td>
  <td data-sort='$($e.StorageTotal_MB)'><strong>$totalFmt</strong><br><small>DB: $dbFmt<br>File: $fileFmt<br>Log: $logFmt</small></td>
  <td>$users</td>
  <td data-sort='$gs'><span class='badge $gsClass'>$gs</span></td>
  <td><small>$ownerEsc</small></td>
  <td>$lastAudit</td>
  <td>$flags</td>
</tr>
"@)
    }
    return $sb.ToString()
}

$envTableRows = Build-EnvTableRows -Envs $envDetails

# ── Top storage consumers ─────────────────────────────────────────────────────
function Build-StorageTable {
    param([object[]]$Envs, [string]$SortField)
    $sb   = [System.Text.StringBuilder]::new()
    $rank = 0
    foreach ($e in ($Envs | Sort-Object $SortField -Descending | Select-Object -First 25)) {
        $rank++
        $val = $e.$SortField
        $fmtVal = Format-MB $val
        $nameEsc = [System.Web.HttpUtility]::HtmlEncode($e.DisplayName)
        $productTags = ''
        if ($e.HasDataverse) { $productTags += '<span class="badge bg-info text-dark" style="font-size:0.7em">CE</span> ' }
        if ($e.HasFO)        { $productTags += '<span class="badge text-white" style="background:#6f42c1;font-size:0.7em">FO</span> ' }
        $bar = [Math]::Min(100, [Math]::Round($val / ([Math]::Max(1,($Envs | Measure-Object $SortField -Max).Maximum)) * 100, 0))
        $barColor = if ($bar -gt 80) {'bg-danger'} elseif ($bar -gt 50) {'bg-warning'} else {'bg-info'}
        $null = $sb.Append(@"
<tr>
  <td>$rank</td>
  <td>$nameEsc $productTags<br><small class='text-muted'>$($e.Sku) | $($e.Location)</small></td>
  <td>$fmtVal</td>
  <td><div class='progress' style='min-width:100px'><div class='progress-bar $barColor' role='progressbar' style='width:${bar}%'>$bar%</div></div></td>
</tr>
"@)
    }
    return $sb.ToString()
}

$topDBRows   = Build-StorageTable -Envs $envDetails -SortField 'StorageDB_MB'
$topFileRows = Build-StorageTable -Envs $envDetails -SortField 'StorageFile_MB'
$topLogRows  = Build-StorageTable -Envs $envDetails -SortField 'StorageLog_MB'

# ── Issue summary cards ───────────────────────────────────────────────────────
$issueCardsHtml = ''
foreach ($cat in $issueCategories.Keys) {
    $cnt = $issueCategories[$cat].Count
    if ($cnt -eq 0) { continue }
    $severity = if ($cnt -gt 10) {'danger'} elseif ($cnt -gt 3) {'warning'} else {'info'}
    $catEsc = [System.Web.HttpUtility]::HtmlEncode($cat)
    $issueCardsHtml += @"
<div class='col-md-4 col-lg-3 mb-3'>
  <div class='card border-$severity h-100'>
    <div class='card-body text-center'>
      <h2 class='text-$severity fw-bold'>$cnt</h2>
      <p class='card-text small'>$catEsc</p>
    </div>
  </div>
</div>
"@
}

# ── Governance score table rows ───────────────────────────────────────────────
$govTableRows = ''
foreach ($e in ($envDetails | Sort-Object GovernanceScore)) {
    $nameEsc = [System.Web.HttpUtility]::HtmlEncode($e.DisplayName)
    $gs = $e.GovernanceScore
    $gsClass = if ($gs -ge 80) {'bg-success'} elseif ($gs -ge 50) {'bg-warning text-dark'} else {'bg-danger'}
    $barColor = if ($gs -ge 80) {'bg-success'} elseif ($gs -ge 50) {'bg-warning'} else {'bg-danger'}

    # Count flags by severity
    $critCount = @($e.AllFlags | Where-Object { (Get-FlagSeverity $_) -eq 'Critical' }).Count
    $highCount = @($e.AllFlags | Where-Object { (Get-FlagSeverity $_) -eq 'High' }).Count
    $medCount  = @($e.AllFlags | Where-Object { (Get-FlagSeverity $_) -eq 'Medium' }).Count
    $lowCount  = @($e.AllFlags | Where-Object { (Get-FlagSeverity $_) -eq 'Low' }).Count

    $ownerEsc = if ($e.Owner) { [System.Web.HttpUtility]::HtmlEncode($e.Owner) } else { '-' }

    $govTableRows += @"
<tr>
  <td>$nameEsc<br><small class='text-muted'>$($e.Sku)</small></td>
  <td data-sort='$gs'><span class='badge $gsClass'>$gs</span>
    <div class='progress mt-1' style='height:4px'><div class='progress-bar $barColor' style='width:${gs}%'></div></div>
  </td>
  <td>$(if ($critCount -gt 0) {"<span class='badge bg-danger'>$critCount</span>"} else {'0'})</td>
  <td>$(if ($highCount -gt 0) {"<span class='badge bg-danger'>$highCount</span>"} else {'0'})</td>
  <td>$(if ($medCount -gt 0) {"<span class='badge bg-warning text-dark'>$medCount</span>"} else {'0'})</td>
  <td>$lowCount</td>
  <td><small>$ownerEsc</small></td>
</tr>
"@
}

# ── FO environment detail rows ────────────────────────────────────────────────
$foEnvs = @($envDetails | Where-Object { $_.HasFO })
$foTableRows = ''
foreach ($e in $foEnvs) {
    $nameEsc     = [System.Web.HttpUtility]::HtmlEncode($e.DisplayName)
    $foSec       = if ($e.FO) { $e.FO.Sections } else { $null }
    $bjCount     = if ($foSec -and $foSec.BatchJobs) { $foSec.BatchJobs.TotalCount  } else { 'N/A' }
    $bjError     = if ($foSec -and $foSec.BatchJobs) { $foSec.BatchJobs.ErrorCount  } else { 'N/A' }
    $missingClnp = if ($foSec -and $foSec.FOCleanupJobs) { $foSec.FOCleanupJobs.MissingStandardJobs.Count } else { 'N/A' }
    $disabledClnp = if ($foSec -and $foSec.FOCleanupJobs) { [int]$foSec.FOCleanupJobs.FoundButAllDisabled } else { 'N/A' }
    $errorClnp    = if ($foSec -and $foSec.FOCleanupJobs) { [int]$foSec.FOCleanupJobs.FoundInErrorOnly    } else { 'N/A' }
    $foUsers     = if ($foSec -and $foSec.FOUsers)  { $foSec.FOUsers.EnabledCount    } else { 'N/A' }
    $foActive90  = if ($foSec -and $foSec.FOUsers)  { $foSec.FOUsers.ActiveLast90d   } else { 'N/A' }
    $dwMaps      = if ($foSec -and $foSec.DualWrite) { $foSec.DualWrite.MapCount     } else { 'N/A' }
    $dwErrors    = if ($foSec -and $foSec.DualWrite) { $foSec.DualWrite.ErrorMapCount} else { 'N/A' }
    $foFlags     = @($e.AllFlags | Where-Object { $_ -match '^FO_|^DUALWRITE' })
    $flagsHtml   = Get-FlagBadgeHtml -Flags $foFlags

    $disabledCell = if ($disabledClnp -eq 'N/A') { 'N/A' } elseif ($disabledClnp -gt 0) { "<span class='text-warning fw-bold'>$disabledClnp</span>" } else { "$disabledClnp" }
    $errorCell    = if ($errorClnp -eq 'N/A')    { 'N/A' } elseif ($errorClnp -gt 0)    { "<span class='text-danger fw-bold'>$errorClnp</span>"    } else { "$errorClnp" }

    $foTableRows += @"
<tr>
  <td>$nameEsc<br><small class='text-muted'>$($e.Sku)</small></td>
  <td>$bjCount</td>
  <td><span class='$(if ($bjError -gt 0) {"text-danger fw-bold"} else {""})'>$bjError</span></td>
  <td>$missingClnp</td>
  <td>$disabledCell</td>
  <td>$errorCell</td>
  <td>$foUsers</td>
  <td>$foActive90</td>
  <td>$dwMaps</td>
  <td><span class='$(if ($dwErrors -gt 0) {"text-danger fw-bold"} else {""})'>$dwErrors</span></td>
  <td>$flagsHtml</td>
</tr>
"@
}

# ── Per-env Top 25 Tables by Record Count (storage concentration proxy) ──────
# Microsoft does not expose per-table bytes via any public Dataverse/F&O API.
# Record count from RetrieveTotalRecordCount is the documented signal we can get,
# so we surface it here as a proxy for where the data lives in each environment.
# Only populated when -IncludeEntityCounts was used during collection.
$topTablesEnvCards = ''
$envsWithCounts    = [System.Collections.Generic.List[PSObject]]::new()

foreach ($envEntry in $master.Environments) {
    if (-not $envEntry.OutputDir) { continue }
    $countsFile   = Join-Path $envEntry.OutputDir 'entity-counts.json'
    $foCountsFile = Join-Path $envEntry.OutputDir 'fo-entity-counts.json'

    # Historical bug: some entity-count files were serialized as {"value":[...],"Count":N}
    # instead of a bare JSON array, because ConvertTo-Json treats piped multi-element
    # arrays that way under PS 5.1. Unwrap transparently so legacy data still loads.
    function _Unwrap-CountArray {
        param($raw)
        if ($null -eq $raw) { return @() }
        if ($raw -is [System.Array]) { return $raw }
        if ($raw.PSObject.Properties.Name -contains 'value' -and ($raw.value -is [System.Array])) {
            return $raw.value
        }
        return @($raw)
    }

    $ceCounts = @()
    if (Test-Path $countsFile) {
        $ceRaw = Get-Content $countsFile -Raw | ConvertFrom-Json
        $ceCounts = @(_Unwrap-CountArray $ceRaw)
    }

    $foCounts = @()
    if (Test-Path $foCountsFile) {
        $foRaw = Get-Content $foCountsFile -Raw | ConvertFrom-Json
        $foArr = @(_Unwrap-CountArray $foRaw)
        # Normalize: older F&O rows may lack Source; tag anything from this file as F&O.
        $foCounts = @($foArr | ForEach-Object {
            $obj = $_
            if (-not ($obj.PSObject.Properties.Name -contains 'Source') -or -not $obj.Source) {
                $obj | Add-Member -NotePropertyName Source -NotePropertyValue 'FO' -Force
            }
            $obj
        })
    }

    $countsArr = @()
    if ($ceCounts.Count -gt 0) { $countsArr += $ceCounts }
    if ($foCounts.Count -gt 0) { $countsArr += $foCounts }
    if ($entityCountIgnore.Count -gt 0) {
        $countsArr = @($countsArr | Where-Object { -not $entityCountIgnore.Contains([string]$_.LogicalName) })
    }
    if ($countsArr.Count -eq 0) { continue }

    $top25       = @($countsArr | Sort-Object -Property RecordCount -Descending | Select-Object -First 25)
    $totalTop25  = ($top25     | Measure-Object -Property RecordCount -Sum).Sum
    $totalAll    = ($countsArr | Measure-Object -Property RecordCount -Sum).Sum

    $envsWithCounts.Add([PSCustomObject]@{
        EnvironmentId = $envEntry.EnvironmentId
        DisplayName   = $envEntry.DisplayName
        Sku           = $envEntry.EnvironmentSku
        HasFO         = $envEntry.HasFO
        HasDataverse  = $envEntry.HasDataverse
        Top25         = $top25
        TotalTop25    = [int64]($totalTop25 | ForEach-Object { if ($null -eq $_) { 0 } else { $_ } })
        TotalAll      = [int64]($totalAll   | ForEach-Object { if ($null -eq $_) { 0 } else { $_ } })
        CountedTables = $countsArr.Count
    })
}

# Sort envs by total records across counted tables (data-heaviest first)
$envsWithCountsSorted = @($envsWithCounts | Sort-Object -Property TotalAll -Descending)

foreach ($envInfo in $envsWithCountsSorted) {
    $nameEsc    = [System.Web.HttpUtility]::HtmlEncode([string]$envInfo.DisplayName)
    $envIdSafe  = ($envInfo.EnvironmentId -replace '[^a-zA-Z0-9]', '_')
    $skuEsc     = [System.Web.HttpUtility]::HtmlEncode([string]$envInfo.Sku)
    $totalFmt   = '{0:N0}' -f $envInfo.TotalAll
    $top25Fmt   = '{0:N0}' -f $envInfo.TotalTop25
    $tableCount = $envInfo.CountedTables

    $productTags = ''
    if ($envInfo.HasDataverse) { $productTags += '<span class="badge bg-info text-dark" style="font-size:0.75em">CE</span> ' }
    if ($envInfo.HasFO)        { $productTags += '<span class="badge text-white" style="background:#6f42c1;font-size:0.75em">FO</span> ' }
    $rows = ''
    $rank = 0
    foreach ($t in $envInfo.Top25) {
        $rank++
        $ln = [System.Web.HttpUtility]::HtmlEncode([string]$t.LogicalName)
        $dnRaw = if ($t.DisplayName) { [string]$t.DisplayName } else { [string]$t.LogicalName }
        $dn = [System.Web.HttpUtility]::HtmlEncode($dnRaw)
        $isFO = ($t.PSObject.Properties.Name -contains 'Source') -and ($t.Source -eq 'FO')
        $typeBadge = if ($isFO) {
            "<span class='badge bg-warning text-dark'>F&amp;O</span>"
        } elseif ($t.IsCustom) {
            "<span class='badge bg-info text-dark'>Custom</span>"
        } else {
            "<span class='badge bg-secondary'>OOB</span>"
        }
        $rc = [int64]$t.RecordCount
        $rcFmt = '{0:N0}' -f $rc
        $rows += "<tr><td>$rank</td><td><code>$ln</code></td><td><small>$dn</small></td><td>$typeBadge</td><td class='text-end fw-bold'>$rcFmt</td></tr>"
    }

    $topTablesEnvCards += @"
<div class='card mb-2'>
  <div class='card-header bg-light p-2'>
    <button class='btn btn-link text-decoration-none p-0 fw-bold text-start' data-bs-toggle='collapse' data-bs-target='#tt_$envIdSafe'>
      $nameEsc $productTags<small class='text-muted fw-normal'>($skuEsc) &mdash; $tableCount tables counted, $totalFmt total records (top-25: $top25Fmt)</small>
    </button>
  </div>
  <div id='tt_$envIdSafe' class='collapse'>
    <div class='card-body p-0'>
      <table class='table table-sm table-striped mb-0'>
        <thead class='table-dark'>
          <tr><th style='width:50px'>#</th><th>Logical Name</th><th>Display Name</th><th>Type</th><th class='text-end' style='width:140px'>Record Count</th></tr>
        </thead>
        <tbody>$rows</tbody>
      </table>
    </div>
  </div>
</div>
"@
}

if ($envsWithCountsSorted.Count -eq 0) {
    $topTablesEnvCards = @"
<div class='alert alert-info mb-0'>
  No entity record counts were collected. Re-run the inventory with <code>-IncludeEntityCounts</code> to populate this section.
</div>
"@
}

# ── Storage cleanup recommendations ───────────────────────────────────────────
# Synthesizes CleanupTableHealth, AsyncOperations, and OrgSettings data from the
# CE collector into per-environment, ranked, actionable cleanup recommendations.
# Each recommendation maps to a specific Bulk Delete Job or settings change.
$storageCleanupRows = ''
$cleanupRecCount    = 0

foreach ($e in ($envDetails | Sort-Object StorageTotal_MB -Descending)) {
    if (-not $e.CE -or -not $e.CE.Sections) { continue }
    $sections = $e.CE.Sections
    $cth        = $sections.CleanupTableHealth
    $asyncCnt   = if ($sections.AsyncOperations) { $sections.AsyncOperations.Counts } else { $null }
    $orgSet     = $sections.OrgSettings
    $bulkDelSec = $sections.BulkDeleteJobs
    $staleProc  = $sections.StaleProcessInstances
    $nameEsc    = [System.Web.HttpUtility]::HtmlEncode($e.DisplayName)

    $recs = [System.Collections.Generic.List[PSObject]]::new()

    # Completed async operations older than 90 days — largest single DB storage category
    if ($asyncCnt -and $asyncCnt.CompletedOlderThan90d -gt 0) {
        $cnt = [int]$asyncCnt.CompletedOlderThan90d
        if ($cnt -gt 0) {
            $prio = if ($cnt -gt 100000) { 'High' } elseif ($cnt -gt 10000) { 'Medium' } else { $null }
            if ($prio) {
                $coverNote = if ($bulkDelSec -and -not $bulkDelSec.CoversAsyncOps) { ' <strong>No matching bulk delete job detected &mdash; create one now.</strong>' } else { '' }
                $recs.Add([PSCustomObject]@{
                    Priority = $prio; StorageType = 'DB + Log'
                    DataType = 'Completed Async Operations (&gt;90d)'
                    Count    = $cnt
                    Action   = "Affects AsyncOperationBase and WorkflowLogBase (typically the largest DB storage category). Create or verify a recurring Bulk Delete job: <strong>Settings &rarr; Data Management &rarr; Bulk Record Deletion &rarr; New</strong> &rarr; Entity = <em>System Jobs</em>, filter Status Reason = Succeeded AND Canceled AND Failed, Created On older than 90 days. Schedule weekly outside business hours.$coverNote"
                })
            }
        }
    }

    # High failed async jobs in last 30 days — systemic breakage, accumulates in AsyncOperationBase
    if ($asyncCnt -and $asyncCnt.Failed_Last30d -gt 0) {
        $cnt = [int]$asyncCnt.Failed_Last30d
        $prio = if ($cnt -gt 5000) { 'High' } elseif ($cnt -gt 1000) { 'Medium' } elseif ($cnt -gt 200) { 'Low' } else { $null }
        if ($prio) {
            $recs.Add([PSCustomObject]@{
                Priority = $prio; StorageType = 'DB'
                DataType = "Failed System Jobs &mdash; Last 30d ($cnt)"
                Count    = $cnt
                Action   = 'Failed jobs accumulate in AsyncOperationBase. Investigate root cause: <strong>Settings &rarr; System &rarr; System Jobs</strong> (filter Status = Failed). Identify and fix broken plugins or flows. Then add a Bulk Delete job: <strong>Settings &rarr; Data Management &rarr; Bulk Record Deletion &rarr; New</strong> &rarr; Entity = <em>System Jobs</em>, Status Reason = Failed AND Created On older than 30 days.'
            })
        }
    }

    # Suspended system jobs — often a sign of a broken plugin or workflow loop
    if ($asyncCnt -and $asyncCnt.Suspended -gt 500) {
        $cnt = [int]$asyncCnt.Suspended
        $recs.Add([PSCustomObject]@{
            Priority = 'High'; StorageType = 'DB'
            DataType = 'Suspended System Jobs'
            Count    = $cnt
            Action   = 'Investigate root cause first (broken plugin or infinite-loop workflow): <strong>Settings &rarr; System &rarr; System Jobs</strong> (filter Status = Suspended). Review job descriptions to identify the offending customization. After resolving, delete via Bulk Delete job: <strong>Settings &rarr; Data Management &rarr; Bulk Record Deletion &rarr; New</strong> &rarr; Entity = <em>System Jobs</em>, Status = Suspended AND Created On older than 30 days.'
        })
    }

    # Old succeeded workflow logs — cascade-deleted when the parent async op is deleted
    if ($cth -and $cth.WorkflowLogOldSucceeded -gt 0) {
        $cnt = [int]$cth.WorkflowLogOldSucceeded
        $prio = if ($cnt -gt 50000) { 'High' } elseif ($cnt -gt 10000) { 'Medium' } else { $null }
        if ($prio) {
            $recs.Add([PSCustomObject]@{
                Priority = $prio; StorageType = 'DB'
                DataType = 'Old Succeeded Workflow Logs (&gt;30d)'
                Count    = $cnt
                Action   = 'WorkflowLog (WorkflowLogBase) records accumulate when the async operation cleanup job is not running. These cascade-delete automatically when their parent System Job is deleted. Ensure a recurring Bulk Delete job is targeting <em>System Jobs</em> (Status = Succeeded AND Created On &lt; 30 days ago): <strong>Settings &rarr; Data Management &rarr; Bulk Record Deletion</strong>.'
            })
        }
    }

    # Plugin trace logging is on — fills Log storage quickly; should be Off in production
    if ($orgSet -and $orgSet.PluginTraceLogSetting -and $orgSet.PluginTraceLogSetting -ne 'Off') {
        $traceCount = if ($cth -and $cth.PluginTraceLogTotal -gt 0) { [int]$cth.PluginTraceLogTotal } else { 'N/A' }
        $recs.Add([PSCustomObject]@{
            Priority = 'High'; StorageType = 'Log'
            DataType = "Plugin Trace Logging ON ($($orgSet.PluginTraceLogSetting))"
            Count    = $traceCount
            Action   = 'Disable immediately: <strong>Settings &rarr; Administration &rarr; System Settings &rarr; Customization tab &rarr; Enable logging to plug-in trace log = Off</strong>. The built-in daily cleanup job removes records &gt;1 day old once disabled. To delete existing records now: <strong>Advanced Settings &rarr; Customizations &rarr; Plug-In Trace Log &rarr; Select All &rarr; Delete</strong>. Never leave enabled in production &mdash; fills Log storage rapidly.'
        })
    } elseif ($cth -and $cth.PluginTraceLogTotal -gt 5000) {
        $recs.Add([PSCustomObject]@{
            Priority = 'Medium'; StorageType = 'Log'
            DataType = 'Plugin Trace Logs Accumulating'
            Count    = [int]$cth.PluginTraceLogTotal
            Action   = 'Trace logging appears Off but records remain. The built-in daily cleanup job should clear these within 24h &mdash; verify it is not suspended: <strong>Settings &rarr; System &rarr; System Jobs</strong> (search for &ldquo;Plug-In Trace Log&rdquo;). To delete manually: <strong>Advanced Settings &rarr; Customizations &rarr; Plug-In Trace Log &rarr; Select All &rarr; Delete</strong>.'
        })
    }

    # Large attachment notes (>1 MB each) — primary file storage consumers
    if ($cth -and $cth.LargeAnnotations -gt 0) {
        $cnt = [int]$cth.LargeAnnotations
        $prio = if ($cnt -gt 500) { 'High' } elseif ($cnt -gt 100) { 'Medium' } else { $null }
        if ($prio) {
            $recs.Add([PSCustomObject]@{
                Priority = $prio; StorageType = 'File'
                DataType = 'Large Attachment Notes (&gt;1 MB)'
                Count    = $cnt
                Action   = 'Affects AnnotationBase and associated file storage. Review which record types generate large attachments (emails, document uploads). Create a recurring Bulk Delete job: <strong>Settings &rarr; Data Management &rarr; Bulk Record Deletion &rarr; New</strong> &rarr; Entity = <em>Notes</em>, filter File Size (Bytes) &gt; 1048576 AND Created On older than [your retention date]. Note: file deletion is permanent in the application.'
            })
        }
    }

    # Old completed email activities — EmailBase, ActivityPartyBase, ActivityPointerBase all grow
    if ($cth -and $cth.OldCompletedEmails -gt 0) {
        $cnt = [int]$cth.OldCompletedEmails
        $prio = if ($cnt -gt 10000) { 'High' } elseif ($cnt -gt 2000) { 'Medium' } else { $null }
        if ($prio) {
            $coverNote = if ($bulkDelSec -and -not $bulkDelSec.CoversEmail) { ' <strong>No email bulk delete job detected &mdash; create one now.</strong>' } else { '' }
            $recs.Add([PSCustomObject]@{
                Priority = $prio; StorageType = 'DB'
                DataType = 'Old Completed Email Activities (&gt;90d)'
                Count    = $cnt
                Action   = "Affects EmailBase, EmailHashBase, ActivityPartyBase, and ActivityPointerBase. Create a recurring Bulk Delete job: <strong>Settings &rarr; Data Management &rarr; Bulk Record Deletion &rarr; New</strong> &rarr; Entity = <em>Email Messages</em>, filter Status = Completed AND Actual End older than 90 days. Schedule outside business hours. Caution: this removes email history &mdash; confirm with business owners and exclude emails linked to open cases if required.$coverNote"
            })
        }
    }

    # Old import job history records
    if ($cth -and $cth.OldImportJobRecords -gt 50) {
        $recs.Add([PSCustomObject]@{
            Priority = 'Low'; StorageType = 'DB'
            DataType = 'Old Import Job History (&gt;90d)'
            Count    = [int]$cth.OldImportJobRecords
            Action   = 'ImportJobBase accumulates history from all data imports. Caution: deleting these records removes rollback capability for those imports. Create a Bulk Delete job: <strong>Settings &rarr; Data Management &rarr; Bulk Record Deletion &rarr; New</strong> &rarr; Entity = <em>System Jobs</em>, System Job Type = Import AND Created On older than 90 days.'
        })
    }

    # Old bulk delete operation history records (the cleanup jobs themselves)
    if ($cth -and $cth.OldBulkDeleteOpRecords -gt 100) {
        $recs.Add([PSCustomObject]@{
            Priority = 'Low'; StorageType = 'DB'
            DataType = 'Old Bulk Delete Operation History (&gt;90d)'
            Count    = [int]$cth.OldBulkDeleteOpRecords
            Action   = 'Create a self-cleaning Bulk Delete job: <strong>Settings &rarr; Data Management &rarr; Bulk Record Deletion &rarr; New</strong> &rarr; Entity = <em>System Jobs</em>, System Job Type = Bulk Delete AND Status = Succeeded AND Created On older than 90 days. Schedule weekly outside business hours.'
        })
    }

    # Stale BPF / process session instances (Active but >180 days old, likely abandoned)
    if ($staleProc -and $null -ne $staleProc.StaleActiveCount -and $staleProc.StaleActiveCount -gt 0) {
        $cnt = [int]$staleProc.StaleActiveCount
        $prio = if ($cnt -gt 5000) { 'High' } elseif ($cnt -gt 500) { 'Medium' } else { $null }
        if ($prio) {
            $recs.Add([PSCustomObject]@{
                Priority = $prio; StorageType = 'DB'
                DataType = 'Stale BPF / Process Session Instances (&gt;180d Active)'
                Count    = $cnt
                Action   = 'Active Business Process Flow instances older than 180 days are likely abandoned and inflate ProcessSessionBase. Review: <strong>Settings &rarr; Advanced Find &rarr; Look for = Process Sessions</strong>, filter Status = Active AND Created On older than 180 days. Deactivate or delete abandoned BPF instances. Consider a recurring Bulk Delete job: <strong>Settings &rarr; Data Management &rarr; Bulk Record Deletion &rarr; New</strong> &rarr; Entity = <em>Process Sessions</em>, Status = Inactive AND Modified On older than [retention date].'
            })
        }
    }

    # Audit retention set to Forever — AuditBase will grow without bound
    if ($orgSet -and $orgSet.AuditEnabled -and $orgSet.AuditRetentionDays -eq -1) {
        $auditCoverNote = if ($bulkDelSec -and -not $bulkDelSec.CoversAudit) { ' <strong>No audit bulk delete job detected.</strong>' } else { '' }
        $recs.Add([PSCustomObject]@{
            Priority = 'High'; StorageType = 'DB'
            DataType = 'Audit Retention = Forever (no auto-delete)'
            Count    = 'N/A'
            Action   = "AuditBase grows indefinitely without a retention limit. (1) Set a retention period: <strong>Settings &rarr; Auditing &rarr; Global Audit Settings &rarr; Retention Period</strong> (e.g., 365 days). (2) Delete existing old logs now: <strong>Power Platform Admin Center &rarr; Environments &rarr; [this environment] &rarr; Settings &rarr; Audit and logs &rarr; Delete logs</strong> &mdash; delete by table or delete all access logs up to a chosen date. Note: audit log deletion prevents historical activity review.$auditCoverNote"
        })
    }

    # Duplicate detection job history — each job run stores a copy of every detected duplicate pair in DuplicateRecordBase
    $dupDetect = $sections.DuplicateDetection
    $dupRecs   = $sections.DuplicateRecords
    if ($dupDetect -and $dupDetect.EnabledCount -gt 0 -and $dupRecs -and $dupRecs.UnresolvedCount -gt 100) {
        $cnt = [int]$dupRecs.UnresolvedCount
        $prio = if ($cnt -gt 5000) { 'Medium' } else { 'Low' }
        $recs.Add([PSCustomObject]@{
            Priority = $prio; StorageType = 'DB'
            DataType = "Duplicate Detection Job History ($cnt unresolved pairs)"
            Count    = $cnt
            Action   = 'Each duplicate detection job run stores a copy of every detected duplicate in DuplicateRecordBase. Delete old job instances to reclaim space: <strong>Settings &rarr; Data Management &rarr; Duplicate Detection Jobs</strong> &rarr; select completed job instances &rarr; Delete. To avoid re-accumulation, resolve duplicate records promptly so they are not reported in multiple subsequent job runs.'
        })
    }

    # Server-Side Sync alert trace logs — mailbox errors generate TraceLogBase entries rapidly
    $mailboxSec = $sections.MailboxHealth
    if ($mailboxSec -and $mailboxSec.InError -gt 0) {
        $prio = if ($mailboxSec.InError -gt 10) { 'Medium' } else { 'Low' }
        $recs.Add([PSCustomObject]@{
            Priority = $prio; StorageType = 'DB'
            DataType = "SSS Alert Trace Logs ($($mailboxSec.InError) mailbox(es) in error)"
            Count    = $mailboxSec.InError
            Action   = 'Server-Side Synchronization errors generate TraceLogBase (Alert) records that accumulate silently. Delete stale alerts: open a <strong>Mailbox</strong> or <strong>Email Server Profile</strong> record in Advanced Settings &rarr; Email Configuration, go to the <strong>Alerts</strong> tab &rarr; Select All &rarr; Delete. Alternatively use Advanced Find or a Bulk Delete job targeting <em>Trace Records</em>. Fix the underlying mailbox errors first to stop new alerts: <strong>Settings &rarr; Email Configuration &rarr; Mailboxes</strong>.'
        })
    }

    # ExchangeSyncIdMapping — item-level monitoring rows accumulate for every SSS-tracked item
    if ($mailboxSec -and $mailboxSec.TotalActive -gt 50) {
        $recs.Add([PSCustomObject]@{
            Priority = 'Low'; StorageType = 'DB'
            DataType = "ExchangeSyncIdMapping Accumulation ($($mailboxSec.TotalActive) active mailboxes)"
            Count    = $mailboxSec.TotalActive
            Action   = 'Server-Side Sync writes item-level monitoring rows to ExchangeSyncIdMappingBase for each tracked email, appointment, contact, and task. The default troubleshooting-record retention is 3 days. To reduce retention or disable item-level monitoring: <strong>Settings &rarr; Administration &rarr; System Settings &rarr; Email tab &rarr; Monitor email processing errors after = [reduce days] or select None</strong>. See: <em>Troubleshoot item level Server-Side Synchronization issues</em> on Microsoft Docs.'
        })
    }

    # ── F&O-specific cleanup recommendations ─────────────────────────────────
    if ($e.HasFO -and $e.FO -and $e.FO.Sections) {
        $foRecSec = $e.FO.Sections
        $criticalFOCategories = @('System', 'DIXF')

        # Batch jobs in error — may be blocking scheduled processing and accumulating history
        if ($foRecSec.BatchJobs -and $null -ne $foRecSec.BatchJobs.ErrorCount) {
            $foErrCnt = [int]$foRecSec.BatchJobs.ErrorCount
            if ($foErrCnt -gt 0) {
                $foPrio = if ($foErrCnt -gt 50) { 'High' } elseif ($foErrCnt -gt 10) { 'Medium' } else { 'Low' }
                $foTotalCnt = if ($null -ne $foRecSec.BatchJobs.TotalCount) { [int]$foRecSec.BatchJobs.TotalCount } else { '?' }
                $recs.Add([PSCustomObject]@{
                    Priority = $foPrio; StorageType = 'DB'
                    DataType = "F&amp;O: Batch Jobs in Error ($foErrCnt of $foTotalCnt)"
                    Count    = $foErrCnt
                    Action   = 'Review: <strong>System administration &rarr; Inquiries &rarr; Batch jobs</strong> (filter Status = Error). Fix root cause, then purge history: <strong>System administration &rarr; Periodic tasks &rarr; Batch job history clean-up</strong> &mdash; recommended 180-day retention, run daily outside business hours. Cleans BatchJobHistory, BatchHistory, and BatchConstraintHistory tables.'
                })
            }
        }

        if ($foRecSec.FOCleanupJobs) {
            $foCleanup = $foRecSec.FOCleanupJobs

            # Missing standard cleanup jobs — each is a specific batch job that should be scheduled
            foreach ($mj in @($foCleanup.MissingStandardJobs)) {
                if (-not $mj -or -not $mj.Purpose) { continue }
                $mjCat     = [string]$mj.Category
                $mjPurpose = [System.Web.HttpUtility]::HtmlEncode([string]$mj.Purpose)
                $mjMenu    = [System.Web.HttpUtility]::HtmlEncode([string]$mj.MenuPath)
                $mjNotes   = if ($mj.Notes) { ' <em>' + [System.Web.HttpUtility]::HtmlEncode([string]$mj.Notes) + '</em>' } else { '' }
                $mjPrio    = if ($criticalFOCategories -contains $mjCat) { 'High' } else { 'Medium' }
                $recs.Add([PSCustomObject]@{
                    Priority = $mjPrio; StorageType = 'DB'
                    DataType = "F&amp;O Missing Cleanup Job &ndash; $mjPurpose"
                    Count    = 'Not scheduled'
                    Action   = "Navigate to <strong>$mjMenu</strong> and schedule a recurring batch job.$mjNotes"
                })
            }

            # Found cleanup jobs that are disabled (Withheld/Canceled, no enabled instances)
            foreach ($dj in @($foCleanup.FoundStandardJobs)) {
                if (-not $dj -or $dj.IsEnabled -ne $false) { continue }
                $djCat     = [string]$dj.Category
                $djPurpose = [System.Web.HttpUtility]::HtmlEncode([string]$dj.Purpose)
                $djMenu    = [System.Web.HttpUtility]::HtmlEncode([string]$dj.MenuPath)
                $djNotes   = if ($dj.Notes) { ' <em>' + [System.Web.HttpUtility]::HtmlEncode([string]$dj.Notes) + '</em>' } else { '' }
                $djPrio    = if ($criticalFOCategories -contains $djCat) { 'High' } else { 'Medium' }
                $djIsError = ($null -ne $dj.ErrorCount -and [int]$dj.ErrorCount -gt 0)
                if ($djIsError) {
                    $djStatus = 'in Error'
                    $djAction = "Cleanup job is <strong>in error state</strong> &mdash; investigate then reset at: <strong>$djMenu</strong>.$djNotes"
                } else {
                    $djStatus = 'Disabled'
                    $djAction = "Cleanup job exists but is <strong>disabled (Withheld/Canceled)</strong> &mdash; re-enable and reschedule at: <strong>$djMenu</strong>.$djNotes"
                }
                $recs.Add([PSCustomObject]@{
                    Priority = $djPrio; StorageType = 'DB'
                    DataType = "F&amp;O Cleanup Job $djStatus &ndash; $djPurpose"
                    Count    = $djStatus
                    Action   = $djAction
                })
            }
        }
    }

    if ($recs.Count -eq 0) { continue }
    $cleanupRecCount += $recs.Count

    # Sort within environment: High first, then Medium, then Low
    $sortedRecs = $recs | Sort-Object { switch ($_.Priority) { 'High' { 0 } 'Medium' { 1 } default { 2 } } }

    foreach ($rec in $sortedRecs) {
        $prioClass = switch ($rec.Priority) {
            'High'   { 'bg-danger' }
            'Medium' { 'bg-warning text-dark' }
            default  { 'bg-secondary' }
        }
        $stClass = switch ($rec.StorageType) {
            'DB'       { 'text-primary' }
            'File'     { 'text-success' }
            'Log'      { 'text-warning' }
            'DB + Log' { 'text-danger' }
            default    { '' }
        }
        $countFmt = if ($rec.Count -is [int]) { '{0:N0}' -f $rec.Count } else { $rec.Count }
        $storageCleanupRows += @"
<tr>
  <td>$nameEsc<br><small class='text-muted'>$($e.Sku)</small></td>
  <td><span class='badge $prioClass'>$($rec.Priority)</span></td>
  <td>$($rec.DataType)</td>
  <td class='$stClass fw-bold small'>$($rec.StorageType)</td>
  <td>$countFmt</td>
  <td><small>$($rec.Action)</small></td>
</tr>
"@
    }
}

Write-Host "  Storage cleanup recommendations: $cleanupRecCount across $($envDetails.Count) environments" -ForegroundColor Cyan

# ── Tenant Governance / DLP section ───────────────────────────────────────────
$tenantGovHtml = if ($tenantGov) {
    $dlpSec  = $tenantGov.Sections.DlpPolicies
    $tsSec   = $tenantGov.Sections.TenantSettings
    $tiSec   = $tenantGov.Sections.TenantIsolation
    $erSec   = $tenantGov.Sections.EnvironmentRequests

    $dlpRowsHtml = ''
    if ($dlpSec -and $dlpSec.Policies) {
        foreach ($p in $dlpSec.Policies) {
            $scopeBadge = switch ($p.Scope) {
                'AllEnvironments' { '<span class="badge bg-primary">All</span>' }
                'AllExcept'       { "<span class=`"badge bg-info text-dark`">All Except ($($p.EnvironmentsExcluded.Count))</span>" }
                'Specific'        { "<span class=`"badge bg-secondary`">Specific ($($p.EnvironmentsIncluded.Count))</span>" }
                default           { '<span class="badge bg-warning text-dark">Unknown</span>' }
            }
            $httpBadge = if ($p.HttpAllowedInNonBusiness) {
                '<span class="badge bg-danger">HTTP in Non-Business</span>'
            } else { '<span class="badge bg-success">HTTP blocked</span>' }
            $dlpRowsHtml += "<tr><td>$([System.Web.HttpUtility]::HtmlEncode([string]$p.DisplayName))</td><td>$scopeBadge</td><td>$($p.BusinessCount)</td><td>$($p.NonBusinessCount)</td><td>$($p.BlockedCount)</td><td>$httpBadge</td></tr>"
        }
    }
    $dlpTable = if ($dlpRowsHtml) {
        "<table class='table table-sm table-bordered'><thead class='table-dark'><tr><th>Policy</th><th>Scope</th><th>Business</th><th>Non-Business</th><th>Blocked</th><th>HTTP</th></tr></thead><tbody>$dlpRowsHtml</tbody></table>"
    } else {
        "<div class='alert alert-warning'>No DLP policies configured in this tenant.</div>"
    }

    $isolBadge = if ($tiSec -and $tiSec.Enabled) { '<span class="badge bg-success">Enabled</span>' } else { '<span class="badge bg-danger">Disabled</span>' }
    $isolNote  = if ($tiSec) { "Allowed inbound tenants: $($tiSec.AllowedInbound.Count); outbound: $($tiSec.AllowedOutbound.Count)" } else { 'Not queried' }

    $envCreateBadge = if ($tsSec -and $tsSec.DisableEnvironmentCreationByNonAdmin) {
        '<span class="badge bg-success">Admin-only</span>'
    } elseif ($tsSec) {
        '<span class="badge bg-warning text-dark">Open to all users</span>'
    } else { '<span class="badge bg-secondary">Unknown</span>' }

    $pendingBadge = if ($erSec) {
        $pc = [int]$erSec.PendingCount
        if ($pc -gt 10) { "<span class='badge bg-warning text-dark'>$pc pending</span>" }
        elseif ($pc -gt 0) { "<span class='badge bg-info text-dark'>$pc pending</span>" }
        else { '<span class="badge bg-success">0 pending</span>' }
    } else { '<span class="badge bg-secondary">N/A</span>' }

@"
<section id="tenant-gov" class="mb-5">
  <div class="section-header"><h5 class="mb-0">Tenant Governance &amp; DLP</h5></div>
  <p class="text-muted small">Tenant-scoped Power Platform governance posture: DLP policies, tenant isolation, environment creation controls. Source: <code>data/tenant/*.json</code>.</p>
  <div class="row g-3 mb-3">
    <div class="col-md-3">
      <div class="card p-2"><div class="small text-muted">DLP Policies</div><div class="h4">$([int]$dlpSec.TotalCount)</div></div>
    </div>
    <div class="col-md-3">
      <div class="card p-2"><div class="small text-muted">Tenant Isolation</div><div class="h5 pt-1">$isolBadge</div></div>
    </div>
    <div class="col-md-3">
      <div class="card p-2"><div class="small text-muted">Env Creation</div><div class="h5 pt-1">$envCreateBadge</div></div>
    </div>
    <div class="col-md-3">
      <div class="card p-2"><div class="small text-muted">Pending Env Requests</div><div class="h5 pt-1">$pendingBadge</div></div>
    </div>
  </div>
  <h6 class="mt-3">DLP Policies</h6>
  $dlpTable
  <p class="small text-muted mt-2">Tenant isolation detail: $isolNote</p>
</section>
"@
} else {
    "<section id='tenant-gov' class='mb-5'><div class='section-header'><h5 class='mb-0'>Tenant Governance &amp; DLP</h5></div><div class='alert alert-secondary'>No tenant governance data available. Re-run with <code>-IncludeGovernance</code>.</div></section>"
}

# ── Maker Inventory section ───────────────────────────────────────────────────
$makerRowsHtml = ''
foreach ($e in ($envDetails | Where-Object { $_.Maker })) {
    $m = $e.Maker.Sections
    $nameEsc = [System.Web.HttpUtility]::HtmlEncode($e.DisplayName)
    $canvas  = [int]$m.CanvasApps.TotalCount
    $orphAps = [int]$m.CanvasApps.OrphanedCount
    $flows   = [int]$m.CloudFlows.TotalCount
    $susp    = [int]$m.CloudFlows.SuspendedCount
    $conns   = [int]$m.Connections.TotalCount
    $errConn = [int]$m.Connections.InErrorCount
    $custCn  = [int]$m.CustomConnectors.TotalCount
    $ppages  = [int]$m.PowerPages.TotalCount
    $bots    = [int]$m.Copilots.TotalCount
    $dfs     = [int]$m.Dataflows.TotalCount
    $ai      = [int]$m.AIModels.TotalCount
    $makerRowsHtml += @"
<tr>
  <td>$nameEsc<br><small class='text-muted'>$($e.Sku)</small></td>
  <td>$canvas$(if ($orphAps -gt 0) { " <span class='badge bg-danger'>$orphAps orphaned</span>" })</td>
  <td>$flows$(if ($susp -gt 0) { " <span class='badge bg-warning text-dark'>$susp susp</span>" })</td>
  <td>$conns$(if ($errConn -gt 0) { " <span class='badge bg-danger'>$errConn err</span>" })</td>
  <td>$custCn</td>
  <td>$ppages</td>
  <td>$bots</td>
  <td>$dfs</td>
  <td>$ai</td>
</tr>
"@
}

$makerHtml = if ($makerRowsHtml) {
@"
<section id="maker" class="mb-5">
  <div class="section-header"><h5 class="mb-0">Maker Inventory</h5></div>
  <p class="text-muted small">Per-environment count of maker-created assets: canvas apps, cloud flows, connections, custom connectors, Power Pages, Copilot Studio bots, dataflows, AI Builder models. Source: <code>maker-summary.json</code> per env.</p>
  <div class="table-responsive">
    <table id="makerTable" class="table table-sm table-hover table-bordered" style="width:100%">
      <thead class="table-dark"><tr>
        <th>Environment</th><th>Canvas Apps</th><th>Cloud Flows</th><th>Connections</th><th>Custom Connectors</th><th>Power Pages</th><th>Copilots</th><th>Dataflows</th><th>AI Models</th>
      </tr></thead>
      <tbody>$makerRowsHtml</tbody>
    </table>
  </div>
</section>
"@
} else {
    "<section id='maker' class='mb-5'><div class='section-header'><h5 class='mb-0'>Maker Inventory</h5></div><div class='alert alert-secondary'>No maker inventory data. Re-run with <code>-IncludeMakerInventory</code>.</div></section>"
}

# ── RBAC Summary section ──────────────────────────────────────────────────────
$rbacRowsHtml = ''
foreach ($e in ($envDetails | Where-Object { $_.RBAC })) {
    $r = $e.RBAC.Sections
    $nameEsc = [System.Web.HttpUtility]::HtmlEncode($e.DisplayName)
    $roles   = [int]$r.SecurityRoles.TotalCount
    $custom  = [int]$r.SecurityRoles.CustomCount
    $bus     = [int]$r.BusinessUnits.TotalCount
    $depth   = [int]$r.BusinessUnits.MaxDepth
    $teams   = [int]$r.Teams.TotalCount
    $fsp     = [int]$r.FieldSecurityProfiles.TotalCount
    $sysAdmin = [int]$r.UserRoleAssignments.UsersWithSystemAdmin
    $sampU   = [int]$r.UserRoleAssignments.SampledUserCount
    $adminBadge = if ($sysAdmin -gt 10 -and $e.Sku -eq 'Production') {
        "<span class='badge bg-danger'>$sysAdmin / $sampU</span>"
    } elseif ($sysAdmin -gt 0) {
        "<span class='badge bg-secondary'>$sysAdmin / $sampU</span>"
    } else { '<span class="text-muted small">0</span>' }
    $rbacRowsHtml += @"
<tr>
  <td>$nameEsc<br><small class='text-muted'>$($e.Sku)</small></td>
  <td>$roles <small class='text-muted'>($custom custom)</small></td>
  <td>$bus <small class='text-muted'>(depth $depth)</small></td>
  <td>$teams</td>
  <td>$fsp</td>
  <td>$adminBadge</td>
</tr>
"@
}

$rbacHtml = if ($rbacRowsHtml) {
@"
<section id="rbac" class="mb-5">
  <div class="section-header"><h5 class="mb-0">RBAC Summary</h5></div>
  <p class="text-muted small">Per-environment access control depth: security roles, business units, teams, field security profiles, and sampled system admin assignment density. Source: <code>rbac-summary.json</code>.</p>
  <div class="table-responsive">
    <table id="rbacTable" class="table table-sm table-hover table-bordered" style="width:100%">
      <thead class="table-dark"><tr>
        <th>Environment</th><th>Security Roles</th><th>Business Units</th><th>Teams</th><th>FSPs</th><th>Users w/ SysAdmin (sampled)</th>
      </tr></thead>
      <tbody>$rbacRowsHtml</tbody>
    </table>
  </div>
</section>
"@
} else {
    "<section id='rbac' class='mb-5'><div class='section-header'><h5 class='mb-0'>RBAC Summary</h5></div><div class='alert alert-secondary'>No RBAC data. Re-run with <code>-IncludeRBAC</code>.</div></section>"
}

# ── Metadata Depth section ────────────────────────────────────────────────────
$mdRowsHtml = ''
foreach ($e in ($envDetails | Where-Object { $_.MetadataDepth })) {
    $m = $e.MetadataDepth.Sections
    $nameEsc = [System.Web.HttpUtility]::HtmlEncode($e.DisplayName)
    $dvVer    = if ($m.Organization) { $m.Organization.Version } else { 'N/A' }
    $pubs     = [int]$m.Publishers.TotalCount
    $pubsCust = [int]$m.Publishers.CustomCount
    $d365     = [int]$m.D365Apps.TotalCount
    $curr     = [int]$m.Currencies.TotalCount
    $langs    = [int]$m.Languages.ProvisionedCount
    $backups  = [int]$m.Backups.TotalCount
    $lcOps    = [int]$m.LifecycleOperations.Recent30dCount
    $lcFail   = [int]$m.LifecycleOperations.Failed30dCount
    $lcBadge  = if ($lcFail -gt 0) { "<span class='badge bg-danger'>$lcFail failed</span>" } else { "$lcOps recent" }
    $mdRowsHtml += @"
<tr>
  <td>$nameEsc<br><small class='text-muted'>$($e.Sku)</small></td>
  <td><code>$dvVer</code></td>
  <td>$pubs <small class='text-muted'>($pubsCust custom)</small></td>
  <td>$d365</td>
  <td>$curr</td>
  <td>$langs</td>
  <td>$backups</td>
  <td>$lcBadge</td>
</tr>
"@
}

$mdHtml = if ($mdRowsHtml) {
@"
<section id="metadata-depth" class="mb-5">
  <div class="section-header"><h5 class="mb-0">Metadata &amp; Lifecycle</h5></div>
  <p class="text-muted small">Per-environment metadata inventory: Dataverse version, publishers, installed D365 apps, currencies, languages, backups, lifecycle operations. Source: <code>metadata-depth-summary.json</code>.</p>
  <div class="table-responsive">
    <table id="mdTable" class="table table-sm table-hover table-bordered" style="width:100%">
      <thead class="table-dark"><tr>
        <th>Environment</th><th>DV Version</th><th>Publishers</th><th>D365 Apps</th><th>Currencies</th><th>Languages</th><th>Backups</th><th>Lifecycle Ops (30d)</th>
      </tr></thead>
      <tbody>$mdRowsHtml</tbody>
    </table>
  </div>
</section>
"@
} else {
    "<section id='metadata-depth' class='mb-5'><div class='section-header'><h5 class='mb-0'>Metadata &amp; Lifecycle</h5></div><div class='alert alert-secondary'>No metadata depth data. Re-run with <code>-IncludeMetadataDepth</code>.</div></section>"
}

$generatedAt = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
$tenantId    = $master.TenantId
$authAs      = $master.AuthenticatedAs

# ── Priority Action Plan rows ─────────────────────────────────────────────────
$actionPlanRows = ''
$rank = 0
foreach ($act in $topActions) {
    $rank++
    $envForAct = $act.Env
    $nameEsc = [System.Web.HttpUtility]::HtmlEncode([string]$envForAct.DisplayName)
    $skuEsc  = [System.Web.HttpUtility]::HtmlEncode([string]$envForAct.Sku)
    $flagEsc = [System.Web.HttpUtility]::HtmlEncode([string]$act.Flag)

    $sevBadgeClass = switch ($act.Severity) {
        'Critical' { 'bg-danger' }
        'High'     { 'bg-danger' }
        'Medium'   { 'bg-warning text-dark' }
        'Low'      { 'bg-secondary' }
        default    { 'bg-secondary' }
    }
    $sevBadge = "<span class='badge $sevBadgeClass'>$($act.Severity)</span>"

    $skuBadgeClass = switch ($envForAct.Sku) {
        'Production' { 'bg-primary'    }
        'Default'    { 'bg-primary'    }
        'Sandbox'    { 'bg-info text-dark' }
        'Developer'  { 'bg-success'    }
        'Trial'      { 'bg-warning text-dark' }
        default      { 'bg-secondary'  }
    }
    $skuBadge = "<span class='badge $skuBadgeClass ms-1'>$skuEsc</span>"

    $fix = Get-FlagFix $act.FlagName
    $fixCell = ''
    if ($fix) {
        $whyRaw  = Expand-FixTokens -Text ([string]$fix.Why)       -Env $envForAct -TenantId $tenantId
        $fixRaw  = Expand-FixTokens -Text ([string]$fix.Fix)       -Env $envForAct -TenantId $tenantId
        $snipRaw = Expand-FixTokens -Text ([string]$fix.Snippet)   -Env $envForAct -TenantId $tenantId
        $linkRaw = Expand-FixTokens -Text ([string]$fix.DeepLink)  -Env $envForAct -TenantId $tenantId

        $whyEsc  = [System.Web.HttpUtility]::HtmlEncode($whyRaw)
        $fixEsc  = [System.Web.HttpUtility]::HtmlEncode($fixRaw)
        $snipEsc = [System.Web.HttpUtility]::HtmlEncode($snipRaw)
        $linkEsc = [System.Web.HttpUtility]::HtmlEncode($linkRaw)

        # Only render the Open button if the URL has no unresolved {TOKEN}
        $linkHtml = if ($linkRaw -and ($linkRaw -notmatch '\{[A-Z_]+\}')) {
            "<a href='$linkEsc' target='_blank' rel='noopener' class='btn btn-sm btn-outline-primary me-2'>Open in portal &rarr;</a>"
        } else { '' }

        $snippetHtml = if ($snipRaw) {
@"
<details class='d-inline-block mt-2'>
  <summary class='btn btn-sm btn-outline-secondary' style='list-style:none'>Show snippet</summary>
  <div class='mt-2'>
    <pre class='fix-snippet mb-1'>$snipEsc</pre>
    <button type='button' class='btn btn-sm btn-outline-primary' onclick='copyFixSnippet(this)'>Copy</button>
  </div>
</details>
"@
        } else { '' }

        $fixCell = @"
<div class='small'><strong>Why:</strong> $whyEsc</div>
<div class='small mt-1'><strong>Fix:</strong> $fixEsc</div>
<div class='mt-2'>$linkHtml$snippetHtml</div>
"@
    } else {
        $fixCell = "<div class='small text-muted'>No pre-baked fix snippet. Add one in <code>config/flag-fixes.json</code> keyed on <code>$([System.Web.HttpUtility]::HtmlEncode($act.FlagName))</code> if this flag recurs.</div>"
    }

    $actionPlanRows += @"
<tr>
  <td class='fw-bold'>$rank</td>
  <td><strong>$nameEsc</strong>$skuBadge<br><small class='text-muted'>$($envForAct.EnvironmentId)</small></td>
  <td>$sevBadge</td>
  <td><code class='small'>$flagEsc</code></td>
  <td>$fixCell</td>
</tr>
"@
}

$actionPlanHtml = if ($topActions.Count -gt 0) {
@"
<section id="action-plan" class="mb-5">
  <div class="section-header"><h5 class="mb-0">Priority Action Plan &mdash; Top $($topActions.Count)</h5></div>
  <p class="text-muted small">
    Highest-priority issues across the tenant, ranked by <strong>severity &times; SKU weight &times; blast radius</strong>.
    Severity is from <code>config/flag-severity.json</code>; SKU weight from <code>config/sku-profiles.json</code>
    (Production 3&times;, Default 2&times;, Sandbox 1.5&times;, Developer 0.5&times;, Trial 0.25&times;);
    blast radius uses the parenthetical count in the flag text (log<sub>10</sub> scale). SKU-suppressed flags are excluded.
    Fix snippets come from <code>config/flag-fixes.json</code>; extend that file to tailor them to your organization.
  </p>
  <div class="table-responsive">
    <table class="table table-sm table-hover table-bordered" style="width:100%">
      <thead class="table-dark">
        <tr>
          <th style="width:50px">#</th>
          <th style="width:18%">Environment</th>
          <th style="width:100px">Severity</th>
          <th style="width:22%">Issue</th>
          <th>Why &amp; Fix</th>
        </tr>
      </thead>
      <tbody>$actionPlanRows</tbody>
    </table>
  </div>
</section>
"@
} else {
@"
<section id="action-plan" class="mb-5">
  <div class="section-header"><h5 class="mb-0">Priority Action Plan</h5></div>
  <div class="alert alert-success">No scored flags across the tenant &mdash; nothing to action right now.</div>
</section>
"@
}

# ── Trends & Forecast rows + JSON arrays for Chart.js ───────────────────────
$forecastRows = ''
foreach ($e in ($envDetails | Sort-Object StorageTotal_MB -Descending)) {
    if (-not $envHistory.ContainsKey($e.EnvironmentId)) { continue }
    $hist = @($envHistory[$e.EnvironmentId] | Sort-Object RunAt)
    if ($hist.Count -lt 2) { continue }
    $fc = Invoke-LinearForecast -Points $hist -YField 'TotalMB'
    if (-not $fc) { continue }
    $mbPerDay = [double]$fc.Slope
    $r2       = [double]$fc.R2
    $currMB   = [double]$e.StorageTotal_MB
    $firstRunAt = $hist[0].RunAt
    $lastRunAt  = $hist[-1].RunAt
    $spanDays   = [Math]::Round(($lastRunAt - $firstRunAt).TotalDays, 1)

    $daysTo150 = if ($mbPerDay -gt 0) { [Math]::Round($currMB * 0.5 / $mbPerDay, 0) } else { $null }
    $daysTo200 = if ($mbPerDay -gt 0) { [Math]::Round($currMB / $mbPerDay, 0) } else { $null }

    $rateCell = if ($mbPerDay -gt 0.5) {
        "<span class='text-danger'>+$(Format-MB $mbPerDay)/day</span>"
    } elseif ($mbPerDay -lt -0.5) {
        "<span class='text-success'>$(Format-MB $mbPerDay)/day</span>"
    } else {
        "<span class='text-muted'>~flat</span>"
    }

    $days150Cell = if ($null -ne $daysTo150 -and $daysTo150 -gt 0 -and $daysTo150 -lt 3650) { "$daysTo150 d" } else { '&mdash;' }
    $days200Cell = if ($null -ne $daysTo200 -and $daysTo200 -gt 0 -and $daysTo200 -lt 3650) { "$daysTo200 d" } else { '&mdash;' }

    $r2Pct = [Math]::Round($r2 * 100, 0)
    $r2Cls = if ($r2 -gt 0.85) { 'text-success' } elseif ($r2 -gt 0.6) { 'text-warning' } else { 'text-muted' }

    $nameEsc = [System.Web.HttpUtility]::HtmlEncode([string]$e.DisplayName)
    $forecastRows += @"
<tr>
  <td><strong>$nameEsc</strong><br><small class='text-muted'>$($e.Sku) &middot; $($hist.Count) points &middot; $spanDays d span</small></td>
  <td>$(Format-MB $currMB)</td>
  <td>$rateCell</td>
  <td>$days150Cell</td>
  <td>$days200Cell</td>
  <td class='small $r2Cls'>$r2Pct%</td>
</tr>
"@
}

# Chart.js data arrays — single-line CSV numbers, safe to embed in JS literal arrays.
$trendScoreCsv = ($trendData | ForEach-Object { [Math]::Round([double]$_.Score, 1) }) -join ','
$trendDbCsv    = ($trendData | ForEach-Object { [double]$_.TotalDbGB })   -join ','
$trendFileCsv  = ($trendData | ForEach-Object { [double]$_.TotalFileGB }) -join ','
$trendLogCsv   = ($trendData | ForEach-Object { [double]$_.TotalLogGB })  -join ','
$trendTotalCsv = ($trendData | ForEach-Object { [double]$_.TotalGB })     -join ','
$trendCritCsv  = ($trendData | ForEach-Object { [int]$_.Critical })       -join ','
$trendHighCsv  = ($trendData | ForEach-Object { [int]$_.High })           -join ','
$trendMedCsv   = ($trendData | ForEach-Object { [int]$_.Medium })         -join ','
$trendLowCsv   = ($trendData | ForEach-Object { [int]$_.Low })            -join ','
$trendLabelsJs = ''
if ($trendLabels.Count -gt 0) {
    $trendLabelsJs = '"' + (@($trendLabels | ForEach-Object { $_.Replace('\','\\').Replace('"','\"') }) -join '","') + '"'
}

$trendsHtml = if ($trendData.Count -ge 2) {
@"
<section id="trends" class="mb-5">
  <div class="section-header"><h5 class="mb-0">Trends &amp; Forecast</h5></div>
  <p class="text-muted small">
    Tenant-wide trend across <strong>$($trendData.Count) runs</strong> in <code>data/run-history/</code>.
    Score and flag counts are recomputed per snapshot using the <em>current</em> <code>config/flag-severity.json</code>,
    so changes to severity config retroactively reshape the history.
    Per-environment storage forecast uses ordinary least-squares linear regression;
    R&sup2; reports how linear the growth actually is (higher = more confident projection).
  </p>

  <div class="row g-3 mb-3">
    <div class="col-lg-4">
      <div class="card p-2"><h6 class="small text-muted mb-1">Tenant Governance Score (0&ndash;100)</h6>
        <div class="chart-box"><canvas id="trendScoreChart"></canvas></div></div>
    </div>
    <div class="col-lg-4">
      <div class="card p-2"><h6 class="small text-muted mb-1">Flag Count by Severity</h6>
        <div class="chart-box"><canvas id="trendFlagChart"></canvas></div></div>
    </div>
    <div class="col-lg-4">
      <div class="card p-2"><h6 class="small text-muted mb-1">Total Storage (GB)</h6>
        <div class="chart-box"><canvas id="trendStorageChart"></canvas></div></div>
    </div>
  </div>

  <h6 class="mt-3">Storage Forecast &mdash; Per-Environment Linear Regression</h6>
  <p class="text-muted small">"Days to 1.5&times; / 2&times;" = days from now until each environment reaches 1.5&times; / 2&times; its current total storage, extrapolated from the growth rate. Flat/shrinking environments show em dashes.</p>
  $(if ($forecastRows) {
    @"
  <div class='table-responsive'>
    <table class='table table-sm table-bordered' style='max-width:900px'>
      <thead class='table-secondary'>
        <tr>
          <th>Environment</th>
          <th>Current Storage</th>
          <th>Growth Rate</th>
          <th>Days to 1.5&times;</th>
          <th>Days to 2&times;</th>
          <th>R&sup2;</th>
        </tr>
      </thead>
      <tbody>$forecastRows</tbody>
    </table>
  </div>
"@
  } else {
    "<p class='text-muted'>Need at least 2 run-history snapshots with matching environment IDs to build a forecast.</p>"
  })
</section>
"@
} else {
@"
<section id="trends" class="mb-5">
  <div class="section-header"><h5 class="mb-0">Trends &amp; Forecast</h5></div>
  <div class="alert alert-secondary">Need at least 2 runs in <code>data/run-history/</code> to compute trends. Currently found: <strong>$($trendData.Count)</strong>.</div>
</section>
"@
}

# ── HTML template ─────────────────────────────────────────────────────────────

$html = @"
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>PPAC Dataverse Inventory Report</title>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css">
  <link rel="stylesheet" href="https://cdn.datatables.net/1.13.7/css/dataTables.bootstrap5.min.css">
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
  <style>
    body { font-size: 0.875rem; }
    .nav-link { font-size: 0.85rem; }
    .section-header { background: #0078d4; color: white; padding: 0.5rem 1rem; border-radius: 4px; margin-bottom: 1rem; }
    .stat-card { border-left: 4px solid #0078d4; }
    .stat-card.danger { border-color: #dc3545; }
    .stat-card.warning { border-color: #ffc107; }
    .badge { font-size: 0.7em; }
    pre { font-size: 0.8em; background: #f8f9fa; padding: 8px; border-radius: 4px; max-height: 200px; overflow: auto; }
    .toc a { display: block; padding: 3px 0; font-size: 0.85rem; }
    .fix-snippet { font-size: 0.78em; background: #0b1020; color: #e6edf3; padding: 8px; border-radius: 4px; max-height: 300px; overflow: auto; white-space: pre-wrap; }
    .chart-box { position: relative; height: 220px; width: 100%; }
    details > summary { cursor: pointer; list-style: none; }
    details > summary::-webkit-details-marker { display: none; }
    @media print { .no-print { display: none; } }
  </style>
</head>
<body>
<nav class="navbar navbar-expand-lg navbar-dark bg-dark no-print">
  <div class="container-fluid">
    <a class="navbar-brand fw-bold" href="#">PPAC Inventory Report</a>
    <span class="navbar-text text-muted small">Generated: $generatedAt | Tenant: $tenantId</span>
  </div>
</nav>

<div class="container-fluid mt-3">
<div class="row">
<!-- TOC sidebar -->
<div class="col-lg-2 no-print">
  <div class="sticky-top pt-3">
    <div class="card">
      <div class="card-header bg-dark text-white small fw-bold">Contents</div>
      <div class="card-body toc p-2">
        <a href="#summary">Executive Summary</a>
        <a href="#issues">Issue Overview</a>
        <a href="#action-plan">Priority Action Plan</a>
        <a href="#delta">Changes (Delta)</a>
        <a href="#trends">Trends &amp; Forecast</a>
        <a href="#governance">Governance Scores</a>
        <a href="#tenant-gov">Tenant Governance / DLP</a>
        <a href="#maker">Maker Inventory</a>
        <a href="#rbac">RBAC Summary</a>
        <a href="#metadata-depth">Metadata &amp; Lifecycle</a>
        <a href="#all-envs">All Environments</a>
        <a href="#storage">Storage Analysis</a>
        <a href="#cleanup">Cleanup Gaps</a>
        <a href="#storage-cleanup">Cleanup Recommendations</a>
        <a href="#activity">Activity / Unused</a>
        <a href="#fo-section">Finance & Operations</a>
        <a href="#top-tables">Top Tables (Records)</a>
        <a href="#run-info">Collection Info</a>
      </div>
    </div>
  </div>
</div>

<!-- Main content -->
<div class="col-lg-10">

<!-- SUMMARY -->
<section id="summary" class="mb-5">
  <div class="section-header"><h5 class="mb-0">Executive Summary</h5></div>
  <div class="row g-3 mb-3">
    <div class="col-6 col-md-3">
      <div class="card stat-card text-center p-3">
        <div class="h2 fw-bold">$($envDetails.Count)</div>
        <div class="small text-muted">Total Environments</div>
      </div>
    </div>
    <div class="col-6 col-md-3">
      <div class="card stat-card text-center p-3">
        <div class="h2 fw-bold">$envsWithDV</div>
        <div class="small text-muted">With Dataverse</div>
      </div>
    </div>
    <div class="col-6 col-md-3">
      <div class="card stat-card text-center p-3">
        <div class="h2 fw-bold">$envsWithFO</div>
        <div class="small text-muted">With Finance & Ops</div>
      </div>
    </div>
    <div class="col-6 col-md-3">
      <div class="card stat-card danger text-center p-3">
        <div class="h2 fw-bold text-danger">$envsWithFlags</div>
        <div class="small text-muted">Environments with Issues</div>
      </div>
    </div>
  </div>
  <div class="row g-3 mb-3">
    <div class="col-6 col-md-3">
      <div class="card stat-card text-center p-3" style="border-left-color: $(if ($tenantScore -ge 70) {'#198754'} elseif ($tenantScore -ge 40) {'#ffc107'} else {'#dc3545'})">
        <div class="h2 fw-bold" style="color: $(if ($tenantScore -ge 70) {'#198754'} elseif ($tenantScore -ge 40) {'#856404'} else {'#dc3545'})">$tenantScore<small style="font-size:0.5em">/100</small></div>
        <div class="small text-muted">Tenant Governance Score</div>
      </div>
    </div>
    <div class="col-6 col-md-3">
      <div class="card stat-card danger text-center p-3">
        <div class="h2 fw-bold text-danger">$criticalEnvs</div>
        <div class="small text-muted">Critical Envs (Score &lt; 50)</div>
      </div>
    </div>
    <div class="col-6 col-md-3">
      <div class="card stat-card text-center p-3" style="border-left-color: #198754">
        <div class="h2 fw-bold text-success">$healthyEnvs</div>
        <div class="small text-muted">Healthy Envs (Score &ge; 80)</div>
      </div>
    </div>
    <div class="col-6 col-md-3">
      <div class="card stat-card text-center p-3">
        <div class="h3 fw-bold">$($envDetails.Count - $healthyEnvs - $criticalEnvs)</div>
        <div class="small text-muted">Needs Attention (50-79)</div>
      </div>
    </div>
  </div>
  <div class="row g-3">
    <div class="col-6 col-md-3">
      <div class="card stat-card text-center p-3">
        <div class="h3 fw-bold">$(Format-MB $totalDbMB)</div>
        <div class="small text-muted">Total Database Storage</div>
      </div>
    </div>
    <div class="col-6 col-md-3">
      <div class="card stat-card text-center p-3">
        <div class="h3 fw-bold">$(Format-MB $totalFileMB)</div>
        <div class="small text-muted">Total File Storage</div>
      </div>
    </div>
    <div class="col-6 col-md-3">
      <div class="card stat-card text-center p-3">
        <div class="h3 fw-bold">$(Format-MB $totalLogMB)</div>
        <div class="small text-muted">Total Log Storage</div>
      </div>
    </div>
    <div class="col-6 col-md-3">
      <div class="card stat-card warning text-center p-3">
        <div class="h3 fw-bold">$(Format-MB $totalAllMB)</div>
        <div class="small text-muted">Grand Total Storage</div>
      </div>
    </div>
  </div>
</section>

<!-- ISSUE OVERVIEW -->
<section id="issues" class="mb-5">
  <div class="section-header"><h5 class="mb-0">Issue Overview</h5></div>
  <p class="text-muted small">Count of environments in each issue category. See tables below for details.</p>
  <div class="row">$issueCardsHtml</div>
</section>

$actionPlanHtml

$deltaHtml

$trendsHtml

<!-- GOVERNANCE SCORES -->
<section id="governance" class="mb-5">
  <div class="section-header"><h5 class="mb-0">Governance Scores</h5></div>
  <p class="text-muted small">Per-environment governance score (0-100) based on flag severity. Scores are adjusted for SKU type using <code>config/sku-profiles.json</code>. Severity levels are defined in <code>config/flag-severity.json</code>.</p>
  <table id="govTable" class="table table-sm table-hover table-bordered" style="width:100%">
    <thead class="table-dark">
      <tr>
        <th>Environment</th>
        <th>Score</th>
        <th>Critical</th>
        <th>High</th>
        <th>Medium</th>
        <th>Low</th>
        <th>Owner</th>
      </tr>
    </thead>
    <tbody>$govTableRows</tbody>
  </table>
</section>

$tenantGovHtml

$makerHtml

$rbacHtml

$mdHtml

<!-- ALL ENVIRONMENTS -->
<section id="all-envs" class="mb-5">
  <div class="section-header"><h5 class="mb-0">All Environments</h5></div>
  <div class="table-responsive">
    <table id="envTable" class="table table-sm table-hover table-bordered" style="width:100%">
      <thead class="table-dark">
        <tr>
          <th>Environment</th>
          <th>SKU</th>
          <th>State</th>
          <th>Region</th>
          <th>Storage</th>
          <th>Active Users</th>
          <th>Score</th>
          <th>Owner</th>
          <th>Last Audit</th>
          <th>Issues</th>
        </tr>
      </thead>
      <tbody>
        $envTableRows
      </tbody>
    </table>
  </div>
</section>

<!-- STORAGE ANALYSIS -->
<section id="storage" class="mb-5">
  <div class="section-header"><h5 class="mb-0">Storage Analysis - Top Consumers</h5></div>
  <div class="row">
    <div class="col-lg-4 mb-4">
      <h6 class="text-primary">Database Storage (Top 25)</h6>
      <table class="table table-sm table-bordered">
        <thead class="table-secondary"><tr><th>#</th><th>Environment</th><th>DB Size</th><th>%</th></tr></thead>
        <tbody>$topDBRows</tbody>
      </table>
    </div>
    <div class="col-lg-4 mb-4">
      <h6 class="text-success">File Storage (Top 25)</h6>
      <table class="table table-sm table-bordered">
        <thead class="table-secondary"><tr><th>#</th><th>Environment</th><th>File Size</th><th>%</th></tr></thead>
        <tbody>$topFileRows</tbody>
      </table>
    </div>
    <div class="col-lg-4 mb-4">
      <h6 class="text-warning">Log Storage (Top 25)</h6>
      <table class="table table-sm table-bordered">
        <thead class="table-secondary"><tr><th>#</th><th>Environment</th><th>Log Size</th><th>%</th></tr></thead>
        <tbody>$topLogRows</tbody>
      </table>
    </div>
  </div>
</section>

<!-- CLEANUP GAPS -->
<section id="cleanup" class="mb-5">
  <div class="section-header"><h5 class="mb-0">Cleanup Gaps</h5></div>
  <p class="text-muted small">Environments that are missing automated cleanup jobs or have evidence of cleanup backlogs.</p>
  <table id="cleanupTable" class="table table-sm table-hover table-bordered" style="width:100%">
    <thead class="table-dark">
      <tr>
        <th>Environment</th>
        <th>SKU</th>
        <th>Scheduled Bulk Delete Jobs</th>
        <th>Old Completed Async Jobs (&gt;90d)</th>
        <th>Suspended Jobs</th>
        <th>Failed Jobs (30d)</th>
        <th>No Retention Policy</th>
        <th>Issues</th>
      </tr>
    </thead>
    <tbody>
$(
    $cleanupEnvs = @($envDetails | Where-Object {
        $_.AllFlags -contains 'NO_SCHEDULED_BULK_DELETE' -or
        $_.AllFlags -contains 'OLD_COMPLETED_JOBS_NOT_CLEANED' -or
        $_.AllFlags -contains 'HIGH_SUSPENDED_JOBS' -or
        $_.AllFlags -contains 'HIGH_FAILED_JOBS_30D' -or
        $_.AllFlags -contains 'NO_RETENTION_POLICIES'
    })
    $cleanupRows = ''
    foreach ($e in ($cleanupEnvs | Sort-Object StorageTotal_MB -Descending)) {
        $nameEsc   = [System.Web.HttpUtility]::HtmlEncode($e.DisplayName)
        $bulkDel   = Get-SectionValue $e.CE 'BulkDeleteJobs' 'ScheduledCount'
        $asyncData = $e.CE.Sections.AsyncOperations.Counts
        $oldComp   = if ($asyncData) { $asyncData.CompletedOlderThan90d } else { 'N/A' }
        $susp      = if ($asyncData) { $asyncData.Suspended             } else { 'N/A' }
        $fail30    = if ($asyncData) { $asyncData.Failed_Last30d        } else { 'N/A' }
        $noRetent  = if ($e.AllFlags -contains 'NO_RETENTION_POLICIES') { 'Yes' } else { 'No' }
        $flags     = Get-FlagBadgeHtml -Flags $e.AllFlags
        $cleanupRows += "<tr><td>$nameEsc</td><td>$($e.Sku)</td><td>$bulkDel</td><td>$oldComp</td><td>$susp</td><td>$fail30</td><td>$noRetent</td><td>$flags</td></tr>"
    }
    $cleanupRows
)
    </tbody>
  </table>
</section>

<!-- STORAGE CLEANUP RECOMMENDATIONS -->
<section id="storage-cleanup" class="mb-5">
  <div class="section-header"><h5 class="mb-0">Storage Cleanup Recommendations ($cleanupRecCount items)</h5></div>
  <p class="text-muted small">
    Ranked, actionable cleanup tasks derived from collected data. All actions use Dataverse
    <strong>Bulk Delete Jobs</strong> (Settings &rarr; Data Management &rarr; Bulk Record Deletion)
    unless stated otherwise. This tool is <strong>read-only</strong> — these are recommendations only;
    nothing has been deleted.
  </p>
  $(if ($cleanupRecCount -gt 0) {
    @"
  <div class="table-responsive">
  <table id="cleanupRecTable" class="table table-sm table-hover table-bordered" style="width:100%">
    <thead class="table-dark">
      <tr>
        <th>Environment</th>
        <th>Priority</th>
        <th>What to Clean</th>
        <th>Storage Type</th>
        <th>Record Count</th>
        <th>Recommended Action / Filter</th>
      </tr>
    </thead>
    <tbody>$storageCleanupRows</tbody>
  </table>
  </div>
"@
  } else {
    "<p class='text-muted'>No cleanup recommendations generated. Run with <code>-IncludeEntityCounts</code> for additional storage analysis, or verify environments have Dataverse enabled.</p>"
  })
</section>

<!-- ACTIVITY / UNUSED -->
<section id="activity" class="mb-5">
  <div class="section-header"><h5 class="mb-0">Activity Analysis - Potentially Unused Environments</h5></div>
  <p class="text-muted small">Environments with no active users, no recent audit log entries, or no process sessions.</p>
  <table id="activityTable" class="table table-sm table-hover table-bordered" style="width:100%">
    <thead class="table-dark">
      <tr>
        <th>Environment</th>
        <th>SKU</th>
        <th>State</th>
        <th>Active Users</th>
        <th>Last Audit Entry</th>
        <th>Audit 90d Count</th>
        <th>Flow Sessions (30d)</th>
        <th>Storage</th>
        <th>Issues</th>
      </tr>
    </thead>
    <tbody>
$(
    $inactiveEnvs = @($envDetails | Where-Object {
        $_.AllFlags -contains 'NO_ACTIVE_USERS' -or
        $_.AllFlags -contains 'NO_AUDIT_ACTIVITY_90D' -or
        $_.AllFlags -contains 'AUDIT_DISABLED_OR_NO_ACTIVITY'
    })
    $actRows = ''
    foreach ($e in ($inactiveEnvs | Sort-Object StorageTotal_MB -Descending)) {
        $nameEsc   = [System.Web.HttpUtility]::HtmlEncode($e.DisplayName)
        $users     = Get-SectionValue $e.CE 'Users' 'ActiveCount'
        $lastAudit = Get-SectionValue $e.CE 'AuditLog' 'LastEntry'
        $auditLast = if ($lastAudit -ne 'N/A' -and $lastAudit) {
                        try { [datetime]$lastAudit | Get-Date -Format 'yyyy-MM-dd' } catch { $lastAudit }
                     } else { 'N/A' }
        $audit90   = Get-SectionValue $e.CE 'AuditLog' 'Recent90dCount'
        $sessions  = Get-SectionValue $e.CE 'ProcessSessions' 'Last30dCount'
        $storageFmt = Format-MB $e.StorageTotal_MB
        $flags     = Get-FlagBadgeHtml -Flags $e.AllFlags
        $actRows += "<tr><td>$nameEsc</td><td>$($e.Sku)</td><td>$($e.State)</td><td>$users</td><td>$auditLast</td><td>$audit90</td><td>$sessions</td><td>$storageFmt</td><td>$flags</td></tr>"
    }
    $actRows
)
    </tbody>
  </table>
</section>

<!-- F&O SECTION -->
<section id="fo-section" class="mb-5">
  <div class="section-header"><h5 class="mb-0">Finance &amp; Operations Environments ($envsWithFO detected)</h5></div>
  $(if ($envsWithFO -eq 0) {
      "<p class='text-muted'>No Finance &amp; Operations integration was detected in any environment.</p>"
  } else {
    @"
  <p class='text-muted small'>Environments where Finance &amp; Operations integration was detected via the Dataverse RetrieveFinanceAndOperationsIntegrationDetails action.</p>
  <table id='foTable' class='table table-sm table-hover table-bordered' style='width:100%'>
    <thead class='table-dark'>
      <tr>
        <th>Environment</th>
        <th>FO Batch Jobs</th>
        <th>Jobs in Error</th>
        <th title='Standard cleanup jobs where no batch job description matches the expected pattern'>Missing Cleanup Jobs</th>
        <th title='Cleanup jobs that exist but are Withheld/Canceled with no active schedule'>Cleanup Disabled</th>
        <th title='Cleanup jobs whose only matching batch job is in Error state'>Cleanup in Error</th>
        <th>FO Users</th>
        <th>Active (90d)</th>
        <th>DualWrite Maps</th>
        <th>DW Errors</th>
        <th>FO Issues</th>
      </tr>
    </thead>
    <tbody>$foTableRows</tbody>
  </table>
"@
  })
</section>

<!-- TOP TABLES BY RECORD COUNT (storage concentration proxy) -->
<section id="top-tables" class="mb-5">
  <div class="section-header"><h5 class="mb-0">Top 25 Tables by Record Count (per environment)</h5></div>
  <p class='text-muted small'>
    Microsoft doesn't expose per-table storage bytes via any public Dataverse or Finance &amp; Operations API &mdash;
    only tenant/environment totals (BAP admin API) and a UI-only drill-down in PPAC.
    Record count from the documented <code>RetrieveTotalRecordCount</code> function is the closest signal we can collect,
    and it's a reasonable proxy for storage concentration: the tables with the most rows almost always dominate database storage for an environment.
    Populated only when the inventory is run with <code>-IncludeEntityCounts</code>.
  </p>
  $topTablesEnvCards
</section>

<!-- COLLECTION INFO -->
<section id="run-info" class="mb-5">
  <div class="section-header"><h5 class="mb-0">Collection Run Information</h5></div>
  <table class="table table-sm table-bordered" style="max-width:600px">
    <tr><th>Run At</th><td>$($master.RunAt)</td></tr>
    <tr><th>Tenant ID</th><td>$tenantId</td></tr>
    <tr><th>Authenticated As</th><td>$authAs</td></tr>
    <tr><th>Total Environments Found</th><td>$($master.TotalEnvironments)</td></tr>
    <tr><th>Successfully Processed</th><td>$($master.Processed)</td></tr>
    <tr><th>Skipped (already collected)</th><td>$($master.Skipped)</td></tr>
    <tr><th>Collection Errors</th><td>$($master.Errors)</td></tr>
    <tr><th>FO Collection Included</th><td>$($master.IncludedFO)</td></tr>
    <tr><th>Entity Counts Included</th><td>$($master.IncludedEntityCounts)</td></tr>
    <tr><th>Maker Inventory Included</th><td>$($master.IncludedMakerInventory)</td></tr>
    <tr><th>Governance (DLP) Included</th><td>$($master.IncludedGovernance)</td></tr>
    <tr><th>RBAC Depth Included</th><td>$($master.IncludedRBAC)</td></tr>
    <tr><th>Metadata Depth Included</th><td>$($master.IncludedMetadataDepth)</td></tr>
    <tr><th>Activity Telemetry Included</th><td>$($master.IncludedActivity)</td></tr>
    <tr><th>Data Path</th><td>$DataPath</td></tr>
  </table>

  <h6 class='mt-4'>Top Issues Across Tenant</h6>
  <table class='table table-sm table-bordered' style='max-width:600px'>
    <thead class='table-secondary'><tr><th>Count</th><th>Flag</th></tr></thead>
    <tbody>
$(
    $flagRows = ''
    foreach ($f in $master.AllFlagsDistinct | Select-Object -First 25) {
        $fEsc = [System.Web.HttpUtility]::HtmlEncode($f.Name)
        $flagRows += "<tr><td><strong>$($f.Count)</strong></td><td>$fEsc</td></tr>"
    }
    $flagRows
)
    </tbody>
  </table>
</section>

</div><!-- /col -->
</div><!-- /row -->
</div><!-- /container -->

<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/js/bootstrap.bundle.min.js"></script>
<script src="https://code.jquery.com/jquery-3.7.0.min.js"></script>
<script src="https://cdn.datatables.net/1.13.7/js/jquery.dataTables.min.js"></script>
<script src="https://cdn.datatables.net/1.13.7/js/dataTables.bootstrap5.min.js"></script>
<script>
function copyFixSnippet(btn) {
  try {
    var pre = btn.parentNode.querySelector('pre.fix-snippet');
    if (!pre) return;
    var txt = pre.textContent || pre.innerText || '';
    navigator.clipboard.writeText(txt).then(function() {
      var orig = btn.textContent;
      btn.textContent = 'Copied!';
      btn.classList.add('btn-success');
      btn.classList.remove('btn-outline-primary');
      setTimeout(function() {
        btn.textContent = orig;
        btn.classList.remove('btn-success');
        btn.classList.add('btn-outline-primary');
      }, 1500);
    }).catch(function() {
      btn.textContent = 'Copy failed';
    });
  } catch(e) { btn.textContent = 'Copy failed'; }
}

`$(document).ready(function() {
  `$('#envTable').DataTable({
    pageLength: 25, order: [[4,'desc']],
    columnDefs: [{ targets: 4, type: 'num' }]
  });
  `$('#cleanupTable').DataTable({ pageLength: 25 });
  `$('#cleanupRecTable').DataTable({ pageLength: 25, order: [[1,'asc'],[4,'desc']] });
  `$('#activityTable').DataTable({ pageLength: 25, order: [[7,'desc']] });
  `$('#govTable').DataTable({ pageLength: 25, order: [[1,'asc']], columnDefs: [{ targets: 1, type: 'num' }] });
  `$('#foTable').DataTable({ pageLength: 25 });
  if (`$('#makerTable').length) { `$('#makerTable').DataTable({ pageLength: 25 }); }
  if (`$('#rbacTable').length)  { `$('#rbacTable').DataTable({ pageLength: 25 }); }
  if (`$('#mdTable').length)    { `$('#mdTable').DataTable({ pageLength: 25 }); }

  // ── Trends charts ─────────────────────────────────────────────────────
  if (typeof Chart !== 'undefined' && document.getElementById('trendScoreChart')) {
    var trendLabels = [$trendLabelsJs];
    var commonOpts = {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { labels: { boxWidth: 10, font: { size: 10 } } } },
      scales: { x: { ticks: { font: { size: 9 }, maxRotation: 45, minRotation: 45 } },
                y: { ticks: { font: { size: 10 } } } }
    };
    new Chart(document.getElementById('trendScoreChart'), {
      type: 'line',
      data: { labels: trendLabels,
        datasets: [{ label: 'Score', data: [$trendScoreCsv],
          borderColor: '#0078d4', backgroundColor: 'rgba(0,120,212,0.15)',
          tension: 0.2, fill: true, pointRadius: 3 }] },
      options: Object.assign({}, commonOpts, { scales: Object.assign({}, commonOpts.scales, { y: { min: 0, max: 100, ticks: { font: { size: 10 } } } }) })
    });
    new Chart(document.getElementById('trendFlagChart'), {
      type: 'line',
      data: { labels: trendLabels, datasets: [
        { label: 'Critical', data: [$trendCritCsv], borderColor: '#dc3545', backgroundColor: 'rgba(220,53,69,0.15)', tension: 0.2, pointRadius: 2 },
        { label: 'High',     data: [$trendHighCsv], borderColor: '#fd7e14', backgroundColor: 'rgba(253,126,20,0.15)', tension: 0.2, pointRadius: 2 },
        { label: 'Medium',   data: [$trendMedCsv],  borderColor: '#ffc107', backgroundColor: 'rgba(255,193,7,0.15)',  tension: 0.2, pointRadius: 2 },
        { label: 'Low',      data: [$trendLowCsv],  borderColor: '#6c757d', backgroundColor: 'rgba(108,117,125,0.15)', tension: 0.2, pointRadius: 2 }
      ]},
      options: commonOpts
    });
    new Chart(document.getElementById('trendStorageChart'), {
      type: 'line',
      data: { labels: trendLabels, datasets: [
        { label: 'Database', data: [$trendDbCsv],   borderColor: '#0d6efd', tension: 0.2, pointRadius: 2 },
        { label: 'File',     data: [$trendFileCsv], borderColor: '#198754', tension: 0.2, pointRadius: 2 },
        { label: 'Log',      data: [$trendLogCsv],  borderColor: '#6c757d', tension: 0.2, pointRadius: 2 },
        { label: 'Total',    data: [$trendTotalCsv],borderColor: '#dc3545', tension: 0.2, pointRadius: 2, borderDash: [4,3] }
      ]},
      options: commonOpts
    });
  }
});
</script>
</body>
</html>
"@

# ── Write report ──────────────────────────────────────────────────────────────
$reportDir = Split-Path -Parent $ReportPath
if (-not (Test-Path $reportDir)) { $null = New-Item -ItemType Directory -Path $reportDir -Force }

[System.IO.File]::WriteAllText($ReportPath, $html, [System.Text.Encoding]::UTF8)

Write-Host ''
Write-Host "Report generated: $ReportPath" -ForegroundColor Green
Write-Host "  - $($envDetails.Count) environments"                  -ForegroundColor Cyan
Write-Host "  - $envsWithFlags environments with flags"              -ForegroundColor $(if ($envsWithFlags -gt 0) {'Yellow'} else {'Green'})
Write-Host "  - $(Format-MB $totalAllMB) total storage across tenant" -ForegroundColor Cyan
Write-Host "  - Tenant governance score: $tenantScore / 100"         -ForegroundColor $(if ($tenantScore -ge 70) {'Green'} elseif ($tenantScore -ge 40) {'Yellow'} else {'Red'})

if ($OpenReport) {
    Write-Host "Opening report in browser..." -ForegroundColor Cyan
    Start-Process $ReportPath
}
