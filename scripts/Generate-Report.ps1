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

# ── Load per-environment CE summaries ─────────────────────────────────────────
$envDetails = [System.Collections.Generic.List[PSObject]]::new()

foreach ($envEntry in $master.Environments) {
    $outDir = $envEntry.OutputDir
    if (-not $outDir -or -not (Test-Path $outDir)) { continue }

    $ceSummaryFile = Join-Path $outDir 'ce-summary.json'
    $foSummaryFile = Join-Path $outDir 'fo-summary.json'
    $metaFile      = Join-Path $outDir 'metadata.json'

    $ceSummary = if (Test-Path $ceSummaryFile) { Get-Content $ceSummaryFile -Raw | ConvertFrom-Json } else { $null }
    $foSummary = if (Test-Path $foSummaryFile) { Get-Content $foSummaryFile -Raw | ConvertFrom-Json } else { $null }

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
        CE              = $ceSummary
        FO              = $foSummary
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
        $cls = switch -Wildcard ($f) {
            'NO_*'           { 'bg-warning text-dark' }
            'HIGH_*'         { 'bg-danger'            }
            'LARGE_*'        { 'bg-danger'             }
            'OLD_*'          { 'bg-warning text-dark' }
            'MANY_*'         { 'bg-warning text-dark' }
            'FO_*ERROR*'     { 'bg-danger'            }
            'FO_*FAILED*'    { 'bg-danger'            }
            'FO_MISSING_*'   { 'bg-warning text-dark' }
            '*DISABLED*'     { 'bg-secondary'          }
            '*FAILED*'       { 'bg-danger'             }
            default          { 'bg-info text-dark'    }
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
    'DualWrite Map Errors'     = @($envDetails | Where-Object { $_.AllFlags | Where-Object { $_ -match 'DUALWRITE_MAPS_IN_ERROR' } })
}

# ── Summary stats ─────────────────────────────────────────────────────────────
$totalDbMB    = ($envDetails | Measure-Object -Property StorageDB_MB    -Sum).Sum
$totalFileMB  = ($envDetails | Measure-Object -Property StorageFile_MB  -Sum).Sum
$totalLogMB   = ($envDetails | Measure-Object -Property StorageLog_MB   -Sum).Sum
$totalAllMB   = ($envDetails | Measure-Object -Property StorageTotal_MB -Sum).Sum
$envsWithFO   = @($envDetails | Where-Object { $_.HasFO }).Count
$envsWithDV   = @($envDetails | Where-Object { $_.HasDataverse }).Count
$envsWithFlags= @($envDetails | Where-Object { $_.AllFlags.Count -gt 0 }).Count

# ── Build HTML rows for environments table ────────────────────────────────────
Add-Type -AssemblyName System.Web

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
        $null = $sb.Append(@"
<tr class='$skuClass'>
  <td><strong>$nameEsc</strong><br><small class='text-muted'>$($e.EnvironmentId)</small><br>$defTag $foTag $errTag</td>
  <td>$($e.Sku)</td>
  <td>$stateVal</td>
  <td>$locationVal</td>
  <td data-sort='$($e.StorageTotal_MB)'><strong>$totalFmt</strong><br><small>DB: $dbFmt<br>File: $fileFmt<br>Log: $logFmt</small></td>
  <td>$users</td>
  <td>$bulkDel</td>
  <td>$failNum</td>
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
        $bar = [Math]::Min(100, [Math]::Round($val / ([Math]::Max(1,($Envs | Measure-Object $SortField -Max).Maximum)) * 100, 0))
        $barColor = if ($bar -gt 80) {'bg-danger'} elseif ($bar -gt 50) {'bg-warning'} else {'bg-info'}
        $null = $sb.Append(@"
<tr>
  <td>$rank</td>
  <td>$nameEsc<br><small class='text-muted'>$($e.Sku) | $($e.Location)</small></td>
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

# ── FO environment detail rows ────────────────────────────────────────────────
$foEnvs = @($envDetails | Where-Object { $_.HasFO })
$foTableRows = ''
foreach ($e in $foEnvs) {
    $nameEsc     = [System.Web.HttpUtility]::HtmlEncode($e.DisplayName)
    $foSec       = if ($e.FO) { $e.FO.Sections } else { $null }
    $bjCount     = if ($foSec -and $foSec.BatchJobs) { $foSec.BatchJobs.TotalCount  } else { 'N/A' }
    $bjError     = if ($foSec -and $foSec.BatchJobs) { $foSec.BatchJobs.ErrorCount  } else { 'N/A' }
    $missingClnp = if ($foSec -and $foSec.FOCleanupJobs) { $foSec.FOCleanupJobs.MissingStandardJobs.Count } else { 'N/A' }
    $foUsers     = if ($foSec -and $foSec.FOUsers)  { $foSec.FOUsers.EnabledCount    } else { 'N/A' }
    $foActive90  = if ($foSec -and $foSec.FOUsers)  { $foSec.FOUsers.ActiveLast90d   } else { 'N/A' }
    $dwMaps      = if ($foSec -and $foSec.DualWrite) { $foSec.DualWrite.MapCount     } else { 'N/A' }
    $dwErrors    = if ($foSec -and $foSec.DualWrite) { $foSec.DualWrite.ErrorMapCount} else { 'N/A' }
    $foFlags     = @($e.AllFlags | Where-Object { $_ -match '^FO_|^DUALWRITE' })
    $flagsHtml   = Get-FlagBadgeHtml -Flags $foFlags

    $foTableRows += @"
<tr>
  <td>$nameEsc<br><small class='text-muted'>$($e.Sku)</small></td>
  <td>$bjCount</td>
  <td><span class='$(if ($bjError -gt 0) {"text-danger fw-bold"} else {""})'>$bjError</span></td>
  <td>$missingClnp</td>
  <td>$foUsers</td>
  <td>$foActive90</td>
  <td>$dwMaps</td>
  <td><span class='$(if ($dwErrors -gt 0) {"text-danger fw-bold"} else {""})'>$dwErrors</span></td>
  <td>$flagsHtml</td>
</tr>
"@
}

# ── HTML template ─────────────────────────────────────────────────────────────
$generatedAt = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
$tenantId    = $master.TenantId
$authAs      = $master.AuthenticatedAs

$html = @"
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>PPAC Dataverse Inventory Report</title>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css">
  <link rel="stylesheet" href="https://cdn.datatables.net/1.13.7/css/dataTables.bootstrap5.min.css">
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
        <a href="#all-envs">All Environments</a>
        <a href="#storage">Storage Analysis</a>
        <a href="#cleanup">Cleanup Gaps</a>
        <a href="#activity">Activity / Unused</a>
        <a href="#fo-section">Finance & Operations</a>
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
          <th>Bulk Del Jobs</th>
          <th>Failed Jobs (30d)</th>
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
      "<p class='text-muted'>No Finance &amp; Operations solutions were detected in any environment.</p>"
  } else {
    @"
  <p class='text-muted small'>Environments where FO solutions (Dual-Write, Dynamics 365 Finance, Supply Chain, etc.) were detected.</p>
  <table id='foTable' class='table table-sm table-hover table-bordered' style='width:100%'>
    <thead class='table-dark'>
      <tr>
        <th>Environment</th>
        <th>FO Batch Jobs</th>
        <th>Jobs in Error</th>
        <th>Missing Cleanup Jobs</th>
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
`$(document).ready(function() {
  `$('#envTable').DataTable({
    pageLength: 25, order: [[4,'desc']],
    columnDefs: [{ targets: 4, type: 'num' }]
  });
  `$('#cleanupTable').DataTable({ pageLength: 25 });
  `$('#activityTable').DataTable({ pageLength: 25, order: [[7,'desc']] });
  `$('#foTable').DataTable({ pageLength: 25 });
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

if ($OpenReport) {
    Write-Host "Opening report in browser..." -ForegroundColor Cyan
    Start-Process $ReportPath
}
