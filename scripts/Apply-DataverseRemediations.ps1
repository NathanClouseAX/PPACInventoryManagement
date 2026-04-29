#Requires -Version 5.1
<#
.SYNOPSIS
    Applies recommended cleanup remediations to Dataverse environments by reading
    recommendations.json (produced by Generate-Report.ps1) and creating Bulk Delete
    jobs and/or org-setting changes via the Dataverse Web API.

.DESCRIPTION
    Companion to Invoke-DataverseInventory.ps1 + Generate-Report.ps1.

    Generate-Report.ps1 emits data/recommendations.json with structured
    Recommendation entries that include a RemediationKind (e.g.
    "BulkDelete:AsyncOps_Completed_90d") and RemediationParams (e.g.
    OlderThanDays=90). This script reads that file and dispatches each
    recommendation to a handler that knows how to translate it into a
    Dataverse Web API call.

    Behavior:
      * Default mode is dry-run: prints what *would* be done.
      * -Apply must be passed explicitly to actually create jobs / change settings.
      * -EnvironmentFilter is REQUIRED (regex on DisplayName). There is no
        "apply to all" switch by design.
      * -IncludeKind is REQUIRED. Pass an explicit allowlist of kinds (or the
        catalog token "AllBulkDelete" to apply every BulkDelete:* kind).
      * Setting:* kinds are gated behind -IncludeSettingChanges separately --
        flipping settings is a different blast radius than scheduling deletes.
      * Each apply attempt is logged to data/remediations/<timestamp>.json
        with the full request body and response, so you can reconstruct what
        happened (and reverse it: see "Rolling back" below).
      * Created jobs use stable names prefixed with "PPAC:" so they are easy
        to filter/audit/delete in the admin UI.

    Authentication:
      Uses the same Az PowerShell context as Invoke-DataverseInventory.ps1.
      Run Connect-AzAccount first if you don't already have a context.

.PARAMETER DataPath
    Root data directory containing recommendations.json. Default: ..\data
    relative to this script.

.PARAMETER EnvironmentFilter
    REGEX on DisplayName. Required -- there is intentionally no "apply to every
    environment in the file" mode. Example: "^(my-sandbox|my-dev)$"

.PARAMETER IncludeKind
    REQUIRED list of RemediationKind values to apply. Example:
        -IncludeKind 'BulkDelete:AsyncOps_Completed_90d','BulkDelete:LargeAnnotations_365d'
    Special token: 'AllBulkDelete' expands to every BulkDelete:* kind.

.PARAMETER IncludeSettingChanges
    Allow Setting:* kinds (e.g. flipping plugintracelogsetting). Off by default
    because settings flips have a different blast radius than scheduling
    Bulk Delete jobs.

.PARAMETER Apply
    Actually call Dataverse. Without -Apply the script runs in dry-run mode
    and only prints the plan.

.PARAMETER IAcknowledgeProduction
    Required additional flag when any matched environment has Sku=Production.
    Acts as a manual brake against accidentally targeting prod.

.PARAMETER AuditPath
    Where to write the per-run audit JSON. Default: <DataPath>\remediations\<UTC timestamp>.json

.EXAMPLE
    # Dry-run plan for two sandboxes, BulkDelete kinds only
    .\Apply-DataverseRemediations.ps1 `
        -EnvironmentFilter '^(my-sandbox|my-dev)$' `
        -IncludeKind AllBulkDelete

.EXAMPLE
    # Actually apply, including a setting change
    .\Apply-DataverseRemediations.ps1 `
        -EnvironmentFilter '^my-sandbox$' `
        -IncludeKind AllBulkDelete,'Setting:DisablePluginTraceLog' `
        -IncludeSettingChanges -Apply

.EXAMPLE
    # Production target - additional brake required
    .\Apply-DataverseRemediations.ps1 `
        -EnvironmentFilter '^my-prod$' `
        -IncludeKind 'BulkDelete:AsyncOps_Completed_90d' `
        -Apply -IAcknowledgeProduction

.NOTES
    Rolling back: every Bulk Delete job created by this script is a row in the
    bulkdeleteoperation table. To cancel a recurring schedule:
      DELETE /api/data/v9.2/bulkdeleteoperations(<bulkdeleteoperationid>)
    The audit JSON records the JobId returned by the BulkDelete action;
    that is the asyncoperationid, NOT the bulkdeleteoperationid. Look up the
    parent series via:
      GET /api/data/v9.2/bulkdeleteoperations?$filter=name eq '<JobName>'
#>

[CmdletBinding()]
param(
    [string]   $DataPath              = '',
    [Parameter(Mandatory)][string]   $EnvironmentFilter,
    [Parameter(Mandatory)][string[]] $IncludeKind,
    [switch]   $IncludeSettingChanges,
    [switch]   $Apply,
    [switch]   $IAcknowledgeProduction,
    [string]   $AuditPath             = ''
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
if (-not $DataPath) {
    $DataPath = Join-Path (Split-Path -Parent $ScriptDir) 'data'
}
$DataPath = (Resolve-Path $DataPath).Path

Import-Module (Join-Path $ScriptDir 'modules\PPACInventory.psm1') -Force

# ── Catalog ───────────────────────────────────────────────────────────────────
# Each kind maps to a handler descriptor:
#   Type           - 'BulkDelete' | 'Setting'
#   DisplayName    - one-line label for the plan output
#   JobName        - stable, identical for the same kind across runs (PPAC: prefix)
#   Build          - ScriptBlock(Params) returning a hashtable describing the
#                    Dataverse query (BulkDelete) or setting patch (Setting)
#
# JobName is the idempotency key for BulkDelete: probing
# bulkdeleteoperations?$filter=name eq '<JobName>' tells us if we've already
# created the recurring schedule.

$Catalog = [ordered]@{

    'BulkDelete:AsyncOps_Completed_90d' = @{
        Type        = 'BulkDelete'
        DisplayName = 'Async Ops Completed >90d (System Jobs)'
        JobName     = 'PPAC: AsyncOps Completed >90d'
        Build       = {
            param($Params)
            @{
                EntityName = 'asyncoperation'
                Conditions = @(
                    @{ AttributeName = 'statecode';  Operator = 'Equal';          Type = 'System.Int32'; Value = 3 }
                    @{ AttributeName = 'createdon';  Operator = 'OlderThanXDays'; Type = 'System.Int32'; Value = [int]$Params.OlderThanDays }
                )
                Recurrence = 'FREQ=WEEKLY;INTERVAL=1;BYDAY=SA'
            }
        }
    }

    'BulkDelete:AsyncOps_Failed_30d' = @{
        Type        = 'BulkDelete'
        DisplayName = 'Async Ops Failed in last 30d'
        JobName     = 'PPAC: AsyncOps Failed >30d'
        Build       = {
            param($Params)
            @{
                EntityName = 'asyncoperation'
                Conditions = @(
                    @{ AttributeName = 'statuscode'; Operator = 'Equal';          Type = 'System.Int32'; Value = 31 }
                    @{ AttributeName = 'createdon';  Operator = 'OlderThanXDays'; Type = 'System.Int32'; Value = [int]$Params.OlderThanDays }
                )
                Recurrence = 'FREQ=WEEKLY;INTERVAL=1;BYDAY=SA'
            }
        }
    }

    'BulkDelete:AsyncOps_Suspended_30d' = @{
        Type        = 'BulkDelete'
        DisplayName = 'Async Ops Suspended >30d'
        JobName     = 'PPAC: AsyncOps Suspended >30d'
        Build       = {
            param($Params)
            @{
                EntityName = 'asyncoperation'
                Conditions = @(
                    @{ AttributeName = 'statecode'; Operator = 'Equal';          Type = 'System.Int32'; Value = 1 }
                    @{ AttributeName = 'createdon'; Operator = 'OlderThanXDays'; Type = 'System.Int32'; Value = [int]$Params.OlderThanDays }
                )
                Recurrence = 'FREQ=WEEKLY;INTERVAL=1;BYDAY=SA'
            }
        }
    }

    'BulkDelete:AsyncOps_Succeeded_30d' = @{
        Type        = 'BulkDelete'
        DisplayName = 'Async Ops Succeeded >30d (cleans WorkflowLogs)'
        JobName     = 'PPAC: AsyncOps Succeeded >30d'
        Build       = {
            param($Params)
            @{
                EntityName = 'asyncoperation'
                Conditions = @(
                    @{ AttributeName = 'statecode';  Operator = 'Equal';          Type = 'System.Int32'; Value = 3 }
                    @{ AttributeName = 'statuscode'; Operator = 'Equal';          Type = 'System.Int32'; Value = 30 }
                    @{ AttributeName = 'createdon';  Operator = 'OlderThanXDays'; Type = 'System.Int32'; Value = [int]$Params.OlderThanDays }
                )
                Recurrence = 'FREQ=WEEKLY;INTERVAL=1;BYDAY=SA'
            }
        }
    }

    'BulkDelete:LargeAnnotations_365d' = @{
        Type        = 'BulkDelete'
        DisplayName = 'Annotations >1 MB older than 365d'
        JobName     = 'PPAC: Large Annotations >365d'
        Build       = {
            param($Params)
            @{
                EntityName = 'annotation'
                Conditions = @(
                    @{ AttributeName = 'filesize';  Operator = 'GreaterThan';    Type = 'System.Int32'; Value = [int]$Params.MinSizeBytes }
                    @{ AttributeName = 'createdon'; Operator = 'OlderThanXDays'; Type = 'System.Int32'; Value = [int]$Params.OlderThanDays }
                )
                # Monthly first Saturday -- file deletion is permanent so cadence is conservative
                Recurrence = 'FREQ=MONTHLY;INTERVAL=1;BYDAY=SA;BYSETPOS=1'
            }
        }
    }

    'BulkDelete:OldCompletedEmails_90d' = @{
        Type        = 'BulkDelete'
        DisplayName = 'Completed Emails >90d'
        JobName     = 'PPAC: Completed Emails >90d'
        Build       = {
            param($Params)
            @{
                EntityName = 'email'
                Conditions = @(
                    @{ AttributeName = 'statecode'; Operator = 'Equal';          Type = 'System.Int32'; Value = 1 }
                    @{ AttributeName = 'actualend'; Operator = 'OlderThanXDays'; Type = 'System.Int32'; Value = [int]$Params.OlderThanDays }
                )
                Recurrence = 'FREQ=WEEKLY;INTERVAL=1;BYDAY=SA'
            }
        }
    }

    'BulkDelete:OldImportJobs_90d' = @{
        Type        = 'BulkDelete'
        DisplayName = 'Import System Jobs >90d'
        JobName     = 'PPAC: Import Jobs >90d'
        Build       = {
            param($Params)
            @{
                EntityName = 'asyncoperation'
                Conditions = @(
                    @{ AttributeName = 'operationtype'; Operator = 'Equal';          Type = 'System.Int32'; Value = 1 }
                    @{ AttributeName = 'createdon';     Operator = 'OlderThanXDays'; Type = 'System.Int32'; Value = [int]$Params.OlderThanDays }
                )
                Recurrence = 'FREQ=MONTHLY;INTERVAL=1;BYDAY=SA;BYSETPOS=1'
            }
        }
    }

    'BulkDelete:OldBulkDeleteOps_90d' = @{
        Type        = 'BulkDelete'
        DisplayName = 'Bulk Delete Op History >90d (self-cleaning)'
        JobName     = 'PPAC: BulkDelete Ops >90d'
        Build       = {
            param($Params)
            @{
                EntityName = 'asyncoperation'
                Conditions = @(
                    @{ AttributeName = 'operationtype'; Operator = 'Equal';          Type = 'System.Int32'; Value = 13 }
                    @{ AttributeName = 'statuscode';    Operator = 'Equal';          Type = 'System.Int32'; Value = 30 }
                    @{ AttributeName = 'createdon';     Operator = 'OlderThanXDays'; Type = 'System.Int32'; Value = [int]$Params.OlderThanDays }
                )
                Recurrence = 'FREQ=WEEKLY;INTERVAL=1;BYDAY=SA'
            }
        }
    }

    'Setting:DisablePluginTraceLog' = @{
        Type        = 'Setting'
        DisplayName = 'Org Setting: plugintracelogsetting = Off (0)'
        Build       = {
            param($Params)
            @{
                Field = 'plugintracelogsetting'
                Value = 0
            }
        }
    }
}

# ── Helpers ───────────────────────────────────────────────────────────────────

function Resolve-IncludedKinds {
    param([string[]]$Requested, [hashtable]$Catalog)
    $expanded = [System.Collections.Generic.List[string]]::new()
    foreach ($k in $Requested) {
        if ($k -eq 'AllBulkDelete') {
            foreach ($name in $Catalog.Keys) {
                if ($Catalog[$name].Type -eq 'BulkDelete') { $expanded.Add($name) }
            }
            continue
        }
        if (-not $Catalog.Contains($k)) {
            throw "Unknown RemediationKind: '$k'. Valid kinds: $($Catalog.Keys -join ', '), AllBulkDelete"
        }
        $expanded.Add($k)
    }
    return ,@($expanded | Select-Object -Unique)
}

function Get-NextStartDateTimeUtc {
    # Next Saturday at 02:00 UTC. If today is Saturday and it's already past 02:00, jump to next week.
    param([int]$HourUtc = 2, [string]$TargetDow = 'Saturday')
    $now    = [datetime]::UtcNow
    $target = [System.DayOfWeek]::$TargetDow
    $delta  = (([int]$target - [int]$now.DayOfWeek) + 7) % 7
    if ($delta -eq 0 -and $now.Hour -ge $HourUtc) { $delta = 7 }
    return [datetime]::SpecifyKind(
        $now.Date.AddDays($delta).AddHours($HourUtc),
        [System.DateTimeKind]::Utc
    )
}

function Build-BulkDeleteBody {
    <#
    .SYNOPSIS
        Builds the JSON-serializable hashtable for a BulkDelete action POST.
    #>
    param(
        [Parameter(Mandatory)][string]   $JobName,
        [Parameter(Mandatory)][string]   $EntityName,
        [Parameter(Mandatory)][object[]] $Conditions,
        [Parameter(Mandatory)][string]   $Recurrence,
        [Parameter(Mandatory)][datetime] $StartDateTimeUtc
    )

    $oDataConditions = foreach ($c in $Conditions) {
        @{
            AttributeName = $c.AttributeName
            Operator      = $c.Operator
            Values        = @(@{
                Value = $c.Value
                Type  = $c.Type
            })
        }
    }

    return @{
        JobName  = $JobName
        QuerySet = @(@{
            '@odata.type' = 'Microsoft.Dynamics.CRM.QueryExpression'
            EntityName    = $EntityName
            ColumnSet     = @{
                AllColumns = $false
                Columns    = @()
            }
            Distinct      = $false
            Criteria      = @{
                FilterOperator = 'And'
                Conditions     = @($oDataConditions)
                Filters        = @()
            }
        })
        StartDateTime          = $StartDateTimeUtc.ToString('yyyy-MM-ddTHH:mm:ssZ')
        RecurrencePattern      = $Recurrence
        SendEmailNotification  = $false
        ToRecipients           = @()
        CCRecipients           = @()
    }
}

function Test-BulkDeleteJobExists {
    <#
    .SYNOPSIS
        Returns the existing bulkdeleteoperation row(s) with the given name, if any.
        Used as the idempotency probe -- if any rows exist, the apply is skipped.
    #>
    param(
        [Parameter(Mandatory)][string] $InstanceApiUrl,
        [Parameter(Mandatory)][string] $JobName
    )

    # Single-quote-escape inside the OData filter literal
    $escaped = $JobName.Replace("'", "''")
    $path    = "bulkdeleteoperations?`$select=bulkdeleteoperationid,name,statecode,statuscode,recurrencepattern&`$filter=name eq '$escaped'"
    $resp    = Invoke-DataverseRequest -InstanceApiUrl $InstanceApiUrl -ODataPath $path
    if ($resp -and $resp.value) { return @($resp.value) }
    return @()
}

function Get-OrganizationId {
    param([Parameter(Mandatory)][string] $InstanceApiUrl)
    $resp = Invoke-DataverseRequest -InstanceApiUrl $InstanceApiUrl `
        -ODataPath 'organizations?$select=organizationid'
    if (-not $resp -or -not $resp.value -or $resp.value.Count -eq 0) {
        throw "Could not resolve organizationid for $InstanceApiUrl"
    }
    return [string]$resp.value[0].organizationid
}

function Format-PlanLine {
    param([string]$EnvName, [string]$Status, [string]$Kind, [string]$Detail)
    $color = switch ($Status) {
        'WOULD-APPLY'    { 'Yellow' }
        'APPLIED'        { 'Green' }
        'SKIPPED-EXISTS' { 'DarkGray' }
        'SKIPPED-GATE'   { 'DarkGray' }
        'FAILED'         { 'Red' }
        default          { 'Gray' }
    }
    Write-Host ("  [{0,-15}] {1,-32} {2}  {3}" -f $Status, $EnvName, $Kind, $Detail) -ForegroundColor $color
}

# ── Load + filter ─────────────────────────────────────────────────────────────

$recsFile = Join-Path $DataPath 'recommendations.json'
if (-not (Test-Path $recsFile)) {
    throw "recommendations.json not found at $recsFile. Run Generate-Report.ps1 first."
}

$recsRoot = Get-Content $recsFile -Raw | ConvertFrom-Json
Write-Host "Loaded recommendations from: $recsFile (generated $($recsRoot.GeneratedAt))" -ForegroundColor Cyan

$kinds = Resolve-IncludedKinds -Requested $IncludeKind -Catalog $Catalog
Write-Host "Applying kinds (after expansion): $($kinds -join ', ')" -ForegroundColor Cyan

# Split into Setting:* vs BulkDelete:*; gate the former unless -IncludeSettingChanges
$settingKinds   = @($kinds | Where-Object { $Catalog[$_].Type -eq 'Setting' })
$bulkDeleteKinds = @($kinds | Where-Object { $Catalog[$_].Type -eq 'BulkDelete' })

if ($settingKinds.Count -gt 0 -and -not $IncludeSettingChanges) {
    Write-Warning "Setting:* kinds requested but -IncludeSettingChanges was not passed -- they will be reported but not applied:"
    foreach ($k in $settingKinds) { Write-Warning "  $k" }
}

# Filter envs
$matchedEnvs = @($recsRoot.Environments | Where-Object { $_.DisplayName -match $EnvironmentFilter })
if ($matchedEnvs.Count -eq 0) {
    Write-Warning "No environments matched filter: $EnvironmentFilter"
    return
}
Write-Host "Matched environments: $($matchedEnvs.Count)" -ForegroundColor Cyan
foreach ($e in $matchedEnvs) {
    Write-Host ("  - {0} ({1})  api={2}" -f $e.DisplayName, $e.Sku, $e.OrgApiUrl) -ForegroundColor DarkCyan
}

# Production guardrail
$prodEnvs = @($matchedEnvs | Where-Object { $_.Sku -eq 'Production' })
if ($prodEnvs.Count -gt 0 -and -not $IAcknowledgeProduction) {
    throw ("Refusing to proceed: {0} matched environment(s) are Production. " +
           "Pass -IAcknowledgeProduction to confirm: {1}") -f $prodEnvs.Count, ($prodEnvs.DisplayName -join ', ')
}

# ── Plan / apply loop ─────────────────────────────────────────────────────────

$mode    = if ($Apply) { 'APPLY' } else { 'DRY-RUN' }
$startUtc = Get-NextStartDateTimeUtc -HourUtc 2 -TargetDow 'Saturday'
Write-Host ""
Write-Host "Mode: $mode    BulkDelete schedules will start at: $($startUtc.ToString('o'))" -ForegroundColor Cyan
Write-Host ""

$auditEntries = [System.Collections.Generic.List[object]]::new()
$counts = @{ WouldApply = 0; Applied = 0; SkippedExists = 0; SkippedGate = 0; Failed = 0 }

foreach ($env in $matchedEnvs) {
    $envName = [string]$env.DisplayName
    if (-not $env.OrgApiUrl) {
        Format-PlanLine $envName 'FAILED' '-' 'No OrgApiUrl in recommendations.json -- env was probably not Dataverse-enabled at collection time'
        continue
    }

    foreach ($rec in $env.Recommendations) {
        $kind = [string]$rec.RemediationKind
        if (-not $kind) { continue }            # advisory-only rec, no remediation
        if ($kinds -notcontains $kind) { continue }

        $entry  = $Catalog[$kind]
        $params = $rec.RemediationParams

        $auditRow = [ordered]@{
            EnvironmentId = $env.EnvironmentId
            DisplayName   = $envName
            OrgApiUrl     = $env.OrgApiUrl
            Kind          = $kind
            Type          = $entry.Type
            DataType      = $rec.DataType
            Count         = $rec.Count
            Params        = $params
            Status        = $null
            Detail        = $null
            Request       = $null
            Response      = $null
            Error         = $null
            AttemptedAtUtc = (Get-Date).ToUniversalTime().ToString('o')
        }

        try {
            switch ($entry.Type) {
                'BulkDelete' {
                    $spec = & $entry.Build $params
                    $body = Build-BulkDeleteBody `
                        -JobName          $entry.JobName `
                        -EntityName       $spec.EntityName `
                        -Conditions       $spec.Conditions `
                        -Recurrence       $spec.Recurrence `
                        -StartDateTimeUtc $startUtc
                    $auditRow.Request = $body

                    if (-not $Apply) {
                        # Don't hit the Dataverse API in dry-run mode -- the plan output
                        # shows what would be POSTed. Idempotency is checked at -Apply time
                        # so dry-run still works without an active Az context.
                        $auditRow.Status = 'WouldApply'
                        $auditRow.Detail = "Would POST /BulkDelete with JobName='$($entry.JobName)', entity=$($spec.EntityName), recurrence='$($spec.Recurrence)'"
                        $counts.WouldApply++
                        Format-PlanLine $envName 'WOULD-APPLY' $kind $entry.DisplayName
                        break
                    }

                    $existing = Test-BulkDeleteJobExists -InstanceApiUrl $env.OrgApiUrl -JobName $entry.JobName
                    if ($existing.Count -gt 0) {
                        $auditRow.Status = 'SkippedExists'
                        $auditRow.Detail = "Found $($existing.Count) existing job(s) with name '$($entry.JobName)'"
                        $auditRow.Response = $existing
                        $counts.SkippedExists++
                        Format-PlanLine $envName 'SKIPPED-EXISTS' $kind ("'{0}' already present ({1} row(s))" -f $entry.JobName, $existing.Count)
                        break
                    }

                    $resp = Invoke-DataverseRequest `
                        -InstanceApiUrl $env.OrgApiUrl `
                        -ODataPath      'BulkDelete' `
                        -Method         'POST' `
                        -Body           $body
                    $auditRow.Status   = 'Applied'
                    $auditRow.Detail   = "Created BulkDelete job '$($entry.JobName)' (asyncoperationid=$($resp.JobId))"
                    $auditRow.Response = $resp
                    $counts.Applied++
                    Format-PlanLine $envName 'APPLIED' $kind ("created '{0}' (JobId={1})" -f $entry.JobName, $resp.JobId)
                }

                'Setting' {
                    if (-not $IncludeSettingChanges) {
                        $auditRow.Status = 'SkippedGate'
                        $auditRow.Detail = '-IncludeSettingChanges was not passed'
                        $counts.SkippedGate++
                        Format-PlanLine $envName 'SKIPPED-GATE' $kind 'Setting:* gated; pass -IncludeSettingChanges'
                        break
                    }

                    $spec = & $entry.Build $params
                    $patch = @{ $spec.Field = $spec.Value }
                    $auditRow.Request = $patch

                    if (-not $Apply) {
                        $auditRow.Status = 'WouldApply'
                        $auditRow.Detail = "Would PATCH organizations() set $($spec.Field)=$($spec.Value)"
                        $counts.WouldApply++
                        Format-PlanLine $envName 'WOULD-APPLY' $kind ("PATCH organizations.{0}={1}" -f $spec.Field, $spec.Value)
                        break
                    }

                    $orgId = Get-OrganizationId -InstanceApiUrl $env.OrgApiUrl
                    $resp  = Invoke-DataverseRequest `
                        -InstanceApiUrl $env.OrgApiUrl `
                        -ODataPath      "organizations($orgId)" `
                        -Method         'PATCH' `
                        -Body           $patch
                    $auditRow.Status   = 'Applied'
                    $auditRow.Detail   = "PATCHed organizations($orgId) $($spec.Field)=$($spec.Value)"
                    $auditRow.Response = $resp
                    $counts.Applied++
                    Format-PlanLine $envName 'APPLIED' $kind ("PATCH organizations.{0}={1}" -f $spec.Field, $spec.Value)
                }

                default {
                    throw "Catalog entry for '$kind' has unknown Type '$($entry.Type)'"
                }
            }
        }
        catch {
            $auditRow.Status = 'Failed'
            $auditRow.Error  = "$($_.Exception.GetType().Name): $($_.Exception.Message)"
            $counts.Failed++
            Format-PlanLine $envName 'FAILED' $kind $_.Exception.Message
        }

        $auditEntries.Add([PSCustomObject]$auditRow)
    }
}

# ── Audit + summary ───────────────────────────────────────────────────────────

if (-not $AuditPath) {
    $auditDir  = Join-Path $DataPath 'remediations'
    if (-not (Test-Path $auditDir)) { New-Item -ItemType Directory -Path $auditDir -Force | Out-Null }
    $stamp     = (Get-Date).ToUniversalTime().ToString('yyyyMMdd_HHmmss')
    $AuditPath = Join-Path $auditDir "$mode-$stamp.json"
}

$audit = [ordered]@{
    Mode             = $mode
    GeneratedAt      = (Get-Date).ToUniversalTime().ToString('o')
    DataPath         = $DataPath
    EnvironmentFilter = $EnvironmentFilter
    IncludeKind      = $IncludeKind
    IncludeSettingChanges = [bool]$IncludeSettingChanges
    StartDateTimeUtc = $startUtc.ToString('o')
    Counts           = $counts
    Entries          = @($auditEntries)
}
$audit | ConvertTo-Json -Depth 12 | Set-Content -Path $AuditPath -Encoding UTF8

Write-Host ""
Write-Host "Summary:" -ForegroundColor Cyan
Write-Host ("  Mode:           {0}" -f $mode)
Write-Host ("  Would-apply:    {0}" -f $counts.WouldApply)
Write-Host ("  Applied:        {0}" -f $counts.Applied)
Write-Host ("  Skipped-exists: {0}" -f $counts.SkippedExists)
Write-Host ("  Skipped-gate:   {0}" -f $counts.SkippedGate)
Write-Host ("  Failed:         {0}" -f $counts.Failed)
Write-Host ("  Audit:          {0}" -f $AuditPath)

if ($mode -eq 'DRY-RUN' -and $counts.WouldApply -gt 0) {
    Write-Host ""
    Write-Host "This was a dry-run. Re-run with -Apply to create the listed jobs/settings." -ForegroundColor Yellow
}
