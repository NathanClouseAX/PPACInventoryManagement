<#
.SYNOPSIS
    Collects Finance & Operations (FO) specific data for an environment.
.DESCRIPTION
    Targets the D365FO AOS OData API to collect:
      - FO environment metadata
      - Batch job status and health
      - Batch job groups
      - System parameters
      - Data management (DIXF) job history
      - Storage size indicators via entity record counts
      - Scheduled cleanup jobs specific to FO

    FO AOS URL is derived from dual-write config or must be passed directly.

    This script is dot-sourced by Invoke-DataverseInventory.ps1.
.NOTES
    The FO OData API uses the same Azure AD token as the AOS URL.
    The authenticated user must have the "System Administrator" role in the FO environment.
#>

function Collect-FOEnvironmentData {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [hashtable]$EnvEntry,       # From Get-AllEnvironments
        [string]$EnvOutputDir,      # Directory to save JSON files into
        [string]$FOBaseUrl = ''     # e.g. https://myenv.cloudax.dynamics.com
                                    # If empty, attempts auto-detection
    )

    $displayName = $EnvEntry.DisplayName

    Write-InventoryLog "  Starting FO data collection for: $displayName" -Indent 1

    # ── Resolve FO URL ────────────────────────────────────────────────────────
    if (-not $FOBaseUrl) {
        # Try to read from saved dual-write config
        $dwConfigFile = Join-Path $EnvOutputDir 'dualwrite-configs.json'
        if (Test-Path $dwConfigFile) {
            try {
                $dwConfigs = Get-Content $dwConfigFile -Raw | ConvertFrom-Json
                foreach ($cfg in $dwConfigs) {
                    # The AOS URL is often stored in the name or a related field
                    if ($cfg.msdyn_name -match 'https://') {
                        $FOBaseUrl = ($cfg.msdyn_name | Select-String -Pattern 'https://[^\s,]+').Matches[0].Value
                        break
                    }
                }
            } catch {}
        }

        # Try to get from BAP (FO-type linked environment)
        if (-not $FOBaseUrl) {
            try {
                $envDetail = Invoke-BAPRequest `
                    -Path "/providers/Microsoft.BusinessAppPlatform/scopes/admin/environments/$($EnvEntry.EnvironmentId)" `
                    -ApiVersion '2021-04-01'
                $linkedApps = $envDetail.properties.linkedD365AppsMetadata
                if ($linkedApps) {
                    foreach ($app in $linkedApps) {
                        if ($app.type -in 'Dynamics365Operations', 'Finance', 'FinanceAndOperations') {
                            $FOBaseUrl = $app.instanceUrl
                            break
                        }
                    }
                }
            } catch {}
        }
    }

    if (-not $FOBaseUrl) {
        Write-InventoryLog "  Could not determine FO AOS URL - skipping FO collection." -Level WARN -Indent 1
        $result = @{
            CollectedAt   = (Get-Date -Format 'o')
            DisplayName   = $displayName
            FOBaseUrl     = $null
            Notes         = @('FO_URL_NOT_FOUND')
        }
        Save-EnvironmentData -EnvironmentDir $EnvOutputDir -FileName 'fo-summary.json' -Data $result
        return $result
    }

    $FOBaseUrl = $FOBaseUrl.TrimEnd('/')
    Write-InventoryLog "  FO AOS URL: $FOBaseUrl" -Indent 1

    $result = [ordered]@{
        CollectedAt = (Get-Date -Format 'o')
        DisplayName = $displayName
        FOBaseUrl   = $FOBaseUrl
        Sections    = [ordered]@{}
        AllFlags    = @()
    }

    # Helper: call FO OData API
    function Invoke-FOOData {
        param(
            [string]$Path,
            [string]$SectionLabel,
            [string]$SaveFileName,
            [switch]$Paginate,
            [int]   $TimeoutSec = 120
        )

        Write-InventoryLog "    [FO: $SectionLabel]..." -Indent 2
        try {
            $token = Get-AzureToken -ResourceUrl "$FOBaseUrl/"
            $uri   = "$FOBaseUrl/data/$($Path.TrimStart('/'))"
            $headers = @{
                Authorization      = "Bearer $token"
                'OData-MaxVersion' = '4.0'
                'OData-Version'    = '4.0'
                Accept             = 'application/json'
            }

            $resp = Invoke-RestWithRetry -Uri $uri -Headers $headers -TimeoutSec $TimeoutSec

            if ($Paginate) {
                $all  = [System.Collections.Generic.List[object]]::new()
                if ($resp.value) { $all.AddRange([object[]]$resp.value) }
                $nextLink = $resp.'@odata.nextLink'
                $pg = 1
                while ($nextLink -and $pg -lt 50) {
                    $pg++
                    $r = Invoke-RestWithRetry -Uri $nextLink -Headers $headers
                    if ($r.value) { $all.AddRange([object[]]$r.value) }
                    $nextLink = $r.'@odata.nextLink'
                }
                $data = $all
            } else {
                $data = if ($resp.value) { $resp.value } else { $resp }
            }

            Save-EnvironmentData -EnvironmentDir $EnvOutputDir -FileName $SaveFileName -Data $data
            $count = if ($data -is [System.Collections.ICollection]) { $data.Count } else { 1 }
            Write-InventoryLog "    -> $count records saved." -Level OK -Indent 3
            return $data
        } catch {
            Write-InventoryLog "    -> FAILED: $_" -Level WARN -Indent 3
            return $null
        }
    }

    # ── 1. FO System Parameters ───────────────────────────────────────────────
    $sysParams = Invoke-FOOData `
        -Path 'SystemParameters?$select=SystemParameters_AccountsPayablePurchaseOrderThreshold,DataAreaId,PurchaseOrderVersion' `
        -SectionLabel 'System Parameters' `
        -SaveFileName 'fo-system-parameters.json'

    # ── 2. Batch Jobs ─────────────────────────────────────────────────────────
    $batchJobs = Invoke-FOOData `
        -Path 'BatchJobs?$select=BatchJobId,Description,Status,StartDateTime,EndDateTime,WithholdedUntil,Recurrence,CaptureName,LastExecutedDateTime,LastExecutionEndTime,LastExecutionStartTime,AlertsForErrors,AlertsForEnded,AlertsForExecuted' `
        -SectionLabel 'Batch Jobs' `
        -SaveFileName 'fo-batch-jobs.json' `
        -Paginate

    if ($batchJobs) {
        $bjArr = @($batchJobs)
        # Status values: Waiting=0, Executing=1, Ready=2, Withheld=3, Error=4, Cancelling=5, Canceled=6
        $batchByStatus = $bjArr | Group-Object Status | ForEach-Object {
            [PSCustomObject]@{ Status = $_.Name; Count = $_.Count }
        }
        $errorJobs    = @($bjArr | Where-Object { $_.Status -eq 4 })
        $waitingJobs  = @($bjArr | Where-Object { $_.Status -eq 0 })
        $withHeldJobs = @($bjArr | Where-Object { $_.Status -eq 3 })
        $noRecurrence = @($bjArr | Where-Object { -not $_.Recurrence })

        $result.Sections['BatchJobs'] = @{
            TotalCount      = $bjArr.Count
            ByStatus        = $batchByStatus
            ErrorCount      = $errorJobs.Count
            WaitingCount    = $waitingJobs.Count
            WithheldCount   = $withHeldJobs.Count
            NoRecurrenceCount = $noRecurrence.Count
            ErrorJobs       = @($errorJobs | Select-Object -First 20 -Property BatchJobId, Description, LastExecutedDateTime)
            Notes           = @(
                if ($errorJobs.Count -gt 0)   { "FO_BATCH_JOBS_IN_ERROR ($($errorJobs.Count))" }
                if ($withHeldJobs.Count -gt 5) { "FO_MANY_WITHHELD_JOBS ($($withHeldJobs.Count))" }
                if ($bjArr.Count -eq 0)        { 'FO_NO_BATCH_JOBS_CONFIGURED' }
            ) | Where-Object { $_ }
        }
    } else {
        $result.Sections['BatchJobs'] = @{ Notes = @('QUERY_FAILED') }
    }

    # ── 3. Batch Job Groups ───────────────────────────────────────────────────
    Invoke-FOOData `
        -Path 'BatchGroups?$select=GroupId,Description,IsThrottled' `
        -SectionLabel 'Batch Groups' `
        -SaveFileName 'fo-batch-groups.json' | Out-Null

    # ── 4. Data Management (DIXF) Job History ─────────────────────────────────
    $dixfJobs = Invoke-FOOData `
        -Path "DataManagementDefinitionGroups?`$select=DefinitionGroupName,Description,IsEnabled,CreatedDateTime,ModifiedDateTime&`$top=500" `
        -SectionLabel 'DIXF Definition Groups' `
        -SaveFileName 'fo-dixf-definition-groups.json'

    $result.Sections['DIXF'] = @{
        DefinitionGroupCount = if ($dixfJobs) { @($dixfJobs).Count } else { 0 }
    }

    # ── 5. DIXF Execution History (recent) ───────────────────────────────────
    $dixfExec = Invoke-FOOData `
        -Path "DataManagementExecutionHistories?`$select=ExecutionId,DefinitionGroupName,Status,StartTime,EndTime,CreatedDateTime&`$orderby=CreatedDateTime desc&`$top=100" `
        -SectionLabel 'DIXF Execution History (last 100)' `
        -SaveFileName 'fo-dixf-execution-history.json'

    if ($dixfExec) {
        $dixfArr = @($dixfExec)
        $failedDixf = @($dixfArr | Where-Object { $_.Status -in 'Error', 'Aborted' })
        $result.Sections['DIXFExecution'] = @{
            RecentCount  = $dixfArr.Count
            FailedCount  = $failedDixf.Count
            Notes        = @(
                if ($failedDixf.Count -gt 5) { "FO_DIXF_JOBS_FAILED ($($failedDixf.Count) recent failures)" }
            ) | Where-Object { $_ }
        }
    }

    # ── 6. FO Cleanup Batch Jobs (well-known names) ───────────────────────────
    # Look for standard FO cleanup jobs that should be scheduled
    Write-InventoryLog '    [FO: Checking standard cleanup batch jobs]...' -Indent 2
    if ($batchJobs) {
        $bjArr = @($batchJobs)
        $knownCleanupJobs = @(
            @{ Pattern = '*Cleanup*session*';          Purpose = 'Session cleanup'          },
            @{ Pattern = '*Cleanup*staging*';          Purpose = 'DIXF staging cleanup'     },
            @{ Pattern = '*Delete old*batch*';         Purpose = 'Old batch job cleanup'    },
            @{ Pattern = '*SysEmailBatchFlush*';       Purpose = 'Email batch flush'        },
            @{ Pattern = '*Inventory value report*';   Purpose = 'Inventory report cleanup' },
            @{ Pattern = '*InventSumDeltaUpdateFix*';  Purpose = 'Inventory sum fix'        },
            @{ Pattern = '*RetailConnScheduler*';      Purpose = 'Retail channel scheduler' }
        )

        $missingCleanup = [System.Collections.Generic.List[hashtable]]::new()
        foreach ($cj in $knownCleanupJobs) {
            $found = $bjArr | Where-Object { $_.Description -like $cj.Pattern }
            if (-not $found) {
                $missingCleanup.Add(@{ Pattern = $cj.Pattern; Purpose = $cj.Purpose })
            }
        }

        $result.Sections['FOCleanupJobs'] = @{
            MissingStandardJobs = @($missingCleanup)
            Notes               = @(
                if ($missingCleanup.Count -gt 0) {
                    "FO_MISSING_CLEANUP_JOBS ($($missingCleanup.Count) standard jobs not found)"
                }
            ) | Where-Object { $_ }
        }
        Save-EnvironmentData -EnvironmentDir $EnvOutputDir -FileName 'fo-missing-cleanup-jobs.json' -Data $missingCleanup
        Write-InventoryLog "    -> $($missingCleanup.Count) standard cleanup jobs not found." -Level OK -Indent 3
    }

    # ── 7. FO Legal Entities ──────────────────────────────────────────────────
    $legalEntities = Invoke-FOOData `
        -Path 'LegalEntities?$select=DataAreaId,Name,Country,Enabled,IsPrimary,IsVirtual' `
        -SectionLabel 'Legal Entities' `
        -SaveFileName 'fo-legal-entities.json'

    $result.Sections['LegalEntities'] = @{
        TotalCount  = if ($legalEntities) { @($legalEntities).Count } else { 0 }
        EnabledCount = if ($legalEntities) { @($legalEntities | Where-Object { $_.Enabled -eq 'Yes' }).Count } else { 0 }
    }

    # ── 8. FO Users ───────────────────────────────────────────────────────────
    $foUsers = Invoke-FOOData `
        -Path 'Users?$select=UserId,Alias,Enabled,NetworkAlias,Language,LastLoginDateTime,StartDateTime&$filter=Enabled eq true' `
        -SectionLabel 'FO Active Users' `
        -SaveFileName 'fo-users.json' `
        -Paginate

    if ($foUsers) {
        $foUsersArr = @($foUsers)
        $since90   = (Get-Date).AddDays(-90)
        $recentFO  = @($foUsersArr | Where-Object {
            $_.LastLoginDateTime -and [datetime]$_.LastLoginDateTime -ge $since90
        })
        $result.Sections['FOUsers'] = @{
            EnabledCount  = $foUsersArr.Count
            ActiveLast90d = $recentFO.Count
            Notes         = @(
                if ($foUsersArr.Count -gt 0 -and $recentFO.Count -eq 0) { 'FO_NO_ACTIVE_USERS_90D' }
                if ($foUsersArr.Count -eq 0) { 'FO_NO_ENABLED_USERS' }
            ) | Where-Object { $_ }
        }
    }

    # ── 9. FO Workflow Instances (pending/stuck) ──────────────────────────────
    $foWF = Invoke-FOOData `
        -Path "WorkflowInstances?`$select=WorkflowInstanceId,TemplateId,Status,Subject,CreatedDateTime,ModifiedDateTime&`$filter=Status eq 'Pending'&`$top=500" `
        -SectionLabel 'FO Pending Workflow Instances' `
        -SaveFileName 'fo-pending-workflows.json'

    if ($foWF) {
        $foWFArr = @($foWF)
        $since30 = (Get-Date).AddDays(-30)
        $oldPending = @($foWFArr | Where-Object {
            $_.CreatedDateTime -and [datetime]$_.CreatedDateTime -lt $since30
        })
        $result.Sections['FOWorkflows'] = @{
            PendingCount   = $foWFArr.Count
            OldPending30d  = $oldPending.Count
            Notes          = @(
                if ($oldPending.Count -gt 20) { "FO_MANY_STALLED_WORKFLOWS ($($oldPending.Count) pending >30d)" }
            ) | Where-Object { $_ }
        }
    }

    # ── Summary flags ─────────────────────────────────────────────────────────
    $allNotes = [System.Collections.Generic.List[string]]::new()
    foreach ($sec in $result.Sections.Values) {
        if ($sec.Notes) {
            foreach ($n in $sec.Notes) { if ($n) { $allNotes.Add($n) } }
        }
    }
    $result['AllFlags'] = @($allNotes | Sort-Object -Unique)

    Save-EnvironmentData -EnvironmentDir $EnvOutputDir -FileName 'fo-summary.json' -Data $result
    Write-InventoryLog "  FO collection complete. Flags: $($result.AllFlags.Count)" -Level OK -Indent 1

    return $result
}
