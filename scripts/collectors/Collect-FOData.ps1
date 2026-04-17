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

    FO AOS URL must be supplied by the caller — the orchestrator obtains it
    from the Dataverse RetrieveFinanceAndOperationsIntegrationDetails action.

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
        [Parameter(Mandatory)]
        [string]$FOBaseUrl          # F&O AOS URL from Get-FOIntegrationDetails
    )

    $displayName = $EnvEntry.DisplayName

    Write-InventoryLog "  Starting FO data collection for: $displayName" -Indent 1

    if (-not $FOBaseUrl) {
        Write-InventoryLog "  No FO AOS URL supplied - skipping FO collection." -Level WARN -Indent 1
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
    # Check for standard FO cleanup jobs that should be scheduled.
    # Each entry includes the menu path and batch class so admins know exactly
    # where to set up any missing jobs.
    Write-InventoryLog '    [FO: Checking standard cleanup batch jobs]...' -Indent 2
    if ($batchJobs) {
        $bjArr = @($batchJobs)

        # Core: always applicable regardless of modules in use
        $knownCleanupJobsCore = @(
            # ── System / Batch History ─────────────────────────────────────────
            @{
                Pattern    = '*Batch job history clean-up*'
                Purpose    = 'Batch job history cleanup'
                Category   = 'System'
                MenuPath   = 'System administration > Periodic tasks > Batch job history clean-up'
                BatchClass = 'SysBatchHistoryCleanUp'
                Notes      = 'Cleans BatchJobHistory, BatchHistory, BatchConstraintHistory. Recommended: 180-day retention, run daily or weekly outside business hours. For large tables (500k+ records / >75% of table) the stored procedure SysTruncateBatchHistory is used automatically.'
            },
            @{
                Pattern    = '*Batch job clean-up*'
                Purpose    = 'Batch job cleanup (accumulated/abandoned jobs)'
                Category   = 'System'
                MenuPath   = 'System administration > Periodic tasks > Batch job clean-up'
                BatchClass = 'SysBatchJobCleanUp'
                Notes      = 'Available from v10.0.39 (Platform update 63). Removes abandoned or unused batch job records. Filter by status (Withhold/Error/Finished/Canceled), caption, or class name.'
            },
            @{
                Pattern    = '*Notification clean up*'
                Purpose    = 'Alert and notification cleanup'
                Category   = 'System'
                MenuPath   = 'System administration > Periodic tasks > Notification clean up'
                BatchClass = ''
                Notes      = 'Cleans EventInbox and EventInboxData tables. Recommended: weekly. If alerts are not used, disable the batch job entirely.'
            },
            @{
                Pattern    = '*Clean up log*'
                Purpose    = 'Database log (audit trail) cleanup'
                Category   = 'System'
                MenuPath   = 'System administration > Inquiries > Database > Database Log > Clean up log'
                BatchClass = ''
                Notes      = 'Cleans SysDatabaseLog table. Grows extremely rapidly depending on configuration. Recommended: weekly or monthly. Example filter: retain 1 year of entries. Records for electronically signed items cannot be deleted.'
            },
            @{
                Pattern    = '*Job history cleanup*'
                Purpose    = 'DIXF execution history and staging cleanup'
                Category   = 'DIXF'
                MenuPath   = 'Data management workspace > Job history cleanup'
                BatchClass = 'DMFExecutionHistoryCleanup'
                Notes      = 'Cleans all staging tables, DMFSTAGINGVALIDATIONLOG, DMFSTAGINGLOG, DMFDEFINITIONGROUPEXECUTIONHISTORY, and related tables. Recommended: at least once per day. Default auto-cleanup at 90 days (since Sept 2023). Configure retention days and max execution hours.'
            },
            @{
                Pattern    = '*Staging clean*'
                Purpose    = 'DIXF staging cleanup (legacy name)'
                Category   = 'DIXF'
                MenuPath   = 'Data management workspace > Job history cleanup'
                BatchClass = 'DMFExecutionHistoryCleanup'
                Notes      = 'Older batch job name for DIXF staging cleanup. In current environments this job is named "Job history cleanup".'
            },
            @{
                Pattern    = '*Clean up session*'
                Purpose    = 'User session cleanup'
                Category   = 'System'
                MenuPath   = 'System administration > Periodic tasks > Clean up sessions'
                BatchClass = 'SysUserSessionCleanup'
                Notes      = 'Removes stale user session records from the database.'
            },
            @{
                Pattern    = '*SysEmailBatchFlush*'
                Purpose    = 'Email batch flush'
                Category   = 'System'
                MenuPath   = 'System administration > Periodic tasks > Email > Email distributor batch'
                BatchClass = 'SysEmailBatchFlush'
                Notes      = 'Processes and flushes the outbound email queue. Required for email delivery from D365FO.'
            }
        )

        # Sales & Marketing
        $knownCleanupJobsSales = @(
            @{
                Pattern    = '*Sales update history cleanup*'
                Purpose    = 'Sales update history cleanup'
                Category   = 'Sales'
                MenuPath   = 'Sales and marketing > Periodic tasks > Clean up > Sales update history cleanup'
                BatchClass = ''
                Notes      = 'Cleans SalesParmTable, SalesParmUpdate, SalesParmLine, SalesParmSubLine for posted confirmations, picking lists, packing slips, and invoices. Run after Delete sales orders/quotations/return orders. Recommended: annually.'
            },
            @{
                Pattern    = '*Delete*sales order*'
                Purpose    = 'Delete old sales orders'
                Category   = 'Sales'
                MenuPath   = 'Sales and marketing > Periodic tasks > Clean up > Delete orders'
                BatchClass = ''
                Notes      = 'Deletes posted sales order headers and lines older than threshold. Process in batches under 5,000 records to avoid locking. Run before Sales update history cleanup.'
            },
            @{
                Pattern    = '*Delete*quotation*'
                Purpose    = 'Delete old sales quotations'
                Category   = 'Sales'
                MenuPath   = 'Sales and marketing > Periodic tasks > Clean up > Delete quotations'
                BatchClass = ''
                Notes      = 'Deletes old sales quotation headers and lines. Run before Sales update history cleanup.'
            },
            @{
                Pattern    = '*Delete*return order*'
                Purpose    = 'Delete old return orders'
                Category   = 'Sales'
                MenuPath   = 'Sales and marketing > Periodic tasks > Clean up > Delete return orders'
                BatchClass = ''
                Notes      = 'Deletes return order headers and lines. Run before Sales update history cleanup.'
            },
            @{
                Pattern    = '*Order events cleanup*'
                Purpose    = 'Order events cleanup'
                Category   = 'Sales'
                MenuPath   = 'Sales and marketing > Periodic tasks > Clean up > Order events cleanup'
                BatchClass = ''
                Notes      = 'Cleans order event records. After running, review Order event setup and disable any unneeded event tracking checkboxes.'
            }
        )

        # Procurement
        $knownCleanupJobsProcurement = @(
            @{
                Pattern    = '*Purchase update history cleanup*'
                Purpose    = 'Purchase update history cleanup'
                Category   = 'Procurement'
                MenuPath   = 'Procurement and sourcing > Periodic tasks > Clean up > Purchase update history cleanup'
                BatchClass = ''
                Notes      = 'Cleans purchase order update history for confirmations, product receipts, and invoices. Mirrors the sales update history cleanup. Recommended: annually.'
            }
        )

        # Warehouse Management
        $knownCleanupJobsWarehouse = @(
            @{
                Pattern    = '*Work creation history cleanup*'
                Purpose    = 'Work creation history cleanup'
                Category   = 'Warehouse'
                MenuPath   = 'Warehouse management > Periodic tasks > Clean up > Work creation history cleanup'
                BatchClass = ''
                Notes      = 'Cleans WHSWorkCreateHistory table. Parameter: days to keep (recommend 90-365 depending on warehouse volume). Reduces storage and simplifies upgrades.'
            },
            @{
                Pattern    = '*Wave batch cleanup*'
                Purpose    = 'Wave batch cleanup'
                Category   = 'Warehouse'
                MenuPath   = 'Warehouse management > Periodic tasks > Clean up > Wave batch cleanup'
                BatchClass = ''
                Notes      = 'Cleans BatchJobHistory entries for wave batch group and WHSWaveTableBatch (wave-batch transaction) records.'
            },
            @{
                Pattern    = '*Wave processing history log cleanup*'
                Purpose    = 'Wave processing history log cleanup'
                Category   = 'Warehouse'
                MenuPath   = 'Warehouse management > Periodic tasks > Clean up > Wave processing history log cleanup'
                BatchClass = ''
                Notes      = 'Cleans WHSWaveExecutionHistory table. Parameter: days to keep.'
            },
            @{
                Pattern    = '*Containerization history cleanup*'
                Purpose    = 'Containerization history cleanup'
                Category   = 'Warehouse'
                MenuPath   = 'Warehouse management > Periodic tasks > Clean up > Containerization history cleanup'
                BatchClass = ''
                Notes      = 'Cleans WHSContainerizationHistory table. Parameter: days to keep (0 purges all records).'
            },
            @{
                Pattern    = '*Mobile device activity log cleanup*'
                Purpose    = 'Mobile device activity log cleanup'
                Category   = 'Warehouse'
                MenuPath   = 'Warehouse management > Periodic tasks > Clean up > Mobile device activity log cleanup'
                BatchClass = ''
                Notes      = 'Cleans WHSMobileDeviceActivityLog table (production order starts, driver check-ins/outs, LP removals). Parameter: days to keep. Recommended: weekly (weekends).'
            },
            @{
                Pattern    = '*Work user session log cleanup*'
                Purpose    = 'Work user session log cleanup'
                Category   = 'Warehouse'
                MenuPath   = 'Warehouse management > Periodic tasks > Clean up > Work user session log cleanup'
                BatchClass = ''
                Notes      = 'Cleans WHSWorkUserSessionLog table. Parameter unit is HOURS (not days). Recommended: daily or weekly.'
            },
            @{
                Pattern    = '*Cycle count plan cleanup*'
                Purpose    = 'Cycle count plan cleanup'
                Category   = 'Warehouse'
                MenuPath   = 'Warehouse management > Periodic tasks > Clean up > Cycle count plan cleanup'
                BatchClass = ''
                Notes      = 'Cleans WHSCycleCountPlanOverview records without planned recurrence, and their associated batch jobs and history. Primary benefit: reduces batch job history size.'
            },
            @{
                Pattern    = '*Wave labels cleanup*'
                Purpose    = 'Wave labels cleanup'
                Category   = 'Warehouse'
                MenuPath   = 'Warehouse management > Periodic tasks > Clean up > Wave labels cleanup'
                BatchClass = ''
                Notes      = 'Cleans WHSWaveLabel table. Parameter: days to keep.'
            },
            @{
                Pattern    = '*Work line history log cleanup*'
                Purpose    = 'Work line history log cleanup'
                Category   = 'Warehouse'
                MenuPath   = 'Warehouse management > Periodic tasks > Clean up > Work line history log cleanup'
                BatchClass = ''
                Notes      = 'Cleans WHSTmpWorkLineHistory table. Parameter: days to keep.'
            },
            @{
                Pattern    = '*License plate registration history*'
                Purpose    = 'License plate registration history cleanup'
                Category   = 'Warehouse'
                MenuPath   = 'Warehouse management > Periodic tasks > Clean up > Clean up License plate registration history'
                BatchClass = ''
                Notes      = 'Cleans WHSLicensePlateReceivingHistory table. Parameter: days to keep.'
            }
        )

        # Inventory Management
        $knownCleanupJobsInventory = @(
            @{
                Pattern    = '*On-hand entries cleanup*'
                Purpose    = 'On-hand entries cleanup (InventSum)'
                Category   = 'Inventory'
                MenuPath   = 'Inventory management > Periodic tasks > Clean up > On-hand entries cleanup'
                BatchClass = ''
                Notes      = 'Cleans InventSum table for zero-quantity tracking-dimension entries. Default: 7-day retention. Run in batch outside business hours. Note: may remove data used by Physical inventory by inventory dimension report.'
            },
            @{
                Pattern    = '*Warehouse management on-hand entries cleanup*'
                Purpose    = 'WMS on-hand entries cleanup (InventSum + WHSInventReserve)'
                Category   = 'Inventory'
                MenuPath   = 'Inventory management > Periodic tasks > Clean up > Warehouse management on-hand entries cleanup'
                BatchClass = ''
                Notes      = 'Cleans InventSum and WHSInventReserve for WMS-enabled items at zero value. Commits in batches of 100. Mandatory "max execution hours" parameter from v10.0.32+. Significantly improves on-hand calculation performance.'
            },
            @{
                Pattern    = '*Inventory dimensions cleanup*'
                Purpose    = 'Inventory dimensions cleanup (InventDim)'
                Category   = 'Inventory'
                MenuPath   = 'Inventory management > Periodic tasks > Clean up > Inventory dimensions cleanup'
                BatchClass = ''
                Notes      = 'Permanently deletes unused InventDim records. WARNING: no alert or database log created. Only run with good reason and outside business hours.'
            },
            @{
                Pattern    = '*Inventory settlements cleanup*'
                Purpose    = 'Inventory settlements cleanup'
                Category   = 'Inventory'
                MenuPath   = 'Inventory management > Periodic tasks > Clean up > Inventory settlements cleanup'
                BatchClass = ''
                Notes      = 'Groups closed and deletes canceled inventory settlements. Do not run close to fiscal year-end. Resource-intensive; run during low-usage periods.'
            },
            @{
                Pattern    = '*Inventory journal*cleanup*'
                Purpose    = 'Inventory journals cleanup'
                Category   = 'Inventory'
                MenuPath   = 'Inventory management > Periodic tasks > Clean up > Inventory journals cleanup'
                BatchClass = ''
                Notes      = 'Cleans posted inventory journals. Resource-intensive; run per company sequentially during low-usage periods.'
            },
            @{
                Pattern    = '*Transfer order update history cleanup*'
                Purpose    = 'Transfer order update history cleanup'
                Category   = 'Inventory'
                MenuPath   = 'Inventory management > Periodic tasks > Clean up > Transfer order update history cleanup'
                BatchClass = ''
                Notes      = 'Cleans InventTransferParmTable, InventTransferParmUpdate, and InventTransferParmLine tables created when posting transfer orders.'
            },
            @{
                Pattern    = '*Inventory*report*clean*'
                Purpose    = 'Inventory on-hand report storage cleanup'
                Category   = 'Inventory'
                MenuPath   = 'Inventory management > Periodic tasks > Clean up > Inventory on-hand report data clean up'
                BatchClass = ''
                Notes      = 'Cleans stored on-hand report output data. Parameter: delete reports executed before specified date.'
            },
            @{
                Pattern    = '*InventSumDeltaUpdateFix*'
                Purpose    = 'Inventory sum delta update fix'
                Category   = 'Inventory'
                MenuPath   = 'Inventory management > Periodic tasks > Clean up'
                BatchClass = 'InventSumDeltaUpdateFix'
                Notes      = 'Corrects InventSum delta update records. Run if inventory on-hand discrepancies are observed.'
            }
        )

        # Production & Cost Management
        $knownCleanupJobsProduction = @(
            @{
                Pattern    = '*Production journals cleanup*'
                Purpose    = 'Production journals cleanup'
                Category   = 'Production'
                MenuPath   = 'Production control > Periodic tasks > Clean up > Production journals cleanup'
                BatchClass = ''
                Notes      = 'Cleans unused production journals.'
            },
            @{
                Pattern    = '*Production orders cleanup*'
                Purpose    = 'Production orders cleanup (ended orders)'
                Category   = 'Production'
                MenuPath   = 'Production control > Periodic tasks > Clean up > Production orders cleanup'
                BatchClass = ''
                Notes      = 'Cleans ended production orders. Also accessible from Cost management > Manufacturing accounting > Clean up > Production orders cleanup.'
            }
        )

        # Master Planning
        $knownCleanupJobsMRP = @(
            @{
                Pattern    = '*Plan version cleanup*'
                Purpose    = 'Master plan version cleanup'
                Category   = 'Master Planning'
                MenuPath   = 'Master planning > Master planning > Maintain plans > Plan version cleanup'
                BatchClass = ''
                Notes      = 'Removes orphaned master planning data and old plan versions. Automatic cleanup can malfunction leaving orphan data that slows queries. Recommended: monthly, never while MRP is running.'
            }
        )

        # Finance / General Ledger
        $knownCleanupJobsFinance = @(
            @{
                Pattern    = '*Clean up ledger journals*'
                Purpose    = 'Ledger journals cleanup (GL/AR/AP posted journals)'
                Category   = 'Finance'
                MenuPath   = 'General ledger > Periodic tasks > Clean up ledger journals'
                BatchClass = ''
                Notes      = 'Permanently deletes posted GL, AR, and AP journal headers, lines, and attachments. WARNING: no reversal possible after deletion. Recommended: annually after year-close and reconciliation. v10.0.47+ includes batch performance improvement feature.'
            }
        )

        # Retail / Commerce (only relevant if Commerce module is in use)
        $knownCleanupJobsRetail = @(
            @{
                Pattern    = '*RetailConnScheduler*'
                Purpose    = 'Retail channel scheduler'
                Category   = 'Retail'
                MenuPath   = 'Retail and Commerce > Retail and Commerce IT > Distribution schedule'
                BatchClass = 'RetailConnScheduler'
                Notes      = 'Required for Retail/Commerce implementations. Manages channel data synchronization to POS and e-commerce.'
            },
            @{
                Pattern    = '*Clean up email notification logs*'
                Purpose    = 'Email notification log cleanup (Retail/Commerce)'
                Category   = 'Retail'
                MenuPath   = 'Retail and Commerce > Retail and Commerce IT > Email and notifications > Clean up email notification logs'
                BatchClass = ''
                Notes      = 'Cleans email notification log records. Retail and Commerce module only.'
            }
        )

        # Combine all categories
        $knownCleanupJobs = $knownCleanupJobsCore +
                            $knownCleanupJobsSales +
                            $knownCleanupJobsProcurement +
                            $knownCleanupJobsWarehouse +
                            $knownCleanupJobsInventory +
                            $knownCleanupJobsProduction +
                            $knownCleanupJobsMRP +
                            $knownCleanupJobsFinance +
                            $knownCleanupJobsRetail

        $missingCleanup = [System.Collections.Generic.List[hashtable]]::new()
        foreach ($cj in $knownCleanupJobs) {
            $found = $bjArr | Where-Object { $_.Description -like $cj.Pattern }
            if (-not $found) {
                $missingCleanup.Add(@{
                    Pattern    = $cj.Pattern
                    Purpose    = $cj.Purpose
                    Category   = $cj.Category
                    MenuPath   = $cj.MenuPath
                    BatchClass = $cj.BatchClass
                    Notes      = $cj.Notes
                })
            }
        }

        # Group missing jobs by category for easier reporting
        $missingByCategory = $missingCleanup | Group-Object Category | ForEach-Object {
            [PSCustomObject]@{ Category = $_.Name; Count = $_.Count; Jobs = @($_.Group) }
        } | Sort-Object Category

        $result.Sections['FOCleanupJobs'] = @{
            TotalChecked        = $knownCleanupJobs.Count
            MissingCount        = $missingCleanup.Count
            MissingStandardJobs = @($missingCleanup)
            MissingByCategory   = @($missingByCategory)
            Notes               = @(
                if ($missingCleanup.Count -gt 0) {
                    "FO_MISSING_CLEANUP_JOBS ($($missingCleanup.Count) of $($knownCleanupJobs.Count) standard jobs not found)"
                }
            ) | Where-Object { $_ }
        }
        Save-EnvironmentData -EnvironmentDir $EnvOutputDir -FileName 'fo-missing-cleanup-jobs.json' -Data $missingCleanup
        Write-InventoryLog "    -> $($missingCleanup.Count) of $($knownCleanupJobs.Count) standard cleanup jobs not found." -Level OK -Indent 3
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
