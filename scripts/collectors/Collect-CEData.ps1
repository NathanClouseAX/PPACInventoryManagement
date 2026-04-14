<#
.SYNOPSIS
    Collects Customer Engagement (CE) / Dataverse data for a single environment.
.DESCRIPTION
    Calls the Dataverse Web API to collect:
      - System users (active/inactive counts, last logon)
      - Bulk delete jobs (scheduled cleanup tasks)
      - Async operation queue health (pending/failed counts)
      - Solutions (managed and unmanaged)
      - Power Automate workflows
      - Plugin assemblies and active steps
      - Duplicate detection rules
      - App modules (model-driven apps)
      - Connection references and environment variables
      - Entity (table) list with record counts for significant tables
      - Audit log sample (to determine last real use)
      - Retention policies
      - Process sessions (flow run volume)

    This script is dot-sourced by Invoke-DataverseInventory.ps1.
#>

function Collect-CEEnvironmentData {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [hashtable]$EnvEntry,         # From Get-AllEnvironments
        [string]$EnvOutputDir,        # Directory to save JSON files into
        [switch]$IncludeEntityCounts, # Whether to fetch record counts (slow)
        [int]$EntityCountTop = 150    # How many entities to count (sorted by est. importance)
    )

    $displayName  = $EnvEntry.DisplayName
    $apiUrl       = $EnvEntry.OrgApiUrl
    $instanceUrl  = $EnvEntry.OrgUrl

    if (-not $EnvEntry.HasDataverse -or -not $apiUrl) {
        Write-InventoryLog "  No Dataverse org linked - skipping CE collection." -Level SKIP -Indent 1
        return $null
    }

    Write-InventoryLog "  Starting CE data collection for: $displayName" -Indent 1

    $result = [ordered]@{
        CollectedAt       = (Get-Date -Format 'o')
        EnvironmentId     = $EnvEntry.EnvironmentId
        DisplayName       = $displayName
        OrgUrl            = $instanceUrl
        OrgApiUrl         = $apiUrl
        HasFO             = $false
        Sections          = [ordered]@{}
    }

    # Helper: run a Dataverse query section, save results, catch non-fatal errors
    function Invoke-DVSection {
        param(
            [string]$SectionName,
            [string]$ODataPath,
            [string]$SaveFileName,
            [switch]$Paginate,
            [int]   $TimeoutSec = 120
        )
        Write-InventoryLog "    [$SectionName]..." -Indent 2
        try {
            $resp = Invoke-DataverseRequest -InstanceApiUrl $apiUrl `
                                           -ODataPath $ODataPath `
                                           -InstanceUrl $instanceUrl `
                                           -TimeoutSec $TimeoutSec

            if ($Paginate) {
                $all = Get-AllODataPages -InitialResponse $resp `
                                         -InstanceApiUrl $apiUrl `
                                         -InstanceUrl $instanceUrl
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

    # ── 1. System Users ──────────────────────────────────────────────────────
    $userOData = "systemusers?" +
        "`$select=systemuserid,fullname,internalemailaddress,domainname,isdisabled," +
        "createdon,modifiedon,identityid,accessmode,islicensed,isintegrationuser&" +
        "`$orderby=fullname asc"
    $users = Invoke-DVSection -SectionName 'System Users' `
                              -ODataPath $userOData `
                              -SaveFileName 'users.json' `
                              -Paginate

    $activeUsers    = @($users | Where-Object { $_.isdisabled -eq $false -and $_.isintegrationuser -eq $false -and $_.accessmode -eq 0 })
    $disabledUsers  = @($users | Where-Object { $_.isdisabled -eq $true })
    $integrationSvc = @($users | Where-Object { $_.isintegrationuser -eq $true })

    $result.Sections['Users'] = @{
        TotalCount       = if ($users) { $users.Count } else { 0 }
        ActiveCount      = $activeUsers.Count
        DisabledCount    = $disabledUsers.Count
        IntegrationSvcCount = $integrationSvc.Count
        Note             = if ($activeUsers.Count -eq 0) { 'NO_ACTIVE_USERS' } else { $null }
    }

    # ── 2. Bulk Delete Jobs ──────────────────────────────────────────────────
    $bulkDelOData = "bulkdeletejobs?" +
        "`$select=name,statecode,statuscode,nextruntime,sendemailtocreatedby," +
        "recurrencepattern,recurrencestarttime,createdon,modifiedon&" +
        "`$orderby=createdon desc"
    $bulkDeleteJobs = Invoke-DVSection -SectionName 'Bulk Delete Jobs' `
                                       -ODataPath $bulkDelOData `
                                       -SaveFileName 'bulk-delete-jobs.json'

    $scheduledJobs = @($bulkDeleteJobs | Where-Object { $_.statuscode -in 0,1,2 })
    $result.Sections['BulkDeleteJobs'] = @{
        TotalCount     = if ($bulkDeleteJobs) { @($bulkDeleteJobs).Count } else { 0 }
        ScheduledCount = $scheduledJobs.Count
        Note           = if ($scheduledJobs.Count -eq 0) { 'NO_SCHEDULED_BULK_DELETE' } else { $null }
        NextRunTimes   = @($scheduledJobs | ForEach-Object { $_.nextruntime }) | Select-Object -First 5
    }

    # ── 3. Async Operations (System Jobs) ────────────────────────────────────
    # Get counts by state - don't retrieve everything (can be millions of rows)
    Write-InventoryLog '    [Async Operations - counts by state]...' -Indent 2
    try {
        $asyncStates = @(
            @{ State = 0; Label = 'Ready'     },
            @{ State = 1; Label = 'Suspended' },
            @{ State = 2; Label = 'Locked'    },
            @{ State = 3; Label = 'Completed' }
        )
        $asyncCounts = [ordered]@{}
        foreach ($s in $asyncStates) {
            try {
                $r = Invoke-DataverseRequest -InstanceApiUrl $apiUrl -InstanceUrl $instanceUrl `
                        -ODataPath "asyncoperations?`$filter=statecode eq $($s.State)&`$count=true&`$top=1&`$select=asyncoperationid" `
                        -TimeoutSec 60
                $asyncCounts[$s.Label] = if ($r.'@odata.count' -ne $null) { [int]$r.'@odata.count' } else { -1 }
            } catch {
                $asyncCounts[$s.Label] = -1
            }
        }

        # Failed jobs in last 30 days
        $since30 = (Get-Date).AddDays(-30).ToString('yyyy-MM-ddTHH:mm:ssZ')
        try {
            $failedR = Invoke-DataverseRequest -InstanceApiUrl $apiUrl -InstanceUrl $instanceUrl `
                           -ODataPath "asyncoperations?`$filter=statuscode eq 31 and createdon ge $since30&`$count=true&`$top=1&`$select=asyncoperationid" `
                           -TimeoutSec 60
            $asyncCounts['Failed_Last30d'] = if ($failedR.'@odata.count' -ne $null) { [int]$failedR.'@odata.count' } else { -1 }
        } catch {
            $asyncCounts['Failed_Last30d'] = -1
        }

        # Old completed operations (>90 days) - indicates no cleanup running
        $since90 = (Get-Date).AddDays(-90).ToString('yyyy-MM-ddTHH:mm:ssZ')
        try {
            $oldComp = Invoke-DataverseRequest -InstanceApiUrl $apiUrl -InstanceUrl $instanceUrl `
                           -ODataPath "asyncoperations?`$filter=statecode eq 3 and createdon le $since90&`$count=true&`$top=1&`$select=asyncoperationid" `
                           -TimeoutSec 60
            $asyncCounts['CompletedOlderThan90d'] = if ($oldComp.'@odata.count' -ne $null) { [int]$oldComp.'@odata.count' } else { -1 }
        } catch {
            $asyncCounts['CompletedOlderThan90d'] = -1
        }

        Save-EnvironmentData -EnvironmentDir $EnvOutputDir -FileName 'async-operations-summary.json' -Data $asyncCounts
        Write-InventoryLog "    -> Async counts: Ready=$($asyncCounts.Ready) Suspended=$($asyncCounts.Suspended) Failed(30d)=$($asyncCounts.Failed_Last30d)" -Level OK -Indent 3

        $result.Sections['AsyncOperations'] = @{
            Counts = $asyncCounts
            Notes  = @(
                if ($asyncCounts.Suspended -gt 1000)    { 'HIGH_SUSPENDED_JOBS' }
                if ($asyncCounts.Failed_Last30d -gt 500){ 'HIGH_FAILED_JOBS_30D' }
                if ($asyncCounts.CompletedOlderThan90d -gt 100000) { 'OLD_COMPLETED_JOBS_NOT_CLEANED' }
            ) | Where-Object { $_ }
        }
    } catch {
        Write-InventoryLog "    -> Async count queries failed: $_" -Level WARN -Indent 3
        $result.Sections['AsyncOperations'] = @{ Counts = @{}; Notes = @('QUERY_FAILED') }
    }

    # ── 4. Solutions ─────────────────────────────────────────────────────────
    $solOData = "solutions?" +
        "`$select=uniquename,friendlyname,version,ismanaged,installedon,modifiedon,solutiontype&" +
        "`$filter=isvisible eq true&" +
        "`$orderby=friendlyname asc"
    $solutions = Invoke-DVSection -SectionName 'Solutions' `
                                  -ODataPath $solOData `
                                  -SaveFileName 'solutions.json' `
                                  -Paginate

    $managed   = @($solutions | Where-Object { $_.ismanaged -eq $true })
    $unmanaged = @($solutions | Where-Object { $_.ismanaged -eq $false })

    # FO detection via solutions
    $hasFO = $false
    if ($solutions) { $hasFO = Test-HasFOSolution -Solutions $solutions }
    $result.HasFO = $hasFO

    $result.Sections['Solutions'] = @{
        TotalCount          = if ($solutions) { @($solutions).Count } else { 0 }
        ManagedCount        = $managed.Count
        UnmanagedCount      = $unmanaged.Count
        HasFOSolution       = $hasFO
        Notes               = @(
            if ($unmanaged.Count -gt 15) { "HIGH_UNMANAGED_SOLUTIONS ($($unmanaged.Count))" }
        ) | Where-Object { $_ }
        TopUnmanaged        = @($unmanaged | Select-Object -First 10 -ExpandProperty uniquename)
    }

    # ── 5. Workflows / Cloud Flows ───────────────────────────────────────────
    $wfOData = "workflows?" +
        "`$select=name,statecode,statuscode,category,type,createdon,modifiedon,clientdata&" +
        "`$filter=type eq 1 or type eq 2&" +   # definition workflows only (not templates)
        "`$orderby=modifiedon desc"
    $workflows = Invoke-DVSection -SectionName 'Workflows/Flows' `
                                  -ODataPath $wfOData `
                                  -SaveFileName 'workflows.json' `
                                  -Paginate

    $activeWFs   = @($workflows | Where-Object { $_.statecode -eq 1 })
    $inactiveWFs = @($workflows | Where-Object { $_.statecode -eq 0 })
    # Category: 0=Workflow,1=Dialog,2=BusinessRule,3=Action,4=BPF,5=ModernFlow,6=AIFlow
    $modernFlows = @($workflows | Where-Object { $_.category -eq 5 })
    $bpf         = @($workflows | Where-Object { $_.category -eq 4 })

    $result.Sections['Workflows'] = @{
        TotalCount      = if ($workflows) { @($workflows).Count } else { 0 }
        ActiveCount     = $activeWFs.Count
        InactiveCount   = $inactiveWFs.Count
        ModernFlowCount = $modernFlows.Count
        BPFCount        = $bpf.Count
        Notes           = @(
            if ($inactiveWFs.Count -gt 20) { "MANY_INACTIVE_WORKFLOWS ($($inactiveWFs.Count))" }
        ) | Where-Object { $_ }
    }

    # ── 6. Plugin Assemblies ─────────────────────────────────────────────────
    $pluginOData = "pluginassemblies?" +
        "`$select=name,version,culture,publickeytoken,sourcetype,createdon,modifiedon,description&" +
        "`$filter=ishidden/Value eq false&" +
        "`$orderby=name asc"
    $plugins = Invoke-DVSection -SectionName 'Plugin Assemblies' `
                                -ODataPath $pluginOData `
                                -SaveFileName 'plugins.json' `
                                -Paginate

    # Plugin Steps (active vs inactive)
    $stepOData = "sdkmessageprocessingsteps?" +
        "`$select=name,statecode,statuscode,stage,mode,rank,createdon,modifiedon,description&" +
        "`$orderby=name asc"
    $steps = Invoke-DVSection -SectionName 'Plugin Steps' `
                              -ODataPath $stepOData `
                              -SaveFileName 'plugin-steps.json' `
                              -Paginate

    $activeSteps   = @($steps | Where-Object { $_.statecode -eq 0 })
    $inactiveSteps = @($steps | Where-Object { $_.statecode -eq 1 })

    $result.Sections['Plugins'] = @{
        AssemblyCount      = if ($plugins) { @($plugins).Count } else { 0 }
        ActiveStepCount    = $activeSteps.Count
        InactiveStepCount  = $inactiveSteps.Count
        Notes              = @(
            if ($inactiveSteps.Count -gt 30) { "MANY_INACTIVE_PLUGIN_STEPS ($($inactiveSteps.Count))" }
        ) | Where-Object { $_ }
    }

    # ── 7. Duplicate Detection Rules ─────────────────────────────────────────
    $ddOData = "duplicatedetectionrules?" +
        "`$select=name,statecode,statuscode,baseentitytypecode,matchingentitytypecode,createdon,modifiedon"
    $ddRules = Invoke-DVSection -SectionName 'Duplicate Detection Rules' `
                                -ODataPath $ddOData `
                                -SaveFileName 'duplicate-detection-rules.json'

    $enabledRules = @($ddRules | Where-Object { $_.statecode -eq 1 })
    $result.Sections['DuplicateDetection'] = @{
        TotalCount   = if ($ddRules) { @($ddRules).Count } else { 0 }
        EnabledCount = $enabledRules.Count
        Notes        = @(
            if ($enabledRules.Count -eq 0) { 'NO_DUPLICATE_DETECTION_RULES' }
        ) | Where-Object { $_ }
    }

    # ── 8. App Modules (Model-Driven Apps) ───────────────────────────────────
    $appOData = "appmodules?" +
        "`$select=name,uniquename,statecode,statuscode,createdon,modifiedon,description,url,clienttype&" +
        "`$orderby=name asc"
    $appModules = Invoke-DVSection -SectionName 'Model-Driven Apps' `
                                   -ODataPath $appOData `
                                   -SaveFileName 'app-modules.json' `
                                   -Paginate

    $result.Sections['AppModules'] = @{
        TotalCount = if ($appModules) { @($appModules).Count } else { 0 }
    }

    # ── 9. Connection References ─────────────────────────────────────────────
    $connOData = "connectionreferences?" +
        "`$select=connectionreferencedisplayname,connectorid,statecode,statuscode,createdon,modifiedon"
    $connRefs = Invoke-DVSection -SectionName 'Connection References' `
                                 -ODataPath $connOData `
                                 -SaveFileName 'connection-references.json' `
                                 -Paginate

    $result.Sections['ConnectionReferences'] = @{
        TotalCount = if ($connRefs) { @($connRefs).Count } else { 0 }
    }

    # ── 10. Environment Variables ─────────────────────────────────────────────
    $evOData = "environmentvariabledefinitions?" +
        "`$select=displayname,schemaname,statecode,type,createdon,modifiedon"
    $envVars = Invoke-DVSection -SectionName 'Environment Variables' `
                                -ODataPath $evOData `
                                -SaveFileName 'environment-variables.json' `
                                -Paginate

    $result.Sections['EnvironmentVariables'] = @{
        TotalCount = if ($envVars) { @($envVars).Count } else { 0 }
    }

    # ── 11. Audit Log Sample (last 200 entries) ───────────────────────────────
    Write-InventoryLog '    [Audit Log sample (last 200)]...' -Indent 2
    try {
        $since90d  = (Get-Date).AddDays(-90).ToString('yyyy-MM-ddTHH:mm:ssZ')
        $auditResp = Invoke-DataverseRequest -InstanceApiUrl $apiUrl -InstanceUrl $instanceUrl `
                         -ODataPath "audits?`$select=createdon,objecttypecode,action,useragent&`$orderby=createdon desc&`$top=200" `
                         -TimeoutSec 90
        $auditSample = if ($auditResp.value) { $auditResp.value } else { @() }
        Save-EnvironmentData -EnvironmentDir $EnvOutputDir -FileName 'audit-sample.json' -Data $auditSample

        $lastAudit = if ($auditSample.Count -gt 0) { $auditSample[0].createdon } else { $null }
        $recentCount = @($auditSample | Where-Object { $_.createdon -ge $since90d }).Count

        $result.Sections['AuditLog'] = @{
            SampleCount  = $auditSample.Count
            LastEntry    = $lastAudit
            Recent90dCount = $recentCount
            Notes        = @(
                if (-not $lastAudit) { 'AUDIT_DISABLED_OR_NO_ACTIVITY' }
                elseif ($recentCount -eq 0) { 'NO_AUDIT_ACTIVITY_90D' }
            ) | Where-Object { $_ }
        }
        Write-InventoryLog "    -> Last audit entry: $lastAudit (${recentCount} in last 90d)" -Level OK -Indent 3
    } catch {
        Write-InventoryLog "    -> Audit query failed (auditing may be disabled): $_" -Level WARN -Indent 3
        $result.Sections['AuditLog'] = @{ SampleCount = 0; Notes = @('QUERY_FAILED_OR_DISABLED') }
    }

    # ── 12. Retention Policies ───────────────────────────────────────────────
    Write-InventoryLog '    [Retention Policies]...' -Indent 2
    try {
        $retentionResp = Invoke-DataverseRequest -InstanceApiUrl $apiUrl -InstanceUrl $instanceUrl `
                             -ODataPath "retentionconfigs?`$select=entitylogicalname,isreadyforretention,statecode,createdon,modifiedon" `
                             -TimeoutSec 60
        $retentions = if ($retentionResp.value) { $retentionResp.value } else { @() }
        Save-EnvironmentData -EnvironmentDir $EnvOutputDir -FileName 'retention-policies.json' -Data $retentions

        $result.Sections['RetentionPolicies'] = @{
            TotalCount = $retentions.Count
            Notes      = @(
                if ($retentions.Count -eq 0) { 'NO_RETENTION_POLICIES' }
            ) | Where-Object { $_ }
        }
        Write-InventoryLog "    -> $($retentions.Count) retention configurations found." -Level OK -Indent 3
    } catch {
        Write-InventoryLog "    -> Retention policies not available (feature may not be enabled): $_" -Level WARN -Indent 3
        $result.Sections['RetentionPolicies'] = @{ TotalCount = 0; Notes = @('NOT_AVAILABLE') }
    }

    # ── 13. Process Sessions (flow run volume indicator) ─────────────────────
    Write-InventoryLog '    [Process Sessions - last 30d count]...' -Indent 2
    try {
        $since30 = (Get-Date).AddDays(-30).ToString('yyyy-MM-ddTHH:mm:ssZ')
        $psResp = Invoke-DataverseRequest -InstanceApiUrl $apiUrl -InstanceUrl $instanceUrl `
                      -ODataPath "processsessions?`$filter=createdon ge $since30&`$count=true&`$top=1&`$select=activityid" `
                      -TimeoutSec 60
        $psCount = if ($psResp.'@odata.count' -ne $null) { [int]$psResp.'@odata.count' } else { -1 }
        $result.Sections['ProcessSessions'] = @{ Last30dCount = $psCount }
        Write-InventoryLog "    -> ~$psCount process sessions in last 30 days." -Level OK -Indent 3
    } catch {
        $result.Sections['ProcessSessions'] = @{ Last30dCount = -1 }
        Write-InventoryLog "    -> Could not count process sessions: $_" -Level WARN -Indent 3
    }

    # ── 14. Entity / Table Statistics ────────────────────────────────────────
    Write-InventoryLog '    [Entity Definitions]...' -Indent 2
    try {
        $entityResp = Invoke-DataverseRequest -InstanceApiUrl $apiUrl -InstanceUrl $instanceUrl `
                          -ODataPath "EntityDefinitions?`$select=LogicalName,DisplayName,EntitySetName,IsCustomEntity,IsActivity,DataProviderId,OwnershipType,IsValidForAdvancedFind&`$orderby=LogicalName asc" `
                          -TimeoutSec 180
        $entities = Get-AllODataPages -InitialResponse $entityResp -InstanceApiUrl $apiUrl -InstanceUrl $instanceUrl -MaxPages 20

        # Strip down to essential fields for storage
        $entitySummary = $entities | ForEach-Object {
            @{
                LogicalName       = $_.LogicalName
                DisplayName       = $_.DisplayName.UserLocalizedLabel.Label
                EntitySetName     = $_.EntitySetName
                IsCustomEntity    = $_.IsCustomEntity
                IsActivity        = $_.IsActivity
                OwnershipType     = $_.OwnershipType
            }
        }
        Save-EnvironmentData -EnvironmentDir $EnvOutputDir -FileName 'entity-definitions.json' -Data $entitySummary
        Write-InventoryLog "    -> $($entitySummary.Count) entity definitions saved." -Level OK -Indent 3

        $result.Sections['Entities'] = @{
            TotalCount  = $entitySummary.Count
            CustomCount = @($entitySummary | Where-Object { $_.IsCustomEntity -eq $true }).Count
        }

        # ── 14b. Entity Record Counts ─────────────────────────────────────────
        if ($IncludeEntityCounts) {
            Write-InventoryLog "    [Entity Record Counts - top $EntityCountTop entities]..." -Indent 2

            # Prioritize: custom entities + known high-volume OOB entities
            $priorityOOB = @(
                'activitypointer','annotation','email','task','phonecall','appointment',
                'systemjob','asyncoperation','bulkoperationlog','duplicaterecord',
                'principalobjectaccess','userentityinstancedata','tracing',
                'auditbase','activityparty','queueitem','connection'
            )

            $toCount = [System.Collections.Generic.List[object]]::new()
            # Custom entities first
            $toCount.AddRange([object[]]@($entitySummary | Where-Object { $_.IsCustomEntity -and $_.EntitySetName }))
            # Then priority OOB
            foreach ($oob in $priorityOOB) {
                $match = $entitySummary | Where-Object { $_.LogicalName -eq $oob }
                if ($match -and ($toCount | Where-Object { $_.LogicalName -eq $oob }).Count -eq 0) {
                    $toCount.Add($match)
                }
            }

            $countResults = [System.Collections.Generic.List[hashtable]]::new()
            $counted = 0
            foreach ($ent in $toCount) {
                if ($counted -ge $EntityCountTop -or -not $ent.EntitySetName) { break }
                Write-Progress -Activity 'Counting entity records' `
                               -Status "$($ent.LogicalName) ($counted/$EntityCountTop)" `
                               -PercentComplete (($counted / $EntityCountTop) * 100)
                $cnt = Get-DataverseEntityCount -InstanceApiUrl $apiUrl `
                                                -EntitySetName  $ent.EntitySetName `
                                                -InstanceUrl    $instanceUrl `
                                                -TimeoutSec     30
                if ($cnt -ge 0) {
                    $countResults.Add(@{
                        LogicalName   = $ent.LogicalName
                        DisplayName   = $ent.DisplayName
                        IsCustom      = $ent.IsCustomEntity
                        RecordCount   = $cnt
                    })
                }
                $counted++
            }
            Write-Progress -Activity 'Counting entity records' -Completed

            $sorted = @($countResults | Sort-Object RecordCount -Descending)
            Save-EnvironmentData -EnvironmentDir $EnvOutputDir -FileName 'entity-counts.json' -Data $sorted

            $largeCustom = @($sorted | Where-Object { $_.IsCustom -and $_.RecordCount -gt 100000 })
            $result.Sections['EntityCounts'] = @{
                CountedEntities     = $sorted.Count
                TopByRecordCount    = @($sorted | Select-Object -First 10)
                LargeCustomEntities = $largeCustom
                Notes               = @(
                    if ($largeCustom.Count -gt 0) { "LARGE_CUSTOM_ENTITIES_NO_CLEANUP ($($largeCustom.Count) entities >100k rows)" }
                ) | Where-Object { $_ }
            }
            Write-InventoryLog "    -> Record counts collected for $($sorted.Count) entities." -Level OK -Indent 3
        }
    } catch {
        Write-InventoryLog "    -> Entity definition fetch failed: $_" -Level WARN -Indent 3
        $result.Sections['Entities'] = @{ TotalCount = -1; Error = $_.ToString() }
    }

    # ── 15. Dual-Write Configuration (FO link indicator) ─────────────────────
    if ($hasFO) {
        Write-InventoryLog '    [Dual-Write Configuration]...' -Indent 2
        try {
            $dwResp = Invoke-DataverseRequest -InstanceApiUrl $apiUrl -InstanceUrl $instanceUrl `
                          -ODataPath "msdyn_dualwriteruntimeconfigs?`$select=msdyn_dualwriteruntimeconfigid,msdyn_name,msdyn_status,createdon,modifiedon" `
                          -TimeoutSec 60
            $dwConfigs = if ($dwResp.value) { $dwResp.value } else { @() }
            Save-EnvironmentData -EnvironmentDir $EnvOutputDir -FileName 'dualwrite-configs.json' -Data $dwConfigs

            $dwMapResp = Invoke-DataverseRequest -InstanceApiUrl $apiUrl -InstanceUrl $instanceUrl `
                             -ODataPath "msdyn_dualwriteentitymaps?`$select=msdyn_name,msdyn_state,msdyn_integrationsolution,msdyn_lastsynctime,createdon" `
                             -TimeoutSec 90
            $dwMaps = if ($dwMapResp.value) { $dwMapResp.value } else { @() }
            Save-EnvironmentData -EnvironmentDir $EnvOutputDir -FileName 'dualwrite-entity-maps.json' -Data $dwMaps

            $errMaps = @($dwMaps | Where-Object { $_.msdyn_state -in 3, 4 })  # Error/Stopped-error
            $result.Sections['DualWrite'] = @{
                ConfigCount   = $dwConfigs.Count
                MapCount      = $dwMaps.Count
                ErrorMapCount = $errMaps.Count
                Notes         = @(
                    if ($errMaps.Count -gt 0) { "DUALWRITE_MAPS_IN_ERROR ($($errMaps.Count))" }
                ) | Where-Object { $_ }
            }
            Write-InventoryLog "    -> $($dwMaps.Count) dual-write maps, $($errMaps.Count) in error state." -Level OK -Indent 3
        } catch {
            Write-InventoryLog "    -> Dual-write query failed (may not be configured): $_" -Level WARN -Indent 3
            $result.Sections['DualWrite'] = @{ Notes = @('NOT_CONFIGURED_OR_INACCESSIBLE') }
        }
    }

    # ── Summary flags ─────────────────────────────────────────────────────────
    $allNotes = [System.Collections.Generic.List[string]]::new()
    foreach ($sec in $result.Sections.Values) {
        if ($sec.Notes) {
            foreach ($n in $sec.Notes) { if ($n) { $allNotes.Add($n) } }
        }
        if ($sec.Note) { $allNotes.Add($sec.Note) }
    }
    $result['AllFlags'] = @($allNotes | Sort-Object -Unique)

    # Save summary
    Save-EnvironmentData -EnvironmentDir $EnvOutputDir -FileName 'ce-summary.json' -Data $result
    Write-InventoryLog "  CE collection complete. Flags: $($result.AllFlags.Count)" -Level OK -Indent 1

    return $result
}
