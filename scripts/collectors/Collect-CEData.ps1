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

    # Check if scheduled jobs cover the highest-priority cleanup targets by job name.
    # Bulk delete job names are user-defined, so we match common naming patterns.
    $coversAsyncOps = ($scheduledJobs | Where-Object {
        $_.name -match 'async|system.?job|workflow.?job|completed.?job'
    }).Count -gt 0
    $coversAudit    = ($scheduledJobs | Where-Object { $_.name -match 'audit' }).Count -gt 0
    $coversEmail    = ($scheduledJobs | Where-Object { $_.name -match 'email|activit' }).Count -gt 0

    $result.Sections['BulkDeleteJobs'] = @{
        TotalCount     = if ($bulkDeleteJobs) { @($bulkDeleteJobs).Count } else { 0 }
        ScheduledCount = $scheduledJobs.Count
        CoversAsyncOps = $coversAsyncOps
        CoversAudit    = $coversAudit
        CoversEmail    = $coversEmail
        Note           = if ($scheduledJobs.Count -eq 0) { 'NO_SCHEDULED_BULK_DELETE' } else { $null }
        Notes          = @(
            if ($scheduledJobs.Count -gt 0 -and -not $coversAsyncOps) {
                'NO_ASYNCOP_BULK_DELETE_JOB - no scheduled job appears to target system jobs or async operations'
            }
        ) | Where-Object { $_ }
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

    # ── 3b. Organization Health Settings ─────────────────────────────────────
    # Single query for key org-level settings that control storage growth and cleanup.
    #   plugintracelogsetting : 0=Off, 1=Exception Only, 2=All
    #   auditretentionperiodv2: -1=Forever (no auto-delete); other value = days
    Write-InventoryLog '    [Organization Health Settings]...' -Indent 2
    $orgSettings = $null
    try {
        $orgResp = Invoke-DataverseRequest -InstanceApiUrl $apiUrl -InstanceUrl $instanceUrl `
                       -ODataPath "organizations?`$select=organizationid,auditretentionperiodv2,plugintracelogsetting,isauditenabled&`$top=1" `
                       -TimeoutSec 60
        $orgSettings = if ($orgResp.value) { $orgResp.value[0] } else { $null }
        if ($orgSettings) {
            Save-EnvironmentData -EnvironmentDir $EnvOutputDir -FileName 'org-settings.json' -Data $orgSettings
        }

        $traceLogSetting    = if ($orgSettings.plugintracelogsetting -ne $null) { [int]$orgSettings.plugintracelogsetting } else { 0 }
        $auditRetentionDays = if ($orgSettings.auditretentionperiodv2 -ne $null) { [int]$orgSettings.auditretentionperiodv2 } else { $null }
        $auditEnabled       = if ($orgSettings) { [bool]$orgSettings.isauditenabled } else { $false }

        $traceLogLabel = switch ($traceLogSetting) { 0 { 'Off' } 1 { 'Exception Only' } 2 { 'All' } default { 'Unknown' } }
        $auditRetentionLabel = if ($auditRetentionDays -eq -1) { 'Forever (no auto-delete)' }
                               elseif ($auditRetentionDays -ne $null) { "$auditRetentionDays days" }
                               else { 'Not configured' }

        $result.Sections['OrgSettings'] = @{
            PluginTraceLogSetting = $traceLogLabel
            AuditRetentionDays    = $auditRetentionDays
            AuditEnabled          = $auditEnabled
            Notes                 = @(
                if ($traceLogSetting -gt 0) {
                    "PLUGIN_TRACE_LOGGING_ENABLED ($traceLogLabel) - plug-in trace log should be Off in production to prevent PluginTraceLogBase table growth"
                }
                if ($auditEnabled -and $auditRetentionDays -eq -1) {
                    'AUDIT_RETENTION_SET_TO_FOREVER - auditing is on but retention is Forever; AuditBase table will grow without bound until manually deleted'
                }
                if ($auditEnabled -and $auditRetentionDays -eq $null) {
                    'AUDIT_RETENTION_NOT_CONFIGURED - auditing is enabled but no retention period is set'
                }
            ) | Where-Object { $_ }
        }
        Write-InventoryLog "    -> Trace logging: $traceLogLabel | Audit retention: $auditRetentionLabel | Auditing on: $auditEnabled" -Level OK -Indent 3
    } catch {
        Write-InventoryLog "    -> Organization settings query failed: $_" -Level WARN -Indent 3
        $result.Sections['OrgSettings'] = @{ Notes = @('QUERY_FAILED') }
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
            if ($managed.Count -eq 0 -and $activeUsers.Count -gt 0) {
                'NO_MANAGED_SOLUTIONS - no managed solutions installed; all customizations are unmanaged indicating no ALM/deployment strategy'
            }
        ) | Where-Object { $_ }
        TopUnmanaged        = @($unmanaged | Select-Object -First 10 -ExpandProperty uniquename)
    }

    # ── 5. Workflows / Cloud Flows ───────────────────────────────────────────
    $wfOData = "workflows?" +
        "`$select=name,statecode,statuscode,category,type,createdon,modifiedon,clientdata,_ownerid_value&" +
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

    # Cross-reference active workflow/flow owners against the disabled users collected in section 1.
    # A flow owned by a disabled account will stop working when its connection token expires.
    $disabledUserIdSet = [System.Collections.Generic.HashSet[string]]::new(
        [string[]]@($disabledUsers | ForEach-Object { $_.systemuserid } | Where-Object { $_ })
    )
    $activeWFsWithDisabledOwner = @($activeWFs | Where-Object {
        $_._ownerid_value -and $disabledUserIdSet.Contains([string]$_._ownerid_value)
    })

    $result.Sections['Workflows'] = @{
        TotalCount             = if ($workflows) { @($workflows).Count } else { 0 }
        ActiveCount            = $activeWFs.Count
        InactiveCount          = $inactiveWFs.Count
        ModernFlowCount        = $modernFlows.Count
        BPFCount               = $bpf.Count
        ActiveOwnedByDisabled  = $activeWFsWithDisabledOwner.Count
        DisabledOwnerFlowNames = @($activeWFsWithDisabledOwner | Select-Object -First 10 -ExpandProperty name)
        Notes                  = @(
            if ($inactiveWFs.Count -gt 20) { "MANY_INACTIVE_WORKFLOWS ($($inactiveWFs.Count))" }
            if ($activeWFsWithDisabledOwner.Count -gt 0) {
                "ACTIVE_FLOWS_OWNED_BY_DISABLED_USERS ($($activeWFsWithDisabledOwner.Count) active flows/workflows owned by disabled accounts - connections will break when they expire)"
            }
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

    $connRefsArr      = @($connRefs)
    # statecode 1 = Inactive; connection references become inactive when the underlying
    # connection is deleted or the connector is removed — every flow using them will fail.
    $inactiveConnRefs = @($connRefsArr | Where-Object { $_.statecode -eq 1 })

    $result.Sections['ConnectionReferences'] = @{
        TotalCount    = $connRefsArr.Count
        InactiveCount = $inactiveConnRefs.Count
        InactiveNames = @($inactiveConnRefs | Select-Object -First 10 -ExpandProperty connectionreferencedisplayname)
        Notes         = @(
            if ($inactiveConnRefs.Count -gt 0) {
                "BROKEN_CONNECTION_REFERENCES ($($inactiveConnRefs.Count) of $($connRefsArr.Count) connection references are inactive - all flows using these will fail)"
            }
        ) | Where-Object { $_ }
    }

    # ── 10. Environment Variables ─────────────────────────────────────────────
    $evOData = "environmentvariabledefinitions?" +
        "`$select=environmentvariabledefinitionid,displayname,schemaname,statecode,type,defaultvalue,createdon,modifiedon"
    $envVars = Invoke-DVSection -SectionName 'Environment Variables' `
                                -ODataPath $evOData `
                                -SaveFileName 'environment-variables.json' `
                                -Paginate

    # Cross-check which definitions have no current value AND no default value set.
    # Components using an env var with no value and no default receive null at runtime.
    Write-InventoryLog '    [Environment Variable Values]...' -Indent 2
    $evVarsMissingValue = @()
    try {
        $evValResp = Invoke-DataverseRequest -InstanceApiUrl $apiUrl -InstanceUrl $instanceUrl `
                         -ODataPath "environmentvariablevalues?`$select=environmentvariablevalueid,_environmentvariabledefinitionid_value,value&`$top=500" `
                         -TimeoutSec 60
        $evValues = if ($evValResp.value) { $evValResp.value } else { @() }
        Save-EnvironmentData -EnvironmentDir $EnvOutputDir -FileName 'environment-variable-values.json' -Data $evValues

        if (@($envVars).Count -gt 0) {
            $defIdsWithValues = [System.Collections.Generic.HashSet[string]]::new(
                [string[]]@($evValues | ForEach-Object { $_.'_environmentvariabledefinitionid_value' } | Where-Object { $_ })
            )
            # Flag definitions that have neither a current value record nor a default value
            $evVarsMissingValue = @($envVars | Where-Object {
                $_.environmentvariabledefinitionid -and
                -not $defIdsWithValues.Contains([string]$_.environmentvariabledefinitionid) -and
                (-not $_.defaultvalue -or $_.defaultvalue -eq '')
            })
        }
        Write-InventoryLog "    -> $(@($evValues).Count) value records; $($evVarsMissingValue.Count) definitions have no value and no default." -Level OK -Indent 3
    } catch {
        Write-InventoryLog "    -> Environment variable values query failed: $_" -Level WARN -Indent 3
    }

    $result.Sections['EnvironmentVariables'] = @{
        TotalCount        = if ($envVars) { @($envVars).Count } else { 0 }
        MissingValueCount = $evVarsMissingValue.Count
        MissingValueNames = @($evVarsMissingValue | Select-Object -First 10 -ExpandProperty displayname)
        Notes             = @(
            if ($evVarsMissingValue.Count -gt 0) {
                "ENV_VARS_MISSING_VALUES ($($evVarsMissingValue.Count) environment variables have no current value and no default - components using these will receive null)"
            }
        ) | Where-Object { $_ }
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

    # ── 12b. Mailbox / Server-Side Sync Health ──────────────────────────────
    Write-InventoryLog '    [Mailbox / Server-Side Sync Health]...' -Indent 2
    try {
        $mailboxResp = Invoke-DataverseRequest -InstanceApiUrl $apiUrl -InstanceUrl $instanceUrl `
                           -ODataPath "mailboxes?`$select=name,emailaddress,statuscode,mailboxstatus,outgoingemailstatus,incomingemailstatus,testmailboxaccesscompletedon,mailboxtypecode,createdon,modifiedon&`$filter=statecode eq 0&`$top=500" `
                           -TimeoutSec 90
        $mailboxes = if ($mailboxResp.value) { $mailboxResp.value } else { @() }
        Save-EnvironmentData -EnvironmentDir $EnvOutputDir -FileName 'mailboxes.json' -Data $mailboxes

        # mailboxstatus: 0=Not Run, 1=Success, 2=Failure
        $errorMailboxes     = @($mailboxes | Where-Object { [int]$_.mailboxstatus -eq 2 })
        $notTestedMailboxes = @($mailboxes | Where-Object { [int]$_.mailboxstatus -eq 0 -and -not $_.testmailboxaccesscompletedon })

        $mailboxNotes = [System.Collections.Generic.List[string]]::new()
        if ($errorMailboxes.Count -gt 0) {
            $mailboxNotes.Add("MAILBOX_SYNC_ERRORS ($($errorMailboxes.Count) of $($mailboxes.Count) mailboxes in error state - email/appointment tracking broken)")
        }
        if ($notTestedMailboxes.Count -gt 5) {
            $mailboxNotes.Add("MAILBOXES_NOT_TESTED ($($notTestedMailboxes.Count) mailboxes have never been tested for server-side sync access)")
        }
        if ($mailboxes.Count -eq 0 -and @($activeUsers).Count -gt 0) {
            $mailboxNotes.Add("NO_MAILBOXES_CONFIGURED (no active mailboxes found but environment has $(@($activeUsers).Count) active users)")
        }

        $result.Sections['MailboxHealth'] = @{
            TotalActive = $mailboxes.Count
            InError     = $errorMailboxes.Count
            NotTested   = $notTestedMailboxes.Count
            ErrorNames  = @($errorMailboxes | Select-Object -First 10 -ExpandProperty emailaddress)
            Notes       = @($mailboxNotes)
        }
        Write-InventoryLog "    -> $($mailboxes.Count) active mailboxes, $($errorMailboxes.Count) in error, $($notTestedMailboxes.Count) not tested." -Level OK -Indent 3
    } catch {
        Write-InventoryLog "    -> Mailbox query failed: $_" -Level WARN -Indent 3
        $result.Sections['MailboxHealth'] = @{ TotalActive = -1; Notes = @('QUERY_FAILED') }
    }

    # ── 12c. Unresolved Duplicate Records ───────────────────────────────────
    Write-InventoryLog '    [Duplicate Record Backlog]...' -Indent 2
    try {
        $dupRecordResp = Invoke-DataverseRequest -InstanceApiUrl $apiUrl -InstanceUrl $instanceUrl `
                             -ODataPath "duplicaterecords?`$count=true&`$top=1&`$select=duplicateid" `
                             -TimeoutSec 60
        $dupRecordCount = if ($dupRecordResp.'@odata.count' -ne $null) { [int]$dupRecordResp.'@odata.count' } else { -1 }

        $dupNotes = [System.Collections.Generic.List[string]]::new()
        if ($dupRecordCount -gt 1000) {
            $dupNotes.Add("HIGH_UNRESOLVED_DUPLICATES ($dupRecordCount unresolved duplicate record pairs - data quality degrading)")
        } elseif ($dupRecordCount -gt 100) {
            $dupNotes.Add("MANY_UNRESOLVED_DUPLICATES ($dupRecordCount unresolved duplicate record pairs)")
        }

        $result.Sections['DuplicateRecords'] = @{
            UnresolvedCount = $dupRecordCount
            Notes           = @($dupNotes)
        }
        Write-InventoryLog "    -> $dupRecordCount unresolved duplicate records." -Level OK -Indent 3
    } catch {
        Write-InventoryLog "    -> Duplicate records query failed: $_" -Level WARN -Indent 3
        $result.Sections['DuplicateRecords'] = @{ UnresolvedCount = -1; Notes = @() }
    }

    # ── 12d. Queue Item Backlog ─────────────────────────────────────────────
    Write-InventoryLog '    [Queue Item Backlog]...' -Indent 2
    try {
        $queueItemResp = Invoke-DataverseRequest -InstanceApiUrl $apiUrl -InstanceUrl $instanceUrl `
                             -ODataPath "queueitems?`$count=true&`$top=1&`$select=queueitemid" `
                             -TimeoutSec 60
        $queueItemCount = if ($queueItemResp.'@odata.count' -ne $null) { [int]$queueItemResp.'@odata.count' } else { -1 }

        $queueNotes = [System.Collections.Generic.List[string]]::new()
        if ($queueItemCount -gt 5000) {
            $queueNotes.Add("HIGH_QUEUE_ITEM_BACKLOG ($queueItemCount items in queues - may indicate routing or processing bottleneck)")
        }

        $result.Sections['QueueItems'] = @{
            TotalCount = $queueItemCount
            Notes      = @($queueNotes)
        }
        Write-InventoryLog "    -> $queueItemCount queue items." -Level OK -Indent 3
    } catch {
        Write-InventoryLog "    -> Queue items query failed: $_" -Level WARN -Indent 3
        $result.Sections['QueueItems'] = @{ TotalCount = -1; Notes = @() }
    }

    # ── 12e. Service Endpoint / Webhook Health ──────────────────────────────
    Write-InventoryLog '    [Service Endpoints / Webhooks]...' -Indent 2
    try {
        $endpointResp = Invoke-DataverseRequest -InstanceApiUrl $apiUrl -InstanceUrl $instanceUrl `
                            -ODataPath "serviceendpoints?`$select=name,namespaceaddress,connectionmode,contract,authtype,createdon,modifiedon" `
                            -TimeoutSec 60
        $endpoints = if ($endpointResp.value) { $endpointResp.value } else { @() }
        Save-EnvironmentData -EnvironmentDir $EnvOutputDir -FileName 'service-endpoints.json' -Data $endpoints

        # contract: 8=Webhook, 7=EventHub, 1=ServiceBus Queue, 2=ServiceBus Topic
        $webhooks  = @($endpoints | Where-Object { [int]$_.contract -eq 8 })
        $eventHubs = @($endpoints | Where-Object { [int]$_.contract -eq 7 })
        $serviceBus = @($endpoints | Where-Object { [int]$_.contract -in @(1, 2) })

        $result.Sections['ServiceEndpoints'] = @{
            TotalCount      = $endpoints.Count
            WebhookCount    = $webhooks.Count
            EventHubCount   = $eventHubs.Count
            ServiceBusCount = $serviceBus.Count
            Notes           = @()
        }
        Write-InventoryLog "    -> $($endpoints.Count) service endpoints ($($webhooks.Count) webhooks, $($eventHubs.Count) event hubs, $($serviceBus.Count) service bus)." -Level OK -Indent 3
    } catch {
        Write-InventoryLog "    -> Service endpoint query failed: $_" -Level WARN -Indent 3
        $result.Sections['ServiceEndpoints'] = @{ TotalCount = -1; Notes = @() }
    }

    # ── 12f. SLA KPI Violations ─────────────────────────────────────────────
    Write-InventoryLog '    [SLA KPI Violations]...' -Indent 2
    try {
        # status: 0=InProgress, 1=Noncompliant, 2=Noncompliant(Paused), 4=Succeeded, 5=Canceled
        $slaResp = Invoke-DataverseRequest -InstanceApiUrl $apiUrl -InstanceUrl $instanceUrl `
                       -ODataPath "slakpiinstances?`$filter=(status eq 1 or status eq 2)&`$count=true&`$top=1&`$select=slakpiinstanceid" `
                       -TimeoutSec 60
        $slaViolationCount = if ($slaResp.'@odata.count' -ne $null) { [int]$slaResp.'@odata.count' } else { -1 }

        $slaNotes = [System.Collections.Generic.List[string]]::new()
        if ($slaViolationCount -gt 50) {
            $slaNotes.Add("HIGH_SLA_VIOLATIONS ($slaViolationCount noncompliant SLA KPI instances - service level commitments being breached)")
        }

        $result.Sections['SLAViolations'] = @{
            NoncompliantCount = $slaViolationCount
            Notes             = @($slaNotes)
        }
        Write-InventoryLog "    -> $slaViolationCount noncompliant SLA KPI instances." -Level OK -Indent 3
    } catch {
        Write-InventoryLog "    -> SLA KPI query failed (SLAs may not be configured): $_" -Level WARN -Indent 3
        $result.Sections['SLAViolations'] = @{ NoncompliantCount = -1; Notes = @() }
    }

    # ── 12g. Stale Process Instances ────────────────────────────────────────
    # Active process sessions older than 180 days indicate abandoned BPF instances
    # or stalled workflow sessions that will never complete.
    Write-InventoryLog '    [Stale Process Instances (>180d)]...' -Indent 2
    try {
        $since180 = (Get-Date).AddDays(-180).ToString('yyyy-MM-ddTHH:mm:ssZ')
        $staleProcResp = Invoke-DataverseRequest -InstanceApiUrl $apiUrl -InstanceUrl $instanceUrl `
                             -ODataPath "processsessions?`$filter=statecode eq 1 and createdon le $since180&`$count=true&`$top=1&`$select=activityid" `
                             -TimeoutSec 60
        $staleProcCount = if ($staleProcResp.'@odata.count' -ne $null) { [int]$staleProcResp.'@odata.count' } else { -1 }

        $staleNotes = [System.Collections.Generic.List[string]]::new()
        if ($staleProcCount -gt 500) {
            $staleNotes.Add("STALE_BPF_INSTANCES ($staleProcCount active process sessions >180 days old - likely abandoned BPF instances inflating storage)")
        }

        $result.Sections['StaleProcessInstances'] = @{
            StaleActiveCount = $staleProcCount
            Notes            = @($staleNotes)
        }
        Write-InventoryLog "    -> $staleProcCount active process sessions older than 180 days." -Level OK -Indent 3
    } catch {
        Write-InventoryLog "    -> Stale process session query failed: $_" -Level WARN -Indent 3
        $result.Sections['StaleProcessInstances'] = @{ StaleActiveCount = -1; Notes = @() }
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

    # ── 13b. Cleanup Table Health Indicators ─────────────────────────────────
    # Targeted counts for high-volume tables that signal specific cleanup gaps.
    # Each count is a read-only diagnostic query - no mutations performed.
    Write-InventoryLog '    [Cleanup Table Health Indicators]...' -Indent 2
    $cleanupHealthNotes = [System.Collections.Generic.List[string]]::new()

    # WorkflowLog (WorkflowLogBase): old succeeded execution records accumulate when
    # the parent AsyncOperation cleanup job isn't running. Cascade-deletes with parent.
    $wfLogOldCount = -1
    try {
        $since30wf = (Get-Date).AddDays(-30).ToString('yyyy-MM-ddTHH:mm:ssZ')
        $wfLogR    = Invoke-DataverseRequest -InstanceApiUrl $apiUrl -InstanceUrl $instanceUrl `
                         -ODataPath "workflowlogs?`$filter=status eq 2 and createdon le $since30wf&`$count=true&`$top=1&`$select=activityid" `
                         -TimeoutSec 60
        $wfLogOldCount = if ($wfLogR.'@odata.count' -ne $null) { [int]$wfLogR.'@odata.count' } else { -1 }
        if ($wfLogOldCount -gt 50000) {
            $cleanupHealthNotes.Add("OLD_WORKFLOW_LOGS_ACCUMULATING ($wfLogOldCount succeeded WorkflowLog records >30d - async operation cleanup may not be running)")
        }
    } catch {
        Write-InventoryLog "    -> WorkflowLog count failed: $_" -Level WARN -Indent 3
    }

    # PluginTraceLog (PluginTraceLogBase): any records indicate trace logging was/is active.
    # Consumes Log storage. Should be Off in production (confirmed via OrgSettings above).
    # Built-in recurring job deletes records >1 day old when logging is active.
    $pluginTraceLogCount = -1
    try {
        $ptlR = Invoke-DataverseRequest -InstanceApiUrl $apiUrl -InstanceUrl $instanceUrl `
                    -ODataPath "plugintracelogs?`$count=true&`$top=1&`$select=plugintraceid" `
                    -TimeoutSec 60
        $pluginTraceLogCount = if ($ptlR.'@odata.count' -ne $null) { [int]$ptlR.'@odata.count' } else { -1 }
        if ($pluginTraceLogCount -gt 5000) {
            $cleanupHealthNotes.Add("PLUGIN_TRACE_LOGS_ACCUMULATING ($pluginTraceLogCount records - verify recurring cleanup job is active and trace logging is disabled in production)")
        }
    } catch {
        Write-InventoryLog "    -> PluginTraceLog count failed: $_" -Level WARN -Indent 3
    }

    # Annotation (AnnotationBase / Notes): records with large file attachments are top
    # File storage consumers. Create a bulk delete job targeting large/old notes.
    $largeAnnotationCount = -1
    try {
        $annotR = Invoke-DataverseRequest -InstanceApiUrl $apiUrl -InstanceUrl $instanceUrl `
                      -ODataPath "annotations?`$filter=filesizeinbytes gt 1048576&`$count=true&`$top=1&`$select=annotationid" `
                      -TimeoutSec 60
        $largeAnnotationCount = if ($annotR.'@odata.count' -ne $null) { [int]$annotR.'@odata.count' } else { -1 }
        if ($largeAnnotationCount -gt 500) {
            $cleanupHealthNotes.Add("LARGE_ANNOTATION_FILES ($largeAnnotationCount notes with >1 MB attachments - significant file storage consumer; consider bulk delete job for old attachments)")
        }
    } catch {
        Write-InventoryLog "    -> Annotation large-file count failed: $_" -Level WARN -Indent 3
    }

    # Email activities (EmailBase): completed emails older than 90 days consume significant
    # database storage (EmailBase, EmailHashBase, ActivityPartyBase, ActivityPointerBase).
    # Recommend a recurring bulk deletion job: statecode=1 AND actualend < 90 days ago.
    $oldEmailCount = -1
    try {
        $since90em = (Get-Date).AddDays(-90).ToString('yyyy-MM-ddTHH:mm:ssZ')
        $emailR    = Invoke-DataverseRequest -InstanceApiUrl $apiUrl -InstanceUrl $instanceUrl `
                         -ODataPath "emails?`$filter=statecode eq 1 and actualend le $since90em&`$count=true&`$top=1&`$select=activityid" `
                         -TimeoutSec 60
        $oldEmailCount = if ($emailR.'@odata.count' -ne $null) { [int]$emailR.'@odata.count' } else { -1 }
        if ($oldEmailCount -gt 10000) {
            $cleanupHealthNotes.Add("OLD_COMPLETED_EMAILS ($oldEmailCount completed email activities >90d - consider a recurring bulk delete job: statecode=1 AND actualend <90d ago)")
        }
    } catch {
        Write-InventoryLog "    -> Email activity count failed: $_" -Level WARN -Indent 3
    }

    # ImportJob (ImportJobBase): completed import history records older than 90 days.
    # Recommend a bulk delete job: System Job Type = Import AND Completed On > 90 days ago.
    $oldImportCount = -1
    try {
        $since90im = (Get-Date).AddDays(-90).ToString('yyyy-MM-ddTHH:mm:ssZ')
        $importR   = Invoke-DataverseRequest -InstanceApiUrl $apiUrl -InstanceUrl $instanceUrl `
                         -ODataPath "importjobs?`$filter=completedon le $since90im&`$count=true&`$top=1&`$select=importjobid" `
                         -TimeoutSec 60
        $oldImportCount = if ($importR.'@odata.count' -ne $null) { [int]$importR.'@odata.count' } else { -1 }
        if ($oldImportCount -gt 50) {
            $cleanupHealthNotes.Add("OLD_IMPORT_JOB_HISTORY ($oldImportCount import job records >90d - no cleanup bulk deletion job found; create one: System Job Type = Import AND Completed On older than 90 days)")
        }
    } catch {
        Write-InventoryLog "    -> ImportJob count failed: $_" -Level WARN -Indent 3
    }

    # BulkDeleteOperation (BulkDeleteOperationBase): history of past bulk delete runs.
    # The records themselves are small, but if never cleaned up they indicate no self-cleanup job.
    $oldBulkDeleteOpCount = -1
    try {
        $since90bd  = (Get-Date).AddDays(-90).ToString('yyyy-MM-ddTHH:mm:ssZ')
        $bulkDelOpR = Invoke-DataverseRequest -InstanceApiUrl $apiUrl -InstanceUrl $instanceUrl `
                          -ODataPath "bulkdeleteoperations?`$filter=statecode eq 3 and completedon le $since90bd&`$count=true&`$top=1&`$select=bulkdeleteoperationid" `
                          -TimeoutSec 60
        $oldBulkDeleteOpCount = if ($bulkDelOpR.'@odata.count' -ne $null) { [int]$bulkDelOpR.'@odata.count' } else { -1 }
        if ($oldBulkDeleteOpCount -gt 100) {
            $cleanupHealthNotes.Add("OLD_BULK_DELETE_OPERATION_HISTORY ($oldBulkDeleteOpCount completed bulk delete operation records >90d - create a self-cleaning bulk delete job: System Job Type = Bulk Delete AND Completed On older than 90 days)")
        }
    } catch {
        Write-InventoryLog "    -> BulkDeleteOperation count failed: $_" -Level WARN -Indent 3
    }

    $result.Sections['CleanupTableHealth'] = @{
        WorkflowLogOldSucceeded  = $wfLogOldCount
        PluginTraceLogTotal      = $pluginTraceLogCount
        LargeAnnotations         = $largeAnnotationCount
        OldCompletedEmails       = $oldEmailCount
        OldImportJobRecords      = $oldImportCount
        OldBulkDeleteOpRecords   = $oldBulkDeleteOpCount
        Notes                    = @($cleanupHealthNotes)
    }
    Save-EnvironmentData -EnvironmentDir $EnvOutputDir -FileName 'cleanup-table-health.json' -Data $result.Sections['CleanupTableHealth']
    Write-InventoryLog "    -> WFLog(>30d): $wfLogOldCount | TraceLog: $pluginTraceLogCount | LargeAnnotations(>1MB): $largeAnnotationCount | OldEmails(>90d): $oldEmailCount | OldImports(>90d): $oldImportCount | OldBulkDelOps(>90d): $oldBulkDeleteOpCount" -Level OK -Indent 3

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
                # Core activity / communication tables (top database storage consumers)
                'activitypointer','email','appointment','task','phonecall','activityparty',
                # Notes / attachments (top file storage consumers)
                'annotation',
                # Async / system jobs (fastest-growing tables; cleanup critical)
                'asyncoperation','workflowlog',
                # Cleanup / bulk operation history
                'bulkdeleteoperation','bulkoperationlog','importjob',
                # Logging tables (Log storage)
                'plugintracelog','tracelog',
                # Sharing / access control (can grow extremely large with heavy sharing)
                'principalobjectaccess',
                # Duplicates / user data
                'duplicaterecord','userentityinstancedata',
                # Queue / connection objects
                'queueitem','connection',
                # Server-side sync mapping (grows with Exchange sync usage)
                'exchangesyncidmapping'
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
            # Teams-linked tables (Dataverse for Teams) have strict 2 GB storage limits
            $teamsLargeEntities = @($sorted | Where-Object { $_.LogicalName -like 'msteams_*' -and $_.RecordCount -gt 10000 })
            $result.Sections['EntityCounts'] = @{
                CountedEntities      = $sorted.Count
                TopByRecordCount     = @($sorted | Select-Object -First 10)
                LargeCustomEntities  = $largeCustom
                TeamsLargeEntities   = $teamsLargeEntities
                Notes                = @(
                    if ($largeCustom.Count -gt 0) { "LARGE_CUSTOM_ENTITIES_NO_CLEANUP ($($largeCustom.Count) entities >100k rows)" }
                    if ($teamsLargeEntities.Count -gt 0) { "TEAMS_TABLE_STORAGE_HIGH ($($teamsLargeEntities.Count) Teams-linked tables with >10k records - Dataverse for Teams has strict 2 GB storage limit)" }
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
