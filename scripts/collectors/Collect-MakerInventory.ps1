<#
.SYNOPSIS
    Collects the "maker surface" inventory for a single environment: canvas
    apps, cloud flows (portal-side), custom connectors, connections, Power
    Pages sites, Copilot Studio bots, dataflows, and AI Builder models.
.DESCRIPTION
    Power Automate workflows and model-driven apps are already collected by
    Collect-CEData.ps1 via Dataverse entities (workflows, appmodules). This
    collector targets the *portal-side* BAP / PowerApps Admin API endpoints
    which return richer data — owner, sharing, connector dependencies, last
    published / last modified — that the Dataverse entity rows don't expose.

    Per-env output files:
      canvas-apps.json
      cloud-flows.json
      custom-connectors.json
      connections.json
      power-pages.json
      copilots.json
      dataflows.json
      ai-models.json
      maker-summary.json   (rolled-up counts + flags)

    This script is dot-sourced by Invoke-DataverseInventory.ps1.
.NOTES
    Endpoints used:
      - BAP (api.bap.microsoft.com):
          /providers/Microsoft.BusinessAppPlatform/scopes/admin/environments/{id}/apps
          /providers/Microsoft.BusinessAppPlatform/scopes/admin/environments/{id}/v2/flows
          /providers/Microsoft.BusinessAppPlatform/scopes/admin/environments/{id}/apis
          /providers/Microsoft.BusinessAppPlatform/scopes/admin/environments/{id}/connections
      - Dataverse Web API (for bots, power pages, AI models, dataflows):
          bots, powerpagesites / mspp_website, msdyn_aimodel, msdyn_dataflow
#>

function Collect-MakerEnvironmentInventory {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable]$EnvEntry,
        [Parameter(Mandatory)][string]$EnvOutputDir
    )

    $displayName  = $EnvEntry.DisplayName
    $envId        = $EnvEntry.EnvironmentId
    $apiUrl       = $EnvEntry.OrgApiUrl
    $instanceUrl  = $EnvEntry.OrgUrl
    $hasDataverse = [bool]$EnvEntry.HasDataverse

    Write-InventoryLog "  Starting maker inventory for: $displayName" -Indent 1

    $result = [ordered]@{
        CollectedAt    = (Get-Date -Format 'o')
        EnvironmentId  = $envId
        DisplayName    = $displayName
        Sections       = [ordered]@{}
        AllFlags       = @()
    }

    # Helper: run a BAP call, save raw results on success, record error category on failure
    function Invoke-MakerBAPSection {
        param(
            [string]$SectionName,
            [string]$Path,
            [string]$ApiVersion = '2016-11-01',
            [string]$SaveFileName,
            [int]   $TimeoutSec = 90
        )
        Write-InventoryLog "    [Maker: $SectionName]..." -Indent 2
        try {
            $resp = Invoke-BAPRequest -Path $Path -ApiVersion $ApiVersion -TimeoutSec $TimeoutSec
            $data = Get-AllBAPPages -InitialResponse $resp
            Save-EnvironmentData -EnvironmentDir $EnvOutputDir -FileName $SaveFileName -Data $data
            Write-InventoryLog "    -> $($data.Count) records saved." -Level OK -Indent 3
            return $data
        } catch {
            $errInfo = Get-HttpErrorClassification -ErrorRecord $_
            Write-InventoryLog "    -> FAILED [$($errInfo.Category) status=$($errInfo.Status)]: $($errInfo.Message)" -Level WARN -Indent 3
            return @{ __Error = $errInfo }
        }
    }

    function Invoke-MakerDVSection {
        param(
            [string]$SectionName,
            [string]$ODataPath,
            [string]$SaveFileName,
            [int]   $TimeoutSec = 90
        )
        if (-not $hasDataverse -or -not $apiUrl) { return $null }
        Write-InventoryLog "    [Maker: $SectionName]..." -Indent 2
        try {
            $resp = Invoke-DataverseRequest -InstanceApiUrl $apiUrl -InstanceUrl $instanceUrl `
                                            -ODataPath $ODataPath -TimeoutSec $TimeoutSec
            $data = Get-AllODataPages -InitialResponse $resp -InstanceApiUrl $apiUrl -InstanceUrl $instanceUrl
            Save-EnvironmentData -EnvironmentDir $EnvOutputDir -FileName $SaveFileName -Data $data
            Write-InventoryLog "    -> $($data.Count) records saved." -Level OK -Indent 3
            return $data
        } catch {
            $errInfo = Get-HttpErrorClassification -ErrorRecord $_
            # Feature-not-enabled / NOT_FOUND is common for bots, power pages, AI
            # models on envs that don't have those capabilities — don't log as ERROR.
            Write-InventoryLog "    -> $SectionName [$($errInfo.Category) status=$($errInfo.Status)]: $($errInfo.Message)" -Level WARN -Indent 3
            return @{ __Error = $errInfo }
        }
    }

    # Build disabled-user id set once so maker sections can cross-reference
    $disabledUserIdSet = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
    try {
        $usersFile = Join-Path $EnvOutputDir 'users.json'
        if (Test-Path $usersFile) {
            $usersJson = Get-Content $usersFile -Raw | ConvertFrom-Json
            foreach ($u in @($usersJson | Where-Object { $_.isdisabled -eq $true })) {
                if ($u.systemuserid) { [void]$disabledUserIdSet.Add([string]$u.systemuserid) }
                if ($u.domainname)   { [void]$disabledUserIdSet.Add([string]$u.domainname) }
                if ($u.internalemailaddress) { [void]$disabledUserIdSet.Add([string]$u.internalemailaddress) }
            }
        }
    } catch {}

    # ── 1. Canvas Apps ─────────────────────────────────────────────────────────
    $canvasApps = Invoke-MakerBAPSection `
        -SectionName 'Canvas Apps' `
        -Path "/providers/Microsoft.PowerApps/scopes/admin/environments/$envId/apps" `
        -ApiVersion '2016-11-01' `
        -SaveFileName 'canvas-apps.json'

    if ($canvasApps -is [hashtable] -and $canvasApps.__Error) {
        $result.Sections['CanvasApps'] = @{
            Notes = @("CANVAS_APPS_$($canvasApps.__Error.Category)")
        }
    } else {
        $apps    = @($canvasApps)
        $owners  = @($apps | ForEach-Object {
            if ($_.properties -and $_.properties.owner) { $_.properties.owner.email } else { $null }
        } | Where-Object { $_ })
        $orphans = @($apps | Where-Object {
            $_.properties -and $_.properties.owner -and
            ($disabledUserIdSet.Contains([string]$_.properties.owner.email) -or
             $disabledUserIdSet.Contains([string]$_.properties.owner.id))
        })
        $stale = @($apps | Where-Object {
            $_.properties -and $_.properties.lastModifiedTime -and
            ([datetime]$_.properties.lastModifiedTime -lt (Get-Date).AddDays(-180))
        })
        $connectors = @()
        foreach ($a in $apps) {
            if ($a.properties -and $a.properties.connectionReferences) {
                $connectors += @($a.properties.connectionReferences.PSObject.Properties.Value | ForEach-Object {
                    if ($_.displayName) { $_.displayName } elseif ($_.id) { $_.id }
                })
            }
        }
        $result.Sections['CanvasApps'] = @{
            TotalCount       = $apps.Count
            OrphanedCount    = $orphans.Count
            StaleCount       = $stale.Count
            UniqueOwners     = @($owners | Select-Object -Unique).Count
            UniqueConnectors = @($connectors | Select-Object -Unique).Count
            TopConnectors    = @($connectors | Group-Object | Sort-Object Count -Descending | Select-Object -First 10 Name, Count)
            Notes            = @(
                if ($orphans.Count -gt 0) {
                    "ORPHANED_CANVAS_APPS ($($orphans.Count) canvas apps owned by disabled users - at risk of breaking when their Azure AD account is removed)"
                }
                if ($stale.Count -gt 10 -and $apps.Count -gt 0) {
                    "STALE_CANVAS_APPS ($($stale.Count) canvas apps not modified in 180+ days)"
                }
            ) | Where-Object { $_ }
        }
    }

    # ── 2. Cloud Flows (v2 — portal-side; richer than Dataverse workflows) ─────
    $cloudFlows = Invoke-MakerBAPSection `
        -SectionName 'Cloud Flows (v2)' `
        -Path "/providers/Microsoft.ProcessSimple/scopes/admin/environments/$envId/v2/flows" `
        -ApiVersion '2016-11-01' `
        -SaveFileName 'cloud-flows.json'

    if ($cloudFlows -is [hashtable] -and $cloudFlows.__Error) {
        $result.Sections['CloudFlows'] = @{
            Notes = @("CLOUD_FLOWS_$($cloudFlows.__Error.Category)")
        }
    } else {
        $flows  = @($cloudFlows)
        $active = @($flows | Where-Object { $_.properties -and $_.properties.state -eq 'Started' })
        $susp   = @($flows | Where-Object { $_.properties -and $_.properties.state -eq 'Suspended' })
        $orphanFlows = @($flows | Where-Object {
            $_.properties -and $_.properties.creator -and
            ($disabledUserIdSet.Contains([string]$_.properties.creator.userId) -or
             $disabledUserIdSet.Contains([string]$_.properties.creator.email))
        })
        $result.Sections['CloudFlows'] = @{
            TotalCount       = $flows.Count
            ActiveCount      = $active.Count
            SuspendedCount   = $susp.Count
            OrphanedCount    = $orphanFlows.Count
            Notes            = @(
                if ($orphanFlows.Count -gt 0) {
                    "CLOUD_FLOWS_OWNED_BY_DISABLED_USERS ($($orphanFlows.Count) cloud flows with disabled-user creators)"
                }
                if ($susp.Count -gt 5) {
                    "SUSPENDED_CLOUD_FLOWS ($($susp.Count) cloud flows in Suspended state - review and reactivate or delete)"
                }
            ) | Where-Object { $_ }
        }
    }

    # ── 3. Custom Connectors ──────────────────────────────────────────────────
    # The /apis endpoint returns both 1P and custom; filter to iscustomapi=true
    $connectors = Invoke-MakerBAPSection `
        -SectionName 'Custom Connectors' `
        -Path "/providers/Microsoft.PowerApps/scopes/admin/environments/$envId/apis" `
        -ApiVersion '2016-11-01' `
        -SaveFileName 'custom-connectors.json'

    if ($connectors -is [hashtable] -and $connectors.__Error) {
        $result.Sections['CustomConnectors'] = @{
            Notes = @("CUSTOM_CONNECTORS_$($connectors.__Error.Category)")
        }
    } else {
        $custom = @($connectors | Where-Object {
            $_.properties -and ($_.properties.isCustomApi -eq $true -or $_.properties.tier -eq 'Custom')
        })
        $uncertified = @($custom | Where-Object {
            -not $_.properties.publisher -or $_.properties.publisher -eq ''
        })
        $result.Sections['CustomConnectors'] = @{
            TotalCount       = $custom.Count
            UncertifiedCount = $uncertified.Count
            Notes            = @(
                if ($uncertified.Count -gt 0) {
                    "UNCERTIFIED_CUSTOM_CONNECTORS ($($uncertified.Count) custom connectors without a publisher listed)"
                }
            ) | Where-Object { $_ }
        }
    }

    # ── 4. Connections ─────────────────────────────────────────────────────────
    # Per-env connection inventory — the live bindings flows/apps depend on.
    $connections = Invoke-MakerBAPSection `
        -SectionName 'Connections' `
        -Path "/providers/Microsoft.PowerApps/scopes/admin/environments/$envId/connections" `
        -ApiVersion '2016-11-01' `
        -SaveFileName 'connections.json'

    if ($connections -is [hashtable] -and $connections.__Error) {
        $result.Sections['Connections'] = @{
            Notes = @("CONNECTIONS_$($connections.__Error.Category)")
        }
    } else {
        $conns  = @($connections)
        $errConns = @($conns | Where-Object {
            $_.properties -and $_.properties.statuses -and
            (@($_.properties.statuses) | Where-Object { $_.status -in 'Error','Broken' }).Count -gt 0
        })
        $orphanConns = @($conns | Where-Object {
            $_.properties -and $_.properties.createdBy -and
            $disabledUserIdSet.Contains([string]$_.properties.createdBy.email)
        })
        # Group by connector api
        $byApi = $conns | ForEach-Object {
            if ($_.properties -and $_.properties.apiId) {
                [PSCustomObject]@{
                    Api = ($_.properties.apiId -split '/')[-1]
                }
            }
        } | Group-Object Api | Sort-Object Count -Descending | Select-Object -First 15 Name, Count

        $result.Sections['Connections'] = @{
            TotalCount       = $conns.Count
            InErrorCount     = $errConns.Count
            OrphanedCount    = $orphanConns.Count
            TopConnectors    = @($byApi)
            Notes            = @(
                if ($errConns.Count -gt 0) {
                    "CONNECTIONS_IN_ERROR_STATE ($($errConns.Count) connections in Error/Broken state - dependent flows and apps will fail)"
                }
                if ($orphanConns.Count -gt 0) {
                    "ORPHANED_CONNECTIONS ($($orphanConns.Count) connections created by disabled users - will stop working when token expires)"
                }
            ) | Where-Object { $_ }
        }
    }

    # ── 5. Power Pages (portals) ──────────────────────────────────────────────
    # mspp_website is the Power Pages website table (formerly adx_website in portals).
    $powerPages = Invoke-MakerDVSection `
        -SectionName 'Power Pages Sites' `
        -ODataPath "mspp_websites?`$select=mspp_name,mspp_websiteid,mspp_primarydomainname,createdon,modifiedon,statecode,statuscode" `
        -SaveFileName 'power-pages.json'

    if ($powerPages -is [hashtable] -and $powerPages.__Error) {
        # NOT_FOUND / FEATURE_NOT_ENABLED here simply means no Power Pages in the env.
        $cat = $powerPages.__Error.Category
        $result.Sections['PowerPages'] = @{
            TotalCount = 0
            Notes      = @(
                if ($cat -notin 'FEATURE_NOT_ENABLED','NOT_FOUND') { "POWER_PAGES_$cat" }
            ) | Where-Object { $_ }
        }
    } else {
        $sites       = @($powerPages)
        $activeSites = @($sites | Where-Object { $_.statecode -eq 0 })
        $result.Sections['PowerPages'] = @{
            TotalCount  = $sites.Count
            ActiveCount = $activeSites.Count
            Notes       = @()
        }
    }

    # ── 6. Copilot Studio bots / PVA ──────────────────────────────────────────
    # bot entity holds Copilot Studio (Power Virtual Agents) bots. Schema varies
    # slightly across feature generations — keep $select modest.
    $bots = Invoke-MakerDVSection `
        -SectionName 'Copilot Studio Bots' `
        -ODataPath "bots?`$select=name,botid,publishedon,createdon,modifiedon,statecode,statuscode" `
        -SaveFileName 'copilots.json'

    if ($bots -is [hashtable] -and $bots.__Error) {
        $cat = $bots.__Error.Category
        $result.Sections['Copilots'] = @{
            TotalCount = 0
            Notes      = @(
                if ($cat -notin 'FEATURE_NOT_ENABLED','NOT_FOUND') { "COPILOTS_$cat" }
            ) | Where-Object { $_ }
        }
    } else {
        $botArr   = @($bots)
        $active   = @($botArr | Where-Object { $_.statecode -eq 0 })
        $stale    = @($botArr | Where-Object {
            $_.modifiedon -and ([datetime]$_.modifiedon -lt (Get-Date).AddDays(-180))
        })
        $neverPublished = @($botArr | Where-Object { -not $_.publishedon })
        $result.Sections['Copilots'] = @{
            TotalCount        = $botArr.Count
            ActiveCount       = $active.Count
            StaleCount        = $stale.Count
            NeverPublishedCount = $neverPublished.Count
            Notes             = @(
                if ($stale.Count -gt 0 -and $botArr.Count -gt 0) {
                    "STALE_COPILOTS ($($stale.Count) Copilot Studio bots not modified in 180+ days)"
                }
            ) | Where-Object { $_ }
        }
    }

    # ── 7. Dataflows ──────────────────────────────────────────────────────────
    # Dataflows surface via BAP; Dataverse also stores refresh metadata in msdyn_dataflow
    # but access varies. Try BAP first, fall back silently.
    $dataflows = Invoke-MakerBAPSection `
        -SectionName 'Dataflows' `
        -Path "/providers/Microsoft.ProcessSimple/scopes/admin/environments/$envId/dataflows" `
        -ApiVersion '2016-11-01' `
        -SaveFileName 'dataflows.json'

    if ($dataflows -is [hashtable] -and $dataflows.__Error) {
        $cat = $dataflows.__Error.Category
        $result.Sections['Dataflows'] = @{
            TotalCount = 0
            Notes      = @(
                if ($cat -notin 'FEATURE_NOT_ENABLED','NOT_FOUND') { "DATAFLOWS_$cat" }
            ) | Where-Object { $_ }
        }
    } else {
        $dfArr  = @($dataflows)
        $failing = @($dfArr | Where-Object {
            $_.properties -and $_.properties.lastRefreshStatus -eq 'Failed'
        })
        $result.Sections['Dataflows'] = @{
            TotalCount    = $dfArr.Count
            FailingCount  = $failing.Count
            Notes         = @(
                if ($failing.Count -gt 0) {
                    "DATAFLOWS_FAILING_REFRESH ($($failing.Count) dataflows with last refresh failed)"
                }
            ) | Where-Object { $_ }
        }
    }

    # ── 8. AI Builder Models ──────────────────────────────────────────────────
    $aiModels = Invoke-MakerDVSection `
        -SectionName 'AI Builder Models' `
        -ODataPath "msdyn_aimodels?`$select=msdyn_name,msdyn_aimodelid,msdyn_publishedonid,statecode,statuscode,createdon,modifiedon" `
        -SaveFileName 'ai-models.json'

    if ($aiModels -is [hashtable] -and $aiModels.__Error) {
        $cat = $aiModels.__Error.Category
        $result.Sections['AIModels'] = @{
            TotalCount = 0
            Notes      = @(
                if ($cat -notin 'FEATURE_NOT_ENABLED','NOT_FOUND') { "AI_MODELS_$cat" }
            ) | Where-Object { $_ }
        }
    } else {
        $mArr      = @($aiModels)
        $published = @($mArr | Where-Object { $_.msdyn_publishedonid })
        $result.Sections['AIModels'] = @{
            TotalCount     = $mArr.Count
            PublishedCount = $published.Count
            Notes          = @()
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

    Save-EnvironmentData -EnvironmentDir $EnvOutputDir -FileName 'maker-summary.json' -Data $result
    Write-InventoryLog "  Maker inventory complete. Flags: $($result.AllFlags.Count)" -Level OK -Indent 1

    return $result
}
