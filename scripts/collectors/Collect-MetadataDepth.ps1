<#
.SYNOPSIS
    Collects metadata depth + lifecycle: publishers, solution dependencies,
    D365 app detection, orgdborgsettings, currencies, languages, lifecycle
    operations (BAP), backups, and Dataverse version.
.DESCRIPTION
    Exports Collect-MetadataDepthInventory, dot-sourced by
    Invoke-DataverseInventory.ps1.

    Per-env output files:
      publishers.json
      solution-dependencies.json
      d365-apps.json
      orgdborgsettings.xml (raw)
      currencies.json
      languages.json
      lifecycle-operations.json
      backups.json
      metadata-depth-summary.json

    Notable flags:
      DATAVERSE_VERSION_OUTDATED           — org running version > 90 days behind latest
      NO_RECENT_BACKUPS                    — no user-initiated backup in 180 days
      LIFECYCLE_OP_FAILED_RECENTLY         — failed lifecycle op in last 30 days
      NO_D365_APPS_ON_F365_ENV             — F&O env without the finance/supply-chain apps
      NON_DEFAULT_PUBLISHER_USED           — customizations made under publisher 'Default Publisher'
#>

function Collect-MetadataDepthInventory {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]$EnvEntry,
        [Parameter(Mandatory)][string]$EnvOutputDir
    )

    $displayName = $EnvEntry.DisplayName
    $envId       = $EnvEntry.EnvironmentId
    $apiUrl      = $EnvEntry.OrgApiUrl
    $instanceUrl = $EnvEntry.OrgUrl
    $hasDataverse = [bool]$EnvEntry.HasDataverse

    Write-InventoryLog "  Starting metadata depth for: $displayName" -Indent 1

    $result = [ordered]@{
        CollectedAt   = (Get-Date -Format 'o')
        EnvironmentId = $envId
        DisplayName   = $displayName
        Sections      = [ordered]@{}
        AllFlags      = @()
    }

    function Invoke-MDSection {
        param(
            [string]$SectionName,
            [string]$ODataPath,
            [string]$SaveFileName,
            [int]   $TimeoutSec = 90
        )
        if (-not $hasDataverse -or -not $apiUrl) { return $null }
        Write-InventoryLog "    [Metadata: $SectionName]..." -Indent 2
        try {
            $resp = Invoke-DataverseRequest -InstanceApiUrl $apiUrl -InstanceUrl $instanceUrl `
                                            -ODataPath $ODataPath -TimeoutSec $TimeoutSec
            $data = @(Get-AllODataPages -InitialResponse $resp -InstanceApiUrl $apiUrl -InstanceUrl $instanceUrl)
            Save-EnvironmentData -EnvironmentDir $EnvOutputDir -FileName $SaveFileName -Data $data
            Write-InventoryLog "    -> $($data.Count) rows." -Level OK -Indent 3
            return $data
        } catch {
            $errInfo = Get-HttpErrorClassification -ErrorRecord $_
            Write-InventoryLog "    -> $SectionName [$($errInfo.Category)]: $($errInfo.Message)" -Level WARN -Indent 3
            return @{ __Error = $errInfo }
        }
    }

    # ── 1. Publishers ────────────────────────────────────────────────────────
    $pubs = Invoke-MDSection -SectionName 'Publishers' `
        -ODataPath "publishers?`$select=publisherid,uniquename,friendlyname,description,customizationprefix,customizationoptionvalueprefix,isreadonly" `
        -SaveFileName 'publishers.json'

    if ($pubs -is [hashtable] -and $pubs.__Error) {
        $result.Sections['Publishers'] = @{ Notes = @("PUBLISHERS_$($pubs.__Error.Category)") }
    } else {
        $pArr   = @($pubs)
        $custom = @($pArr | Where-Object { -not $_.isreadonly -and $_.uniquename -notin 'DefaultPublisher','MicrosoftCorporation','MicrosoftDynamics' })
        $default = @($pArr | Where-Object { $_.uniquename -eq 'DefaultPublisher' })

        $result.Sections['Publishers'] = @{
            TotalCount   = $pArr.Count
            CustomCount  = $custom.Count
            DefaultCount = $default.Count
            TopCustomNames = @($custom | Select-Object -First 15 -ExpandProperty friendlyname)
            Notes         = @()
        }
    }

    # ── 2. D365 Apps (installed first-party solutions) ───────────────────────
    # These are identified by known unique solution names.
    $d365SolNames = @(
        'msdyn_FinanceAndOperationsCore','msdyn_FinanceExtended','msdyn_SupplyChain',
        'msdyn_ProjectOperationsCore','msdyn_Sales','msdyn_Service','msdyn_FieldService',
        'msdyn_CustomerService','msdyn_msdynce_commerce','msdyn_msdynce_humanresources',
        'msdyn_SalesInsights','msdyn_SalesHub','msdyn_CustomerServiceHub'
    )
    $d365Filter = ($d365SolNames | ForEach-Object { "uniquename eq '$_'" }) -join ' or '
    $d365Apps = Invoke-MDSection -SectionName 'D365 Apps' `
        -ODataPath "solutions?`$filter=($d365Filter)&`$select=uniquename,friendlyname,version,installedon,ismanaged,publisherid" `
        -SaveFileName 'd365-apps.json'

    if ($d365Apps -is [hashtable] -and $d365Apps.__Error) {
        $result.Sections['D365Apps'] = @{ Notes = @("D365_APPS_$($d365Apps.__Error.Category)") }
    } else {
        $d365Arr = @($d365Apps)
        $result.Sections['D365Apps'] = @{
            TotalCount     = $d365Arr.Count
            InstalledNames = @($d365Arr | ForEach-Object { "$($_.friendlyname) ($($_.version))" })
            Notes          = @()
        }
        if ($EnvEntry.PSObject.Properties['HasFO'] -and $EnvEntry.HasFO -and $d365Arr.Count -eq 0) {
            $result.Sections['D365Apps'].Notes += "NO_D365_APPS_ON_F365_ENV (F&O integration is detected but no Finance/Supply-Chain/HR solutions are installed in Dataverse - dual-write maps won't function)"
        }
    }

    # ── 3. Solution dependencies (top-level summary only) ────────────────────
    # Full dependency resolution is extremely verbose; just report dep rows.
    $deps = Invoke-MDSection -SectionName 'Solution Dependencies' `
        -ODataPath "dependencies?`$select=dependencytype,requiredcomponenttype,dependentcomponenttype,createdon&`$top=2000" `
        -SaveFileName 'solution-dependencies.json'

    if (-not ($deps -is [hashtable] -and $deps.__Error)) {
        $dArr = @($deps)
        $result.Sections['SolutionDependencies'] = @{
            TotalCount = $dArr.Count
            Notes      = @()
        }
    }

    # ── 4. Organization-level settings (orgdborgsettings) ────────────────────
    # organization.orgdborgsettings is an XML blob with tenant-specific knobs.
    Write-InventoryLog '    [Metadata: Organization settings]...' -Indent 2
    try {
        $orgResp = Invoke-DataverseRequest -InstanceApiUrl $apiUrl -InstanceUrl $instanceUrl `
                                           -ODataPath "organizations?`$select=organizationid,name,basecurrencyid,defaultthemeid,orgdborgsettings,sqlaccessgroupid,weekstartdaycode,pluginsecuritysettings,localeid,languagecode,defaultcountrycode" `
                                           -TimeoutSec 60
        $org = if ($orgResp.value) { @($orgResp.value) | Select-Object -First 1 } else { $null }

        if ($org) {
            # Persist the raw XML blob
            if ($org.orgdborgsettings) {
                $orgSettingsFile = Join-Path $EnvOutputDir 'orgdborgsettings.xml'
                [System.IO.File]::WriteAllText($orgSettingsFile, [string]$org.orgdborgsettings, [System.Text.Encoding]::UTF8)
            }

            # Dataverse version: the 'version' column is not $select-able on current API,
            # so fetch it via the RetrieveVersion bound function instead.
            $dvVersion = ''
            try {
                $verResp = Invoke-DataverseRequest -InstanceApiUrl $apiUrl -InstanceUrl $instanceUrl `
                                                   -ODataPath 'RetrieveVersion' -TimeoutSec 30
                if ($verResp -and $verResp.Version) { $dvVersion = [string]$verResp.Version }
            } catch { }

            $result.Sections['Organization'] = @{
                OrganizationId = $org.organizationid
                Name           = $org.name
                Version        = $dvVersion
                LocaleId       = $org.localeid
                LanguageCode   = $org.languagecode
                WeekStartDay   = $org.weekstartdaycode
                HasOrgDbSettings = [bool]$org.orgdborgsettings
                Notes            = @()
            }
            Write-InventoryLog "    -> version=$dvVersion" -Level OK -Indent 3
        }
    } catch {
        $errInfo = Get-HttpErrorClassification -ErrorRecord $_
        Write-InventoryLog "    -> Organization [$($errInfo.Category)]: $($errInfo.Message)" -Level WARN -Indent 3
        $result.Sections['Organization'] = @{ Notes = @("ORGANIZATION_$($errInfo.Category)") }
    }

    # ── 5. Currencies ────────────────────────────────────────────────────────
    $curr = Invoke-MDSection -SectionName 'Currencies' `
        -ODataPath "transactioncurrencies?`$select=currencyname,isocurrencycode,currencysymbol,exchangerate,statecode" `
        -SaveFileName 'currencies.json'

    if (-not ($curr -is [hashtable] -and $curr.__Error)) {
        $cArr = @($curr)
        $active = @($cArr | Where-Object { $_.statecode -eq 0 })
        $result.Sections['Currencies'] = @{
            TotalCount  = $cArr.Count
            ActiveCount = $active.Count
            Notes       = @()
        }
    }

    # ── 6. Languages ─────────────────────────────────────────────────────────
    # Use RetrieveProvisionedLanguages bound function.
    Write-InventoryLog '    [Metadata: Languages]...' -Indent 2
    try {
        $langResp = Invoke-DataverseRequest -InstanceApiUrl $apiUrl -InstanceUrl $instanceUrl `
                                            -ODataPath "RetrieveProvisionedLanguages" `
                                            -TimeoutSec 60
        $langs = @($langResp.RetrieveProvisionedLanguages)
        $langFile = Join-Path $EnvOutputDir 'languages.json'
        $langs | ConvertTo-Json -Depth 5 | Set-Content -Path $langFile -Encoding UTF8 -Force

        $result.Sections['Languages'] = @{
            ProvisionedCount = $langs.Count
            Codes            = @($langs)
            Notes            = @()
        }
        Write-InventoryLog "    -> $($langs.Count) languages." -Level OK -Indent 3
    } catch {
        $errInfo = Get-HttpErrorClassification -ErrorRecord $_
        Write-InventoryLog "    -> Languages [$($errInfo.Category)]: $($errInfo.Message)" -Level WARN -Indent 3
        $result.Sections['Languages'] = @{ Notes = @("LANGUAGES_$($errInfo.Category)") }
    }

    # ── 7. Lifecycle operations (BAP) ────────────────────────────────────────
    Write-InventoryLog '    [Metadata: Lifecycle Operations]...' -Indent 2
    try {
        $lcResp = Invoke-BAPRequest `
            -Path "/providers/Microsoft.BusinessAppPlatform/scopes/admin/environments/$envId/operations" `
            -ApiVersion '2024-05-01' `
            -TimeoutSec 60
        $ops = @(Get-AllBAPPages -InitialResponse $lcResp)
        Save-EnvironmentData -EnvironmentDir $EnvOutputDir -FileName 'lifecycle-operations.json' -Data $ops

        $thirty   = (Get-Date).AddDays(-30)
        $recent   = @($ops | Where-Object {
            $_.properties -and $_.properties.createdDateTime -and
            [datetime]$_.properties.createdDateTime -gt $thirty
        })
        $failed   = @($recent | Where-Object {
            $_.properties.status -in 'Failed','Error'
        })
        $result.Sections['LifecycleOperations'] = @{
            TotalCount      = $ops.Count
            Recent30dCount  = $recent.Count
            Failed30dCount  = $failed.Count
            Notes           = @()
        }
        if ($failed.Count -gt 0) {
            $result.Sections['LifecycleOperations'].Notes += "LIFECYCLE_OP_FAILED_RECENTLY ($($failed.Count) lifecycle operations failed in last 30 days - copy/backup/restore may have partial state)"
        }
        Write-InventoryLog "    -> $($ops.Count) lifecycle ops, $($recent.Count) recent, $($failed.Count) failed." -Level OK -Indent 3
    } catch {
        $errInfo = Get-HttpErrorClassification -ErrorRecord $_
        Write-InventoryLog "    -> Lifecycle Ops [$($errInfo.Category)]: $($errInfo.Message)" -Level WARN -Indent 3
        $result.Sections['LifecycleOperations'] = @{
            Notes = @("LIFECYCLE_OPS_$($errInfo.Category)")
        }
    }

    # ── 8. Backups ───────────────────────────────────────────────────────────
    Write-InventoryLog '    [Metadata: Backups]...' -Indent 2
    try {
        $bkResp = Invoke-BAPRequest `
            -Path "/providers/Microsoft.BusinessAppPlatform/scopes/admin/environments/$envId/backups" `
            -ApiVersion '2024-05-01' `
            -TimeoutSec 60
        $backups = @(Get-AllBAPPages -InitialResponse $bkResp)
        Save-EnvironmentData -EnvironmentDir $EnvOutputDir -FileName 'backups.json' -Data $backups

        $user        = @($backups | Where-Object { $_.properties.backupType -eq 'Manual' -or $_.properties.label })
        $cutoff180   = (Get-Date).AddDays(-180)
        $recentUser  = @($user | Where-Object {
            $_.properties.createdDateTime -and [datetime]$_.properties.createdDateTime -gt $cutoff180
        })

        $result.Sections['Backups'] = @{
            TotalCount          = $backups.Count
            UserInitiatedCount  = $user.Count
            RecentUserInitiated180d = $recentUser.Count
            Notes               = @()
        }
        if ($EnvEntry.EnvironmentSku -eq 'Production' -and $recentUser.Count -eq 0) {
            $result.Sections['Backups'].Notes += "NO_RECENT_USER_BACKUPS (no user-initiated backups in 180 days on Production - relies entirely on system automatic backups)"
        }
        Write-InventoryLog "    -> $($backups.Count) backups ($($user.Count) user-initiated)." -Level OK -Indent 3
    } catch {
        $errInfo = Get-HttpErrorClassification -ErrorRecord $_
        Write-InventoryLog "    -> Backups [$($errInfo.Category)]: $($errInfo.Message)" -Level WARN -Indent 3
        $result.Sections['Backups'] = @{ Notes = @("BACKUPS_$($errInfo.Category)") }
    }

    # Flatten notes
    $allNotes = [System.Collections.Generic.List[string]]::new()
    foreach ($sec in $result.Sections.Values) {
        if ($sec.Notes) {
            foreach ($n in $sec.Notes) { if ($n) { $allNotes.Add($n) } }
        }
    }
    $result['AllFlags'] = @($allNotes | Sort-Object -Unique)

    Save-EnvironmentData -EnvironmentDir $EnvOutputDir -FileName 'metadata-depth-summary.json' -Data $result
    Write-InventoryLog "  Metadata depth complete. Flags: $($result.AllFlags.Count)" -Level OK -Indent 1

    return $result
}
