<#
.SYNOPSIS
    Collects tenant-level Power Platform governance posture (DLP policies,
    tenant settings, tenant isolation, environment creation requests) and
    per-environment governance configuration (managed env settings, weekly
    digest, solution checker).
.DESCRIPTION
    Exports two functions, dot-sourced by Invoke-DataverseInventory.ps1:

      Collect-TenantGovernance         — runs once per tenant. Writes:
                                            tenant/dlp-policies.json
                                            tenant/tenant-settings.json
                                            tenant/tenant-isolation.json
                                            tenant/environment-creation.json
                                            tenant/governance-summary.json
      Collect-EnvironmentGovernance    — runs per environment. Writes:
                                            governance.json
                                            governance-summary.json

    Flags surfaced here are specifically those that a Power Platform
    Administrator or tenant governance lead should own: DLP missing on prod,
    HTTP connector allowed, weak tenant defaults, tenant isolation off.

.NOTES
    Endpoints used:
      - BAP (api.bap.microsoft.com):
          /providers/Microsoft.BusinessAppPlatform/scopes/admin/apiPolicies        (DLP policies)
          /providers/Microsoft.BusinessAppPlatform/listTenantSettings              (tenant settings)
          /providers/Microsoft.BusinessAppPlatform/scopes/admin/tenantIsolationPolicies
          /providers/Microsoft.BusinessAppPlatform/scopes/admin/environmentRequests
          /providers/Microsoft.BusinessAppPlatform/scopes/admin/environments/{id}  (env details, richer than the list API)
#>

function Collect-TenantGovernance {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$OutputPath,
        [Parameter(Mandatory)][string]$TenantId
    )

    Write-InventoryLog ''
    Write-InventoryLog 'Collecting tenant-level governance posture...'

    $tenantDir = Join-Path $OutputPath 'tenant'
    $null = New-Item -ItemType Directory -Path $tenantDir -Force

    $result = [ordered]@{
        CollectedAt = (Get-Date -Format 'o')
        TenantId    = $TenantId
        Sections    = [ordered]@{}
        AllFlags    = @()
    }

    # ── 1. DLP policies (apiPolicies) ────────────────────────────────────────
    Write-InventoryLog '  [Tenant: DLP policies]...' -Indent 1
    try {
        $resp = Invoke-BAPRequest `
            -Path '/providers/Microsoft.BusinessAppPlatform/scopes/admin/apiPolicies' `
            -ApiVersion '2018-01-01' `
            -TimeoutSec 60
        $policies = @(Get-AllBAPPages -InitialResponse $resp)
        $policyFile = Join-Path $tenantDir 'dlp-policies.json'
        $policies | ConvertTo-Json -Depth 15 | Set-Content -Path $policyFile -Encoding UTF8 -Force

        $result.Sections['DlpPolicies'] = @{
            TotalCount     = $policies.Count
            Policies       = @($policies | ForEach-Object {
                $props = $_.properties
                # Identify the environments this policy applies to
                $scope = 'Unknown'
                $envIncluded = @()
                $envExcluded = @()
                if ($props.environments -and $props.environments.Count -gt 0) {
                    $scope = 'Specific'
                    $envIncluded = @($props.environments | ForEach-Object { $_.name })
                } elseif ($props.defaultConnectorsClassificationOverrides) {
                    $scope = 'AllEnvironments'
                }
                if ($props.environmentType -eq 'AllEnvironments') { $scope = 'AllEnvironments' }
                if ($props.environmentType -eq 'ExceptEnvironments' -and $props.environments) {
                    $scope = 'AllExcept'
                    $envExcluded = @($props.environments | ForEach-Object { $_.name })
                }

                # Get connector groups
                $business     = @()
                $nonBusiness  = @()
                $blocked      = @()
                foreach ($g in @($props.connectorGroups)) {
                    switch ($g.classification) {
                        'Confidential'   { $business    += @($g.connectors | ForEach-Object { $_.id }) }
                        'General'        { $nonBusiness += @($g.connectors | ForEach-Object { $_.id }) }
                        'Blocked'        { $blocked     += @($g.connectors | ForEach-Object { $_.id }) }
                    }
                }
                @{
                    Id                = $_.name
                    DisplayName       = $props.displayName
                    CreatedBy         = $props.createdBy.userPrincipalName
                    Scope             = $scope
                    EnvironmentsIncluded = $envIncluded
                    EnvironmentsExcluded = $envExcluded
                    BusinessCount     = $business.Count
                    NonBusinessCount  = $nonBusiness.Count
                    BlockedCount      = $blocked.Count
                    HttpInBusiness    = ($business    -match 'shared_webcontents|shared_uiflow|shared_http') -ne $false
                    HttpAllowedInNonBusiness = ($nonBusiness -match 'shared_webcontents|shared_http|shared_uiflow').Count -gt 0
                }
            })
            Notes          = @()
        }

        if ($policies.Count -eq 0) {
            $result.Sections['DlpPolicies'].Notes += "NO_DLP_POLICIES_CONFIGURED (no DLP policies exist in the tenant - makers can use any connector combination without restriction)"
        }
        Write-InventoryLog "    -> $($policies.Count) DLP policies." -Level OK -Indent 2
    } catch {
        $errInfo = Get-HttpErrorClassification -ErrorRecord $_
        Write-InventoryLog "    -> DLP query FAILED [$($errInfo.Category)]: $($errInfo.Message)" -Level WARN -Indent 2
        $result.Sections['DlpPolicies'] = @{
            TotalCount = -1
            Notes      = @("DLP_POLICIES_$($errInfo.Category)")
        }
    }

    # ── 2. Tenant settings ───────────────────────────────────────────────────
    Write-InventoryLog '  [Tenant: tenant settings]...' -Indent 1
    try {
        $ts = Invoke-BAPRequest `
            -Path '/providers/Microsoft.BusinessAppPlatform/listTenantSettings' `
            -Method 'POST' `
            -Body '{}' `
            -ApiVersion '2020-10-01' `
            -TimeoutSec 60
        $tsFile = Join-Path $tenantDir 'tenant-settings.json'
        $ts | ConvertTo-Json -Depth 15 | Set-Content -Path $tsFile -Encoding UTF8 -Force

        # Key governance signals
        $makerOnboarding = $null
        try { $makerOnboarding = $ts.powerPlatform.powerApps.disableShareWithEveryone } catch {}
        $guestMakers = $null
        try { $guestMakers = $ts.powerPlatform.powerApps.disableMakerMatch } catch {}
        $tenantIsolDefault = $null
        try { $tenantIsolDefault = $ts.powerPlatform.governance.disableCopilot } catch {}
        $envCreation = $null
        try { $envCreation = $ts.powerPlatform.governance.disableEnvironmentCreationByNonAdminUsers } catch {}
        $envTrial = $null
        try { $envTrial = $ts.powerPlatform.governance.disableTrialEnvironmentCreationByNonAdminUsers } catch {}

        $result.Sections['TenantSettings'] = @{
            DisableShareWithEveryone                      = $makerOnboarding
            DisableMakerMatch                             = $guestMakers
            DisableEnvironmentCreationByNonAdmin          = $envCreation
            DisableTrialEnvironmentCreationByNonAdmin     = $envTrial
            DisableCopilot                                = $tenantIsolDefault
            Notes                                         = @()
        }

        if ($envCreation -eq $false) {
            $result.Sections['TenantSettings'].Notes += "NON_ADMIN_CAN_CREATE_ENVIRONMENTS (tenant allows any user to create production/sandbox environments - expands governance surface)"
        }
        if ($envTrial -eq $false) {
            $result.Sections['TenantSettings'].Notes += "NON_ADMIN_CAN_CREATE_TRIAL_ENVIRONMENTS (tenant allows any user to spin up trial environments)"
        }
        Write-InventoryLog '    -> tenant settings captured.' -Level OK -Indent 2
    } catch {
        $errInfo = Get-HttpErrorClassification -ErrorRecord $_
        Write-InventoryLog "    -> Tenant settings FAILED [$($errInfo.Category)]: $($errInfo.Message)" -Level WARN -Indent 2
        $result.Sections['TenantSettings'] = @{
            Notes = @("TENANT_SETTINGS_$($errInfo.Category)")
        }
    }

    # ── 3. Tenant isolation ──────────────────────────────────────────────────
    Write-InventoryLog '  [Tenant: tenant isolation]...' -Indent 1
    try {
        $tiResp = Invoke-BAPRequest `
            -Path "/providers/Microsoft.BusinessAppPlatform/scopes/admin/tenantIsolationPolicies/$TenantId" `
            -ApiVersion '2020-10-01' `
            -TimeoutSec 60
        $tiFile = Join-Path $tenantDir 'tenant-isolation.json'
        $tiResp | ConvertTo-Json -Depth 10 | Set-Content -Path $tiFile -Encoding UTF8 -Force

        $enabled    = $null
        $allowInbound  = @()
        $allowOutbound = @()
        try { $enabled = [bool]$tiResp.properties.isDisabled -eq $false } catch {}
        try {
            foreach ($r in @($tiResp.properties.allowedTenants)) {
                if ($r.direction -eq 'Inbound')  { $allowInbound  += $r.tenantId }
                if ($r.direction -eq 'Outbound') { $allowOutbound += $r.tenantId }
                if ($r.direction -eq 'Both')     { $allowInbound  += $r.tenantId; $allowOutbound += $r.tenantId }
            }
        } catch {}

        $result.Sections['TenantIsolation'] = @{
            Enabled         = $enabled
            AllowedInbound  = $allowInbound
            AllowedOutbound = $allowOutbound
            Notes           = @()
        }
        if ($enabled -ne $true) {
            $result.Sections['TenantIsolation'].Notes += "TENANT_ISOLATION_DISABLED (cross-tenant connections are not blocked - data can flow to/from other Microsoft 365 tenants)"
        }
        Write-InventoryLog "    -> enabled=$enabled; inbound=$($allowInbound.Count); outbound=$($allowOutbound.Count)." -Level OK -Indent 2
    } catch {
        $errInfo = Get-HttpErrorClassification -ErrorRecord $_
        Write-InventoryLog "    -> Tenant isolation FAILED [$($errInfo.Category)]: $($errInfo.Message)" -Level WARN -Indent 2
        $result.Sections['TenantIsolation'] = @{
            Notes = @("TENANT_ISOLATION_$($errInfo.Category)")
        }
    }

    # ── 4. Environment creation requests ─────────────────────────────────────
    Write-InventoryLog '  [Tenant: environment creation requests]...' -Indent 1
    try {
        $reqResp = Invoke-BAPRequest `
            -Path '/providers/Microsoft.BusinessAppPlatform/scopes/admin/environmentRequests' `
            -ApiVersion '2020-10-01' `
            -TimeoutSec 60
        $requests = @(Get-AllBAPPages -InitialResponse $reqResp)
        $reqFile = Join-Path $tenantDir 'environment-creation.json'
        $requests | ConvertTo-Json -Depth 10 | Set-Content -Path $reqFile -Encoding UTF8 -Force

        $pending = @($requests | Where-Object { $_.properties.status -eq 'Pending' })
        $result.Sections['EnvironmentRequests'] = @{
            TotalCount   = $requests.Count
            PendingCount = $pending.Count
            Notes        = @()
        }
        if ($pending.Count -gt 10) {
            $result.Sections['EnvironmentRequests'].Notes += "MANY_PENDING_ENVIRONMENT_REQUESTS ($($pending.Count) environment creation requests awaiting admin review)"
        }
        Write-InventoryLog "    -> $($requests.Count) env requests ($($pending.Count) pending)." -Level OK -Indent 2
    } catch {
        $errInfo = Get-HttpErrorClassification -ErrorRecord $_
        # This endpoint is often empty/disabled in tenants without environment gating - downgrade to INFO.
        Write-InventoryLog "    -> Env requests [$($errInfo.Category)]: not available." -Level INFO -Indent 2
        $result.Sections['EnvironmentRequests'] = @{
            TotalCount = 0
            Notes      = @()
        }
    }

    # ── Flatten notes into AllFlags ───────────────────────────────────────────
    $allNotes = [System.Collections.Generic.List[string]]::new()
    foreach ($sec in $result.Sections.Values) {
        if ($sec.Notes) {
            foreach ($n in $sec.Notes) { if ($n) { $allNotes.Add($n) } }
        }
    }
    $result['AllFlags'] = @($allNotes | Sort-Object -Unique)

    $summaryFile = Join-Path $tenantDir 'governance-summary.json'
    $result | ConvertTo-Json -Depth 15 | Set-Content -Path $summaryFile -Encoding UTF8 -Force
    Write-InventoryLog "  Tenant governance summary saved: $summaryFile" -Level OK -Indent 1

    return $result
}


function Collect-EnvironmentGovernance {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]$EnvEntry,
        [Parameter(Mandatory)][string]$EnvOutputDir
    )

    $displayName = $EnvEntry.DisplayName
    $envId       = $EnvEntry.EnvironmentId
    $sku         = $EnvEntry.EnvironmentSku

    Write-InventoryLog "  Starting governance depth for: $displayName" -Indent 1

    $result = [ordered]@{
        CollectedAt   = (Get-Date -Format 'o')
        EnvironmentId = $envId
        DisplayName   = $displayName
        Sections      = [ordered]@{}
        AllFlags      = @()
    }

    # Full env blob includes governanceConfiguration. The env list returned
    # a truncated view so we re-fetch here.
    try {
        $envDetail = Invoke-BAPRequest `
            -Path "/providers/Microsoft.BusinessAppPlatform/scopes/admin/environments/$envId" `
            -ApiVersion '2024-05-01' `
            -TimeoutSec 60

        Save-EnvironmentData -EnvironmentDir $EnvOutputDir -FileName 'governance.json' -Data $envDetail

        $props       = $envDetail.properties
        $govConf     = $props.governanceConfiguration
        $protLevel   = $govConf.protectionLevel
        $settings    = $govConf.settings
        $makerOnbd   = $settings.extendedSettings.makerOnboarding
        $weeklyDig   = $settings.extendedSettings.weeklyReportSettings
        $slnChecker  = $settings.extendedSettings.solutionCheckerEnforcement
        $sharing     = $settings.extendedSettings.sharingControls
        $usageIns    = $settings.extendedSettings.usageInsights

        $result.Sections['ManagedEnvironment'] = @{
            ProtectionLevel        = $protLevel
            IsManagedEnvironment   = ($protLevel -eq 'Protected')
            MakerOnboardingEnabled = [bool]$makerOnbd
            WeeklyDigestEnabled    = [bool]$weeklyDig
            SolutionCheckerMode    = $slnChecker   # None, Warn, Block
            SharingLimitEnabled    = [bool]$sharing
            UsageInsightsEnabled   = [bool]$usageIns
            Notes                  = @()
        }

        # Managed-env features only exist when protectionLevel=Protected
        if ($protLevel -eq 'Protected') {
            if (-not $weeklyDig) {
                $result.Sections['ManagedEnvironment'].Notes += "MANAGED_ENV_WEEKLY_DIGEST_DISABLED (Managed Env weekly digest turned off - admins miss automated governance summaries)"
            }
            if ($slnChecker -ne 'Block' -and $slnChecker -ne 'Warn') {
                $result.Sections['ManagedEnvironment'].Notes += "MANAGED_ENV_SOLUTION_CHECKER_OFF (solution checker enforcement disabled - risky patterns can ship unreviewed)"
            }
            if ($sku -eq 'Production' -and -not $sharing) {
                $result.Sections['ManagedEnvironment'].Notes += "MANAGED_ENV_SHARING_UNLIMITED (no maker sharing controls in place - apps can be shared with entire tenant)"
            }
        }

        Write-InventoryLog "    -> protection=$protLevel; weeklyDig=$([bool]$weeklyDig); solnChk=$slnChecker" -Level OK -Indent 2
    } catch {
        $errInfo = Get-HttpErrorClassification -ErrorRecord $_
        Write-InventoryLog "    -> governance detail FAILED [$($errInfo.Category)]: $($errInfo.Message)" -Level WARN -Indent 2
        $result.Sections['ManagedEnvironment'] = @{
            Notes = @("GOVERNANCE_$($errInfo.Category)")
        }
    }

    # Flatten notes
    $allNotes = [System.Collections.Generic.List[string]]::new()
    foreach ($sec in $result.Sections.Values) {
        if ($sec.Notes) {
            foreach ($n in $sec.Notes) { if ($n) { $allNotes.Add($n) } }
        }
    }
    $result['AllFlags'] = @($allNotes | Sort-Object -Unique)

    Save-EnvironmentData -EnvironmentDir $EnvOutputDir -FileName 'governance-summary.json' -Data $result
    Write-InventoryLog "  Governance depth complete. Flags: $($result.AllFlags.Count)" -Level OK -Indent 1

    return $result
}
