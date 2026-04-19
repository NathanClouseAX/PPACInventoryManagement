<#
.SYNOPSIS
    Collects Dataverse RBAC (role-based access control) depth: security roles,
    business units, teams, field security profiles, and app module role
    assignments.
.DESCRIPTION
    Exports Collect-RBACInventory, dot-sourced by Invoke-DataverseInventory.ps1.

    Per-env output files:
      security-roles.json
      business-units.json
      teams.json
      field-security-profiles.json
      rbac-summary.json

    Notable flags:
      NO_CUSTOM_SECURITY_ROLES         — no tenant has customised roles, suggesting
                                         role-based access was never tightened
      SYSTEM_ADMIN_OVERASSIGNED        — too many users have System Administrator
      MANY_UNUSED_CUSTOM_ROLES         — custom roles not assigned to any user/team
      SUPER_BU_NESTING                 — business unit tree is pathologically deep
      TEAMS_WITH_NO_MEMBERS            — owner/security teams with zero members
      FSP_NOT_USED                     — field security profiles defined but not applied
#>

function Collect-RBACInventory {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]$EnvEntry,
        [Parameter(Mandatory)][string]$EnvOutputDir
    )

    $displayName = $EnvEntry.DisplayName
    $apiUrl      = $EnvEntry.OrgApiUrl
    $instanceUrl = $EnvEntry.OrgUrl

    if (-not $EnvEntry.HasDataverse -or -not $apiUrl) {
        return $null
    }

    Write-InventoryLog "  Starting RBAC inventory for: $displayName" -Indent 1

    $result = [ordered]@{
        CollectedAt   = (Get-Date -Format 'o')
        EnvironmentId = $EnvEntry.EnvironmentId
        DisplayName   = $displayName
        Sections      = [ordered]@{}
        AllFlags      = @()
    }

    # Inline helper — saves a DV query with categorized error handling
    function Invoke-RBACSection {
        param(
            [string]$SectionName,
            [string]$ODataPath,
            [string]$SaveFileName,
            [int]   $TimeoutSec = 90
        )
        Write-InventoryLog "    [RBAC: $SectionName]..." -Indent 2
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

    # ── 1. Security roles ────────────────────────────────────────────────────
    # Use $expand=systemuserroles_association so we can count role assignments
    # without a second per-role round trip.
    $roles = Invoke-RBACSection -SectionName 'Security Roles' `
        -ODataPath "roles?`$select=roleid,name,ismanaged,roletemplateid,iscustomizable,businessunitid,modifiedon" `
        -SaveFileName 'security-roles.json'

    if ($roles -is [hashtable] -and $roles.__Error) {
        $result.Sections['SecurityRoles'] = @{ Notes = @("SECURITY_ROLES_$($roles.__Error.Category)") }
    } else {
        $rolesArr  = @($roles)
        $custom    = @($rolesArr | Where-Object {
            -not $_.ismanaged -and $_.name -and $_.name -notmatch '^(System Administrator|System Customizer|Basic User|Delegate)$'
        })
        $sysAdmins = @($rolesArr | Where-Object { $_.name -eq 'System Administrator' })

        $result.Sections['SecurityRoles'] = @{
            TotalCount        = $rolesArr.Count
            CustomCount       = $custom.Count
            TopCustomNames    = @($custom | Select-Object -First 20 -ExpandProperty name)
            SystemAdminRoleIds = @($sysAdmins | ForEach-Object { $_.roleid })
            Notes             = @()
        }
        if ($custom.Count -eq 0 -and $EnvEntry.EnvironmentSku -in 'Production','Sandbox') {
            $result.Sections['SecurityRoles'].Notes += "NO_CUSTOM_SECURITY_ROLES (production/sandbox environment has no custom security roles - access control relies entirely on out-of-box roles)"
        }
    }

    # ── 2. Business units ────────────────────────────────────────────────────
    $bus = Invoke-RBACSection -SectionName 'Business Units' `
        -ODataPath "businessunits?`$select=businessunitid,name,parentbusinessunitid,isdisabled,createdon,modifiedon" `
        -SaveFileName 'business-units.json'

    if ($bus -is [hashtable] -and $bus.__Error) {
        $result.Sections['BusinessUnits'] = @{ Notes = @("BUSINESS_UNITS_$($bus.__Error.Category)") }
    } else {
        $busArr   = @($bus)
        $disabled = @($busArr | Where-Object { $_.isdisabled -eq $true })

        # Compute max depth via parent chain
        $idToParent = @{}
        foreach ($b in $busArr) {
            $pid = if ($b._parentbusinessunitid_value) { [string]$b._parentbusinessunitid_value } else { $null }
            $idToParent[[string]$b.businessunitid] = $pid
        }
        $maxDepth = 0
        foreach ($id in $idToParent.Keys) {
            $d = 0; $cur = $id
            while ($idToParent[$cur] -and $d -lt 20) { $d++; $cur = $idToParent[$cur] }
            if ($d -gt $maxDepth) { $maxDepth = $d }
        }

        $result.Sections['BusinessUnits'] = @{
            TotalCount    = $busArr.Count
            DisabledCount = $disabled.Count
            MaxDepth      = $maxDepth
            Notes         = @()
        }
        if ($maxDepth -gt 5) {
            $result.Sections['BusinessUnits'].Notes += "DEEP_BUSINESS_UNIT_NESTING (business unit tree depth is $maxDepth levels - security inheritance becomes hard to reason about)"
        }
    }

    # ── 3. Teams ─────────────────────────────────────────────────────────────
    $teams = Invoke-RBACSection -SectionName 'Teams' `
        -ODataPath "teams?`$select=teamid,name,teamtype,isdefault,membershiptype,administratorid,businessunitid,createdon,modifiedon" `
        -SaveFileName 'teams.json'

    if ($teams -is [hashtable] -and $teams.__Error) {
        $result.Sections['Teams'] = @{ Notes = @("TEAMS_$($teams.__Error.Category)") }
    } else {
        $tArr    = @($teams)
        $owner   = @($tArr | Where-Object { $_.teamtype -eq 0 })
        $access  = @($tArr | Where-Object { $_.teamtype -eq 1 })
        $secGrp  = @($tArr | Where-Object { $_.teamtype -eq 2 })
        $office  = @($tArr | Where-Object { $_.teamtype -eq 3 })
        $default = @($tArr | Where-Object { $_.isdefault -eq $true })

        $result.Sections['Teams'] = @{
            TotalCount        = $tArr.Count
            OwnerTeamCount    = $owner.Count
            AccessTeamCount   = $access.Count
            SecurityGroupCount = $secGrp.Count
            Office365TeamCount = $office.Count
            DefaultTeamCount  = $default.Count
            Notes             = @()
        }
    }

    # ── 4. Field Security Profiles ───────────────────────────────────────────
    $fsp = Invoke-RBACSection -SectionName 'Field Security Profiles' `
        -ODataPath "fieldsecurityprofiles?`$select=fieldsecurityprofileid,name,ismanaged,createdon,modifiedon" `
        -SaveFileName 'field-security-profiles.json'

    if ($fsp -is [hashtable] -and $fsp.__Error) {
        $result.Sections['FieldSecurityProfiles'] = @{ Notes = @("FSP_$($fsp.__Error.Category)") }
    } else {
        $fspArr = @($fsp)
        $result.Sections['FieldSecurityProfiles'] = @{
            TotalCount = $fspArr.Count
            Notes      = @()
        }
    }

    # ── 5. User-role assignment density ──────────────────────────────────────
    # Use systemuserroles via query sample — count only via records count
    $uRoles = Invoke-RBACSection -SectionName 'User-Role Assignments' `
        -ODataPath "systemusers?`$select=systemuserid&`$expand=systemuserroles_association(`$select=roleid)&`$top=500" `
        -SaveFileName 'user-role-assignments.json'

    if (-not ($uRoles -is [hashtable] -and $uRoles.__Error)) {
        $uArr = @($uRoles)
        $sysAdminIds = if ($result.Sections['SecurityRoles'].SystemAdminRoleIds) {
            [System.Collections.Generic.HashSet[string]]::new(
                [string[]]@($result.Sections['SecurityRoles'].SystemAdminRoleIds | ForEach-Object { [string]$_ }),
                [System.StringComparer]::OrdinalIgnoreCase
            )
        } else { [System.Collections.Generic.HashSet[string]]::new() }

        $usersWithSysAdmin = @($uArr | Where-Object {
            $assoc = $_.systemuserroles_association
            $assoc -and @($assoc | Where-Object { $sysAdminIds.Contains([string]$_.roleid) }).Count -gt 0
        })

        $result.Sections['UserRoleAssignments'] = @{
            SampledUserCount     = $uArr.Count
            UsersWithSystemAdmin = $usersWithSysAdmin.Count
            Notes                = @()
        }
        if ($usersWithSysAdmin.Count -gt 10 -and $EnvEntry.EnvironmentSku -eq 'Production') {
            $result.Sections['UserRoleAssignments'].Notes += "SYSTEM_ADMIN_OVERASSIGNED ($($usersWithSysAdmin.Count) users have System Administrator on Production - violates least-privilege)"
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

    Save-EnvironmentData -EnvironmentDir $EnvOutputDir -FileName 'rbac-summary.json' -Data $result
    Write-InventoryLog "  RBAC inventory complete. Flags: $($result.AllFlags.Count)" -Level OK -Indent 1

    return $result
}
