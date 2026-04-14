<#
.SYNOPSIS
    Collects the full list of Power Platform environments from the BAP API.
.DESCRIPTION
    Retrieves all environments visible to the authenticated global admin,
    expands capacity/linked metadata, and returns a structured list.
    Also retrieves the tenant-wide capacity summary.

    This script is dot-sourced by Invoke-DataverseInventory.ps1.
.OUTPUTS
    [System.Collections.Generic.List[hashtable]] - one entry per environment
#>

function Get-AllEnvironments {
    [CmdletBinding()]
    param(
        [string]$OutputPath
    )

    Write-InventoryLog '--- Enumerating all Power Platform environments ---'

    # ── Tenant-wide capacity summary ─────────────────────────────────────────
    # Try two known endpoint paths; the available path varies by tenant API version
    Write-InventoryLog 'Fetching tenant capacity summary...' -Indent 1
    $capacitySummary = $null
    $capacityPaths = @(
        '/providers/Microsoft.BusinessAppPlatform/scopes/admin/capacity?api-version=2022-03-01-preview',
        '/providers/Microsoft.BusinessAppPlatform/scopes/admin/environments?api-version=2021-04-01&$expand=properties.capacity&$top=1'
    )
    foreach ($cp in $capacityPaths) {
        try {
            # Build the URI manually so we don't double-add api-version
            $token   = Get-AzureToken -ResourceUrl 'https://service.powerapps.com/'
            $headers = @{ Authorization = "Bearer $token" }
            $uri     = "https://api.bap.microsoft.com$cp"
            $capacitySummary = Invoke-RestWithRetry -Uri $uri -Headers $headers -TimeoutSec 30
            Save-RootData -FileName 'tenant-capacity.json' -Data $capacitySummary
            Write-InventoryLog "Tenant capacity saved (from $cp)." -Level OK -Indent 1
            break
        } catch {
            Write-InventoryLog "  Capacity path '$cp' not available: $($_.Exception.Message.Split([char]10)[0])" -Level DEBUG -Indent 1
        }
    }
    if (-not $capacitySummary) {
        Write-InventoryLog "Tenant-level capacity summary unavailable - storage will be read per environment." -Level WARN -Indent 1
    }

    # ── Environment list ─────────────────────────────────────────────────────
    Write-InventoryLog 'Fetching environment list (with capacity expansion)...' -Indent 1

    $allEnvs = [System.Collections.Generic.List[object]]::new()

    try {
        $resp = Invoke-BAPRequest `
            -Path '/providers/Microsoft.BusinessAppPlatform/scopes/admin/environments' `
            -ApiVersion '2021-04-01' `
            -ExtraQuery '$expand=properties.capacity,properties.addons'

        $allEnvs.AddRange([object[]]$resp.value)

        # Handle pagination (large tenants)
        $nextLink = $resp.'@odata.nextLink'
        while ($nextLink) {
            Write-InventoryLog "  Fetching next page of environments..." -Indent 2 -Level DEBUG
            $token = Get-AzureToken -ResourceUrl 'https://service.powerapps.com/'
            $headers = @{ Authorization = "Bearer $token" }
            $resp  = Invoke-RestWithRetry -Uri $nextLink -Headers $headers
            if ($resp.value) { $allEnvs.AddRange([object[]]$resp.value) }
            $nextLink = $resp.'@odata.nextLink'
        }
    } catch {
        Write-InventoryLog "Failed to retrieve environment list: $_" -Level ERROR
        throw
    }

    Write-InventoryLog "Found $($allEnvs.Count) environments." -Level OK -Indent 1

    # ── Additional environment details per env ───────────────────────────────
    # The list endpoint sometimes lacks full capacity; fetch individually
    $detailed = [System.Collections.Generic.List[hashtable]]::new()
    $i = 0

    foreach ($env in $allEnvs) {
        $i++
        $envName    = $env.name        # GUID
        $displayName = $env.properties.displayName
        Write-Progress -Activity 'Fetching environment details' `
                       -Status "$displayName ($i / $($allEnvs.Count))" `
                       -PercentComplete (($i / $allEnvs.Count) * 100)

        # Attempt per-env detail fetch for richer data
        $envDetail = $env  # default to list entry
        try {
            $envDetail = Invoke-BAPRequest `
                -Path "/providers/Microsoft.BusinessAppPlatform/scopes/admin/environments/$envName" `
                -ApiVersion '2021-04-01' `
                -ExtraQuery '$expand=properties.capacity,properties.addons,properties.runtimeState'
        } catch {
            Write-InventoryLog "  Could not fetch detail for $displayName - using summary data." -Level WARN -Indent 2
        }

        # Extract key metadata into a flat hashtable for easier reporting
        $props   = $envDetail.properties
        $linked  = $props.linkedEnvironmentMetadata
        $cap     = $props.capacity

        # Capacity: sum over capacity array if present
        $dbMb   = 0.0
        $fileMb = 0.0
        $logMb  = 0.0
        if ($cap -is [System.Collections.IEnumerable]) {
            foreach ($c in $cap) {
                $consump = if ($null -ne $c.actualConsumption) { [double]$c.actualConsumption } else { 0.0 }
                switch ($c.capacityType) {
                    'Database' { $dbMb   += $consump }
                    'File'     { $fileMb += $consump }
                    'Log'      { $logMb  += $consump }
                }
            }
        }

        $entry = @{
            EnvironmentId      = $envName
            DisplayName        = $displayName
            EnvironmentSku     = $props.environmentSku          # Production/Sandbox/Trial/Default/Developer
            IsDefault          = [bool]($props.isDefault)
            State              = $props.states.management.id    # Ready/Disabled/Deleted
            RuntimeState       = $props.states.runtime.id       # Enabled/Disabled
            Location           = $envDetail.location
            AzureRegion        = $props.azureRegion
            CreatedTime        = $props.createdTime
            CreatedBy          = $props.createdBy.displayName
            # Dataverse link
            HasDataverse        = ($null -ne $linked -and $linked.instanceUrl)
            OrgUrl             = $linked.instanceUrl
            OrgApiUrl          = $linked.instanceApiUrl
            OrgUniqueName      = $linked.uniqueName
            OrgDomainName      = $linked.domainName
            OrgVersion         = $linked.version
            OrgInstanceState   = $linked.instanceState
            OrgCreatedTime     = $linked.createdTime
            # Storage (MB)
            StorageDB_MB       = [Math]::Round($dbMb, 2)
            StorageFile_MB     = [Math]::Round($fileMb, 2)
            StorageLog_MB      = [Math]::Round($logMb, 2)
            StorageTotal_MB    = [Math]::Round($dbMb + $fileMb + $logMb, 2)
            # Raw objects for full JSON
            RawCapacity        = $cap
            RawProperties      = $props
        }

        $detailed.Add($entry)
    }

    Write-Progress -Activity 'Fetching environment details' -Completed

    # Save combined environment list
    Save-RootData -FileName 'environments.json' -Data $detailed

    Write-InventoryLog "Environment list saved ($($detailed.Count) entries)." -Level OK
    return ,$detailed
}
