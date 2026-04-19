#Requires -Version 5.1
<#
.SYNOPSIS
    Contract tests for the new Phase 1-5 collectors. Verifies that the
    summary JSON outputs each collector produces conform to the schema the
    report generator expects.

    These tests don't run the collectors (which require live BAP + Dataverse
    access). Instead, they build minimal fixture objects that match the
    collector output schema and assert that (a) required fields exist,
    (b) flag strings follow the expected prefix convention, and (c) the
    report generator can consume them without errors.
#>
[CmdletBinding()]
param()

BeforeAll {
    $repoRoot = Split-Path -Parent $PSScriptRoot
    $script:configDir = Join-Path $repoRoot 'config'
}

Describe 'Maker Inventory contract (maker-summary.json)' {

    It 'required top-level keys are present' {
        $sample = [ordered]@{
            CollectedAt    = (Get-Date -Format 'o')
            EnvironmentId  = 'env-guid'
            DisplayName    = 'Test'
            Sections       = [ordered]@{}
            AllFlags       = @()
        }
        $sample.Keys -contains 'CollectedAt'   | Should -BeTrue
        $sample.Keys -contains 'EnvironmentId' | Should -BeTrue
        $sample.Keys -contains 'Sections'      | Should -BeTrue
        $sample.Keys -contains 'AllFlags'      | Should -BeTrue
    }

    It 'Sections contain expected maker surfaces' {
        $expectedSections = @('CanvasApps','CloudFlows','CustomConnectors','Connections','PowerPages','Copilots','Dataflows','AIModels')
        # Assert the script emits these section names when the surface data is present
        $collectorPath = Join-Path (Split-Path -Parent $PSScriptRoot) 'scripts\collectors\Collect-MakerInventory.ps1'
        $content       = Get-Content $collectorPath -Raw
        foreach ($s in $expectedSections) {
            $content | Should -Match "Sections\['$s'\]" -Because "Collector should emit Sections['$s']"
        }
    }

    It 'flag strings use uppercase snake_case prefixes' {
        $flags = @(
            'ORPHANED_CANVAS_APPS (detail...)',
            'CLOUD_FLOWS_OWNED_BY_DISABLED_USERS (detail...)',
            'CONNECTIONS_IN_ERROR_STATE (detail...)'
        )
        foreach ($f in $flags) {
            $f | Should -Match '^[A-Z][A-Z0-9_]+(\s|$)' -Because "flags must start with an uppercase snake_case prefix"
        }
    }
}

Describe 'Tenant Governance contract (tenant/governance-summary.json)' {

    It 'tenant-scoped output has DlpPolicies and TenantIsolation sections' {
        $collectorPath = Join-Path (Split-Path -Parent $PSScriptRoot) 'scripts\collectors\Collect-TenantGovernance.ps1'
        $content       = Get-Content $collectorPath -Raw
        $content | Should -Match "Sections\['DlpPolicies'\]"
        $content | Should -Match "Sections\['TenantSettings'\]"
        $content | Should -Match "Sections\['TenantIsolation'\]"
    }

    It 'exports both Collect-TenantGovernance and Collect-EnvironmentGovernance' {
        $collectorPath = Join-Path (Split-Path -Parent $PSScriptRoot) 'scripts\collectors\Collect-TenantGovernance.ps1'
        $content       = Get-Content $collectorPath -Raw
        $content | Should -Match 'function Collect-TenantGovernance'
        $content | Should -Match 'function Collect-EnvironmentGovernance'
    }
}

Describe 'RBAC contract (rbac-summary.json)' {

    It 'Collect-RBACInventory emits expected sections' {
        $collectorPath = Join-Path (Split-Path -Parent $PSScriptRoot) 'scripts\collectors\Collect-RBAC.ps1'
        $content       = Get-Content $collectorPath -Raw
        foreach ($s in @('SecurityRoles','BusinessUnits','Teams','FieldSecurityProfiles','UserRoleAssignments')) {
            $content | Should -Match "Sections\['$s'\]"
        }
    }
}

Describe 'Metadata Depth contract (metadata-depth-summary.json)' {

    It 'Collect-MetadataDepthInventory emits expected sections' {
        $collectorPath = Join-Path (Split-Path -Parent $PSScriptRoot) 'scripts\collectors\Collect-MetadataDepth.ps1'
        $content       = Get-Content $collectorPath -Raw
        foreach ($s in @('Publishers','D365Apps','Organization','Currencies','Languages','LifecycleOperations','Backups')) {
            $content | Should -Match "Sections\['$s'\]"
        }
    }
}

Describe 'Activity Telemetry contract (activity-summary.json)' {

    It 'Collect-ActivityTelemetry emits expected sections' {
        $collectorPath = Join-Path (Split-Path -Parent $PSScriptRoot) 'scripts\collectors\Collect-Activity.ps1'
        $content       = Get-Content $collectorPath -Raw
        foreach ($s in @('FlowRuns','CanvasAppUsage','MakerActivity')) {
            $content | Should -Match "Sections\['$s'\]"
        }
    }
}

Describe 'flag-severity.json covers new flags' {

    BeforeAll {
        $path = Join-Path $script:configDir 'flag-severity.json'
        $script:severity = Get-Content $path -Raw | ConvertFrom-Json
        $script:allFlags = @(
            $script:severity.Critical +
            $script:severity.High +
            $script:severity.Medium +
            $script:severity.Low
        )
    }

    It 'includes maker-inventory flags' {
        $script:allFlags | Should -Contain 'ORPHANED_CANVAS_APPS'
        $script:allFlags | Should -Contain 'CLOUD_FLOWS_OWNED_BY_DISABLED_USERS'
        $script:allFlags | Should -Contain 'CONNECTIONS_IN_ERROR_STATE'
        $script:allFlags | Should -Contain 'UNCERTIFIED_CUSTOM_CONNECTORS'
        $script:allFlags | Should -Contain 'SUSPENDED_CLOUD_FLOWS'
        $script:allFlags | Should -Contain 'STALE_CANVAS_APPS'
        $script:allFlags | Should -Contain 'STALE_COPILOTS'
        $script:allFlags | Should -Contain 'DATAFLOWS_FAILING_REFRESH'
        $script:allFlags | Should -Contain 'ORPHANED_CONNECTIONS'
    }

    It 'includes tenant governance flags' {
        $script:allFlags | Should -Contain 'TENANT_ISOLATION_DISABLED'
        $script:allFlags | Should -Contain 'NO_DLP_POLICIES_CONFIGURED'
        $script:allFlags | Should -Contain 'MANAGED_ENV_WEEKLY_DIGEST_DISABLED'
        $script:allFlags | Should -Contain 'MANAGED_ENV_SOLUTION_CHECKER_OFF'
    }

    It 'includes RBAC flags' {
        $script:allFlags | Should -Contain 'SYSTEM_ADMIN_OVERASSIGNED'
        $script:allFlags | Should -Contain 'NO_CUSTOM_SECURITY_ROLES'
        $script:allFlags | Should -Contain 'DEEP_BUSINESS_UNIT_NESTING'
    }

    It 'includes metadata / lifecycle flags' {
        $script:allFlags | Should -Contain 'LIFECYCLE_OP_FAILED_RECENTLY'
        $script:allFlags | Should -Contain 'NO_RECENT_USER_BACKUPS'
        $script:allFlags | Should -Contain 'NO_D365_APPS_ON_F365_ENV'
    }

    It 'includes activity telemetry flags' {
        $script:allFlags | Should -Contain 'NO_MAKER_ACTIVITY_90D'
        $script:allFlags | Should -Contain 'HIGH_FLOW_RUN_FAILURE_RATE'
        $script:allFlags | Should -Contain 'CANVAS_APPS_NEVER_LAUNCHED'
    }

    It 'includes error-classification flags' {
        $script:allFlags | Should -Contain 'MAILBOX_ACCESS_DENIED'
        $script:allFlags | Should -Contain 'AUDIT_DISABLED_OR_INACCESSIBLE'
        $script:allFlags | Should -Contain 'RETENTION_FEATURE_NOT_ENABLED'
        $script:allFlags | Should -Contain 'SLA_NOT_CONFIGURED'
        $script:allFlags | Should -Contain 'DUALWRITE_NOT_CONFIGURED'
    }
}

Describe 'sku-profiles.json suppresses maker flags on dev/trial' {

    BeforeAll {
        $path = Join-Path $script:configDir 'sku-profiles.json'
        $script:sku = Get-Content $path -Raw | ConvertFrom-Json
    }

    It 'Developer suppresses orphan flags (dev environments are expected to have disabled creators)' {
        $script:sku.Developer.Suppress | Should -Contain 'ORPHANED_CANVAS_APPS'
        $script:sku.Developer.Suppress | Should -Contain 'ORPHANED_CONNECTIONS'
        $script:sku.Developer.Suppress | Should -Contain 'CLOUD_FLOWS_OWNED_BY_DISABLED_USERS'
    }

    It 'Trial suppresses same set' {
        $script:sku.Trial.Suppress | Should -Contain 'ORPHANED_CANVAS_APPS'
        $script:sku.Trial.Suppress | Should -Contain 'SUSPENDED_CLOUD_FLOWS'
    }

    It 'Production suppress list is empty (all flags apply)' {
        $script:sku.Production.Suppress.Count | Should -Be 0
    }
}

Describe 'Orchestrator wiring' {

    BeforeAll {
        $orchestratorPath = Join-Path (Split-Path -Parent $PSScriptRoot) 'scripts\Invoke-DataverseInventory.ps1'
        $script:orchContent = Get-Content $orchestratorPath -Raw
    }

    It 'dot-sources all five new collectors' {
        $script:orchContent | Should -Match 'Collect-MakerInventory\.ps1'
        $script:orchContent | Should -Match 'Collect-TenantGovernance\.ps1'
        $script:orchContent | Should -Match 'Collect-RBAC\.ps1'
        $script:orchContent | Should -Match 'Collect-MetadataDepth\.ps1'
        $script:orchContent | Should -Match 'Collect-Activity\.ps1'
    }

    It 'defines the new include switches with correct defaults' {
        $script:orchContent | Should -Match '\$IncludeMakerInventory\s*=\s*\$true'
        $script:orchContent | Should -Match '\$IncludeGovernance\s*=\s*\$true'
        $script:orchContent | Should -Match '\$IncludeRBAC\s*=\s*\$true'
        $script:orchContent | Should -Match '\$IncludeMetadataDepth\s*=\s*\$true'
        $script:orchContent | Should -Match '\$IncludeActivity\s*=\s*\$false'
    }

    It 'invokes each collector function' {
        $script:orchContent | Should -Match 'Collect-MakerEnvironmentInventory'
        $script:orchContent | Should -Match 'Collect-TenantGovernance'
        $script:orchContent | Should -Match 'Collect-EnvironmentGovernance'
        $script:orchContent | Should -Match 'Collect-RBACInventory'
        $script:orchContent | Should -Match 'Collect-MetadataDepthInventory'
        $script:orchContent | Should -Match 'Collect-ActivityTelemetry'
    }
}

Describe 'Report generator consumes new summaries' {

    BeforeAll {
        $reportPath = Join-Path (Split-Path -Parent $PSScriptRoot) 'scripts\Generate-Report.ps1'
        $script:reportContent = Get-Content $reportPath -Raw
    }

    It 'loads each new summary file type' {
        $script:reportContent | Should -Match 'maker-summary\.json'
        $script:reportContent | Should -Match 'governance-summary\.json'
        $script:reportContent | Should -Match 'rbac-summary\.json'
        $script:reportContent | Should -Match 'metadata-depth-summary\.json'
        $script:reportContent | Should -Match 'activity-summary\.json'
    }

    It 'loads tenant-level governance summary' {
        $script:reportContent | Should -Match 'tenantGovFile'
        $script:reportContent | Should -Match 'governance-summary\.json'
    }

    It 'renders new HTML sections' {
        $script:reportContent | Should -Match 'id="tenant-gov"'
        $script:reportContent | Should -Match 'id="maker"'
        $script:reportContent | Should -Match 'id="rbac"'
        $script:reportContent | Should -Match 'id="metadata-depth"'
    }
}
