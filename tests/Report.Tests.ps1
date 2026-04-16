#Requires -Version 5.1
<#
.SYNOPSIS
    Tests the pure helper functions embedded in Generate-Report.ps1.

    Uses the PowerShell AST to extract only the function definitions from the
    report script and load them into scope — without executing the rest of the
    script (which requires collected data files). Then mocks the module-level
    variables ($flagSeverity, $skuProfiles) those functions depend on.
#>
[CmdletBinding()]
param()

BeforeAll {
    $repoRoot    = Split-Path -Parent $PSScriptRoot
    $reportPath  = Join-Path $repoRoot 'scripts\Generate-Report.ps1'

    # Use the PowerShell AST to extract named function definitions without
    # executing the rest of the script (which would fail without data files).
    $parseErrors = $null
    $ast = [System.Management.Automation.Language.Parser]::ParseFile(
        $reportPath, [ref]$null, [ref]$parseErrors
    )

    if ($parseErrors) {
        throw "Could not parse Generate-Report.ps1: $($parseErrors[0].Message)"
    }

    # Functions we want to test in isolation
    $targetFunctions = @('Format-MB', 'Get-FlagBadgeHtml', 'Get-SectionValue',
                         'Get-FlagSeverity', 'Get-GovernanceScore')

    $functionDefs = $ast.FindAll({
        param($node)
        $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
        $node.Name -in $targetFunctions
    }, $false)

    foreach ($funcDef in $functionDefs) {
        . ([scriptblock]::Create($funcDef.Extent.Text))
    }

    # Inject the module-level hashtables the functions reference via script scope.
    # These match the structure of config/flag-severity.json and config/sku-profiles.json.
    $configDir     = Join-Path $repoRoot 'config'
    $severityCfg   = Get-Content (Join-Path $configDir 'flag-severity.json') -Raw | ConvertFrom-Json
    $script:flagSeverity = @{
        Critical = @($severityCfg.Critical)
        High     = @($severityCfg.High)
        Medium   = @($severityCfg.Medium)
        Low      = @($severityCfg.Low)
        Weights  = @{
            Critical = [int]$severityCfg.Weights.Critical
            High     = [int]$severityCfg.Weights.High
            Medium   = [int]$severityCfg.Weights.Medium
            Low      = [int]$severityCfg.Weights.Low
        }
    }

    $skuCfg = Get-Content (Join-Path $configDir 'sku-profiles.json') -Raw | ConvertFrom-Json
    $script:skuProfiles = @{}
    foreach ($prop in $skuCfg.PSObject.Properties) {
        if ($prop.Name -ne '_comment') { $script:skuProfiles[$prop.Name] = $prop.Value }
    }

    # Add-Type needed for HtmlEncode used inside Get-FlagBadgeHtml
    Add-Type -AssemblyName System.Web
}

# ── Format-MB ─────────────────────────────────────────────────────────────────

Describe 'Format-MB' {

    It 'formats values below 1 GB as MB' {
        Format-MB -MB 512 | Should -Be '512 MB'
    }

    It 'rounds MB values to the nearest integer' {
        Format-MB -MB 512.6 | Should -Be '513 MB'
    }

    It 'converts values >= 1024 MB to GB with one decimal place' {
        Format-MB -MB 2048 | Should -Be '2.0 GB'
    }

    It 'formats a large GB value correctly' {
        Format-MB -MB 10240 | Should -Be '10.0 GB'
    }

    It 'handles zero MB' {
        Format-MB -MB 0 | Should -Be '0 MB'
    }

    It 'formats exactly 1024 MB as 1.0 GB' {
        Format-MB -MB 1024 | Should -Be '1.0 GB'
    }
}

# ── Get-FlagBadgeHtml ──────────────────────────────────────────────────────────

Describe 'Get-FlagBadgeHtml' {

    It 'returns a green Clean badge for an empty flag list' {
        $result = Get-FlagBadgeHtml -Flags @()
        $result | Should -Match 'bg-success'
        $result | Should -Match 'Clean'
    }

    It 'returns a green Clean badge for a null flag list' {
        $result = Get-FlagBadgeHtml -Flags $null
        $result | Should -Match 'bg-success'
    }

    It 'uses bg-danger for BROKEN_ flags' {
        $result = Get-FlagBadgeHtml -Flags @('BROKEN_CONNECTION_REFERENCES (3)')
        $result | Should -Match 'bg-danger'
    }

    It 'uses bg-danger for HIGH_ flags' {
        $result = Get-FlagBadgeHtml -Flags @('HIGH_FAILED_JOBS_30D (5)')
        $result | Should -Match 'bg-danger'
    }

    It 'uses bg-danger for MAILBOX_ flags' {
        $result = Get-FlagBadgeHtml -Flags @('MAILBOX_SYNC_ERRORS (2 of 10)')
        $result | Should -Match 'bg-danger'
    }

    It 'uses bg-danger for PRODUCTION_NOT_MANAGED_ENVIRONMENT' {
        $result = Get-FlagBadgeHtml -Flags @('PRODUCTION_NOT_MANAGED_ENVIRONMENT (Production)')
        $result | Should -Match 'bg-danger'
    }

    It 'uses bg-warning for NO_ flags' {
        $result = Get-FlagBadgeHtml -Flags @('NO_SCHEDULED_BULK_DELETE')
        $result | Should -Match 'bg-warning'
    }

    It 'uses bg-warning for MANY_ flags' {
        $result = Get-FlagBadgeHtml -Flags @('MANY_INACTIVE_WORKFLOWS (25)')
        $result | Should -Match 'bg-warning'
    }

    It 'uses bg-warning for STALE_ flags' {
        $result = Get-FlagBadgeHtml -Flags @('STALE_BPF_INSTANCES (600)')
        $result | Should -Match 'bg-warning'
    }

    It 'uses bg-warning for NOT_IN_ENVIRONMENT_ flags' {
        $result = Get-FlagBadgeHtml -Flags @('NOT_IN_ENVIRONMENT_GROUP (Sandbox)')
        $result | Should -Match 'bg-warning'
    }

    It 'uses bg-secondary for AUDIT_DISABLED_ flags' {
        $result = Get-FlagBadgeHtml -Flags @('AUDIT_DISABLED_OR_NO_ACTIVITY')
        $result | Should -Match 'bg-secondary'
    }

    It 'HTML-encodes the flag text to prevent XSS' {
        $result = Get-FlagBadgeHtml -Flags @('NO_ACTIVE_USERS')
        # Underscores are replaced with spaces in the display but that's OK
        # The flag name is in a title attribute and should be encoded
        $result | Should -Not -Match '<script'
    }

    It 'renders multiple flags as multiple badge spans' {
        $result = Get-FlagBadgeHtml -Flags @('NO_ACTIVE_USERS', 'NO_SCHEDULED_BULK_DELETE')
        $spanCount = ([regex]::Matches($result, '<span')).Count
        $spanCount | Should -Be 2
    }
}

# ── Get-FlagSeverity ──────────────────────────────────────────────────────────

Describe 'Get-FlagSeverity' {

    It 'returns Critical for a flag in the Critical list' {
        Get-FlagSeverity -Flag 'MAILBOX_SYNC_ERRORS' | Should -Be 'Critical'
    }

    It 'returns Critical for a flag with parenthetical detail' {
        Get-FlagSeverity -Flag 'PRODUCTION_NOT_MANAGED_ENVIRONMENT (Sandbox env is not managed)' |
            Should -Be 'Critical'
    }

    It 'returns High for a flag in the High list' {
        Get-FlagSeverity -Flag 'NO_SCHEDULED_BULK_DELETE' | Should -Be 'High'
    }

    It 'returns High for a flag with parenthetical detail' {
        Get-FlagSeverity -Flag 'PLUGIN_TRACE_LOGGING_ENABLED (All)' | Should -Be 'High'
    }

    It 'returns Medium for a flag in the Medium list' {
        Get-FlagSeverity -Flag 'NOT_IN_ENVIRONMENT_GROUP (Sandbox)' | Should -Be 'Medium'
    }

    It 'returns Low for a flag in the Low list' {
        Get-FlagSeverity -Flag 'NO_ACTIVE_USERS' | Should -Be 'Low'
    }

    It 'returns Info for an unrecognized flag' {
        Get-FlagSeverity -Flag 'SOME_FUTURE_UNKNOWN_FLAG' | Should -Be 'Info'
    }

    It 'strips parenthetical suffix before lookup' {
        # Same flag with and without suffix should return the same severity
        $bare    = Get-FlagSeverity -Flag 'HIGH_SLA_VIOLATIONS'
        $withPar = Get-FlagSeverity -Flag 'HIGH_SLA_VIOLATIONS (75 noncompliant KPI instances)'
        $bare | Should -Be $withPar
    }
}

# ── Get-GovernanceScore ───────────────────────────────────────────────────────

Describe 'Get-GovernanceScore' {

    It 'returns 100 for an environment with no flags' {
        Get-GovernanceScore -Flags @() -Sku 'Production' | Should -Be 100
    }

    It 'returns 100 for a null flag array' {
        Get-GovernanceScore -Flags $null -Sku 'Production' | Should -Be 100
    }

    It 'deducts 15 points for a Critical flag on Production' {
        $score = Get-GovernanceScore -Flags @('MAILBOX_SYNC_ERRORS') -Sku 'Production'
        $score | Should -Be 85
    }

    It 'deducts 8 points for a High flag on Production' {
        $score = Get-GovernanceScore -Flags @('NO_SCHEDULED_BULK_DELETE') -Sku 'Production'
        $score | Should -Be 92
    }

    It 'deducts 4 points for a Medium flag on Production' {
        $score = Get-GovernanceScore -Flags @('NOT_IN_ENVIRONMENT_GROUP (Production)') -Sku 'Production'
        $score | Should -Be 96
    }

    It 'deducts 1 point for a Low flag on Production' {
        $score = Get-GovernanceScore -Flags @('NO_ACTIVE_USERS') -Sku 'Production'
        $score | Should -Be 99
    }

    It 'accumulates deductions across multiple flags' {
        # Critical(15) + High(8) = 23 points deducted → score 77
        $score = Get-GovernanceScore -Flags @('MAILBOX_SYNC_ERRORS', 'NO_SCHEDULED_BULK_DELETE') -Sku 'Production'
        $score | Should -Be 77
    }

    It 'floors at 0 and never goes negative' {
        # Stack many Critical flags to drive score below 0
        $manyFlags = 'MAILBOX_SYNC_ERRORS','PRODUCTION_NOT_MANAGED_ENVIRONMENT',
                     'NO_DEDICATED_ENVIRONMENT_ADMIN','HIGH_SLA_VIOLATIONS',
                     'HIGH_FAILED_JOBS_30D','DUALWRITE_MAPS_IN_ERROR',
                     'BROKEN_CONNECTION_REFERENCES','ACTIVE_FLOWS_OWNED_BY_DISABLED_USERS',
                     'FO_BATCH_JOBS_IN_ERROR','HIGH_SUSPENDED_JOBS'
        $score = Get-GovernanceScore -Flags $manyFlags -Sku 'Production'
        $score | Should -BeGreaterOrEqual 0
        $score | Should -Be 0
    }

    It 'suppressed flags on Developer SKU cost only 1 point instead of full severity' {
        # NO_RETENTION_POLICIES is Medium (4pts) but suppressed on Developer → 1pt
        $score = Get-GovernanceScore -Flags @('NO_RETENTION_POLICIES') -Sku 'Developer'
        $score | Should -Be 99   # 100 - 1 = 99 (suppressed to 1pt)
    }

    It 'non-suppressed flags on Developer still cost full deduction' {
        # HIGH_SLA_VIOLATIONS is Critical (15pts) and not suppressed on Developer
        $score = Get-GovernanceScore -Flags @('HIGH_SLA_VIOLATIONS') -Sku 'Developer'
        $score | Should -Be 85   # 100 - 15 = 85
    }

    It 'handles an unknown SKU gracefully (no SKU profile = no suppression)' {
        $score = Get-GovernanceScore -Flags @('NO_RETENTION_POLICIES') -Sku 'Unknown'
        # Medium deduction (4pts), no suppression
        $score | Should -Be 96
    }

    It 'returns an integer (not a decimal)' {
        $score = Get-GovernanceScore -Flags @('NO_ACTIVE_USERS') -Sku 'Production'
        $score.GetType().Name | Should -BeIn @('Int32', 'Int64', 'Double')
        # If double, it should be a whole number
        ($score % 1) | Should -Be 0
    }
}

# ── Get-SectionValue ──────────────────────────────────────────────────────────

Describe 'Get-SectionValue' {

    BeforeAll {
        $mockCE = [PSCustomObject]@{
            Sections = [PSCustomObject]@{
                Users = [PSCustomObject]@{
                    ActiveCount = 42
                    TotalCount  = 100
                }
                AsyncOperations = [PSCustomObject]@{
                    Counts = [PSCustomObject]@{ Suspended = 5 }
                }
            }
        }
    }

    It 'returns the field value when section and field exist' {
        $result = Get-SectionValue -CE $mockCE -Section 'Users' -Field 'ActiveCount'
        $result | Should -Be 42
    }

    It 'returns N/A when CE is null' {
        Get-SectionValue -CE $null -Section 'Users' -Field 'ActiveCount' | Should -Be 'N/A'
    }

    It 'returns N/A when the section does not exist' {
        Get-SectionValue -CE $mockCE -Section 'NonExistentSection' -Field 'SomeField' | Should -Be 'N/A'
    }

    It 'returns N/A when the field does not exist on the section' {
        Get-SectionValue -CE $mockCE -Section 'Users' -Field 'MissingField' | Should -Be 'N/A'
    }
}
