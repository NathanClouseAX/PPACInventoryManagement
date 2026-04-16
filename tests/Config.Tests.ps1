#Requires -Version 5.1
<#
.SYNOPSIS
    Validates the structure and content of all JSON configuration files
    in the config/ directory. These tests catch malformed JSON, missing
    required keys, and logical inconsistencies (e.g. duplicate flag names).
#>
[CmdletBinding()]
param()

Describe 'Config Files' {

    BeforeAll {
        $repoRoot  = Split-Path -Parent $PSScriptRoot
        $configDir = Join-Path $repoRoot 'config'

        $severityFile  = Join-Path $configDir 'flag-severity.json'
        $skuFile       = Join-Path $configDir 'sku-profiles.json'
        $ownersFile    = Join-Path $configDir 'owners.json'
    }

    # ── File existence ──────────────────────────────────────────────────────────

    Context 'Files exist' {
        It 'flag-severity.json exists' {
            Test-Path $severityFile | Should -BeTrue
        }
        It 'sku-profiles.json exists' {
            Test-Path $skuFile | Should -BeTrue
        }
        It 'owners.json exists' {
            Test-Path $ownersFile | Should -BeTrue
        }
    }

    # ── flag-severity.json ──────────────────────────────────────────────────────

    Describe 'flag-severity.json' {

        BeforeAll {
            $raw = Get-Content $severityFile -Raw
            $cfg = $raw | ConvertFrom-Json
        }

        It 'contains valid JSON' {
            { $raw | ConvertFrom-Json } | Should -Not -Throw
        }

        It 'has a Critical array' {
            $cfg.Critical | Should -Not -BeNullOrEmpty
        }

        It 'has a High array' {
            $cfg.High | Should -Not -BeNullOrEmpty
        }

        It 'has a Medium array' {
            $cfg.Medium | Should -Not -BeNullOrEmpty
        }

        It 'has a Low array' {
            $cfg.Low | Should -Not -BeNullOrEmpty
        }

        It 'has a Weights object with all four severity keys' {
            $cfg.Weights           | Should -Not -BeNullOrEmpty
            $cfg.Weights.Critical  | Should -BeGreaterThan 0
            $cfg.Weights.High      | Should -BeGreaterThan 0
            $cfg.Weights.Medium    | Should -BeGreaterThan 0
            $cfg.Weights.Low       | Should -BeGreaterThan 0
        }

        It 'severity weights are in descending order (Critical > High > Medium > Low)' {
            [int]$cfg.Weights.Critical | Should -BeGreaterThan [int]$cfg.Weights.High
            [int]$cfg.Weights.High     | Should -BeGreaterThan [int]$cfg.Weights.Medium
            [int]$cfg.Weights.Medium   | Should -BeGreaterThan [int]$cfg.Weights.Low
        }

        It 'has no duplicate flag names across severity levels' {
            $allFlags = @($cfg.Critical) + @($cfg.High) + @($cfg.Medium) + @($cfg.Low)
            $dupes = $allFlags | Group-Object | Where-Object { $_.Count -gt 1 }
            $dupes | Should -BeNullOrEmpty -Because "flag names must appear in only one severity level"
        }

        It 'all flag names follow the UPPER_SNAKE_CASE convention' {
            $allFlags = @($cfg.Critical) + @($cfg.High) + @($cfg.Medium) + @($cfg.Low)
            $invalid = $allFlags | Where-Object { $_ -notmatch '^[A-Z0-9_]+$' }
            $invalid | Should -BeNullOrEmpty -Because "all flag names should be UPPER_SNAKE_CASE"
        }

        It 'Critical list contains expected high-priority flags' {
            $cfg.Critical | Should -Contain 'MAILBOX_SYNC_ERRORS'
            $cfg.Critical | Should -Contain 'PRODUCTION_NOT_MANAGED_ENVIRONMENT'
            $cfg.Critical | Should -Contain 'NO_DEDICATED_ENVIRONMENT_ADMIN'
        }

        It 'High list contains expected operational flags' {
            $cfg.High | Should -Contain 'NO_SCHEDULED_BULK_DELETE'
            $cfg.High | Should -Contain 'PLUGIN_TRACE_LOGGING_ENABLED'
            $cfg.High | Should -Contain 'AUDIT_RETENTION_SET_TO_FOREVER'
        }
    }

    # ── sku-profiles.json ───────────────────────────────────────────────────────

    Describe 'sku-profiles.json' {

        BeforeAll {
            $raw = Get-Content $skuFile -Raw
            $cfg = $raw | ConvertFrom-Json
        }

        It 'contains valid JSON' {
            { $raw | ConvertFrom-Json } | Should -Not -Throw
        }

        foreach ($sku in @('Production', 'Sandbox', 'Developer', 'Trial', 'Default')) {
            It "has a profile entry for SKU: $sku" {
                $cfg.PSObject.Properties.Name | Should -Contain $sku
            }
        }

        It 'Production profile has no suppressed flags (strictest posture)' {
            $prod = $cfg.Production
            @($prod.Suppress).Count | Should -Be 0
        }

        It 'Developer profile suppresses NO_RETENTION_POLICIES' {
            $cfg.Developer.Suppress | Should -Contain 'NO_RETENTION_POLICIES'
        }

        It 'all GovernanceWeight values are positive numbers' {
            foreach ($prop in $cfg.PSObject.Properties) {
                if ($prop.Name -eq '_comment') { continue }
                $w = $prop.Value.GovernanceWeight
                if ($null -ne $w) {
                    [double]$w | Should -BeGreaterThan 0
                }
            }
        }

        It 'Production has the highest GovernanceWeight' {
            $prodW = [double]$cfg.Production.GovernanceWeight
            foreach ($prop in $cfg.PSObject.Properties) {
                if ($prop.Name -in '_comment','Production') { continue }
                $w = $prop.Value.GovernanceWeight
                if ($null -ne $w) {
                    $prodW | Should -BeGreaterThan ([double]$w) `
                        -Because "Production weight ($prodW) must exceed $($prop.Name) weight ($w)"
                }
            }
        }

        It 'all flagged suppressions in SKU profiles exist in flag-severity.json' {
            $severityCfg = Get-Content $severityFile -Raw | ConvertFrom-Json
            $knownFlags  = @($severityCfg.Critical) + @($severityCfg.High) +
                           @($severityCfg.Medium) + @($severityCfg.Low)

            foreach ($prop in $cfg.PSObject.Properties) {
                if ($prop.Name -eq '_comment') { continue }
                foreach ($sup in @($prop.Value.Suppress)) {
                    $knownFlags | Should -Contain $sup `
                        -Because "Suppress entry '$sup' in SKU '$($prop.Name)' must exist in flag-severity.json"
                }
            }
        }
    }

    # ── owners.json ─────────────────────────────────────────────────────────────

    Describe 'owners.json' {

        BeforeAll {
            $raw = Get-Content $ownersFile -Raw
            $cfg = $raw | ConvertFrom-Json
        }

        It 'contains valid JSON' {
            { $raw | ConvertFrom-Json } | Should -Not -Throw
        }

        It 'has a _comment key (template marker)' {
            $cfg.PSObject.Properties.Name | Should -Contain '_comment'
        }

        It 'has an _example key (usage template)' {
            $cfg.PSObject.Properties.Name | Should -Contain '_example'
        }

        It 'any real owner entries are GUID-format keys' {
            $guidPattern = '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            foreach ($prop in $cfg.PSObject.Properties) {
                if ($prop.Name -in '_comment','_example') { continue }
                $prop.Name | Should -Match $guidPattern `
                    -Because "owner keys must be environment GUIDs (got: $($prop.Name))"
            }
        }
    }
}
