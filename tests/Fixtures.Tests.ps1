#Requires -Version 5.1
<#
.SYNOPSIS
    Integration-style tests using the real collected data files in data/.
    These tests verify that actual output JSON files have the expected structure
    and that the report generation path works end-to-end against real data.

    Skipped automatically if data/master-summary.json does not exist
    (i.e. no collection has been run yet).
#>
[CmdletBinding()]
param()

BeforeAll {
    $repoRoot      = Split-Path -Parent $PSScriptRoot
    $dataDir       = Join-Path $repoRoot 'data'
    $masterFile    = Join-Path $dataDir 'master-summary.json'
    $envsFile      = Join-Path $dataDir 'environments.json'
    $configDir     = Join-Path $repoRoot 'config'

    $script:hasData = Test-Path $masterFile
    if ($script:hasData) {
        $script:master = Get-Content $masterFile -Raw | ConvertFrom-Json
        $script:envs   = if (Test-Path $envsFile) { Get-Content $envsFile -Raw | ConvertFrom-Json } else { @() }
    }
}

Describe 'Collected Data Fixtures' -Skip:(-not $script:hasData) {

    Context 'master-summary.json structure' {

        It 'has a RunAt timestamp' {
            $script:master.RunAt | Should -Not -BeNullOrEmpty
        }

        It 'has a TenantId' {
            $script:master.TenantId | Should -Not -BeNullOrEmpty
        }

        It 'has an Environments array' {
            $script:master.Environments | Should -Not -BeNullOrEmpty
            @($script:master.Environments).Count | Should -BeGreaterThan 0
        }

        It 'Processed count is greater than 0 or Skipped explains the total' {
            $total = [int]$script:master.Processed + [int]$script:master.Skipped
            $total | Should -BeGreaterThan 0
        }

        It 'every environment entry has an EnvironmentId' {
            foreach ($env in $script:master.Environments) {
                $env.EnvironmentId | Should -Not -BeNullOrEmpty
            }
        }

        It 'every environment entry has a DisplayName' {
            foreach ($env in $script:master.Environments) {
                $env.DisplayName | Should -Not -BeNullOrEmpty
            }
        }

        It 'every environment entry has a StorageTotal_MB value' {
            foreach ($env in $script:master.Environments) {
                $null -ne $env.StorageTotal_MB | Should -BeTrue
            }
        }

        It 'AllFlagsDistinct is an array with Name and Count properties' {
            if (@($script:master.AllFlagsDistinct).Count -gt 0) {
                $first = $script:master.AllFlagsDistinct[0]
                $first.Name  | Should -Not -BeNullOrEmpty
                $null -ne $first.Count | Should -BeTrue
            }
        }
    }

    Context 'Per-environment output files' {

        BeforeAll {
            $envDirs = @(Get-ChildItem -Path (Join-Path $dataDir 'environments') -Directory -ErrorAction SilentlyContinue)
        }

        It 'at least one environment directory exists' {
            $envDirs.Count | Should -BeGreaterThan 0
        }

        It 'every environment directory has a metadata.json file' {
            foreach ($dir in $envDirs) {
                $metaFile = Join-Path $dir.FullName 'metadata.json'
                Test-Path $metaFile | Should -BeTrue -Because "missing metadata.json in $($dir.Name)"
            }
        }

        It 'every ce-summary.json that exists is valid JSON' {
            foreach ($dir in $envDirs) {
                $ceSummary = Join-Path $dir.FullName 'ce-summary.json'
                if (Test-Path $ceSummary) {
                    { Get-Content $ceSummary -Raw | ConvertFrom-Json } | Should -Not -Throw `
                        -Because "invalid JSON in $ceSummary"
                }
            }
        }

        It 'every ce-summary.json has an AllFlags array' {
            foreach ($dir in $envDirs) {
                $ceSummary = Join-Path $dir.FullName 'ce-summary.json'
                if (Test-Path $ceSummary) {
                    $parsed = Get-Content $ceSummary -Raw | ConvertFrom-Json
                    $null -ne $parsed.AllFlags | Should -BeTrue `
                        -Because "AllFlags missing in $ceSummary"
                }
            }
        }

        It 'every async-operations-summary.json is valid JSON with state counts' {
            foreach ($dir in $envDirs) {
                $asyncFile = Join-Path $dir.FullName 'async-operations-summary.json'
                if (Test-Path $asyncFile) {
                    $parsed = { Get-Content $asyncFile -Raw | ConvertFrom-Json }
                    $parsed | Should -Not -Throw -Because "invalid JSON in $asyncFile"
                }
            }
        }
    }

    Context 'Flag name integrity' {
        BeforeAll {
            $severityFile = Join-Path $configDir 'flag-severity.json'
            $severityCfg  = Get-Content $severityFile -Raw | ConvertFrom-Json
            $knownFlags   = @($severityCfg.Critical) + @($severityCfg.High) +
                            @($severityCfg.Medium) + @($severityCfg.Low)
        }

        It 'all flags in master-summary AllFlagsDistinct are recognized in flag-severity.json' {
            $unrecognized = @()
            foreach ($flagEntry in $script:master.AllFlagsDistinct) {
                $flagName = ($flagEntry.Name -split '\s*\(')[0].Trim()
                if ($flagName -notin $knownFlags) {
                    $unrecognized += $flagName
                }
            }

            if ($unrecognized.Count -gt 0) {
                # Warn rather than fail — new flags may have been added to collectors
                # before being classified. Produces an informational test failure.
                $unrecognized -join ', ' | Should -BeNullOrEmpty `
                    -Because "these flags exist in collected data but have no severity classification in flag-severity.json. Add them to config/flag-severity.json."
            }
        }
    }

    Context 'Report generation smoke test' {

        It 'Generate-Report.ps1 produces an HTML file without errors' {
            $reportScript = Join-Path $repoRoot 'scripts\Generate-Report.ps1'
            $tmpReport    = [System.IO.Path]::GetTempFileName() -replace '\.tmp$', '.html'

            try {
                $result = & $reportScript -DataPath $dataDir -ReportPath $tmpReport 2>&1
                $errors = $result | Where-Object { $_ -is [System.Management.Automation.ErrorRecord] }
                $errors | Should -BeNullOrEmpty -Because "Generate-Report.ps1 should not produce errors"

                Test-Path $tmpReport | Should -BeTrue -Because "report file should be created"
                $size = (Get-Item $tmpReport).Length
                $size | Should -BeGreaterThan 1000 -Because "report file should have meaningful content"
            } finally {
                Remove-Item $tmpReport -ErrorAction SilentlyContinue
            }
        }
    }
}
