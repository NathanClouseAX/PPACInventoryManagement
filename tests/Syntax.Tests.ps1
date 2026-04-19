#Requires -Version 5.1
<#
.SYNOPSIS
    Parses every PowerShell script and module in the project for syntax errors
    without executing any of them. These tests catch typos, unclosed blocks,
    and other parse-time errors before a real collection run.
#>
[CmdletBinding()]
param()

Describe 'PowerShell Script Syntax' {

    BeforeAll {
        $repoRoot   = Split-Path -Parent $PSScriptRoot
        $scriptFiles = @(Get-ChildItem -Path $repoRoot -Recurse -Include '*.ps1','*.psm1' |
            Where-Object { $_.FullName -notmatch '[\\/]\.git[\\/]' })
    }

    Context 'Parse validation' {

        It 'finds at least one script to validate' {
            $scriptFiles.Count | Should -BeGreaterThan 0
        }

        It 'has no parse errors: <_>' -ForEach @(
            'scripts\modules\PPACInventory.psm1'
            'scripts\Invoke-DataverseInventory.ps1'
            'scripts\Generate-Report.ps1'
            'scripts\collectors\Collect-CEData.ps1'
            'scripts\collectors\Collect-FOData.ps1'
            'scripts\collectors\Collect-EnvironmentList.ps1'
            'scripts\collectors\Collect-MakerInventory.ps1'
            'scripts\collectors\Collect-TenantGovernance.ps1'
            'scripts\collectors\Collect-RBAC.ps1'
            'scripts\collectors\Collect-MetadataDepth.ps1'
            'scripts\collectors\Collect-Activity.ps1'
            'scripts\00_Prerequisites.ps1'
            'Start-Inventory.ps1'
        ) {
            $fullPath = Join-Path $repoRoot $_
            Test-Path $fullPath | Should -BeTrue -Because "file should exist: $_"

            $errors = $null
            $null = [System.Management.Automation.Language.Parser]::ParseFile(
                $fullPath, [ref]$null, [ref]$errors
            )
            $errors | Should -BeNullOrEmpty -Because "parse errors in $_"
        }
    }

    Context 'All discovered scripts parse cleanly' {
        It 'parses without error: <_.Name>' -ForEach $scriptFiles {
            $errors = $null
            $null = [System.Management.Automation.Language.Parser]::ParseFile(
                $_.FullName, [ref]$null, [ref]$errors
            )
            $errors | Should -BeNullOrEmpty -Because "parse error in $($_.FullName)"
        }
    }
}
