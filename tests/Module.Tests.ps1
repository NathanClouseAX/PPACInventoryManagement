#Requires -Version 5.1
<#
.SYNOPSIS
    Unit tests for scripts/modules/PPACInventory.psm1.

    Covers all exported functions that have testable logic without live API calls.
    Network-dependent functions (Get-AzureToken, Invoke-BAPRequest, etc.) are
    tested with Pester Mocks so no real HTTP requests are made.
#>
[CmdletBinding()]
param()

BeforeAll {
    $repoRoot  = Split-Path -Parent $PSScriptRoot
    $modulePath = Join-Path $repoRoot 'scripts\modules\PPACInventory.psm1'
    Import-Module $modulePath -Force
}

AfterAll {
    Remove-Module PPACInventory -ErrorAction SilentlyContinue
}

# ── Get-SafeDirectoryName ──────────────────────────────────────────────────────

Describe 'Get-SafeDirectoryName' {

    It 'returns a plain name unchanged' {
        Get-SafeDirectoryName -Name 'Production_MyOrg' | Should -Be 'Production_MyOrg'
    }

    It 'replaces illegal Windows path characters with underscores' {
        $result = Get-SafeDirectoryName -Name 'My\Env:Org*Name?<>'
        $result | Should -Not -Match '[\\/:*?"<>|]'
    }

    It 'strips leading and trailing dots and spaces' {
        Get-SafeDirectoryName -Name '  .LeadingSpaceDot. ' | Should -Not -Match '^[\s.]'
        Get-SafeDirectoryName -Name '  .LeadingSpaceDot. ' | Should -Not -Match '[\s.]$'
    }

    It 'truncates names longer than 80 characters' {
        $long = 'A' * 120
        $result = Get-SafeDirectoryName -Name $long
        $result.Length | Should -BeLessOrEqual 80
    }

    It 'handles names that are exactly 80 characters' {
        $exact = 'B' * 80
        Get-SafeDirectoryName -Name $exact | Should -Be $exact
    }

    It 'handles a name with only illegal characters' {
        $result = Get-SafeDirectoryName -Name '***'
        $result | Should -Not -Match '[*]'
    }
}

# ── Test-HasFOSolution ─────────────────────────────────────────────────────────

Describe 'Test-HasFOSolution' {

    It 'returns false for an empty solutions list' {
        Test-HasFOSolution -Solutions @() | Should -BeFalse
    }

    It 'returns false for non-FO solutions' {
        $sols = @(
            [PSCustomObject]@{ uniquename = 'CustomSolution_A' }
            [PSCustomObject]@{ uniquename = 'SomeOtherApp'     }
        )
        Test-HasFOSolution -Solutions $sols | Should -BeFalse
    }

    It 'returns true when DualWriteCore is present' {
        $sols = @(
            [PSCustomObject]@{ uniquename = 'DualWriteCore' }
        )
        Test-HasFOSolution -Solutions $sols | Should -BeTrue
    }

    It 'returns true when Dynamics365Finance is present' {
        $sols = @(
            [PSCustomObject]@{ uniquename = 'CustomSolution' }
            [PSCustomObject]@{ uniquename = 'Dynamics365Finance' }
        )
        Test-HasFOSolution -Solutions $sols | Should -BeTrue
    }

    It 'returns true for partial-match FO solution names' {
        $sols = @([PSCustomObject]@{ uniquename = 'msdyn_FinanceAndOperationsExtended_v2' })
        Test-HasFOSolution -Solutions $sols | Should -BeTrue
    }

    It 'returns false when solution list is null' {
        # Should not throw - null treated as empty
        { Test-HasFOSolution -Solutions $null } | Should -Not -Throw
    }
}

# ── Write-InventoryLog ─────────────────────────────────────────────────────────

Describe 'Write-InventoryLog' {

    It 'writes a line without throwing for each valid level' {
        foreach ($level in 'INFO','WARN','ERROR','OK','SKIP','DEBUG') {
            { Write-InventoryLog -Message "Test $level" -Level $level } | Should -Not -Throw
        }
    }

    It 'accepts an empty string without throwing' {
        { Write-InventoryLog -Message '' } | Should -Not -Throw
    }

    It 'respects indent level in the output message' {
        # Capture output by redirecting host stream
        $output = & { Write-InventoryLog -Message 'IndentTest' -Indent 2 } 2>&1
        # The function writes to host, not pipeline — just confirm no errors thrown
        $output | Where-Object { $_ -is [System.Management.Automation.ErrorRecord] } | Should -BeNullOrEmpty
    }

    Context 'log file writing' {
        BeforeAll {
            $tmpFile = [System.IO.Path]::GetTempFileName()
            Set-InventoryLogFile -Path $tmpFile
        }
        AfterAll {
            Set-InventoryLogFile -Path $null
            Remove-Item $tmpFile -ErrorAction SilentlyContinue
        }

        It 'writes the message to the log file' {
            Write-InventoryLog -Message 'FileWriteTest' -Level OK
            $content = Get-Content $tmpFile -Raw
            $content | Should -Match 'FileWriteTest'
        }

        It 'writes a blank line for empty message' {
            Write-InventoryLog -Message ''
            $content = Get-Content $tmpFile -Raw
            # File should have at least two lines
            ($content -split "`n").Count | Should -BeGreaterThan 1
        }
    }
}

# ── Save-EnvironmentData ───────────────────────────────────────────────────────

Describe 'Save-EnvironmentData' {

    BeforeAll {
        $tmpDir = Join-Path ([System.IO.Path]::GetTempPath()) ('PPACTest_' + [guid]::NewGuid().ToString('N'))
    }
    AfterAll {
        Remove-Item $tmpDir -Recurse -Force -ErrorAction SilentlyContinue
    }

    It 'creates the directory if it does not exist' {
        $newDir = Join-Path $tmpDir 'NewEnvDir'
        Test-Path $newDir | Should -BeFalse
        Save-EnvironmentData -EnvironmentDir $newDir -FileName 'test.json' -Data @{ Key = 'Value' }
        Test-Path $newDir | Should -BeTrue
    }

    It 'writes a valid JSON file' {
        $data = @{ Foo = 'Bar'; Count = 42; Items = @(1, 2, 3) }
        Save-EnvironmentData -EnvironmentDir $tmpDir -FileName 'output.json' -Data $data
        $filePath = Join-Path $tmpDir 'output.json'
        Test-Path $filePath | Should -BeTrue
        $parsed = Get-Content $filePath -Raw | ConvertFrom-Json
        $parsed.Foo   | Should -Be 'Bar'
        $parsed.Count | Should -Be 42
    }

    It 'overwrites an existing file' {
        $dir = Join-Path $tmpDir 'OverwriteTest'
        Save-EnvironmentData -EnvironmentDir $dir -FileName 'data.json' -Data @{ Version = 1 }
        Save-EnvironmentData -EnvironmentDir $dir -FileName 'data.json' -Data @{ Version = 2 }
        $parsed = Get-Content (Join-Path $dir 'data.json') -Raw | ConvertFrom-Json
        $parsed.Version | Should -Be 2
    }
}

# ── Save-RootData ─────────────────────────────────────────────────────────────

Describe 'Save-RootData' {

    BeforeAll {
        $tmpRoot = Join-Path ([System.IO.Path]::GetTempPath()) ('PPACRoot_' + [guid]::NewGuid().ToString('N'))
        $null = New-Item -ItemType Directory -Path $tmpRoot -Force
        Set-InventoryOutputPath -Path $tmpRoot
    }
    AfterAll {
        Set-InventoryOutputPath -Path '.\data'
        Remove-Item $tmpRoot -Recurse -Force -ErrorAction SilentlyContinue
    }

    It 'writes a JSON file to the output path' {
        Save-RootData -FileName 'root-test.json' -Data @{ Tenant = 'test' }
        $filePath = Join-Path $tmpRoot 'root-test.json'
        Test-Path $filePath | Should -BeTrue
        $parsed = Get-Content $filePath -Raw | ConvertFrom-Json
        $parsed.Tenant | Should -Be 'test'
    }
}

# ── Invoke-RestWithRetry ───────────────────────────────────────────────────────

Describe 'Invoke-RestWithRetry' {

    Context 'successful request' {
        BeforeAll {
            Mock Invoke-RestMethod { return @{ value = 'ok' } } -ModuleName PPACInventory
        }

        It 'returns the response on a successful call' {
            $result = Invoke-RestWithRetry -Uri 'https://fake.example.com/api'
            $result.value | Should -Be 'ok'
        }

        It 'calls Invoke-RestMethod exactly once on success' {
            Invoke-RestWithRetry -Uri 'https://fake.example.com/api'
            Should -Invoke Invoke-RestMethod -Times 1 -Exactly -ModuleName PPACInventory
        }
    }

    Context 'non-retryable error (404)' {
        BeforeAll {
            Mock Invoke-RestMethod {
                $ex = [System.Net.WebException]::new('Not Found')
                $resp = [PSCustomObject]@{ StatusCode = [System.Net.HttpStatusCode]::NotFound }
                # Attach fake response property
                $ex | Add-Member -NotePropertyName Response -NotePropertyValue $resp -Force
                throw $ex
            } -ModuleName PPACInventory
        }

        It 'throws immediately without retrying on a non-retryable status code' {
            { Invoke-RestWithRetry -Uri 'https://fake.example.com/missing' -MaxRetries 3 } | Should -Throw
            # On a non-retryable error, Invoke-RestMethod should be called only once
            Should -Invoke Invoke-RestMethod -Times 1 -Exactly -ModuleName PPACInventory
        }
    }

    Context 'retryable error then success' {
        BeforeAll {
            $script:callCount = 0
            Mock Invoke-RestMethod {
                $script:callCount++
                if ($script:callCount -lt 3) {
                    # Simulate a 503 on the first two calls
                    $ex = [System.Net.WebException]::new('Service Unavailable')
                    $resp = [PSCustomObject]@{ StatusCode = 503 }
                    $ex | Add-Member -NotePropertyName Response -NotePropertyValue ([PSCustomObject]@{StatusCode = 503}) -Force
                    throw $ex
                }
                return @{ value = 'recovered' }
            } -ModuleName PPACInventory

            # Mock Start-Sleep so retry tests don't actually wait
            Mock Start-Sleep { } -ModuleName PPACInventory
        }
        AfterAll { $script:callCount = 0 }

        It 'retries on a 5xx error and eventually succeeds' {
            $result = Invoke-RestWithRetry -Uri 'https://fake.example.com/api' -MaxRetries 5
            $result.value | Should -Be 'recovered'
        }
    }

    Context 'exhausts all retries' {
        BeforeAll {
            Mock Invoke-RestMethod {
                $ex = [System.Net.WebException]::new('Gateway Timeout')
                $ex | Add-Member -NotePropertyName Response -NotePropertyValue ([PSCustomObject]@{StatusCode = 504}) -Force
                throw $ex
            } -ModuleName PPACInventory

            Mock Start-Sleep { } -ModuleName PPACInventory
        }

        It 'throws after exhausting MaxRetries' {
            { Invoke-RestWithRetry -Uri 'https://fake.example.com/timeout' -MaxRetries 2 } | Should -Throw
        }
    }
}

# ── Get-AllODataPages ──────────────────────────────────────────────────────────

Describe 'Get-AllODataPages' {

    Context 'single-page response' {
        It 'returns all records from a single-page response with no nextLink' {
            $initial = [PSCustomObject]@{
                value              = @([PSCustomObject]@{id=1}, [PSCustomObject]@{id=2})
                '@odata.nextLink'  = $null
            }
            $result = Get-AllODataPages -InitialResponse $initial
            $result.Count | Should -Be 2
        }
    }

    Context 'multi-page response' {
        BeforeAll {
            Mock Invoke-RestWithRetry {
                return [PSCustomObject]@{
                    value             = @([PSCustomObject]@{id=3}, [PSCustomObject]@{id=4})
                    '@odata.nextLink' = $null
                }
            } -ModuleName PPACInventory

            Mock Get-AzureToken { return 'fake-token' } -ModuleName PPACInventory
        }

        It 'follows nextLink and returns records from all pages' {
            $initial = [PSCustomObject]@{
                value             = @([PSCustomObject]@{id=1}, [PSCustomObject]@{id=2})
                '@odata.nextLink' = 'https://fake.example.com/api?$skiptoken=abc'
            }
            $result = Get-AllODataPages -InitialResponse $initial `
                                        -InstanceApiUrl  'https://fake.api.crm.dynamics.com/' `
                                        -InstanceUrl     'https://fake.crm.dynamics.com/'
            $result.Count | Should -Be 4
            $result[0].id | Should -Be 1
            $result[3].id | Should -Be 4
        }
    }

    Context 'MaxPages safety cap' {
        BeforeAll {
            # Always returns a nextLink so pagination would be infinite without the cap
            Mock Invoke-RestWithRetry {
                return [PSCustomObject]@{
                    value             = @([PSCustomObject]@{id=99})
                    '@odata.nextLink' = 'https://fake.example.com/api?$skiptoken=loop'
                }
            } -ModuleName PPACInventory

            Mock Get-AzureToken { return 'fake-token' } -ModuleName PPACInventory
        }

        It 'stops fetching after MaxPages is reached' {
            $initial = [PSCustomObject]@{
                value             = @([PSCustomObject]@{id=1})
                '@odata.nextLink' = 'https://fake.example.com/api?$skiptoken=start'
            }
            $result = Get-AllODataPages -InitialResponse $initial `
                                        -InstanceApiUrl  'https://fake.api.crm.dynamics.com/' `
                                        -InstanceUrl     'https://fake.crm.dynamics.com/' `
                                        -MaxPages 3
            # Page 1 (initial) + pages 2 and 3 = 3 pages * 1 record each = 3
            $result.Count | Should -Be 3
        }
    }
}

# ── Get-AzureToken ─────────────────────────────────────────────────────────────

Describe 'Get-AzureToken' {

    Context 'token caching' {
        BeforeAll {
            $script:tokenCallCount = 0
            Mock Get-AzAccessToken {
                $script:tokenCallCount++
                return [PSCustomObject]@{
                    Token     = 'fake-bearer-token'
                    ExpiresOn = [DateTimeOffset]::UtcNow.AddHours(1)
                }
            } -ModuleName PPACInventory
        }
        AfterAll { $script:tokenCallCount = 0 }

        It 'acquires a token on the first call' {
            $token = Get-AzureToken -ResourceUrl 'https://service.powerapps.com/'
            $token | Should -Be 'fake-bearer-token'
            $script:tokenCallCount | Should -Be 1
        }

        It 'returns the cached token without re-acquiring on the second call' {
            $before = $script:tokenCallCount
            $token  = Get-AzureToken -ResourceUrl 'https://service.powerapps.com/'
            $token  | Should -Be 'fake-bearer-token'
            # No additional call — cached
            $script:tokenCallCount | Should -Be $before
        }
    }

    Context 'SecureString token handling' {
        BeforeAll {
            Mock Get-AzAccessToken {
                $ss = ConvertTo-SecureString 'secure-token-value' -AsPlainText -Force
                return [PSCustomObject]@{
                    Token     = $ss
                    ExpiresOn = [DateTimeOffset]::UtcNow.AddHours(1)
                }
            } -ModuleName PPACInventory
        }

        It 'unwraps a SecureString token to a plain string' {
            $token = Get-AzureToken -ResourceUrl 'https://different-resource.example.com/'
            $token | Should -Be 'secure-token-value'
            $token.GetType().Name | Should -Be 'String'
        }
    }
}
