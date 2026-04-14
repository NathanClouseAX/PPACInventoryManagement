#Requires -Version 5.1
<#
.SYNOPSIS
    Shared helper functions for PPAC Dataverse Inventory scripts.
.DESCRIPTION
    Provides authentication, REST call wrappers with retry/rate-limit handling,
    OData pagination, and structured JSON output helpers.

    Import with:  Import-Module .\scripts\modules\PPACInventory.psm1 -Force
#>

Set-StrictMode -Version Latest

# ── Module-scope state ──────────────────────────────────────────────────────────
$script:TokenCache   = @{}           # [resourceUrl] -> @{Token=...; Expiry=...}
$script:LogFile      = $null         # Set by calling script
$script:OutputPath   = '.\data'      # Overridden by orchestrator
$script:Verbose      = $false

# ── Logging ─────────────────────────────────────────────────────────────────────

function Write-InventoryLog {
    <#
    .SYNOPSIS  Writes a timestamped log line to console and optionally to a file.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][AllowEmptyString()][string]$Message,
        [ValidateSet('INFO','WARN','ERROR','DEBUG','OK','SKIP')]
        [string]$Level = 'INFO',
        [int]$Indent = 0
    )
    # Empty message = blank separator line
    if ($Message -eq '') { Write-Host ''; if ($script:LogFile) { Add-Content -Path $script:LogFile -Value '' -Encoding UTF8 }; return }
    $prefix = '  ' * $Indent
    $ts     = Get-Date -Format 'HH:mm:ss'
    $line   = "$ts  [$Level]  $prefix$Message"

    $color = switch ($Level) {
        'OK'    { 'Green'   }
        'WARN'  { 'Yellow'  }
        'ERROR' { 'Red'     }
        'DEBUG' { 'DarkGray'}
        'SKIP'  { 'DarkGray'}
        default { 'White'   }
    }
    Write-Host $line -ForegroundColor $color

    if ($script:LogFile) {
        Add-Content -Path $script:LogFile -Value $line -Encoding UTF8
    }
}

function Set-InventoryLogFile {
    param([string]$Path)
    $script:LogFile = $Path
}

function Set-InventoryOutputPath {
    param([string]$Path)
    $script:OutputPath = $Path
}

# ── Token acquisition ─────────────────────────────────────────────────────────

function Get-AzureToken {
    <#
    .SYNOPSIS
        Gets a bearer token for the specified Azure resource, caching it until
        5 minutes before expiry to avoid repeated requests.
    .PARAMETER ResourceUrl
        OAuth resource / audience URL (e.g. "https://service.powerapps.com/").
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$ResourceUrl
    )

    # Normalize: ensure trailing slash for caching key consistency
    $key = $ResourceUrl.TrimEnd('/') + '/'

    # Return cached token if still valid
    if ($script:TokenCache.ContainsKey($key)) {
        $cached = $script:TokenCache[$key]
        if ((Get-Date) -lt $cached.Expiry) {
            return $cached.Token
        }
    }

    Write-InventoryLog "Acquiring token for resource: $key" -Level DEBUG

    $tokenResult = Get-AzAccessToken -ResourceUrl $key -ErrorAction Stop

    # Az.Accounts >= 2.17 returns SecureString by default; handle both
    $rawToken = $tokenResult.Token
    if ($rawToken -is [System.Security.SecureString]) {
        $bstr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($rawToken)
        try   { $rawToken = [System.Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr) }
        finally { [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr) }
    }

    # Cache with 5-min buffer before actual expiry
    $expiry = if ($tokenResult.ExpiresOn) {
        $tokenResult.ExpiresOn.LocalDateTime.AddMinutes(-5)
    } else {
        (Get-Date).AddMinutes(55)
    }

    $script:TokenCache[$key] = @{ Token = $rawToken; Expiry = $expiry }
    return $rawToken
}

# ── HTTP helpers ──────────────────────────────────────────────────────────────

function Invoke-RestWithRetry {
    <#
    .SYNOPSIS
        Calls Invoke-RestMethod with exponential-backoff retry for 429 and 5xx errors.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$Uri,
        [hashtable]$Headers       = @{},
        [string]   $Method        = 'GET',
        [object]   $Body          = $null,
        [string]   $ContentType   = 'application/json',
        [int]      $MaxRetries    = 5,
        [int]      $TimeoutSec    = 120
    )

    $attempt = 0
    while ($true) {
        try {
            $params = @{
                Uri             = $Uri
                Headers         = $Headers
                Method          = $Method
                ContentType     = $ContentType
                TimeoutSec      = $TimeoutSec
                UseBasicParsing = $true
                ErrorAction     = 'Stop'
            }
            if ($Body) { $params['Body'] = ($Body | ConvertTo-Json -Depth 10 -Compress) }

            return Invoke-RestMethod @params
        }
        catch {
            $statusCode = $null
            if ($_.Exception.Response) {
                $statusCode = [int]$_.Exception.Response.StatusCode
            }

            if ($attempt -ge $MaxRetries) {
                throw
            }

            $shouldRetry = $statusCode -in @(429, 500, 502, 503, 504) -or
                           $_.Exception.Message -match 'timeout|connection'

            if (-not $shouldRetry) { throw }

            # Calculate delay: respect Retry-After header if present for 429
            $delay = [Math]::Pow(2, $attempt) * 3   # 3, 6, 12, 24, 48 s
            if ($statusCode -eq 429) {
                try {
                    $ra = $_.Exception.Response.Headers['Retry-After']
                    if ($ra) { $delay = [int]$ra + 1 }
                } catch {}
                Write-InventoryLog "Rate limited (429). Waiting ${delay}s (attempt $($attempt+1)/$MaxRetries)..." -Level WARN
            } else {
                Write-InventoryLog "HTTP $statusCode on attempt $($attempt+1). Retrying in ${delay}s..." -Level WARN
            }

            Start-Sleep -Seconds $delay
            $attempt++
        }
    }
}

function Invoke-BAPRequest {
    <#
    .SYNOPSIS
        Calls the Business Application Platform (Power Platform) admin API.
    .PARAMETER Path
        API path after the base URL, e.g.
        "/providers/Microsoft.BusinessAppPlatform/scopes/admin/environments"
    .PARAMETER ApiVersion
        OData api-version query string value.
    .PARAMETER ExtraQuery
        Additional query string parameters (without leading '&').
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$Path,
        [string]$ApiVersion  = '2021-04-01',
        [string]$ExtraQuery  = '',
        [string]$Method      = 'GET',
        [object]$Body        = $null,
        [int]   $TimeoutSec  = 120
    )

    $token = Get-AzureToken -ResourceUrl 'https://service.powerapps.com/'
    $sep   = if ($Path -match '\?') { '&' } else { '?' }
    $uri   = "https://api.bap.microsoft.com$Path${sep}api-version=$ApiVersion"
    if ($ExtraQuery) { $uri += "&$ExtraQuery" }

    $headers = @{
        Authorization  = "Bearer $token"
        'Content-Type' = 'application/json'
    }

    Write-InventoryLog "BAP GET $uri" -Level DEBUG

    return Invoke-RestWithRetry -Uri $uri -Headers $headers -Method $Method `
                                -Body $Body -TimeoutSec $TimeoutSec
}

function Invoke-DataverseRequest {
    <#
    .SYNOPSIS
        Calls the Dataverse Web API for a specific environment.
    .PARAMETER InstanceApiUrl
        The API root URL, e.g. "https://myorg.api.crm.dynamics.com/"
    .PARAMETER ODataPath
        Path after /api/data/v9.2/, e.g. "systemusers?$top=10"
    .PARAMETER InstanceUrl
        The org URL used as OAuth resource (e.g. "https://myorg.crm.dynamics.com/").
        Defaults to deriving from InstanceApiUrl if not supplied.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InstanceApiUrl,
        [Parameter(Mandatory)][string]$ODataPath,
        [string]$InstanceUrl   = '',
        [string]$Method        = 'GET',
        [object]$Body          = $null,
        [int]   $TimeoutSec    = 180
    )

    # Derive the OAuth resource from the instance URL
    if (-not $InstanceUrl) {
        # api.crm -> crm  (remove the 'api.' prefix segment)
        $InstanceUrl = $InstanceApiUrl -replace 'https://([^.]+)\.api\.', 'https://$1.'
    }

    $token   = Get-AzureToken -ResourceUrl ($InstanceUrl.TrimEnd('/') + '/')
    $baseUri = $InstanceApiUrl.TrimEnd('/') + '/api/data/v9.2/'
    $uri     = $baseUri + $ODataPath.TrimStart('/')

    $headers = @{
        Authorization  = "Bearer $token"
        'OData-MaxVersion' = '4.0'
        'OData-Version'    = '4.0'
        Accept             = 'application/json'
        Prefer             = 'odata.include-annotations="*"'
    }

    Write-InventoryLog "DV GET $uri" -Level DEBUG

    return Invoke-RestWithRetry -Uri $uri -Headers $headers -Method $Method `
                                -Body $Body -TimeoutSec $TimeoutSec
}

function Get-AllODataPages {
    <#
    .SYNOPSIS
        Follows @odata.nextLink pagination, returning all records as a flat list.
    .PARAMETER InitialResponse
        The first OData response object (must have .value property).
    .PARAMETER InstanceApiUrl
        Required for Dataverse calls to get tokens.
    .PARAMETER MaxPages
        Safety cap - stops after N pages (default 200, ~100k records at $top=500).
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][object]$InitialResponse,
        [string]$InstanceApiUrl = '',
        [string]$InstanceUrl    = '',
        [int]   $MaxPages       = 200
    )

    $all   = [System.Collections.Generic.List[object]]::new()
    $page  = 1
    $resp  = $InitialResponse

    while ($true) {
        if ($resp.value) { $all.AddRange([object[]]$resp.value) }

        $nextLink = $resp.'@odata.nextLink'
        if (-not $nextLink -or $page -ge $MaxPages) { break }

        Write-InventoryLog "  Fetching page $($page + 1) ($($all.Count) records so far)..." -Level DEBUG
        $page++

        if ($InstanceApiUrl) {
            $token = Get-AzureToken -ResourceUrl ($InstanceUrl.TrimEnd('/') + '/')
            $headers = @{
                Authorization      = "Bearer $token"
                'OData-MaxVersion' = '4.0'
                'OData-Version'    = '4.0'
                Accept             = 'application/json'
                Prefer             = 'odata.include-annotations="*"'
            }
            $resp = Invoke-RestWithRetry -Uri $nextLink -Headers $headers
        } else {
            $token   = Get-AzureToken -ResourceUrl 'https://service.powerapps.com/'
            $headers = @{ Authorization = "Bearer $token" }
            $resp    = Invoke-RestWithRetry -Uri $nextLink -Headers $headers
        }
    }

    return ,$all   # comma forces array return type
}

# ── Data persistence ──────────────────────────────────────────────────────────

function Get-SafeDirectoryName {
    <#
    .SYNOPSIS  Converts an environment display name to a safe directory name.
    #>
    param([Parameter(Mandatory)][string]$Name)
    # Replace chars illegal on Windows filesystem
    $safe = $Name -replace '[\\/:*?"<>|]', '_'
    $safe = $safe.Trim('. ')
    if ($safe.Length -gt 80) { $safe = $safe.Substring(0, 80) }
    return $safe
}

function Save-EnvironmentData {
    <#
    .SYNOPSIS
        Saves a data object as a UTF-8 JSON file under the environment's data directory.
    .PARAMETER EnvironmentDir
        Full path to the environment's data directory.
    .PARAMETER FileName
        File name without path (e.g. "storage.json").
    .PARAMETER Data
        Object to serialize.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$EnvironmentDir,
        [Parameter(Mandatory)][string]$FileName,
        [Parameter(Mandatory)][object]$Data
    )

    if (-not (Test-Path $EnvironmentDir)) {
        $null = New-Item -ItemType Directory -Path $EnvironmentDir -Force
    }

    $outPath = Join-Path $EnvironmentDir $FileName
    $Data | ConvertTo-Json -Depth 15 -Compress:$false | Set-Content -Path $outPath -Encoding UTF8 -Force
    Write-InventoryLog "  Saved: $outPath" -Level DEBUG
}

function Save-RootData {
    <#
    .SYNOPSIS  Saves a top-level data file (not environment-specific).
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$FileName,
        [Parameter(Mandatory)][object]$Data
    )
    if (-not (Test-Path $script:OutputPath)) {
        $null = New-Item -ItemType Directory -Path $script:OutputPath -Force
    }
    $outPath = Join-Path $script:OutputPath $FileName
    $Data | ConvertTo-Json -Depth 15 | Set-Content -Path $outPath -Encoding UTF8 -Force
    Write-InventoryLog "Saved: $outPath" -Level DEBUG
}

# ── Dataverse entity count helper ─────────────────────────────────────────────

function Get-DataverseEntityCount {
    <#
    .SYNOPSIS
        Returns the OData count for an entity set without retrieving any records.
        Returns -1 if the count cannot be retrieved (security/timeout).
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InstanceApiUrl,
        [Parameter(Mandatory)][string]$EntitySetName,
        [string]$InstanceUrl = '',
        [int]   $TimeoutSec  = 60
    )

    try {
        $token = Get-AzureToken -ResourceUrl ($InstanceUrl.TrimEnd('/') + '/')
        $uri   = "$($InstanceApiUrl.TrimEnd('/'))/api/data/v9.2/${EntitySetName}?`$count=true&`$top=1&`$select=$(($EntitySetName -replace 's$','') + 'id')"

        # Fallback: if field derivation fails, use a minimal fetch
        $headers = @{
            Authorization      = "Bearer $token"
            'OData-MaxVersion' = '4.0'
            'OData-Version'    = '4.0'
            Accept             = 'application/json'
            Prefer             = 'odata.include-annotations="*"'
        }

        $resp = Invoke-RestWithRetry -Uri $uri -Headers $headers -TimeoutSec $TimeoutSec
        if ($null -ne $resp.'@odata.count') { return [int]$resp.'@odata.count' }
        if ($resp.value) { return $resp.value.Count }
        return 0
    }
    catch {
        return -1
    }
}

# ── FO detection helpers ───────────────────────────────────────────────────────

$script:FOSolutionPatterns = @(
    'Dynamics365FinanceOperationsExtended',
    'DualWriteCore',
    'DualWriteFinance',
    'DualWriteSupplyChain',
    'DualWriteHumanResources',
    'DualWriteProject',
    'DualWriteAssetManagement',
    'DualWriteParty',
    'DualWriteNotes',
    'msdyn_FinanceAndOperationsExtended',
    'Dynamics365Finance',
    'Dynamics365SupplyChainManagement'
)

function Test-HasFOSolution {
    <#
    .SYNOPSIS  Returns $true if any installed solution matches an FO pattern.
    #>
    param([object[]]$Solutions)

    foreach ($sol in $Solutions) {
        $name = $sol.uniquename
        foreach ($pattern in $script:FOSolutionPatterns) {
            if ($name -like "*$pattern*") { return $true }
        }
    }
    return $false
}

Export-ModuleMember -Function @(
    'Write-InventoryLog'
    'Set-InventoryLogFile'
    'Set-InventoryOutputPath'
    'Get-AzureToken'
    'Invoke-BAPRequest'
    'Invoke-DataverseRequest'
    'Get-AllODataPages'
    'Save-EnvironmentData'
    'Save-RootData'
    'Get-SafeDirectoryName'
    'Get-DataverseEntityCount'
    'Test-HasFOSolution'
    'Invoke-RestWithRetry'
)
