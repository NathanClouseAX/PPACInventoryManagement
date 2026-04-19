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

# ── API endpoint / version pins ─────────────────────────────────────────────────
# Pinned in one place so schema drift becomes a one-line change.
$script:BAPBaseUrl         = 'https://api.bap.microsoft.com'
$script:PowerAppsApiUrl    = 'https://api.powerapps.com'
$script:PowerAppsResource  = 'https://service.powerapps.com/'
$script:GraphBaseUrl       = 'https://graph.microsoft.com/v1.0'
$script:GraphResource      = 'https://graph.microsoft.com/'
$script:BAPApiVersion      = '2021-04-01'
$script:BAPApiVersionPrev  = '2024-05-01'
$script:BAPApiVersion2022  = '2022-11-01'
$script:PowerAppsApiVer    = '2022-11-01'

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

    # Rotate when the existing log exceeds 50 MB. Keeps up to 5 historical copies
    # ($Path.1 newest .. $Path.5 oldest); .5 is dropped on each rotation. Caps disk
    # use at ~300 MB per log family (current + 5 rolls) so an unattended cron run
    # over months doesn't fill the disk, while preserving enough recent history to
    # diagnose a failure after the fact.
    if ($Path -and (Test-Path -LiteralPath $Path)) {
        try {
            $sizeBytes = (Get-Item -LiteralPath $Path).Length
            if ($sizeBytes -gt 50MB) {
                for ($i = 4; $i -ge 1; $i--) {
                    $src = "$Path.$i"
                    $dst = "$Path.$($i + 1)"
                    if (Test-Path -LiteralPath $src) {
                        Move-Item -LiteralPath $src -Destination $dst -Force
                    }
                }
                Move-Item -LiteralPath $Path -Destination "$Path.1" -Force
            }
        } catch {
            # Rotation must never block logging — if Move-Item fails (e.g. another
            # process holds the file open), continue with the existing log.
            Write-Warning "Log rotation skipped for ${Path}: $($_.Exception.Message)"
        }
    }

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

    # Retry token acquisition on transient failures (STS throttling, brief network
    # hiccups, token-broker restart). Each attempt goes through Az.Accounts' MSAL
    # cache so only the first real acquire costs; re-tries are almost free on hit.
    $tokenResult = $null
    $tokenAttempt = 0
    $tokenMaxRetries = 3
    while ($true) {
        try {
            $tokenResult = Get-AzAccessToken -ResourceUrl $key -ErrorAction Stop
            break
        } catch {
            if ($tokenAttempt -ge $tokenMaxRetries) { throw }
            $delay = [Math]::Pow(2, $tokenAttempt) * 2 + (Get-Random -Minimum 0 -Maximum 2)
            Write-InventoryLog ("Token acquisition failed for {0} (attempt {1}/{2}): {3}. Retrying in {4}s..." -f `
                $key, ($tokenAttempt + 1), $tokenMaxRetries, $_.Exception.Message, $delay) -Level WARN
            Start-Sleep -Seconds $delay
            $tokenAttempt++
        }
    }

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

            # Base delay is exponential (3, 6, 12, 24, 48s). Jitter 0-3s is added
            # so parallel workers don't re-synchronize on the same backoff wave
            # when a whole tenant hits 429 at once. If the server sent Retry-After
            # we honor it exactly (plus 1s guard) - no jitter in that case, since
            # the server already told us when to come back.
            $delay = [Math]::Pow(2, $attempt) * 3
            if ($statusCode -eq 429) {
                try {
                    $ra = $_.Exception.Response.Headers['Retry-After']
                    if ($ra) { $delay = [int]$ra + 1 }
                } catch {}
                Write-InventoryLog "Rate limited (429). Waiting ${delay}s (attempt $($attempt+1)/$MaxRetries)..." -Level WARN
            } else {
                $delay += Get-Random -Minimum 0 -Maximum 3
                Write-InventoryLog "HTTP $statusCode on attempt $($attempt+1). Retrying in ${delay}s..." -Level WARN
            }

            Start-Sleep -Seconds $delay
            $attempt++
        }
    }
}

function Get-HttpErrorClassification {
    <#
    .SYNOPSIS
        Classifies a caught exception from a REST call into a stable category.
    .DESCRIPTION
        Collectors previously recorded every REST failure as a generic
        QUERY_FAILED note. This helper maps an exception to one of:
          ACCESS_DENIED        - 401/403 (missing permission, token scope)
          NOT_FOUND            - 404 (entity/endpoint absent on this env)
          FEATURE_NOT_ENABLED  - 404 + body mentions 'not enabled' / 'not provisioned'
          TIMEOUT              - request timeout (network-level)
          RATE_LIMITED         - 429 (already retried upstream; still failed)
          SERVER_ERROR         - 5xx (upstream instability)
          INVALID_QUERY        - 400 with OData error code (bad filter / select)
          UNKNOWN_ERROR        - anything else
        The returned hashtable is intended to be placed directly into a
        section's Notes array so downstream reporting can differentiate real
        problems from "feature not in use on this SKU".
    #>
    [CmdletBinding()]
    param([Parameter(Mandatory)][System.Management.Automation.ErrorRecord]$ErrorRecord)

    $status = 0
    try {
        if ($ErrorRecord.Exception -and $ErrorRecord.Exception.Response) {
            $status = [int]$ErrorRecord.Exception.Response.StatusCode
        }
    } catch {}

    $body = ''
    try { $body = $ErrorRecord.ErrorDetails.Message } catch {}
    if (-not $body) {
        try { $body = $ErrorRecord.Exception.Message } catch {}
    }
    if (-not $body) { $body = '' }

    $category = switch ($status) {
        401 { 'ACCESS_DENIED' }
        403 { 'ACCESS_DENIED' }
        404 {
            if ($body -match 'not enabled|not provisioned|not configured|not available') {
                'FEATURE_NOT_ENABLED'
            } else { 'NOT_FOUND' }
        }
        408 { 'TIMEOUT' }
        429 { 'RATE_LIMITED' }
        default {
            if ($status -ge 500 -and $status -lt 600) { 'SERVER_ERROR' }
            elseif ($status -eq 400) { 'INVALID_QUERY' }
            elseif ($body -match 'timeout|timed out|operation has timed out') { 'TIMEOUT' }
            else { 'UNKNOWN_ERROR' }
        }
    }

    # First line of the body (usually the most actionable) for context in reports.
    $firstLine = ($body -split "`r?`n" | Where-Object { $_ -and $_.Trim() } | Select-Object -First 1)
    if ($firstLine -and $firstLine.Length -gt 280) { $firstLine = $firstLine.Substring(0, 280) + '...' }

    return @{
        Category = $category
        Status   = $status
        Message  = $firstLine
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

    $token = Get-AzureToken -ResourceUrl $script:PowerAppsResource
    $sep   = if ($Path -match '\?') { '&' } else { '?' }
    $uri   = "$($script:BAPBaseUrl)$Path${sep}api-version=$ApiVersion"
    if ($ExtraQuery) { $uri += "&$ExtraQuery" }

    $headers = @{
        Authorization  = "Bearer $token"
        'Content-Type' = 'application/json'
    }

    Write-InventoryLog "BAP GET $uri" -Level DEBUG

    return Invoke-RestWithRetry -Uri $uri -Headers $headers -Method $Method `
                                -Body $Body -TimeoutSec $TimeoutSec
}

function Invoke-PowerAppsAdminRequest {
    <#
    .SYNOPSIS
        Calls the PowerApps Admin API (api.powerapps.com) — distinct host from
        api.bap.microsoft.com. Some maker-surface endpoints (apps, flows v2,
        connections) return richer data here than via BAP.
    .PARAMETER Path
        Path after the host, e.g.
        "/providers/Microsoft.PowerApps/scopes/admin/environments/{id}/apps"
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$Path,
        [string]$ApiVersion = $script:PowerAppsApiVer,
        [string]$ExtraQuery = '',
        [string]$Method     = 'GET',
        [object]$Body       = $null,
        [int]   $TimeoutSec = 120
    )

    $token = Get-AzureToken -ResourceUrl $script:PowerAppsResource
    $sep   = if ($Path -match '\?') { '&' } else { '?' }
    $uri   = "$($script:PowerAppsApiUrl)$Path${sep}api-version=$ApiVersion"
    if ($ExtraQuery) { $uri += "&$ExtraQuery" }

    $headers = @{
        Authorization  = "Bearer $token"
        'Content-Type' = 'application/json'
    }

    Write-InventoryLog "PowerAppsAdmin GET $uri" -Level DEBUG

    return Invoke-RestWithRetry -Uri $uri -Headers $headers -Method $Method `
                                -Body $Body -TimeoutSec $TimeoutSec
}

function Invoke-GraphRequest {
    <#
    .SYNOPSIS
        Calls Microsoft Graph v1.0. Used for resolving AAD groups, users, and
        service principals that Dataverse/BAP only reference by object id.
    .PARAMETER Path
        Relative path, e.g. "/groups/{id}" or "/users/{id}?$select=id,displayName".
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$Path,
        [string]$Method     = 'GET',
        [object]$Body       = $null,
        [int]   $TimeoutSec = 60
    )

    $token = Get-AzureToken -ResourceUrl $script:GraphResource
    $uri   = "$($script:GraphBaseUrl)$Path"
    $headers = @{
        Authorization  = "Bearer $token"
        'Content-Type' = 'application/json'
    }
    Write-InventoryLog "Graph GET $uri" -Level DEBUG
    return Invoke-RestWithRetry -Uri $uri -Headers $headers -Method $Method `
                                -Body $Body -TimeoutSec $TimeoutSec
}

function Get-AllBAPPages {
    <#
    .SYNOPSIS
        Follows @odata.nextLink / nextLink pagination on BAP/PowerApps Admin
        responses. Returns all records as a flat list.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][object]$InitialResponse,
        [int]$MaxPages = 50
    )

    $all  = [System.Collections.Generic.List[object]]::new()
    $resp = $InitialResponse
    $page = 1

    while ($true) {
        if ($resp -and $resp.PSObject.Properties.Name -contains 'value' -and $resp.value) {
            $all.AddRange([object[]]$resp.value)
        }

        $nextLink = $null
        if ($resp) {
            foreach ($linkProp in '@odata.nextLink','nextLink') {
                if ($resp.PSObject.Properties[$linkProp] -and $resp.$linkProp) {
                    $nextLink = $resp.$linkProp
                    break
                }
            }
        }
        if (-not $nextLink -or $page -ge $MaxPages) { break }

        $token   = Get-AzureToken -ResourceUrl $script:PowerAppsResource
        $headers = @{ Authorization = "Bearer $token" }
        $resp    = Invoke-RestWithRetry -Uri $nextLink -Headers $headers
        $page++
    }

    return ,$all
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
        if ($resp -and $resp.PSObject.Properties.Name -contains 'value' -and $resp.value) {
            $all.AddRange([object[]]$resp.value)
        }

        # Under Set-StrictMode -Version Latest, accessing a missing property throws,
        # so we probe via PSObject.Properties before reading the nextLink.
        $nextLink = $null
        if ($resp -and $resp.PSObject.Properties['@odata.nextLink']) {
            $nextLink = $resp.'@odata.nextLink'
        }
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
    # Use -InputObject instead of piping. Piping enumerates arrays and PS 5.1's
    # ConvertTo-Json then wraps multi-element pipeline input as {value:[...],Count:N}
    # rather than a bare JSON array, which breaks any reader expecting an array.
    try {
        ConvertTo-Json -InputObject $Data -Depth 15 -Compress:$false | Set-Content -Path $outPath -Encoding UTF8 -Force
    } catch {
        # Re-throw with path + exception type so the per-collector try/catch in
        # Collect-OneEnvironment.ps1 records actionable context (disk full vs
        # locked file vs permission denied) instead of a bare 'Set-Content failed'.
        $exType = $_.Exception.GetType().FullName
        throw "Save-EnvironmentData failed writing '$outPath' [$exType]: $($_.Exception.Message)"
    }
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
    # See Save-EnvironmentData for why -InputObject (not pipe) is required here.
    try {
        ConvertTo-Json -InputObject $Data -Depth 15 | Set-Content -Path $outPath -Encoding UTF8 -Force
    } catch {
        $exType = $_.Exception.GetType().FullName
        throw "Save-RootData failed writing '$outPath' [$exType]: $($_.Exception.Message)"
    }
    Write-InventoryLog "Saved: $outPath" -Level DEBUG
}

# ── Dataverse entity count helper ─────────────────────────────────────────────

function Get-DataverseEntityCount {
    <#
    .SYNOPSIS
        Returns the OData count for an entity set without retrieving any records.
    .DESCRIPTION
        Returns a PSCustomObject with Uri, Count, Error, HttpStatus, ElapsedMs so
        callers can both read the count and write a trace log entry. Count is -1
        when the call fails.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InstanceApiUrl,
        [Parameter(Mandatory)][string]$EntitySetName,
        [string]$InstanceUrl = '',
        [int]   $TimeoutSec  = 60
    )

    # $count=true returns the total via @odata.count; $top=1 bounds the sample row.
    # We intentionally omit $select: naive pluralization rules (e.g. stripping a
    # trailing 's') produce invalid PK names for tables whose set ends in 'ies'
    # (F&O virtual tables like mserp_*entities), causing every count to 400.
    $uri   = "$($InstanceApiUrl.TrimEnd('/'))/api/data/v9.2/${EntitySetName}?`$count=true&`$top=1"
    $sw    = [System.Diagnostics.Stopwatch]::StartNew()
    $cnt        = -1
    $errMsg     = $null
    $httpStatus = $null

    try {
        $token = Get-AzureToken -ResourceUrl ($InstanceUrl.TrimEnd('/') + '/')
        $headers = @{
            Authorization      = "Bearer $token"
            'OData-MaxVersion' = '4.0'
            'OData-Version'    = '4.0'
            Accept             = 'application/json'
            Prefer             = 'odata.include-annotations="*"'
        }

        $resp = Invoke-RestWithRetry -Uri $uri -Headers $headers -TimeoutSec $TimeoutSec
        $httpStatus = 200
        if ($null -ne $resp.'@odata.count') {
            $cnt = [int64]$resp.'@odata.count'
        } elseif ($resp.value) {
            $cnt = [int64]$resp.value.Count
        } else {
            $cnt = 0
        }
    }
    catch {
        $errMsg = "$($_.Exception.Message)"
        if ($_.Exception -and $_.Exception.Response -and $_.Exception.Response.StatusCode) {
            try { $httpStatus = [int]$_.Exception.Response.StatusCode } catch { }
        }
    }
    $sw.Stop()

    [PSCustomObject]@{
        Uri        = $uri
        Count      = $cnt
        Error      = $errMsg
        HttpStatus = $httpStatus
        ElapsedMs  = [int]$sw.ElapsedMilliseconds
    }
}

# ── F&O integration detection ──────────────────────────────────────────────────

function Get-FOIntegrationDetails {
    <#
    .SYNOPSIS
        Authoritative Finance & Operations integration check for a Dataverse
        environment. Calls the Dataverse Web API unbound function
        RetrieveFinanceAndOperationsIntegrationDetails — the same API Microsoft
        recommends for verifying Power Platform integration at runtime.
    .DESCRIPTION
        When the Dataverse org is linked to an F&O environment, the action
        returns the F&O URL plus tenant/environment IDs. When it isn't linked,
        the API returns error code 0x80048d0b ("Dataverse instance isn't
        integrated with finance and operations.") — we treat that as a normal
        "no F&O" result rather than an error.

        Any other failure (403/404/timeout) is logged and returns HasFO=$false
        so downstream collection is skipped conservatively.
    .LINK
        https://learn.microsoft.com/en-us/power-apps/developer/data-platform/webapi/reference/retrievefinanceandoperationsintegrationdetailsresponse
    .LINK
        https://learn.microsoft.com/en-us/dynamics365/fin-ops-core/dev-itpro/power-platform/enable-power-platform-integration
    .OUTPUTS
        Hashtable with keys: HasFO, FOUrl, FOEnvironmentId, FOTenantId,
        IsUnifiedDatabase, OrgLifecycleStatus.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InstanceApiUrl,
        [string]$InstanceUrl = '',
        [int]   $TimeoutSec  = 60
    )

    $notIntegrated = @{
        HasFO              = $false
        FOUrl              = $null
        FOEnvironmentId    = $null
        FOTenantId         = $null
        IsUnifiedDatabase  = $false
        OrgLifecycleStatus = $null
    }

    try {
        $resp = Invoke-DataverseRequest `
            -InstanceApiUrl $InstanceApiUrl `
            -InstanceUrl    $InstanceUrl `
            -ODataPath      'RetrieveFinanceAndOperationsIntegrationDetails' `
            -TimeoutSec     $TimeoutSec
    } catch {
        $raw = ''
        try { $raw = $_.ErrorDetails.Message } catch {}
        if (-not $raw) { $raw = $_.Exception.Message }

        if ($raw -match '0x80048d0b' -or $raw -match "isn't integrated with finance and operations") {
            return $notIntegrated
        }

        Write-InventoryLog "RetrieveFinanceAndOperationsIntegrationDetails failed: $raw" -Level WARN -Indent 2
        return $notIntegrated
    }

    if (-not $resp) { return $notIntegrated }

    # Strict-mode-safe property access: unbound-function responses are PSCustomObject.
    $getProp = {
        param($obj, $name)
        if ($null -eq $obj) { return $null }
        $p = $obj.PSObject.Properties[$name]
        if ($p) { return $p.Value }
        return $null
    }

    $url = & $getProp $resp 'Url'
    if (-not $url) { return $notIntegrated }

    return @{
        HasFO              = $true
        FOUrl              = [string]$url
        FOEnvironmentId    = [string](& $getProp $resp 'Id')
        FOTenantId         = [string](& $getProp $resp 'TenantId')
        IsUnifiedDatabase  = [bool]  (& $getProp $resp 'IsUnifiedDatabase')
        OrgLifecycleStatus = [string](& $getProp $resp 'OrgLifecycleStatus')
    }
}

Export-ModuleMember -Function @(
    'Write-InventoryLog'
    'Set-InventoryLogFile'
    'Set-InventoryOutputPath'
    'Get-AzureToken'
    'Get-HttpErrorClassification'
    'Invoke-BAPRequest'
    'Invoke-PowerAppsAdminRequest'
    'Invoke-GraphRequest'
    'Invoke-DataverseRequest'
    'Get-AllODataPages'
    'Get-AllBAPPages'
    'Save-EnvironmentData'
    'Save-RootData'
    'Get-SafeDirectoryName'
    'Get-DataverseEntityCount'
    'Get-FOIntegrationDetails'
    'Invoke-RestWithRetry'
)
