<#
.SYNOPSIS
    Collects activity telemetry: flow run history, canvas app last-used
    timestamps, maker activity signals. Opt-in via -IncludeActivity because
    these queries can be slow on large environments.
.DESCRIPTION
    Exports Collect-ActivityTelemetry, dot-sourced by
    Invoke-DataverseInventory.ps1. Only runs when -IncludeActivity is passed.

    Per-env output files:
      flow-runs-summary.json
      app-usage-summary.json
      maker-activity.json
      activity-summary.json

    Notable flags:
      NO_MAKER_ACTIVITY_90D                 — no canvas/flow/connection
                                             changes in 90 days (env likely idle)
      HIGH_FLOW_RUN_FAILURE_RATE            — recent flow runs have >20% failure rate
      CANVAS_APPS_NEVER_LAUNCHED            — apps created but never launched by an end user
#>

function Collect-ActivityTelemetry {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]$EnvEntry,
        [Parameter(Mandatory)][string]$EnvOutputDir
    )

    $displayName = $EnvEntry.DisplayName
    $envId       = $EnvEntry.EnvironmentId

    Write-InventoryLog "  Starting activity telemetry for: $displayName" -Indent 1

    $result = [ordered]@{
        CollectedAt   = (Get-Date -Format 'o')
        EnvironmentId = $envId
        DisplayName   = $displayName
        Sections      = [ordered]@{}
        AllFlags      = @()
    }

    # ── 1. Flow run history (BAP aggregate) ──────────────────────────────────
    # Per-flow run counts — this iterates over the flow list already cached
    # from maker inventory; if not cached, skip.
    $flowsFile = Join-Path $EnvOutputDir 'cloud-flows.json'
    if (Test-Path $flowsFile) {
        Write-InventoryLog '    [Activity: Flow Run Summary]...' -Indent 2
        try {
            $flows = Get-Content $flowsFile -Raw | ConvertFrom-Json
            $flowRunStats = [System.Collections.Generic.List[object]]::new()
            $failingFlows = 0
            $sampled      = 0
            # Cap at 50 flows to bound runtime — enough signal without fanning out.
            foreach ($f in @($flows | Select-Object -First 50)) {
                $sampled++
                $flowName = $f.name
                if (-not $flowName) { continue }
                try {
                    $runsResp = Invoke-BAPRequest `
                        -Path "/providers/Microsoft.ProcessSimple/scopes/admin/environments/$envId/flows/$flowName/runs" `
                        -ExtraQuery '$top=50' `
                        -ApiVersion '2016-11-01' `
                        -TimeoutSec 30
                    $runs     = @($runsResp.value)
                    $succ     = @($runs | Where-Object { $_.properties.status -eq 'Succeeded' }).Count
                    $fail     = @($runs | Where-Object { $_.properties.status -eq 'Failed' }).Count
                    $failureRate = if ($runs.Count -gt 0) { [math]::Round($fail * 100.0 / $runs.Count, 1) } else { 0 }
                    if ($failureRate -gt 20 -and $runs.Count -ge 10) { $failingFlows++ }
                    $flowRunStats.Add([ordered]@{
                        FlowName      = $f.properties.displayName
                        FlowId        = $flowName
                        SampledRuns   = $runs.Count
                        Succeeded     = $succ
                        Failed        = $fail
                        FailureRatePct = $failureRate
                    })
                } catch {
                    # Per-flow 404s are common (deleted but not purged); swallow.
                }
            }
            Save-EnvironmentData -EnvironmentDir $EnvOutputDir -FileName 'flow-runs-summary.json' -Data @($flowRunStats)

            $result.Sections['FlowRuns'] = @{
                FlowsSampled       = $sampled
                FlowsWithRunsFailing = $failingFlows
                Notes              = @()
            }
            if ($failingFlows -gt 0) {
                $result.Sections['FlowRuns'].Notes += "HIGH_FLOW_RUN_FAILURE_RATE ($failingFlows of $sampled sampled flows have >20% failure rate in recent runs)"
            }
            Write-InventoryLog "    -> sampled $sampled flows; $failingFlows with high failure rates." -Level OK -Indent 3
        } catch {
            $errInfo = Get-HttpErrorClassification -ErrorRecord $_
            Write-InventoryLog "    -> Flow Runs [$($errInfo.Category)]: $($errInfo.Message)" -Level WARN -Indent 3
            $result.Sections['FlowRuns'] = @{ Notes = @("FLOW_RUNS_$($errInfo.Category)") }
        }
    } else {
        Write-InventoryLog '    [Activity: Flow Run Summary skipped — no cached flows]' -Level SKIP -Indent 2
    }

    # ── 2. App usage summary (canvas apps lastLaunched/lastModified) ─────────
    $appsFile = Join-Path $EnvOutputDir 'canvas-apps.json'
    if (Test-Path $appsFile) {
        Write-InventoryLog '    [Activity: Canvas App Usage]...' -Indent 2
        try {
            $apps = @(Get-Content $appsFile -Raw | ConvertFrom-Json)
            $neverLaunched = @($apps | Where-Object {
                -not $_.properties.lastLaunchedTime -or $_.properties.lastLaunchedTime -eq '0001-01-01T00:00:00Z'
            })
            $stale90 = @($apps | Where-Object {
                $_.properties.lastLaunchedTime -and
                [datetime]$_.properties.lastLaunchedTime -lt (Get-Date).AddDays(-90)
            })
            $result.Sections['CanvasAppUsage'] = @{
                TotalApps            = $apps.Count
                NeverLaunchedCount   = $neverLaunched.Count
                NotLaunched90dCount  = $stale90.Count
                Notes                = @()
            }
            if ($neverLaunched.Count -gt 0 -and $apps.Count -gt 5) {
                $result.Sections['CanvasAppUsage'].Notes += "CANVAS_APPS_NEVER_LAUNCHED ($($neverLaunched.Count) of $($apps.Count) canvas apps have never been launched by an end user)"
            }
            Save-EnvironmentData -EnvironmentDir $EnvOutputDir -FileName 'app-usage-summary.json' -Data $result.Sections['CanvasAppUsage']
            Write-InventoryLog "    -> $($apps.Count) apps, $($neverLaunched.Count) never launched, $($stale90.Count) stale 90d." -Level OK -Indent 3
        } catch {
            Write-InventoryLog "    -> App usage computation failed: $_" -Level WARN -Indent 3
        }
    } else {
        Write-InventoryLog '    [Activity: App Usage skipped — no cached canvas apps]' -Level SKIP -Indent 2
    }

    # ── 3. Maker activity (latest change across apps/flows/connections) ──────
    $latestChange = $null
    foreach ($cache in 'canvas-apps.json','cloud-flows.json','connections.json') {
        $p = Join-Path $EnvOutputDir $cache
        if (-not (Test-Path $p)) { continue }
        try {
            $items = @(Get-Content $p -Raw | ConvertFrom-Json)
            foreach ($i in $items) {
                $stamp = $null
                if ($i.properties -and $i.properties.lastModifiedTime) { $stamp = $i.properties.lastModifiedTime }
                elseif ($i.properties -and $i.properties.lastModifiedDateTime) { $stamp = $i.properties.lastModifiedDateTime }
                if ($stamp) {
                    $dt = [datetime]$stamp
                    if (-not $latestChange -or $dt -gt $latestChange) { $latestChange = $dt }
                }
            }
        } catch {}
    }
    $result.Sections['MakerActivity'] = @{
        LatestMakerChange = if ($latestChange) { $latestChange.ToString('o') } else { $null }
        DaysSinceLastChange = if ($latestChange) { [int]((Get-Date) - $latestChange).TotalDays } else { $null }
        Notes              = @()
    }
    if ($latestChange -and ((Get-Date) - $latestChange).TotalDays -gt 90) {
        $result.Sections['MakerActivity'].Notes += "NO_MAKER_ACTIVITY_90D (no maker-surface changes - canvas apps, flows, connections - in the last 90 days; environment may be effectively idle)"
    }
    Save-EnvironmentData -EnvironmentDir $EnvOutputDir -FileName 'maker-activity.json' -Data $result.Sections['MakerActivity']

    # Flatten notes
    $allNotes = [System.Collections.Generic.List[string]]::new()
    foreach ($sec in $result.Sections.Values) {
        if ($sec.Notes) {
            foreach ($n in $sec.Notes) { if ($n) { $allNotes.Add($n) } }
        }
    }
    $result['AllFlags'] = @($allNotes | Sort-Object -Unique)

    Save-EnvironmentData -EnvironmentDir $EnvOutputDir -FileName 'activity-summary.json' -Data $result
    Write-InventoryLog "  Activity telemetry complete. Flags: $($result.AllFlags.Count)" -Level OK -Indent 1

    return $result
}
