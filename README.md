# PPAC Dataverse Inventory

Read-only PowerShell toolkit for auditing every Power Platform / Dataverse environment in a Microsoft 365 tenant. Collects environment health, storage, governance posture, and operational data, then generates a self-contained interactive HTML report.

All operations are **read-only**. Nothing is modified in any environment.

---

## Quick Start

```powershell
# 1. Open PowerShell 5.1+ as your Power Platform Administrator account
# 2. Navigate to the project folder
cd C:\...\PPACInventoryManagement

# 3. Run the convenience launcher (installs prereqs, collects data, generates report)
#    Collects everything available by default: entity record counts + F&O data.
.\Start-Inventory.ps1

# Fast pass: skip the slow per-entity record counts and F&O collection
.\Start-Inventory.ps1 -IncludeEntityCounts:$false -IncludeFO:$false

# Scoped to production environments on a specific tenant
.\Start-Inventory.ps1 -TenantId "your-tenant-guid" -EnvironmentFilter "Production"
```

## Manual Step-by-Step

```powershell
# Step 1: Check / install required PowerShell modules
.\scripts\00_Prerequisites.ps1

# Step 2: Full collection (entity counts + F&O both on by default)
.\scripts\Invoke-DataverseInventory.ps1 -OutputPath .\data

# Step 2 (device code / headless / remote session):
.\scripts\Invoke-DataverseInventory.ps1 -OutputPath .\data -UseDeviceCode

# Step 2 (fast pass — skip entity record counts and F&O):
.\scripts\Invoke-DataverseInventory.ps1 -OutputPath .\data -IncludeEntityCounts:$false -IncludeFO:$false

# Step 3: Generate the HTML report and open it
.\scripts\Generate-Report.ps1 -DataPath .\data -OpenReport
```

---

## Requirements

| Requirement | Details |
|---|---|
| PowerShell | 5.1 or later |
| `Az.Accounts` | >= 2.12.0 (auto-installed by `00_Prerequisites.ps1`) |
| `Az.Resources` | >= 6.0.0 (auto-installed by `00_Prerequisites.ps1`) |
| Azure AD role | Global Administrator **or** Power Platform Administrator |
| FO data collection | System Administrator role in each Finance & Operations environment |

---

## Parameters

### `Invoke-DataverseInventory.ps1`

| Parameter | Default | Description |
|---|---|---|
| `-OutputPath` | `.\data` | Root directory for collected JSON files |
| `-TenantId` | _(current Az context)_ | Azure AD tenant GUID |
| `-SubscriptionId` | _(current context)_ | Azure subscription ID (multi-sub tenants) |
| `-EnvironmentFilter` | _(all)_ | Regex filter on environment display name |
| `-SkipEnvironmentIds` | _(none)_ | Array of environment GUIDs to exclude |
| `-IncludeEntityCounts` | `$true` | Fetch record counts per table for CE, and (with `-IncludeFO`) every entity set exposed by the F&O OData service. Adds significant time on large tenants; pass `-IncludeEntityCounts:$false` to skip. |
| `-EntityCountTop` | `0` (unlimited) | Safety cap on CE entities counted per environment. `0` counts every queryable entity (`mserp_*` always excluded — those go through the F&O endpoint). Priority order: custom entities, known high-volume OOB, then everything else. |
| `-IncludeFO` | `$true` | Collect Finance & Operations batch/user/DIXF data for F&O-integrated envs. Pass `-IncludeFO:$false` to skip. |
| `-IncludeMakerInventory` | `$true` | Collect maker surfaces: canvas apps, cloud flows, custom connectors, connections, Power Pages, Copilots, dataflows, AI Builder models. |
| `-IncludeGovernance` | `$true` | Collect tenant-level DLP policies, tenant settings, tenant isolation, plus per-env Managed Environments config. |
| `-IncludeRBAC` | `$true` | Collect security roles, business units, teams, field security profiles, and user-role assignment depth. |
| `-IncludeMetadataDepth` | `$true` | Collect publishers, D365 first-party apps, orgdborgsettings, currencies, languages, lifecycle operations, and backup history. |
| `-IncludeActivity` | `$false` | **Opt-in** — samples flow run history and canvas app usage. Slow on large envs; off by default. |
| `-MaxEnvironments` | `0` (unlimited) | Safety cap — stops after N environments |
| `-MaxDegreeOfParallelism` | `8` | Number of environments to collect concurrently. Each environment runs in its own PowerShell runspace with an isolated log file that is merged into `inventory.log` when it finishes. Set to `1` to force fully sequential collection (useful when debugging or when hitting throttling). Capped to the environment count so a 2-env tenant with `-MaxDegreeOfParallelism 8` just runs 2 workers. |
| `-Force` | `$false` | Re-collect environments that already have data |
| `-UseDeviceCode` | `$false` | Device-code authentication flow (headless sessions) |

### `Generate-Report.ps1`

| Parameter | Default | Description |
|---|---|---|
| `-DataPath` | `..\data` | Path to the collected data directory |
| `-ReportPath` | `.\reports\PPACInventoryReport_<ts>.html` | Output path for the HTML report |
| `-OpenReport` | `$false` | Open the report in the default browser after generation |

---

## What It Collects

### Per Environment — CE / Dataverse Side

| Data | Output File | Flags Generated |
|---|---|---|
| Environment metadata & storage capacity (BAP API) | `metadata.json` | Storage threshold flags |
| System users (active / disabled / integration) | `users.json` | `NO_ACTIVE_USERS` |
| Bulk delete jobs (scheduled cleanup tasks) | `bulk-delete-jobs.json` | `NO_SCHEDULED_BULK_DELETE`, `NO_ASYNCOP_BULK_DELETE_JOB` |
| Async operation queue health (by state + age) | `async-operations-summary.json` | `HIGH_SUSPENDED_JOBS`, `HIGH_FAILED_JOBS_30D`, `OLD_COMPLETED_JOBS_NOT_CLEANED` |
| Organization health settings | `org-settings.json` | `PLUGIN_TRACE_LOGGING_ENABLED`, `AUDIT_RETENTION_SET_TO_FOREVER`, `AUDIT_RETENTION_NOT_CONFIGURED` |
| Solutions (managed + unmanaged) | `solutions.json` | `HIGH_UNMANAGED_SOLUTIONS`, `NO_MANAGED_SOLUTIONS` |
| Workflows & Power Automate flows | `workflows.json` | `MANY_INACTIVE_WORKFLOWS`, `ACTIVE_FLOWS_OWNED_BY_DISABLED_USERS` |
| Plugin assemblies & processing steps | `plugins.json`, `plugin-steps.json` | `MANY_INACTIVE_PLUGIN_STEPS` |
| Duplicate detection rules | `duplicate-detection-rules.json` | `NO_DUPLICATE_DETECTION_RULES` |
| Model-driven app modules | `app-modules.json` | — |
| Connection references | `connection-references.json` | `BROKEN_CONNECTION_REFERENCES` |
| Environment variables (definitions + values) | `environment-variables.json` | `ENV_VARS_MISSING_VALUES` |
| Audit log sample (last 200 entries + 90d count) | `audit-sample.json` | `AUDIT_DISABLED_OR_NO_ACTIVITY`, `NO_AUDIT_ACTIVITY_90D` |
| Retention policies (Dataverse Long-Term Retention) | `retention-policies.json` | `NO_RETENTION_POLICIES` |
| Mailbox / Server-Side Sync health | `mailboxes.json` | `MAILBOX_SYNC_ERRORS`, `MAILBOXES_NOT_TESTED`, `NO_MAILBOXES_CONFIGURED` |
| Unresolved duplicate records backlog | _(summary only)_ | `HIGH_UNRESOLVED_DUPLICATES`, `MANY_UNRESOLVED_DUPLICATES` |
| Queue item backlog | _(summary only)_ | `HIGH_QUEUE_ITEM_BACKLOG` |
| Service endpoints & webhooks inventory | `service-endpoints.json` | — _(inventory only)_ |
| SLA KPI violation count | _(summary only)_ | `HIGH_SLA_VIOLATIONS` |
| Stale process / BPF instances (>180d active) | _(summary only)_ | `STALE_BPF_INSTANCES` |
| Process session volume (last 30d) | _(summary only)_ | — |
| Cleanup table health indicators | `cleanup-table-health.json` | `OLD_WORKFLOW_LOGS_ACCUMULATING`, `PLUGIN_TRACE_LOGS_ACCUMULATING`, `LARGE_ANNOTATION_FILES`, `OLD_COMPLETED_EMAILS`, `OLD_IMPORT_JOB_HISTORY`, `OLD_BULK_DELETE_OPERATION_HISTORY` |
| Entity definitions | `entity-definitions.json` | — |
| Entity record counts _(optional, slow)_ | `entity-counts.json` | `LARGE_CUSTOM_ENTITIES_NO_CLEANUP`, `TEAMS_TABLE_STORAGE_HIGH` |
| Entity record-count request trace — one entry per URL hit: URI, HTTP status, outcome, elapsed ms, error text. | `entity-counts.trace.json` | — _(diagnostic only)_ |
| F&O virtual tables (`mserp_*`) are excluded here and counted via the F&O native OData endpoint instead — see `fo-entity-counts.json` below. | | |
| Dual-write config & map status | `dualwrite-configs.json`, `dualwrite-entity-maps.json` | `DUALWRITE_MAPS_IN_ERROR` |

### Per Environment — Governance Checks (BAP API)

| Data | Flags Generated |
|---|---|
| Managed Environments status | `PRODUCTION_NOT_MANAGED_ENVIRONMENT` |
| Environment Group membership | `NOT_IN_ENVIRONMENT_GROUP` |
| Environment Admin role assignments | `NO_DEDICATED_ENVIRONMENT_ADMIN`, `ENVIRONMENT_ADMIN_IS_USER_NOT_GROUP` |

### Per Environment — Maker Inventory (`-IncludeMakerInventory`)

| Data | Output File | Flags Generated |
|---|---|---|
| Canvas apps (owner, last modified, last launched) | `canvas-apps.json` | `ORPHANED_CANVAS_APPS`, `STALE_CANVAS_APPS` |
| Cloud flows (Power Automate) — owner + state | `cloud-flows.json` | `CLOUD_FLOWS_OWNED_BY_DISABLED_USERS`, `SUSPENDED_CLOUD_FLOWS` |
| Custom connectors | `custom-connectors.json` | `UNCERTIFIED_CUSTOM_CONNECTORS` |
| Connections (per-connector count + error state) | `connections.json` | `CONNECTIONS_IN_ERROR_STATE`, `ORPHANED_CONNECTIONS` |
| Power Pages sites | `power-pages.json` | — _(inventory only)_ |
| Copilots (chatbots) | `copilots.json` | `STALE_COPILOTS` |
| Dataflows (Gen2) | `dataflows.json` | `DATAFLOWS_FAILING_REFRESH` |
| AI Builder models | `ai-models.json` | — _(inventory only)_ |
| Combined maker summary | `maker-summary.json` | Aggregates all of the above |

### Tenant-Scoped — Governance & DLP (`-IncludeGovernance`)

Collected **once per tenant** (not per environment) and written to `data/tenant/`.

| Data | Output File | Flags Generated |
|---|---|---|
| DLP policies (connector classifications, scope) | `tenant/dlp-policies.json` | `NO_DLP_POLICIES_CONFIGURED`, `DLP_POLICY_MISSING_ON_PRODUCTION`, `PROD_ALLOWS_HTTP_CONNECTOR` |
| Tenant settings (walkme, sharing, search, etc.) | `tenant/tenant-settings.json` | `NON_ADMIN_CAN_CREATE_ENVIRONMENTS`, `NON_ADMIN_CAN_CREATE_TRIAL_ENVIRONMENTS` |
| Tenant isolation config (cross-tenant connector blocking) | `tenant/tenant-isolation.json` | `TENANT_ISOLATION_DISABLED` |
| Environment creation requests (pending) | `tenant/environment-creation.json` | `MANY_PENDING_ENVIRONMENT_REQUESTS` |
| Tenant governance summary (aggregated) | `tenant/governance-summary.json` | — _(roll-up)_ |

**Per-environment** Managed Environments configuration (weekly digest, solution checker, sharing limits):

| Data | Output File | Flags Generated |
|---|---|---|
| Managed Env config (digest / sharing / solution checker) | `governance.json`, `governance-summary.json` | `MANAGED_ENV_WEEKLY_DIGEST_DISABLED`, `MANAGED_ENV_SOLUTION_CHECKER_OFF`, `MANAGED_ENV_SHARING_UNLIMITED` |

### Per Environment — RBAC Depth (`-IncludeRBAC`)

| Data | Output File | Flags Generated |
|---|---|---|
| Security roles (managed + unmanaged) | `security-roles.json` | `NO_CUSTOM_SECURITY_ROLES` |
| Business units (full tree + max depth) | `business-units.json` | `DEEP_BUSINESS_UNIT_NESTING` |
| Teams (owner + access teams) | `teams.json` | — _(inventory only)_ |
| Field security profiles | `field-security-profiles.json` | — _(inventory only)_ |
| User-role assignments sample (500 users, sys admin count) | `user-role-assignments.json` | `SYSTEM_ADMIN_OVERASSIGNED` |
| Combined RBAC summary | `rbac-summary.json` | Aggregates all of the above |

### Per Environment — Metadata Depth & Lifecycle (`-IncludeMetadataDepth`)

| Data | Output File | Flags Generated |
|---|---|---|
| Publishers (managed + unmanaged) | `publishers.json` | — _(inventory only)_ |
| D365 first-party apps detection (F&O, CE Sales/Service, etc.) | `d365-apps.json` | `NO_D365_APPS_ON_F365_ENV` |
| Solution dependencies graph | `solution-dependencies.json` | — _(inventory only)_ |
| Organization orgdborgsettings (raw XML blob) | `orgdborgsettings.xml` | — _(reference snapshot)_ |
| Currencies (base + additional) | `currencies.json` | — _(inventory only)_ |
| Provisioned languages | `languages.json` | — _(inventory only)_ |
| Lifecycle operations (copy / restore / reset history) | `lifecycle-operations.json` | `LIFECYCLE_OP_FAILED_RECENTLY` |
| Manual user backups | `backups.json` | `NO_RECENT_USER_BACKUPS` |
| Combined metadata depth summary | `metadata-depth-summary.json` | Aggregates all of the above |

### Per Environment — Activity Telemetry (`-IncludeActivity`, **opt-in**)

> Disabled by default because per-flow run sampling is slow on large environments.

| Data | Output File | Flags Generated |
|---|---|---|
| Flow run sampling (up to 50 flows, recent runs) | `flow-runs-summary.json` | `HIGH_FLOW_RUN_FAILURE_RATE` |
| Canvas app usage (lastLaunchedTime) | `app-usage-summary.json` | `CANVAS_APPS_NEVER_LAUNCHED` |
| Maker activity (latest change across apps/flows/connections) | `maker-activity.json` | `NO_MAKER_ACTIVITY_90D` |
| Combined activity summary | `activity-summary.json` | Aggregates all of the above |

### Per Environment — Finance & Operations Side (`-IncludeFO`)

F&O data is collected from the AOS OData API at `<FOBaseUrl>/data/<EntitySet>`. Requires **System Administrator** role in each F&O environment. Entity sets and field names are validated against the F&O `$metadata` schema.

| Data | Output File | Flags Generated |
|---|---|---|
| System parameters (currency, language, exchange rate) | `fo-system-parameters.json` | — _(inventory only)_ |
| Batch jobs (all, with `BatchStatus` enum) | `fo-batch-jobs.json` | `FO_BATCH_JOBS_IN_ERROR`, `FO_MANY_WITHHELD_JOBS`, `FO_NO_BATCH_JOBS_CONFIGURED` |
| Batch job groups | `fo-batch-groups.json` | — _(inventory only)_ |
| DIXF data management projects (definition groups) | `fo-dixf-definition-groups.json` | — _(inventory only)_ |
| DIXF execution job details (last 200, ordered by staging start) | `fo-dixf-execution-history.json` | `FO_DIXF_JOBS_FAILED` |
| Standard FO cleanup jobs — missing | `fo-missing-cleanup-jobs.json` | `FO_MISSING_CLEANUP_JOBS` |
| Standard FO cleanup jobs — found (with enabled/error status per match) | `fo-cleanup-jobs-found.json` | `FO_CLEANUP_JOBS_NOT_ENABLED`, `FO_CLEANUP_JOBS_IN_ERROR` |
| Legal entities | `fo-legal-entities.json` | — _(inventory only)_ |
| Enabled FO system users | `fo-users.json` | `FO_NO_ENABLED_USERS` |
| Pending workflow work items (overdue proxy via `DueDateTime`) | `fo-pending-workflows.json` | `FO_MANY_STALLED_WORKFLOWS` |
| F&O entity record counts _(only with `-IncludeEntityCounts`)_ — enumerates every entity set from `<FOBaseUrl>/data/` service document and counts each via `?$count=true&$top=0`. `config/fo-count-entities.json` is used as optional Category/Why metadata enrichment, not the authoritative list. Counts roll up into the report's Top Tables section alongside CE tables with an F&O badge. | `fo-entity-counts.json` | — _(inventory only)_ |
| F&O entity record-count request trace — one entry per URL hit (starts with the `/data/` service-document fetch), with URI, HTTP status, outcome, elapsed ms, and error text for silent-skipped entities. | `fo-entity-counts.trace.json` | — _(diagnostic only)_ |
| Cross-section FO summary + all FO flags | `fo-summary.json` | Aggregates all of the above |

**Cleanup jobs catalog**: ~38 standard D365FO cleanup batch jobs grouped across 9 module categories — System, DIXF, Sales, Procurement, Warehouse, Inventory, Production, Master Planning, Finance, Retail. Each catalog entry carries the menu path, batch class, and admin guidance Notes so missing jobs are immediately actionable. Found jobs are further classified by `BatchStatus`: **Enabled** (`Waiting`/`Executing`/`Ready`/`Scheduled`), **In Error** (`Error`), or **Disabled** (`Hold`/`Canceled`/`NotRun`/`Finished` with no recurrence).

---

## Governance Scoring

Each environment receives a **governance score from 0 to 100** based on its active flags. Scores are computed when the report is generated and do not require re-running the collection.

### How Scores Are Calculated

1. Start at **100** (perfect).
2. Each active flag deducts points based on its severity:

   | Severity | Deduction | Examples |
   |---|---|---|
   | Critical | 15 pts | `PRODUCTION_NOT_MANAGED_ENVIRONMENT`, `NO_DLP_POLICIES_CONFIGURED`, `TENANT_ISOLATION_DISABLED`, `MAILBOX_SYNC_ERRORS`, `SYSTEM_ADMIN_OVERASSIGNED`, `LIFECYCLE_OP_FAILED_RECENTLY`, `CLOUD_FLOWS_OWNED_BY_DISABLED_USERS` |
   | High | 8 pts | `NO_SCHEDULED_BULK_DELETE`, `ORPHANED_CANVAS_APPS`, `UNCERTIFIED_CUSTOM_CONNECTORS`, `NO_CUSTOM_SECURITY_ROLES`, `MANAGED_ENV_SOLUTION_CHECKER_OFF`, `HIGH_FLOW_RUN_FAILURE_RATE`, `NO_RECENT_USER_BACKUPS`, `FO_CLEANUP_JOBS_NOT_ENABLED`, `FO_CLEANUP_JOBS_IN_ERROR` |
   | Medium | 4 pts | `NO_RETENTION_POLICIES`, `NOT_IN_ENVIRONMENT_GROUP`, `SUSPENDED_CLOUD_FLOWS`, `DATAFLOWS_FAILING_REFRESH`, `DEEP_BUSINESS_UNIT_NESTING`, `MANAGED_ENV_WEEKLY_DIGEST_DISABLED` |
   | Low | 1 pt | `NO_ACTIVE_USERS`, `STALE_CANVAS_APPS`, `NO_MAKER_ACTIVITY_90D`, `CANVAS_APPS_NEVER_LAUNCHED`, `OLD_WORKFLOW_LOGS_ACCUMULATING` |

3. **SKU suppression**: flags listed in the `Suppress` array of a SKU profile (see `config/sku-profiles.json`) count as **1 point** instead of their full deduction. For example, `NO_RETENTION_POLICIES` on a Developer environment is suppressed because retention policies are not expected there.
4. Score is clamped to **[0, 100]**.

### Thresholds

| Score | Label | Badge Color |
|---|---|---|
| ≥ 80 | Healthy | Green |
| 50 – 79 | Needs Attention | Amber |
| < 50 | Critical | Red |

### Tenant Score

The **tenant-wide governance score** is a weighted average of all environment scores, where the weight is the environment's `GovernanceWeight` from `config/sku-profiles.json`:

| SKU | Weight |
|---|---|
| Production | 3× |
| Default | 2× |
| Sandbox | 1.5× |
| Developer | 0.5× |
| Trial | 0.25× |

Production environments dominate the tenant score by design.

---

## Delta Reporting

The report includes a **Changes Since Last Run** section that compares the current collection with the previous one.

### How It Works

- `Invoke-DataverseInventory.ps1` saves a timestamped snapshot to `data/run-history/` at the end of every run.
- `Generate-Report.ps1` loads the second-to-last snapshot and diffs it against the current data.
- **Flags are compared by name prefix**, stripping the parenthetical detail suffix (e.g., `HIGH_FAILED_JOBS_30D (12 jobs)` and `HIGH_FAILED_JOBS_30D (8 jobs)` are the same issue — not resolved+new).
- Storage differences ≥ 10 MB between runs are surfaced — growth and reductions are shown in separate top-15 tables.

The delta section is omitted if fewer than two snapshots exist (first run).

---

## Storage Cleanup Recommendations

The report's **Storage Cleanup Recommendations** section synthesizes data from multiple collectors into ranked, actionable cleanup tasks for each environment.

Each recommendation includes:
- **Priority** (High / Medium / Low)
- **What to clean** (data type and approximate record count)
- **Storage type** (DB / File / Log)
- **Recommended action** with the exact Bulk Delete Job filter or settings path

### Common Cleanup Actions

| Issue | Bulk Delete Job Filter |
|---|---|
| Completed async operations >90d | System Jobs → Status = Succeeded/Canceled/Failed AND Created On < 90 days ago |
| Old succeeded workflow logs | System Jobs → Status = Succeeded AND Created On < 30 days ago |
| Old completed email activities | Email Messages → Status = Completed AND Actual End < 90 days ago |
| Large attachment notes | Notes → File Size (Bytes) > 1048576 AND Created On < [your date] |
| Old import job history | System Jobs → System Job Type = Import AND Created On < 90 days ago |
| Self-cleaning bulk delete history | System Jobs → System Job Type = Bulk Delete AND Status = Succeeded AND Created On < 90 days ago |

> **Tip**: Entity record counts are collected by default. If you're on a very large tenant and want a fast pass, disable them with `-IncludeEntityCounts:$false`.

---

## Configuration Files

All configuration files are in the `config/` directory. They are read at **report generation time** and do not affect data collection.

### `config/flag-severity.json`

Maps every flag name to a severity level (Critical / High / Medium / Low) and the point deduction weights used in governance scoring. Edit this file to adjust severity thresholds for your organization.

```json
{
  "Critical": ["MAILBOX_SYNC_ERRORS", "PRODUCTION_NOT_MANAGED_ENVIRONMENT", ...],
  "High":     ["NO_SCHEDULED_BULK_DELETE", "PLUGIN_TRACE_LOGGING_ENABLED", ...],
  "Weights":  { "Critical": 15, "High": 8, "Medium": 4, "Low": 1 }
}
```

### `config/sku-profiles.json`

Per-SKU governance baseline profiles. The `Suppress` array lists flags that are acceptable for that SKU and should not heavily penalize the score. The `GovernanceWeight` controls the SKU's contribution to the tenant governance score.

```json
{
  "Production": {
    "Suppress": [],
    "GovernanceWeight": 3
  },
  "Developer": {
    "Suppress": ["NO_RETENTION_POLICIES", "NOT_IN_ENVIRONMENT_GROUP", ...],
    "GovernanceWeight": 0.5
  }
}
```

### `config/fo-count-entities.json`

Optional metadata enrichment for F&O entity counts. When `-IncludeEntityCounts` is used against an F&O-integrated environment, the collector enumerates **every** entity set exposed by `<FOBaseUrl>/data/` and counts each one. This config file does not gate which entities are counted — it only supplies `Category` and `Why` tags that get attached to matching entities in the output. Entities not listed here are tagged `Category = "Other"` and `Why = ""`.

```json
{
  "Entities": [
    { "Name": "GeneralJournalAccountEntries", "Category": "Finance",   "Why": "posted ledger entries" },
    { "Name": "InventoryTransactions",        "Category": "Inventory", "Why": "inventory movements" },
    { "Name": "BatchJobHistories",            "Category": "System",    "Why": "batch execution history" }
  ]
}
```

`Name` must be the public PascalCase Data Entity name exposed by the F&O AOS — **not** the underlying SQL table name.

### `config/owners.json`

Maps environment IDs to owner information. Populated automatically from Environment Admin role assignments during collection. You can manually override entries by setting `"AutoPopulated": false`.

```json
{
  "00000000-0000-0000-0000-000000000001": {
    "Owner": "admin@contoso.com",
    "Team": "Platform Engineering",
    "Notes": "Primary CRM environment",
    "AutoPopulated": false
  }
}
```

---

## Report Sections

| Section | Contents |
|---|---|
| Executive Summary | Total environments, storage totals, tenant governance score, critical/healthy environment counts |
| Issue Overview | Count cards for each issue category across the tenant |
| Changes (Delta) | New flags, resolved flags, and storage growth/reductions vs. previous run |
| Governance Scores | Per-environment score table with Critical/High/Medium/Low flag counts and owners |
| Tenant Governance & DLP | Tenant-level DLP policy list, tenant isolation state, Managed Env digest/solution-checker coverage across environments |
| All Environments | Full sortable/filterable table of all environments with flags, storage, and scores |
| Storage Analysis | Top 25 DB, File, and Log storage consumers with progress bars |
| Cleanup Gaps | Environments missing bulk delete jobs or with evidence of cleanup backlogs |
| Cleanup Recommendations | Ranked, actionable cleanup tasks with Bulk Delete Job filters and record counts |
| Maker Inventory | Per-environment canvas app / flow / connector / connection / Power Pages / Copilot / dataflow / AI Builder counts + orphan and stale flags |
| RBAC Summary | Security role counts (managed vs custom), business unit tree depth, team count, sys-admin over-assignment flags |
| Metadata & Lifecycle | Publisher / D365 first-party app detection, currency + language counts, recent lifecycle ops, backup recency |
| Activity / Unused | Environments with no active users, no recent audit activity, and (with `-IncludeActivity`) canvas apps never launched + flow run failure rates |
| Finance & Operations | FO batch job health, missing cleanup jobs, cleanup jobs disabled or in error, DualWrite map errors, DIXF failures, enabled user count, pending workflow work items |
| Top Tables (Records) | Per-environment collapsible drill-down: top 25 tables by record count, merging CE tables (from `entity-counts.json`) and F&O data entities (from `fo-entity-counts.json`). Storage-concentration proxy (bytes-per-table isn't exposed by any public Dataverse/F&O API). F&O rows are badged separately from Custom/OOB CE rows. Populated only with `-IncludeEntityCounts`. |
| Collection Info | Run metadata, authentication details, top flags across the tenant |

---

## Output Structure

```
data/
  environments.json              # All environments list (from BAP API)
  master-summary.json            # Cross-environment summary + all flags
  tenant-capacity.json           # Tenant-wide storage capacity
  inventory.log                  # Structured collector log (Write-InventoryLog output, timestamped)
  logs/
    session-20260418-102354.log  # Full host transcript (prereqs + collector + report) — one file per run
  run-history/
    2025-04-01_120000.json       # Timestamped snapshots for delta reporting
    2025-04-08_120000.json
  tenant/                        # (only with -IncludeGovernance) — tenant-scoped collection, runs once
    dlp-policies.json
    tenant-settings.json
    tenant-isolation.json
    environment-creation.json
    governance-summary.json
  environments/
    Production_MyOrg/
      worker.log                  # Per-env log written by the parallel worker; merged into inventory.log at run end
      metadata.json
      users.json
      bulk-delete-jobs.json
      async-operations-summary.json
      org-settings.json
      solutions.json
      workflows.json
      plugins.json
      plugin-steps.json
      duplicate-detection-rules.json
      app-modules.json
      connection-references.json
      environment-variables.json
      environment-variable-values.json
      audit-sample.json
      retention-policies.json
      mailboxes.json
      service-endpoints.json
      cleanup-table-health.json
      entity-definitions.json
      entity-counts.json          # (only with -IncludeEntityCounts) — CE tables only, mserp_* excluded
      entity-counts.trace.json    # (only with -IncludeEntityCounts) — per-request trace: uri, http status, outcome, elapsed, error
      role-assignments.json
      fo-integration-details.json # (only when F&O integration is detected)
      dualwrite-configs.json      # (only when F&O integration is detected)
      dualwrite-entity-maps.json  # (only when F&O integration is detected)
      fo-summary.json             # (only with -IncludeFO) — cross-section FO summary
      fo-system-parameters.json
      fo-batch-jobs.json
      fo-batch-groups.json
      fo-dixf-definition-groups.json
      fo-dixf-execution-history.json
      fo-missing-cleanup-jobs.json
      fo-cleanup-jobs-found.json
      fo-legal-entities.json
      fo-users.json
      fo-pending-workflows.json
      fo-entity-counts.json       # (only with -IncludeFO + -IncludeEntityCounts) — native F&O OData counts
      fo-entity-counts.trace.json # (only with -IncludeFO + -IncludeEntityCounts) — per-request trace incl. service-doc fetch
      # ── Maker Inventory (-IncludeMakerInventory) ──────────────────
      canvas-apps.json
      cloud-flows.json
      custom-connectors.json
      connections.json
      power-pages.json
      copilots.json
      dataflows.json
      ai-models.json
      maker-summary.json
      # ── Per-env Managed Environments config (-IncludeGovernance) ─
      governance.json
      governance-summary.json
      # ── RBAC (-IncludeRBAC) ───────────────────────────────────────
      security-roles.json
      business-units.json
      teams.json
      field-security-profiles.json
      user-role-assignments.json
      rbac-summary.json
      # ── Metadata Depth & Lifecycle (-IncludeMetadataDepth) ────────
      publishers.json
      d365-apps.json
      solution-dependencies.json
      orgdborgsettings.xml
      currencies.json
      languages.json
      lifecycle-operations.json
      backups.json
      metadata-depth-summary.json
      # ── Activity Telemetry (-IncludeActivity, opt-in) ─────────────
      flow-runs-summary.json
      app-usage-summary.json
      maker-activity.json
      activity-summary.json
      ce-summary.json             # Cross-section CE summary + all CE flags

config/
  flag-severity.json             # Flag → severity mapping and score weights
  sku-profiles.json              # Per-SKU governance baseline and weights
  fo-count-entities.json         # Curated F&O Data Entity list for -IncludeEntityCounts
  owners.json                    # Environment owner registry (auto-populated)

reports/
  PPACInventoryReport_20250401_120000.html
```

---

## Notes

- **Read-only**: no changes are made to any environment at any time.
- **Resume support**: re-run without `-Force` to skip already-collected environments and continue an interrupted run. Storage values always reflect the live BAP API data, even for skipped environments.
- **FO detection**: F&O-integrated environments are identified authoritatively by calling the Dataverse `RetrieveFinanceAndOperationsIntegrationDetails` action, which also returns the F&O AOS URL, linked environment ID, and tenant ID. Detection runs on every environment with Dataverse; `-IncludeFO` only gates the deeper F&O AOS queries (batch jobs, DIXF, users, legal entities).
- **Per-table storage**: Microsoft does not expose per-table byte-level storage via any public Dataverse or F&O API (the PPAC UI shows this, but the endpoint is undocumented). The report instead surfaces the **top 25 tables by record count** per environment (collected via the documented `RetrieveTotalRecordCount` function when `-IncludeEntityCounts` is used) as a storage-concentration proxy — tables with the most rows almost always dominate database storage.
- **F&O counts bypass the CE federation layer**: `mserp_*` virtual tables are Dataverse projections of F&O entities, but counting them via the Dataverse Web API routes through the F&O federation layer — slow, and the `RetrieveTotalRecordCount` PK-derivation is unreliable on those synthetic names. The CE collector explicitly filters `mserp_*` out of `entity-counts.json`, and `-IncludeFO -IncludeEntityCounts` separately enumerates **every** entity set from the F&O OData service document (`<FOBaseUrl>/data/`) and counts each one into `fo-entity-counts.json`. Entities that return 400/403/404/500 (not queryable, permission-scoped, requires parameters, module not installed) are silently skipped. Both files are merged in the report's Top Tables section.
- **Thoroughness vs. time**: with `-IncludeEntityCounts` at its default, the collector counts every queryable CE entity (typically 500–1500 per environment) and every F&O entity set (2000–5000 depending on installed modules). Allow 10–45 min per environment; pass `-EntityCountTop N` to cap CE counts, or `-IncludeEntityCounts:$false` to skip entirely.
- **Entity-count trace logs**: every URL hit during record counting is recorded in `entity-counts.trace.json` (CE) and `fo-entity-counts.trace.json` (F&O). Each entry has `EntityName`, `Uri`, `HttpStatus`, `Outcome` (`Success`/`Error`), `RecordCount`, `ElapsedMs`, `Error`, `Timestamp`. Use these to diagnose which entities are failing to count and why — the main `*-counts.json` files only contain successful rows. A WARN line in the run log summarises the total error count per environment.
- **Tenant-scoped vs per-env collection**: `-IncludeGovernance` produces both tenant-scoped output (DLP, tenant settings, tenant isolation — written to `data/tenant/` once per run) and per-env output (Managed Environments config, written under each environment folder). The other Include switches are per-environment only.
- **SKU-aware suppression**: maker noise flags (orphan apps/connections, stale canvas apps, never-launched apps) are automatically suppressed on Developer and Trial environments via `config/sku-profiles.json`, since dev/trial envs are expected to accumulate abandoned artifacts.
- **Categorized error flags**: API failures surface as category-tagged flags rather than opaque errors — e.g. `MAILBOX_ACCESS_DENIED`, `AUDIT_DISABLED_OR_INACCESSIBLE`, `RETENTION_FEATURE_NOT_ENABLED`, `SLA_NOT_CONFIGURED`, `DUALWRITE_NOT_CONFIGURED`. This distinguishes genuine problems from "feature not enabled on this SKU" no-ops.
- **Logging**: `Start-Inventory.ps1` writes two logs per run by default. `data/inventory.log` (append-only) is the structured collector log — every `Write-InventoryLog` call, timestamped and leveled (INFO/WARN/OK/DEBUG/SKIP). `data/logs/session-<ts>.log` (one file per run) is the full `Start-Transcript` capture — host banner, prereqs output, report-generator progress, uncaught exceptions, everything that reaches the console. Pass `-NoTranscript` to skip the session log (e.g. when piping output yourself).
- **Parallel collection**: by default the orchestrator collects up to 8 environments concurrently via PowerShell runspace pools (`-MaxDegreeOfParallelism 8`). Each worker writes to its own `environments/<Name>/worker.log`. Once all workers have completed, the parent performs a single merge pass that appends every worker log to the canonical `data/inventory.log` with a section header per environment, so timestamps and levels are preserved. Cross-environment ordering in `inventory.log` reflects worker completion order, not dispatch order. Set `-MaxDegreeOfParallelism 1` to force sequential collection. Reduce if you see HTTP 429 throttling — the 8-wide default is tuned for a typical admin tenant; very small tenants or aggressive BAP throttling may benefit from 3–4.
- **Token caching**: each runspace calls `Get-AzAccessToken`, which hits the Az module's process-wide MSAL cache — so token acquisition is effectively free after the first call in the parent, even across parallel workers.
- **Rate limiting**: API calls use exponential backoff with up to 5 retries on HTTP 429 / 503 responses. Retry jitter is per-runspace, so parallel workers do not all back off in lockstep.
- **PS 5.1 compatibility**: the entire toolkit targets Windows PowerShell 5.1. No PS 7+ features are used.
