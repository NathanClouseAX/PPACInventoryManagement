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
.\Start-Inventory.ps1

# With all options:
.\Start-Inventory.ps1 -TenantId "your-tenant-guid" `
                      -IncludeFO `
                      -IncludeEntityCounts `
                      -EnvironmentFilter "Production"
```

## Manual Step-by-Step

```powershell
# Step 1: Check / install required PowerShell modules
.\scripts\00_Prerequisites.ps1

# Step 2: Collect data (interactive browser login)
.\scripts\Invoke-DataverseInventory.ps1 -OutputPath .\data

# Step 2 (device code / headless / remote session):
.\scripts\Invoke-DataverseInventory.ps1 -OutputPath .\data -UseDeviceCode

# Step 2 (full collection with FO and entity record counts):
.\scripts\Invoke-DataverseInventory.ps1 -OutputPath .\data -IncludeFO -IncludeEntityCounts

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
| `-IncludeEntityCounts` | `$false` | Fetch record counts per table (adds significant time) |
| `-EntityCountTop` | `150` | Max entities to count per environment |
| `-IncludeFO` | `$false` | Collect Finance & Operations batch/user/DIXF data |
| `-MaxEnvironments` | `0` (unlimited) | Safety cap — stops after N environments |
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
| Dual-write config & map status | `dualwrite-configs.json`, `dualwrite-entity-maps.json` | `DUALWRITE_MAPS_IN_ERROR` |

### Per Environment — Governance Checks (BAP API)

| Data | Flags Generated |
|---|---|
| Managed Environments status | `PRODUCTION_NOT_MANAGED_ENVIRONMENT` |
| Environment Group membership | `NOT_IN_ENVIRONMENT_GROUP` |
| Environment Admin role assignments | `NO_DEDICATED_ENVIRONMENT_ADMIN`, `ENVIRONMENT_ADMIN_IS_USER_NOT_GROUP` |

### Per Environment — Finance & Operations Side (`-IncludeFO`)

| Data | Output File | Flags Generated |
|---|---|---|
| Batch jobs (all, with status) | `fo-batch-jobs.json` | `FO_BATCH_JOBS_IN_ERROR`, `FO_MANY_WITHHELD_JOBS` |
| Missing standard FO cleanup jobs | `fo-missing-cleanup-jobs.json` | `FO_MISSING_CLEANUP_JOBS` |
| DIXF execution history (last 100) | `fo-dixf-execution-history.json` | `FO_DIXF_JOBS_FAILED` |
| Legal entities | `fo-legal-entities.json` | — |
| Active FO users + last login | `fo-users.json` | `FO_NO_ACTIVE_USERS_90D`, `FO_NO_ENABLED_USERS` |
| Pending workflow instances | `fo-pending-workflows.json` | `FO_MANY_STALLED_WORKFLOWS` |

---

## Governance Scoring

Each environment receives a **governance score from 0 to 100** based on its active flags. Scores are computed when the report is generated and do not require re-running the collection.

### How Scores Are Calculated

1. Start at **100** (perfect).
2. Each active flag deducts points based on its severity:

   | Severity | Deduction | Examples |
   |---|---|---|
   | Critical | 15 pts | `PRODUCTION_NOT_MANAGED_ENVIRONMENT`, `NO_DEDICATED_ENVIRONMENT_ADMIN`, `MAILBOX_SYNC_ERRORS` |
   | High | 8 pts | `NO_SCHEDULED_BULK_DELETE`, `PLUGIN_TRACE_LOGGING_ENABLED`, `AUDIT_RETENTION_SET_TO_FOREVER` |
   | Medium | 4 pts | `NO_RETENTION_POLICIES`, `NOT_IN_ENVIRONMENT_GROUP`, `NO_MANAGED_SOLUTIONS` |
   | Low | 1 pt | `NO_ACTIVE_USERS`, `NO_AUDIT_ACTIVITY_90D`, `OLD_WORKFLOW_LOGS_ACCUMULATING` |

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
- Storage differences ≥ 10 MB between runs are reported as growth events.

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

> **Tip**: Run with `-IncludeEntityCounts` to get additional table-level record counts that further inform cleanup decisions.

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
| Changes (Delta) | New flags, resolved flags, and storage growth vs. previous run |
| Governance Scores | Per-environment score table with Critical/High/Medium/Low flag counts and owners |
| All Environments | Full sortable/filterable table of all environments with flags, storage, and scores |
| Storage Analysis | Top 25 DB, File, and Log storage consumers with progress bars |
| Cleanup Gaps | Environments missing bulk delete jobs or with evidence of cleanup backlogs |
| Cleanup Recommendations | Ranked, actionable cleanup tasks with Bulk Delete Job filters and record counts |
| Activity / Unused | Environments with no active users or no recent audit activity |
| Finance & Operations | FO batch job health, DualWrite map errors, DIXF failures |
| Collection Info | Run metadata, authentication details, top flags across the tenant |

---

## Output Structure

```
data/
  environments.json              # All environments list (from BAP API)
  master-summary.json            # Cross-environment summary + all flags
  tenant-capacity.json           # Tenant-wide storage capacity
  inventory.log                  # Full run log with timestamps
  run-history/
    2025-04-01_120000.json       # Timestamped snapshots for delta reporting
    2025-04-08_120000.json
  environments/
    Production_MyOrg/
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
      entity-counts.json          # (only with -IncludeEntityCounts)
      role-assignments.json
      dualwrite-configs.json      # (only when FO solutions detected)
      dualwrite-entity-maps.json  # (only when FO solutions detected)
      fo-batch-jobs.json          # (only with -IncludeFO)
      fo-missing-cleanup-jobs.json
      fo-users.json
      fo-legal-entities.json
      fo-dixf-execution-history.json
      fo-pending-workflows.json
      ce-summary.json             # Cross-section summary + all CE flags
      fo-summary.json             # FO cross-section summary + FO flags

config/
  flag-severity.json             # Flag → severity mapping and score weights
  sku-profiles.json              # Per-SKU governance baseline and weights
  owners.json                    # Environment owner registry (auto-populated)

reports/
  PPACInventoryReport_20250401_120000.html
```

---

## Notes

- **Read-only**: no changes are made to any environment at any time.
- **Resume support**: re-run without `-Force` to skip already-collected environments and continue an interrupted run. Storage values always reflect the live BAP API data, even for skipped environments.
- **FO detection**: FO-enabled environments are identified by scanning for known FO-specific solution names in Dataverse. No FO API calls are made without `-IncludeFO`.
- **Token caching**: the Az module caches bearer tokens and reuses them across environments, minimizing authentication overhead on large tenants.
- **Rate limiting**: API calls use exponential backoff with up to 5 retries on HTTP 429 / 503 responses.
- **PS 5.1 compatibility**: the entire toolkit targets Windows PowerShell 5.1. No PS 7+ features are used.
