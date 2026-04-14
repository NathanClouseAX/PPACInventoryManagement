# PPAC Dataverse Inventory

Read-only PowerShell toolkit for auditing all Power Platform / Dataverse environments in a Microsoft tenant.

## What it collects

### Per environment (CE / Dataverse side)
| Data | File | Flags generated |
|---|---|---|
| Environment metadata & storage capacity | `metadata.json` | Storage thresholds |
| System users (active/inactive/integration) | `users.json` | `NO_ACTIVE_USERS` |
| Bulk delete jobs (scheduled cleanup) | `bulk-delete-jobs.json` | `NO_SCHEDULED_BULK_DELETE` |
| Async operation queue (by state + age) | `async-operations-summary.json` | `HIGH_SUSPENDED_JOBS`, `HIGH_FAILED_JOBS_30D`, `OLD_COMPLETED_JOBS_NOT_CLEANED` |
| Solutions (managed + unmanaged) | `solutions.json` | `HIGH_UNMANAGED_SOLUTIONS` |
| Workflows & Power Automate flows | `workflows.json` | `MANY_INACTIVE_WORKFLOWS` |
| Plugin assemblies & steps | `plugins.json`, `plugin-steps.json` | `MANY_INACTIVE_PLUGIN_STEPS` |
| Duplicate detection rules | `duplicate-detection-rules.json` | `NO_DUPLICATE_DETECTION_RULES` |
| Model-driven app modules | `app-modules.json` | — |
| Connection references | `connection-references.json` | — |
| Environment variables | `environment-variables.json` | — |
| Audit log sample (last 200, 90d activity) | `audit-sample.json` | `NO_AUDIT_ACTIVITY_90D` |
| Retention policies (Dataverse LTR) | `retention-policies.json` | `NO_RETENTION_POLICIES` |
| Process sessions count (flow volume) | _(summary only)_ | — |
| Entity definitions | `entity-definitions.json` | — |
| Entity record counts _(optional)_ | `entity-counts.json` | `LARGE_CUSTOM_ENTITIES_NO_CLEANUP` |
| Dual-write config & map status | `dualwrite-configs.json` | `DUALWRITE_MAPS_IN_ERROR` |

### Per environment (FO side, when `-IncludeFO` is used)
| Data | File | Flags generated |
|---|---|---|
| Batch jobs (all, with status) | `fo-batch-jobs.json` | `FO_BATCH_JOBS_IN_ERROR`, `FO_MANY_WITHHELD_JOBS` |
| Missing standard FO cleanup jobs | `fo-missing-cleanup-jobs.json` | `FO_MISSING_CLEANUP_JOBS` |
| Batch groups | `fo-batch-groups.json` | — |
| DIXF definition groups | `fo-dixf-definition-groups.json` | — |
| DIXF execution history (last 100) | `fo-dixf-execution-history.json` | `FO_DIXF_JOBS_FAILED` |
| Legal entities | `fo-legal-entities.json` | — |
| Active users + last login | `fo-users.json` | `FO_NO_ACTIVE_USERS_90D` |
| Pending workflow instances | `fo-pending-workflows.json` | `FO_MANY_STALLED_WORKFLOWS` |

## Quick start

```powershell
# 1. Open PowerShell as your Global Admin account
# 2. Navigate to the project folder
cd C:\...\PPACInventoryManagement

# 3. Run the convenience launcher (does prereqs + collect + report)
.\Start-Inventory.ps1

# With all options:
.\Start-Inventory.ps1 -TenantId "your-tenant-guid" `
                      -IncludeFO `
                      -IncludeEntityCounts `
                      -EnvironmentFilter "Production"
```

## Manual step-by-step

```powershell
# Step 1: Check/install required modules
.\scripts\00_Prerequisites.ps1

# Step 2: Collect data (interactive browser login)
.\scripts\Invoke-DataverseInventory.ps1 -OutputPath .\data

# Step 2 (device code / headless):
.\scripts\Invoke-DataverseInventory.ps1 -OutputPath .\data -UseDeviceCode

# Step 2 (with FO + entity counts):
.\scripts\Invoke-DataverseInventory.ps1 -OutputPath .\data -IncludeFO -IncludeEntityCounts

# Step 3: Generate HTML report
.\scripts\Generate-Report.ps1 -DataPath .\data -OpenReport
```

## Parameters

### `Invoke-DataverseInventory.ps1`

| Parameter | Default | Description |
|---|---|---|
| `-OutputPath` | `.\data` | Where to store collected JSON files |
| `-TenantId` | _(current context)_ | Azure AD tenant GUID |
| `-EnvironmentFilter` | _(all)_ | Regex filter on environment display name |
| `-SkipEnvironmentIds` | _(none)_ | Array of environment GUIDs to skip |
| `-IncludeEntityCounts` | `$false` | Fetch record counts per table (slow) |
| `-EntityCountTop` | `150` | How many entities to count per environment |
| `-IncludeFO` | `$false` | Collect FO batch/user/DIXF data |
| `-MaxEnvironments` | `0` (all) | Safety limit |
| `-Force` | `$false` | Re-collect environments already done |
| `-UseDeviceCode` | `$false` | Use device code flow (headless) |

## Output structure

```
data/
  environments.json              # All environments list
  master-summary.json            # Cross-env summary + all flags
  tenant-capacity.json           # Tenant-wide storage capacity
  inventory.log                  # Full run log
  environments/
    Production_MyOrg/
      metadata.json
      users.json
      bulk-delete-jobs.json
      async-operations-summary.json
      solutions.json
      workflows.json
      plugins.json
      plugin-steps.json
      duplicate-detection-rules.json
      app-modules.json
      connection-references.json
      environment-variables.json
      audit-sample.json
      retention-policies.json
      entity-definitions.json
      entity-counts.json          # (if -IncludeEntityCounts)
      dualwrite-configs.json      # (if FO detected)
      dualwrite-entity-maps.json  # (if FO detected)
      fo-batch-jobs.json          # (if -IncludeFO)
      fo-missing-cleanup-jobs.json
      fo-users.json
      fo-legal-entities.json
      fo-dixf-execution-history.json
      fo-pending-workflows.json
      ce-summary.json             # Cross-section summary + flags
      fo-summary.json             # FO cross-section summary + flags

reports/
  PPACInventoryReport_20240413_120000.html
```

## Requirements

- PowerShell 5.1+
- `Az.Accounts` >= 2.12.0, `Az.Resources` >= 6.0.0 (auto-installed by `00_Prerequisites.ps1`)
- Global Administrator **or** Power Platform Administrator role
- For FO data: System Administrator role in each FO environment

## Notes

- **Read-only**: no changes are made to any environment
- **Resume support**: re-run without `-Force` to skip already-collected environments
- FO is detected by scanning for known FO-specific solution names in Dataverse
- Token caching minimizes auth overhead across hundreds of environments
- Rate limiting is handled automatically with exponential backoff (up to 5 retries)
