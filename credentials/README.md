# Credentials

This folder holds local credentials and secrets used when running the PPAC
Inventory collection. It is excluded from source control via `.gitignore`.

## What to store here

| File | Purpose |
|---|---|
| `tenant-config.json` | Tenant ID and optional subscription ID so you don't have to pass them as parameters each run |
| `service-principal.json` | App registration client ID and secret for non-interactive / scheduled runs |
| `*.pfx` / `*.pem` | Certificate files for certificate-based auth |

## Example: `tenant-config.json`

```json
{
  "TenantId":       "00000000-0000-0000-0000-000000000000",
  "SubscriptionId": "00000000-0000-0000-0000-000000000000",
  "Notes":          "Contoso production tenant"
}
```

## Example: `service-principal.json`

Store these values here instead of hard-coding them in scripts.

```json
{
  "ClientId":     "00000000-0000-0000-0000-000000000000",
  "ClientSecret": "your-secret-here",
  "TenantId":     "00000000-0000-0000-0000-000000000000"
}
```

Then reference in your run script:

```powershell
$creds = Get-Content .\credentials\service-principal.json | ConvertFrom-Json
Connect-AzAccount -ServicePrincipal `
    -TenantId     $creds.TenantId `
    -Credential   (New-Object PSCredential $creds.ClientId,
                    (ConvertTo-SecureString $creds.ClientSecret -AsPlainText -Force))
.\Start-Inventory.ps1 -TenantId $creds.TenantId -SkipPrereqCheck
```

## Security reminders

- Never commit files from this folder to git.
- Rotate secrets regularly.
- Prefer certificate-based auth or Managed Identity over client secrets when possible.
- This folder is already listed in `.gitignore` — verify with `git status` before any commit.
