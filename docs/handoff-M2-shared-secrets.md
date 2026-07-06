# Handoff → shared-secrets — dedicated Google SA + Shopify token enumeration (unblocks M2)

Scansia Manager = new single-origin FastAPI web service replacing the 3 Sync-Scansia cron jobs (already suspended). Branch `feat/scansia-manager`. Library layer complete (175 tests); this handoff unblocks deploy/integration.

## Requests

- [ ] **Dedicated Google Service Account for the `Scarpe_in_Scansia` sheet only**: the `spreadsheets` OAuth scope is all-sheets → per-SA sharing is the only containment. Provision a new SA, share ONLY that sheet with it, set `GOOGLE_CREDENTIALS_JSON` on Render. Note: may require Google Workspace admin rights.
- [ ] **Shopify token**: after cutover to the dedicated custom app, remove the shared "Management esterno" token from THIS service's env ONLY. **Blocking prereq**: first enumerate (via `../docs/shared-secrets.md`) that this is the only consumer of the shared token, or coordinate with the other consumers. **DO NOT rotate** the shared token without the shared-secrets owner.

Blocks: M2
