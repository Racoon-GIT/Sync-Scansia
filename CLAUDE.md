# Sync-Scansia

Python jobs for Shopify outlet maintenance, driven by the "Scarpe_in_Scansia" Google Sheet. Three workflows via a single entry point: `RUN_MODE=<SYNC|REORDER|FIX_PRICES> python -m main` (SYNC = outlet creation/management, REORDER = collection ordering, FIX_PRICES = bulk price updates).

## Always-on rules

1. Parent rules apply in full (`../CLAUDE.md`) — no project-specific overrides.
2. **Default to DRY-RUN** (`DRY_RUN=true`): APPLY mutates the live Shopify store (products, collections, prices). Run APPLY only with explicit user confirmation (parent ASK-vs-ACT).
3. Config is env-only (`SHOPIFY_ADMIN_TOKEN`, `GSPREAD_SHEET_ID`, `GOOGLE_CREDENTIALS_JSON`, …) — never hardcode. The Shopify token and Google SA are **shared secrets**: see `../docs/shared-secrets.md` before rotating.
4. REORDER requires `COLLECTION_ID` (or it has nothing to order).

## Lazy doc — `README.md` (1,011 lines, do NOT read it whole)

| Current task trigger | README section |
|---|---|
| Understand/modify a workflow's behavior | "MODALITÀ OPERATIVE (RUN_MODE)" — per-workflow subsections |
| Run, install, or configure (env vars, defaults) | "QUICK START" |
| Auth: Shopify scopes, Google Service Account, location IDs | "PREREQUISITI E DIPENDENZE" |

Historical deploy guides: `../_archive/Sync-Scansia-main-to-check_2026-06-12.zip` — unzip only for history research.
