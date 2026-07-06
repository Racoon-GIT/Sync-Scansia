# Handoff → SERVER — Scansia Manager secrets: RESOLVED (reuse existing)

Superseded by SERVER's own discovery (2026-07-06) + the switch to the house auth pattern (HTTP Basic Auth, no network perimeter). **Nothing to provision, nothing to rotate.**

- **Shopify token**: reuse the shared **"Management esterno"** token — live-verified (REST + GraphQL) to hold all 8 required scopes, incl. `read/write_publications`. **No dedicated custom app. No rotation.** Real consumers of this shared token = NoSluts, APP-Shopify_Order_Cleaner, STOCK_Manager, TAX-MANAGER, Price_Bulk-UPDT (5) — coordinate before any future change (registry: `../docs/shared-secrets.md`).
- **Google SA**: reuse the **Sync-Scansia** Service Account already scoped to the sheet. A dedicated per-sheet SA is optional, only if strict containment is later wanted (not required for M2).

No dedicated-SA / dedicated-app / rotation work remains. This row can be **closed** once the env vars are set on the Render service (see `handoff-M2-server.md`).

Blocks: nothing (folded into the server deploy).
