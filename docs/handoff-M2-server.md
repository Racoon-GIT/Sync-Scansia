# Handoff → SERVER — Scansia Manager hosting + CF Access (unblocks M2)

Scansia Manager = new single-origin FastAPI web service replacing the 3 Sync-Scansia cron jobs (already suspended). Branch `feat/scansia-manager`. Library layer complete (175 tests); this handoff unblocks deploy/integration.

## Requests

- [ ] **Hosting**: Render **web service**, single-instance (`numInstances: 1`, `--workers 1`, autoscaling OFF), **persistent disk** (~1GB, for SQLite: append-only audit + confirm-token + plans). Confirm the `starter` tier can sustain always-on single-instance + persistent disk + the brief downtime on each deploy.
- [ ] **Deploy model**: is `render.yaml` **blueprint-driven or dashboard-driven**? The current cron `type: cron` has NO `schedule` key → the yaml may not be authoritative. This decides whether the M3 cron decommission is a yaml edit or a manual dashboard delete.
- [ ] **Cloudflare Access from the first deploy**: IdP = Google Workspace, domain-only policy `@racoon-lab.it`. Backend validates `Cf-Access-Jwt-Assertion` (JWKS + `aud` from env + `exp`/`nbf`); the domain restriction is enforced both at CF Access AND re-verified in the backend via `email.endswith('@racoon-lab.it')` (NOT via the `hd` claim).
- [ ] **Origin lockdown**: custom domain proxied + CF Access with a CF-injected secret header (backend rejects requests missing the header), OR Render IP allowlist. Cloudflare Tunnel ruled out for a single container.
- [ ] **Health-probe under lockdown**: how does Render's internal probe reach the origin? → carve-out `GET /health` (ungated, no sensitive data: no store/counts/version). Reconcile the probe's source in the allowlist/service-token. Integration check: the deployed service must pass health under lockdown BEFORE declaring M2 done.
- [ ] **Shopify custom-app scopes** (dedicated app): `read_products, write_products, read_inventory, write_inventory, read_locations, read_publications, write_publications` (+ `read_orders` for F2). `read_publications`/`write_publications` are REQUIRED by `publishablePublish` (publish to the Online Store channel in the CREATE branch) — without them, M3 publish → access-denied.
- [ ] **Env vars on Render** (all secrets `sync: false`, set in dashboard, NEVER inline values in the committed yaml): `SHOPIFY_ADMIN_TOKEN`, `GOOGLE_CREDENTIALS_JSON`, `GSPREAD_SHEET_ID`, `PROMO_LOCATION_ID`, `SHOPIFY_STORE`; non-secret: `SHOPIFY_API_VERSION=2025-07`.
- [ ] **Deploy gate**: the first push that arms the live web service requires **explicit owner confirmation** (Render auto-deploy on push).

Blocks: M2
