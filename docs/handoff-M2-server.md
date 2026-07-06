# Handoff → SERVER — Scansia Manager hosting + CF Access (unblocks M2)

Scansia Manager = new single-origin FastAPI web service replacing the 3 Sync-Scansia cron jobs (already suspended). Branch `feat/scansia-manager`. Library layer complete (175 tests); this handoff unblocks deploy/integration.

**Persistence decision (2026-07-06) — supersedes the earlier SQLite-on-disk plan:** NO server-side DB and NO persistent disk. State lives in the Google Sheet (audit + delete `before_snapshot` as tabs) + stateless HMAC-signed confirm-tokens; plans are recomputed at apply. This removes the paid persistent-disk requirement → **Render FREE tier is acceptable**. (Future GSheet→MySQL migration = one storage adapter behind an injected interface, no redesign.)

## Requests

- [ ] **Hosting**: Render **web service on the FREE tier** — no persistent disk, no paid plan. Free-tier caveats are ACCEPTABLE for an internal single-operator tool: (a) spins down after inactivity → ~30–60s cold start on the first request after idle; (b) ephemeral filesystem → fine, no local state is kept. Free is inherently single-instance (matches the in-process mutex model). An always-on/warm upgrade is a separate future decision, NOT required for M2.
- [ ] **Deploy model**: is `render.yaml` **blueprint-driven or dashboard-driven**? The current cron `type: cron` has NO `schedule` key → the yaml may not be authoritative. This decides whether the M3 cron decommission is a yaml edit or a manual dashboard delete.
- [ ] **Cloudflare Access from the first deploy**: IdP = Google Workspace, domain-only policy `@racoon-lab.it`. Backend validates `Cf-Access-Jwt-Assertion` (JWKS + `aud` from env + `exp`/`nbf`); the domain restriction is enforced both at CF Access AND re-verified in the backend via `email.endswith('@racoon-lab.it')` (NOT via the `hd` claim).
- [ ] **Origin lockdown**: custom domain proxied + CF Access with a CF-injected secret header (backend rejects requests missing the header), OR Render IP allowlist. Cloudflare Tunnel ruled out for a single container.
- [ ] **Health-probe under lockdown**: how does Render's internal probe reach the origin? → carve-out `GET /health` (ungated, no sensitive data: no store/counts/version). Reconcile the probe's source in the allowlist/service-token. On the FREE tier the service sleeps when idle → confirm how the probe + CF Access behave with spin-down (a cold start on the first gated request is expected/acceptable). Integration check: the deployed service must pass health under lockdown BEFORE declaring M2 done.
- [ ] **Shopify custom-app scopes** (dedicated app): `read_products, write_products, read_inventory, write_inventory, read_locations, read_publications, write_publications` (+ `read_orders` for F2). `read_publications`/`write_publications` are REQUIRED by `publishablePublish` (publish to the Online Store channel in the CREATE branch) — without them, M3 publish → access-denied.
- [ ] **Env vars on Render** (all secrets `sync: false`, set in dashboard, NEVER inline values in the committed yaml): `SHOPIFY_ADMIN_TOKEN`, `GOOGLE_CREDENTIALS_JSON`, `GSPREAD_SHEET_ID`, `PROMO_LOCATION_ID`, `SHOPIFY_STORE`, `TOKEN_SIGNING_SECRET` (HMAC key for the signed confirm-tokens — Option A), `CF_ACCESS_AUD` (Cloudflare Access JWT audience; backend fails closed if absent); non-secret: `SHOPIFY_API_VERSION=2025-07`. No disk/DB env vars (no persistence backend to provision).
- [ ] **Deploy gate**: the first push that arms the live web service requires **explicit owner confirmation** (Render auto-deploy on push).

Blocks: M2
