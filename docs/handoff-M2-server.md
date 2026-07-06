# Handoff → SERVER — Scansia Manager deploy (unblocks M2)

Scansia Manager = new single-origin FastAPI web service replacing the 3 Sync-Scansia cron jobs (already suspended). Branch `feat/scansia-manager`. Library layer complete (175 tests); this handoff unblocks the deploy.

**Design decisions (2026-07-06) — align to the house pattern, drop the earlier CF Access posture:**
- **Perimeter: NONE at the network layer.** Like every other live Racoon tool, access is gated in-app — not via CF Access / VPN / Tailscale / IP-allowlist (none are used in front of any live tool; Cloudflare isn't even in the stack — DNS is on Aruba).
- **App auth: HTTP Basic Auth** (single `APP_PASSWORD`, constant-time compare, fail-closed) — same pattern as Manager_Console. The irreversible-delete safety is app-level (signed confirm + `before_snapshot` + `CONFERMO` gesture), independent of the perimeter.
- **Persistence: no DB, no disk** (Option A): audit + delete `before_snapshot` in GSheet tabs, stateless confirm-token, plans recomputed at apply → **Render FREE tier**.

## Requests (much reduced vs the original CF Access plan)

> **STATUS: READY NOW (2026-07-07).** The F1 web layer is built and tested (branch `feat/scansia-manager`, tip `9049c13`, 344 tests in a venv with fastapi). This handoff is fully actionable — nothing left on the SVILUPPO side before deploy.

- [ ] **Hosting**: Render **web service on the FREE tier** (same as the other 7 Render services). No persistent disk, no paid plan. Deploy is **dashboard-driven** (confirmed) → the M3 cron decommission is a manual dashboard delete of the `Sync-Scansia` cron. Deploy config: `buildCommand: pip install -r requirements.txt` · `startCommand: uvicorn backend.app:app --workers 1 --host 0.0.0.0 --port $PORT` · `healthCheckPath: /health` (ungated) · Python 3.12 (runtime.txt).
- [ ] **Spin-down / keepalive**: the free tier sleeps when idle. Confirm whether to add this service to the existing **Scheduler keepalive layer** (infra §5-bis) or accept the ~30–60s cold start (fine for a 2×/year tool).
- [ ] **Secrets — REUSE existing (per SERVER's own discovery), nothing to provision**:
  - Shopify: **reuse the shared "Management esterno" token** — already holds all required scopes (8/8, live-verified, incl. `read/write_publications`). **No dedicated custom app. No rotation.**
  - Google: **reuse the Sync-Scansia Service Account** (dedicated SA only if strict per-sheet containment is later wanted — not required for M2).
- [ ] **Env vars on Render** (secrets `sync: false`, set in dashboard, NEVER inline in the committed yaml): `SHOPIFY_ADMIN_TOKEN`, `GOOGLE_CREDENTIALS_JSON`, `GSPREAD_SHEET_ID`, `PROMO_LOCATION_ID`, `SHOPIFY_STORE`, `APP_PASSWORD` (Basic Auth), `TOKEN_SIGNING_SECRET` (signed confirm-token); non-secret `SHOPIFY_API_VERSION=2025-07`.
- [ ] **Deploy gate**: the first push that arms the live web service requires **explicit owner confirmation** (Render auto-deploy on push).

## Dropped from the original brief (do NOT provision)
CF Access, Google Workspace SSO, JWKS validation, `Cf-Access-Jwt-Assertion`, origin lockdown / CF-injected secret-header, custom proxied domain, `CF_ACCESS_AUD`. None are used by any live Racoon tool — disproportionate for a 2×/year internal tool.

## Sequencing
The SERVER action is now a **trivial Render-free deploy + env vars**, and it waits on **SVILUPPO building the M2 web layer** (FastAPI + Basic Auth + dashboard + preview/apply endpoints). Not "blocked on SERVER" anymore — mostly SVILUPPO work, then a small deploy behind the owner deploy-gate.

Blocks: M2
