# Sync-Scansia — Scansia Manager

Internal FastAPI web app (`backend/app.py`, Render service `scansia-manager`) that runs the Shopify outlet from the
returned-pairs register on MySQL `racoon` (`sc_returned_pair` + `sc_returned_pair_event`, owner STOCK_Manager; its
own tables `sm_outlet_audit`, `sm_outlet_lease`). The sheet `Scarpe_in_Scansia` is frozen since the F6 cutover and
no code reads it (plan §3.8). `main.py` keeps one cron entry point: `RUN_MODE=REORDER` (collection order by discount).

## Always-on rules

1. Parent rules apply in full (`../CLAUDE.md`).
2. **Writes are confirm-gated and DRY-RUN by default.** The app's writing verticals run preview → plan hash →
   confirm token (`CONFERMO` where the vertical asks); REORDER is DRY-RUN unless `DRY_RUN` carries an explicit
   APPLY token. An apply against the live store or `racoon` only with Ale's explicit confirmation (parent ASK-vs-ACT).
3. Config is env-only, never hardcoded: `SHOPIFY_STORE`, `SHOPIFY_ADMIN_TOKEN`, `SHOPIFY_API_VERSION` (may only
   restate the pin), `PROMO_LOCATION_ID`, `APP_USERNAME`/`APP_PASSWORD`, `TOKEN_SIGNING_SECRET`, `TRIGGER_SECRET`,
   `SCANSIA_DB_*` (`docs/f7-build-notes.md` §Code layering). The Shopify token is a shared secret
   (`../docs/shared-secrets.md`).
4. Integration tests run on `racoon_staging` only (`SCANSIA_STAGING=1`; option files
   `~/.config/racoon-scansia/<user>.cred`, read by the driver only — never opened, never printed).
5. `main` deploys `scansia-manager` on commit: a push to `main` is `GATE:push`.
6. `render.yaml` is the suspended `Sync-Scansia` cron (`RUN_MODE=SYNC`, retired): its deletion is SERVER/Ale's
   Render change (plan §3.8) — the file stays until then.
7. REORDER requires `COLLECTION_ID` (numeric), or it has nothing to order.

## Coverage test

Every item of plan §4 item 4 maps to its test in `docs/f7-coverage-map.md`: a change to a mechanism updates its row
and its test. The store contract (`tests/test_register_store.py`) runs on the in-memory fake and, opt-in, on staging.

## Lazy docs — read ONLY if the task matches

| Trigger | File |
|---|---|
| The design: reconciliation, baseline, prices, deletion, storage, connection rules | `docs/f7-mysql-adapter-plan.md` (the contract — never edited here) |
| How it is built: layering, engine, parameters, staging harness, facts read | `docs/f7-build-notes.md` |
| Which test covers which plan item (gate-2's checklist) | `docs/f7-coverage-map.md` |
| Running the suite: venv, skips, staging | `docs/testing.md` |
| What the outlet is for the business | `docs/business-context.md` |
