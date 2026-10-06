# Sync-Scansia — Scansia Manager

Internal web utility for the Racoon-LAB Shopify outlet: it publishes the returned pairs on the shelf (the
"scansia resi") as outlet products, keeps their Promo stock in step with the register, prices them, and deletes the
outlets that are done. The register is MySQL `racoon` — STOCK_Manager's `sc_returned_pair` and
`sc_returned_pair_event`; the app's own tables are `sm_outlet_audit` (one row per write: the audit trail and the
write-ahead log) and `sm_outlet_lease` (one writing pass at a time). Business context (Italian):
`docs/business-context.md`.

## Layout

| Path | What |
|---|---|
| `backend/app.py` | the ASGI app (`backend.app:app`): HTTP Basic Auth on every route but `/health` and the decrement trigger |
| `backend/api/` | the routes and the shared preview → confirm → apply job pattern (`mutations.py`) |
| `backend/services/` | the reconciliation engine (`ledger`, `flows`, `recovery`, `passes`) and the verticals |
| `backend/persistence/` | the register, its MySQL and in-memory stores, the audit sink, the confirm tokens |
| `backend/shopify/` | Admin GraphQL operations and transport (pinned API version) |
| `backend/static/` | the single-page UI (tabs Scansia, Publish, Prezzi, Delete, Audit) |
| `main.py`, `src/reorder_collection.py` | the REORDER cron job (collection order by discount) |

## Routes

| Route | What |
|---|---|
| `GET /health` | liveness, no auth |
| `GET /scansia`, `POST /scansia/inventory` | the shelf; the live inventory join (a job) |
| `GET /audit` | the newest audit rows |
| `/init/*`, `/relevel/*` | "Inizializza" (the baseline) and the per-key re-level; `GET /init/status`, `GET /relevel/held` |
| `/outlet/publish/*` | publish |
| `POST /outlet/decrement/trigger`, `GET /outlet/decrement/status` | the decrement pass (bearer `TRIGGER_SECRET`; status behind Basic Auth) |
| `/prices/*` | pricing, revert, discharge-debt |
| `/outlet/delete/*`, `/outlet/cleanup/*`, `/outlet/exit/*`, `/outlet/deny-normalize/*`, `/outlet/zero-stock` | single delete, bulk cleanup, multi-outlet exit, deny-normalize, the zero-stock recon |
| `GET /outlet/reconcile/open`, `POST /outlet/reconcile/close` | a person's close of a row not closed |

A writing route runs a read-only preview job, returns a plan hash and a short-lived confirm token, and applies only
on a confirm that carries both — the apply re-reads live state first.

## Configuration (environment only)

| Variable | Use |
|---|---|
| `SHOPIFY_STORE`, `SHOPIFY_ADMIN_TOKEN` | the store and its Admin token (required) |
| `SHOPIFY_API_VERSION` | optional; may only restate the pinned version, else the app does not boot |
| `PROMO_LOCATION_ID` | the Promo location (GID or numeric id), checked against Shopify before every writing pass |
| `APP_PASSWORD`, `APP_USERNAME` (default `racoon`) | Basic Auth; no password → every gated route answers 503 |
| `TOKEN_SIGNING_SECRET` | the HMAC confirm tokens; unset → previews and applies fail closed |
| `TRIGGER_SECRET` | the decrement trigger's bearer; unset → the trigger answers 503 |
| `SCANSIA_DB_HOST`, `SCANSIA_DB_PORT` (3306), `SCANSIA_DB_USER`, `SCANSIA_DB_PASSWORD`, `SCANSIA_DB_NAME`, `SCANSIA_DB_CA_PATH` or `SCANSIA_DB_CA_B64` | the register over TLS, verified against the in-house CA |

REORDER reads `RUN_MODE=REORDER`, `COLLECTION_ID` (numeric), `DRY_RUN`, the two Shopify variables, and optionally
`SHOPIFY_MIN_INTERVAL_SEC` / `SHOPIFY_MAX_RETRIES`.

## Run

- Web app: `uvicorn backend.app:app` with the environment above. Production (Render `scansia-manager`) runs one
  instance with one worker (`IT/SERVER/infra-inventory.md`).
- REORDER: `RUN_MODE=REORDER COLLECTION_ID=<id> python -m main` — DRY-RUN unless `DRY_RUN` carries an explicit
  APPLY token (`apply`, `false`, `0`, `no`, `off`).
- Tests: `docs/testing.md`.

## Docs

`docs/f7-mysql-adapter-plan.md` (the design, the contract) · `docs/f7-build-notes.md` (how it is built) ·
`docs/f7-coverage-map.md` (plan item → test) · `TRACKER.md` (state).

## History

Until F7 the tool read the Google Sheet `Scarpe_in_Scansia` (`RUN_MODE=SYNC`, `FIX_PRICES`, gspread). That code and
its README are in git history before the F7 §4.5 removal, and in `../_archive/Sync-Scansia-main-to-check_2026-06-12.zip`.
