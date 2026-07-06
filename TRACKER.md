# TRACKER — Scansia Manager (refactor Sync-Scansia → web utility interna)

## Current state

**Fase**: F1 (MVP) — inizio esecuzione. Branch `feat/scansia-manager`.
**Piano approvato**: `~/.claude/plans/compressed-foraging-stream.md` (327 righe, 9 giri di critica avversariale, F1≈8 execution-ready; F2 reorder = ciclo di pianificazione dedicato successivo).

**Milestone F1** (critical path `M1a→M1b→[handoff SERVER+shared-secrets]→M2→M3→M4→M5→M-docs`):
- [x] **M1a-lib** — *(layer libreria + 8 fix COMPLETO)* `transport.py` (unificato, fix timeout, retry) · `shopify/ops.py` (+net-new) · `gsheet/` · `services/resolvers.py` · `services/outlet_service.py` (orchestrazione publish CREATE/ACTIVE/DRAFT). **Fix**: 1 (cleanup tutte location≠Promo) ✅ · 2 (DRY_RUN fail-closed main.py) ✅ · 3 (userErrors→raise) ✅ · 4 (blocco prezzo 0/≥compareAt) ✅ · 5 (size-match Size+SKU cross-check) ✅ · 7 (media→productSet) ✅ · **6 (tag) e 8 (batching) = gated/F2, non M1a**. Review avversariale su ogni layer; 2 HIGH phantom-stock/oversell intercettati e corretti (duplicato DRAFT-atomico; DRAFT-revive quarantena). **129 test verdi.**
- [~] **M1a-consolidate** — ✅ **GSheet layer** `backend/gsheet/` (append-oriented, row_uuid+reconciled, `backfill_cutover` idempotente anti-re-inflate VERIFICATO, write-back per row_uuid+CAS, `parse_qta`, `eligible_rows` parità legacy, DRY_RUN-safe read). Review avversariale PASS. ✅ `docs/business-context.md` (ground-truth verificata: 3 collezioni smart, loop publish→SALDI→reorder, Q-bf-storefront, oversell 38 CONTINUE, segnale rientro, downstream feed). ✅ cleanup moduli morti (`src/gsheets.py`, `src/utils.py`, `reorder/` — verificato zero import esterni prima di rimuovere; `src/exceptions.py` era già assente). ⏭ resta SOLO: regression checkpoint CLI-fixato-vs-prod (richiede store live → deferito post-credenziali). Orfani deps `pandas`/`slugify`/`tenacity` (solo in `utils.py` rimosso) → rimozione da requirements deferita (tocca build; `slugify` rientra a F2).
- [~] **M1b** — ✅ **codice** op net-new (`read_variant_inventory` 0-vs-assente + cap-guard via `hasNextPage`, `product_delete`, `get_online_store_publication_id`+`product_publish` via `publishablePublish`, `enumerate_outlet_products` paginata) + `services/resolvers.py` (outlet/source, SKU-non-univoco: dedup per gid, multi-match→warning, mai outlet-come-source). Review verificata su doc 2025-07, 107 test. ⏭ resta (richiede store live, post-credenziali): verifica read-only op contro store reale + check orphan-handle + productDuplicate→canale Online Store + wildcard enum case-insensitive.
- [ ] **M2** — Read+Dashboard + endpoint preview/apply + **HTTP Basic Auth** + adapter GSheet (audit/token/persistence ports) + deploy Render free. **NON più bloccato**: è codice SVILUPPO (nessun perimetro di rete, secret riusati). La parte SERVER = deploy free banale a fine-M2 (owner deploy-gate). **Buildabile ora.**
- [ ] **M3** — Publish (decommissiona cron; integrazione Make return-signal). *(bloccato su AUTOMATE; gate Q-bf-storefront)*
- [~] **M4** — *(libreria COMPLETA)* `services/pricing_service.py`: `prices_preview`/`prices_apply`/`revert_prices`/`discharge_debt_count`; 3 modalità (% per prodotto, prezzo diretto/repair, regole bulk override>bulk); validazione price≤0/≥compareAt; eligibility+ACTIVE-only; fill-missing RIFIUTA; revert-capture prima di ogni push (TOCTOU plan_hash). Review: invarianti CORRETTI. ⏭ resta: endpoint web + audit_sink concreto + confirm-token (M2+); acceptance discharge-debt su store live.
- [~] **M5** — *(libreria COMPLETA)* `services/delete_service.py`: `zero_stock_candidates` (predicato per-variante esatto: available==0 tutte + Promo-present + committed==0 + for-all DENY; UNKNOWN/oversell/non-Promo/`available is None`→REVIEW), `cleanup_preview/apply` (hard cap 50 + soglia nel plan_hash, ARCHIVE-first surfaced), `delete_single_apply`, `deny_normalize`; **snapshot durevole OBBLIGATORIO prima di product_delete con abort-on-failure** + gesto umano + re-verifica drift. +1 op `get_product_core` (title/handle/tags/collections). Review delete-safety: invariante centrale REGGE su tutti i percorsi, 0 high/critical. ⏭ resta: endpoint web + audit_sink 2-sedi concreto + confirm-token (M2/M5-web); `urlRedirectCreate` follow-up.
- [ ] **M-docs** — runbook + rewrite README + ritiro FIX_PRICES_README + cross-doc (system-map, dependencies-graph, shared-secrets) + CLAUDE.md progetto.

**Handoff (loggati in ../OPEN-HANDOFFS.md)**:
- **SERVER** (fine-M2, NON più bloccante): deploy Render **free** web service + env var (`APP_PASSWORD`, `TOKEN_SIGNING_SECRET`, + secret riusati) + eventuale keepalive Scheduler. Design risolto (Basic Auth, no perimetro). SERVER discovery DONE. Brief: `docs/handoff-M2-server.md`.
- **~~shared-secrets~~ RISOLTO** (SERVER-verified): riuso token "Management esterno" (8/8 scope) → no app dedicata; riuso SA Sync-Scansia → no SA dedicata; no rotazione. Niente da provisionare. Brief: `docs/handoff-M2-shared-secrets.md`.
- **AUTOMATE** (pre-M3, ancora aperto): integrazione flusso Make return-signal (contratto merge/check, colonne row_uuid/reconciled, append per header-non-indice, backfill cutover). Non è un peer OPEN-HANDOFFS → azione owner. Brief: `docs/handoff-M3-automate.md`.

**Domande owner aperte** (tracciate nel piano §Open items): Q-oversell (default DENY), Q-bf-storefront (gate M3), Q-prodcollection, Q-modelsrc, Q-nest, Q-nonfootwear, Q-saleschannel (verifico io in M1b).

**Decisioni owner risolte**: hard-delete · titolo=sorgente · motore reorder D/B/A B-over-model · MVP-first · Q-active/freshreturn=**Make append-row = segnale rientro, delta idempotente reconciled, PULL** · Q-roweligibility=filtro legacy `online=SI AND qta>0` · Q-fixprices=rifiuta fill-missing · **Q-persistenza (2026-07-06) = Option A "no DB, sheet-centric"** (supera SQLite-on-disk: disco Render a pagamento escluso): confirm-token firmati HMAC stateless, audit + `before_snapshot` delete su tab GSheet, piani ricalcolati all'apply → **Render FREE ok**. Storage dietro Protocol iniettati (`AuditSink`/`TokenService`) → porting a MySQL domani = 1 adapter, no redesign. 2ª sede durevole delete opzionale = Cloudflare R2 (free). · **Q-auth (2026-07-06) = HTTP Basic Auth app-level, NESSUN perimetro di rete** (Explore+SERVER: nessun tool interno Racoon usa CF/Tailscale/VPN/IP-allowlist; CF non è nemmeno nello stack, DNS Aruba). `APP_PASSWORD` constant-time fail-closed, come Manager_Console. Eliminati CF Access/JWKS/origin-lockdown/`CF_ACCESS_AUD`/dominio custom. **Secret riusati** (SERVER-verified): token "Management esterno" 8/8 scope → no app dedicata; SA Sync-Scansia → no SA dedicata; no rotazione. Safety delete resta app-level.

**Layer libreria F1 = COMPLETO** (publish + prezzi + delete + resolver + gsheet + transport/ops). Restano SOLO due item **locali non bloccati**: (a) `docs/business-context.md` (recon fresco, M1a-consolidate); (b) cleanup moduli morti (`src/gsheets.py`, `src/utils.py`, dir `reorder/`, `.DS_Store`). Tutto il resto (endpoint web, audit_sink concreto, confirm-token, deploy) è **M2+ bloccato sui 3 handoff** (SERVER, shared-secrets, AUTOMATE) — **da loggare in `../OPEN-HANDOFFS.md` via /handoff**.

**Known follow-up** (non bloccanti): (1) fallimento di `read_variant_inventory` DOPO `productDuplicate` in CREATE → **duplicato DRAFT orfano** (il resolver lo ri-aggancia al run successivo; TODO reconcile per-titolo). (2) delete: `urlRedirectCreate` (404/SEO su delete ACTIVE) non implementato (nessun op) → runbook M-docs. (3) delete drift a re-verify → abort dell'INTERO batch (più stretto di esclusione per-item; il conteggio umano è vincolato al set). (4) `delete_single_apply` non ri-applica il predicato zero-stock (by design: serve a un CREATE crashato con stock ereditato).

**Stato working tree**: layer libreria completo — `backend/services/{outlet_service,resolvers,pricing_service,delete_service}` + `shopify/{transport,ops}` + `gsheet/` + `config` + main.py + tests. **175 test verdi.** Commit `a76ff86` (M1a-lib, 129 test) fatto; incremento M4+M5 (pricing/delete/get_product_core + test, +46) in commit.

---

## Milestone dettaglio & log

*(Dettaglio storico e note di esecuzione qui sotto; la sezione Current state va RISCRITTA a ogni fine sessione, mai appesa.)*

### M1a-lib — 8 fix da portare (con test)
1. Magazzino cleanup saltato su path ID (`sync.py:868-892`).
2. DRY_RUN fail-closed (`main.py:74`): unset/non-riconosciuto → DRY-RUN; muta solo con token APPLY in allowlist.
3. `userErrors` silenziati (`sync.py:562`, `465`) → raise.
4. Fallback prezzo `0.00/0.00` → blocca.
5. Size-matching su "Size" + surfacing.
6. Tag `outlet` + backfill (gated, non M1a) — se adottato, publish SETTA il tag e non azzera gli altri.
7. Images legacy → `productSet`/`productUpdate` (branch batch/REST-fallback DROPPED).
8. Batching >250 (F2): attesa `job.done` per-batch + ABORT su timeout/errore.
